# Audit Remediation Plan — par-rt-db (Fable audit 2026-11-13)

Per-issue execution playbook consumed by `/fix-audit`. Order matches the
`## Remediation Plan` phases in `AUDIT.md`. Every entry is executable without
re-deriving the analysis. Global rules for every agent:

- The project gate is `make checkall` (this repo has no `checkall-ci` target; `checkall` is the done gate). Dev Postgres first: `make dev-db-up`. `merge_test` is a load canary — a TRY-2 pass is acceptable evidence.
- Never violate: single-writer committer (no second writer), `Committers::submit` routing, `publish_taps` in committer arms only, RtDbError envelope genericity, SQL quoting/binding invariants, no-co-author commits, machine specifics out of commits.
- Doc sweeps MUST exclude `.claude/worktrees/` and `.worktrees/` copies.
-parsight (`repo_id: par-rt-db`) is indexed current at `eea4aab`; use it to enumerate callers before moving/renaming anything.

---

## Phase 1

### [SEC-205] Scope /metrics away from unauthenticated build-identity exposure
- **Files**: `server/src/lib.rs:858` (route registration), `deploy/docker-compose.yml` + `.env.example` if the port-scoping option is chosen, `README.md` (metrics section).
- **Steps**:
  1. Choose the mechanism: (a) admin-gate the route (bearer admin key, reusing the existing admin auth extractor — simplest, but breaks the local curl/scraper habit documented in deploy/README), or (b) move the exporter to an interior port via a second `axum::Router` listener bound to `127.0.0.1` (compose then does not expose it through the tunnel). Prefer (b) if the tunnel ingress is the only public path; confirm with the deploy runbook first.
  2. Implement; keep `/metrics` path semantics identical for whatever still reaches it.
  3. If config gained a knob: add the `RTDB_*` var to `.env.example` AND `docker-compose.yml` environment block, or env-drift-check fails.
- **Method**: this is Phase-1-promoted because it edits `lib.rs`, which ARC-206 later decomposes — land this first, and ARC-206 reads post-change state. parsight: `get_impact` on the metrics router builder before moving the listener.
- **Verify**: `curl -s https://<prod-rtdb-host>/metrics` (or locally `curl localhost:<port>/metrics`) reflects the chosen scope; `make checkall` green; deploy/README metrics paragraph updated to match.

---

## Phase 3a — Security

### [SEC-201] Re-check admin session expiry on per-op WS authorization
- **Files**: `server/src/ws.rs:557-570` (`authorize_op`), `server/src/auth/session.rs:366-382` (`session_still_valid`), `server/src/auth/mod.rs:307-319` (the correct pattern), `server/src/subs/mod.rs:228,453` (audit only), `server/src/auth/session.rs` tests.
- **Steps**:
  1. Confirm `Principal::User` already carries `expires_at` (the HTTP admin path checks it — find where: `get_symbol_context` on `session_still_valid`). Expose it to the WS admin branch.
  2. In `authorize_op`'s admin branch, after the revocation check, add: `if expires_at < now_ms() { return Err(RtDbError::unauthorized("session expired")) }` BEFORE constructing `PrincipalCtx::admin_bypass()`.
  3. Read `subs/mod.rs:228,453` (`authorize_host_push`, `fan_out_inner`): if they can take an admin principal from an open connection, apply the same check.
  4. Test: create an admin OAuth session, force `expires_at` into the past (direct insert via the test harness's db handle, or a TTL override), open `/sync`, send an admin op, assert `unauthorized` + that the socket did NOT get admin_bypass. Pin it next to the existing per-op re-auth tests (grep `authorize_op` in `server/tests/`).
- **Method**: this is security-relevant on the public deployment; generic error message only (`"session expired"` matches the existing user-branch message — reuse it verbatim for consistency). Do not touch the non-admin branch (already correct).
- **Verify**: `cargo test --test main <new_test_name>` green; `make checkall` green.

### [SEC-202] Genericize serde-error 500s on the schema-push path
- **Files**: `server/src/migrate/apply.rs:426`, `:545`, `server/src/committer/arms/migrate.rs:95`, `server/src/admin/dbs.rs:155`; verify-also `server/src/admin/dbs.rs:487,508`, `server/src/workflows.rs:821`, `server/src/scheduler.rs:541,544`, `server/src/http_api/schedule.rs:123`.
- **Steps**:
  1. At each `map_err(|e| RtDbError::internal(e.to_string()))` / `internal(format!("... {e}"))` site on a CLIENT-REACHABLE path: capture the error with `tracing::error!(error = %e, "…")`, return `RtDbError::internal("internal error")` (or the existing operator-facing constant used by the sqlx From impl).
  2. Admin-gated routes (`admin/dbs.rs`) may keep an operator-facing constant like `"export serialization failed"` — still no raw serde/sqlx text.
  3. Grep-sweep to confirm no other `internal(e.to_string())` / `internal(format!("...{e:?}"))` sites on client paths remain: `grep -rn "internal(" server/src | grep -E "to_string|format!"` — each hit either genericized or justified in the commit message.
- **Method**: the invariant text lives at `error.rs:3-4` and CLAUDE.md; `From<sqlx::Error>` at `error.rs:158-176` is the pattern. Do not change error CODES (that would ripple the error-code matrix across seven clients).
- **Verify**: `make checkall` green; the existing generic-500 envelope tests still pass unchanged.

### [SEC-202b / SEC-203 + SEC-204] Hardening batch
- **Files**: `ts-client/README.md` (token-mode section), `server/src/http_api/storage.rs:226`.
- **Steps**: (1) In ts-client README's auth/token section, add a paragraph: token mode stores the session token in `localStorage` and is readable by any XSS on the page; browser apps should prefer cookie mode (HttpOnly). (2) Rewrite the storage.rs comment to state the real default: per-IP public-storage limit is on by default at 300 RPM (`config/limits.rs:80`).
- **Verify**: `make checkall` green (comment + doc only).

---

## Phase 3b — Architecture

### [ARC-205] Reconcile subscription invalidation after a listener reconnect gap (LANDS FIRST)
- **Files**: `server/src/notify.rs:461-561` (`run_write_set_listener`), `server/src/notify.rs` tests, `server/src/metrics/` (reconnect counter).
- **Steps**:
  1. In the reconnect path of `run_write_set_listener`, after re-establishing LISTEN: trigger a bounded global invalidation — re-fan-out all subscriptions for the dbs this server serves (reuse the same fan-out entry the normal notification path uses), OR sweep `change_head` seq since the disconnect timestamp and re-publish. Pick whichever reads smaller against the existing fan-out API; do both only if trivial.
  2. Add a `PgListener` reconnect counter metric (`rtdb_pg_listener_reconnects_total`) following the counter pattern in `metrics/counters.rs`; render it in `render_prometheus`.
  3. Document the bounded-staleness contract in `docs/ARCHITECTURE.md`'s notify section (one paragraph: what a reconnect gap costs now).
- **Method**: reuse the self-dedupe and backoff plumbing already in `notify.rs` — do not add a second backoff mechanism. This card MUST land before/with ARC-201 (same listener lifecycle, one review).
- **Verify**: `make checkall` green; new unit test pins "notification published during a simulated gap is delivered after reconnect" (mock or integration-test the re-fan-out hook).

### [ARC-201 + ARC-203] HotConfig cross-replica coherence + PATCH single-flight
- **Files**: `server/src/admin/settings.rs:112-189` (`patch_config`), `server/src/config/hot.rs:298-315` (`save_hot`), `server/src/notify.rs` (new config listener, ARC-205's pattern), `server/src/main.rs` (listener spawn + keep the boot `load_hot`), `docs/ARCHITECTURE.md` + `CLAUDE.md` (constraint docs if the full propagation is deferred).
- **Steps**:
  1. Single-flight first (ARC-203): add a `tokio::sync::Mutex<()>` to `Runtime`; `patch_config` holds it across load→apply→save→swap. This alone kills the concurrent-PATCH last-store-win and the stale-replica-reverts-operator hazard.
  2. NOTIFY (ARC-201): after a successful `save_hot`, `pg_notify('rtdb_config_changed', <instance_id>)` following the `publish_schema_changed` shape in `notify.rs`. Add a config listener task (spawned in `main.rs` alongside the schema listener) that re-runs `load_hot` + ArcSwap swap on every notification, with the schema listener's self-dedupe (skip own instance id) and backoff.
  3. If the full listener is too large for one pass, ship step 1 + documentation of the multi-replica constraint (ARCHITECTURE.md hot-config section, CLAUDE.md invariant list) and leave the NOTIFY as the follow-up in the card notes — the criterion allows the documented-constraint path.
- **Method**: `run_schema_listener` (`notify.rs:509`) is the pattern; reuse its reconnect/backoff. The hot row persists in Postgres and outlives processes (CLAUDE.md) — never reset via SQL; the listener must call the same `load_hot` path boot uses.
- **Verify**: `make checkall` green; test pins (a) two concurrent PATCHes both land (second sees first's fields), (b) with multi-instance feature config: a notification on replica B swaps its ArcSwap (unit-test the listener handler with a fake notify payload).

### [ARC-202 + QA-002] Mirror-engine management program
- **Files**: per the linked enhancement plans — `docs/fable/ENH-053-spec-table-rule-families.md` (spec-table families, board card ENH-053) and `docs/fable/ENH-054-mirror-engine-coverage-gates.md` (coverage floors, board card ENH-054).
- **Steps**: `/fix-audit` should NOT implement this card directly — it is the umbrella for ENH-053/ENH-054 (which `/enhancement-all` works) plus a parsight `audit_repository(decomposition)` run over `rust-client/src/in_memory/query.rs` to pick a seam (analysis-only; file the chosen seam as a follow-up card). Mark the umbrella card done once ENH-053, ENH-054 are filed-worked and the decomposition analysis exists.
- **Verify**: parsight decomposition output exists for `rust-client/src/in_memory/query.rs`; ENH-053/ENH-054 cards exist (they do — filed 2026-11-13).

### [ARC-204 + QA-006] Server complexity splits
- **Files**: `server/src/db/bootstrap.rs:111`, `server/src/committer/arms/workflow.rs:16`, `server/src/query/search.rs:184`, `server/src/webhook.rs:136`, `server/src/query/terminals.rs:264,1033`.
- **Steps** (ordered by dependency): 1) QA-007's `ensure_module_table` helper lands FIRST; 2) split `bootstrap_ddl` into per-subsystem functions (`auth_tables`, `rate_counters`, `storage_tables`, …) each returning/ executing its CREATE list through the shared helper — purely mechanical; 3) extract `handle_workflow_advance` sub-steps into `committer/arms/workflow/` modules following the existing arms precedent; 4) `compile_search` — extract the per-clause compilers (`get_impact` on it first; it has fan-in from the query compile path); 5) `is_blocked_ip` and the terminals guards — extract CIDR/allowlist parsing helpers. NO behavior changes; every extraction is a pure move.
- **Method**: nesting is already shallow; keep it that way — split breadth, don't add early-returns. QA-003 (committer loop) coordinates: `committer/mod.rs` and `arms/purge.rs` belong to QA-003's pass, not this one.
- **Verify**: `make checkall` green; `cargo clippy` unchanged; parsight `find_most_complex_functions` shows each target below its before value.

### [ARC-206] Decompose AppState::new
- **Files**: `server/src/lib.rs:337-619`.
- **Steps**: extract per-concern builders following the provider-split pattern: `build_realtime(...) -> RealtimeGroup`, `build_runtime(...) -> RuntimeGroup`, `build_limits(...) -> LimitsGroup` (group structs hold the moved fields; `AppState::new` becomes ordered group construction + the final struct literal). Preserve the move-order constraints as `// built before its dependent` comments ON THE GROUP CONSTRUCTION ORDER.
- **Method**: SEC-205 edits this file first (Phase 1) — read current state. Behavior-preserving; no public API change.
- **Verify**: `make checkall` green; `AppState::new` reduced to ≤ ~80 lines of ordered calls.

### [ARC-207] spec_corpus/ submodule
- **Files**: `server/src/spec_corpus.rs`, `spec_corpus_pagination.rs`, `spec_corpus_validation.rs`, rows JSON modules.
- **Steps**: pure move into `server/src/spec_corpus/` (mod.rs re-exports or `mod spec_corpus;` with pub(crate) use paths updated). Update the Dockerfile test-stub list ONLY if any of these are declared `[[test]]` targets (they are modules, not targets — check `server/Cargo.toml`; the dockerfile-stub-check gate will fail loudly if a stub is missed).
- **Verify**: `make checkall` green (its dockerfile-stub leg catches stub drift).

---

## Phase 3c — Code Quality

### [QA-003] Committer lease-verification extraction
- **Files**: `server/src/committer/mod.rs:1491` (`run_committer`), lines 1524-1569 (the block), `committer/arms/purge.rs` context only.
- **Steps**: extract lines 1524-1569 verbatim into `async fn verify_write_lease_and_demote(&mut self, ...)` (match the existing helper visibility/naming conventions in the file); the call site passes the same locals. NO logic edits — if the block reads a mutable local, pass it `&mut`.
- **Method**: single-writer invariant, `Committers::submit` routing, `publish_taps` arm rule all untouched (pure move). QA-003 before ARC-204's committer-adjacent work (same conflict files). parsight: `get_impact` on `run_committer` to confirm no macro/tests reference the inner block shape.
- **Verify**: `make checkall` green; existing committer/lease tests pass UNCHANGED (no test edits — if a test needs edits, the extraction changed behavior; stop and reassess).

### [QA-004 → ENH-055] Python admin ops extraction
- **Files/Steps/Verify**: `docs/fable/ENH-055-python-admin-ops-extraction.md` (board card ENH-055). `/fix-audit` should defer to `/enhancement-all` for this one, or implement directly from the plan — the plan is complete either way.

### [QA-005] Three duplicate hoists
- **Files**: `python-client/src/par_rt_db/http_client.py:771` + `aio_http_client.py:656`; `dashboard/src/components/webhooks.tsx:30` + `dashboard/src/pages/WebhooksPage.tsx:231`; `scripts/bench/lib/{scenarioB,scenarioD,scenarioE}.ts`.
- **Steps**: (1) python: hoist `transform_url` into the shared HTTP base module both clients already inherit from (read the class hierarchy first — likely `http_client.py` defines it; make it a module-level function both import). (2) dashboard: export `deliveryBadgeClass` from `components/webhooks.tsx`, import in `WebhooksPage.tsx`. (3) bench: create `scripts/bench/lib/common.ts` with `waitAuthenticated`; update the three scenario imports.
- **Verify**: `make checkall` green (python + dashboard + bench legs all covered by the gate).

### [QA-007] Shared ensure_module_table helper
- **Files**: `server/src/change_log.rs:55`, `server/src/mutation_log.rs:31`, `server/src/scheduler.rs:264`, `server/src/schema_history.rs:42`, `server/src/storage.rs:42`, plus the sixth site (grep `CREATE TABLE IF NOT EXISTS` in server/src to enumerate).
- **Steps**: add `pub(crate) async fn ensure_module_table(pool: &PgPool, db: &str, ddl: &str) -> Result<PgPool, RtDbError>` in a shared module (follow `change_log.rs`'s version — the most elaborate, with singleton seeding; decide whether seeding stays per-caller via a callback param or moves in). Replace each site's body with a call. The DDL strings stay per-module.
- **Method**: behavior-preserving; identifiers were validated at each call site — the helper centralizes validate/quote/execute. Lands BEFORE ARC-204's bootstrap split. No wire change → no corpus case.
- **Verify**: `make checkall` green (the DDL/quote-sensitive integration tests all exercise these tables).

### [QA-008] Go-client corpus-coverage verification (verify-only)
- **Files**: read-only over `go-client/`, `wire-corpus/`.
- **Steps**: for each candidate export (cursor codecs, OrderAsc/OrderDesc, RecordOf, TableBuilder.Unique, HTTPStatus, the ~20 httpclient/inmemory methods), grep the corpus runners + tests. Classify each: corpus-covered / intentional surface (document in the card) / corpus gap (file a card with acceptance criteria per the upstream-filing rule). DELETIONS ARE FORBIDDEN by this card.
- **Verify**: card notes carry the per-name classification list.

### [QA-009] Swift weak-self audit
- **Files**: `swift-client/Sources/ParRtDbClient/WsClient.swift:353,650-652,408`.
- **Steps**: (1) verify the shutdown path releases `runLoopTask` (trace `stop()`/`finish()` → task cancellation → stored task nil'd); (2) switch the stored task to `Task { [weak self] ... }` guarding each await (or keep strong capture with a written justification comment if the loop must own the client until cancellation — document which); (3) same for the `:408` inner Task.
- **Verify**: `make swift-client-checkall` green (Darwin leg).

### [QA-010] Go wsclient frame-send logging
- **Files**: `go-client/wsclient/feed.go:145,174,230,269`, `presence.go:434`, `subscription.go:133,171`, `client.go:231,398`.
- **Steps**: replace `_ = f.client.sendFrame(...)` with `if err := ...; err != nil { <debug-level log> }` following the file's existing logging idiom (check whether the package uses stdlib log or a logger field; stdlib-only client constraint — use `log.Printf` with a `wsclient:` prefix if no logger exists).
- **Verify**: `cd go-client && go test ./...` green; `gofmt`/`go vet` clean (`make go-client-lint`).

### [QA-011 + QA-012 + QA-013] Low quality batch
- **Files**: dashboard pages (QA-011 — wrapper hook OR documented keep-decision), `go-client/wire/mutation_extra_test.go:253` + `golden_vector_test.go:219` (table-driven), `ts-client/src/client.ts:147,205` (type the timer handles, e.g. `ReturnType<typeof setTimeout>`).
- **Verify**: `make checkall` green.

---

## Phase 3d — Documentation

Sweep hygiene for every entry: exclude `.claude/worktrees/` and `.worktrees/`.

### [DOC-001 + DOC-012 + DOC-013] Stale source-path sweep
- **Files**: `server/README.md` (layout table ~106-130 + intro base-path sentence), `PRODUCT.md`, `rust-client/README.md`.
- **Steps**: update each stale `src/<name>.rs` reference to the module directory (`subs/`, `db/`, `metrics/`, `http_api/`, `ws/`, …); verify each new path exists on disk before writing it (`ls server/src/<dir>`); reconcile the intro's stated base path with the row prefixes.
- **Verify**: `grep -rn "src/subs\.rs\|src/db\.rs\|src/metrics\.rs\|src/http_api\.rs\|src/ws\.rs" --include="*.md" . | grep -v worktrees` returns nothing.

### [DOC-002] Trigram paragraph
- **Files**: `server/README.md:156-162`.
- **Steps**: rewrite to the opt-in contract: `trgm: true` gates the GIN trigram index; omitted indexes pay nothing; `mode: "trgm"` against a non-declaring index is BAD_REQUEST; pre-flag indexes grandfathered on next push.
- **Verify**: cross-read root README "Search modes" + FEATURE_MATRIX row 30 — no contradiction remains.

### [DOC-003] CHANGELOG entries
- **Files**: `CHANGELOG.md` `[Unreleased]`.
- **Steps**: add entries for: stage-timing gauges + additive SlowQueryRecord fields (Added, `f130391`); ParMeshV1 delivered→queued redelivery revert (Fixed/behavior, `eea4aab`); provider split (internal, one line). Then a version bump per `docs/RELEASING.md` — NOTE: a version-bump push publishes to public registries via the main-push pipeline; that push needs Paul's approval per the standing Git Workflow rule (file an approval card instead of pushing).
- **Verify**: `make checkall` green (no gate leg reads the changelog, but cli-docs-check must not regress); the changelog's own consistency claims (Keep-a-Changelog headers) hold.

### [DOC-004] Release-model alignment (DECISION FIRST)
- **Files**: `README.md` roadmap ~2013, `docs/README.md:12`, `docs/clients.md`, `docs/RELEASING.md` (Swift ordering sentence).
- **Steps**: CONFIRM with Paul: docs → reality (main-push publishing) or reality → docs (tag-gating). Default (docs stale — RELEASING.md already matches the workflow): rewrite the three spots to describe the green-gated main-push pipeline with the tag created by the release job; fix the RELEASING.md Swift sentence to state the remote version resolves BEFORE the tag-creating job runs.
- **Verify**: the three docs + RELEASING.md + release.yml all describe one model.

### [DOC-005] Client-count sweep
- **Files**: `rust-client/README.md`, `python-client/README.md`, `docs/transaction-deadlines.md`, `docs/transaction-field-guards.md`, `docs/atomic-counter.md`, `PRODUCT.md`.
- **Steps**: grep the live tree for `four implementations|five clients|six clients|six implementations|Four client`; update counts to seven and add Kotlin rows to SDK method tables (verify each Kotlin claim against `kotlin-client/src/main/kotlin/dev/parrtdb/` before adding it — e.g. `expectFields` at `dsl/Mutation.kt:35`).
- **Verify**: the grep returns nothing (worktrees excluded); FEATURE_MATRIX still agrees.

### [DOC-006..DOC-011, DOC-014] Medium doc batch
- One agent can take all seven; they are one-file-each edits with the locations already pinned in AUDIT.md's medium section (Go version ×3 files; hot-config count; monitoring gauges paragraph; docs index additions; kotlin quickstart schema; CONTRIBUTING count/prose/tool-list; go-client README quickstart + TOC).
- **Verify**: per-card criteria in AUDIT.md; `make checkall` green (cli-docs-check, env-drift legs unaffected).

### [DOC-015 batch + DOC-018] Low doc batch
- DOC-015/016/017/019/020/022/023 per AUDIT.md's low section; DOC-018 requires the design-authority decision first (default: leave README as-is, record the pending decision on its card).
- **Verify**: per-card criteria.

---

## Verification (Phase 4, all agents done)

1. `make checkall` — the project's done gate (full suite; TRY-2 acceptable for the `merge_test` load canary only, with the failure output recorded from TRY-1).
2. `git status` clean of unexpected files; no AUDIT.md/AUDIT-REMEDIATION-PLAN.md edits leaked into commits beyond what fix commits need.
3. Per-card acceptance criteria checked with evidence; cards closed with `item done`.
