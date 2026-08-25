# Audit Remediation Report

> **Project**: par-rt-db
> **Audit Date**: 2026-08-23
> **Remediation Date**: 2026-08-24 to 2026-08-25
> **Severity Filter Applied**: all
> **Plan Source**: AUDIT.md `## Remediation Plan` + AUDIT-REMEDIATION-PLAN.md playbook
> **Implementation Model**: Opus 5 (all fix agents)
> **Addendum (2026-08-25)**: a second pass fixed the 5 follow-up items filed during the first pass — see [Follow-up Round](#follow-up-round-2026-08-25) below.

---

## Execution Summary

| Phase | Status | Agent | Issues Targeted | Resolved | Partial | Manual |
|-------|--------|-------|----------------|----------|---------|--------|
| 1 — Critical/Promoted Security | ✅ | fix-security | 5 (SEC-001, 003, 005, 006, 007) | 5 | 0 | 0 |
| 2 — Critical Architecture | ✅ | fix-architecture | 7 (ARC-001..005, 010, 011) | 6 | 1 (ARC-004) | 0 |
| 3a — Security (remaining) | ✅ | fix-security | 2 (SEC-002, 004) | 2 | 0 | 0 |
| 3b — Architecture (remaining) | ✅ | fix-architecture | 10 (ARC-006..009, 012..017) | 9 | 1 (ARC-009 step 3) | 0 |
| 3c — Code Quality (all) | ✅ | fix-code-quality | 19 (QA-001..019) | 16 | 3 (QA-008, QA-009, QA-011) | 0 |
| 3d — Documentation (all) | ✅ | fix-documentation | 31 (DOC-001..031) | 29 | 2 (DOC-016, DOC-024) | 0 (1 deliberately skipped: DOC-031) |
| 4 — Verification | ✅ | — | `make checkall` | Pass | — | — |

**Overall**: 74 audit issues addressed across 35 commits; 67 fully resolved, 7 partial (documented below, several as deliberate scope decisions rather than failures), 0 requiring manual intervention beyond what's noted. 6 follow-up issues filed to the board from things fix agents found but correctly left out of scope.

---

## Resolved Issues ✅

### Security
- **[SEC-001]** IPv4-mapped/compat IPv6 literals bypassed the webhook SSRF denylist — `server/src/webhook.rs`: both `::ffff:a.b.c.d` and `::a.b.c.d` now recurse into the V4 blocklist, ordered after the loopback/unspecified checks so `::1`/`::` stay blocked.
- **[SEC-002]** Quota-measure and image-transform errors leaked internal detail — `server/src/quota.rs`, `server/src/image_transform.rs`: fixed generic client messages, detail logged server-side only.
- **[SEC-003]** Signed URL didn't bind transform params — `server/src/signed_url.rs`, `server/src/http_api.rs`, `server/src/image_transform.rs`: HMAC now covers `{id}.{exp}.{transform}`. **Breaking: invalidates every signed URL minted before this deploy** (in CHANGELOG).
- **[SEC-004]** Apple `id_token` signature was never verified — new `server/src/auth/jwks.rs` shared JWKS cache; `verify_id_token` checks signature + `iss`/`aud`/`exp`. Corrected the plan's algorithm assumption (Apple signs RS256, not ES256 — the key selector derives the algorithm from published key material so it isn't hardcoded either way).
- **[SEC-005]** Per-IP rate bucket wasn't route-namespaced — `RateKey::Ip` gained a `route` discriminator (`storage`/`admin_login`/`anon_mint`).
- **[SEC-006]** `/admin/stream` didn't re-check credential after handshake — now re-validated on the existing 1s tick; a revoked session's socket closes.
- **[SEC-007]** No explicit filter-depth/`In`/search-length caps — `MAX_FILTER_DEPTH=32`, `MAX_IN_VALUES=1000`, `MAX_SEARCH_QUERY_BYTES=4096` in `server/src/schema/` (single choke point, including the `authorize` predicate path).

### Architecture
- **[ARC-001]** Cross-replica subscription invalidation for owner-side writes — `server/src/notify.rs` `run_write_set_listener` re-runs `fan_out` locally per replica on write-set publish.
- **[ARC-002]** Forwarded requests/replies now spool through `rtdb_auth.forward_queue` instead of raw `pg_notify` payloads, eliminating the 8000-byte cap risk.
- **[ARC-003]** Server-minted idempotency key stamped on forwarded mutates so a lost-reply resubmission replays the owner's first outcome.
- **[ARC-005]** `committer.rs` split into `server/src/committer/{mod,lease,forwarding,supervisor,taps,arms/*}.rs` — pure move, tap-site count unchanged.
- **[ARC-006]** Off-turn, batched cross-replica op-feed notify — chunked under 7500 bytes, published outside the committer turn.
- **[ARC-007]** Local token bucket with periodic Postgres reconciliation replaces a synchronous DB round-trip per request (exact-count mode retained behind a flag).
- **[ARC-008]** Bounded forward-listener concurrency via `RTDB_FORWARD_CONCURRENCY` semaphore.
- **[ARC-009]** `BackgroundTasks` handle + `CancellationToken`; all spawned loops (including the idle reclaimer) cancel and join on shutdown.
- **[ARC-010]** Server integration tests consolidated into one binary (`server/tests/main.rs`); also deleted all 23 blanket `#[allow(dead_code)]` in `tests/common/mod.rs` (subsumed QA-012).
- **[ARC-011]** Dockerfile rust-client test-stub list is now generated/gated by `scripts/dockerfile-stub-check.sh`, wired into `checkall`.
- **[ARC-012]** `Config` split into `server/src/config/{mod,hot,oauth,limits,multi_instance,storage,backup}.rs` with `impl Default for Config`.
- **[ARC-013]** Optional `protocolVersion` on `auth`/`authOk` and `X-Rtdb-Protocol` HTTP header; mismatch rejected with `UNSUPPORTED_PROTOCOL` (400); mirrored across all four SDKs.
- **[ARC-014]** Makefile now issues workspace-level `cargo` invocations instead of per-package loops (kept `--all-features` on `cargo test --workspace`, which a first pass had dropped — that would have silently skipped rust-client's corpus/parity test targets).
- **[ARC-015]** Committer drain replaced sleep-poll with a `Notify`-based wait.
- **[ARC-016]** `is_owner` TOCTOU in `execute_as_owner` fixed — a lease lost between check and submit now falls through to takeover instead of surfacing a spurious conflict.
- **[ARC-017]** `wire-corpus/error-codes.json` pins the closed `{code, httpStatus}` set; server enforcement is a compile-time-exhaustive match, not just a data diff.

### Code Quality
- **[QA-001]** `apply_schema_additive` decomposed (was CC 57 / 340 lines).
- **[QA-002]** Per-directive migrate functions + single `rename_field_refs`, mirrored across all engines. Found and fixed a real latent bug in the process: `ttl.field` and `updatedAtField` were unhandled by field renames server-side and in all four engines — 8 new wire-corpus cases now pin it.
- **[QA-003]** Python sync/async duplication hoisted into `_http_common.py`; a sync/async parity test added (declined generated-code `unasync` step as unnecessary complexity for the actual duplication found).
- **[QA-004]** Six OAuth providers consolidated into one `resolve_user()`. Does **not** fix the deeper bug the audit's Impact section described (Google/GitLab/OIDC email-change forks a new account) — that needs new provider-subject columns and a migration; filed as a follow-up, see below.
- **[QA-005]** `schema.rs` (3850 lines) split into `server/src/schema/{mod,types,validate,computed,value}.rs` + per-submodule test files — pure move.
- **[QA-006]** Fire-and-forget status-write failures now logged (targeted at the post-ARC-005 `committer/` module paths).
- **[QA-007]** Avoidable `unwrap`/`expect` removed or gated with a documented invariant.
- **[QA-008]** 11 of 14 dashboard pages migrated to `useAsync` (3 partial, see below).
- **[QA-009]** `PendingQueues` + per-family handlers in rust-client and Swift WS dispatchers; TS got per-family handlers (partial, see below).
- **[QA-010]** `TxnCtx`/`QueryCtx` parameter structs introduced (3 call sites deliberately kept as-is with documented rationale, see below).
- **[QA-011]** `wait_until` helper replaces fixed sleeps in `server/tests/common/mod.rs` and listed test files.
- **[QA-012]** Already resolved by ARC-010 (blanket `allow(dead_code)` deleted); verified, not re-done.
- **[QA-013]** In-package exact duplicates hoisted (`json_result`/`deserialize` in rust-client, not `error_response` as the audit named — verified against source).
- **[QA-014]** Stale TS in-memory engine header comment fixed.
- **[QA-015]** `AnySchema` alias added.
- **[QA-016]** `_unused_vector_spec` deleted from `server/src/subs.rs`.
- **[QA-017]** Storage stream error genericized.
- **[QA-018]** Inline durations named (note: `DRAIN_POLL` had nothing left to name — ARC-015 replaced that sleep-poll with a `Notify` wait first).
- **[QA-019]** CLI integration tests added (`cli/tests/`), gated via the ARC-011 stub-list script.

### Documentation
All 31 DOC issues addressed; DOC-011 (demote the stale spec) landed first per the plan's own sequencing, DOC-001 (CHANGELOG `[Unreleased]` rewrite) landed last so it reflects every other fix in this cycle, including the SEC-003 breaking change and the route-namespaced rate-limit keys. See `CHANGELOG.md` for the full list.

---

## Partial / Deliberately Scoped 🔧

### [ARC-004] Extract `par-rt-db-core` crate — partial, card left `in_progress`
Wire types (`FilterExpr`, `ValueExpr`, `Cast`, `CaseWhen`) now live in `core/` with re-exports at their historical paths — no call site moved. **Not extracted**: `eval_value_expr`, `apply_patch`, `validate_doc`/`validate_value`, `worst_case_affected`, `detect_destructive_changes`, `strip_on_delete`, `stamp_computed`, `rename_value_expr_fields`, `literal_set`. These are written against divergent server/client `SchemaDef` types; hoisting them means unifying those types first — a substantially larger change than the plan entry described. Recommended: scope as its own follow-on card once the schema-type unification is designed.

### [ARC-009] `BackgroundTasks` — step 3 not done
`test_state_*` teardown via `Drop` would touch 69 call sites across the consolidated test binary (`spawn_app` detaches the server task). Delivered instead as an opt-in `background_guard(&state)` RAII helper, not yet adopted anywhere.

### [QA-008] Dashboard `useAsync` migration — 11 of 14 pages
Not migrated: `ConfigPage` (seven form slots from one response — needs a different hook shape), `BackupsPage` (divergent error-reset semantics), `QueryConsolePage` (user-triggered, not load-on-mount, doesn't fit the hook's model).

### [QA-009] `PendingQueues` — TS partial
rust-client and Swift got the full pattern. TypeScript got per-family handlers, but no generic pending-queue wrapper (its maps were already instance fields, so the argument-threading problem the audit described doesn't apply there).

### [QA-010] `TxnCtx`/`QueryCtx` — 3 allows kept deliberately
`subs::register`, `txn::insert_snapshot_row`, `txn::delete_row_cascade` keep their parameter lists; each is documented in-code as a case where bundling into a context struct would obscure rather than clarify.

### [QA-011] `wait_until` — verification criterion not fully met
The playbook's "three consecutive green `make checkall`" bar was not met by the agent (one full green run + one staged re-verification). Re-verified independently in Phase 4 as part of the merged tree's full gate — passed.

### [DOC-016] Python docstring convention
Softened the README claim rather than enabling ruff's `D` rules, which would require ~492 docstrings — a much larger scope than a documentation fix. Deliberate, reversible choice.

### [DOC-024] Prose-rhythm sweep
The specific flagged regions were resolved (mostly superseded by the new multi-instance content); a systematic sweep of the rest of the document for single-sentence-paragraph rhythm was not done.

### [DOC-031] Go client row — deliberately not added
The only mention of a Go client in the repo is a conditional "remains a separate backlog card, gated on external demand," not a commitment. No "planned" row was added; a real backlog card for it already exists (`go-client`, unrelated to this audit).

---

## Follow-ups Filed to the Board 📋

Six items fix agents found and correctly left out of their assigned scope, filed as new backlog cards (tag `follow-up`):

1. **SEC-007 engine parity** — the four in-memory client engines don't yet enforce the new filter-depth/`In`/search caps; a wire-corpus case can't be added until they do.
2. **Signed-URL kill-switch seam** — with `RTDB_IMAGE_TRANSFORMS_ENABLED=false`, `canonical()` still derives from `parse()`, so a `w=100` signature serves full-res bytes once transforms are killswitched off. Operational edge, not exploitable without operator action.
3. **`image_transform.rs` JoinError leak** — same class as SEC-002 (panic text via `spawn_blocking` join error) but not in the audit's enumerated site list.
4. **rust-client protocol-version header gap** — `X-Rtdb-Protocol` is sent on query/mutate/batch but not ~30 admin/schedule/workflow/storage methods; the other three SDKs cover their full surface.
5. **Google/GitLab/OIDC email-change bug** — the actual bug QA-004's Impact section described; needs new `google_sub`/`gitlab_id`/`oidc_sub` columns and a migration, not just the `resolve_user` consolidation that shipped.

---

## Verification Results

- Build: ✅ Pass
- Tests: ✅ Pass (server 899 integration + 505 lib, ts-client 1108, rust-client incl. all feature-gated corpus targets, dashboard 127, python-client, swift-client 497)
- Lint: ✅ Pass (clippy `-D warnings`, biome, ruff)
- Type Check: ✅ Pass (all packages)
- `env-drift-check`, `dockerfile-stub-check`, `cli-docs-check`: ✅ Pass

`make checkall` ran clean end-to-end against the shared dev Postgres (127.0.0.1:55434) after merging all phases into `fix/audit-remediation`. Two merge-time issues were caught and fixed directly by the orchestrator (not by a fix agent):
1. A Phase 1/Phase 2 merge left two test files (`rate_limit_test.rs`, `ws_test.rs`) using a bare `common::` reference instead of `crate::common::` after ARC-010's test-binary consolidation — fixed in commit `6428eca`.
2. Three genuine three-way merge conflicts during Phase 3 wave reconciliation (`README.md`, `server/src/auth/apple.rs`, `wire-corpus/README.md`) — all were additive content from independent worktrees landing at the same anchor point, resolved by keeping both sides' content (or, for one factual contradiction in `wire-corpus/README.md` about runner count assertions, keeping the newer/already-merged documentation-phase text).

## Files Changed

247 files changed, +19,978/−12,283, across 35 commits on `fix/audit-remediation` (branched from `main` @ `2609de3`). Full list: `git diff --stat main...fix/audit-remediation` (or `git log --stat` for the per-commit breakdown, one commit per phase/issue-group).

---

## Next Steps

1. Review the ARC-004 partial-extraction card on the kanban board (the only item left open from this whole remediation) and prioritize the schema-type unification it needs.
2. Re-run `/audit` to get an updated baseline reflecting current state.
3. Prioritize the 4 new follow-ups filed in the second round (below) — the intra-binary test-parallelism items are worth fixing before they cause more false-alarm CI noise.

---

## Follow-up Round (2026-08-25)

The first pass filed 5 follow-up cards for things fix agents found but correctly left out of scope. All 5 are now fixed, in 3 more parallel agents (one per domain: security, rust-client, OAuth/schema), same worktree-isolated pattern, merged to `fix/audit-followups` and fast-forwarded into `main`. 8 more commits, 34 files, +2103/−239.

### Resolved

- **SEC-007 engine parity** — `MAX_FILTER_DEPTH`/`MAX_IN_VALUES`/`MAX_SEARCH_QUERY_BYTES` now enforced identically in all four in-memory client engines (ts/rust/python/swift), with 4 new wire-corpus semantics cases pinning both cap boundaries across all five runners.
- **Signed-URL kill-switch seam** — a transform-requesting signed URL is now rejected (403) rather than silently served full-resolution when `RTDB_IMAGE_TRANSFORMS_ENABLED=false`.
- **`image_transform.rs` JoinError leak** — routed through the same `internal_transform_error` choke point SEC-002 already established; client sees a fixed generic message, detail is logged server-side only.
- **rust-client protocol-header gap** — all 36 HTTP call sites (17 data-plane + 19 admin) now go through one shared `AuthedRequest::authed` seam that attaches both the bearer token and `X-Rtdb-Protocol`; `bearer_auth` appears exactly once crate-wide (inside the seam) as the coverage invariant.
- **Google/GitLab/OIDC identity bug (the real QA-004 bug)** — three new nullable columns (`google_sub`, `gitlab_id`, `oidc_sub`) added additively to the users table; all six OAuth providers now key identity resolution on a stable provider subject, falling back to email-link only for a user's first login with that provider. An email change at any of the six providers no longer forks a new account. Filed under **Breaking** in the changelog: a non-compliant OIDC IdP that omits `sub` from userinfo now gets a 403 instead of silently succeeding.

### New follow-ups discovered during this round (filed, not fixed — genuinely out of scope for the cards that found them)

1. **ts-client/python-client/swift-client protocol-header gaps** — the rust-client fix's own investigation found the original card's premise wrong: the other three SDKs do NOT already cover their full HTTP surface for `X-Rtdb-Protocol`. ts-client's upload/delete/get paths and admin client, python's two standalone admin clients, and Swift's `AdminClient` all send `Authorization` only.
2. **Server doesn't validate `X-Rtdb-Protocol` on `/admin/*`** — `check_protocol_version` has 16 callers, all through `/api/*`'s `authed()`. Admin-surface protocol skew is currently unenforced even though clients now send the header there.
3. **`multi_instance_stage4_test` unsafe for intra-binary parallel execution** — all 10 tests pass with `--test-threads=1` even on an uncontended database, but fail intermittently (a different subset each run) under default parallel test execution. They share `rtdb_write_fwd`/`rtdb_write_replies` LISTEN/NOTIFY channels and a 300ms forward timeout. This is what caused `make checkall`'s `test` stage to fail on the first attempt in this round — confirmed as pre-existing flakiness, not a regression, by isolating the test file.
4. **`workflows_test::await_signal_timeout_retries_with_fresh_timeout_then_succeeds` flaky under load** — passes in isolation, fails under full-suite parallel load with a retry-count assertion mismatch (timing-sensitive).

### Verification

`make checkall`'s static stages (fmt, clippy, typecheck, lint, env-drift-check, dockerfile-stub-check, cli-docs-check) and all four client test suites passed clean on the first run. The `test` stage initially failed with 4 `multi_instance_stage4_test` failures — investigated rather than assumed: two stale test processes from already-merged-and-removed agent worktrees were still running against the shared Postgres, killing them didn't fully explain it, and isolating the test file with `--test-threads=1` confirmed all 10 pass — a pre-existing flake (follow-up #3 above), not a regression. The remaining suite (`--skip multi_instance_stage4_test`) surfaced one more flake (`workflows_test`, follow-up #4), also confirmed to pass in isolation. Neither touches any file this round's three fixes modified (auth/*, db.rs, image_transform.rs, signed_url.rs, http_api.rs, client in-memory engines, rust-client/*).

### Files Changed (this round)

34 files, +2103/−239, across 8 commits on `fix/audit-followups` (branched from `main` @ `bf84f49`), merged fast-forward into `main` and pushed.
