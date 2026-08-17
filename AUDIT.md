# Project Audit Report

> **Project**: par-rt-db
> **Date**: 2026-08-16
> **Stack**: Rust (axum/tokio + Postgres 17) server · TypeScript client (bun) · Rust client · Python client (uv) · React/Vite dashboard · Rust CLI
> **Audited by**: Claude Code Audit System (`/fable-audit` — four Fable 5 domain agents at HEAD 42d08a3)

> **ID convention for this run**: issues are numbered in the **2xx range** (`ARC-201`, `SEC-201`, …) because the repo's source comments carry a historical remediation ledger (`SEC-001`…`SEC-132`, `ARC-001`…`ARC-126`, `QA-00x`, `DOC-0xx`) from prior cycles; low numbers would falsely alias closed findings. Board cards for this cycle are tagged `audit-2026-08-16`.

---

## Executive Summary

par-rt-db is in genuinely good health: the load-bearing invariants (single serialized committer per database, seven `publish_taps` tap arms, four-way wire-contract mirroring with an executable golden-vector gate) are all intact, security posture is **strong** (zero Critical/High security findings — SQL construction, credential storage, SSRF defense, and CSRF handling are exemplary), and hygiene is near-perfect (zero TODO/FIXME markers, every lint suppression justified in writing). The single Critical finding is documentation, not code: the disaster-recovery cutover step in `deploy/README.md` edits an env var that `docker-compose.yml` never reads, so the documented restore procedure silently no-ops. The dominant structural liability is the triplicated in-memory query engine — the repo's four most complex functions (CC 105–119), which are also its top three churn hotspots — where every server semantic must be re-derived three times by hand. Estimated effort for the Critical + High set: roughly one focused day for the five documentation fixes, plus a multi-session mechanical decomposition for the engine triplication.

### Issue Count by Severity

| Severity | Architecture | Security | Code Quality | Documentation | Total |
|----------|:-----------:|:--------:|:------------:|:-------------:|:-----:|
| 🔴 Critical | 0 | 0 | 0 | 1 | **1** |
| 🟠 High     | 1 | 0 | 0 | 4 | **5** |
| 🟡 Medium   | 5 | 2 | 3 | 7 | **17** |
| 🔵 Low      | 4 | 4 | 3 | 7 | **18** |
| **Total**   | **10** | **6** | **6** | **19** | **41** |

Cross-domain dedup performed: the in-memory-engine findings (Architecture High + both Code Quality Highs) are merged as **ARC-201**; the dashboard god-component finding (Architecture Low + Code Quality Medium) is merged as **QA-201**; the `Config::from_env` finding (Architecture Medium + Code Quality Medium) is merged as **ARC-205**; the `query.rs` decomposition finding (both domains Medium) is merged as **ARC-203**.

---

## 🔴 Critical Issues (Resolve Immediately)

### [DOC-201] Restore-cutover runbook edits a variable compose never reads — the DR cutover silently no-ops
- **Area**: Documentation
- **Location**: `deploy/README.md` (cutover step "Point `RTDB_DATABASE_URL` at `rtdb_restored_<stamp>` in `.env`" and the matching Troubleshooting bullet)
- **Description**: `deploy/docker-compose.yml` hardcodes `RTDB_DATABASE_URL: postgres://rtdb:${POSTGRES_PASSWORD}@postgres:5432/rtdb` in its `environment:` block and no compose file uses `env_file:`. Editing `.env` and restarting leaves the server on the old database with no error. The README's own env-drift section correctly describes `environment:` as an explicit allowlist — the runbook contradicts it.
- **Impact**: An operator performing disaster recovery follows the runbook, believes the cutover succeeded, and keeps serving the pre-restore database — discovered exactly when under pressure.
- **Remedy**: Rewrite the cutover step to edit `docker-compose.yml`'s `RTDB_DATABASE_URL` line (or introduce genuine `.env` indirection in compose and keep the runbook as written). Fix the Troubleshooting bullet. Add a verification step (`/healthz` + a query against a known post-restore row).

---

## 🟠 High Priority Issues

### [ARC-201] Triplicated in-memory query engines: the repo's four most complex functions are its top three churn hotspots
- **Area**: Architecture + Code Quality (merged: arch High, QA-101, QA-102)
- **Location**: `rust-client/src/in_memory/query.rs:39` (`run_query`, CC 119), `python-client/src/par_rt_db/in_memory.py:1051` (`run_query`, CC 112; file is 4,120 lines), `ts-client/src/in_memory.ts:2723` (`executeQuery`, CC 105; file is 3,865 lines); plus the migration-directive interpreters `rust-client/src/in_memory/migrate.rs:13` (CC 56) and `ts-client/src/in_memory.ts:1516` (CC 42)
- **Description**: Each client ships a full reimplementation of the server's query/txn/migrate semantics as a test harness, concentrated in one 100+-branch method per client. par-mem ranks these #1–#3 hotspots (scores 2,499–3,248; 21–29 recent changes). The rust-client is at least module-split (`mod.rs`/`query.rs`/`migrate.rs`/`validate.rs`); TS and Python are single-file monoliths.
- **Impact**: Every query-DSL feature lands three edits deep inside the most frequently changed, most complex code in the repo — the highest-probability place to introduce mirror drift. The golden-vector corpus catches wire drift after the fact but nothing enforces behavioral parity structurally.
- **Remedy**: (1) Split `in_memory.ts` and `in_memory.py` to mirror the rust-client's module decomposition (query / migrate / validate / store). (2) Decompose each engine per terminal (the pattern already exists: `executeGetTerminal`, `executeSearchTerminal`) so each becomes a thin dispatcher over per-terminal executors, mirrored identically across the three clients. (3) One function per migration-directive kind. Gate every step on the golden-vector suite. (A follow-on enhancement, ENH-023, extends the corpus to behavioral-semantics fixtures.)

### [DOC-202] CLI README claims a `--url` default that does not exist and omits 7 of 16 commands
- **Area**: Documentation
- **Location**: `cli/README.md`
- **Description**: README says `--url` defaults to `http://127.0.0.1:8300`; `cli/src/main.rs` declares it required with only an `RTDB_URL` env fallback. Undocumented commands: `sessions list|revoke`, `merge-users`, `clone-db`, `explain`, `slow-queries`, `mint-token`, `revoke-token`. The four env vars (`RTDB_URL`, `RTDB_DB`, `RTDB_TOKEN`, `RTDB_ADMIN_KEY`) are documented nowhere.
- **Impact**: The first copy-pasted command fails with a clap error; nearly half the CLI surface is invisible to operators.
- **Remedy**: Correct the `--url` claim, add a Configuration section for the four env vars, document all sixteen commands. (Enhancement ENH-025 proposes generating this section from `--help` output to prevent recurrence.)

### [DOC-203] CONTRIBUTING.md's op-feed tap invariant list is stale — five arms documented, seven exist
- **Area**: Documentation
- **Location**: `CONTRIBUTING.md` ("Op-feed tap" invariant bullet)
- **Description**: Lists four tap sites plus `handle_restore_schema`; the code has seven `publish_taps` call sites (verified in `server/src/committer.rs`) — CLAUDE.md/ARCHITECTURE.md correctly add `handle_merge_users` and `handle_workflow_advance`.
- **Impact**: This is the invariant checklist a contributor uses when adding a durable-write path; an incomplete list undermines the exact guarantee it protects.
- **Remedy**: Update to the seven-arm list, or replace the enumeration with a pointer to the single canonical list in `docs/ARCHITECTURE.md`.

### [DOC-204] deploy/README.md has no real rollback procedure and no env-var reference
- **Area**: Documentation
- **Location**: `deploy/README.md`
- **Description**: "Rollback" is only `docker compose down` (stops the stack; `make deploy` rsyncs and builds in place with no image tags, so reverting means re-rsyncing an older commit — documented nowhere). Secrets section covers ~5 of ~83 `RTDB_*` vars and never links `.env.example`. Missing operator surfaces: slow-query log (`RTDB_SLOW_QUERY_*` incl. the `_LOG_PARAMS` privacy tradeoff), anonymous auth (`RTDB_AUTH_ANONYMOUS_ENABLED` + per-db SEC-103 toggle), `RTDB_INSTANCE_ID` in the multi-instance section, any link to `docs/OAUTH_SETUP.md`. ToC omits the two newest sections (Topology, Tracing). Also: the "standing canary at `RTDB_SUBS_VERIFY_SKIP_EVERY=200`" claim describes the live host's `.env` and is unverifiable from the checkout — rephrase as a recommendation.
- **Impact**: An operator cannot revert a bad deploy from the runbook, and security-relevant knobs are undiscoverable from the ops doc.
- **Remedy**: Add a real rollback procedure (checkout prior commit → rsync → rebuild → verify), link `.env.example` as the env reference, add pointers for slow-query log / anonymous auth / `RTDB_INSTANCE_ID` / OAUTH_SETUP.md, refresh the ToC.

### [DOC-205] ts-client README documents APIs that don't compile or don't exist
- **Area**: Documentation
- **Location**: `ts-client/README.md`
- **Description**: (1) `.ttl({ field: "expiresAt" })` — actual signature is `ttl(field: string, defaultDurationMs?: number)` (`src/schema.ts`). (2) `revokeSessionsForUser(user)` does not exist — the method is `revokeUserSessions(userId)` (`src/admin.ts`). (3) The workflows section annotates `http.startWorkflow(spec)` as returning `WorkflowInfo`; the HTTP client returns `{ id: string }` (only the reactive client returns `WorkflowInfo`), and the snippet references unbound variables. Also omits `transformUrl`/`batchQuery` from listings.
- **Impact**: SDK consumers copy examples that fail to compile or call nonexistent methods.
- **Remedy**: Fix the three claims to match `src/schema.ts`, `src/admin.ts`, `src/http.ts`; make the workflow snippet self-contained; add the missing methods.

---

## 🟡 Medium Priority Issues

### Architecture

### [ARC-202] Wire/DSL types entangled with SQL compilation; `query.rs` ↔ `txn.rs` circular dependency
- **Location**: `server/src/query.rs:22` (imports `txn::{EqBind, eq_bind_for, eq_binds, row_visible_to}`), `server/src/txn.rs:30` (imports `query::{FilterExpr, filter_matches}`), `server/src/protocol.rs:11-13`
- **Description**: The wire-contract types (`Query`, `FilterExpr`, `Transaction`, `Step`) live inside the 4,038- and 2,434-line executor modules alongside SQL compilation; `protocol.rs` imports its core payload types from the executors, and the executors mutually depend on each other.
- **Impact**: The "byte-identical four-way mirror" surface is diffuse — a reviewer checking serde tags must read across four files, which is how `skip_serializing_if` drift slipped through before.
- **Remedy**: Extract a pure `wire`/`dsl` module (types + serde derives + shared index-value typing, zero SQL) that `protocol.rs`, `query.rs`, and `txn.rs` all depend on. Serde output must stay byte-identical — the wire-corpus/golden-vector tests in all four packages are the gate.

### [ARC-203] `server/src/query.rs` is a 4,038-line multi-responsibility module (merged QA-104)
- **Location**: `server/src/query.rs` (`compile_query` CC 47 at :804, `compile_query_window` CC 36 at :1174, 3 of the repo's 12 `too_many_arguments` allows)
- **Description**: One file holds DSL types, filter compilation, eight terminal compilers, FTS/vector/hybrid search compilation, point reads, and per-row auth predicate rendering. The `SearchCtx` parameter-object pattern shows the fix is already known.
- **Remedy**: Convert to a `query/` directory module (`terminals.rs`, `filter.rs`, `search.rs`, `row_auth.rs`), public surface unchanged. Sequence after ARC-202.

### [ARC-204] Constructor parameter explosion in the committer and friends
- **Location**: `server/src/committer.rs:176` (`Committers::new` — 15 positional parameters), plus ~10 more `#[allow(clippy::too_many_arguments)]` sites (`audit.rs:82`, `webhook.rs:691`, `subs.rs:1028`, `migrate.rs` ×2, `txn.rs` ×2, `auth/provider.rs:147`, `query.rs` ×3)
- **Description**: Eight of the fifteen parameters are plain config scalars threaded individually from `Config`; every feature widens the signature.
- **Impact**: Positional-transposition risk (adjacent `u64`s/`bool`s); merge-conflict magnet.
- **Remedy**: Introduce a `CommitterConfig` struct built from `Config` in one place; apply the ctx-struct pattern opportunistically at the other allow-sites when next touched.

### [ARC-205] `Config::from_env` is a CC-48 monolith and the single highest-churn function in the repo (merged QA-103)
- **Location**: `server/src/config.rs:376` (file is 1,470 lines; 39 changes in the recent window)
- **Description**: All ~40 `RTDB_*` vars parse in one function; subsystems with their own config structs (`TransformConfig`, `PresenceConfig`) still hydrate from the flat `Config`; each OAuth provider adds another cluster of `std::env::var` lines.
- **Remedy**: Per-subsystem `from_env` constructors (e.g. `GithubOAuthConfig::from_env()`) composed by the top-level function. Purely mechanical. Keep the env-drift-check contract unchanged. Sequence after SEC-201/SEC-203 (same file).

### [ARC-206] `cli/src/main.rs` is a 1,318-line single-file binary
- **Location**: `cli/src/main.rs`
- **Description**: Arg definitions, `Command` enum, dispatch, output formatting, and tests all live in `main.rs`.
- **Remedy**: Split into `args.rs` / `commands/*.rs` / `output.rs`; no behavior change. Sequence after SEC-204 (same file).

### Security

### [SEC-201] Unconditional trust of `CF-Connecting-IP` / `X-Forwarded-For` for rate-limit identity
- **CWE/OWASP**: CWE-348 / OWASP A05, A07
- **Location**: `server/src/http_api.rs:869-884` (`client_ip_key`), consumed by `server/src/admin/login.rs:46`, `server/src/auth/provider.rs:829` (anonymous mint), and the public storage rate limit at `http_api.rs:813`; same pattern in `request_is_secure` (`server/src/auth/cookie.rs:218`)
- **Description**: `client_ip_key` prefers `CF-Connecting-IP`, then rightmost `X-Forwarded-For`, from any request — with no check that the peer is a trusted proxy. Any client that can reach the origin directly can rotate its rate-limit key per request.
- **Impact**: Bypasses the SEC-109 admin-login brute-force limit, the SEC-103 anonymous-mint limit, and the public-storage limit whenever the listener is directly reachable. The shipped compose binds `127.0.0.1:8300`, so the deployed instance is mitigated — but this is a self-hosted product; any operator exposing the port directly silently loses all per-IP limiting.
- **Remedy**: Add an `RTDB_TRUSTED_PROXY` (bool or CIDR list) config; only consult forwarding headers when the peer is in the trusted set, otherwise use the socket peer address. Apply the same gate to `request_is_secure`'s `X-Forwarded-Proto` read for consistency.

### [SEC-202] RUSTSEC-2023-0071 — `rsa` 0.9.10 Marvin timing side-channel (no fix released)
- **CWE/OWASP**: CWE-208 / OWASP A06
- **Location**: `Cargo.lock` — `rsa v0.9.10` ← `jsonwebtoken v10.4.0` ← `rtdb-server`
- **Description**: `cargo audit` flags the Marvin attack (private-key recovery via decryption/signing timing). The server uses `jsonwebtoken` for Microsoft/Apple id_token **verification** (RSA public-key ops); Apple client-secret signing is ES256 via ring — the vulnerable private-key path is not exercised. No fixed upstream release exists.
- **Remedy**: Record the advisory as accepted in an `audit.toml` ignore with a justification comment (verification-only usage); re-evaluate when `rsa` 0.10 lands in `jsonwebtoken`.

### Code Quality

### [QA-201] Dashboard god components: `WebhooksPage` (CC 31, ~20 `useState` hooks) and `SchemaHistoryPage` (CC 30) (merged arch Low)
- **Location**: `dashboard/src/pages/WebhooksPage.tsx:60`, `dashboard/src/pages/SchemaHistoryPage.tsx:47`
- **Description**: `WebhooksPage` holds list, create-form, per-row action, inline-edit, and deliveries-drill-down state — five concerns in one component. Both pages are also among those without component tests.
- **Remedy**: Extract `WebhookCreateForm`, `WebhookEditPanel`, `DeliveriesPanel` (and the SchemaHistoryPage equivalents) as child components with their own state.

### [QA-202] Dead utility hook: `useAsync` has zero consumers
- **Location**: `dashboard/src/lib/useAsync.ts:28`
- **Description**: A polished, documented load-on-mount hook whose docstring says it exists to replace the pages' inline fetch pattern — no importer exists (confirmed by par-mem `find_dead_code` and grep). It even carries one of the seven `biome-ignore`s.
- **Remedy**: Either adopt it in the simple list pages or delete it. Don't leave it in limbo.

### [QA-203] ~10 stale `# type: ignore` comments in python-client, self-acknowledged as deferred
- **Location**: `python-client/src/par_rt_db/ws_client.py:629-665` (nine `# type: ignore[return-value]` on `_sched_op`/`_wf_op` call sites); `python-client/pyproject.toml` (pyright comment deferring `reportUnnecessaryTypeIgnoreComment`)
- **Remedy**: Give the two helpers `@overload`s or a generic parameter keyed by op name; enable `reportUnnecessaryTypeIgnoreComment`; delete the stale ignores in one pass.

### Documentation

### [DOC-206] SPEC_STATUS.md is five weeks stale: the seven newest specs are missing
- **Location**: `docs/superpowers/SPEC_STATUS.md`
- **Description**: Lists 33 specs; the directory holds 40 (missing: anon-merge, step-schedule, phrase-search-snippets, trgm-search, workflows, cascade-delete-soft-delete, field-defaults — all shipped per FEATURE_MATRIX). Legend still says the gap matrix runs "#1–#26" (now #35); its dashboard row contradicts FEATURE_MATRIX row 18.
- **Remedy**: Add the seven rows, fix the "#1–#26" references, reconcile the dashboard-row status after DOC-207 lands.

### [DOC-207] FEATURE_MATRIX internal staleness: row #11 contradicts row #31 and the code; row #18 prefix contradicts its own body
- **Location**: `FEATURE_MATRIX.md` rows 11 and 18
- **Description**: Row #11 says the search terminal matches `plainto_tsquery`; the code uses `websearch_to_tsquery` (verified in `server/src/query.rs`) as row #31 and the root README correctly state. Row #18 opens "In progress —" though its body and status column say shipped.
- **Remedy**: Update row 11's tsquery reference (pointer to row 31); change row 18's prefix to "Implemented".

### [DOC-208] `RTDB_SUBS_VERIFY_SKIP_EVERY` default documented as 0/off in two docs; actual default is 1000/on
- **Location**: `deploy/README.md` (invalidation-canary section), `CHANGELOG.md` (fine-grained invalidation entry)
- **Description**: `server/src/config.rs` sets `DEFAULT_SUBS_VERIFY_SKIP_EVERY = 1000` (ships on); `.env.example` and compose agree; the two docs say "default 0 = off".
- **Remedy**: Fix both to "ships on at 1000; 0 disables"; in the CHANGELOG add a note that the default changed rather than silently rewriting history.

### [DOC-209] rust-client README: stale module path and an example teaching a deprecated API
- **Location**: `rust-client/README.md`
- **Description**: References `src/in_memory.rs` (now `src/in_memory/`); migration example calls `RtDbHttpClient::migrate_schema`, which is `#[deprecated]` — contradicting the README's own ARC-121 text. Missing from listings: `transform_url`, `upload_stream`, `batch_query`.
- **Remedy**: Fix the path; rewrite the example via `db.admin_client().migrate_schema(...)`; add the three methods.

### [DOC-210] server/README.md: misleading `make` cwd, incomplete layout table, stale test list, no config reference
- **Location**: `server/README.md`
- **Description**: Develop block runs root make targets as if from `server/`; Layout table omits `db.rs`, `notify.rs`, `privacy.rs`, `static/`; `GET /privacy`, `GET /metrics`, `POST /api/query-batch` undocumented here; "integration test binaries" names 8 of 46; ~15 `RTDB_*` knobs name-dropped with no config section.
- **Remedy**: Prefix make commands with "from the repo root", add the missing modules/routes, replace the test enumeration with a pattern description, link the root Configuration section.

### [DOC-211] Published-SDK doc-comment gaps: rust-client `http.rs` and ts-client authoring API
- **Location**: `rust-client/src/http.rs`, `rust-client/src/lib.rs`, `ts-client/src/schema.ts`, `ts-client/src/react.tsx`, `ts-client/src/client.ts`
- **Description**: rust-client is bimodal — `admin.rs`/`ws.rs` at 100%, `http.rs` (the primary surface) ~38% undocumented including `RtDbHttpClient` itself; no `#![warn(missing_docs)]`. ts-client: `defineTable`/`defineSchema`/`toInt64`/`fromInt64`, the React hooks, and the storage methods have zero JSDoc.
- **Remedy**: Add `#![warn(missing_docs)]` to `rust-client/src/lib.rs` and document the gaps; add JSDoc to the ts-client exports.

### [DOC-212] python-client README: stale version pins and an unbound-variable snippet
- **Location**: `python-client/README.md`
- **Description**: Claims `pydantic>=2.7` (actual `>=2.13.4,<3`) and `httpx>=0.27` (actual `>=0.28.1`); the workflows snippet calls `db.start_workflow(...)` with `db` never bound; the style guide explicitly discourages duplicating manifest versions in prose.
- **Remedy**: Replace inline pins with "see `pyproject.toml`"; fix the snippet binding.

---

## 🔵 Low Priority / Improvements

### Architecture
- **[ARC-207] `AppState` grouping applied inconsistently** — `server/src/lib.rs:126-161`: `Realtime`/`Runtime`/`Auth` substructs exist, but `rate_limiter`, `backup_running`, `image`, `quotas`, `signed_url_key`, `instance_id` remain loose. Fold on next touch; not worth standalone churn.
- **[ARC-208] Local gate diverges from CI gate** — `Makefile:137` vs `.github/workflows/ci.yml:140`: `make checkall` omits `rust-client-check-features`; a locally-green checkall can still fail CI on a feature-gating regression. Append it to `checkall`.
- **[ARC-209] Dashboard build coupling to gitignored `ts-client/dist`** — `dashboard/package.json`: fresh/stale checkouts fail at dashboard typecheck (documented gotcha; has bitten worktrees repeatedly). Consider a `development` export condition pointing at `ts-client/src`.
- **[ARC-210] Committer-turn subscription fan-out couples write latency to subscriber load** — accepted design (`committer.rs` + `subs.rs`); residual risk is a db heavy in `distinct`/`aggregate`/`search`/`vector` subscriptions throttling writes. `rtdb_subs_skips_total`/`rtdb_subs_reruns_total` exist — add an alerting threshold on rerun ratio before production surprises (see enhancement ENH-024).

### Security
- **[SEC-203] All rate limits default to 0 (off) in code** — `server/src/config.rs:462-465, 592`: `.env.example` sets admin/anonymous to 10 rpm, but storage and token/db limits ship 0 even there, and hand-configured deployments get no brute-force bound at all. Recommend safe non-zero code defaults (0 = explicit opt-out), especially `/admin/login`, plus a non-zero storage limit in `.env.example` (unauthenticated `GET /storage/{id}` is a bandwidth-abuse surface).
- **[SEC-204] CLI accepts secrets as argv flags** — `cli/src/main.rs:34-40`: `--token`/`--admin-key` visible in `ps`/shell history. Document env vars as primary, and/or hide the flags or warn on argv-supplied secrets.
- **[SEC-205] OAuth token-exchange failure logs the full provider response JSON** — `server/src/auth/provider.rs:132`: a response missing `access_token` can still contain `id_token`/`refresh_token` fragments. Log key names only (the Apple provider at `apple.rs:144` already does this right).
- **[SEC-206] `random_token` uses `rand::thread_rng()`** — `server/src/db.rs:663-667`: mints session/machine tokens, OAuth state, CSRF nonces. CSPRNG in practice but not an API contract across versions; the codebase already uses `OsRng` (`webhook.rs:174`) and `ring` (`notify.rs:89`) elsewhere. Standardize on `OsRng`. **Must land as its own reviewed commit** per the standing security rule (no silent secret-generation changes).

### Code Quality
- **[QA-204] `rust-client/src/admin.rs` is 3,844 lines, ~two-thirds inline test module** — move wiremock tests to a sibling `admin/tests.rs` (as `in_memory/` already does).
- **[QA-205] `rust-client/src/in_memory/tests.rs` is a single 6,088-line test file** — split by feature area; also `server/tests/golden_vector_test.rs` has a CC-31 harness fn (cosmetic, test code).
- **[QA-206] `server/src/query.rs:2636-2643`: `#[allow(dead_code)]` on `SearchCtx.db`/`table_name` "for symmetry"** — speculative fields nothing reads; drop until a caller needs them.

### Documentation
- **[DOC-213] docs/README.md stale file counts** — claims 33 specs / 58 plans; actual 40 / 61. Drop the counts (brittle per the style guide) or fix them.
- **[DOC-214] `auth/` subsystem has no module-level docs** — 10 of the 12 server files lacking `//!` headers are the entire `auth/` tree (plus `admin/mod.rs`, `main.rs`).
- **[DOC-215] OAUTH_SETUP.md minor staleness** — closing "Adding a new provider (Microsoft, Apple, …)" names two providers that shipped above it; anonymous-auth/merge deserves a cross-reference. Otherwise verified perfectly accurate.
- **[DOC-216] Root README minor omissions** — endpoints table omits `GET /privacy`; quickstart uses `jq`/`cargo` with no prerequisites pointer (CONTRIBUTING has the list).
- **[DOC-217] No release has ever been cut** — ~390 CHANGELOG lines under `[Unreleased]` with ad-hoc dated subsections; consider tagging `0.1.0` (see enhancement ENH-026).
- **[DOC-218] ARCHITECTURE.md has no diagrams** — the style guide prescribes Mermaid; a committer/tasks/taps component diagram and side-table schema map exist only as prose.
- **[DOC-219] python-client docstrings lack structured convention** — ~95% present but plain prose; `Args:`/`Returns:`/`Raises:` in only 2 of 16 files. Adopt Google style incrementally.

---

## Detailed Findings

### Architecture & Design
Overall health **Good** with a strong trajectory — the server core is near-excellent. The committer is a textbook actor pattern: one message-driven task per db, seven typed request arms, a single `publish_taps` enforcement point, and background tasks that only enqueue work back through it — the invariant is structurally enforced, not just documented. Prior audit remediation is real (workspace-unified cargo deps, shared OAuth HTTP client, Python admin dedup via request-description layer, dashboard consuming the SDK). Build/CI discipline is excellent (SHA-pinned actions, least-privilege permissions, env-drift-check). The one High finding is the client-side in-memory harness triplication (ARC-201); mediums are decomposition debt (ARC-202…ARC-206). Scalability posture is honest and staged, with multi-instance gaps documented at the exact field declaring them. Graph-analytics caveat: central-symbol/community queries ran during an index rebuild and returned degenerate results; the agent compensated with direct structural inspection, and complexity/hotspot figures were served from the current HEAD.

### Security Assessment
Posture **Strong** — zero Critical/High. The codebase shows a sustained hardening trail (SEC-001…SEC-132 in comments, each closing a real class). Verified strengths: strict identifier validation + `$n` binding on all SQL with case-insensitive collision rejection at table/field/index level; the one raw-SQL escape hatch structurally denied to delegated admins; hashed-at-rest tokens with plaintext returned exactly once; constant-time compares with boot-time key-strength validation; single-use Postgres-backed OAuth state with CSRF double-submit enforced as router middleware; best-in-class webhook SSRF defense (registration denylist + connect-time DNS re-check closing TOCTOU rebinding); hardened unauthenticated blob serve (content-type allowlist, nosniff, signed-URL enforcement option, per-row blob ownership, bounded Range handling); per-op auth re-check on open WebSockets; generic 500s; cardinality-conscious `/metrics`; clean `bun audit`; gitleaks + detect-private-key in pre-commit. The two Mediums are the forwarded-header trust seam (SEC-201, currently mitigated only by the compose loopback bind) and the unfixed-upstream `rsa` advisory (SEC-202, unexercised code path).

### Code Quality
Health **Good**; debt is **Low–Moderate**, concentrated almost entirely in complexity/decomposition rather than hygiene. Zero TODO/FIXME markers across all six packages; every one of the 45 `#[allow]`s, 7 `biome-ignore`s, and deferred pyright rules carries a written justification; the unwrap ban holds (~7 non-test sites, all infallible with explanatory messages); strict TS, ruff bugbear+SIM, clippy `-D warnings`. Test coverage estimated **Good (>70%)**: ~142 test files (47 server integration files ≈43k lines against real Postgres with RAII per-test databases; error-path tests assert specific `{code, message}` envelopes). The standout is the parity harness: `wire-corpus/golden-vector.json` consumed by the server test and all three client engines converts "mirror drift is a defect" into an executable gate. Key untested areas: several high-state dashboard pages have no component test; rust-client's integration footprint is the thinnest relative to source size.

### Documentation Review
Health **Good** — unusually deep and mostly code-verified-accurate, held back by a broken DR runbook (DOC-201), an actively-wrong CLI README (DOC-202), and SDK example errors (DOC-205, DOC-209, DOC-212). Verified excellent: `docs/OAUTH_SETUP.md` (all six providers' callback paths, scopes, and env vars exactly match code), `dashboard/README.md` (all 21 routes verified one-for-one), root README endpoint/config tables, CLAUDE.md / ARCHITECTURE.md / FEATURE_MATRIX on the load-bearing claims (seven tap arms, verify-skip default, quota/committer invariants). Doc accuracy is tool-enforced where it matters most (env-drift-check, wire-corpus, zero broken doc links per par-mem). Inventory: README Excellent; API docs present per package; ARCHITECTURE.md excellent prose (no diagrams); CHANGELOG present but never versioned; CONTRIBUTING excellent (one stale invariant); deploy guide good content with a broken cutover; docstring coverage server ~81%, python ~95%, rust/ts moderate.

---

## Remediation Roadmap

### Immediate Actions (Before Next Deployment)
1. **DOC-201** — fix the restore-cutover runbook (the documented DR path does not work).
2. **SEC-201** — add trusted-proxy gating for forwarding headers (self-hosted operators without the loopback bind are exposed today).
3. **DOC-203** — restore the seven-arm tap invariant in CONTRIBUTING.md (protects the op-feed guarantee on the next contribution).

### Short-term (Next 1–2 Sprints)
1. DOC-202, DOC-204, DOC-205 — the remaining High documentation fixes (CLI README, deploy rollback/env reference, ts-client examples).
2. ARC-202 → ARC-203 — extract the wire/DSL module, then split `query.rs` (in that order).
3. ARC-201 — decompose the three in-memory engines (mechanical split first, per-terminal executors second), gated on golden-vector.
4. SEC-202, SEC-203, SEC-206 — advisory acceptance, rate-limit defaults, RNG standardization (SEC-206 as its own commit).
5. DOC-206…DOC-212 — the Medium documentation reconciliation pass.

### Long-term (Backlog)
1. ARC-204/ARC-205 — parameter-object and `from_env` decomposition (opportunistic, next-touch).
2. ARC-206 + SEC-204 — CLI split with secret-flag hardening.
3. QA-201…QA-206 — dashboard component extraction, `useAsync` decision, type-ignore cleanup, test-file splits.
4. Low documentation items (DOC-213…DOC-219) and enhancements ENH-023…ENH-027 (tracked as board cards with plans under `docs/fable/`).

---

## Positive Highlights

- **The committer actor pattern is structurally enforced, not aspirational**: seven typed request arms, one `publish_taps` seam, background tasks that only enqueue — the audit verified the invariant holds at every arm.
- **Four-way wire-contract risk is actively managed by an executable gate**: shared `wire-corpus/` + `golden-vector.json` fixtures with parity tests in all four implementations.
- **Security hardening is deep and real**: SQL injection discipline is exemplary; SSRF defense includes connect-time DNS re-checking; credential storage is hash-at-rest with single plaintext return; constant-time compares throughout.
- **Hygiene is near-exemplary**: zero TODO/FIXME markers repo-wide; every lint suppression carries a written justification; the unwrap ban holds.
- **Doc accuracy is enforced by tooling**: `make env-drift-check`, the wire-corpus, and zero broken doc links; OAUTH_SETUP.md and dashboard/README.md verified exactly against code.
- **Prior audit remediation is genuine** — a visible SEC/ARC/QA ledger in comments maps to real structural fixes (workspace deps, shared OAuth client, admin dedup, SDK-consuming dashboard).
- **CI discipline**: SHA-pinned actions, least-privilege permissions, feature-combination gates for the flagged rust-client.
- **Test isolation done right**: per-test databases with RAII cleanup against a real Postgres, error-path assertions on exact error envelopes.

---

## Audit Confidence

| Area | Files Reviewed | Confidence |
|------|---------------|-----------|
| Architecture | ~30 (all package manifests, core server modules, build system; graph analytics partially degraded by a mid-audit index rebuild, compensated by direct inspection) | High |
| Security | ~46 tool passes across auth/, SQL construction, storage, deploy, dependency audits | High |
| Code Quality | ~25 + full par-mem complexity/hotspot/dead-code analytics at current HEAD | High |
| Documentation | ~26 docs verified line-by-line against code | High |

---

## Remediation Plan

> This section is generated by the audit and consumed directly by `/fix-audit`.
> It pre-computes phase assignments and file conflicts so the fix orchestrator
> can proceed without re-analyzing the codebase.
> Per-issue execution detail lives in `AUDIT-REMEDIATION-PLAN.md`.

### Phase Assignments

#### Phase 1 — Critical Security (Sequential, Blocking)
<!-- No Critical Security issues exist. These three Security issues are PROMOTED here because they edit files also targeted by Architecture/Code Quality restructures (config.rs, cli/src/main.rs) — landing them first keeps the conflicts out of parallel execution. DOC-201 (the run's only Critical, Documentation) rides in Phase 1 so the broken DR runbook is fixed before anything else ships. -->
| ID | Title | File(s) | Severity |
|----|-------|---------|----------|
| DOC-201 | Fix restore-cutover runbook (compose never reads `.env` for `RTDB_DATABASE_URL`) | `deploy/README.md` | Critical |
| SEC-201 | Trusted-proxy gating for `CF-Connecting-IP`/XFF | `server/src/http_api.rs`, `server/src/config.rs`, `server/src/auth/cookie.rs`, `.env.example`, `deploy/docker-compose.yml` | Medium (promoted) |
| SEC-203 | Non-zero rate-limit defaults | `server/src/config.rs`, `.env.example` | Low (promoted) |
| SEC-204 | Stop accepting secrets via argv silently | `cli/src/main.rs`, `cli/README.md` | Low (promoted) |

#### Phase 2 — Critical Architecture (Sequential, Blocking)
<!-- No Critical Architecture issues exist. These are PROMOTED because each explicitly blocks Code Quality/Documentation work (they relocate the symbols those fixes would touch). Execute in the order listed. -->
| ID | Title | File(s) | Severity | Blocks |
|----|-------|---------|----------|--------|
| ARC-202 | Extract pure wire/DSL module; break `query.rs` ↔ `txn.rs` cycle | `server/src/query.rs`, `server/src/txn.rs`, `server/src/protocol.rs` | Medium | ARC-203, QA-206 |
| ARC-203 | Split `query.rs` into a `query/` directory module | `server/src/query.rs` | Medium | QA-206 |
| ARC-205 | Decompose `Config::from_env` per subsystem | `server/src/config.rs` | Medium | (after SEC-201, SEC-203) |
| ARC-201 | Decompose the three in-memory engines (split files, per-terminal executors, per-directive functions) | `ts-client/src/in_memory.ts`, `python-client/src/par_rt_db/in_memory.py`, `rust-client/src/in_memory/query.rs`, `rust-client/src/in_memory/migrate.rs` | High | QA-205 |

#### Phase 3 — Parallel Execution

**3a — Security (remaining)**
| ID | Title | File(s) | Severity |
|----|-------|---------|----------|
| SEC-202 | Accept RUSTSEC-2023-0071 in `audit.toml` with justification | `server/` (new `audit.toml` at workspace root) | Medium |
| SEC-205 | Log OAuth token-exchange failures by key names only | `server/src/auth/provider.rs` | Low |
| SEC-206 | `random_token`: `thread_rng` → `OsRng` (own commit) | `server/src/db.rs` | Low |

**3b — Architecture (remaining)**
| ID | Title | File(s) | Severity |
|----|-------|---------|----------|
| ARC-204 | `CommitterConfig` parameter object | `server/src/committer.rs`, `server/src/lib.rs` | Medium |
| ARC-206 | Split `cli/src/main.rs` into modules | `cli/src/main.rs` (+ new `cli/src/*.rs`) | Medium |
| ARC-207 | Fold loose `AppState` fields into substructs | `server/src/lib.rs` | Low |
| ARC-208 | Add `rust-client-check-features` to `checkall` | `Makefile` | Low |
| ARC-209 | Dev-mode export condition for `@par-rt-db/client` | `ts-client/package.json`, `dashboard/package.json` | Low |
| ARC-210 | Rerun-ratio observability threshold (defer to ENH-024 if preferred) | `server/src/subs.rs`, deploy docs | Low |

**3c — Code Quality (all)**
| ID | Title | File(s) | Severity |
|----|-------|---------|----------|
| QA-201 | Extract dashboard god components | `dashboard/src/pages/WebhooksPage.tsx`, `dashboard/src/pages/SchemaHistoryPage.tsx` | Medium |
| QA-202 | Adopt or delete `useAsync` | `dashboard/src/lib/useAsync.ts` (+ list pages if adopting) | Medium |
| QA-203 | Type the `_sched_op`/`_wf_op` helpers; drop stale ignores | `python-client/src/par_rt_db/ws_client.py`, `python-client/pyproject.toml` | Medium |
| QA-204 | Move `admin.rs` inline tests to `admin/tests.rs` | `rust-client/src/admin.rs` | Low |
| QA-205 | Split `in_memory/tests.rs` by feature area (after ARC-201) | `rust-client/src/in_memory/tests.rs` | Low |
| QA-206 | Drop dead `SearchCtx` fields (after ARC-202/203) | `server/src/query.rs` (post-split: `server/src/query/search.rs`) | Low |

**3d — Documentation (all)**
| ID | Title | File(s) | Severity |
|----|-------|---------|----------|
| DOC-202 | Fix CLI README (`--url`, 7 commands, env vars) — after SEC-204/ARC-206 | `cli/README.md` | High |
| DOC-203 | Seven-arm tap invariant in CONTRIBUTING | `CONTRIBUTING.md` | High |
| DOC-204 | Real rollback procedure + env reference in deploy README | `deploy/README.md` | High |
| DOC-205 | Fix ts-client README APIs | `ts-client/README.md` | High |
| DOC-206 | Refresh SPEC_STATUS.md (after DOC-207) | `docs/superpowers/SPEC_STATUS.md` | Medium |
| DOC-207 | Fix FEATURE_MATRIX rows 11 & 18 | `FEATURE_MATRIX.md` | Medium |
| DOC-208 | Correct verify-skip default in two docs | `deploy/README.md`, `CHANGELOG.md` | Medium |
| DOC-209 | Fix rust-client README | `rust-client/README.md` | Medium |
| DOC-210 | Fix server README | `server/README.md` | Medium |
| DOC-211 | SDK doc-comments (`missing_docs` + JSDoc) | `rust-client/src/lib.rs`, `rust-client/src/http.rs`, `ts-client/src/schema.ts`, `ts-client/src/react.tsx`, `ts-client/src/client.ts` | Medium |
| DOC-212 | Fix python-client README | `python-client/README.md` | Medium |
| DOC-213 | docs/README counts | `docs/README.md` | Low |
| DOC-214 | Module-level `//!` docs for `auth/` | `server/src/auth/*.rs`, `server/src/admin/mod.rs`, `server/src/main.rs` | Low |
| DOC-215 | OAUTH_SETUP closing section + anon cross-ref | `docs/OAUTH_SETUP.md` | Low |
| DOC-216 | Root README `/privacy` + prerequisites pointer | `README.md` | Low |
| DOC-217 | Changelog versioning note (full release = ENH-026) | `CHANGELOG.md` | Low |
| DOC-218 | Mermaid diagrams for ARCHITECTURE.md | `docs/ARCHITECTURE.md` | Low |
| DOC-219 | Google-style docstrings convention (incremental) | `python-client/src/par_rt_db/` | Low |

### File Conflict Map
<!-- Files touched by issues in multiple domains. Fix agents must read current file state
     before editing — a prior agent may have already changed these. -->

| File | Domains | Issues | Risk |
|------|---------|--------|------|
| `server/src/config.rs` | Security + Architecture | SEC-201, SEC-203, ARC-205 | ⚠️ Sequenced Phase 1 → Phase 2; read before edit |
| `cli/src/main.rs` | Security + Architecture (+ Documentation reads it) | SEC-204, ARC-206, DOC-202 | ⚠️ SEC-204 first; DOC-202 documents the post-change surface |
| `server/src/query.rs` | Architecture + Code Quality | ARC-202, ARC-203, QA-206 | ⚠️ ARC-202 → ARC-203 relocate every symbol; QA-206 last |
| `server/src/txn.rs` | Architecture | ARC-202, ARC-204 | ⚠️ Read before edit |
| `rust-client/src/in_memory/` | Architecture + Code Quality | ARC-201, QA-205 | ⚠️ ARC-201 restructures the module; QA-205 after |
| `deploy/README.md` | Documentation | DOC-201, DOC-204, DOC-208 | ⚠️ Same file, three edits — one agent should batch them |
| `server/src/lib.rs` | Architecture | ARC-204, ARC-207 | ⚠️ Read before edit |
| `.env.example` | Security | SEC-201, SEC-203 | ⚠️ Sequential within Phase 1 |

### Blocking Relationships
<!-- Explicit dependency declarations from audit agents.
     Format: [blocker issue] → [blocked issue] — reason -->
- SEC-201 → ARC-205: both edit `server/src/config.rs`; land the trusted-proxy knob before the `from_env` restructure relocates the parsing code
- SEC-203 → ARC-205: same file, same reason
- SEC-204 → ARC-206: both edit `cli/src/main.rs`; land the secret-flag hardening before the module split relocates the arg definitions
- ARC-202 → ARC-203: extract the wire/DSL types before splitting `query.rs`, or the split moves symbols the extraction then moves again
- ARC-202 → QA-206 and ARC-203 → QA-206: `SearchCtx` moves in both restructures; delete its dead fields afterward
- ARC-201 → QA-205: the engine refactor restructures `rust-client/src/in_memory/`; split its test file afterward
- ARC-206 → DOC-202: document the CLI after the flag surface settles (SEC-204 may add warnings/hide flags)
- DOC-207 → DOC-206: SPEC_STATUS defers to FEATURE_MATRIX as source of truth; fix the matrix first
- SEC-206: no blocker, but MUST be its own reviewed commit (standing security rule — no silent secret-generation changes)
- ARC-201 must not run concurrently with any wire-protocol or DSL change from another domain (a protocol change mid-refactor would land four times into moving code)

### Dependency Diagram

```mermaid
graph TD
    P1["Phase 1: DOC-201 + promoted SEC-201/203/204"]
    P2["Phase 2: ARC-202 → ARC-203 → ARC-205 → ARC-201"]
    P3a["Phase 3a: Security (SEC-202/205/206)"]
    P3b["Phase 3b: Architecture (ARC-204/206/207/208/209/210)"]
    P3c["Phase 3c: Code Quality (QA-201…206)"]
    P3d["Phase 3d: Documentation (DOC-202…219)"]
    P4["Phase 4: Verification (make checkall)"]

    P1 --> P2
    P2 --> P3a & P3b & P3c & P3d
    P3a & P3b & P3c & P3d --> P4

    SEC201["SEC-201"] -->|config.rs| ARC205["ARC-205"]
    SEC204["SEC-204"] -->|cli/main.rs| ARC206["ARC-206"]
    ARC202["ARC-202"] -->|wire types| ARC203["ARC-203"]
    ARC203 -->|SearchCtx| QA206["QA-206"]
    ARC201["ARC-201"] -->|in_memory/| QA205["QA-205"]
    ARC206 -->|CLI surface| DOC202["DOC-202"]
    DOC207["DOC-207"] -->|source of truth| DOC206["DOC-206"]
```
