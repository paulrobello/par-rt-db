# Audit Remediation Report

> **Project**: par-rt-db
> **Audit Date**: 2026-07-25 (AUDIT.md, 55 findings across 4 domains)
> **Remediation Date**: 2026-07-25
> **Severity Filter Applied**: all (everything incl. backlog)
> **Branch**: `fix/audit-remediation` (base `bec6abc`)

---

## Execution Summary

| Phase | Status | Agent(s) | Targeted | Resolved | Partial | Manual/Deferred |
|-------|--------|----------|----------|----------|---------|-----------------|
| 1 — Promoted Security (SEC-004, SEC-001) | ✅ | fix-security (opus) | 2 | 2 | 1 (SEC-001 stopgap) | 1 |
| Wave 1 — Wire / Dashboard / CI / Docs | ✅ | 5 parallel (2 opus, 3 sonnet) | 22 | 21 | 1 (ARC-008) | 2 |
| Wave 2 — Server core / auth-config / query | ✅ | 3 parallel (2 opus, 1 sonnet) | 18 | 17 | 1 (SEC-008) | 1 |
| Wave 3 — AppState regroup (ARC-006) | ✅ | fix-architecture (opus) | 1 | 1 | 0 | 0 |
| 4 — Verification (`make checkall`) | ✅ | orchestrator (+ wire-agent resume) | — | — | — | — |

**Overall**: of 55 findings, **46 resolved** (3 of those have a documented manual follow-up), **9 deferred / no-action** (per the audit's own "no change recommended" or optional/low verdicts). The full gate (`make checkall`: fmt-check + clippy `-D warnings` + typecheck + tests across all five packages, against a live Postgres 17 + pgvector) is **green**.

> The work was partitioned into **file-disjoint agents** (each shared file owned by exactly one agent) rather than the audit's 4 domain lanes, because the audit's own Conflict Map flagged ~12 shared files where parallel domain agents would clobber each other (the wire-contract enum sweep, the dead `AdminPrincipal::User` payload, and `AppState` regroup are each "one root, do once"). Each wave was independently verified and checkpoint-committed.

### Commits on the branch
- `b0f7108` Phase 1 — SEC-004 (per-op `is_admin`), SEC-001 (in-memory admin token)
- `a3400b1` Wave 1 — wire enums + parity corpus, dashboard hardening, CI, docs
- `7b0471f` Wave 2 — `SubscriptionManager` sharding, pool config + auth hardening, query cascade refactor
- `e46ea64` Wave 3 — `AppState` sub-struct regroup
- `c5434f0` Phase 4 — five runtime issues caught by the full gate + the flaky-test fix + DOC-010

---

## Resolved Issues ✅

### Security
- **[SEC-004]** WS `is_admin` sticky for connection lifetime — `server/src/ws.rs` — recompute `is_admin` on each Subscribe/Mutate arm (handshake value no longer threads through); revoked admins lose the bypass on the next op. Fail-safe preserved (DB error ⇒ not-admin).
- **[SEC-001]** Admin token in `localStorage` — `dashboard/src/lib/session.tsx` — token now held in React state for the page session only; all `localStorage` persistence removed. *(stopgap — see Manual)*
- **[SEC-003]** `react-router-dom` 6.30→7.18 — `dashboard/package.json` — clears GHSA-jmj-jmhj-qwj2 / -wrjc-x8rr-h8h6 / -337j-9hxr-rhxg; clean v6→v7 upgrade (dashboard uses only stable router APIs); redirect-XSS vector confirmed not exercised.
- **[SEC-005]** OAuth callback origin interpolation (self-XSS) — `server/src/config.rs`, `server/src/auth/provider.rs` — strict `origin_is_valid` (reject `"`,`<`,`>`,backtick,backslash, control; require `https?://host[:port]`) **plus** JS/HTML-string escaping of `origin`+`token` at interpolation (defense in depth). Tests cover the breakout payload.
- **[SEC-006]** Unverified GitHub profile-email fallback — `server/src/auth/github.rs` — dropped; only verified emails admitted (403 otherwise).
- **[SEC-008]** Upload size hard ceiling — `server/src/config.rs`, `server/src/http_api.rs` — `HARD_MAX_FILE_SIZE` const (100 MiB) clamps `RTDB_MAX_FILE_SIZE` at boot seed and at the upload buffering point. *(PATCH-side check deferred — see Manual)*

### Architecture
- **[ARC-001]** `SubscriptionManager` global `Mutex` — `server/src/subs.rs` — sharded per-db (`HashMap<String, Arc<Mutex<DbSubs>>>`); outer lock held only to clone/insert a shard `Arc`, never across a `fan_out` Postgres re-run. **Orchestrator fix:** empty shards are retained lazily — the agent's eviction raced a concurrent `register` (which reused the same `Arc`) and could orphan its subscription; lazy eviction eliminates the race and drops the `ptr_eq` guard.
- **[ARC-002]** Hardcoded pool size — `server/src/config.rs`, `server/src/main.rs` — `RTDB_POOL_MAX_CONNECTIONS` (default 75) + `min_connections(5)` + `acquire_timeout(10s)`.
- **[ARC-003]** No CI — `.github/workflows/ci.yml` (new) — runs `make checkall` on push/PR; lets `make dev-db-up` bring up the `pgvector/pgvector:pg17` dev image (vector tests need pgvector; `max_connections=300` tuned for concurrent test binaries).
- **[ARC-004] / [ARC-009] / [QA-008]** Stringly-typed protocol enums — `server/src/protocol.rs` + ts/rust/python wire files — `AuthedUser.kind`, `ScheduleInfo.kind/.status` → typed serde enums (wire-byte-identical, bare snake_case variants); TS narrowed to `"user"|"machine"`. All construction sites updated.
- **[ARC-005] / [QA-006]** Dead `AdminPrincipal::User` payload — `server/src/admin.rs` — dropped to unit variant; stale comment + `#[allow(dead_code)]` removed.
- **[ARC-006]** `AppState` 10-field kitchen-sink — `server/src/lib.rs` + 15 files — grouped into `Realtime { subs, committers, op_feed }`, `Runtime { hot, metrics, started_at }`, `Auth { oauth_states }` (pool/config/schemas stay top-level). `AppState::new` signature unchanged. Pure regroup, no behavior change.
- **[ARC-007]** `mutation_log::check` DELETE-then-SELECT — `server/src/mutation_log.rs`, `server/src/committer.rs` — expiry moved to a 60s per-db background task (`run_cleanup`, spawned alongside the scheduler). **Orchestrator fix:** restored the read-time `expires_at > now` filter the agent had dropped (the bg task does physical cleanup, but `check` must still filter expired rows so it never returns stale cached results).
- **[ARC-008]** Cross-client wire-parity corpus — `wire-corpus/wire-corpus.json` + per-package tests (server/rust-client/ts/python) asserting parse→serialize byte-identity + `deny_unknown_fields`/enum rejection. *(f32-on-wire precision flagged — see Manual)*
- **[ARC-010]** `is_admin` swallows DB errors — `server/src/auth/mod.rs` — added `tracing::warn!` on the error path; still fails safe to `false`.
- **[ARC-011]** `compile_filter` placeholder arithmetic — `server/src/query.rs` — `push_filter_bind` helper centralizes the "compute `$N`, then push" ordering.
- **[ARC-014]** `count` under global lock — folds into ARC-001 (per-shard sum).
- **[ARC-015]** Dashboard SDK link build step — `dashboard/package.json` — `prepare` hook builds `ts-client` so a fresh checkout works (auto-skipped in production installs).

### Code Quality
- **[QA-001]** TS in-memory `get`-guard drift — `ts-client/src/in_memory.ts` — added the 3 missing clauses (`filter`/`search`/`vectorSearch`) + matched server message; new 79-case cross-client combination-matrix test (server + TS). Also added the missing `vectorSearch`/`search` cascade guards to the in-memory replica.
- **[QA-002]** `execute_query` complexity 181 — `server/src/query.rs` — validation cascade collapsed into a `Peer` dispatch table (7 const peer tables + 2 helpers); adding a terminal is now a one-line table edit. Behavior-preserving (matrix test + existing suite prove it).
- **[QA-003]** Dashboard zero tests — `dashboard/src/**` — Vitest + RTL (8 tests: `session` token shape, `useLiveTable` cleanup-on-unmount, `ConfigPage` validation); `make dashboard-test` target wired in.
- **[QA-004]** `Value::Null` fallback in `handle_subscribe` — `server/src/committer.rs` — dropped; serialization failure now returns an internal error (mirrors `fan_out`'s skip).
- **[QA-005]** Three identical WS schedule arms — `server/src/ws.rs` — extracted `run_simple_schedule` helper (cancel/pause/resume); public match structure preserved, SEC-004's per-op `is_admin` untouched.
- **[QA-007]** CLAUDE.md four→five packages, three→four clients — `CLAUDE.md` — updated throughout.
- **[QA-011]** `#[allow(too_many_arguments)]` on `register` — `server/src/subs.rs` — revisited after ARC-001; still warranted, kept with its comment.

### Documentation
- **[DOC-001]** *(Critical)* README session-expiry contradiction — `README.md` — inverted the "Known MVP limitations" bullet to document actual behavior (expiry/revocation/admin-removal take effect on open WS connections); struck the stale "deferred to Plan 2".
- **[DOC-002]** Python client undocumented — `python-client/README.md` (full rewrite), `python-client/src/par_rt_db/__init__.py` (14 re-exports, `Order` correctly omitted as nonexistent), client-count sweep across FEATURE_MATRIX/server-README.
- **[DOC-003 / 004 / 005]** README Make-targets / Configuration / Endpoints tables — rebuilt against verified source (Makefile, `config.rs`, router).
- **[DOC-006 / 007 / 008]** New root `CHANGELOG.md` (Keep a Changelog), `CONTRIBUTING.md`, `LICENSE` (MIT); license fields verified across all manifests.
- **[DOC-009]** Spec status — flipped all four design specs to "Implemented" + `docs/superpowers/SPEC_STATUS.md` index; MVP "out of scope" list marked shipped.
- **[DOC-010]** deploy/README dashboard section — added "Dashboard / SPA" section + `RTDB_STATIC_DIR` (image-baked, not a volume).
- **[DOC-011..018]** README architecture overview + Mermaid + Packages section; FEATURE_MATRIX "four clients"; dashboard operator guide; `.env.example` missing vars (incl. corrected `RTDB_STATIC_DIR`); pagination generalized to Rust+Python; real Quickstart; MVP-limitations consistency.

---

## Requires Manual Intervention 🔧

### [SEC-001] Move admin token to an HttpOnly cookie (long-term remedy)
- **Why**: the implemented in-memory holder removes the persistent `localStorage` exposure (the highest-risk vector) but a page reload now requires re-auth (intended UX tradeoff). The audit's *preferred* remedy is an HttpOnly+Secure+SameSite=Strict cookie set by a server-issued admin-login endpoint.
- **Recommended approach**: add a `POST /auth/admin-login` endpoint that validates the admin key and sets the cookie; have the dashboard send credentialed requests; clear the cookie on logout. **This is a security-sensitive auth change — review before deploy.**
- **Effort**: medium.

### [SEC-008] `PATCH /admin/config` should reject oversized `max_file_size`
- **Why**: `HARD_MAX_FILE_SIZE` (100 MiB) is enforced at boot seed and at the upload buffering point, so the on-disk worst case is bounded regardless. But the PATCH handler still *persists* a value above the ceiling (then silently ignores it at upload time). Defense-in-depth: reject with `BadRequest` at PATCH time.
- **Recommended approach**: one arm in `admin.rs::patch_config` — `if size > HARD_MAX_FILE_SIZE { return Err(BadRequest(...)) }`.
- **Effort**: small.

### [ARC-008] `Vec<f32>` on the wire narrows precision
- **Why**: the server's `VectorSearchQuery.vector` narrows JSON numbers to `f32` (to match pgvector), which serde_json re-widens to f64 on serialize — so `0.1` round-trips as `0.10000000149011612`. Pre-existing (not introduced by this remediation); surfaced by the new corpus test, which uses f32-clean fixture values to avoid coupling to it.
- **Recommended approach**: decide between (a) `Vec<f64>` everywhere + cast at the pgvector boundary, (b) document the f32 narrowing as intended, or (c) a wire-format change. Coordinated server + client change.
- **Effort**: medium.

### [QA-010] Python typed `Query`/`Transaction` tightening
- **Why**: the four `TODO(tasks 9-10)` markers in `wire.py` would swap loose `dict[str, Any]` for the typed `Query`/`Transaction` models — but that changes parse-time validation (loose → strict), a behavioral change, not a clean type-only tightening. Deferred deliberately.
- **Effort**: small-medium, but coordinate with python-client validation tests.

### Monitor-only / optional (no code change now)
- **[SEC-002]** `rsa 0.9.10` Marvin Attack (RUSTSEC-2023-0071) — no upstream fix; exposure is outbound HTTPS to fixed GitHub/Google endpoints only. Monitor.
- **[ARC-012 / ARC-013 / ARC-016]** Low hardening not pursued: rust-client tilde version reqs; lift `Principal` to its own file; enforce subprotocol uniqueness in `bearer_from_subprotocol`.
- **[DOC-019]** Optional project-specific documentation style guide (the generic `docs/DOCUMENTATION_STYLE_GUIDE.md` left by the audit was removed as an unintended write; a real project-specific guide can be added deliberately later).

### No-action (the audit itself recommended no change)
- **[SEC-007]** `RTDB_MAX_AFFECTED_DOCS` cap asymmetry — documenting only.
- **[QA-009]** par-mem `find_dead_code` false positives on public client APIs.
- **[QA-012]** Three near-identical `ensure_table` impls (helper only if a 4th appears).

---

## Verification Results

- **fmt-check**: ✅ Pass (all five packages)
- **Lint (clippy `-D warnings`)**: ✅ Pass (server + rust-client)
- **Type Check**: ✅ Pass (ts-client + dashboard `tsc --noEmit`)
- **Tests**: ✅ Pass — `make checkall` exit 0. Server (106 lib + 17 integration binaries, incl. the new `wire_corpus`/`query_combinations`), rust-client (full suite), ts-client (141 + 83-corpus), dashboard (8), python-client (157 + 81-parity). Dev Postgres 17 + pgvector via `make dev-db-up`.

### Runtime issues caught by Phase 4 (the wave agents couldn't run `cargo` due to the concurrency guardrail)
1. **ARC-007**: `check` dropped its read-time expiry filter → returned stale cached results. *(orchestrator-fixed)*
2. **ARC-008**: corpus tests used a runtime-relative path wrong under `cargo test`'s CWD → `include_str!`. *(orchestrator-fixed)*
3. **ARC-008 drift**: server `Query` serialized all fields while TS sends minimal → added `skip_serializing_if` (wire-compatible). *(wire-agent resume)*
4. **SEC-005**: the agent's own test had a stray `]` in the payload its expected string omitted. *(orchestrator-fixed)*
5. **Pre-existing flaky race** (not a regression): three dashboard tests touched the global `rtdb_config` id=1 row in parallel → serialized with a dep-free static `tokio::Mutex` so the new CI gets a reliable gate. *(orchestrator-fixed)*

All five were fixed in-place; the gate is green. None required reverting a wave.

---

## Files Changed

67 files changed, +5576 / −529, across 5 commits (`b0f7108`…`c5434f0`). 16 new files:

- `.github/workflows/ci.yml`, `CHANGELOG.md`, `CONTRIBUTING.md`, `LICENSE`, `docs/superpowers/SPEC_STATUS.md`
- `wire-corpus/wire-corpus.json` + per-package tests: `server/tests/wire_corpus.rs`, `rust-client/tests/wire_corpus.rs`, `ts-client/tests/wire-corpus.test.ts`
- Combination-matrix tests: `server/tests/query_combinations.rs`, `ts-client/tests/query_combinations.test.ts`
- Dashboard tests + config: `dashboard/src/lib/{session,useLiveTable}.test.tsx`, `dashboard/src/pages/ConfigPage.test.tsx`, `dashboard/src/test-setup.ts`, `dashboard/vitest.config.ts`

Key modified files: `server/src/{subs,committer,mutation_log,ws,admin,query,config,main,lib,protocol}.rs` + `auth/{mod,provider,github,google}.rs`; `rust-client/src/{wire,ws,http,lib}.rs`; `ts-client/src/{protocol,in_memory}.ts`; `python-client/src/par_rt_db/{wire,__init__}.py`; `dashboard/{package.json,README.md}`; root `README.md`/`CLAUDE.md`/`FEATURE_MATRIX.md`/`Makefile`/`.env.example`/`deploy/README.md`; the four design specs.

---

## Next Steps

1. **Review the Manual items above**, especially **SEC-001** (auth change — the in-memory stopgap is safe and ships now, but the HttpOnly-cookie remedy is the real fix and needs design sign-off).
2. **Decide ARC-008's f32-on-wire question** — it's the one real pre-existing wire-semantics issue the new parity corpus surfaced.
3. The branch `fix/audit-remediation` is ready to merge to `main` (rebase first — see `guides/git-ci.md`). Per the `/fix-audit` wrap-up, the final workflow (CHANGELOG update, deleting AUDIT.md/AUDIT-REMEDIATION.md, merge) awaits your explicit confirmation.
4. Re-running `/audit` after merge will reflect the new state (notably: zero Critical/High findings expected to remain; the wire-contract sweep + CI + corpus make the load-bearing invariants self-enforcing).
