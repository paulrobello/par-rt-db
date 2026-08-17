# Audit Remediation Report

> **Project**: par-rt-db
> **Audit Date**: 2026-08-16
> **Remediation Date**: 2026-08-17
> **Severity Filter Applied**: all (no argument)
> **Plan Source**: AUDIT.md `## Remediation Plan` + AUDIT-REMEDIATION-PLAN.md playbook (per-issue Files/Steps/Method/Verify)
> **Implementation Model**: Opus 5 (all fix agents)

---

## Execution Summary

| Phase | Status | Agent | Issues Targeted | Resolved | Partial | Manual |
|-------|--------|-------|----------------:|---------:|---------:|-------:|
| 1 — Critical Security (+promoted DOC/SEC) | ✅ | fix-security | 4 | 4 | 0 | 0 |
| 2a — Critical Architecture (ARC-202/203/205) | ✅ | fix-architecture | 3 | 3 | 0 | 0 |
| 2b — Critical Architecture (ARC-201) | ✅ | fix-architecture | 1 | 1 | 0 | 0 |
| 3a — Security (remaining) | ✅ | fix-security | 3 | 3 | 0 | 0 |
| 3b — Architecture (remaining) | ✅ | fix-architecture | 5 | 5 | 0 | 0 |
| 3c — All Code Quality | ✅ | fix-code-quality | 6 | 6 | 0 | 0 |
| 3d — All Documentation (+ARC-210) | ✅ | fix-documentation | 19 | 19 | 0 | 0 |
| 4 — Verification | ✅ | orchestrator | 41 | 41 | 0 | 0 |

**Overall**: 41 issues resolved, 0 partial, 0 requiring manual intervention. 4 commits are security-flagged for your explicit review (below).

---

## Resolved Issues ✅

### Security
- **[SEC-201]** Trusted-proxy gating for `CF-Connecting-IP`/XFF — `server/src/http_api.rs`, `auth/cookie.rs`, `config.rs` (new `RTDB_TRUSTED_PROXY`, default false), `.env.example`, `docker-compose.yml` — commit `04cbc38`. Scope extension (flagged): `lib.rs` `security_headers` HSTS decision now also routes through the gated `request_is_secure`.
- **[SEC-202]** RUSTSEC-2023-0071 accepted with justification in `.cargo/audit.toml` (empirically the path cargo-audit reads; a workspace-root file is ignored) — commit `5f8686a`. `cargo audit` exits 0.
- **[SEC-203]** Non-zero per-IP rate-limit defaults (admin 10, anonymous 10, storage 300; 0 still disables; per-token/per-db deliberately remain 0, documented) — commit `4091f7f` + CHANGELOG entry.
- **[SEC-204]** CLI argv-secret warning (`secrets_on_argv` predicate + `hide_env_values`) — commit `cac3b12`.
- **[SEC-205]** OAuth token-exchange failures log key names only (`present_keys`) — `provider.rs` + a same-class instance in `github.rs` — commit `ec160ae`.
- **[SEC-206]** `random_token` on `OsRng` (last `thread_rng` in the workspace) — own single-purpose commit `92b2935`.

### Architecture
- **[ARC-201]** Three in-memory engines decomposed into structurally identical layouts: per-terminal executors (9 per client) + per-directive migration functions (8 per client) — commits `7c3b7b0` (ts), `7138a52` (python), `6e92ab1` (rust); rust `tests.rs` untouched for QA-205.
- **[ARC-202]** Pure `server/src/dsl.rs` wire/DSL module extracted; `query.rs`↔`txn.rs` import cycle broken (zero cross-imports verified); old paths kept compiling via re-exports; golden-vector parity green after the move — commit `1308f0a`.
- **[ARC-203]** `query.rs` (4,038 lines) split into `query/{mod,terminals,filter,search,row_auth}.rs` — commit `c0e092d`.
- **[ARC-204]** `CommitterConfig` parameter object (15 positional params → 7, allow removed; `quotas` rides in the struct — the playbook's 8-arg shape still trips clippy) — commit `1b65c74`.
- **[ARC-205]** `Config::from_env` decomposed into 12 per-subsystem env parsers; env-var/default parity proven by multiset diff — commit `be070f7`.
- **[ARC-206]** `cli/src/main.rs` (1,376 lines) split into `args.rs`/`output.rs`/`commands/{6 files}`; `--help` output byte-identical before/after; 27 tests before == after — commit `be5d9b8`.
- **[ARC-207]** Loose `AppState` fields folded into `Limits`/`Runtime` substructs — commit `a3debc8`.
- **[ARC-208]** `rust-client-check-features` appended to `checkall` (closes the local-vs-CI gate gap) — commit `2b8d5aa`.
- **[ARC-209]** `development` export condition in ts-client package.json (dev-serve resolves `src/` without `dist`; tsc still resolves `dist`; prod build unchanged) — commit `72f40be`.
- **[ARC-210]** Rerun-ratio observability subsection in deploy/README.md with a corrected PromQL (`sum()` around the labeled skips counter — the playbook's literal expression was a label-mismatch no-op) — commit `475d1fd`. Reassigned from Phase 3b to keep `deploy/README.md` single-owner.

### Code Quality
- **[QA-201]** `WebhooksPage` (621→~240 lines) and `SchemaHistoryPage` (402→~100) decomposed into extracted child components with key-remount behavior preservation — commit `6665515`.
- **[QA-202]** `useAsync` adopted in SessionsPage, SlowQueriesPage, WebhookDeliveriesPanel (3 real consumers) — commit `dfb1e7c`.
- **[QA-203]** `_sched_op`/`_wf_op` typed with `Literal`-keyed overloads; 8 stale ignores deleted; `reportUnnecessaryTypeIgnoreComment = "error"` enabled (surfaced and fixed 7 more) — commit `a48c954`.
- **[QA-204]** `admin.rs` (3,844 lines) → `admin/{mod,tests}.rs`; 245 test names identical before/after — commit `ac8ff0c`.
- **[QA-205]** `in_memory/tests.rs` (6,088 lines) split into 15 feature submodules; 195==195 test count, byte-identical name sets — commit `c3ac29d`.
- **[QA-206]** Dead `SearchCtx.db`/`table_name` fields + `#[allow(dead_code)]` removed — commit `5ff9937`.

### Documentation
- **[DOC-201]** Restore-cutover runbook now edits the compose `environment:` block, with `/healthz` + restored-row verification — commit `542033a`.
- **[DOC-202]** CLI README: `--url` corrected (required, `RTDB_URL` fallback, no default), 4 env vars + argv warning documented, all 16 commands from live `--help` with scripted bidirectional diff — commit `03613cb`.
- **[DOC-203]** CONTRIBUTING seven-arm tap invariant (phrase survives arm #8; grep-verified 7 call sites) — commit `3d94a3d`.
- **[DOC-204]** Deploy README: real rollback procedure, `.env.example` as canonical env reference, slow-query/anon-auth/instance-id/OAUTH pointers, ToC refreshed — commit `375b427`.
- **[DOC-205]** ts-client README: `.ttl()` signature, `revokeUserSessions`, workflow snippet, `transformUrl`/`batchQuery`; scratch-tsc compile check caught one more wrong signature (`getSignedUrl`) — commit `d0427f0`.
- **[DOC-206]** SPEC_STATUS refreshed: 7 spec rows added, 40==40 verified — commit `77a4732`.
- **[DOC-207]** FEATURE_MATRIX rows 11/18 consistent with code (`websearch_to_tsquery`) — commit `1f0c2c9`.
- **[DOC-208]** Verify-skip default corrected in deploy README + CHANGELOG (annotated, not rewritten) — commit `29fa620`.
- **[DOC-209]** rust-client README fixed (module paths, non-deprecated migrate example, 3 missing methods); `cargo doc` unblocked — commits `91480f1` + orchestrator fixups `37f5646`/`0141baa` (feature-gated doc links; two cross-phase stale paths).
- **[DOC-210]** server README: repo-root make note, post-split layout table, `/privacy` `/metrics` `/api/query-batch`, test pattern description — commit `89876e0`.
- **[DOC-211]** `#![warn(missing_docs)]` (deny via crate lints) + ~790 rust items documented + 13 JSDoc blocks — commit `20e3247`.
- **[DOC-212]** python README pins → pyproject pointers; snippet bound — commits `40d4c06`+`13ef271`.
- **[DOC-213]** docs/README brittle counts dropped — commit `b6c7567`.
- **[DOC-214]** `//!` headers for all of `auth/` + `admin/mod.rs` + `main.rs` — commit `2fd4165`.
- **[DOC-215]** OAUTH_SETUP closing section + anon cross-ref — commit `a175be9`.
- **[DOC-216]** Root README `/privacy` row + prerequisites pointer — commit `be270b4`.
- **[DOC-217]** CHANGELOG versioning note (release cut is ENH-026) — commit `58f1086`.
- **[DOC-218]** Two Mermaid diagrams in ARCHITECTURE.md (committer/taps component map; side-table schema map) — commit `fcdd77f`.
- **[DOC-219]** Google-style docstring convention + entry-point conversions (`http_client.py` — the playbook's `client.py` doesn't exist) — commit `fbfeb67`.

---

## Requires Manual Intervention 🔧

None blocking. Four items for your awareness:

1. **Security-flagged commits for explicit review** (standing rule — never merged silently): `04cbc38` (SEC-201), `4091f7f` (SEC-203), `cac3b12` (SEC-204), `92b2935` (SEC-206). Each is a single-purpose commit with full rationale in its message.
2. **Deployment behavior changes**: bare-env deployments now get per-IP rate-limit defaults 10/10/300 (0 = explicit off); header-derived client IPs now require `RTDB_TRUSTED_PROXY=true` (the shipped compose sets it).
3. **SEC-202 re-evaluation trigger**: the RUSTSEC-2023-0071 ignore stands until `jsonwebtoken` adopts `rsa` 0.10 — remove the ignore then.
4. **ENH backlog untouched by design**: ENH-023..027 cards and their `docs/fable/` plans remain open for `/enhancement-all`; nothing was closed here.

---

## Verification Results

- Build/Lint/Format: ✅ `env-drift-check`, `fmt-check`, `lint` (clippy `-D warnings` workspace-wide) all green
- Type Check: ✅ `typecheck` green (ts-client, dashboard)
- Tests: ✅ full `make checkall` sequence green — 1,721 cargo tests passed / 0 failed across server + rust-client + cli (incl. all 46 server integration binaries against real Postgres), ts-client vitest 846+, python-client pytest 961+, dashboard vitest 118, rust-client feature-matrix (8 combinations)
- `cargo doc -p par-rt-db-client --no-deps`: ✅ 0 errors (after orchestrator doc-link fixup)
- `cargo audit`: ✅ exits 0
- Per-issue validation: 41/41 confirmed present at current file locations (playbook Verify commands + presence greps)
- `make dev-db-clean` run post-gate (239 leaked schemas dropped)

Operational notes: the gate ran against the main checkout's dev Postgres (a worktree-local `dev-db-up` collides on 55434 — known worktree behavior); three Phase 3 agents were interrupted mid-run by an API usage-limit 429 and resumed to completion with no lost work; agent worktrees branch from stale `origin/main` and each began with a fast-forward to the integration tip.

---

## Files Changed

Integration branch `fix/audit-remediation` (43 commits, base `42d08a3` local main):

- **Server**: `dsl.rs` (new), `query.rs`→`query/{mod,terminals,filter,search,row_auth}.rs`, `txn.rs`, `protocol.rs`, `config.rs`, `http_api.rs`, `lib.rs`, `committer.rs`, `rate_limit.rs`, `db.rs`, `auth/{cookie,provider,github}.rs`, `auth/*.rs` (headers), `admin/{login,mod}.rs`, `main.rs`, 8 test files; `.cargo/audit.toml` (new)
- **CLI**: `main.rs` → `main.rs`+`args.rs`+`output.rs`+`commands/{mod,dbs,schema,tokens,sessions,data,workflows}.rs`; `README.md`
- **ts-client**: `in_memory.ts` → `in_memory/{index,query,migrate,validate,store}.ts`, `index.ts`, `package.json`, `schema.ts`, `react.tsx`, `client.ts`, `README.md`, 7 test files (imports)
- **python-client**: `in_memory.py` → `in_memory/{__init__,query,migrate,validate,store}.py`, `ws_client.py`, `http_client.py`, `pyproject.toml`, `README.md`, 4 test files
- **rust-client**: `in_memory/{query,migrate}.rs`, `tests.rs`→`tests/{mod,15 submodules}.rs`, `admin.rs`→`admin/{mod,tests}.rs`, `{lib,http,ws,error,cursor,migration,mutation,query,schema}.rs`, `wire/admin.rs`, `in_memory/mod.rs`, `README.md`
- **Dashboard**: `pages/{Webhooks,SchemaHistory,Sessions,SlowQueries}Page.tsx`, `components/{webhooks,schema-history}.tsx`
- **Docs/config**: `README.md`, `CONTRIBUTING.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`, `deploy/README.md`, `docs/{ARCHITECTURE,OAUTH_SETUP,superpowers/SPEC_STATUS}.md` + `docs/README.md`, `.env.example`, `docker-compose.yml`, `Makefile`, `server/README.md`, `cli/README.md`

---

## Next Steps

1. Review the four security-flagged commits (`04cbc38`, `4091f7f`, `cac3b12`, `92b2935`) — each is isolated and revertable
2. Confirm wrap-up (CHANGELOG is already current from the phase commits; deletes AUDIT.md/AUDIT-REMEDIATION.md/AUDIT-REMEDIATION-PLAN.md; merge to local main; worktree cleanup). Push to origin is a separate explicit step
3. Optional: re-run `/audit` to confirm the finding count drops to near-zero for this cycle
3. The enhancement backlog (ENH-023..027) remains queued for `/enhancement-all` — ENH-023 (behavioral-semantics corpus) builds directly on ARC-201's mirrored decomposition
