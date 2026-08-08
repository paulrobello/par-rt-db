# Audit Remediation Report

> **Project**: par-rt-db — self-hosted, Convex-inspired realtime document database (Rust/axum+tokio + Postgres 17)
> **Audit Date**: 2026-08-07 (AUDIT.md against HEAD `a15e7ca`)
> **Remediation Date**: 2026-08-07
> **Severity Filter Applied**: `all`
> **Plan Source**: AUDIT.md `## Remediation Plan` (no `AUDIT-REMEDIATION-PLAN.md` playbook present)
> **Implementation Model**: Opus 5 fix agents (security, documentation, architecture, code-quality); orchestrator (this session) ran the authoritative gate and fixed regressions
> **Branch**: `fix/audit-remediation` (worktree `.claude/worktrees/fix-audit-remediation`), 37 commits ahead of `a15e7ca`
> **Final gate**: `make checkall` **GREEN** across all six packages (fmt-check, clippy `-D warnings`, typecheck, tests)

---

## Execution Summary

| Phase | Status | Agent | Targeted | Resolved | Partial | Manual/Deferred |
|-------|--------|-------|----------|---------:|--------:|----------------:|
| 1 — Critical Security | ⏭️ Skipped (none found) | — | 0 | 0 | 0 | 0 |
| 2 — Critical Architecture | ⏭️ Skipped (none found) | — | 0 | 0 | 0 | 0 |
| 3a — Security | ✅ | fix-security | 8 | 5 | 1 | 2 (monitor) |
| 3b — Architecture | ✅ | fix-architecture | 16 | 7 | 0 | 7 + 2 no-action |
| 3c — Code Quality | ✅ | fix-code-quality | 10 | 9 | 1 | 0 |
| 3d — Documentation | ✅ | fix-documentation | 21 | 20 | 0 | 1 (skipped, no tag) |
| 4 — Verification | ✅ | orchestrator | — | — | — | — |

**Overall (55 issues): 41 resolved · 2 partial · 9 deferred/manual · 2 monitor-only · 2 no-action (by design) · 1 correctly skipped.**

Two orchestrator decisions shaped Wave 2 on this correctness-critical codebase: ARC-004 (quota *enforcement behavior* change) and ARC-005 (authz *caching*) were **deferred by design** — both are semantic changes whose risk exceeds what the test suite covers and warrant deliberate review. The big correctness-core refactors (ARC-003 admin split, ARC-006/011/012, QA-002's three remaining arms) were **deferred by the agents under a behavior-preserving rule** rather than shipped half-verified.

---

## Resolved Issues ✅

### Security (5)
- **[SEC-001]** Webhook SSRF — `server/src/webhook.rs`, `admin.rs`, `config.rs`. `validate_webhook_url` enforces https-only (dev opt-in `RTDB_WEBHOOK_ALLOW_HTTP` for http), blocks embedded credentials, rejects the cloud-metadata hostnames, IP-literal + DNS-resolution denylist (loopback/RFC1918/link-local/`169.254.169.254`/multicast); `build_delivery_client` uses `redirect(Policy::none())`. New `RTDB_WEBHOOK_ALLOW_HTTP` + `RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM` wired through `.env.example` + compose.
- **[SEC-004]** Per-IP rate limit on unauthenticated `GET /storage/{id}` (incl. transform path) — `http_api.rs`, `rate_limit.rs`, `main.rs` (now serves with `into_make_service_with_connect_info`).
- **[SEC-006]** Regression test pinning the DDL single-quote-doubling defense against `'; DROP TABLE x; --`.
- **[SEC-007]** Webhook error-text disclosure — subsumed by SEC-001's `redirect(Policy::none())`.
- **[SEC-008]** `debug_assert!(is_valid_identifier(field))` at migrate field-interpolation sites.

### Documentation (20)
- **[DOC-001]** (Critical) README OAuth flow corrected to the live six-provider routes + `OAUTH_SETUP.md` narrative; `postMessage` dropped.
- **[DOC-002]** (Critical) Stale "unmitigated login-CSRF residual" bullet removed; `rtdb-oauth-csrf` double-submit defense documented.
- **[DOC-003]/[DOC-009]** README config table trimmed + pointer to `.env.example`; ~10 missing admin routes added.
- **[DOC-004]** Package count swept to six (incl `cli`) across README + CONTRIBUTING + **CLAUDE.md** (the doc agent's allowed-files list omitted CLAUDE.md; orchestrator fixed it).
- **[DOC-005]/[DOC-006]** FEATURE_MATRIX §1 six providers (matches row #14); ENH-011 quotas row #26; §7 date bumped.
- **[DOC-007]** dashboard/README surfaces table → all 18 `App.tsx` routes.
- **[DOC-008]** CHANGELOG `[Unreleased]` backfilled; stale Python/Rust "pending" claims reconciled.
- **[DOC-010]** server/README refreshed (4 SDKs + dashboard + cli, six providers, three per-row-auth options, regenerated module layout).
- **[DOC-011]–[DOC-018], [DOC-020], [DOC-021]** Surgical fixes (duplicated paragraph, python/rust/ts READMEs, PRODUCT.md SPA claim, CONTRIBUTING tap-site count, DESIGN.md preamble, ts-client JSDoc).

### Architecture (7)
- **[ARC-001]** `publish_taps(ctx, schema, write_set, owner, source, docop_taps, refresh_quota_cache)` helper extracted; the four durable-write arms (`handle_mutate`/`handle_scheduled`/`handle_migrate`/`handle_reaper`) now call it. Behavior identical — tap-site contract preserved (audit/webhook/admin/subs tests green).
- **[ARC-002]** `run_committer` takes `CommitterCtx` by value via a `make_ctx` builder; `#[allow(too_many_arguments)]` removed.
- **[ARC-008]** `cli` crate bumped to edition 2024 (test-only `set_var` wrapped in `unsafe`).
- **[ARC-009]** `deny_unknown_fields` on `ServerMessage` (server only serializes it; no wire impact).
- **[ARC-010]** `scripts/env-drift-check.sh` extended: diffs every `Config::from_env` key against the compose forwarded set.
- **[ARC-013]** `handle_restore_schema` routed through `publish_taps` with `docop_taps=false` (the DDL-not-DocOps exception is now visible at the call site).
- **[ARC-014]** Local pre-commit hooks for biome (ts-client/dashboard) + ruff (python-client).

### Code Quality (9)
- **[QA-001]** Golden-vector parity test: shared `wire-corpus/golden-vector.json` (9 query cases) consumed by **all four** in-memory engines + the server (Postgres-backed). 20 new tests; found+fixed a real `TestDb` RAII race in the server test.
- **[QA-003]** python-client pyright tightened incrementally (still 0 errors).
- **[QA-004]** `noExplicitAny` policy documented in `biome.policy.md`. *(Orchestrator follow-up: the agent's `//` comments in `dashboard/biome.json` were rejected by Biome v2.5.4, silently disabling the config — stripped them; the valid config is restored.)*
- **[QA-005]** Bare `except Exception:` in the Python WS client now `logger.exception(...)` + documented.
- **[QA-006]** Fragile production `unreachable!()` in the comparison compiler eliminated (new variant is now a compile error).
- **[QA-007]** `quota::UsageCache` recovers from `PoisonError` explicitly (production paths; the remaining `.unwrap()` is test-only).
- **[QA-008]** rust-client 700-line embedded `pub mod admin` extracted to `wire/admin.rs`.
- **[QA-009]** `server/Cargo.toml` `[lints]` section added.
- **[QA-010]** Stale `# TODO(tasks 9-10)` markers dropped.

---

## Partial 🔶

- **[SEC-002]** Apple `id_token` — `iss`/`aud`/`exp` claims now validated; **JWKS ES256 signature verification deferred** (non-trivial new fetcher/cache/rotation surface). Documented as a residual transport-trust trade-off in the code.
- **[QA-002]** `schema.rs::validate_structure` (cc 55) extracted into 5 named helpers. **Three correctness-core arms deferred** (`execute_query` cc-216, `apply_one`, `execute_txn`) — see Manual Intervention.

---

## Requires Manual Intervention 🔧

### Deferred correctness-core refactors (behavior-preserving, needs a focused session)
- **[ARC-003]** `admin.rs` 2125-line god-module split into 8 submodules. Agent started but could not complete with line-by-line behavior-identical verification in one session (drafts discarded, **not** shipped). **Approach:** read `admin.rs` end-to-end, split one submodule at a time, verify the server compiles between each. Sequence after any `admin.rs` security work (SEC-001 already landed). **Effort:** large (1–2 focused sessions).
- **[QA-002] (remaining 3 arms)** Extract terminal arms from `execute_query`/`apply_one`/`execute_txn`. **Approach:** one arm per follow-up PR, full `make checkall` between extractions. **Effort:** medium each.
- **[ARC-006]** Coalesce per-mutate quota-refresh spawns (debounce vs fold-into-reaper) — touches observable spawn timing. **Effort:** medium.
- **[ARC-011]** Dedupe the three `bearer_token` helpers into `auth::extract_bearer` — the cookie/subprotocol policy differences are exactly what drift on a quick refactor; wants cross-cutting tests. **Effort:** small-medium.
- **[ARC-012]** `drop_db` orphan per-db tasks → add `CommitterRequest::Shutdown` + `JoinHandle` registry (changes the committer lifecycle). Current self-termination works (single-writer intact), just log spam. **Effort:** medium.
- **[ARC-015]** Gate cli's `tokio` behind a feature (low value). **Effort:** small.

### Deferred by orchestrator decision (semantic risk)
- **[ARC-004]** Move quota `enforce()` off the committer critical path. This is a **behavioral change to enforcement** (risk of over-cap writes committing), not a pure refactor. **Approach:** best-effort background refresh + cheap in-memory check + growth-signal re-measure, plus dedicated quota stress tests. Current critical-path enforce + fire-and-forget refresh left intact. **Effort:** medium.
- **[ARC-005]** Short-TTL cache for per-WS-frame `is_admin`/`authorize`. **Caching authz** means a revoked token stays valid up to the TTL — a deliberate security trade-off that should be a reviewed decision, not autonomous. **Effort:** medium.

### Monitor-only (no local fix available)
- **[SEC-003]** `rsa 0.9.10` Marvin Attack — transitive (jsonwebtoken/ring), RUSTSEC-2023-0071, no upstream fix. Pin/deny via `[patch]` when one ships.
- **[SEC-005]** `event-listener 5.4.1` unsound — transitive, RUSTSEC-2026-0221, not directly exercised. Update when fixed upstream.

### No action (by design)
- **[ARC-007]** Per-db single-writer scalability — intentional (correctness over horizontal scale; scales by adding databases).
- **[ARC-016]** `WebhooksPage.tsx` size — watch-item only.

### Skipped (correct)
- **[DOC-019]** Bundle a tagged release with the CHANGELOG backfill — no version tag exists yet; `[Unreleased]` is the correct home until `0.1.0` is cut.

---

## Verification Results

| Gate | Result |
|------|--------|
| `make fmt-check` (cargo fmt + biome + ruff) | ✅ Pass |
| `make lint` (clippy `-D warnings` + biome + ruff) | ✅ Pass |
| `make typecheck` (tsc + cargo check + pyright) | ✅ Pass (pyright 0 errors) |
| server tests | ✅ Pass — every binary `ok`, 0 failed (incl. new `golden_vector_test`) |
| rust-client tests | ✅ 376 passed, 0 failed (wire split intact) |
| cli tests | ✅ 9 passed (edition 2024) |
| ts-client tests | ✅ 630 passed (+9 golden-vector) |
| dashboard tests | ✅ 106 passed |
| python tests | ✅ 574 passed (+9 golden-vector) |

**Two regressions caught and fixed by the orchestrator during verification** (neither shipped):
1. SEC-004 made `serve_public_handler` require the `ConnectInfo` extractor; the test harness `spawn_app` wasn't updated → 4 image-transform tests failed. Fixed (mirrors `main.rs`).
2. The QA agent's new golden-vector tests needed formatting + a ts type fix; QA-004's `//` comments in `dashboard/biome.json` were rejected by Biome v2.5.4 (silently disabling the config → a mass spaces→tabs reformat + spurious warnings). Fixed (stripped comments; policy lives in `biome.policy.md`).

---

## Files Changed (high level)

- **Server:** `committer.rs`, `protocol.rs`, `config.rs`, `webhook.rs`, `admin.rs`, `http_api.rs`, `main.rs`, `rate_limit.rs`, `query.rs`, `migrate.rs`, `quota.rs`, `schema.rs`, `Cargo.toml`, `tests/common/mod.rs`, `tests/golden_vector_test.rs`
- **rust-client:** `wire.rs` → `wire/admin.rs`, `tests/golden_vector.rs`
- **ts-client:** `tests/golden-vector.test.ts`, `src/{index,errors}.ts` (JSDoc), `biome.policy.md`
- **dashboard:** `README.md`, `biome.json`, `biome.policy.md`
- **python-client:** `pyproject.toml`, `ws_client.py`, `wire.py`, `README.md`, `tests/test_golden_vector.py`
- **Docs/config:** `README.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`, `CONTRIBUTING.md`, `CLAUDE.md`, `server/README.md`, `python-client/README.md`, `ts-client/README.md`, `rust-client/README.md`, `PRODUCT.md`, `DESIGN.md`, `.env.example`, `docker-compose.yml`, `.pre-commit-config.yaml`, `scripts/env-drift-check.sh`, `cli/{Cargo.toml,src/main.rs}`
- **New:** `wire-corpus/golden-vector.json` (shared fixture)

Full per-commit history: `git log --oneline a15e7ca..HEAD` (37 commits).

---

## Next Steps

1. **Review the 7 deferred refactors + 2 partials** above and decide which to schedule. ARC-003 (admin split) and QA-002's arms are the highest-value structural follow-ups; ARC-004/005 are the two semantic decisions needing a human call.
2. **Wrap-up** (pending your confirmation): update CHANGELOG, delete the consumed audit artifacts (`AUDIT.md`, this report), and merge `fix/audit-remediation` → `main`. Pushing to `origin` is a separate, explicitly-confirmed step.
3. **Re-run `/audit`** after merging to confirm SEC-001/004/006/007/008, the docs, ARC-001/002/008–014, and QA-001/003–010 clear, leaving only the deferred/monitor items.
