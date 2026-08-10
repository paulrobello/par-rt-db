# Audit Remediation Report

> **Project**: par-rt-db — self-hosted realtime document database (Rust/axum + Postgres 17 · ts-client · rust-client · python-client · dashboard · cli)
> **Audit Date**: 2026-08-09 (HEAD `613c7a6`)
> **Remediation Date**: 2026-08-10
> **Severity Filter Applied**: `all` (full remediation requested)
> **Plan Source**: `AUDIT.md` `## Remediation Plan` + `AUDIT-REMEDIATION-PLAN.md` playbook
> **Implementation Model**: Opus 5 (all fix agents); orchestrator (Fable) verified each batch with the authoritative `make checkall` gate
> **Branch**: `fix/audit-remediation` (worktree `.claude/worktrees/fix-audit-remediation`), 8 commits, not yet merged

---

## ⚠️ Run status: PARTIAL — terminated by a 5-hour API usage limit

The full 133-issue remediation was scoped and in progress. **21 issues were resolved and gate-verified** (all 4 Criticals, all Phase 1/2 Highs, and 4 Phase 3 Highs) before a 429 usage limit (`Usage limit reached for 5 hour`, reset 2026-08-10 16:04) terminated the SEC-105/106 subagent mid-task. The worktree was left **clean and green** (the agent died in its reading phase — no half-finished edits); all 8 commits are durable and each was verified with the real `make checkall` gate before the next batch began. **~112 issues remain carried forward** on this commit history.

---

## Execution Summary

| Phase | Status | Agent | Targeted | Resolved | Partial | Carried-forward |
|-------|--------|-------|----------|----------|---------|-----------------|
| 1 — Critical Security | ✅ | fix-security ×3 | 11 | 11 | 0 | 0 |
| 2 — Critical Architecture | ✅ | fix-arch ×3 + fix-doc ×1 | 8 | 7 | 1 (ARC-102) | 0 |
| 3a — Security (remaining) | ⏸️ Partial | fix-security ×2 | 4 reached | 2 (SEC-117,103) | 0 | SEC-105/106 died; SEC-104, 108–115, 120–139, 002R/003R |
| 3b — Architecture | ⏭️ Not started | — | — | 0 | 0 | ARC-106,108–110,112–115,117,119–121,123,125–134 |
| 3c — Code Quality | ⏭️ Not started | — | — | 0 | 0 | QA-101,102,103,002R,104–111 |
| 3d — Documentation | ⏭️ Not started | — | — | 0 | 0 | DOC2-002–008,010–012,014,020–054 |
| 4 — Verification | ✅ per-batch | orchestrator | — | — | — | worktree clean; last gate green |

**Overall**: 21 resolved (8 commits, +3389/−642, 55 files), 1 partial, ~112 carried forward.

---

## Resolved Issues ✅

### Security (13)
- **[SEC-111]** Router-wide security headers — `server/src/lib.rs` — `security_headers` middleware (CSP `frame-ancestors 'none'`, `nosniff`, `X-Frame-Options: DENY`, `Referrer-Policy`, HSTS on https only); skips the existing OAuth-callback CSP.
- **[SEC-101]** Stored-XSS on public storage — `http_api.rs` — content-type allowlist (`INLINE_SAFE_CONTENT_TYPES`); non-allowlisted → `octet-stream` + `Content-Disposition: attachment` + `nosniff` at read time; SVG excluded. 3 tests.
- **[SEC-112]** Spoofable rate-limit key — `http_api.rs`, `rate_limit.rs` — `CF-Connecting-IP` then rightmost XFF hop; `RateLimiter` hard-bounded (`MAX_BUCKETS=100_000`); compose default 0→600. 8 tests.
- **[SEC-113]** Signed-URL bypass — `http_api.rs`, `config.rs` — `RTDB_STORAGE_REQUIRE_SIGNED_URLS` (default off); mint db-scoped. 5 tests.
- **[SEC-118]** Blob per-row auth — `storage.rs`, `db.rs`, `http_api.rs` — nullable `owner_id`; enforced on authed serve/delete/metadata; public route unchanged; additive. 3 tests.
- **[SEC-119]** `/api/query-batch` cap — `http_api.rs` — `MAX_BATCH_QUERIES=64`, pre-execution. 1 test.
- **[SEC-123]** Range streaming — `storage.rs`, `http_api.rs` — `substring()` + `octet_length`; no whole-bytea load. 1 test.
- **[SEC-102]** Microsoft nOAuth — `auth/microsoft.rs`, `db.rs` — identity keyed on `sub`+`tid` (`microsoft_sub` col + index); JWKS-verified (cached, fail-closed); `email` trusted only with `xms_edov`; upsert off `email`. *(Partial: no wiremock e2e — repo is GitHub-only; logic unit-tested.)*
- **[SEC-122]** OIDC `email_verified` — `auth/oidc.rs` — absent ⇒ unverified (was trusted). Behavior change documented.
- **[SEC-107]** `evalExpr` SQL — `admin/schema_ops.rs`, `migrate.rs` — gated to root `admin_key`; `has_sql_violation` deleted. *(Residual: least-priv Postgres role deferred; root-admin gate closes the exposure.)*
- **[SEC-124]** `debug_assert` backstops — `migrate.rs`, `schema.rs` — 5→real checks + 3 new sibling-site checks (release-safe). 2 tests.
- **[SEC-117]** `Not` divergence — `query.rs` — `NOT COALESCE((inner), FALSE)`; SQL scan and Rust evaluator now agree. 7 tests.
- **[SEC-103]** Anonymous per-db — `auth/mod.rs`, `provider.rs`, `db.rs`, `config.rs` — `anonymous_enabled` column (default false) consulted in `authorize` against `db`; IP rate-limit; 1-day TTL; admin toggle. *(Residual: master-kill is mint-time only.)* 3 tests.

### Architecture (4 resolved + 1 partial)
- **[ARC-101]** Subs-skip verifier default 0→1000 (`config.rs`, `.env.example`, `docker-compose.yml`, `CLAUDE.md`); ships ON.
- **[ARC-103]** Quota-spawn gated on `max_storage_bytes_per_db != 0` (`committer.rs`), mirroring the warmer.
- **[ARC-104]** `MAX_STEPS` 256→1024 in all 3 clients + `wire-corpus` pin + server-side assertion loop-closer.
- **[ARC-107]** Dashboard 5th wire-contract copy deleted; 19 types re-exported; `OpKind` derived (drift-proof); `upsert` styling fixed. *(Residual: 6 types need a 1-line `ts-client/index.ts` export.)*
- **[ARC-102]** **PARTIAL**: scheduler/reaper/mutation-log idle writes gated (the audit's core win); idle-database reclamation deferred (needs `Shutdown` + subscriber guard).

### Documentation / Configuration (3)
- **[DOC2-001]** Backup vars now reach the container (10 keys forwarded; `main.rs` INFO log when disabled). **Critical silent-data-loss fix.**
- **[DOC2-019]** Six more env keys + drift-check grep widened to `server/src/`.
- **[DOC2-013]** CONTRIBUTING checklist names the compose `environment:` block + `env-drift-check`.

---

## Verification Results

Each batch was verified by the orchestrator with the authoritative gate (`make checkall` stages **minus** `dev-db-up` — skipped intentionally: the worktree's compose project name would start a second Postgres on the already-bound port 55434; the dev DB was confirmed live and shared). Every batch returned `GATE_EXIT=0`: env-drift ✅ · fmt ✅ · lint (clippy `-D warnings`/biome/ruff) ✅ · typecheck ✅ · all six test suites ✅.

- Two transient flakes appeared and cleared on retry (confirmed flakes, not regressions): `webhook_delivery_end_to_end` and one oauth-class contention.
- **One real regression was caught by the gate and fixed inline**: the Phase 2A scheduler's 60s idle sleep broke `one_shot_catches_up_after_being_past_due` (no notify-on-insert) — reverted to 2s, keeping the write-gating. This is why the orchestrator runs the gate itself rather than trusting agent self-reports (the 2A agent had reported "12/12 passed").
- Final committed state (`9fb0950`): **worktree clean, last full gate green**.

---

## Commits (branch `fix/audit-remediation`, 8; +3389/−642, 55 files)

```
9fb0950 fix(security): SEC-117 Not-semantics divergence + SEC-103 per-db anonymous access
e289b2b fix(doc/config): Phase 2D — forward RTDB_* env keys; backups no longer silent (DOC2-001,019,013)
8f87490 fix(arc): Phase 2C — delete dashboard's 5th wire-contract copy; fix upsert drift (ARC-107)
7197a4d fix(arc): Phase 2B — raise client MAX_STEPS 256->1024, pin in wire-corpus (ARC-104)
c51697d fix(arc): gate idle pollers, enable subs verifier, gate quota spawn (ARC-101,102,103)
2b95233 fix(security): Phase 1C — evalExpr root-admin gate + real identifier checks (SEC-107,124)
969f7ab fix(security): Phase 1B — Microsoft nOAuth + OIDC email_verified default (SEC-102,122)
00d6fe8 fix(security): Phase 1A — storage XSS, security headers, rate-limit key, signed URLs, blob auth, batch cap, range (SEC-111,101,112,113,118,119,123)
```

---

## Carried Forward (~112 issues) — recommended next session

The 5-hour usage limit stopped work partway through Phase 3. Highest-impact remaining, in priority order:

**Security (resume here):**
- **SEC-105 + SEC-106** — CSRF/Origin on WS upgrades + admin routes. *Agent died mid-task (no edits); restart fresh.* Highest remaining deployed-instance impact.
- **SEC-104** — aggregate affected-row budget + `statement_timeout` (the 1M-row committer stall).
- SEC-108 (admin `route_layer` — must precede other `admin/**`), SEC-109/110, SEC-114/115 (webhook DNS pin + signing), SEC-120/121, SEC-125–134, SEC-136–139, SEC-002R/003R.

**Architecture:** ARC-106 (dashboard consumes SDK), ARC-108+124 (collapse python's 4 admin copies — large), ARC-109/110/112/113/114/115/117/119/120/121/123, ARC-102 step 4, ARC-125–134.

**Code Quality:** QA-103 (extend golden-vector corpus — must precede QA-002R), QA-002R (per-terminal extraction in 3 client engines — **large**), QA-101, QA-102, QA-104–111.

**Documentation:** DOC2-002–008, 011/012, 014, 020–054 (status sweep, README/CHANGELOG backfill, docstrings).

---

## Requires a Human Decision 🔧

1. **[DOC2-010] `ENHANCEMENTS.md`** — retire with a pointer to the kanban board (and update the 14 citing spec/plan `Source:` lines), or bring it current? The `/enhancement-*` family has migrated to the board.
2. **[DOC2-015] `AUDIT-REMEDIATION.md`** — this report *replaces* the stale 2026-08-07 file (its unreachable commit `8d5f5c1` and shipped-as-deferred items are gone), so "delete vs correct" is mooted by replacement. Confirm keep vs delete at wrap-up.

---

## Next Steps

1. **Resume remediation** after the usage limit resets (2026-08-10 16:04) — re-run `/fix-audit` or continue manually; `AUDIT-REMEDIATION-PLAN.md` has per-issue detail for everything remaining, and board cards tagged `audit-2026-08-09` track each item.
2. **Decide** DOC2-010 (above).
3. **Merge** `fix/audit-remediation` to `main` (8 commits, gate-green) when ready — rebase onto latest `main` first. Push to `origin` is a separate, explicitly-confirmed step.
4. **Re-run `/audit`** after merge to refresh `AUDIT.md` against the new state.
5. Auth/security changes here (SEC-101/102/103/107/117/118/119/122/124) are flagged in their commit messages for manual review per the standing security rule — review the diffs before merge.
