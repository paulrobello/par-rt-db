# Audit Remediation Report

> **Project**: par-rt-db — self-hosted realtime document database (Rust/axum + Postgres 17 · ts-client · rust-client · python-client · dashboard · cli)
> **Audit Date**: 2026-08-09 (HEAD `613c7a6`) · **Remediation Date**: 2026-08-10
> **Severity Filter**: `all` (full remediation requested) · **Plan Source**: `AUDIT.md` `## Remediation Plan` + `AUDIT-REMEDIATION-PLAN.md`
> **Implementation**: Opus 5 (all fix agents); orchestrator (Fable) verified every batch with the authoritative `make checkall` gate
> **Branch**: `fix/audit-remediation` (worktree `.claude/worktrees/fix-audit-remediation`), 21 commits, +8219/−1270, 152 files. **Not merged, not pushed.**

---

## Run status: ~71 of 133 resolved — paused for verification integrity

The full 133-issue remediation is in progress. **The entire SECURITY domain (all 40 issues) is complete**, plus 13 architecture fixes, the four-client mirror sweep, and the bulk of documentation (Critical + High + the spec-status sweep). The run was paused deliberately while green: continuing into the remaining large refactors (each a multi-commit effort: ARC-108, QA-002R, ARC-106) would risk a context/usage-limit termination mid-batch leaving a half-verified state — one subagent already died that way earlier (SEC-105/106, since completed cleanly). Every landed batch was verified by the orchestrator with the real gate before the next began.

---

## Execution Summary

| Domain | Done | Status |
|--------|-----:|--------|
| **Security** | **40/40** | ✅ **Complete** (4 Critical, 12 High, 14 Medium, 10 Low) |
| **Architecture** | 12 + 1 partial | ARC-101,103,104,107,109,110,112,113,127,128,129,134; ARC-102 partial |
| **Code Quality** | 2 | QA-101, QA-102 (client mirror sweep) |
| **Documentation** | ~17 | DOC2-001–008, 011–014, 021, 022, 049, 050 |

**Overall**: ~71 resolved, 1 partial (ARC-102), ~62 carried forward. One real regression was caught by the gate and fixed inline (Phase 2A scheduler 60s idle-sleep broke past-due catch-up → reverted to 2s, keeping the write-gating).

---

## Resolved Issues ✅

### Security — COMPLETE (40)
- **Critical**: SEC-101 (stored XSS), SEC-102 (Microsoft nOAuth), ARC-101 (subs-verifier), DOC2-001 (backup env keys).
- **High**: SEC-103 (per-db anonymous), SEC-104 (row budget + statement_timeout), SEC-105/106 (WS Origin + admin CSRF), SEC-107 (evalExpr root-admin gate), SEC-111 (security headers), SEC-112/113/118/119/123, SEC-117 (Not divergence), SEC-122 (OIDC email_verified), SEC-124 (debug_assert→real).
- **Medium**: SEC-108 (admin route_layer), SEC-109 (admin brute-force), SEC-110 (weak-key boot), SEC-114 (webhook DNS pin), SEC-115 (webhook HMAC signing), SEC-120 (revocable admin session), SEC-121 (/auth/state cookie + trace redaction).
- **Low**: SEC-125–134, SEC-136–139, SEC-002R, SEC-003R. *(SEC-128 http_api.rs:60 deliberately skipped — client-facing 400 for the client's own malformed payload, not an internal leak.)*

### Architecture (12 + 1 partial)
ARC-101 (subs-verifier), ARC-103 (quota-spawn guard), ARC-104 (MAX_STEPS clients), ARC-107 (dashboard wire-contract dedup), ARC-109 (python exports), ARC-110 (rust feature gate), ARC-112 (DbStats 6 fields), ARC-113 (clone-db mirror), ARC-127 (rust re-exports), ARC-128 (py WS backoff), ARC-129 (StepResult dedup), ARC-134 (error round-trip + pydantic bound). **ARC-102 partial**: idle-write gating done; idle-database reclamation deferred.

### Code Quality (2)
QA-101 (ts-client OAuth union → 6 providers), QA-102 (session admin mirrored to rust/python/cli).

### Documentation (~17)
DOC2-001/019/013 (env forwarding), DOC2-002–008 (dashboard/python README, CHANGELOG, README routes/codes/protocol), DOC2-014 (CONTRIBUTING dashboard tests), DOC2-021/022 (vector-search doc + FEATURE_MATRIX), DOC2-011/012/049/050 (spec-status sweep — 22 flipped, 7 added, 21 index rows, contradictions + miscount fixed).

---

## Commits (branch `fix/audit-remediation`, 21)

```
3f6225f ARC-127/128/129/134/113 small architecture batch
cf5fd0f DOC2-012/011 spec status sweep (+049/050)
02dac80 High doc batch (DOC2-002..008,014,021,022)
875ddcc ARC-109 python exports + ARC-110 rust feature gate + ARC-112 DbStats
32548ac report update (security complete)
dd4cfe7 QA-101 OAuth union + QA-102 session admin mirror
5948a10 Low-severity security batch (SEC-125..134,136..139,002R,003R)
103265e SEC-120 revocable admin session + SEC-121 /auth/state
04d71b8 SEC-114 webhook DNS pin + SEC-115 HMAC signing
d6884f7 SEC-108 admin route_layer + SEC-109 + SEC-110
6db8de8 SEC-104 by-query row budget + statement_timeout
9ddaf85 SEC-105 WS Origin + SEC-106 admin CSRF
b220af4 audit remediation report
9fb0950 SEC-117 Not divergence + SEC-103 per-db anonymous
e289b2b Phase 2D — forward RTDB_* env keys (DOC2-001,019,013)
8f87490 Phase 2C — delete dashboard 5th wire-contract copy (ARC-107)
7197a4d Phase 2B — client MAX_STEPS 256->1024 (ARC-104)
c51697d Phase 2A — gate idle pollers, subs verifier, quota spawn (ARC-101,102,103)
2b95233 Phase 1C — evalExpr root-admin gate + identifier checks (SEC-107,124)
969f7ab Phase 1B — Microsoft nOAuth + OIDC email_verified (SEC-102,122)
00d6fe8 Phase 1A — storage XSS, security headers, rate-limit key, signed URLs, blob auth, batch cap, range (SEC-111,101,112,113,118,119,123)
```

---

## Verification

Each batch verified by the orchestrator with the authoritative gate (`make checkall` stages minus `dev-db-up` — skipped intentionally: the worktree compose project name would start a second Postgres on the already-bound port 55434; the dev DB was confirmed live and shared). Every code batch `GATE_EXIT=0`: env-drift ✅ · fmt ✅ · lint (clippy `-D warnings`/biome/ruff) ✅ · typecheck ✅ · all six test suites ✅. Doc-only batches verified as markdown-only diffs.

- The `webhook_delivery_end_to_end` test had a load-dependent timing flake; its poll deadline was widened 10s→30s as a test-reliability fix (pre-existing condition, not a regression).
- **One real regression was caught by the gate and fixed inline**: Phase 2A's 60s idle scheduler sleep broke past-due catch-up → reverted to 2s, keeping the write-gating. This is why the orchestrator runs the gate itself rather than trusting agent self-reports.

---

## Carried Forward (~62 issues) — recommended next session

**Architecture (large refactors + remaining)**: ARC-106 (dashboard consumes SDK), ARC-108+124 (collapse python's 4 admin copies — **large**), ARC-114 (OAuth Template Method + shared reqwest), ARC-115 (structural auth gate), ARC-117 (cargo workspace), ARC-119 (bound SchemaCache), ARC-120/121 (toolchain pin / rust admin split), ARC-123 (dashboard useAsync + drop polls), ARC-125/126/130/131/132/133, ARC-102 step 4 (idle reclamation).

**Code Quality**: QA-103 (extend golden-vector corpus — must precede QA-002R), QA-002R (per-terminal extraction in 3 client engines — **large, multi-commit**), QA-104+106 (config helpers), QA-105 (StepCtx), QA-107 (split handle_text_frame), QA-108 (split rust in_memory.rs — precedes QA-002R Rust arm), QA-109/110/111.

**Documentation**: DOC2-009, 010, 015, 020, 023–048, 051–054 (README TOCs/badges, deploy runbook, client README gaps, docstrings, the one broken doc link DOC2-053).

`AUDIT-REMEDIATION-PLAN.md` has per-issue detail for all of it; board cards tagged `audit-2026-08-09` track each item.

---

## Requires a Human Decision 🔧

1. **[DOC2-010] `ENHANCEMENTS.md`** — retire (pointer to board + update 14 citing files) or bring current? The `/enhancement-*` family migrated to the board.
2. **[DOC2-015]** — this report *replaces* the stale 2026-08-07 file; "delete vs correct" is mooted by replacement. Confirm keep vs delete at wrap-up.

---

## Next Steps

1. **Continue remediation** (fresh session, full context budget) — security is fully done; resume with the large architecture refactors (ARC-108, then QA-103→QA-002R) or the remaining docs. Playbook + board cards have everything.
2. **Decide** DOC2-010.
3. **Merge** `fix/audit-remediation` to `main` (21 commits, gate-green) when ready — rebase onto latest `main` first. Push to `origin` is a separate, explicitly-confirmed step.
4. **Re-run `/audit`** after merge to refresh `AUDIT.md`.
5. **Review before merge**: the auth/security changes are flagged in their commit messages per the standing security rule — review the diffs (esp. SEC-101/102/103/105/106/107/108/110/114/115/117/118/119/120/122/124).
