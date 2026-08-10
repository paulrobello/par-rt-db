# Audit Remediation Report

> **Project**: par-rt-db — self-hosted realtime document database (Rust/axum + Postgres 17 · ts-client · rust-client · python-client · dashboard · cli)
> **Audit Date**: 2026-08-09 (HEAD `613c7a6`) · **Remediation Date**: 2026-08-10
> **Severity Filter**: `all` (full remediation requested) · **Plan Source**: `AUDIT.md` `## Remediation Plan` + `AUDIT-REMEDIATION-PLAN.md`
> **Implementation**: Opus 5 (all fix agents); orchestrator (Fable) verified every batch with the authoritative `make checkall` gate
> **Branch**: `fix/audit-remediation` (worktree `.claude/worktrees/fix-audit-remediation`), 16 commits, +7640/−1128, 101 files. **Not merged, not pushed.**

---

## ⚠️ Run status: PARTIAL (~50 of 133 resolved) — paused for verification integrity

The full 133-issue remediation is in progress. **The entire SECURITY domain (all 40 issues) is complete**, plus the key architecture/code-quality/documentation fixes. The run was paused deliberately while green: continuing further would risk a context/usage-limit termination mid-batch (one subagent already died that way earlier — SEC-105/106, since completed cleanly). Every landed batch was verified by the orchestrator with the real gate before the next began. The remaining ~83 items are carried forward on this durable commit history.

---

## Execution Summary

| Domain | Done | Notable | Status |
|--------|-----:|---------|--------|
| **Security** | **40/40** | All 4 Criticals, 12 High, 14 Medium, 10 Low | ✅ **Complete** |
| **Architecture** | 4 + 1 partial | ARC-101, 103, 104, 107; ARC-102 partial | ⏸️ ARC-106/108/109/110/112–134 remain |
| **Code Quality** | 2 | QA-101, QA-102 (client mirror sweep) | ⏸️ QA-103/002R/104–111 remain |
| **Documentation** | 3 | DOC2-001, 019, 013 | ⏸️ DOC2-002–008, 010–012, 014, 020–054 remain |

**Overall**: ~50 resolved, 1 partial (ARC-102), ~83 carried forward. One real regression was caught by the gate and fixed inline (Phase 2A scheduler 60s idle-sleep broke past-due catch-up → reverted to 2s, keeping the write-gating).

---

## Resolved Issues ✅

### Security — COMPLETE (40)
- **Critical**: SEC-101 (stored XSS — content-type allowlist), SEC-102 (Microsoft nOAuth — sub+tid + JWKS + xms_edov), ARC-101 (subs-verifier default on), DOC2-001 (backup env keys forwarded).
- **High**: SEC-103 (per-db anonymous), SEC-104 (row budget + statement_timeout), SEC-105/106 (WS Origin + admin CSRF), SEC-107 (evalExpr root-admin gate), SEC-117 (Not divergence), SEC-111 (security headers), SEC-112 (rate-limit key), SEC-113 (signed URLs), SEC-118 (blob per-row auth), SEC-119 (batch cap), SEC-122 (OIDC email_verified), SEC-123 (range streaming), SEC-124 (debug_assert→real), plus the Phase 2 architecture Highs (below).
- **Medium**: SEC-108 (admin route_layer), SEC-109 (admin brute-force), SEC-110 (weak-key boot fail), SEC-114 (webhook DNS pin), SEC-115 (webhook HMAC signing), SEC-120 (revocable admin session), SEC-121 (/auth/state cookie + trace redaction).
- **Low**: SEC-125 (filter validation), SEC-126 (silent sub death), SEC-127 (committer panic guard), SEC-128 (stringified errors), SEC-129 (build fingerprint gating), SEC-130 (token logging), SEC-131 (CSRF dev warn), SEC-132 (OAuth state binding), SEC-133 (dump perms), SEC-134 (webhook db scope), SEC-136 (container hardening), SEC-137 (sourcemap off), SEC-138 (CI SHA pinning), SEC-139 (localStorage doc), SEC-002R (Apple JWKS note), SEC-003R (rsa audit.toml). *(SEC-128 http_api.rs:60 deliberately skipped — client-facing 400 for the client's own malformed payload, not an internal leak.)*

### Architecture (4 + 1 partial)
- **ARC-101** subs-verifier default 0→1000 · **ARC-103** quota-spawn guard · **ARC-104** MAX_STEPS 256→1024 in 3 clients + wire-corpus pin · **ARC-107** dashboard 5th wire-contract copy deleted (upsert drift fixed).
- **ARC-102** ⚠️ PARTIAL: idle-write gating done (scheduler/reaper/mutation-log); idle-database reclamation deferred (needs `Shutdown` + subscriber guard).

### Code Quality (2)
- **QA-101** ts-client OAuth union → 6 providers + signInWith* exports · **QA-102** session admin mirrored to rust/python/cli.

### Documentation / Configuration (3)
- **DOC2-001** backup keys reach the container (+ INFO log) · **DOC2-019** six more env keys + drift-check grep widened · **DOC2-013** CONTRIBUTING checklist.

---

## Commits (branch `fix/audit-remediation`, 16)

```
dd4cfe7 QA-101 OAuth union + QA-102 session admin mirror (rust/python/cli)
5948a10 Low-severity security batch (SEC-125..134,136..139,002R,003R)
103265e SEC-120 revocable admin session + SEC-121 /auth/state binding
04d71b8 SEC-114 webhook DNS pin + SEC-115 HMAC-signed deliveries
d6884f7 SEC-108 admin route_layer + SEC-109 brute-force + SEC-110 weak-key boot
6db8de8 SEC-104 by-query row budget + statement_timeout backstop
9ddaf85 SEC-105 WS Origin validation + SEC-106 admin CSRF
b220af4 audit remediation report
9fb0950 SEC-117 Not divergence + SEC-103 per-db anonymous
e289b2b Phase 2D — forward RTDB_* env keys (DOC2-001,019,013)
8f87490 Phase 2C — delete dashboard 5th wire-contract copy (ARC-107)
7197a4d Phase 2B — client MAX_STEPS 256->1024 (ARC-104)
c51697d Phase 2A — gate idle pollers, subs verifier, quota spawn (ARC-101,102,103)
2b95233 Phase 1C — evalExpr root-admin gate + real identifier checks (SEC-107,124)
969f7ab Phase 1B — Microsoft nOAuth + OIDC email_verified (SEC-102,122)
00d6fe8 Phase 1A — storage XSS, security headers, rate-limit key, signed URLs, blob auth, batch cap, range (SEC-111,101,112,113,118,119,123)
```

---

## Verification

Each batch verified by the orchestrator with the authoritative gate (`make checkall` stages minus `dev-db-up` — skipped intentionally: the worktree compose project name would start a second Postgres on the already-bound port 55434; the dev DB was confirmed live and shared). Every batch `GATE_EXIT=0`: env-drift ✅ · fmt ✅ · lint (clippy `-D warnings`/biome/ruff) ✅ · typecheck ✅ · all six test suites ✅.

- The `webhook_delivery_end_to_end` test had a load-dependent timing flake (passed isolated, timed out under full-suite parallel contention); its poll deadline was widened 10s→30s as a test-reliability fix. It is otherwise a pre-existing condition, not a regression.
- **One real regression was caught by the gate and fixed inline**: Phase 2A's 60s idle scheduler sleep broke `one_shot_catches_up_after_being_past_due` (no notify-on-insert) → reverted to 2s, keeping the write-gating that is ARC-102's actual win. This is why the orchestrator runs the gate itself rather than trusting agent self-reports.

---

## Carried Forward (~83 issues) — recommended next session

**Architecture** (resume with the smaller Highs first): ARC-109 (export ScheduleWhen), ARC-110 (rust feature-combo gate), ARC-112 (DbStats 6 fields — before ARC-108), ARC-113/114/115/117/119/120/121/123, ARC-125–134, ARC-102 step 4. **Large refactors**: ARC-106 (dashboard consumes SDK), ARC-108+124 (collapse python's 4 admin copies).

**Code Quality**: QA-103 (extend golden-vector corpus — must precede QA-002R), QA-002R (per-terminal extraction in 3 client engines — **large, multi-commit**), QA-104/105/106/107/108/109/110/111.

**Documentation**: DOC2-002–008, 011/012, 014, 020–054 (spec status sweep, README/CHANGELOG backfill, docstrings, troubleshooting).

`AUDIT-REMEDIATION-PLAN.md` has per-issue detail for all of it; board cards tagged `audit-2026-08-09` track each item.

---

## Requires a Human Decision 🔧

1. **[DOC2-010] `ENHANCEMENTS.md`** — retire (pointer to the board + update 14 citing files) or bring current? The `/enhancement-*` family has migrated to the board.
2. **[DOC2-015]** — this report *replaces* the stale 2026-08-07 file; "delete vs correct" is mooted by replacement. Confirm keep vs delete at wrap-up.

---

## Next Steps

1. **Continue remediation** — re-run `/fix-audit` or resume manually; the playbook + board cards have everything remaining. Security is fully done, so the next pass is architecture → code-quality → documentation.
2. **Decide** DOC2-010.
3. **Merge** `fix/audit-remediation` to `main` (16 commits, gate-green) when ready — rebase onto latest `main` first. Push to `origin` is a separate, explicitly-confirmed step.
4. **Re-run `/audit`** after merge to refresh `AUDIT.md`.
5. **Review before merge**: the auth/security changes (SEC-101/102/103/105/106/107/108/110/114/115/117/118/119/120/122/124) are flagged in their commit messages per the standing security rule.
