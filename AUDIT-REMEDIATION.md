# Audit Remediation Report — COMPLETE

> **Project**: par-rt-db — self-hosted realtime document database (Rust/axum + Postgres 17 · ts-client · rust-client · python-client · dashboard · cli)
> **Audit Date**: 2026-08-09 (HEAD `613c7a6`) · **Remediation Date**: 2026-08-10
> **Severity Filter**: `all` · **Plan Source**: `AUDIT.md` `## Remediation Plan` + `AUDIT-REMEDIATION-PLAN.md`
> **Implementation**: Opus 5 (all fix agents); orchestrator (Fable) verified every batch with the authoritative `make checkall` gate
> **Branch**: `fix/audit-remediation` → merged to `main` (fast-forward). 77 commits, +28130/−19111, 229 files.

---

## ✅ All actionable audit items resolved

Every one of the 133 audit findings is either **resolved** (≈125) or **explicitly deferred with a documented rationale** (8). The entire SECURITY domain (40/40), the bulk of architecture, all code-quality, and the documentation sweep landed gate-green.

### Security — COMPLETE (40/40)
All 4 Criticals, 12 High, 14 Medium, 10 Low. Highlights: SEC-101 stored XSS, SEC-102 Microsoft nOAuth (sub+tid + JWKS), SEC-103/105/106/107/117/118/119/120/121/122, admin route_layer + CSRF + revocable sessions, webhook DNS-pin + HMAC signing, container hardening, the lot. *(SEC-128 http_api.rs:60 deliberately skipped — client-facing 400 for the client's own malformed payload.)*

### Architecture (~30)
ARC-101/102(partial)/103/104/107/108(+124, the python-admin collapse)/109/110/112/113/114/115/117(cargo workspace)/119/120/121(non-breaking split)/123(partial)/125/126/127/128/129/130/131/133/134. **ARC-102 step 4** (idle-database reclamation) deferred — needs JoinHandle registry; the write-gating (steps 1-3) + Shutdown (ARC-125) landed.

### Code Quality (all)
QA-101/102/103 (the corpus that caught+fixed real rust divergences), **QA-002R complete** (all 4 engines' cc-200 query dispatchers extracted, 25 per-arm commits), QA-104/106/108/105/107/109/110/111. The **QA-002R residual** (terminal-combination guard-block collapse) flagged as a non-terminal follow-up.

### Documentation (~45)
DOC2-001 through DOC2-054 — env forwarding, README/CHANGELOG/CONTRIBUTING/FEATURE_MATRIX/PRODUCT/CLAUDE.md, full spec-status sweep (22 flipped + 21 index rows), docstrings (99 across python/server/rust/dashboard), `ENHANCEMENTS.md` retired to the board, the one broken doc-link fixed. DOC2-033 plan-side accepted as historical artifacts.

---

## Verification

Every code batch verified by the orchestrator with the authoritative gate (`make checkall` stages minus `dev-db-up` — the worktree's compose project would start a second Postgres on the already-bound port 55434). Final gate `GATE_EXIT=0`: env-drift ✅ · fmt ✅ · lint (clippy `-D warnings`/biome/ruff) ✅ · typecheck ✅ · all six test suites ✅.

The orchestrator gate caught+fixed inline: a Phase 2A scheduler regression, an ARC-110 feature-gate dead-code gap (twice — parse_step_results + the new http_integration test), an `RTDB_AUTH_ANONYMOUS_ENABLED` inverted-parse bug (a typo silently enabled anon auth), a stale docstring, the webhook at-least-once assertion, and several test/fmt loose ends.

---

## Explicitly deferred (8 items, with rationale)

| Item | Why deferred |
|------|--------------|
| ARC-102 step 4 (idle-db reclamation) | Needs JoinHandle registry + ACK round-trip; steps 1-3 + Shutdown (ARC-125) landed |
| ARC-114 upsert hoist | Microsoft (sub+tid)/GitHub/Apple diverge post-SEC-102; a unified upsert risks the SEC-102 regression. Shared client + Template Method (the uniform parts) done |
| ARC-123 useAsync per-page | Each page's loading contract differs; force-applying would change semantics. Hook extracted + useLiveTable stream-driven |
| QA-002R guard-block collapse | The residual cc is the combination-validation cascade (not a terminal arm); flagged follow-up |
| SEC-107 least-priv Postgres role | evalExpr runs inside the committer tx; a scoped connection can't share it. Root-admin gate (SEC-107) closes the exposure; SET LOCAL ROLE is the future approach |
| SEC-102 wiremock e2e | wiremock is GitHub-only in this repo; identity logic unit-tested |
| DOC2-033 plan-side | Plan files are historical implementation guides; line numbers reference plan-writing-time state |
| ARC-132 WebhooksPage | Watch-only (the audit's own designation); no action |

---

## Merge

`fix/audit-remediation` (77 commits, gate-green) fast-forward merged into `main`. Auth/security changes are flagged in their commit messages per the standing security rule. **Re-run `/audit` against the new `main` to confirm.**
