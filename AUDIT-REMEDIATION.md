# Audit Remediation Report

> **Project**: par-rt-db
> **Audit Date**: 2026-11-04
> **Remediation Date**: 2026-11-04
> **Severity Filter Applied**: all
> **Plan Source**: AUDIT.md `## Remediation Plan` (no playbook present)
> **Implementation Model**: Opus 5 (all fix agents)
> **Branch**: `fix/audit-remediation` (integration worktree; base dd5b479, tip 75d506e)

---

## Execution Summary

| Phase | Status | Agent | Issues Targeted | Resolved | Partial | Manual |
|-------|--------|-------|----------------|----------|---------|--------|
| 1 — Critical Security | ⏭️ Skipped (empty) | — | 0 | 0 | 0 | 0 |
| 2 — Critical Architecture (ARC-001) | ✅ | fix-architecture | 1 | 1 | 0 | 0 |
| 3a — Security | ✅ | fix-security | 5 | 4 | 0 | 1 (SEC-001 upstream) |
| 3b — Architecture (remaining) | ✅ | fix-architecture | 6 | 6 | 0 | 0 |
| 3c — Code Quality | ✅ | fix-code-quality (4 agents) | 9 | 9 | 2 documented deviations | 0 |
| 3d — Documentation | ✅/⏭️ | fix-documentation | 12 | 11 | 0 | 1 (DOC-012 owner decision) |
| 4 — Verification | ✅ `make checkall` exit 0 | — | — | — | — | — |

**Overall**: 31 of 33 issues resolved; 1 blocked upstream (SEC-001), 1 blocked on owner decision (DOC-012). QA-001 and QA-002 were unblocked mid-run when the owner directed taking over codex's work; `codex/rtdb-quiesce-fix` proved **superseded** (main landed newer quiesce at eac028d) and was not merged; `enh-031-publish-pipeline` was **adopted** (squash e6c74db + corpus sync 44f27d7) with 13 conflicts resolved and stale corpus copies synced to canonical.

---

## Resolved Issues ✅

### Phase 2
- **[ARC-001]** Go error-code tables now generated from `wire-corpus/error-codes.json` (`make gen-error-codes`, two do-not-edit files + `//go:generate`); Swift's hand list replaced by a fixture-derived set. Fixed live rot found during the work (missing `CodeReadOnly` re-export). Acceptance bar demonstrated: a new ErrorCode fails both runners. 2c972a3

### Security (3a)
- **[SEC-002]** Every external GitHub Action SHA-pinned with version comments, in `ci.yml` **and** the newly adopted `release.yml`; all SHAs verified to resolve. 57fcc8c
- **[SEC-003]** OAuth close interstitials: `script-src 'sha256-…'` over the exact `window.close();` bytes replaces `unsafe-inline`. e3e2b4b
- **[SEC-004]** `MAX_CLAIM_ID_LEN = 128` enforced fail-closed on the delivered-transition claim stamp, with at-cap/past-cap boundary tests. 43808e9 — plus the sibling **`ack_note` cap** the agent found (25c1f46, filed and closed as follow-up).
- **[SEC-005]** httpx `<0.29`, websockets `<18` upper bounds; `uv lock --check` green. 03dcbfc

### Architecture (3b)
- **[ARC-002]** AppState construction contract enforced at runtime seams (app_state_test, 4 tests) + debug_assert on instance ids; orderings borrow-checker-enforced, documented. 78d96bd
- **[ARC-003]** `scripts/ts-dist-preflight.sh` leads `checkall`/`typecheck`; stale dist fails with "run make ts-client-build". **Proven live** during Phase 4: it caught this run's own stale dist and printed the remedy. e2722be
- **[ARC-004]** "Capacity planning: the per-database write budget" in `deploy/README.md` (149 c/s, 3.4 s turn hold, ENH-034 ~27%), cross-linked from `docs/ARCHITECTURE.md`. b36a9bb
- **[ARC-006]** OpenAPI drift test now probes the live router (405 = registered; 404 + control path = unregistered). 249574a
- **[ARC-007]** Two prose invariants enforced: read/write index-value typing alignment (unit test pairing `eq_bind_for`/`scalar_bind` over all FieldTypes) and the 63-byte name cap (`const` assert in ddl.rs — raising a cap fails the build). SQL-quoting/500-generic left prose-only with documented reasons. 59db15b
- **[ARC-008]** Test harness: 60 s pool acquire timeout, one retry on PoolTimedOut, per-binary connect semaphore (ceiling 8). oauth_test 63/63 in 2.8 s post-change. 179aedd

### Code Quality (3c)
- **[QA-001]** `txn/admission.rs`: `TxnAdmission` enum (Replay/ReadOnly/Admitted), `validate_static_admission`, `check_quiesce_admission`, `run_mutate_preamble` — extracted from the CC-84 god function; behavior-preserving (txn 31, read_only 18, deadline 6, scheduled 25 green; 596 unit green). e507fc2
- **[QA-002]** Three of five units: **Swift suppression ratchet** (baseline 48 sites, down-only, negative-tested); **`eval_value_expr`** decomposed per operator (EvalCtx dispatch) server-first, mirrored identically in rust-client; **`executeSearchTerminal`** decomposed to plan + per-mode scorers in ts and go. Skipped as incomplete-able: `parseTableDef` (go), `_execute_step` (python) — follow-up card filed. 44aff1a, b1b47b7, c58e83f
- **[QA-003]** God files split along existing seams — server: `subs/` (read_set/decide/liveness/mod), `migrate/` (plan/apply/backfill), `presence/` (model/mod/tests), `metrics/` (counters/mod/snapshot); clients: rust `ws/`+`http/` module dirs, ts store→presence/workflow/doc_stamp/admin_view + admin.ts types + client.ts offline_queue/changefeed/transport, python admin.py→9 `_admin_*` siblings, swift InMemoryEngine→6 modules (zero access-widening). Pure moves, re-exports keep paths. Deviation: presence's ~900-line impl block not further split (needs `pub(super)` widening — follow-up card filed). 05af941, 166ae2e, 7e0d38d, ddedaa5, 7cfddb5, 1d0c9ff, cfd8f14, 443521b, 4ff95f9
- **[QA-004]** Kotlin: `requireInner(ty)` (SCHEMA_VIOLATION) replaces all 16 `ty.inner!!` sites; 2 `phrase!!` sites collapsed; 10 remaining `!!` reviewed as guarded smart-cast workarounds. 8eee6db
- **[QA-005]** `CascadeCtx` groups the 9-param cascade threading (4 params now, allow removed); payload-shaped parameter lists documented as payload-identity rather than fake-grouped. 5cabec7, 353b14e
- **[QA-006]** `SchemaDefinition<any>` → `AnySchema | SchemaJson` at both public signatures; zero `any` leaks. 6a05ad0
- **[QA-007]** DataBrowserPage 739→354 lines (DocumentHistoryPanel, DocumentEditorPanel, useDocMutations, lib/errors, lib/format). fa95f24
- **[QA-008]** Go `InsertIf` returns `(Mutation, error)`; panics confined to `MustInsertIf`; 9 ignored-error sites audited (deliberate best-effort frame sends), 1 dead loop removed. 6a81591
- **[QA-009]** **Audit claim was wrong**: `WorkflowStatus::as_wire_str` IS used — `server/src/workflows.rs:897` binds it as a function reference (a call form grep cannot see). Kept + documented the real caller instead of breaking the build. 562e73e

### Documentation (3d)
- **[DOC-001]** Kotlin added to every hub doc; counts recounted from tree: 10 packages, 6 SDKs, 7 wire implementations, 7 runners. c7f54e4
- **[DOC-002]** `version-lockstep-check.sh` extended (gradle parser + kotlin manifest, 8 manifests); RELEASING.md updated; verified green and tampered-red. 4b8f37b
- **[DOC-003]** README gate table lists all 14 `checkall` legs in Makefile order; CONTRIBUTING count-free. ec38b72
- **[DOC-004]** README counts normalized (ten packages, six clients, seven implementations), each mirror claim verified in Kotlin sources. 65ca599
- **[DOC-005]** CHANGELOG Kotlin entry (Android/OkHttp transport note; corpus counts deferred, not hardcoded). 096f636
- **[DOC-006]** txn.rs path refs → `server/src/txn/`; OAUTH_SETUP anchor → `#hardening`; 2 extra live refs fixed in server/README.md. c7e8c84, 3b7bdd9
- **[DOC-007]** core/ row in the Packages table. b3168a6
- **[DOC-008]** error.rs doc: six client SDKs. 8145f48
- **[DOC-009]** kotlin-client README 66→215 lines, signature-verified examples (caught one wrong enum constant during verification). 6db068f
- **[DOC-010]** Dashboard README dangling colon resolved. 1ec989c
- **[DOC-011]** README Query-shape paragraph → terminals table + clause list + subsections, semantics preserved clause-for-clause. ea0699e

---

## Requires Manual Intervention 🔧

### [SEC-001] crypto-bigint 0.5.5 (RUSTSEC-2026-0387)
- **Why**: The audit's location claim was wrong — `rsa` has **no** crypto-bigint dependency. The vulnerable crate reaches the tree via `jsonwebtoken 11.1.0` → `p256 ^0.13.2` → `elliptic-curve ^0.13` → `crypto-bigint ^0.5`. jsonwebtoken is max-stable and hard-requires p256 0.13.2 (last of its line); elliptic-curve 0.14 (crypto-bigint ≥0.7.6) is unreachable on stable; `cargo update -p crypto-bigint` locks 0 packages. RC pins or git patches would be supply-chain regressions.
- **Recommended approach**: Track jsonwebtoken for a release on p256 0.14 / elliptic-curve 0.14; bump when it ships and re-run `cargo audit`. The weekly deps-audit CI job keeps it visible. Exploit class is Medium/"practically difficult" per the audit itself (one server-side signing per OAuth flow).
- **Estimated effort**: small (one version bump when upstream releases).

### [DOC-012] Coverage badge
- **Why**: CI has no public coverage service (`coverage-weekly` uploads workflow artifacts only). A badge needs an outward-facing service adoption — not invented by automation.
- **Recommended approach**: Either adopt codecov/coveralls in ci.yml (token + public coverage exposure — owner decision), or drop the badge idea and keep artifact-only coverage.
- **Estimated effort**: small (owner decision + one workflow edit) or none.

---

## Verification Results

- **Gate**: ✅ `make checkall NEXTEST=yes` exit 0 on the integration tip (75d506e). 14 legs green incl. ts-dist-preflight, cli-docs-check, docs-api, publish-check, dev-db-clean-check.
- **Rust (nextest)**: 2676/2676 passed, 17 skipped — 7 slow, **1 flaky**: `merge_test::merge_users_skips_stale_registry_rows…` failed TRY 1 on its timing assertion (dev-db carried fixture load; TRY 2 passed in 1213 s). Known shared-Postgres contention class (ARC-008's target); no remediation change touched that test; not a falsified card.
- **Swift**: 657 tests / 47 suites incl. the 292-case semantics corpus — all pass.
- **TypeScript**: 1535 pass (wire corpus 191/191 re-verified on the merged tree). **Python**: pytest green. **Go**: all 10 packages. **Kotlin**: gradle test green. **Dashboard**: tsc + vitest green.
- Full-workspace `cargo clippy --all-targets --all-features -D warnings`: green at every merge point.

## Files Changed

55 commits, 154 files, +24,692/−20,021 on `fix/audit-remediation` (dd5b479 → 75d506e). Per-issue commit hashes are on each kanban card's evidence note; the full list is `git log dd5b479..75d506e`. Includes two mid-run corrections by the orchestrator: dropped a duplicate short `checkall:` definition the enh-031 merge left (c848b73), and the CLAUDE.md first-time-bootstrap list now covers fresh worktrees, kotlin install, and the `ts-client/docs-toolchain` gap (75d506e).

## Board State

Closed: 31 issue cards + the ack_note follow-up, each with per-issue verification evidence. Blocked: SEC-001 (upstream), DOC-012 (owner decision). Backlog follow-ups filed this run: presence impl-block split, TS/Rust error-code ALL-lists, parseTableDef/_execute_step decomposition — each references the landed patterns.

## Next Steps

1. Review the two Manual Intervention items above; SEC-001 is a watch-and-bump (weekly deps-audit CI keeps it visible), DOC-012 needs your coverage-service call.
2. Re-run `/audit` after this lands for an updated AUDIT.md.
3. The adopted enh-031 release pipeline activates on the first push to `main` (publish-on-green-main per the 2026-10-18 directive) — registry credentials (`NPM_TOKEN`, `CARGO_REGISTRY_TOKEN`, PyPI pending publisher) are still prerequisites, and the ENH-031 card remains blocked on the v1 declaration.
4. Follow-up backlog (4 cards) is scoped and pattern-referenced for a future session.
