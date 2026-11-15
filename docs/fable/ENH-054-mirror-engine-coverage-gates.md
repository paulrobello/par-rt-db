# ENH-054 — Coverage-threshold gates on the client-mirror engine files

**Consumer**: `/enhancement-all` (Opus 5 implementer). **Source**: Fable audit 2026-11-13, finding ARC-202/QA-002 remedy (c).

## Goal

Make mirror-engine drift unable to land silently: per-language coverage floors over the in-memory engine files, so a semantics change that adds code without adding the matching engine tests fails the gate instead of shipping unpinned.

## Current-state context

The no-codegen SDKs each carry a hand-maintained in-memory engine (~24 files, all above complexity 300) that doubles as the SDK's test harness. Coverage tooling already exists per language (codecov reports all of them; local legs: `make coverage-go`, vitest coverage, pytest-cov, etc.). There are no per-file or per-directory floors — the repo average can pass while an engine file's new branch goes untested.

Current local coverage legs (from the 2026-11-13 coverage work): go-client `make coverage-go` (~82% total), dashboard vitest 89.1%, ts-client, python-client, rust-client have their own legs in the Makefile. The Rust *server* coverage is local-only by policy (CI stays lean per Paul's 2026-10-12 rule) — this enhancement must respect that: CI carries only fast legs.

## Implementation steps

1. **Enumerate the floors.** Target directory-per-language (engine dirs only, not whole packages):
   - `rust-client/src/in_memory/` (cargo tarpaulin or llvm-cov, local leg)
   - `python-client/src/par_rt_db/in_memory/`
   - `ts-client/src/in_memory/`
   - `go-client/inmemory/`
   - `swift-client` InMemory* sources
   - `kotlin-client` inmemory sources
2. **Pick the floor per directory** from measured current coverage minus a small step (e.g. if a dir measures 88%, floor it at 85%). Floors ratchet up later; never ship a floor the current tree fails.
3. **Wire the checks into each language's local coverage target** in the root `Makefile` (e.g. `coverage-go` gains a per-package floor assertion via `go tool cover -func` parsing; vitest/pytest via their threshold configs scoped to the in_memory paths). Where a tool lacks per-path thresholds, add a small parser script under `scripts/` following the existing drift-check script pattern (`env-drift-check.sh` style: read the report, compare, exit nonzero).
4. **Keep CI lean**: CI coverage continues to run fast legs only. The floors live in the same local targets CI already invokes where those legs are fast (go/ts/python/dashboard); rust-client floors run in the local-only coverage target.
5. **Document** the floors and the ratchet policy in the coverage README section (root README Verification Gates or `docs/`).

## Files to touch

- Root `Makefile` coverage targets (per-language).
- Possibly `scripts/` — one new threshold-check script if tooling lacks native per-path floors.
- Language coverage configs (vitest config, `pyproject.toml` coverage settings, go test flags) where scoped thresholds are native.
- README/docs coverage section.

## Verify

- `make checkall` green with floors active.
- Negative test: inject one untested branch into `ts-client/src/in_memory/query.ts` (e.g. an early-return), run the coverage leg, confirm the floor check FAILS, revert.
- Confirm CI remains fast (check the CI workflow has no new slow leg).

## Rollback

Revert the Makefile/config commit; floors are pure gating, no product code touched.

## Acceptance criteria (for the card)

- Every language's engine directory has a coverage floor that the current tree passes.
- The negative test (untested branch fails the floor) demonstrated once.
- No new slow CI leg added.
