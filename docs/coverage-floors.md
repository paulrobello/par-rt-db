# Coverage floors — client-mirror in-memory engine directories (ENH-054)

These floors exist so mirror-engine drift cannot land silently: a semantics
change that adds engine code without the matching engine tests fails its
coverage leg instead of shipping unpinned. Floors were set on 2026-11-14 from
measured coverage on the current tree minus a small ratchet step. They apply to
the engine directories only, never the packages as a whole.

## The floors

| Engine directory | Measured | Floor | Where enforced |
| --- | --- | --- | --- |
| `go-client/inmemory/` | 80.4% statements | **78.0** | `coverage-go` (fast leg — CI `coverage-weekly` enforces it) |
| `python-client/src/par_rt_db/in_memory/` | 91.3% lines | **89.0** | `coverage-python` (fast leg — CI `coverage-weekly` enforces it) |
| `ts-client/src/in_memory/` | 92.7% lines | **90.0** | vitest native glob threshold in `ts-client/vitest.config.ts` (runs on every `coverage-ts`, including CI) |
| `kotlin-client` `dev.parrtdb.inmemory` | 92.7% lines | **90.0** | `coverage-kotlin` (local-only) |
| swift-client `InMemory*` sources | 85.7% lines | **83.0** | `coverage-swift` (local-only) |
| `rust-client/src/in_memory/` | 89.5% lines | **87.0** | `coverage-rust` (local-only; floor check reads the lcov llvm-cov already writes) |
| dashboard `src/**` | (whole-package vitest) | none | report-only (unchanged) |

## Ratchet policy

Floors are ratchet-only: they move up after genuine test improvements and are
never lowered to get a red leg green. To ratchet a floor, run the relevant
coverage leg, read the engine directory's number from the floor-check output,
and raise the floor to the new measured value minus a small step. Lowering or
bypassing a floor in a change that touches engine code is treated like any
failing gate: not allowed.

## How to run each leg

```bash
make coverage-go      # go test -coverprofile + floor check (fast, CI-enforced)
make coverage-python  # pytest-cov lcov + floor check (fast, CI-enforced)
make coverage-ts      # vitest --coverage (fast, CI-enforced; threshold in vitest.config.ts)
make coverage-dashboard   # report-only, unchanged
make coverage-kotlin  # gradle jacocoTestReport + floor check (local-only)
make coverage-swift   # swift test --enable-code-coverage + llvm-cov + floor check (local-only)
make coverage-rust    # llvm-cov workspace run + rust floor check (local-only)
```

The fast legs (go, python, ts) run inside CI's `coverage-weekly` job via
`make coverage-fast`; CI carries no new slow leg — kotlin and swift floors are
local-only, and the rust floor activates only inside the existing local-only
`coverage-rust` target. This preserves the CI-lean policy (no long-running CI
suites, 2026-10-12).

## The floor-check script

`scripts/coverage-floor-check.sh <format> <profile> <pattern> <floor>` reads a
coverage report and exits nonzero when the aggregate coverage of files matching
`pattern` is below `floor`. Formats: `lcov` (pytest-cov / llvm-cov traces),
`go` (go coverprofile), `kover` (JaCoCo/Kover XML), `llvmcov` (`llvm-cov export
--summary-only` JSON). A pattern that matches nothing fails loudly rather than
passing an empty match.

## Rust floor (active)

The rust floor activated on 2026-11-14, the day after the other engines: the
first measured `make coverage-rust` run gave `rust-client/src/in_memory/` **89.5%
lines (4662/5208)**, so `ENGINE_FLOOR_RUST := 87.0` and the `coverage-rust`
recipe now ends with the floor-check line parsing `target/coverage/rust.lcov`
(the same lcov llvm-cov already writes — no second pass). With this, every
mirror engine directory carries a ratchet floor.

## History

- 2026-11-14 — floors created (ENH-054). Measured: go 80.4% statements
  (3635/4522), python 91.3% lines (3308/3622), ts 92.7% lines (2360/2547),
  kotlin 92.7% lines (2662/2872), swift 85.7% (5117/5970). Rust activation
  same day: measured 89.5% lines (4662/5208), floor set to 87.0.
