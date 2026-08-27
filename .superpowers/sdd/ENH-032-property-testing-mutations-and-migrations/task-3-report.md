
## Review-fix round

- Transaction envelopes now label seeded docs with `$id: "seed"`, rewrite `seed-id` strings to `$idRef`, and normalize both minted `id` and `scheduleId` values.
- Output resolution is explicit: `CARGO_MANIFEST_DIR/../target/proptest-counterexamples` is the repository-root `target/proptest-counterexamples` directory.
- `semantics_corpus_test::run_case` is visible to sibling test modules for replay checks.

## Final review-fix completion

- Both failure closures now replay the exported envelope through the server-side corpus path to capture actual expectations (`semantics_corpus_test::capture_case_expect`). Transaction exports carry server step results or an error envelope plus a captured post-state `then` query; migration exports use the actual additive baseline schema and capture `applied`, the derived migration schema, per-directive reports, and the post-migration query result for apply/other phases (preview envelopes omit `then` because dry-run rolls back).
- Runtime IDs are corpus-expressible: only the first seeded document is `$id`-labeled, `seed-id` operands render as `{"$idRef": "seed"}`, and corpus-inexpressible `cancelSchedule` steps (their schedule is minted by a property-harness preamble; corpus `$idRef` labels come only from seed inserts) are omitted with the count and reason recorded in `$comment` — the property itself still exercises them.
- Syntax-only envelope checks were replaced by filesystem write/read roundtrips plus actual `semantics_corpus_test::run_case` execution using nonempty representative cases with `$idRef` operands (`txn_counterexample_envelope_replays_through_corpus_runner`, `migrate_counterexample_envelope_replays_through_corpus_runner`).
- `wire-corpus/README.md`, `CONTRIBUTING.md`, and `CHANGELOG.md` document the repository-root output path and the real `RTDB_COUNTEREXAMPLE` loader command; the ENH-027 authority/supersession note remains explicit and accurate.
- Every parity property now uses `case_count()` directly (the two `min(16)` caps are gone): 64 cases by default, 1000 under `PROPTEST_CASES=1000`. The weekly CI cron and the 1000-case `proptest-weekly` job are preserved.

## Verification

- `cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` — passed, 8 tests (unsuppressed default case count).
- Focused loader test, from the generated file on disk: `RTDB_COUNTEREXAMPLE=/Users/probello/Repos/par-rt-db/.worktrees/enh-032/target/proptest-counterexamples/txn-counterexample-replay-check.json cargo test --manifest-path server/Cargo.toml --all-features --test main semantics_corpus_counterexample -- --ignored` — passed.
- `actionlint` — passed with no findings; `git diff --check` — passed.

## All-features alignment verification

- Updated the weekly CI property command and contributor examples to include `--all-features`, matching the server Makefile/baseline invocation.
- `cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` — passed, 8 tests.
- `actionlint .github/workflows/ci.yml` — passed with no findings.
- Command consistency sweep found the weekly CI command, loader rustdoc, wire-corpus README, and contributor commands all carrying `--all-features --test main`.

## All-features necessity (proven, not stylistic)

- Without `--all-features`, the pre-alignment weekly-CI command fails to COMPILE the `main` test binary, reproducible at HEAD: `cargo test --manifest-path server/Cargo.toml --test main semantics_corpus_counterexample --no-run` → `error: function 'ws_mutate_reply' is never used` (`server/tests/multi_instance_subs_test.rs:95`), `-D dead-code implied by -D warnings`.
- Cause chain: `ws_mutate_reply`'s only caller is `dropped_replies_commit_once_and_replay_after_takeover`, gated `#[cfg(feature = "test-support")]`; without `--all-features` the caller is compiled out, the helper becomes dead code, and `server/Cargo.toml [lints.rust] warnings = "deny"` (Cargo lints table — enforced in CI too, not a local env artifact) turns it into a hard error. `--all-features` compiles the caller, so the aligned command builds cleanly.

## Task 3 final verification round

- `cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` — passed, 8 tests, unsuppressed default case count; finished in 6.49s.
- `PROPTEST_CASES=1000 cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` — passed, 8 tests; finished in 103.80s.
- Deliberate divergence proof was performed (not merely asserted): temporarily disabled the in-memory engine's `auto_increment_field` rename, then `cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity::migrate_dsl_server_vs_in_memory_parity` failed with a shrunk one-directive `ticket` → `number` counterexample and wrote `target/proptest-counterexamples/migrate-dsl-server-vs-in-memory-parity.json`; the temporary edit was restored.
- The generated divergence envelope was replayed successfully with `RTDB_COUNTEREXAMPLE=/Users/probello/Repos/par-rt-db/.worktrees/enh-032/target/proptest-counterexamples/migrate-dsl-server-vs-in-memory-parity.json cargo test --manifest-path server/Cargo.toml --all-features --test main semantics_corpus_counterexample -- --ignored` (1 passed).
- `actionlint .github/workflows/ci.yml` — passed; `bash scripts/dockerfile-stub-check.sh` — passed (`8 declared [[test]]/[[bench]] targets, all stubbed`).
- `make dev-db-up` could not start this worktree's Postgres because port `127.0.0.1:55434` was already allocated by `enh-031-publish-pipeline-postgres-1`; parity gates used that compatible existing listener without stopping the sibling container.
- `make checkall` — exit 2 at `ts-client-doc`: `cd ts-client && bun run doc` failed because `docs-toolchain` has no `typedoc` script (`error: Script not found "typedoc"`). The command reached TypeScript documentation generation after Rust checks; no claim of a green `make checkall` is made.
- Deliberate divergence was restored before commit; no generated `target/` artifacts are committed.

## ENH032-H1: dry-run preview envelopes must replay against the rolled-back database

- Defect: preview (`dryRun=true`) migration counterexamples carried the post-migration `then` query, which the corpus runner resolves through the derived schema while the dry-run transaction has rolled back — the envelope is not replayable against its own rolled-back state.
- Fix: preview envelopes omit `then` and record the reason in `$comment`; apply/other phases keep the post-migration `then` unchanged.
- Regression test `migrate_counterexample_preview_rollback_replays` (owner→account rename, `query_kind=1` by_added index query) — failed pre-fix at the `then`-present assertion, passes post-fix with full corpus-runner replay.
