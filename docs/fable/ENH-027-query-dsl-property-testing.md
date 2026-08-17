# ENH-027 — Property-based parity testing for the query DSL

## Goal

Add generative testing that the hand-written suites and the fixed corpora structurally cannot
provide: randomly generated schemas/documents/queries executed against **both** the real server
(Postgres) and the rust-client in-memory engine, asserting identical results. Divergence found by
generation is exactly the mirror-drift class the audit (ARC-201) identifies as the repo's top risk,
caught with zero per-case authoring cost.

## Current state

- Strong example-based nets exist (client in-memory suites, golden-vector wire corpus, and — if
  ENH-023 lands first — a shared semantics corpus). All are enumerated cases; none explore the
  input space.
- The server test harness already provides per-test isolated databases (`wrap_test_db` RAII) and
  a schema-push path (`ddl::push_schema`); integration tests run against the dev Postgres on
  127.0.0.1:55434 (`make dev-db-up`).
- `proptest` is the standard Rust choice; check `Cargo.toml` workspace deps first — add it as a
  workspace dev-dependency.
- **Sequence after ARC-201** (decomposed engine) and ideally after ENH-023 (shared fixture
  vocabulary informs the generators). Server-vs-rust-engine is the highest-leverage pair; the TS
  and Python engines are covered transitively by ENH-023's shared corpus (optionally extended
  later by dumping proptest counterexamples into `wire-corpus/semantics/`).

## Implementation

1. **Generators** (new `server/tests/proptest_parity.rs`, or a `server/tests/proptest/` module):
   - Schema: 1–3 tables, 2–6 fields drawn from the scalar index-typable field types (string, int64,
     float, bool, timestamp; skip vector/FTS in v1), a subset indexed, optional soft-delete/TTL off
     in v1.
   - Documents: 5–50 rows per table with nulls/missing optionals at meaningful frequency; int64
     boundary values; strings including empty and unicode.
   - Queries: filter trees to depth 3 over the generated fields (eq/neq/lt/lte/gt/gte/in/and/or/not
     — whatever the DSL's operator set actually is; enumerate from the `FilterExpr` type),
     order-by an indexed field + `_id` tiebreak, collect and count terminals in v1 (paginate in v2 —
     cursor equivalence needs the tiebreak reasoning done carefully).
2. **Oracle loop**: for each generated case — create db, push schema, insert docs (server), load the
   same schema+docs into the rust in-memory engine, run the query on both, compare row sets
   (normalize `_id`/`_creationTime` ordering the same way ENH-023 does; compare as ordered lists
   when an order-by is present, as multisets otherwise).
3. **Budget**: cap cases per run (e.g. `PROPTEST_CASES=64` default via config) so `make checkall`
   stays fast; mark the test `#[ignore]`-gated behind an env opt-in ONLY if measured runtime
   exceeds ~30s — prefer keeping it in the default suite at a small case count. Nightly/CI can
   raise the count via env.
4. **Regression persistence**: commit `proptest-regressions/` files (proptest's native mechanism)
   so found counterexamples re-run forever. Document in the test header: when proptest finds a
   divergence, ALSO add the minimized case to `wire-corpus/semantics/` so all three clients get it.
5. **Float caveat**: compare floats bitwise-as-stored (both sides round-trip through the same JSON
   representation); if aggregate terminals are added later, use epsilon comparison and document it.

## Files to touch

- `Cargo.toml` (workspace dev-dep `proptest`), `server/Cargo.toml`
- `server/tests/proptest_parity.rs` (new; may reuse rust-client's in-memory engine via the existing
  dev-dependency path — check whether `server` can dev-depend on `par-rt-db-client`; it's in the
  same workspace, so `par-rt-db-client = { path = "../rust-client" }` as dev-dependency)
- `proptest-regressions/` (committed as found)
- `CONTRIBUTING.md` — one bullet on the counterexample-to-corpus rule

## Verification

- `make checkall` green with the property test running at the default case count (measure runtime;
  record it in the PR/commit message).
- Mutation check: invert one comparison in the in-memory engine's filter evaluator — the property
  test finds a counterexample within the default budget; revert.
- Dev-db hygiene: the loop's per-case databases clean up via the RAII harness (`make dev-db-clean`
  shows no growing leak after a full run).

## Rollback

Additive test-only change: remove the test file and the dev-dependency. No production code paths.
