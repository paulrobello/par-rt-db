# ENH-023 — Behavioral-semantics corpus for the in-memory engines

## Goal

Extend the existing `wire-corpus/` infrastructure with **behavioral** fixtures — input state +
query/txn + expected result — consumed by the server test harness and all three client in-memory
engines, so cross-implementation parity is asserted from shared data rather than re-derived logic.
Today `golden-vector.json` catches *wire* drift (serde shape); nothing structurally catches
*behavior* drift (a filter evaluating differently in the Python engine than on the server), which
is exactly the failure class the memory note "wire-corpus skip_serializing_if drift" recorded.

## Current state

- `wire-corpus/golden-vector.json` + parity tests exist in all four packages:
  `server/tests/golden_vector_test.rs`, `ts-client/tests/wire-corpus.test.ts`,
  `rust-client/tests/wire_corpus.rs`, `python-client/tests/test_golden_vector.py`.
- The three in-memory engines (`ts-client/src/in_memory.ts`, `python-client/src/par_rt_db/in_memory.py`,
  `rust-client/src/in_memory/`) each have large hand-written unit suites asserting behavior
  independently — thorough but unshared, so a semantic gap in one suite is invisible.
- Audit finding ARC-201 (2026-08-16) decomposes these engines; this enhancement is the follow-on
  that locks their semantics together. **Sequence after ARC-201 lands** (the decomposed engines
  are easier to drive from a fixture runner).

## Implementation

1. **Fixture schema.** Add `wire-corpus/semantics/` holding JSON case files, each:
   `{ "name", "schema": <pushed schema object>, "seed": [<docs per table>], "op": {"query": ...} | {"txn": ...}, "expect": <result rows/step-results> | {"error": {"code": ...}} }`.
   Keep values within the DSL's typed universe (int64 as the corpus already encodes them).
   Deterministic ids/timestamps only — no generated values in `expect` (or mask them via a
   `"normalize": ["_id", "_creationTime"]` list the runners apply before comparing).
2. **Seed the corpus from real semantics** (~30 cases to start): one per query terminal
   (collect/paginate/count/distinct/aggregate/get + search variants where seedable), filter
   operators incl. null/missing-field edges, order+cursor pagination, a multi-step txn with
   per-step results, upsert-multiple-matches, soft-delete visibility, TTL-expired-row visibility,
   defaults application, and 3–4 error cases (bad field, type mismatch).
3. **Runners.** One table-driven test per package:
   - Server: new `server/tests/semantics_corpus_test.rs` — create a per-test db (use `wrap_test_db`,
     NOT `fresh_db` — the latter seeds the kanban fixture), push `schema`, insert `seed`, run `op`
     via the normal execute path, compare to `expect`.
   - Each client: drive its in-memory engine with the same schema/seed/op and compare.
4. **Drift gate.** Each runner iterates every file in `wire-corpus/semantics/` so adding a case is
   data-only. A case one runner cannot express yet fails loudly (no silent skip); allow an explicit
   per-case `"skip": {"python": "reason"}` field so gaps are visible in the fixture itself.
5. **Authoring rule.** Document in `wire-corpus/README` (or create it): every new server semantic
   ships with at least one semantics case in the same change — mirroring the existing golden-vector
   convention.

## Files to touch

- `wire-corpus/semantics/*.json` (new), `wire-corpus/README.md` (new or extended)
- `server/tests/semantics_corpus_test.rs` (new)
- `ts-client/tests/semantics-corpus.test.ts` (new)
- `rust-client/tests/semantics_corpus.rs` (new)
- `python-client/tests/test_semantics_corpus.py` (new)
- `CONTRIBUTING.md` — one bullet pointing at the authoring rule

## Verification

- `make checkall` passes with the four new runners active.
- Mutation check: temporarily flip one comparison operator in one client engine — the corpus test
  for that client fails; revert.
- All four runners execute the same case count (assert count parity in each runner, e.g. expect
  N cases loaded; a runner seeing fewer files fails).

## Rollback

Pure additive test infrastructure: delete the four runner files and `wire-corpus/semantics/` to
revert. No production code paths are touched.
