# ENH-032 — Extend property-based parity testing to transactions and migrations

## Goal

ENH-027 shipped `server/tests/proptest_parity.rs`: generated schemas, documents and *queries* run on
the real server and the rust in-memory engine with results compared. The write side — transactions
(`insert`/`patch`/`replace`/`delete`/`upsert*`/`patchByQuery`/`deleteByQuery`, with `ValueExpr`
computed fields and step-result shapes) and schema migrations (`RenameField`, `ChangeType`,
`Drop*`, destructive-change detection, `rename_field_refs` surfaces) — is still covered only by
enumerated cases. The audit's top hotspots are exactly those functions (`validate_one` CC 55,
`apply_schema_additive` CC 57, `execute_txn` CC 39, the three `execute_step` mirrors), and QA-002's
concern is a missed name-bearing surface on rename. Generation finds that class for free.

## Current state

- `server/tests/proptest_parity.rs` (ENH-027): generators for schema (1–3 tables, scalar field
  types), documents, filter trees to depth 3, order/collect/count terminals; oracle loop against
  `ddl::push_schema` + the rust engine; `proptest = "1"` dev-dep in `server/Cargo.toml:119`.
- Wire-corpus semantics cases pin specific txn and migrate behaviors on all five runners.
- ARC-004 (core crate) would let the property test call one `apply_patch`/`eval_value_expr` in
  both places; not a prerequisite.

## Task 1: Transaction Generator and Oracle

- **Transaction generator** (`server/tests/proptest/txn.rs` or directly in `server/tests/proptest_parity.rs`):
  - From a generated schema + seeded docs, generate a `Transaction` of 1–8 steps drawn from every mirrored wire `Step` variant: `Insert`, `Patch`, `Replace`, `Delete`, `ExpectVersion`, `ExpectAbsent`, `Upsert`, `PatchByQuery`, `DeleteByQuery`, `Schedule`, `CancelSchedule`, and `Undelete`. Use wire `Upsert` rather than a client-only `upsertByIndex` helper. Include generated filters and computed-field tables so `ValueExpr` evaluation is exercised.
  - Exclude `StartWorkflow` and `CancelWorkflow`: the in-memory harness intentionally returns `Internal` for these unsupported workflow steps, so generating them would only assert identical unsupported behavior rather than parity. If workflow parity becomes supported later, add these variants to the generator and oracle.
  - Patches reference existing ids (from the seed) 80% of the time and random ids 20% (exercises `NOT_FOUND` paths); values respect field types with nulls/missing optionals; strings include unicode and empty.
  - Per-step caps (`server/src/txn.rs` limits) become generator bounds so generated txns are valid by construction; a second, smaller generator deliberately exceeds one cap to assert the same `BAD_REQUEST` on both runners.
- **Transaction oracle**: run the txn on the server (through the committer via the HTTP mutate
  handler or `Committers::mutate`, never `execute_txn` directly) and on the engine; compare the
  `TxnOutcome` step results (normalize ids/timestamps as ENH-027 does) and the post-state (collect
  every table on both, compare ordered by `_id`). Shrinking gives a minimal divergent txn.

## Task 2: Migration Generator and Oracle

- **Migration generator** (`server/tests/proptest/migrate.rs` or directly in `server/tests/proptest_parity.rs`):
  - Start from a schema with name-bearing surfaces populated (indexes, `ownerField`,
    `collaboratorsField`, auto-increment, `authorize` referencing fields, defaults, computed
    exprs referencing fields, soft-delete field).
  - Generate 1–4 migration directives from the implemented generator arms: `RenameField`,
    `ChangeType`, `DropField`, and `DropIndex`, with validity by construction; use additive schema-push
    setup for added fields and indexes, including defaults, rather than inventing `AddField` or
    `AddIndex` migration variants. Include a small "invalid" generator for the destructive-change
    detector.
  - Oracle: server `migrate` (via the admin migrate handler, dry-run and apply) vs the rust engine's
    `SchemaDef::validate`/apply; compare the resulting `SchemaDef` JSON and, after apply, re-run a
    generated query on both to prove data survived. Every renamed field must be absent from the
    serialized schema afterwards (`grep`-style assertion on the JSON) — that is the QA-002
    surface-miss detector.

## Task 3: Counterexample Export, Runtime Budget, CI, and Docs

- **Counterexample export**: on failure, write the minimal case to
  `target/proptest-counterexamples/<name>.json` in the wire-corpus semantics format so it can be
  promoted to a permanent corpus case (document this in `wire-corpus/README.md`).
- **Runtime budget**: default 64 cases per property in `make test`, `PROPTEST_CASES=1000` in a
  weekly CI job (`schedule:` in ci.yml) so the gate stays fast.
- **Docs**: update the `CONTRIBUTING.md` testing section and `FEATURE_MATRIX.md` testing row; record
  ENH-032's supersession of the deleted ENH-027 plan in `CHANGELOG.md` and the property-test module
  documentation.
- **Files to touch**:
  - `server/tests/proptest_parity.rs` → `server/tests/proptest/{mod,query,txn,migrate}.rs` (or keep one file with modules)
  - `server/Cargo.toml` (`[[test]]` if the layout changes; `Dockerfile` stub list accordingly)
  - `wire-corpus/README.md`, `CONTRIBUTING.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`
  - `.github/workflows/ci.yml` (weekly high-case run)

## Verify

- `cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` green with the default case count.
- `PROPTEST_CASES=1000 cargo test --manifest-path server/Cargo.toml --all-features --test main proptest_parity` green locally once before merge.
- Inject a deliberate divergence (e.g. comment out one surface in the engine's `rename_field_refs`) and confirm the migration property fails with a shrunk counterexample; revert.
- A counterexample file is written on failure and is loadable by the semantics corpus runner.
- `make checkall` green; `bash scripts/dockerfile-stub-check.sh` (post ARC-011) green.

## Rollback

Tests only; revert the commit. Promoted corpus cases stay.
