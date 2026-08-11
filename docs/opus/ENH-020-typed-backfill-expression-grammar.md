# ENH-020 — Typed backfill expression grammar (safe destructive migrations)

> **Source**: kanban card `[ENH-020]`, project `par-rt-db`. Derived from the 2026-08-09 Opus audit.
> **Impact**: high · **Effort**: large · **Breaking**: yes (migrate directive shape)
> **Closes**: audit finding `SEC-107` — coordinate, do not duplicate.

## Goal

Replace `evalExpr`'s free-text SQL with a **closed, typed expression grammar**, then build on it to
make genuinely destructive migrations (rename, retype, drop-with-backfill) safe and declarative.

This is two wins from one piece of work: it removes the audit's High-severity SQL-execution hole, and
it converts `FEATURE_MATRIX.md` §5's "schema-change safety (backfill vs additive-only)" from a
documented *tradeoff* into a shipped capability.

## Current state

- Schema changes are **additive-only** (`server/src/schema.rs`, `ddl.rs`). `FEATURE_MATRIX.md` §5
  lists this as a deliberate operating point, not a defect.
- `server/src/migrate.rs` (1,647 lines) executes an ordered `Directive` list inside the committer's
  serialized turn (`CommitterRequest::RunMigrate` → `handle_migrate`), publishing through the same
  tap sites as every other durable write.
- `evalExpr` is the escape hatch, and it is the audit's `SEC-107`: `apply_eval_expr`
  (`migrate.rs:876-880`) interpolates client SQL text **unbound** into
  `UPDATE … SET doc = jsonb_set(doc, …, to_jsonb((EXPR))) WHERE COND`, guarded only by
  `has_sql_violation` (`:360-377`) — a substring denylist with **two verified bypasses**: the
  ` FROM `/` JOIN `/` INTO ` entries require a literal space on both sides (a newline defeats them),
  and `SELECT` is not on the list at all.
- The machinery for a closed grammar **already exists**: `FilterExpr` is a typed, validated,
  parameter-bound predicate DSL used by `.filter()`, per-row `authorize`, partial-index `where`
  predicates, and the by-query txn steps. `compile_scan_where` composes it safely today.

## Implementation

> **Sequencing**: the `SEC-107` `evalExpr` injection concern offers two paths — (A) closed grammar,
> (B) least-privilege Postgres role. **This enhancement IS path (A).** If the remediation agent already
> shipped path (B), this work supersedes it: remove the role indirection as part of Stage 1 rather than
> layering a grammar on top of it. If neither has shipped, do this and mark `SEC-107` resolved by it.

### Stage 1 — The expression grammar (closes SEC-107)

Define `ValueExpr` in `server/src/protocol.rs`, mirroring `FilterExpr`'s shape and serde conventions:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum ValueExpr {
    Field { field: String },              // a declared field on THIS table only
    Literal { value: serde_json::Value },
    Concat { parts: Vec<ValueExpr> },
    Add { left: Box<ValueExpr>, right: Box<ValueExpr> },   // + Sub/Mul/Div
    Coalesce { parts: Vec<ValueExpr> },
    Lower { value: Box<ValueExpr> },      // + Upper/Trim
    Cast { value: Box<ValueExpr>, to: FieldType },
    Now,
    Case { when: Vec<(FilterExpr, ValueExpr)>, otherwise: Box<ValueExpr> },
}
```

Compile it with **every literal bound as `$n`** and every field name resolved through the
`TableDef` lookup that already errors on an unknown field — the same discipline the read path uses.
There is deliberately **no** subquery node, no function-call-by-name node, and no raw-SQL escape.

Then:

1. Replace `EvalExpr`'s `expr: String` with `expr: ValueExpr` and its `where: String` with
   `where: FilterExpr` (the predicate half needs no new type — reuse `compile_scan_where`).
2. **Delete `has_sql_violation` entirely.** Leaving it invites the next reader to believe a denylist
   is a control. Delete the "blast-radius scoping" comment with it.
3. Bump the migrate directive wire shape and mirror `ValueExpr` into all four protocol files
   byte-identically (`server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`,
   `python-client/src/par_rt_db/wire.py`), plus builder ergonomics in each client's migration DSL.

**This is a wire-breaking change.** Existing `evalExpr` directives with string bodies will be rejected.
Document it prominently in `CHANGELOG.md` and provide the rewrite mapping for the common cases in
`docs/superpowers/specs/2026-07-31-schema-migration-backfill.md`.

### Stage 2 — Destructive directives built on the grammar

With a safe expression type, add three directives that were previously impossible to offer:

| Directive | Semantics |
|---|---|
| `renameField { table, from, to }` | Add the new typed column, backfill `to = ValueExpr::Field(from)`, reconcile the jsonb `doc`, drop the old column. Rejected if `to` already exists. |
| `retypeField { table, field, to, using: ValueExpr }` | Add a shadow column of the new type, backfill via `using`, swap, drop. Rejected when `using` cannot produce `to`. |
| `dropField { table, field, confirm }` | Typed `confirm == field` guard (mirroring `delete-db`'s and `restoreSchema`'s existing confirm pattern). Removes the typed column and strips the key from `doc`. |

All three run inside `handle_migrate`'s serialized committer turn and **must publish through
`publish_taps`** like every other durable write — the op-feed / audit / webhook / subscription
contract is the repo's hardest invariant. `handle_restore_schema` (`committer.rs:1053`) is the
reference for a DDL-shaped tap call with `docop_taps=false`; a *backfill* writes documents, so it
needs `docop_taps=true` and real `DocOp` entries.

**Bound the row count.** A backfill over a large table inside the serialized committer turn blocks
every write to that database — the same failure `SEC-104` describes for by-query steps. Reuse or
extend `MAX_BY_QUERY_ROWS`-style batching: process in chunks with a progress row in
`schema_history`, or reject a backfill whose match set exceeds a cap and require the operator to
narrow it.

### Stage 3 — Surfaces

- **Schema history**: `schema_history.rs` already snapshots on push/migrate/restore. Confirm the new
  directives capture bracketing snapshots so `POST /admin/db/{db}/schema/restore` can walk one back.
  A destructive migration that is not reversible from the console defeats ENH-013.
- **Dashboard**: extend `MigratePage.tsx` with the three new directive forms and a typed-confirm
  input for `dropField`.
- **CLI**: `rtdb migrate --db <db> --file <directives.json>` — the operator path for a scripted migration.

## Files to touch

- `server/src/protocol.rs` — `ValueExpr`, revised `EvalExpr`, three new directives
- `server/src/migrate.rs` — compile `ValueExpr`; delete `has_sql_violation`; the three handlers
- `server/src/committer.rs` — tap-site wiring for backfill `DocOp`s
- `server/src/schema.rs`, `ddl.rs` — column add/swap/drop mechanics
- `server/src/schema_history.rs` — bracketing snapshots
- `ts-client/src/protocol.ts` + migration builder; `rust-client/src/wire.rs` + `schema.rs`;
  `python-client/src/par_rt_db/{wire.py,migration.py}`
- All three in-memory harnesses (`ts-client/src/in_memory.ts`, `rust-client/src/in_memory.rs`,
  `python-client/.../in_memory.py`) — `apply_migration_directive` must learn the new directives
- `wire-corpus/wire-corpus.json` — serialization cases for `ValueExpr` and each directive
- `dashboard/src/pages/MigratePage.tsx`; `cli/src/main.rs`
- `docs/superpowers/specs/2026-07-31-schema-migration-backfill.md`, `README.md`, `FEATURE_MATRIX.md`
  (§5 — move schema-change safety out of "genuine tradeoffs"), `CHANGELOG.md`

## Verify

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
make -C /Users/probello/Repos/par-rt-db ts-client-build
make -C /Users/probello/Repos/par-rt-db checkall > /tmp/enh020.log 2>&1; echo "EXIT=$?" >> /tmp/enh020.log
grep '^EXIT=' /tmp/enh020.log
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml migrate
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml schema_history
cd /Users/probello/Repos/par-rt-db/ts-client && bunx vitest run
cd /Users/probello/Repos/par-rt-db/python-client && uv run pytest -q
cargo test --manifest-path /Users/probello/Repos/par-rt-db/rust-client/Cargo.toml --all-features
```

**Acceptance criteria** (mirror these onto the card):
1. `make checkall` green.
2. `has_sql_violation` is **unreachable from the `ValueExpr` path**, and deleted outright once the
   legacy `expr: String` form is removed. (Under the recommended dual-accept rollout it survives one
   deprecation cycle guarding *only* the legacy path — assert unreachability from the new path now, and
   `grep -c has_sql_violation server/src/migrate.rs` = 0 at the end of that cycle.)
3. Both `SEC-107` bypasses are regression-tested and rejected: a newline before `FROM`, and a bare
   `SELECT current_setting(...)`. Under the new grammar these should fail to *deserialize*, not merely
   fail a filter — assert the error is a parse/validation error.
4. Every `ValueExpr` literal reaches SQL as a bound `$n` — a test asserting the generated SQL contains
   no interpolated literal from a hostile input string.
5. `renameField`, `retypeField`, and `dropField` each publish `DocOp`s through `publish_taps` — proven
   by an op-feed/audit assertion, not by reading the code.
6. A destructive migration is reversible via `POST /admin/db/{db}/schema/restore`.
7. `ValueExpr` is byte-identical across all four protocol files, with wire-corpus cases.
8. All three in-memory harnesses apply the new directives.

## Rollback

**Stage 1 is wire-breaking and is the hard part to roll back** — once clients emit `ValueExpr`, a
server revert rejects them. Mitigate by shipping Stage 1 as a **dual-accept** release: accept both the
legacy `expr: String` (with `has_sql_violation` retained *only* for that path) and the new
`ValueExpr`, deprecate the string form for one release, then delete it. That turns an irreversible
cutover into two reversible steps and is strongly recommended.

Stage 2's directives are purely additive — reverting removes capability without breaking existing
schemas. Stage 3 is UI/CLI only.

## Risks

- **Committer blocking.** A backfill runs inside the serialized turn. Batch it and cap it, or one
  migration on a large table is a tenant-wide write outage.
- **Scope creep in the grammar.** Every node added is surface to validate and mirror four times. Ship
  the minimum set above; resist a general function-call node, which reintroduces exactly the
  unbounded-capability problem the grammar exists to remove.
- **`retypeField` data loss.** A narrowing cast (text → int) silently drops non-conforming rows unless
  guarded. Require the `using` expression to be total (wrap in `Coalesce`) or fail the migration on the
  first non-castable row — decide explicitly and test it.
- **Four-client mirror cost is real** — `ValueExpr` lands in three protocol files, three migration
  builders, and three in-memory harnesses. Budget for it; `ARC-105`/`QA-103` document what happens when
  a new terminal-shaped type is added without corpus coverage.
