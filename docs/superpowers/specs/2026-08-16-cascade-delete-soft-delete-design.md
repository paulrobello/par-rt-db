# Cascade delete (`onDelete`) + soft delete (`softDelete`) — FM-33

Date: 2026-08-16 · Card: `[FM-33] Cascade delete (FK) + soft delete` (`01a0034797df7a30b161187f2b797fb4`) · FEATURE_MATRIX #33

Two separable halves that interact at one seam (a cascade child table may itself be
`softDelete`). Both are beyond-parity features: Convex makes apps hand-roll both.

**Design deviation from the card sketch, noted up front:** the card sketches `ref: "tableName"`
compiling to a real Postgres FK. The `ref` half already exists — `FieldType::Id { table }` has
carried the reference target on the wire since the first schema push, so FM-33(a) only adds an
additive `onDelete` action to that variant. And the cascade executes **app-level inside
`execute_txn`, not as a SQL `ON DELETE CASCADE`**: Postgres cascaded deletes are invisible to
`DELETE … RETURNING` (they happen inside the referential trigger machinery), so a SQL FK would
leave the committer unable to record per-row `DocOp`/`WriteSet` entries — criterion 1's hard
part — without trigger-based side-table capture. Walking children in `execute_txn` and deleting
them through the existing delete path makes every cascaded row a first-class `DocOp` **by
construction**. The single-writer invariant already guarantees no write bypasses `execute_txn`,
so a SQL FK adds no enforcement this architecture lacks (the `psql` escape hatch is manual by
definition).

## (a) Cascade delete

### Wire shape (additive)

```jsonc
// field on the CHILD table, top-level only
"projectId": { "type": "id", "table": "projects", "onDelete": "cascade" }
// setNull requires the field to be optional (the null must produce a valid doc):
"projectId": { "type": "optional", "inner": { "type": "id", "table": "projects", "onDelete": "setNull" } }
```

- `OnDeleteAction { Cascade, Restrict, SetNull }`, `rename_all = "camelCase"` → wire
  `cascade` | `restrict` | `setNull`; `#[serde(default, skip_serializing_if = "Option::is_none",
  rename = "onDelete")]` on the `Id` variant — existing schemas deserialize unchanged.
- `TableDef.soft_delete: bool` (see half (b)) — same additive pattern, `skip_serializing_if`
  when false.

### Push validation (`validate_structure`)

An `onDelete` is legal only on a **top-level** field of the table, in one of two shapes:
`Id { table, on_delete: Some(_) }` or `Optional { inner: Id { table, on_delete: Some(_) } }`
(nested deeper — union/object/array — is rejected: there is no well-defined "the ref field" to
index or null). Additional rules, mirroring the ttl precedent:

- the referenced `table` must be declared in the same schema;
- the field must have a **single-field, non-unique, non-partial btree index** on it (the cascade
  lookup `WHERE f_<field> = $1` must be an index scan; a partial `where` could hide children
  from the lookup and orphan them);
- `setNull` additionally requires the `Optional` wrapper.

Self-reference (child table == parent table, e.g. reply threads) is legal; runtime cycle guard
below.

### Execution (`txn.rs`)

Cascade expands **inside the initiating step, in the same sqlx tx**, before the parent row's
delete lands:

- **Trigger sites**: `step_delete` and `step_delete_by_query` (per matched row). A soft delete
  (half b) is *not* a trigger — a soft-deleted parent still physically exists, so its children
  are untouched; cascades fire only on **hard** row deletion. The TTL reaper hard-deletes, so it
  cascades too (see below).
- **Child walk**: for every table with a top-level ref targeting the deleted row's table,
  `SELECT "id" FROM t_child WHERE f_<field> = $1` (+ `AND "deleted_at" IS NULL` when the child
  table is `softDelete` — invisible children neither cascade-block nor get touched). Children
  first (recursively — the child's own delete re-enters the walk), parent last. Order inside one
  atomic tx is not correctness-relevant; depth-first keeps the visited set simple.
- **`cascade`**: delete each child through the same soft/hard delete machinery (a `softDelete`
  child table gets its `deleted_at` stamp; everything else a row delete), recording
  `write_set.touch(table, id, OpKind::Delete)` + `capture_doc(after = None)` per row — so
  subscriptions, op-feed, audit, and webhooks fire per cascaded row through the existing
  `publish_taps`. Cascaded rows run **bypass** (no per-row owner check): FK semantics must be
  deterministic from the schema, not from row ownership — an owner-mismatch mid-cascade would
  abort the whole txn and turn referential integrity into "restrict-by-ownership".
- **`restrict`**: same lookup, `LIMIT 1`; a visible hit → `RtDbError::conflict` (409) naming the
  child table/field, aborting the txn atomically.
- **`setNull`**: per visible child, `UPDATE` clearing the typed column (`f_<field> = NULL`) and
  removing the key from the `doc` jsonb (`doc - 'field'`, matching the patch-null rule for
  optional fields), bumping `version`. Recorded as a patch-shaped `DocOp`
  (`capture_doc(after = Some(new_doc))`) so content-bearing subscriptions re-run.
- **Cycle guard**: a `visited: HashSet<(String, String)>` of `(table, id)` on the recursion
  stack — a self-referential cycle terminates instead of looping.
- **Bound**: `MAX_CASCADE_ROWS` (10_000, const) rows per initiating delete step; over →
  `conflict`, txn aborts. Postgres FKs are unbounded; this is cheap insurance consistent with
  the admin `RTDB_MAX_AFFECTED_DOCS` philosophy. Cascades are not `Step`s, so the admin
  step-count cap does not see them — this is their bound.
- **Reaper interaction**: `handle_reaper` keeps its bulk `DELETE … WHERE f_ttl < now() LIMIT N`
  only when **no** table in the schema declares an `onDelete` ref targeting the reaped table;
  when one does, it switches to select-ids-then-delete-row-by-row through the cascade path for
  those rows (correctness over throughput; still one committer turn, still batch-bounded).

## (b) Soft delete

### Wire shape

`TableDef.soft_delete: bool`, wire key `softDelete`, omitted when false — additive, existing
schemas unchanged.

### Physical storage

A real `deleted_at timestamptz NULL` column (`ADD COLUMN IF NOT EXISTS` on create and on
flag-add, the `apply_schema_additive` pattern at ddl.rs ~310). It is **never merged into
client-visible docs** — soft-deleted rows are filtered everywhere, so there is nothing to
surface; `version` bumps on both delete and undelete so any stale client copy fails OCC.

### Write path (`txn.rs`)

- `step_delete` / `step_delete_by_query` on a `softDelete` table become
  `UPDATE … SET deleted_at = now(), version = version + 1 WHERE id = $1 AND deleted_at IS NULL`
  (0 rows ⇒ `NotFound`, matching today's hard-delete miss). The `WriteSet` entry stays
  **delete-shaped** (`OpKind::Delete`, `capture_doc(after = None)`) — from every subscriber's
  perspective the doc is gone, so `fan_out` re-runs and the doc vanishes from results; op-feed /
  audit / webhooks see a delete.
- `Upsert`'s eq lookup filters soft-deleted rows (below), so upserting a soft-deleted key
  **inserts a fresh row** — the right semantics, and the unique-index exclusion (below) means no
  conflict.
- **Undelete**: new `Step::Undelete { table, id }` (wire tag `"undelete"`, camelCase `op` tag
  like every step). `UPDATE … SET deleted_at = NULL, version = version + 1 WHERE id = $1` —
  `NotFound` if the row is absent; **idempotent Ok** if present and not soft-deleted. Patch-shaped
  `DocOp` (the doc re-appears). This is the restore story: declarative, atomic with other steps,
  mirrors across clients like any step.
- **TTL reaper always hard-deletes** (even on a `softDelete` table): the reaper is the purge
  mechanism; a table declaring both gets soft deletes from clients and physical expiry from the
  reaper. Documented, deliberate.

### Read path (`query.rs`)

`deleted_at IS NULL` is injected as a literal (no binds) at every composition point:

- `compile_scan_where` (query.rs ~2074) — covers index/take/collect/unique/count/distinct/
  aggregate/paginate/first and the db-side `filter()` compose for free, plus the owner/
  authorize predicates which AND into the same accumulator;
- `compile_point_read` (the `get(id)` arm) — a soft-deleted id returns `Doc(None)`, the same
  silent-miss as an owner-mismatched row;
- `search` / `vectorSearch` / `hybridSearch` WHERE construction — same literal;
- `eq_lookup` (txn.rs ~992) — soft-deleted rows are absent to `ExpectAbsent` and `Upsert`
  (the existing per-user visibility precedent);
- `execute_query` gains an internal `include_deleted: bool` parameter (default `false` at every
  client-facing call site) so the **admin-only** document browser can see soft-deleted rows
  (`POST /admin/db/{db}/query` with `includeDeleted: true`). It is deliberately NOT a field on
  the wire `Query` — a client-settable flag would be a soft-delete bypass; only the admin route
  passes `true`.

### Unique indexes (`ddl.rs` ~440-492)

On a `softDelete` table, every **unique** index's partial predicate gains
`AND "deleted_at" IS NULL` (a declared `where` composes with it; a bare unique index becomes
`WHERE "deleted_at" IS NULL`). Soft-deleted rows must not conflict — that is half the point of
soft delete. Non-unique indexes are untouched (their `WHERE` is scan shaping, not correctness).
Adding `softDelete` to an existing table is additive by the declared-schema diff (fields +
indexes unchanged), but the physical unique indexes must be **rebuilt** in the same push
(`DROP INDEX IF EXISTS` + re-`CREATE` with the widened predicate) — `apply_schema_additive`
gains this targeted rebuild; the dup pre-check runs against the widened predicate so a push onto
rows that only conflict among soft-deleted ones succeeds.

## Half interaction

A `softDelete` child under a cascading parent gets **stamped** (not hard-deleted) — the child
table's own delete semantics apply to every delete that reaches it, initiator-initiated or
cascaded. A `restrict` child blocks only via *visible* rows; a `setNull` child clears only
*visible* rows. A soft-deleted **parent** triggers nothing (its row persists).

## Client mirrors (all three SDKs + dashboard; CLI unchanged)

- Wire: `FieldType` id-variant `onDelete`, `TableDef.softDelete`, `Step::Undelete`
  (`undelete(table, id)` in the txn builders) — byte-identical casing across
  `protocol.rs` / `protocol.ts` / `wire.rs` / `wire.py`.
- Schema DSL: `t.id(table, { onDelete })` (ts), `id(table).on_delete(action)`-style (rust),
  `t.id(table, on_delete=...)` (python); `.softDelete()` / `.soft_delete()` / `.soft_delete()`
  table builders.
- In-memory harnesses: soft-delete stamp + read-filter + undelete, cascade/restrict/setNull over
  the harness schema, cycle guard. Push-time validation mirrored at the harness's existing
  validation depth only.
- Dashboard: Schema page renders an id field's `onDelete` and a table's `softDelete` badge
  (display-only).
- CLI: deserializes the rust-client `SchemaDef` — additive `Option`/defaulted fields need no
  change (FM-32 precedent).

## Tests mapped to the card criteria

1. *Cascaded deletes visible to the committer* — cascade fires subs/op-feed per child row
   (integration: parent delete with N children → each child id appears in write-set/fan-out);
   restrict rejects atomically; setNull clears column + doc key and re-runs content subs; cycle
   (self-ref) terminates; MAX_CASCADE_ROWS aborts.
2. *softDelete semantics* — delete stamps (row still physically present, invisible on every
   terminal incl. get/search/vector), unique index excludes soft-deleted rows (re-insert same
   key succeeds), undelete restores, upsert-over-soft-deleted inserts fresh, reaper hard-deletes.
3. `make checkall` green (all six packages).
