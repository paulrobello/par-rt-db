# Unique + Partial Index Constraints — Design

**Date:** 2026-08-01
**Status:** Approved (brainstormed 2026-08-01)
**Board:** par-rt-db → "Unique + partial index constraints" (in_progress)
**Effort:** M

## Motivation

par-rt-db can declare secondary + compound **btree** indexes, full-text **search**
indexes, and **vector** indexes — but it cannot declare a **uniqueness constraint**.
There is no `UNIQUE` index today (the only `unique` hit in `schema.rs`/`ddl.rs` is a
comment about index-name uniqueness). Apps that need uniqueness — unique slug /
email-per-owner / dedup key / one-active-session-per-user — have no DB-layer
guarantee: they must read-then-insert and hope the single-writer committer orders
things kindly, with no declarative enforcement, no `upsert`-by-natural-key, and no
partial uniqueness ("unique slug among non-deleted rows").

Postgres gives this for free (`CREATE UNIQUE INDEX`, including partial unique
indexes with a `WHERE` clause). Convex has no first-class declarative unique
constraint either, so this is a place for par-rt-db to **lead**, not just match —
reinforcing the §6 "ahead" story (Postgres-native, declarative integrity).

## Goals

1. A declarative **`unique`** flag on a btree index → `CREATE UNIQUE INDEX`.
2. A declarative **`where`** partial-index predicate on a btree index →
   `CREATE [UNIQUE] INDEX … WHERE <pred>`, reusing the existing `filter()`
   `FilterExpr` DSL.
3. The two compose: a **partial unique index** ("unique among rows matching the
   predicate") is the headline use case.
4. Atomic runtime enforcement: a uniqueness violation aborts the whole txn, like
   `expectVersion`.
5. Byte-identical mirror across all four clients (ts / rust / python + in-memory
   harnesses), per the standing "clients mirror the core" invariant.

## Non-goals (filed as deferred follow-ups)

- **Upsert-by-unique-key** — targeting an `upsert` at a unique index instead of
  `id`. Enlarges the txn surface; separate spec.
- **`alterIndex` migrate directive** — flipping uniqueness on a live index without
  drop+re-add. Today you drop the index (migrate `dropIndex`) and push the new one.
- Generalizing `where` to non-btree (search/vector) indexes — out of scope; unique
  and partial apply to plain btree indexes only.

## 1. Schema DSL — two additive flags on `IndexDef`

Mirror the existing `search: bool` / `vector: Option<VectorIndexSpec>` additive-flag
pattern in `server/src/schema.rs` exactly, so existing schemas deserialize
byte-identically:

```rust
pub struct IndexDef {
    pub name: String,
    pub fields: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub search: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorIndexSpec>,
    // NEW:
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
    #[serde(default, rename = "where", skip_serializing_if = "Option::is_none")]
    pub r#where: Option<FilterExpr>,   // partial-index predicate
}
```

- **Wire keys:** `unique` and `where`. Rust uses the raw identifier `r#where` so the
  field serializes as `where` without colliding with the `where` keyword.
- **Omitted when absent** (`skip_serializing_if`) ⇒ a plain btree index still
  serializes as `{"name","fields"}` only — existing schemas and client payloads
  deserialize unchanged.
- **`where` reads as the SQL concept** and is intentionally distinct from the
  query-time `filter()` terminal (same `FilterExpr` type, different role: a baked-in
  index predicate vs. a per-query narrow).
- **Module coupling:** `FilterExpr` is defined in `server/src/query.rs` (mirrored in
  each client's wire/protocol file). `IndexDef` lives in `schema.rs`. Reuse it via
  `use crate::query::FilterExpr;` in `schema.rs` — Rust resolves intra-crate module
  cycles fine (and `query.rs` already depends on `schema::TableDef` via
  `compile_filter`). If the coupling is later deemed undesirable, `FilterExpr` is a
  wire type and can move to `protocol.rs` as a follow-up cleanup; it is **not**
  required for this work.

### Validation (`schema.validate()`)

- `unique` and `where` are legal **only on a plain btree index** — mutually exclusive
  with `search: true` and with `vector`. Combining them with search/vector ⇒
  `BadRequest` ("unique/where may only be set on a plain btree index").
- A `where` predicate may reference only **declared fields** of the table — already
  enforced when the predicate is compiled (see §2). `validate()` resolves the
  predicate structurally (every referenced field exists) for an early, clear error
  before DDL.

## 2. DDL — `CREATE [UNIQUE] INDEX … [WHERE <literal>]`

In the existing btree `else` branch of `push_schema` (`server/src/ddl.rs`), the
statement is generalized:

```text
CREATE [UNIQUE] INDEX "<index_ident>" ON "<schema>"."<table>" (<cols>, "created_at")
[WHERE <partial-sql>]
```

- `unique` ⇒ emit `UNIQUE` before `INDEX`.
- `where` ⇒ append `WHERE ` + the SQL from `compile_filter_literal` (§3).

Everything else in the branch is unchanged: the index identifier (`i_<table>_<name>`,
already lowercased + length-capped), the columns (`"<f_<field>>"` per index field,
plus the `created_at` tiebreaker), and the surrounding `push_schema` transaction. No
new Postgres extension is required (the `CREATE EXTENSION IF NOT EXISTS vector` line
already runs first and is unrelated).

## 3. `compile_filter_literal` — partial-index predicate as literal SQL

**Constraint:** a Postgres partial-index predicate is baked into `CREATE INDEX` as
**literal SQL** — bind parameters (`$1`) are forbidden in the predicate. The existing
`compile_filter` (`server/src/query.rs`) emits `$n` placeholders + typed `EqBind`s,
so it cannot be used directly. We add a sibling that reuses all of its validation and
emits literals instead:

```text
fn compile_filter_literal(filter: &FilterExpr, table: &TableDef) -> Result<String, RtDbError>
```

- **Reuse** the existing leaf machinery (`field_lhs_and_bind` — resolves a field to
  its double-quoted typed column LHS and types the comparison value into an
  `EqBind`) and the `and`/`or` parenthesization from `compile_filter_node`.
- **Replace** the `$n` emission (`push_filter_bind`) with a new
  `render_literal(bind: &EqBind) -> String` that inlines the typed value:
  - `EqBind::Text(s)`  → `'` + `s.replace('\'', "''")` + `'`
  - `EqBind::Bool(b)`  → `true` / `false`
  - `EqBind::Num(n)`   → Rust float rendered bare (Postgres `double precision`)
  - `EqBind::I64(n)`   → decimal bare (Postgres `bigint`)
- String escaping uses the SQL-standard `''` doubling (the same care the DDL already
  applies to doc-field literals in backfill expressions). Identifiers are already
  double-quoted via `pg_col`. The predicate is therefore immutable — comparisons of
  typed columns against literals — so it is a legal partial-index predicate.
- The function is **DDL-only** (called from `push_schema`). It is never on a
  per-query hot path, so literal inlining here carries none of the risk a bindless
  query path would.

`FilterExpr` operators all map cleanly onto immutable SQL (`=`/`<>`/`>`/`>=`/`<`/`<=`,
`IN (literal, …)`, `AND`/`OR`), matching the brainstormed decision: **full
`FilterExpr`** expressiveness for the partial predicate.

## 4. Schema evolution — uniqueness/partial flips are destructive

`detect_destructive_changes` (`server/src/ddl.rs`) already rejects index kind flips
(`search`⇄btree, `vector` changes). Add two sibling arms, byte-for-byte in the same
style:

- changed `unique` ⇒ `BadRequest` "changed uniqueness of index '<name>'"
- changed `where`  ⇒ `BadRequest` "changed partial predicate of index '<name>'"

To change uniqueness on a live index: **migrate `dropIndex`** (already shipped) to
drop it, then push the new index declaration. (A future `alterIndex` migrate
directive is a deferred follow-up; out of scope here.)

## 5. Enabling `unique` on a table that already has data

`push_schema` is **not** serialized through the committer (migrate was added as a
*separate* `RunMigrate` arm precisely because plain schema-push does not ride the
single-writer path). Therefore the `CREATE UNIQUE INDEX` statement, executed inside
`push_schema`'s own transaction, is the **authoritative** atomic guarantee — relying
on it (rather than a check-then-create sequence) eliminates any TOCTOU window against
concurrent writes.

For a better error in the common case (adding a unique index to a table that already
holds duplicates), `push_schema` runs a pre-check immediately before the CREATE:

```text
SELECT <cols> FROM "<schema>"."<table>"
[WHERE <partial-pred-sql>]          -- only for a partial unique index
GROUP BY <cols>
HAVING count(*) > 1
LIMIT 5
```

The optional `WHERE` reuses the **same** `compile_filter_literal` output baked into
the `CREATE UNIQUE INDEX`, so the pre-check looks for dupes across exactly the rows
the partial unique index will constrain.

- If it returns any rows ⇒ return `CONFLICT` (see §6) **before** attempting the
  CREATE, with a message naming the index and the offending key values.
- If it passes ⇒ run `CREATE UNIQUE INDEX`; the CREATE remains the guarantee and
  catches anything the pre-check raced past (aborting the whole `push_schema` tx →
  mapped to `CONFLICT`).
- For a **brand-new table** there is no data, so both the pre-check and any risk are
  no-ops; the CREATE always succeeds.

The pre-check is purely UX. It must never be treated as the enforcement boundary.

## 6. Runtime enforcement + new `CONFLICT` error code

Postgres enforces the unique index on every `insert` / `patch` / `replace` /
`upsert`-update inside `execute_txn`; a `unique_violation` (SQLSTATE 23505) aborts
the whole transaction atomically — exactly the semantics of a failed
`expectVersion`/`expectAbsent` precondition. We map that violation to a clean error
rather than leaking a sqlx string:

- **New wire code `CONFLICT`, HTTP 409** — added to `ErrorCode` (`server/src/error.rs`)
  and mirrored across all four clients. This is the one protocol change. It is the
  correct HTTP semantic and lets clients branch on "uniqueness clash" distinctly.
- Detection: in the write path, a sqlx error whose `SqlxError::Database` SQLSTATE is
  `23505` is mapped to `RtDbError { code: Conflict, message: "unique index
  '<name>' violated …" }` (message names the index when determinable). All other DB
  errors keep their existing generic-500 handling (`tracing` log + generic body).
- The create-time violation (§5) and the runtime write-time violation both surface as
  `CONFLICT`.

> Rejected alternative: reuse `PRECONDITION_FAILED` (412). Avoided because uniqueness
> is not a version precondition and a distinct code lets clients react differently.
> The one-time mirror cost is small.

## 7. Wire + client mirror (standing invariant)

The four `IndexDef` representations gain `unique` + `where` byte-identically:

- **server** `schema.rs` (above) + the `CONFLICT` code in `error.rs`.
- **ts-client** `protocol.ts` `IndexDefJson` (`unique?: boolean`, `where?: FilterExpr`)
  + `ErrorCode.CONFLICT`; the index builder gains `.unique()` and `.where(pred)`.
- **rust-client** `wire.rs` (`IndexDef` + `ErrorCode::Conflict`) and `schema.rs`
  `TableBuilder::index(...)` builder (`.unique()`, `.where(pred)`).
- **python-client** `par_rt_db/schema.py` `IndexDef` (`unique`, `where` via the
  `_drop_absent_flags` serializer that already omits falsy `search`/`None vector`) +
  `ErrorCode.CONFLICT`; `TableBuilder.index(...)` gains `.unique()`/`.where(pred)`.

Wire key casing is the load-bearing non-uniform protocol convention — match the four
wire files exactly (camelCase is **not** uniform here; follow what each file already
does for `search`/`vector`).

## 8. In-memory test harness enforcement

All three client harnesses (ts `InMemoryRtDbClient`, rust `in_memory` feature,
python `par_rt_db.in_memory`) currently mirror schema/query/txn semantics with no
network. They must additionally **enforce unique indexes**: an `insert` / `patch` /
`replace` / `upsert`-update whose result would produce two rows with identical values
on a unique index's columns (and, for a partial unique index, where both rows satisfy
the `where` predicate) ⇒ raise the client's `CONFLICT` error and roll back the txn,
mirroring the server. Partial-predicate evaluation reuses each harness's existing
`FilterExpr` evaluator (`evalFilterExpr` / `eval_filter_expr` / `_eval_filter_expr`).
This is what makes app-level tests catch duplicate writes offline.

## 9. Subscriptions, op-feed, audit log, webhooks — unchanged

A unique index is a storage-layer constraint. It does not change `DocOp`, read-sets,
or invalidation: subscriptions stay table-level (a uniqueness rejection never commits,
so it never publishes), and the op-feed / audit / webhook tap sites
(`handle_mutate` / `handle_scheduled` / `handle_migrate`) are untouched. Enabling
`unique` cannot remove an already-pushed document — it only blocks future writes —
so no new publish path or `fan_out` change is needed.

## 10. Testing

- **`schema_validators_test`** — `unique`/`where` round-trip through serde; additive
  omission (absent ⇒ not on the wire); rejection of `unique`/`where` on search/vector
  indexes.
- **`ddl` test** — assert `CREATE UNIQUE INDEX` is emitted for `unique`; assert the
  `WHERE <literal>` fragment is appended for `where` (and composes with `UNIQUE`);
  literal escaping (a string value containing `'`).
- **`txn_test`** — duplicate `insert` on a unique index ⇒ `CONFLICT`, txn fully
  rolled back; partial unique index: a second row with the same key but **excluded**
  by the predicate is allowed, one **matching** the predicate is rejected; `patch`
  that would create a collision ⇒ `CONFLICT`.
- **`detect_destructive_changes`** — flipping `unique` or `where` on an existing
  index ⇒ `BadRequest` (mirrors the search/vector flip tests).
- **Client builder + wire tests ×4** — `.unique()` / `.where(pred)` produce the
  expected wire shape.
- **In-memory harness dupe-rejection ×3** — ts / rust / python each reject a
  colliding write on a unique (and partial-unique) index with `CONFLICT`.
- **Gate:** `make checkall` (fmt + clippy `-D warnings` + typecheck + tests) across
  all five packages. Requires `make dev-db-up` (real Postgres) for the server tests.

## 11. Invariants preserved

- **SQL construction:** every identifier double-quoted; the partial-predicate values
  are typed+validated (never raw user strings interpolated unescaped). Physical names
  lowercased + 63-byte capped (existing `pg_col`/index-ident helpers) — caps
  unchanged.
- **Errors:** uniqueness violations surface as the typed `CONFLICT` envelope, never a
  stringified sqlx error in the body; create-time failures log via `tracing`.
- **Single-writer invariant:** untouched — enforcement is Postgres-side inside the
  existing `execute_txn`; no new writer, no call outside the committer's mutate path.
  (`push_schema` was already outside the committer; that is unchanged by this work.)
- **Clients mirror the core:** the `CONFLICT` code and both `IndexDef` flags land in
  all four clients in the same change.
