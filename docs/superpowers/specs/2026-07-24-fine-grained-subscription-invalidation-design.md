# Fine-Grained Subscription Invalidation (v1: Point Reads)

- **Status:** Design
- **Date:** 2026-07-24
- **Related:** FEATURE_MATRIX #21; main design spec `2026-07-21-par-rt-db-design.md` ("Reactivity", "Future").
- **Scope:** Server-only. No protocol or wire change (no client mirror required).

## Summary

Replace coarse table-level subscription invalidation with **sound, document-level
skipping for `get(id)` point reads**, while preserving today's table-level behavior
for every other query shape. Ships the `WriteSet` / `ReadSet` plumbing that
finer-grained schemes (range/boundary tracking) can extend additively later.

## Background (today)

After each committed transaction the committer re-runs every subscription whose
`query.table` is in the txn's `write_set` (a `BTreeSet<String>` of table names),
canonicalizes the result, and pushes a `queryUpdate` only when it differs from the
last pushed value (`subs.rs::fan_out`). Over-invalidation is harmless — the
canonical diff suppresses spurious pushes. The only failure mode is
**under-invalidation**: a missed re-run is a missed realtime update.

This is correct but coarse: a subscription to a single document by id re-runs on
*every* write to its table, even writes to entirely unrelated documents.

## The correctness constraint

Invalidation may over-approximate freely but must **never under-approximate**. Any
new skip must therefore be *provably safe*: the written documents must be provably
irrelevant to the subscription's result.

A `get(id)` query is the one shape where provable irrelevance follows from the
query alone. Its result is exactly document `id` — a point read by id, and the
`get` terminal excludes every other terminal (`query.rs`: "point read by id;
excludes all below"). No other document can affect it. So skipping is safe iff the
written set does not contain `(table, id)`.

Every other shape admits an **entering-document** case — a document not currently
in the result can enter it when a row is inserted, or when an existing row's sort
key / eq-field changes:

| Query shape | Result depends on | Entering-document case |
|---|---|---|
| `get(id)` | exactly doc `id` | none — bulletproof |
| `unique` / `eq` point | the matching doc | another doc's eq-fields update to match |
| `take(N)` ordered | top-N window | a new/better-ranked doc enters the window |
| `collect` / `count` / eq-set | unbounded set | any matching insert |
| `paginate` | a page + cursor boundary | inserts/updates shift the window |
| `search` / `vectorSearch` | ranked top-K | any insert changes ranking |

Sound skipping for those requires range/boundary or predicate tracking, which is
**deferred**. v1 treats them all as `Table` — re-run on any write to the table,
identical to today.

## Design

### `WriteSet`

Replace `TxnOutcome.write_set: BTreeSet<String>` with a richer server-internal type:

```rust
pub struct WriteSet {
    pub tables: BTreeSet<String>,           // unchanged: tables touched
    pub docs: BTreeSet<(String, String)>,   // NEW: (table, id) of every written document
}
```

`execute_txn` (`txn.rs`) populates it: each write step already resolves the written
`id` in hand at the point `write_set` is touched today — `Insert` returns it,
`Patch`/`Replace`/`Delete` take it as a step input, `Upsert` has it from the insert
or the matched row. Each appends `table` to `tables` and `(table, id)` to `docs`.
Read-only steps (`ExpectVersion`, `ExpectAbsent`) touch neither, as today.

`TxnOutcome` is server-internal: the transports send only `outcome.results`
(`http_api.rs`, `ws.rs`), never `write_set`. Changing its type is therefore not a
protocol change and needs no client mirror.

### `ReadSet`

A new enum, derived once from the subscription's immutable `Query` at registration
and stored on `SubEntry`:

```rust
pub enum ReadSet {
    Point { id: String },   // query.get.is_some()
    Table,                  // every other shape
}
```

For v1 this is a pure function of the `Query` — `get`'s id is `query.get` — so no
execution-time instrumentation of `execute_query` is required. The enum is shaped
so a future "B" extends it additively (e.g. `Window { .. }`, `Predicate { .. }`).

Only a `get` query yields `Point`. `first` and `unique` also return a single
document but are *not* point reads — which document they return can change on an
insert or an eq-field update — so they fall through to `Table`.

### Registration

`SubEntry` gains `read_set: ReadSet`; `SubscriptionManager::register` computes it
from the query. It never changes for the life of the subscription (the `Query` is
immutable), so it is not recomputed on re-run.

### Invalidation (`fan_out`)

For each subscription on table `T`:

```
if T not in write_set.tables: skip                         // unchanged fast path
else match read_set:
    Point { id } => if (T, id) in write_set.docs { re_run } else { skip }   // the win
    Table         => re_run                                 // today's behavior
```

`re_run` is the existing `execute_query` + canonical diff + push-on-change. The
`(T, id) ∈ docs` test is **op-agnostic**: a patch/replace/delete/upsert of `id`
and an insert that happens to target `id` all place `(T, id)` in `docs`, so all
correctly trigger a re-run with no operation-type classification.

### Soundness argument

- `T ∉ tables` ⇒ no write touched the table ⇒ result unchanged. *(unchanged)*
- `get(id)` with `(T,id) ∉ docs` ⇒ the only document the result depends on was not
  written ⇒ result unchanged. *(new; safe)*
- All other cases ⇒ re-run ⇒ over-approximation, diff-suppressed. *(unchanged)*

Under-invalidation is impossible: the only new skip is the case where the written
set provably excludes the sole relevant document.

## Edge cases

- **Insert specifying an existing id** — if a client-supplied id collides with a
  `get`'s id, the insert places `(table, id)` in `docs` ⇒ re-run. Membership
  suffices; no op-type classification needed.
- **Delete of a different doc** — `(T, other_id)` is in `docs` but the `get(id)`
  sub reads `id` ⇒ skip. Correct: deleting an unrelated doc cannot change a point
  read of `id`.
- **Schema evolution** — unchanged. DDL pushes do not call `fan_out`; subscriptions
  re-run lazily on the next write to their table and may error (logged/skipped).
  Not a regression.
- **Idempotent replay** — a repeat `idempotency_key` returns `WriteSet::default()`
  (empty) ⇒ no fan-out, preserving "already fanned out" semantics.

## Testing

New integration cases against the real dev Postgres:

1. Subscribe `get(idA)` on table `T`. Insert/patch/delete a *different* doc `idB`
   ⇒ **no** `queryUpdate` (today this needlessly re-runs).
2. Patch/replace/delete `idA` ⇒ `queryUpdate` pushed.
3. A `take`/`collect` subscription on `T` still re-runs on any write to `T`
   (table-level behavior preserved).
4. `WriteSet` collects `(table, id)` correctly across insert / patch / replace /
   delete / upsert (unit-level).
5. Idempotent replay (`idempotency_key` repeat) produces no fan-out.

## Files

- `server/src/txn.rs` — `WriteSet`, `TxnOutcome.write_set`, populate per step.
- `server/src/subs.rs` — `ReadSet`, `SubEntry` field, derive at `register`,
  consult in `fan_out`.
- `server/src/committer.rs` — idempotent-replay returns `WriteSet::default()`.
- `server/tests/*` — new invalidation cases.
- `FEATURE_MATRIX.md` — update #21 row (🟡 → ✅ with an honest scope note).

## Deferred (future "B")

Range/boundary tracking for `take(N)` ordered queries; eq-predicate evaluation for
`unique` / `eq`; ranking-aware handling for `search` / `vectorSearch`. The
`WriteSet.docs` and `ReadSet` shapes are designed so these extend additively rather
than requiring a rewrite.
