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

---

# v2 (eq-prefix + range) — 2026-07-28

- **Status:** Implementing.
- **Scope:** Server-only, no protocol/wire change (same as v1). Extends v1's
  `WriteSet` / `ReadSet` plumbing additively — `Point` is unchanged.

## Summary

Extend sound skipping from `get(id)` point reads to **`count`, `collect`, and
`unique` queries that filter on a btree index's eq-prefix (and an optional range
bound on the next index field)**. A subscription of one of these shapes re-runs
only when a written document may have crossed its window boundary; writes to
unrelated documents are skipped, same as v1 does for `get(id)`.

Every other shape stays table-level: `take(N)` / `first` / `paginate` (top-N or
page window — a doc can enter/leave the truncated window even when its eq-prefix
is unchanged), `distinct` / `aggregate` (value-sensitive over matching docs),
and `search` / `vectorSearch` / `hybridSearch` (ranking). These remain `Table`.

## Why only count / collect / unique, and the content-bearing split

These three terminals return their **full** matching set (no truncation):

- `count` → the number of matching docs. Pure **membership**: the result changes
  iff a written doc's membership in the window changed.
- `collect` (no terminal) → the matching doc **bodies** (capped at `MAX_TAKE`).
  A cap only ever causes over-approximation (a matching insert beyond the cap is
  re-run needlessly), never under-approximation. But because it returns bodies,
  the result also changes when a **member's content** changes.
- `unique` → the one matching doc (errors if >1). Same as collect: content-bearing.

So a written doc `d` **affects** such a subscription iff:

- **count:** `d`'s window-membership changed (`old_match != new_match`).
- **collect / unique:** `d` is or was a member (`old_match || new_match`) — a
  body change to a current member, or a doc entering/leaving, both count.

(`take`/`first`/`paginate` are excluded because membership-unchanged does NOT
imply the ordered window is unchanged; `distinct`/`aggregate` because they depend
on the *values* of members, not just membership.)

## Carrying written doc values

`WriteSet` gains a `doc_values` map, **separate from `ops`** so the op-feed,
audit-log, and webhook taps (which consume only `&ops`) are untouched:

```rust
pub struct WriteSet {
    pub tables: BTreeSet<String>,
    pub docs: BTreeSet<(String, String)>,
    pub ops: Vec<DocOp>,                 // unchanged
    /// Per written `(table, id)`: the doc as it stood at txn START (`before`,
    /// `None` if created inside this txn) and at txn END (`after`, `None` if
    /// deleted inside this txn). Used only by `fan_out`; never sent on the wire.
    pub doc_values: BTreeMap<(String, String), DocValues>,
}

pub struct DocValues {
    pub before: Option<serde_json::Map<String, serde_json::Value>>,
    pub after: Option<serde_json::Map<String, serde_json::Value>>,
}
```

`WriteSet` loses its `Eq` derive (a `Map` is not `Eq`); the derive is unused
today (no `==` on `WriteSet` anywhere). `DocOp` keeps `Eq` — it is unchanged.

`execute_txn` populates `doc_values` per step, keyed by `(table, id)`, recording
the **net** effect across the whole txn (a doc touched by several steps collapses
to one entry — earliest `before`, latest `after`):

| Step | `before` | `after` |
|---|---|---|
| `Insert` | `None` (created) | the stamped doc |
| `Patch` | the fetched pre-merge doc (first touch only) | the merged doc |
| `Replace` | the existing doc (first touch only — `do_replace` fetches `doc`, not just `id`) | the new (stripped) doc |
| `Upsert` insert branch | `None` | the stamped insert doc |
| `Upsert` update branch | the matched doc (first touch only) | the merged doc |
| `Delete` | unchanged | `None` (marks deleted) |

The write helpers already hold these values: `do_patch` and the upsert-update
branch fetch the old doc to merge; `do_replace` is widened to `SELECT "doc"` (it
today selects only `"id"` to confirm existence). `Delete` records no values — its
net effect is "after = None", which `fan_out` treats as always-affecting.

## `ReadSet::Indexed`

A new variant derived once at registration (needs the table def for field types —
see Registration below):

```rust
enum ReadSet {
    Point { id: String },                                     // get(id) — v1
    Indexed(IndexedRead),                                     // NEW
    Table,
}

struct IndexedRead {
    /// The eq-prefix: index field name + its FieldType, and the typed bind.
    eq: Vec<(String, FieldType, EqBind)>,
    /// Optional range bound on `index.fields[eq.len()]`.
    range: Option<RangeBound>,
    /// collect / unique return doc bodies (a member's content change matters);
    /// count does not.
    content_bearing: bool,
}

struct RangeBound {
    field: String,
    field_type: FieldType,
    lower: Option<(EqBind, bool)>,  // (gt, false) / (gte, true)
    upper: Option<(EqBind, bool)>,  // (lt, false) / (lte, true)
}
```

`from_query` yields `Indexed` iff **all** hold:
- terminal is `count`, or no terminal (`collect`), or `unique`;
- `index` is set **and** (`eq` non-empty **or** a range bound is present)
  (otherwise the window is the whole table → no skip benefit → `Table`);
- none of `take` / `first` / `paginate` / `distinct` / `aggregate` / `search` /
  `vector_search` / `hybrid_search` is set.

Otherwise `Table` (or `Point` for `get`). The query is immutable, so `Indexed`
is computed once and never recomputed.

### `in_window(doc)` — pure, total, over-approximating on any doubt

```
for (field, ty, want) in eq:
    have = eq_bind_for(ty, doc[field])      // None/NULL/wrong-type ⇒ no-match
    if have != Ok(want): return false        // eq is AND: one miss ⇒ outside
match &range:
    Some(r) ⇒ satisfy r.lower / r.upper against eq_bind_for(r.field_type, doc[r.field])
              (NULL / typing failure ⇒ outside ⇒ false)
    None ⇒ ()
true
```

**Any typing failure, missing field, or comparison ambiguity ⇒ return `false`
(outside the window)** for eq/range membership — this can only cause a re-run
(over-approximation), never a missed one. (`eq_bind_for` is the existing shared
typer in `txn.rs`, so doc values and query values are typed identically to the DB.)

## `fan_out` per-`Indexed` subscription

For each written `(table, id)` on the subscription's table, with its `DocValues`:

```
deleted   = after.is_none()
created   = before.is_none() && after.is_some()
// updated = before.is_some() && after.is_some()

affects =
    if deleted                 { true }                          // values gone ⇒ re-run
    else if created            { in_window(after) }              // entered iff now in
    else /* updated, both known */ {
        let old_m = in_window(before), new_m = in_window(after);
        if content_bearing { old_m || new_m }                    // collect / unique
        else               { old_m != new_m }                    // count
    }
```

Re-run the subscription iff **any** written doc on its table `affects`. Otherwise
skip (every doc provably irrelevant). Owner filtering and `filter` are ignored in
the skip decision — they can only narrow the real result, so ignoring them
over-approximates (re-runs a matching-but-filtered-out or not-visible doc),
never under-approximates.

### Soundness argument

- `get(id)` — unchanged from v1.
- `Indexed` sub skipped ⟹ for every written doc `affects == false`:
  - **created** with `!in_window(after)`: a brand-new doc outside the window can
    never be in the result ⇒ sound.
  - **updated** with `old_m == new_m == false` (collect/unique) or
    `old_m == new_m` (count): the doc was never a member (collect/unique) or its
    membership didn't change (count), so it neither entered, left, nor (for
    count) altered the count ⇒ sound.
  - deletes are never skipped (`affects == true`).
- All other shapes (`Table`) and all uncertain cases re-run ⇒ over-approximation,
  diff-suppressed.

Under-invalidation is impossible: the only new skips are docs provably unable to
change the result (created-outside, or membership-stable).

## Registration threading

`ReadSet::from_query` now needs the `TableDef` (for field types + index fields).
`committer::handle_subscribe` already loads the schema; it resolves
`schema.table(&query.table)` and passes `&TableDef` into `register`, which passes
it to `from_query`. (`fan_out` already holds the schema but does not need to
re-derive — `Indexed` is stored on `SubEntry`.) A schema that has evolved since
registration can make a stored `Indexed` reference a field/type that no longer
matches; `in_window`'s "any doubt ⇒ outside ⇒ re-run" rule keeps that sound (it
biases toward re-running).

## Testing (soundness matrix)

New integration cases against the real dev Postgres, plus unit cases for
`in_window` / `affects`:

1. `count` / `collect` / `unique` with `eq=[x]`: insert a doc whose eq-prefix is
   `y` ⇒ **no** `queryUpdate`. Insert/patch one to `x` ⇒ pushed.
2. Range: `collect` with `eq=[x], gte=10`; insert eq=`x` value `5` ⇒ **no** push;
   insert eq=`x` value `20` ⇒ pushed.
3. Patch a **member** of a `collect` sub (eq unchanged, body changed) ⇒ pushed
   (content-bearing). Same patch on a `count` sub (membership unchanged) ⇒ **no**
   push.
4. Patch a doc **out** of the window (`x`→`y`) on a `collect` sub ⇒ pushed
   (it was a member). On a `count` sub ⇒ pushed (count decreased). Requires
   `before` to be captured — this is the regression guard for the out-move case.
5. Delete any doc on the table of a `count`/`collect` sub ⇒ pushed (always).
6. `take` / `first` / `paginate` / `distinct` / `aggregate` / `search` subs still
   re-run on any write to the table (table-level preserved).
7. Owner-filtered + `filter`-bearing `collect` subs: a write to a not-visible /
   filtered-out doc whose eq-prefix still matches ⇒ re-run (over-approximation,
   sound); a write whose eq-prefix doesn't match ⇒ skip.
8. Unit: `in_window` returns `false` on null/wrong-typed/missing field (no crash,
   no false match).

## Files

- `server/src/txn.rs` — `WriteSet.doc_values` + `DocValues`; capture per step
  (widen `do_replace` to fetch `doc`); drop `Eq` on `WriteSet`.
- `server/src/subs.rs` — `ReadSet::Indexed` + `IndexedRead` / `RangeBound`;
  `from_query(&Query, &TableDef)`; `in_window` + per-doc `affects`; consult in
  `fan_out`.
- `server/src/committer.rs` — `handle_subscribe` resolves `TableDef` and passes
  it to `register`.
- `server/tests/*` — the soundness matrix above.
