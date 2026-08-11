# Full-Text Search Filter (#11 follow-up) — Design

**Date:** 2026-08-09
**Status:** Implemented (2026-08-10)
**Parent:** `docs/superpowers/specs/2026-07-21-par-rt-db-design.md`, `FEATURE_MATRIX.md` row #11
**Precedent:** Full-text search (#11, `execute_search`), the db-side `filter()` DSL (#15, `compile_filter` / `compile_filter_node`), and vector search (#17, whose eq-only `filter` is the narrower analog).

## 1. Goal & scope

par-rt-db's full-text `search` terminal matches a declared search index's
tsvector and ranks by `ts_rank`, but it cannot be **narrowed** — you can search a
whole table, not "messages in `#general` containing `hi`" or "docs in tenant `X`
matching `invoice`." Convex's `withSearchIndex` supports both index-declared
`filterFields` (fast eq) and a general `.filter()`
([Convex text-search docs](https://docs.convex.dev/search/text-search)). This
closes that gap.

The change is small because every ingredient already exists:

- `FilterExpr` (the db-side `filter()` DSL — `Eq/Neq/Gt/Gte/Lt/Lte/In/And/Or/Not/
  Contains/Exists`, `server/src/query.rs:258`) and its compiler
  `compile_filter(node, table, start_pos)` (`query.rs:1801`) already produce a
  fully-parenthesized, `$n`-bound SQL fragment that uses a field's typed `f_`
  column when indexed and jsonb extraction (with an inferred cast) otherwise.
- `execute_search` (`query.rs:2208`) already composes per-row auth predicates into
  its `WHERE` with correct placeholder numbering (`auth_start = 2`, since `$1` is
  the tsquery text). Folding a client filter in is the same pattern
  `compile_scan_where` (`query.rs:1767`) uses for the ordinary read path.

### In scope (v1)

- Optional `filter: FilterExpr` on the `search` terminal, compiled into the search
  `WHERE`.
- Mirrored to **all four clients** (ts / rust / python): wire type + builder + the
  in-memory test harness.
- Update `FEATURE_MATRIX.md` row #11 (search now composes with `filter`).

### Non-goals (v1)

- **Index-declared `filterFields`** (Convex's fast-eq path). par-rt-db does not
  need it: `FilterExpr` already binds a typed `f_` column for an indexed field, so
  eq on an indexed field is already an indexed lookup. One filter mechanism covers
  both Convex modes.
- **Upgrading vector search's eq-only `filter` to the full `FilterExpr`.** Vector
  search scoped its filter to eq-only deliberately (#17 non-goal) and was tracked
  as a separate follow-up. That follow-up shipped same-day as commit `613c7a6`
  (`feat(vector): upgrade vectorSearch filter to full FilterExpr (#17 follow-up)`),
  so `vectorSearch` now composes with the full `FilterExpr` just like `search`.
- **Composition with `order` / a `take` beyond the terminal's own limit.** Search
  keeps `ts_rank` ordering and its `take` → `LIMIT`; `filter` only narrows the
  `WHERE`.
- A similarity `_score` in results (unchanged — `QueryResult::Docs`).

## 2. Wire contract (load-bearing)

Four implementations of this protocol — `server/src/protocol.rs` +
`server/src/query.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, and
`python-client/src/par_rt_db/wire.py` — must stay byte-identical. The change is
strictly additive: `SearchQuery` gains one optional field, omitted on the wire when
`None`, so **existing search requests deserialize unchanged**.

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub index: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<FilterExpr>,
}
```

Wire (`FilterExpr` is already `#[serde(tag = "op", rename_all = "lowercase",
deny_unknown_fields)]`, so ops are lowercase):

```json
{
  "table": "messages",
  "search": {
    "index": "search_body",
    "query": "hi",
    "filter": {
      "op": "and",
      "exprs": [
        { "op": "eq", "field": "channel", "value": "#general" },
        { "op": "gt", "field": "createdAt", "value": 1780000000000 }
      ]
    }
  }
}
```

The Query-level top-level `filter` field is **untouched** and remains mutually
exclusive with the `search` terminal — the search filter lives nested on the
terminal, exactly as vector search's `filter` does. No collision, and the existing
terminal peer/mutual-exclusion tables are unchanged (the nested `filter` is not a
new terminal).

## 3. Server design

### 3.1 Read path (`query.rs::execute_search`)

`$1` stays the tsquery text. The client filter compiles at `start_pos = 2`, ahead
of the row-auth and `authorize` binds — the same ordering `compile_scan_where`
uses (filter → row-auth → authorize → limit):

```sql
SELECT "id","doc","created_at","version" FROM "<schema>"."<table>"
WHERE "<sv_col>" @@ <plainto_tsquery $1>
  [AND (<compiled filter>)]        -- $2..   only when search.filter is Some
  [AND <owner/collaborator pred>]  -- next free slot
  [AND <authorize pred>]           -- next free slot
ORDER BY ts_rank("<sv_col>", <tsq>) DESC, "created_at" DESC, "id" DESC
LIMIT $<limit_ph>
```

Implementation: replace `execute_search`'s inline `auth_start = 2` accounting with
the same accumulator `compile_scan_where` uses — push the client filter fragment +
binds first (`compile_filter(search.filter, table_def, 2)?`, only when `Some`),
then the row-auth predicate, then the `authorize` predicate, renumbering via
`auth_start + binds.len()`. The `execute_search` signature is unchanged (the filter
rides on `search: &SearchQuery`). A bad/empty filter surfaces the existing
`BadRequest` from `compile_filter_node` (unknown field, wrong type, empty
`and`/`or`).

### 3.2 Reactivity — unchanged

`search` already rides the committer's **table-level** invalidation: any committed
write to the query's table re-runs every affected `search` subscription and pushes
only on canonical diff. A `filter` narrows the *result*, not the invalidation
window — a filtered search still depends on member values + ranking, so a write
anywhere in the table can change it. Table-level is the sound over-approximation
(the same reason `distinct`/`aggregate`/`vector`/`hybrid` stay table-level, per
FEATURE_MATRIX #21). **No committer change, no `ReadSet` change.**

### 3.3 No DDL / write-path / deployment changes

`FilterExpr` filters on existing fields via typed `f_` columns (already maintained
on every write) or jsonb extraction. No new index declaration, no new column, no
extension, no compose-image change.

## 4. Error handling

All failures use the existing `RtDbError` envelope `{code, message}`:

- Filter contract violations (unknown field, wrong-type value, empty `and`/`or`,
  `in` with no values) → `BAD_REQUEST` (existing `compile_filter` errors).
- Search-index-not-found / empty query text → `BAD_REQUEST` (existing).
- Never a 500; never stringify a sqlx error into the body (existing discipline).

## 5. Client mirror

Each client's `SearchQuery` wire type gains the additive optional `filter`, and the
`.search()` builder accepts a filter option — mirroring vector search's
`{ filter }` ergonomics but with a full `FilterExpr`:

- **ts-client** (`protocol.ts` + `query.ts`): `SearchQuery.filter?: FilterExpr`;
  `TableQuery.search(text, { filter })` (extend the existing options arg
  additively). The in-memory harness (`in_memory.ts`) applies the `FilterExpr` to
  the in-memory search result set via the existing `validateFilter`/
  `evalFilterExpr`, so the filter path is covered without Postgres.
- **rust-client** (`wire.rs` + `query.rs`): `SearchQuery { filter:
  Option<FilterExpr> }`; `.search(text)` gains a filter option; the `in_memory`
  harness applies it.
- **python-client** (`wire.py` + `query.py`): `SearchQuery.filter`;
  `TableQuery.search(...)` gains `filter=`; the in-memory harness applies it.

**Builder ergonomics:** the existing top-level `.filter()` builder sets the
Query-level filter (exclusive with `search`), so the search filter is passed
through the `.search(text, { filter })` options, not a chained `.filter()`, to
avoid the ambiguity of two `.filter()` calls meaning different things.

## 6. Testing

- **Server `query` (`query_test.rs`):** search + `filter` narrows results — eq on
  an indexed field, range on a number field, `and`/`or`/`not`; filter + per-row
  `ownerField` together (both apply); bad filter (unknown field) → `BadRequest`;
  filter omitted behaves identically to today; a `search` subscription with a
  filter pushes an updated (filtered) result on a matching write and a
  correctly-narrowed result on a non-matching write.
- **Server `protocol`:** `SearchQuery` with `filter` round-trips; `filter` omitted
  round-trips to the same shape as today (proves additive).
- **Clients:** builder-shape coverage in `query.test.ts` / rust-client / python
  -client; in-memory harness filter-on-search coverage. (The cross-client
  `query_combinations` matrix is unaffected — the nested `filter` is not a new
  terminal.)

## 7. Convex parity

Update `FEATURE_MATRIX.md` row #11: `search` now composes with an optional
`filter` (`FilterExpr`) — narrowing full-text search by any field, fast on indexed
fields. Convex supports index `filterFields` (eq) plus a slower one-by-one
`.filter()`; par-rt-db's single `FilterExpr` binds typed `f_` columns in-SQL, so it
narrows in the same pass as the tsvector match rather than post-filtering row by
row. Record the deliberate asymmetry: vector search's filter stays eq-only (v1
scope cut, #17); aligning it is a recorded follow-up, not part of this feature.
