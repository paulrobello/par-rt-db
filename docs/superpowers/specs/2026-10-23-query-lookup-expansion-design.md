# Query lookup expansion — server-side id joins on the read DSL

**Status:** spec (pre-implementation; card criterion 1 requires this document before any implementation merges)
**Card:** kanban `01a16bac86ef759da5a0eb7ba9cd9d3f` — "Add server-side lookup expansion to the Query DSL"
**Scope:** `core/src/query.rs` (wire) + `server/src/query/` (read path) + `server/src/subs.rs` (invalidation gate) + all five client mirrors + `wire-corpus/` + docs (FEATURE_MATRIX.md, README.md, docs/ARCHITECTURE.md).

## Problem

Relational-ish reads today cost N+1 round trips: fetch N parents, then one
`get(id)` per id-array member. Convex has no joins either, so this is a place
par-rt-db leads: the wire `Query` (`core/src/query.rs`) grows an optional
lookup list that resolves an id or id-array field into joined documents
server-side, in one round trip, with the joined rows passing through the SAME
per-row authorization the read path already applies.

## Wire shape

`Query` (`core/src/query.rs`) gains one field, additive in the FM-32 style —
`#[serde(default, skip_serializing_if = "Vec::is_empty")]`, so every existing
query serializes byte-identically and no `PROTOCOL_VERSION` bump is needed:

```rust
/// Server-side lookup expansion: resolve an id / id-array field on each
/// result doc into the referenced document(s) of `table`, exposed under the
/// `as` key on the same doc. Depth-1 only; id-equality only.
#[serde(default, skip_serializing_if = "Vec::is_empty")]
pub lookups: Vec<LookupSpec>,
```

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LookupSpec {
    /// Declared field on the query's table: `Id { table }`,
    /// `Optional<Id { table }>`, or `Array { element: Id { table } }`
    /// (`core/src/schema.rs` `FieldType`).
    pub field: String,
    /// The referenced table. Must equal the field's declared `Id.table`.
    pub table: String,
    /// Output key on each parent doc. `as` is a Rust keyword; the raw
    /// identifier mirrors `IndexDef.r#where` (`core/src/schema.rs`).
    #[serde(rename = "as")]
    pub r#as: String,
    /// Per-parent joined-row cap for an id-ARRAY lookup. Default 100,
    /// hard max 1000 (values above are `BadRequest`, see Caps).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cap: Option<u32>,
}
```

Wire example:

```json
{"table": "posts", "index": "by_author", "eq": ["u1"],
 "lookups": [{"field": "authorId", "table": "users", "as": "author"},
             {"field": "tagIds", "table": "tags", "as": "tags", "cap": 20}]}
```

`query_clauses` (`core/src/query.rs`) gains a `"lookups"` entry when the vec
is non-empty, and `"lookups"` is appended to the `clauses` array in
`wire-corpus/query-combinations.json` — the combination evaluator
(`core/src/query_combinations.rs`) and all in-memory engines read that table.

Single-vs-array output is NOT a wire flag: it is derived from the field's
declared type (D4).

### Five-client mirroring

Per CLAUDE.md, the server is the protocol source of truth and every wire
change mirrors in all five clients. The `Query` type is shared server/rust
via `core` (`rust-client/src/query.rs:16` re-exports
`par_rt_db_core::query::Query`), so the Rust client needs no wire edit —
the other four do:

| Client | Wire type | Query engine |
| --- | --- | --- |
| ts-client | `QueryJson.lookups?: LookupSpecJson[]` in `ts-client/src/protocol.ts` | `in_memory.ts` |
| rust-client | free via `core` re-export | `rust-client/src/in_memory/` |
| python-client | `wire.py` query dataclass + `query.py` builder | `src/par_rt_db/in_memory/` |
| swift-client | `Wire.swift` + `Query.swift` | `InMemoryQuery.swift` |
| go-client | `go-client/wire/query.go` | wire/corpus decode only — Go runs no query-semantics engine (see the `$comment` in `wire-corpus/query-combinations.json`) |

The four in-memory engines implement expansion against their in-memory docs
using their existing `filter_matches`-equivalent evaluators, so the corpus
exercises all six runners.

**Mixed-version note:** `deny_unknown_fields` on `Query` means an older
server rejects a lookup-bearing query with `BAD_REQUEST`; clients must treat
that rejection as a capability signal, not retry.

## Semantics

For each result doc of a doc-returning terminal, for each lookup:

- **Id / `Optional<Id>` field** → parent doc gains `as: <joined doc or null>`.
  Null when the field is null/absent, the referenced doc does not exist
  (dangling id — a `get()` of a missing id reads as null), or the joined row
  fails authorization (below).
- **`Array<Id>` field** → parent doc gains `as: [<joined docs>]` in the
  id-array's declared order, dangling and unauthorized ids dropped, truncated
  to the cap (below).
- Joined docs carry the child table's system fields (`_id`, `_creationTime`,
  `_version`) merged the same way `merge_doc` merges the parent's
  (`server/src/query/terminals.rs`), so a client can patch/subscribe against
  a joined doc's identity.
- The `as` key lives on the parent doc alongside user fields. It must not
  collide with a declared field name, a `_`-prefixed name, or another
  lookup's `as` — `BadRequest` at compile time.

## Authorization on joined rows

Every joined row passes the exact predicates the read path applies to the
parent: `ownerField`/`collaboratorsField` (the `row_auth_predicate_body`
fragment, `server/src/query/row_auth.rs`) and the schema `authorize`
predicate with principal markers resolved from the same `PrincipalCtx`
(`authorize_predicate_body`, `row_auth.rs`). Bypass callers (machine tokens,
admin, scheduled jobs — `ctx.user_id = None`) bypass identically, mirroring
`point_read` (`terminals.rs:1543`).

**Decision D1 — omit, don't error.** A joined row that fails authorization
reads as absent: null for the single form, dropped from the array form.
Justification: `point_read` already silent-filters an invisible doc to
`Doc(None)` (terminals.rs:1568-1586, "Convex-like"), so erroring on a joined
row would newly leak, per id, that a row exists. The stronger reason is the
subscription path: a re-run that errors is logged and skipped
(`server/src/subs.rs` apply pass), leaving the last pushed payload stale
forever — an authorization failure on one joined child must never freeze a
live query. Implementation-wise the predicates compile into the batched
lookup WHERE clause, so a filtered row is simply never fetched.

## Caps and bounds

- **Per-parent cap (D2 — truncate):** id-array lookups truncate to `cap`
  joined docs (default 100, keeping first-in-array order). Justification:
  same subscription-staleness argument as D1 — a hard error turns benign list
  growth into a permanently stale live query; truncation matches the
  windowed-page mental model `MAX_TAKE`/`paginate` already establish.
- **`cap` range (D3 — error at compile time):** values above 1000 are
  `BadRequest`, mirroring `take > MAX_TAKE` → `BadRequest`
  (`server/src/query/terminals.rs` compile cascade). A caller declaring
  `cap: 5000` made a static mistake; clamping would hide it.
- **Per-query joined-row ceiling:** a fixed const (8192) bounds total joined
  rows materialized per query execution; on exceed, truncation proceeds in
  parent-result order (deterministic). Worst case is then ~2× `MAX_TAKE`,
  consistent with `MAX_TAKE`'s stated role as the per-query memory ceiling
  (`server/src/query/mod.rs`).
- **Lookups per query:** at most 4; a fifth is `BadRequest`.
- **Pagination interaction:** expansion applies to the returned page's docs
  only (all doc-returning terminals, ranked ones included — after ranking and
  limit). Cursor derivation and has-more probes never read lookup fields, so
  `paginate`/`nextCursor` semantics are unchanged.

## Composition

- **Terminals:** lookups are legal with every doc-returning terminal
  (`get`, `unique`, `first`, `take`/collect, `paginate`, `search`,
  `vectorSearch`, `hybridSearch`) and illegal with the doc-less ones
  (`count`, `distinct`, `aggregate`) — D7; expansion over a non-doc result is
  meaningless, and "aggregate over joined rows" is a separate future feature.
  Enforced as three new `forbid` rules in `wire-corpus/query-combinations.json`
  (`lookups`+`count`, `lookups`+`distinct`, `lookups`+`aggregate`), each with
  its `semantics/query-combo-<rule-id>.json` case — the
  `server/src/query/combinations_coverage.rs` guard fails the build if a rule
  lacks its case.
- **Projection:** `fields` may name lookup aliases. `validate_projection`
  (`server/src/query/mod.rs`) accepts an alias when lookups are present, and
  `project_result` retains alias keys alongside `_`-prefixed keys (aliases
  are synthetic result fields, the same class as `_searchSnippet`). A `fields`
  list that omits an alias drops the joined docs — full docs remain the
  default. Nested projection INTO a joined doc (`author.name`) is a v1
  non-goal.
- **Subscriptions (the diffability requirement):** expansion runs inside
  `execute_query` (`server/src/query/mod.rs:267`) before the projection seam
  and before `diff_canonical` (`mod.rs:240`), so joined keys are part of the
  canonical form: a child write that changes joined output changes the
  canonical and pushes; a re-run where nothing visible changed diffs equal
  and pushes nothing — "unchanged parents don't re-push" falls out of the
  existing machinery. The part that does NOT fall out is dispatch:
  `fan_out_inner` gates re-runs on `write_set.tables.contains(&entry.query.table)`
  (`server/src/subs.rs:1296`) and the `ReadSet` skip classes (`subs.rs:56`)
  reason only about parent-table windows. With lookups, a subscription's
  effective read set is `{query.table} ∪ {lookup tables}`: the dispatch gate
  must consult the union, and any `write_set` row naming a lookup table must
  force a re-run (the window skip classes remain sound only for parent-table
  writes). Cheap v1 rule: a query with lookups uses the
  re-run-on-any-referenced-table path and lets the canonical diff absorb the
  cost — fine-grained skip optimization across joined tables is deferred.

## Validation and error mapping

All resolved via existing `RtDbError` codes (`server/src/error.rs`); no new
`ErrorCode` (which would trigger the error-code corpus checklist for nothing):

| Condition | Code | Precedent |
| --- | --- | --- |
| Unknown lookup `table` | `NOT_FOUND` | "Unknown table -> NotFound" for the parent (`terminals.rs` compile cascade) |
| `field` not declared, or not `Id` / `Optional<Id>` / `Array<Id>` | `BAD_REQUEST` | filter-field validation (`compile_filter`) |
| Declared `Id.table` ≠ lookup `table` | `BAD_REQUEST` | schema-mismatch rejections |
| `as` collides with a declared field, `_`-prefixed name, or another alias | `BAD_REQUEST` | projection validation |
| `cap` > 1000 | `BAD_REQUEST` | `take > MAX_TAKE` |
| > 4 lookups | `BAD_REQUEST` | resource-cap rejections |
| `lookups` + `count`/`distinct`/`aggregate` | `BAD_REQUEST` | combination rules (code asserted by corpus, message is documentation) |

All checks run in the compile cascade (`compile_query`, `terminals.rs`), so
they fire identically for HTTP one-shots, the WS initial push, `/explain`,
and every subscription re-run — a schema push that retypes a lookup field
makes the next re-run fail validation, which logs-and-skips like any
re-run error (existing behavior; the sub stays stale until the client
re-subscribes, consistent with today's schema-push semantics).

## Implementation sketch

1. **core** (`core/src/query.rs`, `wire-corpus/query-combinations.json`):
   `LookupSpec`, `Query.lookups`, `query_clauses` entry, `"lookups"` clause +
   three forbid rules + per-rule `semantics/query-combo-*.json` cases.
2. **server** — new `server/src/query/lookup.rs` (alongside `row_auth.rs`):
   compile-time validation (table above) invoked from `compile_query`; an
   `expand_lookups(result, schema, q, ctx, pool)` pass called from
   `execute_query` immediately before the `project_result` seam
   (`query/mod.rs:378`). Per lookup: collect distinct ids across the result
   set, one batched `SELECT _id, doc, … WHERE _id = ANY($n)` appended with
   the owner/collaborator fragment (`row_auth_predicate_body`) and the
   `authorize` fragment (`authorize_predicate_body`) plus soft-delete
   visibility — the same `$n`-bind discipline as the parent query — then
   fan out in memory (order, dangling, cap, ceiling). `subs.rs` dispatch gate
   gains the lookup-table union.
3. **clients:** wire types + in-memory expansion per the table above; each
   client's query-combination checker consumes the updated JSON (already
   table-driven for ts/rust/python/swift).
4. **corpus:** cases below, executed by all six runners
   (`wire-corpus/README.md` "Every wire section must be consumed").

### Corpus cases (authoring rule: every behavior-changing change ships a case)

Per `wire-corpus/README.md` ("The authoring rule", semantics; and the
combination-rule specialization):

1. `lookup-single` — collect with an Id-field lookup; joined doc present with
   child system fields.
2. `lookup-array-order-cap` — `Array<Id>` lookup preserves id-array order;
   `cap` truncates to first N.
3. `lookup-authz-omit` — joined row failing `ownerField`/`authorize` is
   null (single) / dropped (array).
4. `lookup-dangling` — dangling id → null (single) / dropped (array).
5. `lookup-unknown-table` — `NOT_FOUND`.
6. `lookup-field-not-id` — `BAD_REQUEST`.
7. `lookup-cap-overflow` — `cap: 1001` → `BAD_REQUEST`.
8. `lookup-projection` — `fields` listing the alias keeps the joined key;
   omitting it drops it.
9. `query-combo-lookups-excludes-count` / `-distinct` / `-aggregate` (rule ids `lookups-excludes-*`) — one
   `semantics/query-combo-<rule-id>.json` per new forbid rule (required by
   `combinations_coverage.rs`).

## Non-goals (v1)

- **No chained/nested lookups** (looking up from a joined doc). Depth-1 only.
  Chaining is additive later (a `lookups` list on `LookupSpec`) if demand
  appears; not cheap enough to fold in now — it changes the expansion pass
  from one batched fetch per lookup to a per-level scheduler and compounds
  the ceiling math.
- **No reverse lookups** (find children whose field points at the parent).
  Different execution shape (per-parent subquery/aggregation), a separate
  feature.
- **No cross-database lookups.** Schemas are db-scoped and `authorize_table`
  is per-db; nothing in the auth model admits it.
- **No filters or arbitrary ON predicates inside a lookup** — id equality
  only. An optional `filter` on `LookupSpec` (compiled like the parent's) is
  the natural later extension.
- **No lookup-aware pagination cursors** — cursors stay parent-index-only.
- **No new `ErrorCode`, no `PROTOCOL_VERSION` bump** — the field is additive.

## Docs to update with the implementation

FEATURE_MATRIX.md (new row + count), README.md's DSL section,
docs/ARCHITECTURE.md's read-path paragraph (expansion sits between terminal
execution and projection), and each client README's query section. Stale
docs contradicting the code are bugs per CLAUDE.md.
