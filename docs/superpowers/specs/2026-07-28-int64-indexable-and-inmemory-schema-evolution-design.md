# int64 indexability (#13) + in-memory additive schema evolution (#19)

**Date:** 2026-07-28
**Status:** Implemented (2026-08-10) — `int64` field is HNSW-indexable (#13, server-only) and the ts/rust clients do additive in-memory schema evolution (#19).
**Scope:** Two independent FEATURE_MATRIX gap closures, both additive. #13 is
server-only (no wire change); #19 is ts-client + rust-client only (no server
change). Plus the doc-sync the matrix itself needs.

## Background

- **#13** — the `int64` field type (a JSON decimal-string validated as `i64`) is
  deliberately non-indexable today (`indexed_column_type` returns `Err` for it;
  `schema.rs:1238` asserts that). Convex has no native int64; par-rt-db exceeds
  it once indexed.
- **#19** — the in-memory test harnesses (`ts-client/src/in_memory.ts`,
  `rust-client/src/in_memory.rs`) wholesale-replace the schema and wipe all docs
  on every `pushSchema`/`push_schema`. The live server is additive-only
  (`ddl.rs::detect_destructive_changes`). The harnesses should mirror that.

Both are mechanical mirrors of existing, proven patterns — no new architecture.

---

## #13 — make `int64` indexable

### Approach
Mirror the `Number → double precision` template with `Int64 → bigint`. The `int64`
wire value is already a decimal JSON string (`schema.rs::is_valid_int64` parses it
to `i64`), so every conversion is a `parse::<i64>()` on that string.

**Server touch points** (all in `server/src/`):

| File | Change |
|---|---|
| `schema.rs:219-236` `indexed_column_type` | add `FieldType::Int64 => Ok(("bigint", false))` |
| `schema.rs:1238-1248` test | drop the `Int64` assertion from `indexed_column_type_rejects_new_non_indexable_types`; add a positive `bigint` case to the matrix test |
| `ddl.rs:61-70` `backfill_expr` | add `"bigint" => (doc->>'{field_name}')::bigint` (used when an int64 field is newly indexed on an existing table) |
| `txn.rs:196-201` `EqBind` | add `I64(i64)` variant |
| `txn.rs:235-254` `eq_bind_for` | add `"bigint"` arm: parse the decimal-string bound value to `i64`, `bad_request` on parse failure |
| `txn.rs:258-265` `ColBind` | add `I64(Option<i64>)` variant |
| `txn.rs:376-401` `scalar_bind` | add `"bigint"` arms (null → `I64(None)`; non-null → parse decimal string → `I64(Some(n))`) |
| `txn.rs` 3 write bind sites (insert `:509`, update `:558`, snapshot `:712`) | add `ColBind::I64(v) => query.bind(v)` |
| `query.rs` ~18 read bind sites (count/distinct/aggregate×2/paginate/main/vector-filter + `txn.rs::eq_lookup`) | add `EqBind::I64(v) => query.bind(v)` |

**What unlocks for free** (all route through `eq_bind_for`): eq-prefix, range
`gt/gte/lt/lte`, keyset cursor pagination, `filter` eq, `count`, `collect`,
`unique`, `take`, `first`. Range SQL is identical for any numeric type (integer
`<`/`<=`/`>`/`>=` on a `bigint` column with an `i64` bind — no cast needed).

**`aggregate` sum/avg/min/max** (decision: **include now**): extend
`query.rs:2099-2105` `is_numeric_index_field` to admit `Int64` (and
`Optional<Int64>`), updating its doc comment. The aggregate SQL projects via
`to_jsonb(SUM/AVG/MIN/MAX(bigint))` into `serde_json::Value`, so results
serialize as JSON **numbers** — consistent with how `Number` aggregates serialize
today. Accepted wrinkle: `SUM(bigint)→numeric` and `AVG(bigint)→numeric` lose
precision past 2^53 when serialized as f64 JSON (deferred to a future
arbitrary-precision aggregate if ever needed). `min`/`max` return exact bigint.

**In-memory harness parity** (ts + rust): today these harnesses compare indexed
values for ordering/range. A `Number` index compares numerically; an int64 index
must too — otherwise `"100" < "50"` lexicographically. Where each harness already
does type-aware `Number` comparison in its query/order/range path, add an int64
branch that parses the decimal string to a numeric value before comparing. (Exact
mechanism confirmed at implementation time against each harness's existing
Number-handling site.)

**No wire/protocol change.** Indexability is declared via the index definition
(a field is indexable iff `indexed_column_type` returns `Ok`); the
`{"type":"int64"}` field tag and `IndexDef` shape already round-trip through all
four clients. No client DSL/builder changes.

### #13 Tests
- Server: drop the int64 rejection assertion; add index-declaration acceptance;
  integration coverage in `query_test.rs`/`txn_test.rs` for int64 eq, range
  (`gt`/`gte`/`lt`/`lte`), `count`, `collect`/`unique`, cursor paginate, and
  `aggregate` sum/avg/min/max over an int64 index; `filter` eq over int64;
  `replace`/`patch` recomputing the int64 column.
- In-memory (ts + rust): int64 index ordering + range correctness.

---

## #19 — in-memory additive schema evolution

### Approach
Port `server/src/ddl.rs::detect_destructive_changes` (lines 75-133) into both
harnesses and rewrite the push path to merge instead of wipe.

**Destructive-change detection** (mirror the server exactly — same `bad_request`
error messages, so harness behavior matches the server):
1. removed table
2. removed field
3. changed field type (deep `PartialEq` on `FieldType`)
4. removed index (by name)
5. changed index `fields`
6. changed index kind (btree ↔ search)
7. changed index vector spec

Like the server, detection walks **old** only (one-directional), so additions
and `ownerField`/`collaboratorsField` changes are not flagged. (The server does
not check those either; the harness matches.)

**Push rewrite** (`pushSchema` in `in_memory.ts:530`, `push_schema` in
`in_memory.rs:259`):
- **First push** (no prior schema): install the schema, seed an empty doc store
  per table. (No wipe needed — nothing to wipe.)
- **Subsequent push**: run destructive detection against the prior schema; on any
  destructive change, throw the same `bad_request` error the server would
  (`RtDbError`-shaped in rust, equivalent in ts). If clean, **merge additively**:
  keep all existing docs and the idempotency cache; add new tables (empty doc
  store); fold new fields and new indexes into the live schema snapshot/per-table
  defs. **Never clear docs or idempotency.**

No backfill is needed in-memory (unlike the server's `f_<field>` columns): the
harness validates docs against `TableDef.fields`/`TableJson.fields` at write time
and scans `docs` + filters in memory on read, so adding a field/index to the
schema snapshot is sufficient.

### #19 Tests
- Replace the test that pins wholesale-replace
  (`rust-client::push_schema_replaces_the_previous_schema`) — under additive
  semantics a second push that drops a table is **destructive** and must throw.
- Add: additive second push (new table + new field + new index) preserves
  existing docs and they remain queryable; a second push that adds an indexed
  field lets new writes populate it and existing docs still read.
- Add: destructive second pushes (removed field, changed field type, removed
  index, changed index fields) each throw the matching `bad_request` error.
- Mirror the above in the ts harness.
- Drop the "deferred" docstring prose on both push functions and update the
  header parity disclaimers.

---

## Doc sync (FEATURE_MATRIX.md)

- **#13 row**: flip to ✅, note `int64` is now indexable as `bigint` (eq/range/
  count/collect/unique/take/first/paginate/filter + sum/avg/min/max), server-only,
  no wire change; note the sum/avg precision wrinkle as deliberate.
- **#19 row**: update the "Deferred gap: additive schema evolution" note to
  shipped — harnesses now mirror the server's additive-only semantics.
- **§5 Status**: bump the date (2026-07-25 → 2026-07-28) and **fix the stale
  python-client paragraph** — it says python ships "wire + DSL only" and that
  HTTP/WS/admin/storage are pending, but `CLAUDE.md` records that HTTP/admin/
  storage **have shipped** (`pip install par-rt-db[http]`, sync `httpx` client)
  and only the reactive **WS** surface remains. (This drift was surfaced by the
  CI failure — `http_client.py` exists and is type-checked.)
- **Header**: bump "gap matrix last updated" date.

## Verification
- `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests
  across all five packages), run after each implementation phase.
- Confirm the in-memory harness int64 comparison fix doesn't regress existing
  Number/string index ordering tests.

## Out of scope
- Ordered top-N boundary invalidation (separate backlog item, tracked in kanban).
- Arbitrary-precision `sum`/`avg` aggregate results (the accepted f64 wrinkle).
- Per-row auth collaborator/role fields (#20 model B) and the predicate DSL (#20
  model C) — unrelated.
- Python-client reactive WS surface — the one remaining client-parity item, not
  touched here.
