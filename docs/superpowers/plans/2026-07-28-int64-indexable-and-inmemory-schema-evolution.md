# int64 indexability (#13) + in-memory additive schema evolution (#19) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `int64` field type fully indexable (server + both in-memory harnesses), and make the two in-memory harnesses mirror the server's additive-only schema-push semantics.

**Architecture:** #13 mirrors the existing `Number → double precision` template as `Int64 → bigint` — a new `I64` arm in the server's two bind enums (`ColBind`/`EqBind`) plus a `bigint` case in the type→column map and backfill cast, threaded through every bind-emission site. The in-memory harnesses each have their own type→storage map and value comparator, which get a new `int64` storage variant whose comparator parses the decimal string to a number. #19 ports `ddl.rs::detect_destructive_changes` into both harnesses and rewrites `pushSchema`/`push_schema` to merge additively instead of wipe.

**Tech Stack:** Rust (axum/sqlx/tokio, server + rust-client), TypeScript (ts-client, bun/vitest), vitest, cargo integration tests against a real Postgres.

## Global Constraints

- **Definition of done:** `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests across server, ts-client, rust-client, dashboard, python-client). Run from repo root after each batch. `make dev-db-up` is a hard prereq of `make test` (integration tests hit a real Postgres on `127.0.0.1:55434`).
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`.
- **SQL safety:** identifiers double-quoted, values `$n`-bound. The int64 value is a decimal JSON string — parse to `i64` in Rust before binding; never interpolate.
- **Wire contract unchanged:** the `{"type":"int64"}` field tag and `IndexDef` shape already round-trip through all four clients. #13 adds no wire/DSL surface — indexability is declared via the index definition and resolved server-side by `indexed_column_type`.
- **Match exact casing** of error messages to the server (`removed table '<name>'`, etc.) so in-memory behavior mirrors the server.
- **Surgical:** touch only the sites each task lists. Match surrounding style.

## File Structure

| File | Responsibility | Touched by |
|---|---|---|
| `server/src/schema.rs` | `FieldType` enum, `indexed_column_type` (type→pg column map) | T1 |
| `server/src/ddl.rs` | schema→DDL: `backfill_expr` cast map, `push_schema` | T1 |
| `server/src/txn.rs` | `ColBind`/`EqBind` bind enums, `scalar_bind`, `eq_bind_for`, write bind sites, `eq_lookup` | T1 |
| `server/src/query.rs` | read bind-emission sites (~18), `is_numeric_index_field` | T1 |
| `server/tests/query_test.rs`, `txn_test.rs`, `schema_test.rs`(in `schema.rs`) | integration + unit tests | T1 |
| `ts-client/src/in_memory.ts` | `PgType`, `indexColumnType`, `coerceIndexValue`, `compareIndexValues`, callers, aggregate `isNumeric`, `pushSchema` | T2, T3 |
| `ts-client/tests/in_memory.test.ts` | harness tests | T2, T3 |
| `rust-client/src/in_memory.rs` | `PgType`, `index_column_type`, `coerce_index_value`, `compare_index_values`, callers, `push_schema` | T4, T5 |
| `FEATURE_MATRIX.md` | parity doc sync | T6 |

**Parallelization:** three disjoint package streams — server (T1), ts-client (T2→T3), rust-client (T4→T5). Run as two batches:
- **Batch A (concurrent):** T1, T2, T4 (disjoint files). Then `make checkall`.
- **Batch B (concurrent):** T3, T5 (T2/T4 done, so the shared `in_memory` files are no longer mid-flight). Then `make checkall`.
- **T6** last. Then final `make checkall`.
Within ts-client, T2 and T3 both edit `in_memory.ts` + `in_memory.test.ts` → they are sequenced across batches (never concurrent). Same for rust T4/T5.

---

## Task 1: Server — `int64` indexable end-to-end (incl. aggregate)

**Files:**
- Modify: `server/src/schema.rs` (`indexed_column_type` ~219-236; test `indexed_column_type_rejects_new_non_indexable_types` ~1238-1248; positive matrix test ~980-1039)
- Modify: `server/src/ddl.rs` (`backfill_expr` ~61-70)
- Modify: `server/src/txn.rs` (`EqBind` ~196-201, `eq_bind_for` ~235-254, `ColBind` ~258-265, `scalar_bind` ~376-401, write bind sites ~509-516/~558-565/~712-719, `eq_lookup` ~800-806)
- Modify: `server/src/query.rs` (every `match bind { EqBind::… }` block; `is_numeric_index_field` ~2099-2105)
- Test: `server/tests/query_test.rs`, `server/tests/txn_test.rs`

**Interfaces:**
- Consumes: the existing `Number → double precision` path as the template.
- Produces: `EqBind::I64(i64)` and `ColBind::I64(Option<i64>)` variants, consumed by every bind-emission match in `txn.rs`/`query.rs`; `indexed_column_type` now returns `Ok(("bigint", false))` for `Int64`.

- [ ] **Step 1: Write the failing tests** in `server/tests/query_test.rs`. Add a schema + insert helper, then tests for eq, numeric range, count, paginate, and aggregate over an int64 index.

```rust
fn int64_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({
        "tables": {
            "events": {
                "fields": {
                    "ts": { "type": "int64" },
                    "kind": { "type": "string" }
                },
                "indexes": [{ "name": "by_ts", "fields": ["ts"] }]
            }
        }
    }))
    .expect("parse int64 schema")
}

async fn insert_event(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    ts: &str,
    kind: &str,
) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool, db, schema,
        &Transaction { steps: vec![Step::Insert {
            table: "events".to_string(),
            doc: doc(serde_json::json!({ "ts": ts, "kind": kind })),
        }] },
        None,
    ).await?;
    Ok(outcome.results[0]["id"].as_str().expect("id string").to_string())
}

fn docs_kinds(result: QueryResult) -> Vec<String> {
    match result {
        QueryResult::Docs(docs) => docs
            .iter()
            .map(|d| d["kind"].as_str().expect("kind").to_string())
            .collect(),
        _ => panic!("expected Docs, got {result:?}"),
    }
}

#[tokio::test]
async fn int64_index_range_and_eq_compare_numerically() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = int64_schema();
    insert_event(&pool, &db, &schema, "100", "a").await?;
    insert_event(&pool, &db, &schema, "20", "b").await?;
    insert_event(&pool, &db, &schema, "3", "c").await?;

    // Numeric range [20, +inf) asc -> ["b" (20), "a" (100)] — NOT lexicographic
    // ("100" would sort before "20" as strings).
    let r = execute_query(&pool, &db, &schema, &Query {
        table: "events".to_string(), get: None,
        index: Some("by_ts".to_string()), eq: vec![],
        gt: None, gte: Some(serde_json::json!("20")), lt: None, lte: None,
        order: Some(Order::Asc), take: Some(10),
        unique: false, first: false, count: false, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None, aggregate: None,
    }, None).await?;
    assert_eq!(docs_kinds(r), vec!["b".to_string(), "a".to_string()]);

    // eq on the int64 field matches the decimal-string value.
    let r = execute_query(&pool, &db, &schema, &Query {
        table: "events".to_string(), get: None,
        index: Some("by_ts".to_string()), eq: vec![serde_json::json!("100")],
        gt: None, gte: None, lt: None, lte: None,
        order: None, take: Some(10),
        unique: false, first: false, count: false, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None, aggregate: None,
    }, None).await?;
    assert_eq!(docs_kinds(r), vec!["a".to_string()]);
    Ok(())
}

#[tokio::test]
async fn int64_index_count_and_aggregate() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = int64_schema();
    insert_event(&pool, &db, &schema, "10", "a").await?;
    insert_event(&pool, &db, &schema, "20", "b").await?;
    insert_event(&pool, &db, &schema, "30", "c").await?;

    let r = execute_query(&pool, &db, &schema, &Query {
        table: "events".to_string(), get: None,
        index: Some("by_ts".to_string()), eq: vec![],
        gt: None, gte: None, lt: None, lte: None, order: None, take: None,
        unique: false, first: false, count: true, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None, aggregate: None,
    }, None).await?;
    assert!(matches!(r, QueryResult::Count(3)));

    let r = execute_query(&pool, &db, &schema, &Query {
        table: "events".to_string(), get: None,
        index: Some("by_ts".to_string()), eq: vec![],
        gt: None, gte: None, lt: None, lte: None, order: None, take: None,
        unique: false, first: false, count: false, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None,
        aggregate: Some(AggregateSpec { op: AggregateOp::Sum, field: "ts".to_string() }),
    }, None).await?;
    // SUM(bigint) projects via to_jsonb -> JSON number.
    assert!(matches!(r, QueryResult::Aggregate(ref v) if v.as_f64() == Some(60.0)));
    Ok(())
}
```

Add to `server/tests/txn_test.rs` a test that `patch`/`replace` recompute the int64 column (insert `ts:"5"`, patch to `ts:"50"`, then a `gte("20")` query matches it). Follow the existing `txn_test.rs` helper style.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --test query_test int64_index`
Expected: FAIL — `RtDbError` "field type 'int64' is not indexable" at schema-push/query time (the schema push compiles the index and calls `indexed_column_type`, which rejects int64).

- [ ] **Step 3: Admit int64 to the type→column map.** In `server/src/schema.rs::indexed_column_type`, add an arm before the `other =>` catch-all:

```rust
FieldType::Int64 => Ok(("bigint", false)),
```

- [ ] **Step 4: Add the bigint backfill cast.** In `server/src/ddl.rs::backfill_expr`, add an arm:

```rust
"bigint" => Ok(format!("(doc->>'{field_name}')::bigint")),
```

- [ ] **Step 5: Add the write-side bind variant + extraction.** In `server/src/txn.rs`:

Add to `ColBind`:
```rust
I64(Option<i64>),
```
In `scalar_bind`, add a null arm alongside the others:
```rust
"bigint" => Ok(ColBind::I64(None)),
```
and a non-null arm (the wire value is a decimal JSON string):
```rust
"bigint" => value
    .as_str()
    .and_then(|s| s.parse::<i64>().ok())
    .map(|n| ColBind::I64(Some(n)))
    .ok_or_else(|| RtDbError::internal("expected int64 string value for indexed column")),
```
Add `ColBind::I64(v) => query.bind(v),` to all three write bind-emission matches (insert ~512, update ~560, snapshot ~714).

- [ ] **Step 6: Add the read-side bind variant + extraction.** In `server/src/txn.rs`:

Add to `EqBind`:
```rust
I64(i64),
```
In `eq_bind_for`, add a `"bigint"` arm:
```rust
"bigint" => value
    .as_str()
    .and_then(|s| s.parse::<i64>().ok())
    .map(EqBind::I64)
    .ok_or_else(|| RtDbError::bad_request("eq value must be an int64 string")),
```
Add `EqBind::I64(v) => query.bind(v),` to the `eq_lookup` bind match (~804).

- [ ] **Step 7: Thread the new arm through every read bind site in `query.rs`.** Add `EqBind::I64(v) => query.bind(v),` to every `match bind { EqBind::Text(v) => …, EqBind::Num(v) => …, EqBind::Bool(v) => … }` block. The sites (from the map): count terminal, distinct terminal, grouped aggregate, scalar aggregate, paginate, main select/unique/first/collect, vector-search filter binds, and cursor binds if they match on `EqBind`. (The range-comparison SQL and `eq_bind_for`-based cursor typing need no change — they route through `eq_bind_for`.)

- [ ] **Step 8: Admit int64 to aggregate sum/avg.** In `server/src/query.rs::is_numeric_index_field`, extend the match and update the doc comment:

```rust
/// Whether an indexed field's declared type is numeric enough for `sum`/`avg`.
/// `Number` and `Int64` both qualify (`Optional<…>` unwraps one layer). Note:
/// `SUM(bigint)`/`AVG(bigint)` return Postgres `numeric`, which serializes as a
/// JSON number (f64) — precision is lost past 2^53; accepted trade-off.
fn is_numeric_index_field(ty: &FieldType) -> bool {
    match ty {
        FieldType::Number | FieldType::Int64 => true,
        FieldType::Optional { inner } => is_numeric_index_field(inner),
        _ => false,
    }
}
```

- [ ] **Step 9: Update the schema unit tests.** In `server/src/schema.rs`:
  - In `indexed_column_type_rejects_new_non_indexable_types`, **delete** the `assert!(indexed_column_type(&FieldType::Int64).is_err());` line (keep the Bytes/Any/Record assertions).
  - In the positive matrix test (`indexed_column_type_matrix` ~980-1039), add `assert_eq!(indexed_column_type(&FieldType::Int64).unwrap(), ("bigint", false));`.

- [ ] **Step 10: Run the full server suite.**

Run: `cd server && cargo test`
Expected: PASS, including the new int64 tests. clippy clean: `cargo clippy --all-targets -- -D warnings`.

- [ ] **Step 11: Commit**

```bash
git add server/src/schema.rs server/src/ddl.rs server/src/txn.rs server/src/query.rs server/tests/query_test.rs server/tests/txn_test.rs
git commit -m "feat(query): make int64 indexable as bigint (#13)

Mirror the Number→double precision path as Int64→bigint: new ColBind::I64 /
EqBind::I64 arms threaded through every write/read bind site, a bigint arm in
indexed_column_type + backfill_expr, and is_numeric_index_field now admits int64
so aggregate sum/avg/min/max work. Unlocks eq/range/count/collect/unique/
paginate/filter + aggregates over int64 indexes. Server-only; no wire change."
```

---

## Task 2: ts-client in-memory — `int64` indexable

**Files:**
- Modify: `ts-client/src/in_memory.ts` (`PgType` ~45, `indexColumnType` ~273-299, `coerceIndexValue` ~302-325, `compareIndexValues` ~327-351, sort caller ~1355-1376, range caller ~1218-1235, distinct sort ~1274, aggregate `isNumeric` ~1292)
- Test: `ts-client/tests/in_memory.test.ts`

**Interfaces:**
- Consumes: the existing `number` storage type as the template.
- Produces: `"int64"` in the `PgType` union; `compareIndexValues` gains an optional `pg` parameter so int64 decimal strings compare numerically.

- [ ] **Step 1: Write the failing tests** in `ts-client/tests/in_memory.test.ts`. Add a schema with an int64 index and tests that ordering/range/distinct are numeric, not lexicographic:

```ts
const int64Schema = defineSchema({
  events: defineTable({
    ts: schema.int64(),
    kind: schema.string(),
  }).index("by_ts", ["ts"]),
});

it("in-memory int64 index orders and ranges numerically", async () => {
  const c = new InMemoryRtDbClient();
  c.pushSchema(int64Schema);
  await c.mutate(api.events.insert({ ts: "100", kind: "a" }));
  await c.mutate(api.events.insert({ ts: "20", kind: "b" }));
  await c.mutate(api.events.insert({ ts: "3", kind: "c" }));

  const asc = await c.query(api.events.query().index("by_ts").order("asc").take(10));
  expect(asc.map((d) => d.kind)).toEqual(["c", "b", "a"]); // 3, 20, 100 — not "100","20","3"

  const range = await c.query(api.events.query().index("by_ts").gte("20").take(10));
  expect(range.map((d) => d.kind)).toEqual(["b", "a"]); // 20, 100
});
```

(Mirror the existing test file's `defineSchema`/`api`/`new InMemoryRtDbClient()` helpers — see the `items` schema tests at the top of the file. Adjust builder method names to match `TableQuery` exactly, e.g. `.index("by_ts")` vs the established builder spelling.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts -t int64`
Expected: FAIL — `SCHEMA_VIOLATION "field type 'int64' is not indexable"` from `indexColumnType`.

- [ ] **Step 3: Add the `int64` storage type.** In `ts-client/src/in_memory.ts`:

Extend the `PgType` union (~45):
```ts
type PgType = "text" | "number" | "boolean" | "int64";
```
In `indexColumnType` (~273-299), add a case:
```ts
case "int64":
  return { pg: "int64", nullable: false };
```
In `coerceIndexValue` (~302-325), add a case that validates the decimal-string form and returns it **as a string** (eq is string `===` string; the comparator handles numeric ordering):
```ts
case "int64":
  if (typeof value !== "string" || !/^[+-]?\d+$/.test(value)) {
    throw new RtDbError("BAD_REQUEST", "eq value must be an int64 string");
  }
  return value;
```

- [ ] **Step 4: Make the comparator int64-aware.** In `compareIndexValues` (~327-351), add an optional `pg` parameter; when `pg === "int64"`, parse both operands via `BigInt` and compare numerically (exact — no 2^53 limit):

```ts
function compareIndexValues(a: unknown, b: unknown, pg?: PgType): number {
  const aNull = a === null || a === undefined;
  const bNull = b === null || b === undefined;
  if (aNull && bNull) return 0;
  if (aNull) return 1;
  if (bNull) return -1;
  if (pg === "int64") {
    const an = BigInt(a as string);
    const bn = BigInt(b as string);
    if (an < bn) return -1;
    if (an > bn) return 1;
    return 0;
  }
  const av = a as number | string;
  const bv = b as number | string;
  if (av < bv) return -1;
  if (av > bv) return 1;
  return 0;
}
```

- [ ] **Step 5: Thread `pg` from the callers.** Pass the field's storage type into each `compareIndexValues` call:
  - Sort caller (~1355-1376): for index-field keys use `indexColumnType(this.schema.tables[q.table].fields[field]).pg`; for `__createdAt` pass `"number"`; for `__id` pass `"text"`.
  - Range caller (~1218-1235): pass `indexColumnType(tableDef.fields[rangeField]).pg` to each of the four bound comparisons.
  - Distinct sort (~1274): pass the distinct field's `pg`.

- [ ] **Step 6: Admit int64 to the aggregate numeric check.** In the aggregate `isNumeric` helper (~1292), also return `true` for `FieldType.type === "int64"` (and `optional<int64>`), mirroring the server's `is_numeric_index_field`.

- [ ] **Step 7: Run the ts-client suite.**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts`
Expected: PASS (new int64 tests + no regressions in existing Number/string ordering tests). Then `cd ts-client && bun run typecheck && bun run lint`.

- [ ] **Step 8: Commit**

```bash
git add ts-client/src/in_memory.ts ts-client/tests/in_memory.test.ts
git commit -m "feat(ts-client): int64 indexable in the in-memory harness (#13)

Add an int64 storage type whose comparator parses the decimal string to BigInt
so ordering/range/distinct are numeric (not lexicographic), and admit int64 to
the aggregate numeric check. eq stays string===string."
```

---

## Task 3: ts-client in-memory — additive schema evolution (#19)

**Files:**
- Modify: `ts-client/src/in_memory.ts` (add `detectDestructiveChanges`; rewrite `pushSchema` ~527-537)
- Test: `ts-client/tests/in_memory.test.ts`

**Interfaces:**
- Consumes: `ddl.rs::detect_destructive_changes` (server/src/ddl.rs:75-133) as the semantic reference.
- Produces: `pushSchema` now merges additively and throws `BAD_REQUEST` on destructive changes (matching the server's messages).

- [ ] **Step 1: Write the failing tests** in `ts-client/tests/in_memory.test.ts`:

```ts
describe("InMemoryRtDbClient — additive schema push", () => {
  it("an additive second push preserves existing docs", async () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(itemsSchema); // has table `items`
    await c.mutate(api.items.insert({ name: "x", /* …required fields… */ }));
    c.pushSchema(additiveItemsSchema); // items + a new field + a new table
    const docs = await c.query(api.items.query().collect());
    expect(docs).toHaveLength(1); // not wiped
  });

  it("a destructive second push throws BAD_REQUEST", () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(itemsSchema);
    const onlyOther = defineSchema({ solo: defineTable({ x: schema.number() }) });
    expect(() => c.pushSchema(onlyOther)).toThrow(/removed table 'items'/);
  });

  it("removing a field is destructive", () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(itemsSchema);
    expect(() => c.pushSchema(itemsWithoutAField)).toThrow(/removed field 'items\./);
  });
});
```

(Construct `additiveItemsSchema`/`itemsWithoutAField` from the existing `itemsSchema` fixture at the top of the file, adding/removing one field or table.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts -t "additive schema push"`
Expected: FAIL — the additive test sees a wiped table (`length 0`); the destructive test does not throw.

- [ ] **Step 3: Add `detectDestructiveChanges`.** In `ts-client/src/in_memory.ts`, add a free function mirroring the server's checks (deep equality via `JSON.stringify` on field type / index fields / vector spec):

```ts
function detectDestructiveChanges(oldSchema: SchemaJson, newSchema: SchemaJson): void {
  for (const [tableName, oldTable] of Object.entries(oldSchema.tables)) {
    const newTable = newSchema.tables[tableName];
    if (!newTable) {
      throw new RtDbError("BAD_REQUEST", `removed table '${tableName}'`);
    }
    for (const [fieldName, oldFieldType] of Object.entries(oldTable.fields)) {
      const newFieldType = newTable.fields[fieldName];
      if (!newFieldType) {
        throw new RtDbError("BAD_REQUEST", `removed field '${tableName}.${fieldName}'`);
      }
      if (JSON.stringify(newFieldType) !== JSON.stringify(oldFieldType)) {
        throw new RtDbError("BAD_REQUEST", `changed type of field '${tableName}.${fieldName}'`);
      }
    }
    for (const oldIndex of oldTable.indexes ?? []) {
      const newIndex = (newTable.indexes ?? []).find((i) => i.name === oldIndex.name);
      if (!newIndex) {
        throw new RtDbError("BAD_REQUEST", `removed index '${oldIndex.name}'`);
      }
      if (JSON.stringify(newIndex.fields) !== JSON.stringify(oldIndex.fields)) {
        throw new RtDbError("BAD_REQUEST", `changed fields of index '${oldIndex.name}'`);
      }
      if (!!newIndex.search !== !!oldIndex.search) {
        throw new RtDbError("BAD_REQUEST", `changed kind of index '${oldIndex.name}' (btree <-> search)`);
      }
      if (JSON.stringify(newIndex.vector ?? null) !== JSON.stringify(oldIndex.vector ?? null)) {
        throw new RtDbError("BAD_REQUEST", `changed vector spec of index '${oldIndex.name}'`);
      }
    }
  }
}
```

- [ ] **Step 4: Rewrite `pushSchema` to merge.** Replace the body (~530-537) so it never wipes existing docs/idempotency:

```ts
pushSchema(schema: SchemaDefinition<any> | SchemaJson): void {
  const next = toSchemaJson(schema);
  if (this.schema) {
    detectDestructiveChanges(this.schema, next);
    // Additive: keep existing tables' rows + idempotency cache; only seed empty
    // doc stores for brand-new tables.
    for (const tableName of Object.keys(next.tables)) {
      if (!this.tables.has(tableName)) {
        this.tables.set(tableName, new Map());
      }
    }
  } else {
    for (const tableName of Object.keys(next.tables)) {
      this.tables.set(tableName, new Map());
    }
  }
  this.schema = next;
}
```

Also drop the "(The live server is additive-only; full additive evolution is deferred.)" sentence from the `pushSchema` doc comment (~527-529).

- [ ] **Step 5: Run the ts-client suite + typecheck + lint.**

Run: `cd ts-client && bunx vitest run tests/in_memory.test.ts && bun run typecheck && bun run lint`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ts-client/src/in_memory.ts ts-client/tests/in_memory.test.ts
git commit -m "feat(ts-client): additive schema evolution in the in-memory harness (#19)

Mirror ddl.rs::detect_destructive_changes: a second pushSchema now merges
additively (preserving existing docs + idempotency) and throws BAD_REQUEST on
removed tables/fields/indexes or changed field/index types, matching the server."
```

---

## Task 4: rust-client in-memory — `int64` indexable

**Files:**
- Modify: `rust-client/src/in_memory.rs` (`PgType` ~1692-1697, `index_column_type` ~1710-1759, `coerce_index_value` ~1764-1803, `compare_index_values` ~1805-1834, sort caller ~628-648, range caller ~586-611)
- Test: `rust-client/src/in_memory.rs::tests`

**Interfaces:**
- Consumes: the existing `Number`/`PgType::Number` path as the template.
- Produces: `PgType::Int64`; `compare_index_values` gains a `pg: PgType` parameter.

- [ ] **Step 1: Write the failing tests** in the `tests` module. Add an int64-index schema and tests for numeric ordering + range:

```rust
fn int64_test_schema() -> SchemaDef {
    Schema::builder()
        .table("events", Table::new()
            .field("ts", FieldType::Int64)
            .field("kind", FieldType::String)
            .index("by_ts", &["ts"]))
        .build()
}

#[test]
fn int64_index_orders_and_ranges_numerically() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&int64_test_schema());
    for (ts, kind) in [("100", "a"), ("20", "b"), ("3", "c")] {
        c.mutate(&Transaction { steps: vec![Step::Insert {
            table: "events".into(),
            doc: serde_json::json!({ "ts": ts, "kind": kind }).as_object().unwrap().clone(),
        }] }, None).unwrap();
    }
    let r = c.run_query(&Query {
        table: "events".into(), get: None,
        index: Some("by_ts".into()), eq: vec![],
        gt: None, gte: None, lt: None, lte: None,
        order: Some(Order::Asc), take: Some(10),
        unique: false, first: false, count: false, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None, aggregate: None,
    }).unwrap();
    let kinds = doc_kinds(&r); // helper matching QueryResult::Docs -> Vec<String>
    assert_eq!(kinds, vec!["c".to_string(), "b".to_string(), "a".to_string()]); // 3,20,100

    let r = c.run_query(&Query {
        table: "events".into(), get: None,
        index: Some("by_ts".into()), eq: vec![],
        gt: None, gte: Some(serde_json::json!("20")), lt: None, lte: None,
        order: Some(Order::Asc), take: Some(10),
        unique: false, first: false, count: false, distinct: false,
        paginate: None, filter: None, search: None, vector_search: None,
        hybrid_search: None, aggregate: None,
    }).unwrap();
    assert_eq!(doc_kinds(&r), vec!["b".to_string(), "a".to_string()]);
}
```

(Confirm the exact `Schema`/`Table` builder + `Query` field spellings against `rust-client/src/schema.rs` and the existing `test_schema()`/`run_query` tests; the rust harness has no `distinct`/`aggregate` terminals, so don't test those here.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust-client && cargo test --features in_memory int64_index`
Expected: FAIL — `SchemaViolation "field type 'int64' is not indexable"` from `index_column_type`.

- [ ] **Step 3: Add the `Int64` storage type.** In `rust-client/src/in_memory.rs`:

Add to `PgType` (~1692-1697):
```rust
pub enum PgType { Text, Number, Boolean, Int64 }
```
In `index_column_type` (~1710-1759), add an arm:
```rust
FieldType::Int64 => PgType::Int64,
```
In `coerce_index_value` (~1764-1803), add a case that validates the decimal string and returns it unchanged (`Value::String`), so eq stays structural:
```rust
PgType::Int64 => {
    match value.as_str().and_then(|s| s.parse::<i64>().ok()) {
        Some(_) => {}
        None => return Err(RtDbError::new(
            ErrorCode::BadRequest, "eq value must be an int64 string")),
    }
}
```

- [ ] **Step 4: Make the comparator int64-aware.** In `compare_index_values` (~1805-1834), add a `pg: PgType` parameter and parse int64 strings to `i64` (exact):

```rust
pub fn compare_index_values(a: &Value, b: &Value, pg: PgType) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a_null = a.is_null();
    let b_null = b.is_null();
    if a_null && b_null { return Ordering::Equal; }
    if a_null { return Ordering::Greater; }
    if b_null { return Ordering::Less; }
    if pg == PgType::Int64 {
        let an = a.as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(i64::MIN);
        let bn = b.as_str().and_then(|s| s.parse::<i64>().ok()).unwrap_or(i64::MIN);
        return an.cmp(&bn);
    }
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            an.as_f64().unwrap_or(f64::NAN).partial_cmp(&bn.as_f64().unwrap_or(f64::NAN)).unwrap_or(Ordering::Equal)
        }
        (Value::String(a_s), Value::String(b_s)) => a_s.cmp(b_s),
        (Value::Bool(ab), Value::Bool(bb)) => ab.cmp(bb),
        _ => Ordering::Equal,
    }
}
```

- [ ] **Step 5: Thread `pg` from the callers.** Update both call sites to pass the field's storage type:
  - Sort caller (~628-648): for index fields, `index_column_type(&table_def.fields[*field])?.pg`; for `created_at` pass `PgType::Number`; for `id` pass `PgType::Text`.
  - Range caller (~586-611): pass `index_column_type(&table_def.fields[field])?.pg` to each `compare_index_values` call.

- [ ] **Step 6: Run the rust-client suite + clippy.**

Run: `cd rust-client && cargo test --all-features && cargo clippy --all-features --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add rust-client/src/in_memory.rs
git commit -m "feat(rust-client): int64 indexable in the in-memory harness (#13)

Add PgType::Int64 and parse the decimal string to i64 in compare_index_values so
ordering/range are numeric. eq stays Value structural equality on the string."
```

---

## Task 5: rust-client in-memory — additive schema evolution (#19)

**Files:**
- Modify: `rust-client/src/in_memory.rs` (add `detect_destructive_changes`; rewrite `push_schema` ~259-267; update `push_schema_replaces_the_previous_schema` test ~2358)
- Test: `rust-client/src/in_memory.rs::tests`

**Interfaces:**
- Consumes: `ddl.rs::detect_destructive_changes` (server/src/ddl.rs:75-133). `FieldType` derives `PartialEq`, so field-type comparison is direct (`!=`), not JSON.
- Produces: `push_schema` merges additively and returns `Err(BadRequest)` on destructive changes.

- [ ] **Step 1: Write the failing tests.** Replace the `push_schema_replaces_the_previous_schema` test (~2358, which currently pins wholesale-replace) with additive-semantics tests:

```rust
#[test]
fn push_schema_rejects_a_destructive_second_push() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&test_schema());
    let only_other = Schema::builder()
        .table("solo", Table::new().field("x", FieldType::Number))
        .build();
    let err = c.push_schema(&only_other).unwrap_err();
    assert!(matches!(err.code, ErrorCode::BadRequest));
    assert!(err.message.contains("removed table 'items'"));
}

#[test]
fn push_schema_additively_preserves_docs() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&test_schema());
    c.mutate(&Transaction { steps: vec![Step::Insert {
        table: "items".into(),
        doc: serde_json::json!({ /* …test_schema's required item fields… */ }).as_object().unwrap().clone(),
    }] }, None).unwrap();
    // Add a new field + new table on top.
    let additive = /* test_schema() + one new optional field, built via Schema::builder */;
    c.push_schema(&additive).unwrap();
    let r = c.run_query(&Query { table: "items".into(), /* …collect… */ }).unwrap();
    assert!(matches!(r, QueryResult::Docs(ref d) if !d.is_empty())); // not wiped
}
```

(Adjust `push_schema`'s signature to return `Result<(), RtDbError>` — see Step 3 — and construct `additive` from `test_schema()`'s builder.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd rust-client && cargo test --features in_memory push_schema`
Expected: FAIL — `push_schema` returns `()` (won't compile with `unwrap_err()`), and the additive test wipes docs.

- [ ] **Step 3: Add `detect_destructive_changes`.** In `rust-client/src/in_memory.rs`, add a free function returning `Result<(), RtDbError>`, mirroring the server (`FieldType`/`IndexDef` compare with `!=`):

```rust
fn detect_destructive_changes(old: &SchemaDef, new: &SchemaDef) -> Result<(), RtDbError> {
    for (table_name, old_table) in &old.tables {
        let new_table = new.tables.get(table_name).ok_or_else(|| {
            RtDbError::new(ErrorCode::BadRequest, format!("removed table '{table_name}'"))
        })?;
        for (field_name, old_field_type) in &old_table.fields {
            match new_table.fields.get(field_name) {
                None => return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("removed field '{table_name}.{field_name}'"))),
                Some(new_field_type) if new_field_type != old_field_type => return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("changed type of field '{table_name}.{field_name}'"))),
                _ => {}
            }
        }
        for old_index in old_table.indexes.iter().flatten() {
            let new_index = new_table.indexes.iter().flatten().find(|i| i.name == old_index.name);
            let new_index = match new_index {
                None => return Err(RtDbError::new(
                    ErrorCode::BadRequest, format!("removed index '{}'", old_index.name))),
                Some(i) => i,
            };
            if new_index.fields != old_index.fields {
                return Err(RtDbError::new(ErrorCode::BadRequest,
                    format!("changed fields of index '{}'", old_index.name)));
            }
            if new_index.search != old_index.search {
                return Err(RtDbError::new(ErrorCode::BadRequest,
                    format!("changed kind of index '{}' (btree <-> search)", old_index.name)));
            }
            if new_index.vector != old_index.vector {
                return Err(RtDbError::new(ErrorCode::BadRequest,
                    format!("changed vector spec of index '{}'", old_index.name)));
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 4: Rewrite `push_schema` to return `Result` and merge.** Change the signature and body (~259-267) — remove the three `.clear()` calls:

```rust
pub fn push_schema(&mut self, schema: &SchemaDef) -> Result<(), RtDbError> {
    if let Some(prev) = &self.schema {
        detect_destructive_changes(prev, schema)?;
    }
    self.schema = Some(schema.clone());
    for (name, def) in &schema.tables {
        self.tables.insert(name.clone(), def.clone());
    }
    Ok(())
}
```

**Callers of `push_schema`** (tests + any internal call sites) must now handle the `Result` — update them (`.unwrap()`/`?` as appropriate). Drop the "(The live server is additive-only; that evolution is deferred here.)" docstring sentence (~252-254).

- [ ] **Step 5: Run the rust-client suite + clippy.**

Run: `cd rust-client && cargo test --all-features && cargo clippy --all-features --all-targets -- -D warnings`
Expected: PASS (note: `push_schema`'s signature change is `in_memory`-feature-gated; confirm no non-`in_memory` caller exists — `grep -rn "push_schema" rust-client/src rust-client/tests`).

- [ ] **Step 6: Commit**

```bash
git add rust-client/src/in_memory.rs
git commit -m "feat(rust-client): additive schema evolution in the in-memory harness (#19)

Mirror ddl.rs::detect_destructive_changes: push_schema now returns Result, merges
additively (preserving docs + idempotency), and rejects removed/changed tables,
fields, and indexes with BAD_REQUEST — matching the server."
```

---

## Task 6: FEATURE_MATRIX.md doc sync

**Files:**
- Modify: `FEATURE_MATRIX.md`

- [ ] **Step 1: Update the #13 row.** Change the par-rt-db cell to ✅ and rewrite the note: int64 is now indexable as `bigint` (server-only, no wire change) — eq/range/count/collect/unique/take/first/paginate/filter + aggregate sum/avg/min/max; note the deliberate f64 precision wrinkle for `sum`/`avg` over values past 2^53; note in-memory harness parity (ts + rust).

- [ ] **Step 2: Update the #19 row.** Change the "Deferred gap: additive schema evolution (marked as TODOs)." sentence to shipped — both in-memory harnesses now mirror the server's additive-only semantics (detect destructive changes, merge additively, preserve docs).

- [ ] **Step 3: Fix the stale python-client paragraph in §5.** It currently says python ships "wire + schema/mutation/query DSL only" and that HTTP/WS/admin/storage are pending. Correct it to: HTTP/admin/storage **have shipped** (`pip install par-rt-db[http]`, sync `httpx` client); only the **reactive WS** surface remains pending. (Surfaced by the httpx CI failure — `http_client.py` exists and is type-checked.)

- [ ] **Step 4: Bump the dates.** Header "gap matrix last updated 2026-07-25" → `2026-07-28`; §5 "As of 2026-07-25" → `2026-07-28`.

- [ ] **Step 5: Commit**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs: sync FEATURE_MATRIX — #13 int64 indexable, #19 in-memory additive evolution

Flip #13 and #19 to shipped; fix the stale §5 python-client paragraph (HTTP/
admin/storage shipped, only reactive WS pending — surfaced by the httpx CI
failure); bump the matrix dates to 2026-07-28."
```

---

## Self-Review

**1. Spec coverage:**
- #13 server indexability (all terminals + aggregate) → Task 1. ✓
- #13 in-memory comparison parity (ts + rust) → Tasks 2, 4. ✓
- #19 additive evolution (ts + rust) → Tasks 3, 5. ✓
- FEATURE_MATRIX doc-sync (incl. stale python paragraph + dates) → Task 6. ✓
- sum/avg inclusion decision → Task 1 Step 8, Task 2 Step 6. ✓

**2. Placeholder scan:** Test code uses `/* … */` only for fixture-field values the implementer must copy from the existing `items`/`test_schema()` fixtures (named explicitly) — not for logic. All production code is complete. No "TBD"/"implement later".

**3. Type consistency:** `EqBind::I64(i64)` / `ColBind::I64(Option<i64>)` (server); `PgType::Int64` (rust) / `"int64"` (ts); `compare_index_values(a, b, pg)` (rust) / `compareIndexValues(a, b, pg?)` (ts) — consistent across the steps that define and consume them. `push_schema` returns `Result<(), RtDbError>` in rust (Task 5 notes the caller updates).

**Sequencing note for the orchestrator:** run Batch A (T1, T2, T4) concurrently → `make checkall`; then Batch B (T3, T5) concurrently → `make checkall`; then T6 → final `make checkall`. Do not run T2+T3 or T4+T5 concurrently (shared `in_memory` files). Verify each batch with `make checkall` yourself — never trust a sub-agent's self-reported green.
