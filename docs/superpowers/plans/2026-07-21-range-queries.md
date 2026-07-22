# Index Range Queries (gt/gte/lt/lte) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional `gt`/`gte`/`lt`/`lte` inequality bounds to the query DSL, applying to the single index field immediately after the `eq` prefix, mirrored end-to-end (server, wire protocol, TS client, both test suites, FEATURE_MATRIX.md).

**Architecture:** `query.rs`'s `Query` struct gains four new optional JSON fields. `execute_query` resolves them against `index_def`/`eq_len` (already computed for `eq`), typing each bound value via `txn.rs`'s existing `eq_binds`/`EqBind`/`eq_bind_for` conversion (widened to `pub(crate)`) rather than forking a parallel typed-conversion path, and appends `>`/`>=`/`<`/`<=` SQL conditions + binds alongside the existing eq conditions. The TS client mirrors the wire shape in `protocol.ts` and exposes chainable `.gt()/.gte()/.lt()/.lte()` builder methods on `TableQuery` in `query.ts`.

**Tech Stack:** Rust (axum/tokio/sqlx/Postgres 17) server in `server/`; TypeScript client SDK in `client/` (bun/vitest/biome).

## Global Constraints

- `client/src/protocol.ts` must stay byte-identical in wire shape (camelCase field names) to `server/src/protocol.rs` / `server/src/query.rs`'s `Query` struct — this is the load-bearing wire coupling documented in `CLAUDE.md`.
- Reuse `eq_binds`/`EqBind`/`eq_bind_for` typing from `server/src/txn.rs` for range-bound value conversion. Do not fork a second type-conversion path in `query.rs`.
- Every failure path returns the `RtDbError` envelope; validation failures use `RtDbError::bad_request`. No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Single commit only.** Tasks 1 and 2 below must NOT run `git commit` (or `git add`) — the user asked for exactly one conventional-style commit for the whole feature, made in Task 3 after `make checkall` is fully green. Do not push. Do not touch the kanban board.
- Format surgically: only run `cargo fmt` / `biome format --write` against the files this plan actually touches, never the whole tree (`cargo fmt --all` / `bun run fmt` would reformat unrelated files into the diff).
- `make checkall` requires the dev Postgres running (`make dev-db-up` — idempotent, safe to re-run); `make checkall`'s `test` target already depends on it.

---

### Task 1: Server — range bounds in `query.rs`, shared typing from `txn.rs`, integration tests

**Files:**
- Modify: `server/src/txn.rs` (widen `eq_bind_for` visibility)
- Modify: `server/src/query.rs` (`Query` struct + `execute_query` body)
- Modify: `server/tests/common/mod.rs` (add a fixture index for numeric-range tests)
- Modify: `server/tests/query_test.rs` (existing `Query` literals + new range tests)
- Modify: `server/tests/txn_test.rs` (one existing `Query` literal, line ~236)

**Interfaces:**
- Consumes: `server/src/txn.rs`'s existing `pub(crate) enum EqBind { Text(String), Num(f64), Bool(bool) }` and `pub(crate) fn eq_binds(table: &TableDef, index: &IndexDef, eq: &[serde_json::Value]) -> Result<Vec<EqBind>, RtDbError>`.
- Produces: `server/src/txn.rs`'s `eq_bind_for` becomes `pub(crate) fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError>` (used by Task 1 only, but exported crate-wide for reuse). `Query` gains `pub gt: Option<serde_json::Value>`, `pub gte: Option<serde_json::Value>`, `pub lt: Option<serde_json::Value>`, `pub lte: Option<serde_json::Value>` — Task 2's client mirror and this plan's Task 3 docs reference these exact field names.

- [ ] **Step 1: Widen `eq_bind_for` to `pub(crate)` in `txn.rs`**

In `server/src/txn.rs`, change:

```rust
fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError> {
```

to:

```rust
/// Shared with `query.rs`, which reuses this to type range-bound (`gt`/`gte`/`lt`/`lte`)
/// values the same way `eq` values are typed here.
pub(crate) fn eq_bind_for(ty: &FieldType, value: &serde_json::Value) -> Result<EqBind, RtDbError> {
```

- [ ] **Step 2: Add `gt`/`gte`/`lt`/`lte` fields to the `Query` struct (no behavior yet)**

In `server/src/query.rs`, change the import line:

```rust
use crate::txn::{EqBind, eq_binds};
```

to:

```rust
use crate::txn::{EqBind, eq_bind_for, eq_binds};
```

Change the `Query` struct from:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default)]
    pub get: Option<String>, // point read by id; excludes all below
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub eq: Vec<serde_json::Value>, // prefix binds on index fields
    #[serde(default)]
    pub order: Option<Order>, // default Asc
    #[serde(default)]
    pub take: Option<u32>, // cap 4096; absent => collect (cap 4096)
    #[serde(default)]
    pub unique: bool, // with unique, take/order must be absent
}
```

to:

```rust
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default)]
    pub get: Option<String>, // point read by id; excludes all below
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub eq: Vec<serde_json::Value>, // prefix binds on index fields
    #[serde(default)]
    pub gt: Option<serde_json::Value>, // exclusive lower bound on the index field after the eq prefix
    #[serde(default)]
    pub gte: Option<serde_json::Value>, // inclusive lower bound; mutually exclusive with gt
    #[serde(default)]
    pub lt: Option<serde_json::Value>, // exclusive upper bound on the index field after the eq prefix
    #[serde(default)]
    pub lte: Option<serde_json::Value>, // inclusive upper bound; mutually exclusive with lt
    #[serde(default)]
    pub order: Option<Order>, // default Asc
    #[serde(default)]
    pub take: Option<u32>, // cap 4096; absent => collect (cap 4096)
    #[serde(default)]
    pub unique: bool, // with unique, take/order must be absent
}
```

Update the doc comment immediately above `execute_query` from:

```rust
/// Result docs = stored doc merged with {"_id", "_creationTime", "_version"}.
/// get: point SELECT, null if missing. unique: error PreconditionFailed "unique query matched
/// multiple documents" if >1 row, null if 0. eq len may be a PREFIX of index fields (0..=all),
/// each typed like Task 5. Sort: unbound index fields in index order, then created_at, then id —
/// all in `order` direction. No index => eq must be empty, sort by (created_at, id).
/// Unknown table -> NotFound; unknown index / eq too long / get+query mix / unique+take -> BadRequest.
/// `take: 0` is valid and returns an empty `Docs([])`, not an error.
/// `unique` without an `index` scans the whole table (LIMIT 2) and applies the same 0/1/>1 rule.
```

to:

```rust
/// Result docs = stored doc merged with {"_id", "_creationTime", "_version"}.
/// get: point SELECT, null if missing. unique: error PreconditionFailed "unique query matched
/// multiple documents" if >1 row, null if 0. eq len may be a PREFIX of index fields (0..=all),
/// each typed like Task 5. Sort: unbound index fields in index order, then created_at, then id —
/// all in `order` direction. No index => eq must be empty, sort by (created_at, id).
/// `gt`/`gte`/`lt`/`lte` add an optional inequality bound on the single index field immediately
/// after the `eq` prefix (`index.fields[eq.len()]`): at most one of `gt`/`gte` and at most one of
/// `lt`/`lte` may be set, both may be set together for a bounded range, and the bound value is
/// typed via the same `eq_binds`/`eq_bind_for` conversion `txn.rs` uses for `eq`. A range bound
/// requires an index and a remaining (unconsumed by `eq`) index field -> BadRequest otherwise.
/// Unknown table -> NotFound; unknown index / eq too long / get+query mix / unique+take -> BadRequest.
/// `take: 0` is valid and returns an empty `Docs([])`, not an error.
/// `unique` without an `index` scans the whole table (LIMIT 2) and applies the same 0/1/>1 rule.
```

- [ ] **Step 3: Fix every `Query { ... }` struct literal broken by the new fields**

Run:

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo check --all-targets 2>&1 | grep -B2 "missing field"
```

`cargo check` will report every struct literal (across `tests/query_test.rs` and `tests/txn_test.rs`) that doesn't set `gt`/`gte`/`lt`/`lte`. For each one, insert these four lines immediately after that literal's `eq: ...,` line and before its `order: ...,` line:

```rust
            gt: None,
            gte: None,
            lt: None,
            lte: None,
```

Re-run `cargo check --all-targets` until it reports zero errors. (`server/tests/subs_test.rs` builds `Query` via `serde_json::from_value(...)` — untouched by this, since `#[serde(default)]` fills the new fields in.)

- [ ] **Step 4: Add a numeric-range fixture index**

In `server/tests/common/mod.rs`, change the `workItems` table's `indexes` array from:

```rust
        "indexes":[{"name":"by_project","fields":["projectId"]},
                   {"name":"by_status","fields":["status"]},
                   {"name":"by_project_and_status","fields":["projectId","status"]}]}
```

to:

```rust
        "indexes":[{"name":"by_project","fields":["projectId"]},
                   {"name":"by_status","fields":["status"]},
                   {"name":"by_project_and_status","fields":["projectId","status"]},
                   {"name":"by_project_and_order","fields":["projectId","order"]}]}
```

(Additive-only schema change on a test fixture; every other test still passes since nothing asserts the total index count — `admin_test.rs` only checks that specific named indexes exist.)

- [ ] **Step 5: Write the failing range-query tests**

Append to the end of `server/tests/query_test.rs` (after the existing `canonical_is_stable_for_identical_results` test, i.e. after the file's current final `}`):

```rust

// Range queries: gt/gte/lt/lte after the eq prefix.

// (range-a) gt excludes the boundary value; results still sorted by (status, created_at).
#[tokio::test]
async fn range_gt_excludes_boundary_and_sorts_by_bound_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!("backlog")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[3].clone(), items[1].clone(), items[4].clone()]
    );
    Ok(())
}

// (range-b) gte is inclusive: with the minimum status as the bound, every doc matches.
#[tokio::test]
async fn range_gte_includes_boundary() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: None,
            gte: Some(serde_json::json!("backlog")),
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![
            items[0].clone(),
            items[2].clone(),
            items[3].clone(),
            items[1].clone(),
            items[4].clone(),
        ]
    );
    Ok(())
}

// (range-c) numeric gt+lt bounded range on `order`, combined with the eq prefix.
#[tokio::test]
async fn range_gt_and_lt_bounded_numeric_range() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[1].clone(), items[2].clone(), items[3].clone()]
    );
    Ok(())
}

// (range-d) same bounded range with order: desc reverses, and take limits further.
#[tokio::test]
async fn range_bounded_numeric_range_with_order_desc_and_take() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: Some(Order::Desc),
            take: Some(2),
            unique: false,
        },
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[3].clone(), items[2].clone()]);
    Ok(())
}

// (range-e) range bound with no eq prefix applies directly to the index's first field.
#[tokio::test]
async fn range_without_eq_prefix_applies_to_first_index_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: Some(serde_json::json!("backlog")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[3].clone(), items[1].clone(), items[4].clone()]
    );
    Ok(())
}

// (range-f) gt and gte both set -> BadRequest.
#[tokio::test]
async fn range_gt_and_gte_both_set_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: Some(serde_json::json!("backlog")),
            gte: Some(serde_json::json!("backlog")),
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-g) lt and lte both set -> BadRequest.
#[tokio::test]
async fn range_lt_and_lte_both_set_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: None,
            gte: None,
            lt: Some(serde_json::json!("in_progress")),
            lte: Some(serde_json::json!("in_progress")),
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-h) range bound without an index -> BadRequest.
#[tokio::test]
async fn range_without_index_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: None,
            eq: vec![],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-i) eq already consumes every index field -> no remaining field for the range bound.
#[tokio::test]
async fn range_with_no_remaining_index_field_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            gt: Some(serde_json::json!("x")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-j) get combined with a range bound -> BadRequest.
#[tokio::test]
async fn range_combined_with_get_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: Some(items[0].clone()),
            index: None,
            eq: vec![],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-k) a range value of the wrong type for the field is BadRequest, same as eq typing.
#[tokio::test]
async fn range_value_wrong_type_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!("not-a-number")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}
```

- [ ] **Step 6: Run the new tests to verify they fail**

```bash
cd /Users/probello/Repos/par-rt-db && make dev-db-up
cd server && cargo test --test query_test range_ -- --nocapture
```

Expected: compiles clean (Steps 1-4 already made the fields exist), but assertions fail — e.g. `range_gt_excludes_boundary_and_sorts_by_bound_field` gets back all 5 docs (unfiltered) instead of 3, and the `*_is_bad_request` tests get `Ok(...)` back instead of an error, because `execute_query` doesn't read `gt`/`gte`/`lt`/`lte` yet.

- [ ] **Step 7: Implement the range-bound logic in `execute_query`**

In `server/src/query.rs`, replace the entire `execute_query` function body with:

```rust
pub async fn execute_query(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
) -> Result<QueryResult, RtDbError> {
    validate_db_name(db)?;
    let table_def = schema.table(&q.table)?;

    if let Some(id) = &q.get {
        if q.index.is_some()
            || !q.eq.is_empty()
            || q.gt.is_some()
            || q.gte.is_some()
            || q.lt.is_some()
            || q.lte.is_some()
            || q.order.is_some()
            || q.take.is_some()
            || q.unique
        {
            return Err(RtDbError::bad_request(
                "get cannot be combined with index, eq, range bounds, order, take, or unique",
            ));
        }
        return point_read(pool, db, &q.table, id).await;
    }

    if q.unique && (q.take.is_some() || q.order.is_some()) {
        return Err(RtDbError::bad_request(
            "unique cannot be combined with take or order",
        ));
    }

    if q.gt.is_some() && q.gte.is_some() {
        return Err(RtDbError::bad_request("gt and gte cannot both be set"));
    }
    if q.lt.is_some() && q.lte.is_some() {
        return Err(RtDbError::bad_request("lt and lte cannot both be set"));
    }

    if let Some(take) = q.take
        && take > MAX_TAKE
    {
        return Err(RtDbError::bad_request(format!(
            "take exceeds maximum of {MAX_TAKE}"
        )));
    }

    let index_def: Option<&IndexDef> = match &q.index {
        Some(name) => Some(table_def.index(name)?),
        None => {
            if !q.eq.is_empty() {
                return Err(RtDbError::bad_request("eq requires an index"));
            }
            None
        }
    };

    let binds = match index_def {
        Some(idx) => eq_binds(table_def, idx, &q.eq)?,
        None => Vec::new(),
    };
    let eq_len = binds.len();

    let has_range_bound = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();
    let range_field_name: Option<&str> = if has_range_bound {
        let idx =
            index_def.ok_or_else(|| RtDbError::bad_request("range bound requires an index"))?;
        if eq_len >= idx.fields.len() {
            return Err(RtDbError::bad_request(
                "range bound requires a remaining index field after eq",
            ));
        }
        Some(idx.fields[eq_len].as_str())
    } else {
        None
    };

    let mut range_where: Vec<String> = Vec::new();
    let mut range_binds: Vec<EqBind> = Vec::new();
    if let Some(field_name) = range_field_name {
        let field_type = table_def.fields.get(field_name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{field_name}'"))
        })?;
        let col = pg_col(field_name);
        if let Some(v) = &q.gt {
            range_where.push(format!("\"{col}\" > ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.gte {
            range_where.push(format!("\"{col}\" >= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
        if let Some(v) = &q.lt {
            range_where.push(format!("\"{col}\" < ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.lte {
            range_where.push(format!("\"{col}\" <= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
    }
    let limit_placeholder = eq_len + range_binds.len() + 1;

    let mut where_conditions: Vec<String> = match index_def {
        Some(idx) => idx.fields[..eq_len]
            .iter()
            .enumerate()
            .map(|(i, field_name)| format!("\"{}\" = ${}", pg_col(field_name), i + 1))
            .collect(),
        None => Vec::new(),
    };
    where_conditions.extend(range_where);

    let mut sort_cols: Vec<String> = match index_def {
        Some(idx) => idx.fields[eq_len..]
            .iter()
            .map(|field_name| format!("\"{}\"", pg_col(field_name)))
            .collect(),
        None => Vec::new(),
    };
    sort_cols.push("\"created_at\"".to_string());
    sort_cols.push("\"id\"".to_string());

    let dir = match q.order {
        Some(Order::Desc) => "DESC",
        _ => "ASC",
    };
    let order_by = sort_cols
        .iter()
        .map(|col| format!("{col} {dir}"))
        .collect::<Vec<_>>()
        .join(", ");

    let limit: u32 = if q.unique {
        2
    } else {
        q.take.unwrap_or(MAX_TAKE)
    };

    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(&q.table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    for bind in range_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    query = query.bind(i64::from(limit));
    let mut rows = query.fetch_all(pool).await?;

    if q.unique {
        if rows.len() > 1 {
            return Err(RtDbError::precondition(
                "unique query matched multiple documents",
            ));
        }
        return match rows.pop() {
            Some((id, doc, created_at, version)) => Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?))),
            None => Ok(QueryResult::Doc(None)),
        };
    }

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}
```

(`point_read`, `merge_doc`, and `canonical` below it in the file are unchanged.)

- [ ] **Step 8: Run the full query test file to verify everything passes**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test --test query_test
```

Expected: all tests pass (the pre-existing ones plus the 11 new `range_*` tests).

- [ ] **Step 9: Do NOT commit.** Leave the working tree as-is — Task 3 makes the single commit for the whole feature after `make checkall` is green.

---

### Task 2: Client — mirror the wire shape and add typed builder methods

**Files:**
- Modify: `client/src/protocol.ts` (`QueryJson` interface)
- Modify: `client/src/query.ts` (`TableQuery` class)
- Modify: `client/tests/query.test.ts` (new tests)

**Interfaces:**
- Consumes: nothing from Task 1 directly (TS and Rust are separate compilation units) — but the field names below must match `server/src/query.rs`'s `Query` struct exactly (`gt`, `gte`, `lt`, `lte`, all optional).
- Produces: `TableQuery<DocT, Indexes>.gt(value: unknown)`, `.gte(value: unknown)`, `.lt(value: unknown)`, `.lte(value: unknown)`, each returning `TableQuery<DocT, Indexes>` (chainable, like `.order()`).

- [ ] **Step 1: Add `gt`/`gte`/`lt`/`lte` to `QueryJson`**

In `client/src/protocol.ts`, change:

```typescript
/** Mirrors server `query::Query` (serde `deny_unknown_fields`). */
export interface QueryJson {
  table: string;
  get?: string;
  index?: string;
  eq?: unknown[];
  order?: Order;
  take?: number;
  unique?: boolean;
}
```

to:

```typescript
/** Mirrors server `query::Query` (serde `deny_unknown_fields`). */
export interface QueryJson {
  table: string;
  get?: string;
  index?: string;
  eq?: unknown[];
  gt?: unknown;
  gte?: unknown;
  lt?: unknown;
  lte?: unknown;
  order?: Order;
  take?: number;
  unique?: boolean;
}
```

- [ ] **Step 2: Write the failing client tests**

Append to `client/tests/query.test.ts`, inside the existing `describe("query builder", ...)` block, immediately before its closing `});` (after the `it("builds a point read", ...)` test):

```typescript
  it("builds a range query with gt and lt after an eq prefix", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).gt(1).lt(5).collect();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], gt: 1, lt: 5 });
  });

  it("builds a range query with gte and lte", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).gte("a").lte("m").collect();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], gte: "a", lte: "m" });
  });

  it("combines a range bound with order and take", () => {
    const q = api.items
      .query()
      .withIndex("by_project", ["p1"])
      .gt(1)
      .order("desc")
      .take(10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      gt: 1,
      order: "desc",
      take: 10,
    });
  });
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
cd /Users/probello/Repos/par-rt-db/client && bunx vitest run tests/query.test.ts
```

Expected: fails with a TypeScript error (`Property 'gt' does not exist on type 'TableQuery<...>'`), since `TableQuery` has no `gt`/`gte`/`lt`/`lte` methods yet.

- [ ] **Step 4: Add the chainable range methods to `TableQuery`**

In `client/src/query.ts`, change:

```typescript
  withIndex(index: Indexes, eq: unknown[] = []): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, index, eq });
  }

  order(order: Order): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, order });
  }
```

to:

```typescript
  withIndex(index: Indexes, eq: unknown[] = []): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, index, eq });
  }

  gt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gt: value });
  }

  gte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gte: value });
  }

  lt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lt: value });
  }

  lte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lte: value });
  }

  order(order: Order): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, order });
  }
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd /Users/probello/Repos/par-rt-db/client && bunx vitest run tests/query.test.ts
```

Expected: all tests pass, including the 3 new ones.

- [ ] **Step 6: Typecheck the whole client package**

```bash
cd /Users/probello/Repos/par-rt-db/client && bun run typecheck
```

Expected: clean — `QueryJson`'s new fields are all optional, so no existing call site needs updating.

- [ ] **Step 7: Do NOT commit.** Same reason as Task 1 — Task 3 makes the single commit.

---

### Task 3: Docs, full verification gate, and the single commit

**Files:**
- Modify: `FEATURE_MATRIX.md` (rank-1 row in section 2)

**Interfaces:**
- Consumes: the completed, uncommitted working tree from Tasks 1 and 2.
- Produces: one git commit containing every file this plan touched.

- [ ] **Step 1: Update the FEATURE_MATRIX.md rank-1 row**

In `FEATURE_MATRIX.md`, in section "## 2. Gap matrix — ranked by utility ÷ effort", change the rank-1 table row from:

```markdown
| 1 | 1 | Index **range queries** (`gt/gte/lt/lte` after the `eq` prefix) | ✅ | ❌ | High | M | Add optional range bound to the query DSL; SQL comparators over the same typed `f_<field>` columns. `query.rs` already shares `eq_binds` typing with `txn.rs` — extend, don't fork. Unlocks time-window and "since X" queries that currently require `collect` + client filter. |
```

to:

```markdown
| 1 | 1 | Index **range queries** (`gt/gte/lt/lte` after the `eq` prefix) | ✅ | ✅ | High | M | Implemented — `Query` carries optional `gt`/`gte`/`lt`/`lte` bounds on the index field immediately after the `eq` prefix, typed via the existing `eq_binds`/`eq_bind_for` conversion in `txn.rs` (no forked typing). Mirrored end-to-end: `protocol.rs`/`protocol.ts` wire shape and `TableQuery.gt()/.gte()/.lt()/.lte()` in the TS client, with integration coverage in `query_test.rs` and `query.test.ts`. |
```

Leave every other row and section unchanged — this plan is scoped to rank 1 only.

- [ ] **Step 2: Format only the files this plan touched**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo fmt -- src/query.rs src/txn.rs tests/query_test.rs tests/txn_test.rs tests/common/mod.rs
cd /Users/probello/Repos/par-rt-db/client && bunx biome format --write src/protocol.ts src/query.ts tests/query.test.ts
```

- [ ] **Step 3: Run the full verification gate**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
```

This runs `fmt-check`, `lint` (clippy `-D warnings` + biome lint), `typecheck` (cargo check + tsc), and `test` (starts the dev Postgres, then the full server + client suites). If anything fails, fix the root cause in the file it points at (not by loosening the check) and re-run `make checkall` from the top until it is fully green. Do not stop at the first green step — every one of fmt-check/lint/typecheck/test must pass in the same run.

- [ ] **Step 4: Review the diff, then make the single commit**

```bash
cd /Users/probello/Repos/par-rt-db && git status
git diff --stat
```

Confirm the changed-file list matches exactly: `server/src/query.rs`, `server/src/txn.rs`, `server/tests/common/mod.rs`, `server/tests/query_test.rs`, `server/tests/txn_test.rs`, `client/src/protocol.ts`, `client/src/query.ts`, `client/tests/query.test.ts`, `FEATURE_MATRIX.md`. If anything else changed unexpectedly (e.g. an unrelated file reformatted by an editor), revert that file before committing.

```bash
git add server/src/query.rs server/src/txn.rs server/tests/common/mod.rs server/tests/query_test.rs server/tests/txn_test.rs client/src/protocol.ts client/src/query.ts client/tests/query.test.ts FEATURE_MATRIX.md
git commit -m "$(cat <<'EOF'
feat(query): add index range queries (gt/gte/lt/lte)

Extends the query DSL with optional inequality bounds on the index
field immediately after the eq prefix, typed via the same
eq_binds/eq_bind_for conversion txn.rs already uses for eq. Mirrored
in the TS client (protocol.ts wire shape, TableQuery builder methods)
with integration coverage in both test suites. Closes rank-1 gap in
FEATURE_MATRIX.md.
EOF
)"
git status
```

Expected: `git status` reports a clean working tree (nothing to commit) afterward. Do not push — the user will handle that separately.

- [ ] **Step 5: Report completion**

Summarize: what was implemented, the `make checkall` result (must be reported as actually green, with evidence — not assumed), and the commit hash. Do not touch the kanban board (the user is tracking that separately). Stop and wait.
