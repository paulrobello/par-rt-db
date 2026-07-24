# Realtime Dashboard — Phase 2: Metadata Read-Back Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the dashboard read back what it needs to render a database view — the current schema, the machine tokens (minus secrets), and per-table row counts + storage sizes — all behind the Phase 1 admin gate.

**Architecture:** Three additive `GET` endpoints on the existing admin router, all gated on `require_admin(&state, &headers).await` (Phase 1). Schema read-back returns the cached `SchemaDef` (`state.schemas.get`); the token list queries `rtdb_auth.machine_tokens` (never `token_hash`); stats enumerate the schema's tables and run one bounded `COUNT(*)` + `pg_total_relation_size` per table, reusing `ddl::pg_schema`/`ddl::pg_table` for physical names.

**Tech Stack:** Rust, axum, sqlx, Postgres 17.

## Global Constraints

(copy verbatim from `CLAUDE.md`; apply to every task)

- Double-quote every SQL identifier; bind every value via `$n`; never interpolate an unvalidated value. Identifiers built via `format!` MUST be system-generated from already-validated inputs (db name via `validate_db_name`/`database_exists`; table names from the pushed, lowercased, length-capped schema) — exactly the pattern `mutation_log.rs` and `ddl.rs` already use. For sizing prefer `pg_total_relation_size(format('%I.%I', $schema, $table))` with the names `$n`-bound.
- Every failure is an `RtDbError {code, message}`; client-facing 500s are **generic** — never stringify a sqlx/serde error. Use `fetch_optional` for lookups that can miss; `fetch_all`/`fetch_one` for aggregate/COUNT queries that always return a row.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- `make checkall` is the definition of done; `make dev-db-up` is required for tests (auto-run by `make test`; if it reports the known port-held condition, the Postgres is already up — run legs directly: `cd server && cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`).
- Tests share one Postgres and isolate via uniquely-named databases; never assume exclusive access to shared tables. `dashboard_test.rs` uses only `mod common;` + fully-qualified paths (no `use` block); append new tests, reuse helpers (`fresh_db` from `common`), do not redefine existing ones.
- Phase 1 (already on `main`) provides: `require_admin(&state, &headers).await -> Result<AdminPrincipal, RtDbError>`, `state.schemas.get(&pool, &db).await -> Result<Arc<SchemaDef>, RtDbError>`, `db::database_exists(&pool, &db).await -> Result<bool, RtDbError>`, `ddl::pg_schema(db) -> String`, `ddl::pg_table(name) -> String`.

## File Structure

- `server/src/admin.rs` — add `get_schema`, `list_tokens`, `db_stats` handlers + register three routes (all behind `require_admin`).
- `server/tests/dashboard_test.rs` — append Phase 2 tests (the file already holds Phase 1's tests + helpers; keep using `mod common;` + fully-qualified paths).

---

## Task 1: Schema read-back + token list

**Files:**
- Modify: `server/src/admin.rs` (two handlers + two routes).
- Test: `server/tests/dashboard_test.rs` (append).

**Interfaces:**
- Consumes: `require_admin` (Phase 1), `state.schemas.get`, `db::database_exists`.
- Produces: `GET /admin/dbs/{db}/schema` → the current `SchemaDef` as JSON; `GET /admin/tokens?db=` → `{tokens:[{id,name,createdAt,revoked}]}` (no secret).

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/dashboard_test.rs` (fully-qualified, no `use` block):

```rust

// GET /admin/dbs/{db}/schema returns the pushed schema back.
#[tokio::test]
async fn get_schema_returns_pushed_schema() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let resp = common::admin_get(addr, &format!("/admin/dbs/{db}/schema")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    // fresh_db pushes the kanban fixture, which has a `projects` table.
    assert!(body["tables"].get("projects").is_some(), "schema missing projects table: {body}");
    // Unknown db → 404 (NotFound), not 500.
    let resp = common::admin_get(addr, "/admin/dbs/does-not-exist/schema").await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

// GET /admin/tokens?db= lists tokens by id/name/revoked and never exposes the secret hash.
#[tokio::test]
async fn list_tokens_omits_secret() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let _resp = common::admin_post(addr, "/admin/mint-token", serde_json::json!({"db": db, "name": "ci"})).await;

    let resp = common::admin_get(addr, &format!("/admin/tokens?db={db}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let tokens = body["tokens"].as_array().expect("tokens array");
    assert!(tokens.iter().any(|t| t["name"] == "ci"), "minted token missing: {body}");
    // The response must never carry the secret or its hash.
    let body_str = body.to_string();
    assert!(!body_str.contains("token_hash") && !body_str.contains("hash"), "secret leaked: {body_str}");
    // And the DB's stored hash is not equal to any field value in the response.
    let (stored_hash,): (String,) = sqlx::query_as("SELECT token_hash FROM rtdb_auth.machine_tokens WHERE db_name = $1")
        .bind(&db).fetch_one(&pool).await?;
    assert!(!body_str.contains(&stored_hash), "token hash leaked: {stored_hash}");
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cd server && cargo test --test dashboard_test get_schema_returns_pushed_schema list_tokens_omits_secret
```
Expected: FAIL — the routes return 404 (not registered).

- [ ] **Step 3: Add the two handlers**

First, add `Path` to admin.rs's extract import (the existing line is `use axum::extract::{Query as QueryParams, State};`):

```rust
use axum::extract::{Path, Query as QueryParams, State};
```

Then, in `server/src/admin.rs`, immediately before `pub fn admin_routes()`, add:

```rust
async fn get_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<crate::schema::SchemaDef>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    Ok(Json((*schema).clone()))
}

#[derive(Serialize)]
struct TokenRow {
    id: String,
    name: String,
    #[serde(rename = "createdAt")]
    created_at: i64,
    revoked: bool,
}

#[derive(Serialize)]
struct TokensResponse {
    tokens: Vec<TokenRow>,
}

#[derive(Deserialize)]
struct TokensParams {
    db: String,
}

async fn list_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<TokensParams>,
) -> Result<Json<TokensResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let rows: Vec<(String, String, i64, bool)> = sqlx::query_as(
        "SELECT id, name, created_at, revoked FROM rtdb_auth.machine_tokens \
         WHERE db_name = $1 ORDER BY created_at",
    )
    .bind(&params.db)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(TokensResponse {
        tokens: rows
            .into_iter()
            .map(|(id, name, created_at, revoked)| TokenRow { id, name, created_at, revoked })
            .collect(),
    }))
}
```

`Path`, `QueryParams` (`Query as QueryParams`), `Serialize`/`Deserialize`, `Json`, `Arc`, `HeaderMap`, `require_admin`, `db`, `RtDbError` are all already imported in `admin.rs` (Phase 1 + existing imports). `crate::schema::SchemaDef` is fully qualified.

- [ ] **Step 4: Register the routes**

In `admin_routes()`, add (e.g. after the `/admin/admins` route from Phase 1):

```rust
        .route("/admin/dbs/{db}/schema", get(get_schema))
        .route("/admin/tokens", get(list_tokens))
```

- [ ] **Step 5: Run tests to verify they pass**

```
cd server && cargo test --test dashboard_test get_schema_returns_pushed_schema list_tokens_omits_secret
```
Expected: PASS.

- [ ] **Step 6: Run the full gate and commit**

```
cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test
```
Expected: all green. Then:

```bash
git add server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): dashboard metadata — schema read-back + token list (#18 phase 2)"
```

---

## Task 2: Database/table stats (row counts + storage sizes)

**Files:**
- Modify: `server/src/admin.rs` (one handler + one route).
- Test: `server/tests/dashboard_test.rs` (append).

**Interfaces:**
- Consumes: `require_admin`, `state.schemas.get`, `db::database_exists`, `ddl::pg_schema`, `ddl::pg_table`.
- Produces: `GET /admin/dbs/{db}/stats` → `{tables:[{name, rowCount, sizeBytes}], totalSizeBytes}`.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/dashboard_test.rs`:

```rust

// GET /admin/dbs/{db}/stats returns one row per document table (logical names from the
// schema) with an integer rowCount + integer sizeBytes, plus a positive total. COUNT(*)
// correctness is inherent; this verifies the endpoint enumerates tables and queries each.
#[tokio::test]
async fn db_stats_reports_table_counts_and_sizes() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let resp = common::admin_get(addr, &format!("/admin/dbs/{db}/stats")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let tables = body["tables"].as_array().expect("tables array");
    let names: Vec<String> = tables
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect();
    // The kanban fixture has exactly these two document tables (logical schema names).
    assert!(
        names.contains(&"projects".to_string()) && names.contains(&"workItems".to_string()),
        "expected projects+workItems: {body}"
    );
    for t in tables {
        assert!(t["rowCount"].as_i64().is_some(), "rowCount not an integer: {t}");
        assert!(t["sizeBytes"].as_i64().is_some(), "sizeBytes not an integer: {t}");
    }
    assert!(body["totalSizeBytes"].as_i64().unwrap_or(0) > 0, "total size not positive: {body}");

    // Unknown db → 404.
    let resp = common::admin_get(addr, "/admin/dbs/does-not-exist/stats").await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

```
cd server && cargo test --test dashboard_test db_stats_reports_table_counts_and_sizes
```
Expected: FAIL — route 404.

- [ ] **Step 3: Add the stats handler**

In `server/src/admin.rs`, immediately before `pub fn admin_routes()`, add:

```rust
#[derive(Serialize)]
struct TableStat {
    name: String,
    #[serde(rename = "rowCount")]
    row_count: i64,
    #[serde(rename = "sizeBytes")]
    size_bytes: i64,
}

#[derive(Serialize)]
struct DbStatsResponse {
    tables: Vec<TableStat>,
    #[serde(rename = "totalSizeBytes")]
    total_size_bytes: i64,
}

async fn db_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<DbStatsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema_def = state.schemas.get(&state.pool, &db).await?;
    let pg_schema = crate::ddl::pg_schema(&db);

    let mut tables = Vec::with_capacity(schema_def.tables.len());
    let mut total_size_bytes: i64 = 0;
    for name in schema_def.tables.keys() {
        let pg_table = crate::ddl::pg_table(name);
        // Identifiers are system-generated from the validated db name + pushed (lowercased,
        // length-capped) table name, so double-quoting via format! is safe — same pattern as
        // mutation_log.rs. COUNT always returns exactly one row.
        let count_sql = format!("SELECT COUNT(*) FROM \"{pg_schema}\".\"{pg_table}\"");
        let row_count: i64 = sqlx::query_scalar(&count_sql).fetch_one(&state.pool).await?;
        // Size via the injection-safe %I.%I regclass form, names $n-bound.
        let size_bytes: i64 = sqlx::query_scalar(
            "SELECT pg_total_relation_size(format('%I.%I', $1, $2))::bigint",
        )
        .bind(&pg_schema)
        .bind(&pg_table)
        .fetch_one(&state.pool)
        .await?;
        total_size_bytes += size_bytes;
        tables.push(TableStat { name: name.clone(), row_count, size_bytes });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(DbStatsResponse { tables, total_size_bytes }))
}
```

- [ ] **Step 4: Register the route**

In `admin_routes()`, add:

```rust
        .route("/admin/dbs/{db}/stats", get(db_stats))
```

- [ ] **Step 5: Run test to verify it passes**

```
cd server && cargo test --test dashboard_test db_stats_reports_table_counts_and_sizes
```
Expected: PASS.

- [ ] **Step 6: Run the full gate and commit**

```
cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test
```
Expected: all green. Then:

```bash
git add server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): dashboard metadata — per-db table stats (counts + sizes) (#18 phase 2)"
```

---

## Phase 2 Done — Definition of Done

- `GET /admin/dbs/{db}/schema` returns the current schema (404 for unknown db).
- `GET /admin/tokens?db=` lists tokens without ever exposing the secret/hash.
- `GET /admin/dbs/{db}/stats` returns per-table row counts + storage sizes + total.
- `make checkall` green; all pre-existing tests still pass.
- FEATURE_MATRIX #18 sketch may note Phase 2 (metadata read-back) shipped.

## Next phases (separate plans)

Phase 3 metrics + op feed · Phase 4 config + dynamic CORS · Phase 5 admin document access · Phase 6 static hosting · then `/impeccable` frontend.
