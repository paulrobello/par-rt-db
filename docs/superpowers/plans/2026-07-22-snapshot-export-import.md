# Per-Database Snapshot Export/Import Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add admin-only `GET /admin/export-db` and `POST /admin/import-db` endpoints that stream a named database's schema plus every document as JSONL, and load that JSONL back into a (typically empty or newly created) database — FEATURE_MATRIX.md rank 7.

**Architecture:** A new `server/src/snapshot.rs` module holds the core logic (`export_database`, `import_database`), following the existing pattern where `query.rs`/`txn.rs`/`ddl.rs` hold logic that thin `admin.rs`/`http_api.rs` handlers wire up. Export reads every row of every table (ordered `(created_at, id)`, tables in the schema's stable `BTreeMap` order) and renders one JSON object per line: a leading `{"kind":"schema",...}` line, then `{"kind":"doc",...}` lines carrying each row's raw `id`/`doc`/`createdAt`/`version`. Import applies the schema line through the existing `ddl::push_schema` (reused as-is — this is exactly what "push a schema" already means) and replays each doc line through a new `txn::insert_snapshot_row`, a sibling of `txn::do_insert` that preserves the row's original id/timestamp/version instead of minting new ones, recomputing indexed columns from the doc the same way every other write path does. Both HTTP handlers reuse `admin::require_admin` (the existing constant-time admin-key check) completely unchanged — no new auth code.

**Tech Stack:** Rust (axum, sqlx/Postgres), TypeScript client (fetch-based `RtDbAdminClient`).

## Global Constraints

- Reuse `admin::require_admin` for both new routes exactly as it exists today — do not add or modify any auth code.
- Match the existing admin endpoint conventions in `server/src/admin.rs` (constant-time key check first, `RtDbError` envelope on every failure path, `AppState` access via `State<Arc<AppState>>`).
- `SchemaDef.tables` is a `BTreeMap`, so schema/table iteration order is already deterministic — rely on it rather than sorting again.
- JSONL wire format: line 1 is always `{"kind":"schema","schema":<SchemaDef JSON>}`; every following line is `{"kind":"doc","table":<name>,"id":<id>,"doc":<object>,"createdAt":<i64>,"version":<i64>}`. Blank lines are ignored on import.
- Content-type for the export response body is `application/x-ndjson`.
- No new production dependencies. No literal HTTP chunked-transfer streaming — the response body is assembled in memory (matches this project's existing scale: `query.rs`'s own `MAX_TAKE = 4096` cap and every other admin handler already work this way; this is a personal/small-team instance, not a SaaS at scale).

---

## Task 1: Server core — `txn::insert_snapshot_row` + `snapshot.rs` module

**Files:**
- Modify: `server/src/txn.rs` (add one `pub(crate)` function after `do_replace`)
- Create: `server/src/snapshot.rs`
- Modify: `server/src/lib.rs` (register the new module)

**Interfaces:**
- Consumes: `crate::txn::{ColBind, table_columns, column_binds, strip_unset_optionals}` (already-private helpers in `txn.rs`, reused in-file — no new `pub(crate)` surface needed for them), `crate::schema::{SchemaDef, TableDef, validate_doc}`, `crate::ddl::{pg_schema, pg_table, push_schema}`, `crate::db::validate_db_name`.
- Produces: `pub(crate) async fn insert_snapshot_row(conn: &mut PgConnection, pg_schema_name: &str, table_def: &TableDef, table_name: &str, id: &str, doc: &serde_json::Map<String, serde_json::Value>, created_at: i64, version: i64) -> Result<(), RtDbError>` in `txn.rs`. `pub async fn export_database(pool: &PgPool, db: &str, schema: &SchemaDef) -> Result<String, RtDbError>` and `pub async fn import_database(pool: &PgPool, db: &str, jsonl: &str) -> Result<SchemaDef, RtDbError>` in `snapshot.rs` — these are the two functions Task 2's admin handlers call.

- [ ] **Step 1: Add `insert_snapshot_row` to `server/src/txn.rs`**

Insert this function immediately after `do_replace` (which ends at line 407, right before `async fn do_delete`). It reuses `table_columns`, `column_binds`, `ColBind`, and `strip_unset_optionals` — all already defined earlier in this same file, so no new imports are needed.

```rust
/// Inserts a row with an explicit id/created_at/version, preserving a document's
/// original identity and history instead of minting new ones like `do_insert`.
/// Indexed columns are recomputed from `doc` the same way `do_insert` does. Used
/// by `snapshot::import_database` to replay an exported row exactly.
pub(crate) async fn insert_snapshot_row(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    id: &str,
    doc: &serde_json::Map<String, serde_json::Value>,
    created_at: i64,
    version: i64,
) -> Result<(), RtDbError> {
    validate_doc(table_def, doc)?;
    let doc = strip_unset_optionals(table_def, doc.clone());
    let columns = table_columns(table_def)?;
    let binds = column_binds(&columns, &doc)?;

    let table_ident = pg_table(table_name);
    let mut col_names = vec![
        "\"id\"".to_string(),
        "\"doc\"".to_string(),
        "\"created_at\"".to_string(),
        "\"version\"".to_string(),
    ];
    for (name, _) in &columns {
        col_names.push(format!("\"{}\"", pg_col(name)));
    }
    let placeholders: Vec<String> = (1..=col_names.len()).map(|i| format!("${i}")).collect();

    let sql = format!(
        "INSERT INTO \"{pg_schema_name}\".\"{table_ident}\" ({}) VALUES ({})",
        col_names.join(", "),
        placeholders.join(", ")
    );

    let doc_value = serde_json::Value::Object(doc);
    let mut query = sqlx::query(&sql)
        .bind(id.to_string())
        .bind(doc_value)
        .bind(created_at)
        .bind(version);
    for bind in binds {
        query = match bind {
            ColBind::Text(v) => query.bind(v),
            ColBind::Num(v) => query.bind(v),
            ColBind::Bool(v) => query.bind(v),
        };
    }
    query.execute(&mut *conn).await?;
    Ok(())
}
```

- [ ] **Step 2: Create `server/src/snapshot.rs`**

```rust
use sqlx::PgPool;

use crate::db::validate_db_name;
use crate::ddl::{pg_schema, pg_table, push_schema};
use crate::error::RtDbError;
use crate::schema::SchemaDef;
use crate::txn::insert_snapshot_row;

/// One line of a database snapshot's JSONL wire format: a leading `schema` line
/// carries the pushed `SchemaDef`, followed by one `doc` line per stored document
/// (raw `doc` jsonb plus its `id`/`createdAt`/`version` columns — see `query.rs`'s
/// `merge_doc` for how these become `_id`/`_creationTime`/`_version` on read).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum SnapshotLine {
    Schema {
        schema: SchemaDef,
    },
    Doc {
        table: String,
        id: String,
        doc: serde_json::Map<String, serde_json::Value>,
        #[serde(rename = "createdAt")]
        created_at: i64,
        version: i64,
    },
}

/// Renders `db`'s current schema and every row of every table as JSONL: a `schema`
/// line first, then `doc` lines in schema-table order (`SchemaDef::tables` is a
/// `BTreeMap`, so this is deterministic), rows within a table ordered by
/// `(created_at, id)` to match `query.rs`'s default sort.
pub async fn export_database(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let pg_schema_name = pg_schema(db);
    let mut out = String::new();

    let schema_line = SnapshotLine::Schema {
        schema: schema.clone(),
    };
    out.push_str(&serde_json::to_string(&schema_line).map_err(|err| {
        RtDbError::internal(format!("failed to serialize snapshot schema line: {err}"))
    })?);
    out.push('\n');

    for table_name in schema.tables.keys() {
        let table_ident = pg_table(table_name);
        let rows: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" ORDER BY \"created_at\", \"id\""
        ))
        .fetch_all(pool)
        .await?;

        for (id, doc_value, created_at, version) in rows {
            let doc = match doc_value {
                serde_json::Value::Object(map) => map,
                _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
            };
            let line = SnapshotLine::Doc {
                table: table_name.clone(),
                id,
                doc,
                created_at,
                version,
            };
            out.push_str(&serde_json::to_string(&line).map_err(|err| {
                RtDbError::internal(format!("failed to serialize snapshot doc line: {err}"))
            })?);
            out.push('\n');
        }
    }

    Ok(out)
}

/// Loads a snapshot produced by `export_database` into `db`: the first non-blank
/// line must be a `schema` line, applied via `ddl::push_schema` (creates `db`'s
/// tables/indexes when empty, or additively updates them like any other schema
/// push); every following `doc` line is inserted with its original id, `doc`,
/// `createdAt`, and `version` preserved exactly via `txn::insert_snapshot_row`.
/// Blank lines are skipped. Malformed JSON, a doc line before the schema line, or
/// a doc naming a table absent from the schema is a `BadRequest`. Returns the
/// applied schema so the caller can refresh its schema cache.
pub async fn import_database(pool: &PgPool, db: &str, jsonl: &str) -> Result<SchemaDef, RtDbError> {
    validate_db_name(db)?;
    let mut lines = jsonl.lines().filter(|line| !line.trim().is_empty());

    let first = lines
        .next()
        .ok_or_else(|| RtDbError::bad_request("snapshot is empty"))?;
    let schema = match serde_json::from_str::<SnapshotLine>(first) {
        Ok(SnapshotLine::Schema { schema }) => schema,
        Ok(SnapshotLine::Doc { .. }) => {
            return Err(RtDbError::bad_request(
                "snapshot must start with a schema line",
            ));
        }
        Err(err) => {
            return Err(RtDbError::bad_request(format!(
                "invalid snapshot schema line: {err}"
            )));
        }
    };

    let applied = push_schema(pool, db, schema).await?;
    let pg_schema_name = pg_schema(db);
    let mut tx = pool.begin().await?;

    for line in lines {
        let parsed: SnapshotLine = serde_json::from_str(line)
            .map_err(|err| RtDbError::bad_request(format!("invalid snapshot doc line: {err}")))?;
        let (table, id, doc, created_at, version) = match parsed {
            SnapshotLine::Doc {
                table,
                id,
                doc,
                created_at,
                version,
            } => (table, id, doc, created_at, version),
            SnapshotLine::Schema { .. } => {
                return Err(RtDbError::bad_request(
                    "schema line must be the first line",
                ));
            }
        };
        let table_def = applied.table(&table)?;
        insert_snapshot_row(
            &mut tx,
            &pg_schema_name,
            table_def,
            &table,
            &id,
            &doc,
            created_at,
            version,
        )
        .await?;
    }

    tx.commit().await?;
    Ok(applied)
}
```

- [ ] **Step 3: Register the module in `server/src/lib.rs`**

In the alphabetical `pub mod` list at the top, add `pub mod snapshot;` between `pub mod schema;` and `pub mod subs;`:

```rust
pub mod schema;
pub mod snapshot;
pub mod subs;
```

- [ ] **Step 4: Verify it compiles**

Run: `cd server && cargo build --all-targets`
Expected: builds cleanly (warnings about `export_database`/`import_database` being unused are fine — Task 2 wires them up next).

- [ ] **Step 5: Commit**

```bash
git add server/src/txn.rs server/src/snapshot.rs server/src/lib.rs
git commit -m "feat(server): add snapshot export/import core logic"
```

---

## Task 2: Server admin routes — `GET /admin/export-db`, `POST /admin/import-db`

**Files:**
- Modify: `server/src/admin.rs`

**Interfaces:**
- Consumes: `crate::snapshot::{export_database, import_database}` (Task 1), `crate::db::database_exists`, `state.schemas.get`/`state.schemas.put` (existing `SchemaCache` API, already used by `push_schema`/`query_handler`).
- Produces: two new routes on the router returned by `admin_routes()`, used by Task 3's tests.

- [ ] **Step 1: Add imports**

At the top of `server/src/admin.rs`, add two new `use` lines and extend the existing `crate::{...}` import:

```rust
use axum::body::Body;
use axum::response::Response;
```

Change:
```rust
use crate::{AppState, auth, db, ddl};
```
to:
```rust
use crate::{AppState, auth, db, ddl, snapshot};
```

- [ ] **Step 2: Add the two handlers**

Insert this after `allowlist_list` (the function ending just before `/// Admin routes, all gated on ...` / `pub fn admin_routes()`):

```rust
#[derive(Deserialize)]
struct ExportDbParams {
    db: String,
}

/// Streams `db`'s current schema and every document in every table as JSONL (see
/// `snapshot::export_database`); a plain app-level companion to host-level
/// `pg_dump` for seed data and clone-to-dev workflows.
async fn export_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<ExportDbParams>,
) -> Result<Response, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &params.db).await?;
    let body = snapshot::export_database(&state.pool, &params.db, &schema).await?;

    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(body))
        .map_err(|err| RtDbError::internal(format!("failed to build export response: {err}")))
}

#[derive(Deserialize)]
struct ImportDbParams {
    db: String,
}

/// Loads a JSONL snapshot produced by `export_db` back into `db` (see
/// `snapshot::import_database`), refreshing the schema cache with whatever schema
/// the snapshot applied.
async fn import_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<ImportDbParams>,
    body: String,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
    let applied = snapshot::import_database(&state.pool, &params.db, &body).await?;
    state.schemas.put(&params.db, applied).await;
    Ok(Json(OkResponse { ok: true }))
}
```

- [ ] **Step 3: Register the routes**

In `admin_routes()`, add the two routes after the `allowlist` route:

```rust
pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/create-db", post(create_db))
        .route("/admin/push-schema", post(push_schema))
        .route("/admin/dbs", get(list_dbs))
        .route("/admin/mint-token", post(mint_token))
        .route("/admin/revoke-token", post(revoke_token))
        .route(
            "/admin/allowlist",
            get(allowlist_list).post(allowlist_write),
        )
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
}
```

- [ ] **Step 4: Verify it compiles and existing tests still pass**

Run: `cd server && cargo build --all-targets`
Expected: builds cleanly, no warnings about unused `snapshot` functions now (both are called).

Run: `RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test`
Expected: the full existing suite still passes (no new tests yet — Task 3 adds those). If port 55434 is already bound by a healthy `*-postgres-1` container from another worktree, reuse it directly rather than running `make dev-db-up` (which will fail to rebind the port).

- [ ] **Step 5: Commit**

```bash
git add server/src/admin.rs
git commit -m "feat(server): wire up /admin/export-db and /admin/import-db routes"
```

---

## Task 3: Server integration tests

**Files:**
- Modify: `server/tests/common/mod.rs` (add `admin_get`, `admin_post_raw` helpers)
- Modify: `server/tests/admin_test.rs` (add 6 new tests)

**Interfaces:**
- Consumes: routes from Task 2, `rtdb_server::txn::{Step, Transaction, execute_txn}`, `rtdb_server::db::load_schema`.
- Produces: nothing consumed by later tasks — this is a leaf task.

- [ ] **Step 1: Add test helpers to `server/tests/common/mod.rs`**

Append after the existing `admin_post` function (end of file):

```rust
#[allow(dead_code)]
pub async fn admin_get(addr: SocketAddr, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await
        .expect("send admin request")
}

#[allow(dead_code)]
pub async fn admin_post_raw(addr: SocketAddr, path: &str, body: String) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .body(body)
        .send()
        .await
        .expect("send admin request")
}
```

- [ ] **Step 2: Update `admin_test.rs` imports**

Replace the top of `server/tests/admin_test.rs`:
```rust
mod common;

use common::{admin_post, fresh_db, kanban_schema_json, spawn_app, test_state};

fn fresh_name() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}
```
with:
```rust
mod common;

use common::{
    admin_get, admin_post, admin_post_raw, fresh_db, kanban_schema_json, spawn_app, test_state,
};
use rtdb_server::db;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn fresh_name() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}
```

- [ ] **Step 3: Add the round-trip test**

Append to `server/tests/admin_test.rs` (this is the primary coverage: export then import into a fresh database preserving all docs/indexes/schema):

```rust
// (h) export then import into a fresh database round-trips docs, indexes, and schema.
#[tokio::test]
async fn export_then_import_round_trips_docs_indexes_and_schema() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let source_db = fresh_db(&state).await;

    let schema = state.schemas.get(&pool, &source_db).await?;
    let insert_outcome = execute_txn(
        &pool,
        &source_db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "projects".to_string(),
                    doc: serde_json::json!({
                        "name": "Roadmap",
                        "status": "active",
                        "tags": ["q3"],
                        "updatedAt": 1
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                },
                Step::Insert {
                    table: "projects".to_string(),
                    doc: serde_json::json!({
                        "name": "Archive",
                        "status": "archived",
                        "tags": [],
                        "updatedAt": 2
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                },
            ],
        },
    )
    .await?;
    let project_id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("project id")
        .to_string();

    execute_txn(
        &pool,
        &source_db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "workItems".to_string(),
                doc: serde_json::json!({
                    "projectId": project_id,
                    "title": "Ship it",
                    "status": "in_progress",
                    "order": 1
                })
                .as_object()
                .unwrap()
                .clone(),
            }],
        },
    )
    .await?;

    let export_resp = admin_get(addr, &format!("/admin/export-db?db={source_db}")).await;
    assert_eq!(export_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        export_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson")
    );
    let jsonl = export_resp.text().await?;
    let lines: Vec<&str> = jsonl.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 4); // 1 schema line + 2 projects + 1 workItem

    let target_db = fresh_name();
    let resp = admin_post(
        addr,
        "/admin/create-db",
        serde_json::json!({"name": target_db}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let import_resp = admin_post_raw(
        addr,
        &format!("/admin/import-db?db={target_db}"),
        jsonl.clone(),
    )
    .await;
    assert_eq!(import_resp.status(), reqwest::StatusCode::OK);
    let import_body: serde_json::Value = import_resp.json().await?;
    assert_eq!(import_body["ok"], true);

    let source_schema = db::load_schema(&pool, &source_db).await?.expect("source schema");
    let target_schema = db::load_schema(&pool, &target_db).await?.expect("target schema");
    assert_eq!(source_schema, target_schema);

    let source_projects: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{source_db}\".\"t_projects\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    let target_projects: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{target_db}\".\"t_projects\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(source_projects, target_projects);

    let source_items: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{source_db}\".\"t_workitems\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    let target_items: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{target_db}\".\"t_workitems\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(source_items, target_items);

    let index_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2",
    )
    .bind(format!("db_{target_db}"))
    .bind("i_workitems_by_project_and_status")
    .fetch_one(&pool)
    .await?;
    assert_eq!(index_count, 1);

    Ok(())
}
```

- [ ] **Step 4: Add the empty-database export test**

```rust
// (i) export of an empty database (schema pushed, no docs) yields just the schema line.
#[tokio::test]
async fn export_of_empty_database_yields_only_schema_line() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_get(addr, &format!("/admin/export-db?db={name}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let jsonl = resp.text().await?;
    let lines: Vec<&str> = jsonl.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1);

    let line: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(line["kind"], "schema");
    assert_eq!(line["schema"], kanban_schema_json());

    Ok(())
}
```

- [ ] **Step 5: Add unauthorized-access tests for both routes**

```rust
// (j) wrong admin key on export-db -> 401 UNAUTHORIZED.
#[tokio::test]
async fn export_db_wrong_admin_key_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/export-db?db={name}"))
        .header("Authorization", "Bearer wrong-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}

// (k) wrong admin key on import-db -> 401 UNAUTHORIZED.
#[tokio::test]
async fn import_db_wrong_admin_key_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/import-db?db={}", fresh_name()))
        .header("Authorization", "Bearer wrong-key")
        .body("{}")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}
```

- [ ] **Step 6: Add unknown-database tests for both routes (mirrors the existing `push_schema_against_unknown_database_is_not_found` pattern)**

```rust
// (l) export-db against an unknown database -> 404 NOT_FOUND.
#[tokio::test]
async fn export_db_of_unknown_database_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = admin_get(addr, &format!("/admin/export-db?db={}", fresh_name())).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "NOT_FOUND");

    Ok(())
}

// (m) import-db into an unknown database -> 404 NOT_FOUND.
#[tokio::test]
async fn import_db_into_unknown_database_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let jsonl = format!(
        "{}\n",
        serde_json::json!({"kind": "schema", "schema": kanban_schema_json()})
    );
    let resp = admin_post_raw(
        addr,
        &format!("/admin/import-db?db={}", fresh_name()),
        jsonl,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "NOT_FOUND");

    Ok(())
}
```

- [ ] **Step 7: Run the new tests**

Run: `cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test --test admin_test`
Expected: all tests in `admin_test.rs` pass, including the 6 new ones (13 total).

- [ ] **Step 8: Run clippy and the full server suite**

Run: `cd server && cargo clippy --all-targets --all-features -- -D warnings`
Expected: no warnings.

Run: `cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test`
Expected: the entire server suite passes (no regressions in other test files).

- [ ] **Step 9: Commit**

```bash
git add server/tests/common/mod.rs server/tests/admin_test.rs
git commit -m "test(server): cover snapshot export/import round trip, auth, and edge cases"
```

---

## Task 4: Client — `RtDbAdminClient.exportDb()` / `.importDb()`

**Files:**
- Modify: `client/src/admin.ts`
- Modify: `client/tests/admin.test.ts`

**Interfaces:**
- Consumes: server routes from Task 2 (mirrors their wire shape exactly — `GET /admin/export-db?db=`, `POST /admin/import-db?db=` with a raw JSONL body).
- Produces: `exportDb(db: string): Promise<string>`, `importDb(db: string, jsonl: string): Promise<void>` — public API, nothing else in this codebase depends on it.

- [ ] **Step 1: Write the failing tests**

Append to the `describe("RtDbAdminClient", ...)` block in `client/tests/admin.test.ts`, right before the closing `});`:

```ts
  it("exports a database as JSONL text", async () => {
    const jsonl = '{"kind":"schema","schema":{"tables":{}}}\n';
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(jsonl, { status: 200, headers: { "content-type": "application/x-ndjson" } }),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.exportDb("kanban")).resolves.toBe(jsonl);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/export-db?db=kanban");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("imports a JSONL snapshot into a database", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const jsonl = '{"kind":"schema","schema":{"tables":{}}}\n';

    await admin.importDb("kanban", jsonl);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/import-db?db=kanban");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(init.headers["content-type"]).toBe("application/x-ndjson");
    expect(init.body).toBe(jsonl);
  });

  it("throws RtDbError when exportDb receives an error envelope", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ code: "NOT_FOUND", message: "unknown database" }, 404),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.exportDb("missing")).rejects.toThrow("unknown database");
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd client && bun run test -- admin.test.ts`
Expected: FAIL — `admin.exportDb is not a function` / `admin.importDb is not a function`.

- [ ] **Step 3: Implement `exportDb`/`importDb` in `client/src/admin.ts`**

Insert after `allowlistList` and before the existing `private async request(...)` method:

```ts
  /** Fetches `db`'s schema and every document as JSONL text (see server `snapshot::export_database`). */
  async exportDb(db: string): Promise<string> {
    const response = await this.fetchImpl(`${this.url}/admin/export-db?db=${encodeURIComponent(db)}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${this.adminKey}` },
    });
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
    return await response.text();
  }

  /** Loads a JSONL snapshot from `exportDb` into `db` (see server `snapshot::import_database`). */
  async importDb(db: string, jsonl: string): Promise<void> {
    const response = await this.fetchImpl(`${this.url}/admin/import-db?db=${encodeURIComponent(db)}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.adminKey}`,
        "content-type": "application/x-ndjson",
      },
      body: jsonl,
    });
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
  }

  private async throwFromResponse(response: Response): Promise<never> {
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) {
      throw RtDbError.fromEnvelope(parsed);
    }
    throw new RtDbError("INTERNAL", `admin request failed with status ${response.status}`);
  }

```

Leave the existing `private async request(...)` method exactly as-is below this — it is unrelated to these two new methods and already covered by its own tests.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd client && bun run test -- admin.test.ts`
Expected: PASS — all 7 tests in `admin.test.ts` (4 existing + 3 new).

- [ ] **Step 5: Typecheck and run the full client suite**

Run: `cd client && bun run typecheck`
Expected: no errors.

Run: `cd client && bun run test`
Expected: all client tests pass (no regressions).

- [ ] **Step 6: Commit**

```bash
git add client/src/admin.ts client/tests/admin.test.ts
git commit -m "feat(client): add RtDbAdminClient.exportDb()/importDb()"
```

---

## Task 5: Documentation — mark FEATURE_MATRIX.md rank 7 implemented

**Files:**
- Modify: `FEATURE_MATRIX.md`

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: nothing consumed elsewhere.

- [ ] **Step 1: Update the rank-7 row**

In section "## 2. Gap matrix — ranked by utility ÷ effort", replace the rank-7 row:

```
| 7 | 1 | **Snapshot export / import** per database | ✅ | ❌ | Med | S–M | `/admin/export-db` streaming JSONL (+ schema), `/admin/import-db` inverse. Complements host-level `pg_dump` with app-level portability (seed data, clone-to-dev). |
```

with:

```
| 7 | 1 | **Snapshot export / import** per database | ✅ | ✅ | Med | S–M | Implemented — `GET /admin/export-db?db=` (`snapshot::export_database`) renders the pushed schema plus every document across every table as JSONL (a `{"kind":"schema"}` line, then one `{"kind":"doc"}` line per document carrying its `id`/`doc`/`createdAt`/`version`, tables and rows in stable order); `POST /admin/import-db?db=` (`snapshot::import_database`) applies the schema line through the existing `ddl::push_schema` and replays each doc line with its original id/timestamp/version preserved, recomputing indexed columns the same way `txn::do_insert` does. Both routes reuse `admin::require_admin`'s constant-time key check unchanged — no new auth mechanism. Mirrored end-to-end: `RtDbAdminClient.exportDb()/importDb()` in the TS client, with integration coverage in `admin_test.rs` (export→import round trip, unauthorized access, empty-database export). Complements host-level `pg_dump` with app-level portability (seed data, clone-to-dev). |
```

- [ ] **Step 2: Verify**

Run: `grep -n "Snapshot export" FEATURE_MATRIX.md`
Expected: the row shows `✅ | ✅` and the new "Implemented — ..." text.

- [ ] **Step 3: Commit**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs: mark snapshot export/import FEATURE_MATRIX rank 7 implemented"
```

---

## Final Verification (after all tasks)

Run the project's real gate from the repo root:

```bash
make checkall
```

If port 55434 is held by another worktree's already-running, healthy dev-db container, run the equivalent steps manually instead of the `dev-db-up`-gated `make checkall`/`make test` targets, reusing that container directly:

```bash
make fmt-check
make lint
make typecheck
cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test
cd client && bun run test
```

Expected: everything green. Fix anything that fails before considering the feature complete.
