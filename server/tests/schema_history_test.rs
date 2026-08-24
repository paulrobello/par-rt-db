//! Integration tests for schema change history (ENH-013).

use crate::common::{admin_get, admin_post, spawn_app, test_state};
use serde_json::json;

/// Create a bare database (no schema push) and register RAII cleanup. The brief
/// assumes a freshly-created db has no schema history yet, so we cannot use
/// `crate::common::fresh_db` (which pushes the kanban fixture as part of its setup).
async fn bare_db(state: &std::sync::Arc<rtdb_server::AppState>) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create bare database");
    crate::common::wrap_test_db(name)
}

/// POST `/admin/push-schema` and assert success.
async fn push(addr: std::net::SocketAddr, db: &str, schema: serde_json::Value) {
    let resp = admin_post(
        addr,
        "/admin/push-schema",
        json!({ "db": db, "schema": schema }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "push failed: {:?}",
        resp.text().await
    );
}

async fn history(addr: std::net::SocketAddr, db: &str) -> serde_json::Value {
    let resp = admin_get(addr, &format!("/admin/db/{db}/schema/history")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse history");
    body["entries"].clone()
}

#[tokio::test]
async fn push_captures_a_version_and_latest_matches_live() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;

    push(
        addr,
        &db,
        json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } }),
    )
    .await;
    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" }, "qty": { "type": "number" } } } } })).await;

    let entries = history(addr, &db).await;
    let arr = entries.as_array().expect("entries array");
    assert_eq!(arr.len(), 2, "two pushes -> two versions");
    assert_eq!(arr[0]["source"], "push"); // newest first
    assert!(arr[0]["version"].as_i64() > arr[1]["version"].as_i64());

    // Latest snapshot's schema equals the live schema.
    let newest_version = arr[0]["version"].as_i64().unwrap();
    let resp = admin_get(
        addr,
        &format!("/admin/db/{db}/schema/history/{newest_version}"),
    )
    .await;
    let entry: serde_json::Value = resp.json().await?;
    assert!(entry["schema"]["tables"]["items"]["fields"]["qty"].is_object());
    Ok(())
}

#[tokio::test]
async fn history_isolated_per_db_and_missing_version_404s() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let a = bare_db(&state).await;
    let b = bare_db(&state).await;
    push(
        addr,
        &a,
        json!({ "tables": { "t": { "fields": { "x": { "type": "string" } } } } }),
    )
    .await;
    // db b never had a schema pushed — its history is empty.
    let entries_b = history(addr, &b).await;
    assert_eq!(entries_b.as_array().unwrap().len(), 0);
    // Missing version on a -> 404.
    let resp = admin_get(addr, &format!("/admin/db/{a}/schema/history/999999")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn lazy_table_self_heals_for_preexisting_db() -> anyhow::Result<()> {
    // A db created directly (no push) has no schema_history table until the first
    // capture/ensure. GET history must still succeed (empty) rather than error.
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await; // creates the db, no schema push
    let entries = history(addr, &db).await;
    assert_eq!(entries.as_array().unwrap().len(), 0);
    Ok(())
}

/// Apply a migrate (non-dry-run) and assert it captured a "migrate" row.
/// `bare_db` creates a truly-empty db (the kanban fixture from `fresh_db` would
/// make the subsequent items-only push fail — schema changes are additive-only),
/// so the only captured versions come from the HTTP `push` (1) and the real
/// migrate (2). Dry-run migrates roll back before the capture tap.
#[tokio::test]
async fn migrate_captures_a_version_and_dry_run_does_not() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    push(
        addr,
        &db,
        json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } }),
    )
    .await;

    // Dry-run must NOT capture.
    let dry = admin_post(
        addr,
        &format!("/admin/db/{db}/migrate"),
        json!({
            "directives": [{ "op": "renameField", "table": "items", "from": "name", "to": "title" }],
            "dryRun": true
        }),
    )
    .await;
    assert_eq!(dry.status(), reqwest::StatusCode::OK);
    assert_eq!(
        history(addr, &db).await.as_array().unwrap().len(),
        1,
        "dry-run captured nothing"
    );

    // Real migrate captures a second row tagged "migrate".
    let real = admin_post(
        addr,
        &format!("/admin/db/{db}/migrate"),
        json!({
            "directives": [{ "op": "renameField", "table": "items", "from": "name", "to": "title" }],
            "dryRun": false
        }),
    )
    .await;
    assert_eq!(real.status(), reqwest::StatusCode::OK);
    let entries = history(addr, &db).await;
    let arr = entries.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["source"], "migrate");
    Ok(())
}

/// POSTs `{txn: {steps}}` to `/admin/db/{db}/mutate` and returns the `results`.
async fn mutate(
    addr: std::net::SocketAddr,
    db: &str,
    steps: serde_json::Value,
) -> serde_json::Value {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({ "txn": { "steps": steps } }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "mutate should succeed: {:?}",
        resp.text().await
    );
    let body: serde_json::Value = resp.json().await.expect("parse mutate response");
    body["results"].clone()
}

/// POSTs `{query: {table}}` to `/admin/db/{db}/query` and returns the `result`.
async fn query(addr: std::net::SocketAddr, db: &str, table: &str) -> serde_json::Value {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/query"),
        json!({ "query": { "table": table } }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "query should succeed: {:?}",
        resp.text().await
    );
    let body: serde_json::Value = resp.json().await.expect("parse query response");
    body["result"].clone()
}

/// Restore reverts schema shape to a prior snapshot and writes two rows
/// (outgoing + incoming). The typed `confirm` guard rejects a mismatch.
#[tokio::test]
async fn restore_reverts_shape_and_is_undoable() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;

    // v1: items with name.
    push(
        addr,
        &db,
        json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } }),
    )
    .await;
    let v1 = history(addr, &db).await.as_array().unwrap()[0]["version"]
        .as_i64()
        .unwrap();
    // v2: add a second table.
    push(
        addr,
        &db,
        json!({ "tables": {
            "items": { "fields": { "name": { "type": "string" } } },
            "orders": { "fields": { "amt": { "type": "number" } } }
        } }),
    )
    .await;

    // Restore to v1 (drops `orders` table). confirm == db name.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": db.to_string() }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "restore: {:?}",
        resp.text().await
    );
    // Response echoes the restored version.
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["restoredTo"], v1);
    assert_eq!(body["ok"], true);

    // Live schema no longer has `orders`.
    let live: serde_json::Value = admin_get(addr, &format!("/admin/dbs/{db}/schema"))
        .await
        .json()
        .await?;
    assert!(live["tables"]["items"].is_object());
    assert!(
        live["tables"].get("orders").is_none(),
        "orders table should be dropped"
    );

    // Restore writes two new rows (outgoing v2-state + incoming v1-state),
    // both tagged source "restore". The list endpoint returns summaries (no
    // `schema` body); the live-schema check above already proved shape, so
    // here we only verify source tagging.
    let entries = history(addr, &db).await;
    let arr = entries.as_array().unwrap();
    let restores = arr.iter().filter(|e| e["source"] == "restore").count();
    assert_eq!(restores, 2);
    // Newest entry is the incoming (v1-state) capture.
    assert_eq!(arr[0]["source"], "restore");

    // Guard: wrong confirm is rejected.
    let bad = admin_post(
        addr,
        &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": "nope" }),
    )
    .await;
    assert_eq!(bad.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

/// Restoring to a snapshot without an index drops the index's typed `f_<field>`
/// column but preserves the document's jsonb data (the `doc` column is the
/// source of truth; `f_<field>` is a redundant index copy). Builds the lower-
/// index snapshot via two pushes (push is additive, so a first push with no
/// index captures a no-index snapshot, then a second push adds the index), then
/// restores back to the no-index snapshot.
#[tokio::test]
async fn restore_dropping_index_column_preserves_doc_data() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;

    // v1: items with field `name`, NO index. Captures a no-index snapshot.
    push(
        addr,
        &db,
        json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } }),
    )
    .await;
    let v1 = history(addr, &db).await.as_array().unwrap()[0]["version"]
        .as_i64()
        .unwrap();

    // v2: same field + `by_name` index (additive push creates the `f_name`
    // typed column + the btree index).
    push(
        addr,
        &db,
        json!({ "tables": {
            "items": {
                "fields": { "name": { "type": "string" } },
                "indexes": [{ "name": "by_name", "fields": ["name"] }]
            }
        } }),
    )
    .await;

    // Insert a doc; its `name` lives in the jsonb `doc` AND the `f_name` index
    // column.
    let results = mutate(
        addr,
        &db,
        json!([{ "op": "insert", "table": "items", "doc": { "name": "alpha" } }]),
    )
    .await;
    let id = results[0]["id"]
        .as_str()
        .expect("insert returns id")
        .to_string();

    // Restore to v1 (no index): reconcile drops `by_name` index + `f_name`
    // column. The `doc` jsonb is untouched.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": db.to_string() }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "restore: {:?}",
        resp.text().await
    );

    // The document survives in the jsonb `doc` — the field value is still
    // readable after the index column that mirrored it is gone.
    let rows = query(addr, &db, "items").await;
    let arr = rows.as_array().expect("items array");
    assert_eq!(arr.len(), 1, "the inserted doc survived the restore");
    assert_eq!(arr[0]["_id"], id);
    assert_eq!(arr[0]["name"], "alpha", "jsonb field value is preserved");

    // Live schema no longer declares the index.
    let live: serde_json::Value = admin_get(addr, &format!("/admin/dbs/{db}/schema"))
        .await
        .json()
        .await?;
    let indexes = live["tables"]["items"]["indexes"]
        .as_array()
        .expect("indexes array");
    assert!(
        indexes.is_empty(),
        "by_name index should be dropped by the restore"
    );
    Ok(())
}

/// Count physical columns matching `<schema>.<table>.<col>` via `pg_attribute`.
/// Used to prove a restore dropped a generated/maintained column (the `v_*`
/// vector column here), not just the index on top of it.
async fn column_count(pool: &sqlx::PgPool, schema: &str, table: &str, col: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_attribute a \
         JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3",
    )
    .bind(schema)
    .bind(table)
    .bind(col)
    .fetch_one(pool)
    .await
    .expect("pg_attribute probe");
    count
}

/// Count HNSW indexes named `<index>` on `<schema>.<table>` via `pg_indexes`.
async fn index_count(pool: &sqlx::PgPool, schema: &str, table: &str, index: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 AND indexname = $3",
    )
    .bind(schema)
    .bind(table)
    .bind(index)
    .fetch_one(pool)
    .await
    .expect("pg_indexes probe");
    count
}

/// Restoring away a vector index drops BOTH its HNSW index AND its
/// write-maintained `vector(N)` column (`v_<index>`). Before the vector-col
/// fix the HNSW index dropped but the `v_` column leaked as orphan storage
/// forever (the vector field is not in `indexed_fields`, so `drop_columns`
/// never touched it). This test pins the fix: assert both are gone.
#[tokio::test]
async fn restore_dropping_vector_index_drops_its_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let db = bare_db(&state).await;
    let pg_schema = format!("db_{db}");

    // v1: docs with a scalar field only (no vector, no index). Captures the
    // lower-shape snapshot we will restore back to.
    push(
        addr,
        &db,
        json!({ "tables": { "docs": { "fields": { "content": { "type": "string" } } } } }),
    )
    .await;
    let v1 = history(addr, &db).await.as_array().unwrap()[0]["version"]
        .as_i64()
        .unwrap();

    // v2: same scalar field + a vector field and a vector index over it.
    // Additive push creates the `v_by_embedding` vector(N) column + HNSW index.
    push(
        addr,
        &db,
        json!({ "tables": { "docs": {
            "fields": {
                "content": { "type": "string" },
                "embedding": { "type": "vector", "dimensions": 3 }
            },
            "indexes": [{
                "name": "by_embedding",
                "fields": ["embedding"],
                "vector": { "dimensions": 3 }
            }]
        } } }),
    )
    .await;

    // Pre-condition: the vector column and HNSW index exist after v2.
    assert_eq!(
        column_count(&pool, &pg_schema, "t_docs", "v_by_embedding").await,
        1,
        "v_by_embedding column should exist after the vector index is pushed"
    );
    assert_eq!(
        index_count(&pool, &pg_schema, "t_docs", "i_docs_by_embedding").await,
        1,
        "HNSW index should exist after the vector index is pushed"
    );

    // Restore to v1 (no vector index): reconcile drops the HNSW index AND the
    // v_by_embedding column.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": db.to_string() }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "restore: {:?}",
        resp.text().await
    );

    // The fix: the vector column is gone (not leaked as orphan storage).
    assert_eq!(
        column_count(&pool, &pg_schema, "t_docs", "v_by_embedding").await,
        0,
        "v_by_embedding column must be dropped when its vector index is restored away"
    );
    assert_eq!(
        index_count(&pool, &pg_schema, "t_docs", "i_docs_by_embedding").await,
        0,
        "HNSW index must be dropped when its vector index is restored away"
    );
    Ok(())
}
