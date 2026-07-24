//! Integration tests for file storage (FEATURE_MATRIX #16).
//!
//! These cover only the table lifecycle: every new database gets a
//! `db_<name>.storage` table from `db::create_database`, the global
//! `rtdb.storage_index` exists after `db::bootstrap`, and `storage::ensure_table`
//! is idempotent and revives a dropped table for databases that predate the
//! feature. Accessors and HTTP routes land in later tasks.

mod common;

use common::{admin_post, fresh_db, spawn_app, test_state};

#[tokio::test]
async fn storage_table_created_with_database() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // create_database ran inside fresh_db; the per-db storage table must exist.
    let schema = format!("db_{db}");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage'
        )",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "storage table should exist for a fresh database");

    // And the global index exists after bootstrap (test_state bootstraps).
    let idx: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'rtdb' AND table_name = 'storage_index'
        )",
    )
    .fetch_one(&state.pool)
    .await?;
    assert!(idx, "rtdb.storage_index should exist after bootstrap");
    Ok(())
}

#[tokio::test]
async fn ensure_table_is_idempotent_and_revives_dropped_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema = format!("db_{db}");

    // Simulate a database that predates the feature: drop storage, then ensure.
    sqlx::query(&format!("DROP TABLE \"{schema}\".storage"))
        .execute(&state.pool)
        .await?;
    rtdb_server::storage::ensure_table(&state.pool, &db).await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage')",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "ensure_table should recreate the storage table");
    Ok(())
}

use rtdb_server::storage;

#[tokio::test]
async fn put_get_meta_delete_round_trip() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let bytes = b"hello file storage";
    let sha = storage::sha256_hex_bytes(bytes);
    let id = storage::put(
        &state.pool,
        &db,
        &sha,
        bytes.len() as i64,
        Some("text/plain"),
        bytes,
    )
    .await?;

    let fetched = storage::get(&state.pool, &db, &id)
        .await?
        .expect("row present");
    assert_eq!(fetched.0, bytes);
    assert_eq!(fetched.1.as_deref(), Some("text/plain"));

    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .expect("meta present");
    assert_eq!(meta.id, id);
    assert_eq!(meta.sha256, sha);
    assert_eq!(meta.size, bytes.len() as i64);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));

    assert!(storage::delete(&state.pool, &db, &id).await?);
    assert!(storage::get(&state.pool, &db, &id).await?.is_none());
    assert_eq!(
        storage::resolve_db(&state.pool, &id).await?,
        None,
        "index row removed on delete"
    );
    Ok(())
}

#[tokio::test]
async fn resolve_db_maps_id_to_owner() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let id = storage::put(&state.pool, &db, "deadbeef", 1, None, b"x").await?;
    assert_eq!(storage::resolve_db(&state.pool, &id).await?, Some(db));
    Ok(())
}

// --- HTTP upload route (POST /api/storage/{db}) ---

use axum::http::StatusCode;
use serde_json::json;
use sha2::Digest;
use std::net::SocketAddr;

/// Mint a machine token for `db` via the admin route and return the bare token
/// string. Mirrors the helper in `http_api_test.rs` (which is private per-file).
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

#[tokio::test]
async fn upload_returns_id_sha_size_and_content_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let bytes = b"upload payload body";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "text/plain")
        .body(bytes.to_vec())
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert!(body["id"].is_string());
    assert_eq!(body["size"], json!(bytes.len() as i64));
    assert_eq!(body["contentType"], json!("text/plain"));
    // sha256 of the body, lowercase hex
    let mut h = sha2::Sha256::new();
    sha2::Digest::update(&mut h, bytes);
    assert_eq!(
        body["sha256"],
        json!(hex::encode(sha2::Digest::finalize(h)))
    );
    Ok(())
}

#[tokio::test]
async fn upload_rejects_oversized_body() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let too_big = vec![0u8; state.config.max_file_size + 1];
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .body(too_big)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    Ok(())
}
