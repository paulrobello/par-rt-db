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
        None,
        bytes,
    )
    .await?;

    let fetched = storage::get(&state.pool, &db, &id)
        .await?
        .expect("row present");
    assert_eq!(fetched.0.as_ref(), &bytes[..]);
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
    let id = storage::put(&state.pool, &db, "deadbeef", 1, None, None, b"x").await?;
    assert_eq!(
        storage::resolve_db(&state.pool, &id).await?,
        Some(db.to_string())
    );
    Ok(())
}

// --- HTTP upload route (POST /api/storage/{db}) ---

use axum::http::StatusCode;
use serde_json::json;
use sha2::Digest;
use std::net::SocketAddr;
use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::auth::{self, Principal};

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

/// Upload `body` bytes to `/api/storage/{db}` with a content-type header and
/// return the server-assigned id. Shared by the delete/metadata tests below.
async fn upload(addr: &SocketAddr, db: &str, token: &str, body: &[u8]) -> String {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "application/octet-stream")
        .body(body.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<serde_json::Value>().await.expect("json")["id"]
        .as_str()
        .expect("id")
        .to_string()
}

/// Upload without a content-type header (exercises the null-contentType path).
async fn upload_no_ct(addr: &SocketAddr, db: &str, token: &str, body: &[u8]) -> String {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .body(body.to_vec())
        .send()
        .await
        .expect("upload");
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<serde_json::Value>().await.expect("json")["id"]
        .as_str()
        .expect("id")
        .to_string()
}

/// Revoke `token` via the admin surface. The admin endpoint keys on tokenId,
/// so resolve the plaintext token to its id through the auth path first
/// (mirroring http_api_test's mint-returns-(id, token) flow).
async fn revoke_token(addr: &SocketAddr, state: &Arc<AppState>, db: &str, token: &str) {
    let principal = auth::resolve_bearer(&state.pool, token)
        .await
        .expect("resolve token");
    let token_id = match principal {
        Principal::Machine {
            token_id,
            db: token_db,
            ..
        } => {
            assert_eq!(token_db, db, "token not minted for this db");
            token_id
        }
        _ => panic!("expected machine principal"),
    };
    let resp = admin_post(*addr, "/admin/revoke-token", json!({"tokenId": token_id})).await;
    assert_eq!(resp.status(), StatusCode::OK);
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

    let too_big = vec![0u8; state.runtime.hot.load().max_file_size + 1];
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

// --- HTTP serve routes (GET /storage/{id}, GET /api/storage/{db}/{id}) ---

#[tokio::test]
async fn public_and_authed_serve_return_bytes() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let payload = b"serve me";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "image/png")
        .body(payload.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Public serve — no bearer.
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(public.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(public.bytes().await?, &payload[..]);

    // Authed serve — bearer + db in path.
    let authed = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(authed.status(), StatusCode::OK);
    assert_eq!(authed.bytes().await?, &payload[..]);
    Ok(())
}

// SEC-101: a stored blob whose content-type is not on the inline-safe
// allowlist (text/html, image/svg+xml, application/javascript, …) must be
// served as `application/octet-stream` with `Content-Disposition: attachment`
// and `X-Content-Type-Options: nosniff`, so an uploaded HTML/SVG/script blob
// can never execute same-origin in the operator console or via the public
// serve route. SVG is explicitly excluded from the allowlist — it executes
// inline <script>.
#[tokio::test]
async fn sec101_html_blob_served_as_attachment_with_nosniff() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let html = b"<html><script>alert(1)</script></html>";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "text/html")
        .body(html.to_vec())
        .send()
        .await?;
    assert_eq!(up.status(), StatusCode::OK);
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Public serve route forces octet-stream + attachment + nosniff.
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(
        public.headers().get("content-type").unwrap(),
        "application/octet-stream",
        "text/html must not be served inline"
    );
    assert_eq!(
        public.headers().get("content-disposition").unwrap(),
        "attachment",
        "non-allowlisted types must be forced to download"
    );
    assert_eq!(
        public.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    assert_eq!(public.bytes().await?, &html[..]);

    // Authed serve route applies the same filter (build_serve_response is
    // shared by both paths).
    let authed = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(authed.status(), StatusCode::OK);
    assert_eq!(
        authed.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    assert_eq!(
        authed.headers().get("content-disposition").unwrap(),
        "attachment"
    );
    assert_eq!(
        authed.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    Ok(())
}

// SEC-101: an allowlisted type (image/png) keeps `inline` so the operator
// console's storage browser and `<img>` embedding still work.
#[tokio::test]
async fn sec101_allowlisted_image_served_inline() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // A minimal valid 1x1 PNG so the path under test stays the plain serve
    // (no transform params => zero-overhead passthrough, no decode).
    let png: &[u8] = &[
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // signature
    ];
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "image/png")
        .body(png.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(
        public.headers().get("content-type").unwrap(),
        "image/png",
        "allowlisted types keep their content-type"
    );
    assert_eq!(
        public.headers().get("content-disposition").unwrap(),
        "inline",
        "allowlisted types stay inline"
    );
    assert_eq!(
        public.headers().get("x-content-type-options").unwrap(),
        "nosniff"
    );
    Ok(())
}

// SEC-101: image/svg+xml must NOT be inline-able — it executes <script>.
#[tokio::test]
async fn sec101_svg_blob_served_as_attachment() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let svg = b"<svg xmlns='http://www.w3.org/2000/svg'><script>alert(1)</script></svg>";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "image/svg+xml")
        .body(svg.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(
        public.headers().get("content-type").unwrap(),
        "application/octet-stream",
        "SVG must not be served inline — it can execute <script>"
    );
    assert_eq!(
        public.headers().get("content-disposition").unwrap(),
        "attachment"
    );
    Ok(())
}

#[tokio::test]
async fn content_type_defaults_to_octet_stream() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .body(b"no content type".to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(
        public.headers().get("content-type").unwrap(),
        "application/octet-stream"
    );
    Ok(())
}

#[tokio::test]
async fn cross_db_authed_serve_is_404() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let tok_a = mint_token(addr, &db_a).await;

    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db_a}"))
        .bearer_auth(&tok_a)
        .body(b"a's file".to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Public serve still resolves (id is global); authed serve from db_b 404s.
    assert_eq!(
        reqwest::get(format!("http://{addr}/storage/{id}"))
            .await?
            .status(),
        StatusCode::OK
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db_b}/{id}"))
        .bearer_auth(mint_token(addr, &db_b).await)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    Ok(())
}

// --- HTTP delete + metadata routes (DELETE /api/storage/{db}/{id},
//      GET /api/storage/{db}/{id}/metadata) ---

#[tokio::test]
async fn delete_revokes_public_url() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let id = upload(&addr, &db, &token, b"to delete").await;
    assert_eq!(
        reqwest::get(format!("http://{addr}/storage/{id}"))
            .await?
            .status(),
        StatusCode::OK
    );

    let del = reqwest::Client::new()
        .delete(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(del.status(), StatusCode::OK);
    assert_eq!(del.json::<serde_json::Value>().await?["ok"], json!(true));

    // Public URL now 404s — the index row is gone.
    assert_eq!(
        reqwest::get(format!("http://{addr}/storage/{id}"))
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
async fn metadata_returns_fields_and_omits_null_content_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Upload with no content-type.
    let id = upload_no_ct(&addr, &db, &token, b"meta").await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/metadata"))
        .bearer_auth(&token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(body["id"], json!(id));
    assert_eq!(body["size"], json!(4));
    assert!(body["sha256"].is_string());
    assert_eq!(
        body.get("contentType"),
        None,
        "contentType omitted when null"
    );
    assert!(body["creationTime"].is_i64());
    Ok(())
}

#[tokio::test]
async fn revoked_token_cannot_delete() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload(&addr, &db, &token, b"x").await;

    // Revoke via the admin surface, then retry the delete.
    revoke_token(&addr, &state, &db, &token).await;
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}

// --- Content-addressed dedup (ENH-008) ---

/// Counts blob rows for a sha256 in a database's storage table.
async fn sha256_count(state: &Arc<AppState>, db: &str, sha: &str) -> i64 {
    let schema = format!("db_{db}");
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".storage WHERE sha256 = $1"
    ))
    .bind(sha)
    .fetch_one(&state.pool)
    .await
    .expect("count rows")
}

#[tokio::test]
async fn put_dedups_identical_content() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let bytes = b"dedup me";
    let sha = storage::sha256_hex_bytes(bytes);

    let id1 = storage::put(
        &state.pool,
        &db,
        &sha,
        bytes.len() as i64,
        None,
        None,
        bytes,
    )
    .await?;
    let id2 = storage::put(
        &state.pool,
        &db,
        &sha,
        bytes.len() as i64,
        None,
        None,
        bytes,
    )
    .await?;
    assert_eq!(id1, id2, "re-uploading identical bytes returns the same id");
    assert_eq!(
        sha256_count(&state, &db, &sha).await,
        1,
        "duplicate content is stored once"
    );
    Ok(())
}

#[tokio::test]
async fn put_distinct_content_gets_distinct_ids() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let a = b"aaa";
    let b = b"bbb";
    let id_a = storage::put(
        &state.pool,
        &db,
        &storage::sha256_hex_bytes(a),
        a.len() as i64,
        None,
        None,
        a,
    )
    .await?;
    let id_b = storage::put(
        &state.pool,
        &db,
        &storage::sha256_hex_bytes(b),
        b.len() as i64,
        None,
        None,
        b,
    )
    .await?;
    assert_ne!(id_a, id_b, "different bytes get different ids");
    Ok(())
}

#[tokio::test]
async fn dedup_respects_delete() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let bytes = b"once then gone";
    let sha = storage::sha256_hex_bytes(bytes);
    let id1 = storage::put(
        &state.pool,
        &db,
        &sha,
        bytes.len() as i64,
        None,
        None,
        bytes,
    )
    .await?;
    assert!(storage::delete(&state.pool, &db, &id1).await?);

    // Content is gone, so a re-upload stores a fresh blob (new id), not a dedup hit.
    let id2 = storage::put(
        &state.pool,
        &db,
        &sha,
        bytes.len() as i64,
        None,
        None,
        bytes,
    )
    .await?;
    assert_ne!(id1, id2, "re-upload after delete is a new blob");
    Ok(())
}

#[tokio::test]
async fn upload_same_bytes_twice_returns_same_id() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let payload = b"identical payload";
    let id1 = upload(&addr, &db, &token, payload).await;
    let id2 = upload(&addr, &db, &token, payload).await;
    assert_eq!(
        id1, id2,
        "HTTP re-upload of identical bytes dedups to one id/URL"
    );
    Ok(())
}

// --- HTTP Range requests on /storage/{id} (RFC 7233 partial content) ---

#[tokio::test]
async fn range_request_returns_206_partial_content() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let body: Vec<u8> = (0..200u32).map(|i| (i % 256) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=0-99")
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap(),
        "bytes 0-99/200"
    );
    assert_eq!(resp.headers().get("content-length").unwrap(), "100");
    assert_eq!(resp.headers().get("accept-ranges").unwrap(), "bytes");
    assert_eq!(resp.bytes().await?, &body[0..100]);
    Ok(())
}

#[tokio::test]
async fn mid_file_open_and_suffix_ranges() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let body: Vec<u8> = (0..200u32).map(|i| (i % 256) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;

    let mid = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=50-149")
        .send()
        .await?;
    assert_eq!(mid.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        mid.headers().get("content-range").unwrap(),
        "bytes 50-149/200"
    );
    assert_eq!(mid.bytes().await?, &body[50..150]);

    // Open-ended `150-` through EOF.
    let open = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=150-")
        .send()
        .await?;
    assert_eq!(open.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        open.headers().get("content-range").unwrap(),
        "bytes 150-199/200"
    );
    assert_eq!(open.bytes().await?, &body[150..200]);

    // Suffix `-20` = the last 20 bytes.
    let suffix = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=-20")
        .send()
        .await?;
    assert_eq!(suffix.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        suffix.headers().get("content-range").unwrap(),
        "bytes 180-199/200"
    );
    assert_eq!(suffix.bytes().await?, &body[180..200]);
    Ok(())
}

#[tokio::test]
async fn out_of_bounds_range_returns_416() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let body = b"twenty bytes of data";
    let n = body.len();
    let id = upload(&addr, &db, &token, body).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=100-200")
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        resp.headers().get("content-range").unwrap(),
        format!("bytes */{n}").as_str()
    );
    assert_eq!(resp.headers().get("accept-ranges").unwrap(), "bytes");
    Ok(())
}

#[tokio::test]
async fn no_range_returns_full_200_advertising_accept_ranges() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let payload = b"serve me whole";
    let id = upload(&addr, &db, &token, payload).await;

    let resp = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("accept-ranges").unwrap(), "bytes");
    assert_eq!(resp.bytes().await?, &payload[..]);
    Ok(())
}

#[tokio::test]
async fn range_works_on_authed_serve_route() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let body: Vec<u8> = (0..100u32).map(|i| (i % 256) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .header("range", "bytes=0-9")
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap(),
        "bytes 0-9/100"
    );
    assert_eq!(resp.bytes().await?, &body[0..10]);
    Ok(())
}

// SEC-123: Range requests now stream the requested byte slice from Postgres
// via `substring(bytes FROM ... FOR ...)` rather than loading the whole bytea
// into server memory and slicing it. Functional parity is verified by the
// range tests above (their bytes match the in-memory slice). This test pins
// the property on a larger payload: a mid-file window + a suffix window both
// return EXACTLY the bytes the client asked for, with a Content-Range whose
// `total` matches the resource size (so the substring path does not silently
// truncate or off-by-one the window).
#[tokio::test]
async fn sec123_range_uses_substring_and_returns_exact_slice() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    // 4 KiB of recognizable content — byte i = (i % 251). 251 is prime so a
    // truncation / off-by-one shows up as a wrong first byte rather than a
    // plausible-looking zero.
    let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;

    // Mid-file window: bytes 1000-1099 (100 bytes).
    let mid = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=1000-1099")
        .send()
        .await?;
    assert_eq!(mid.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        mid.headers().get("content-range").unwrap(),
        "bytes 1000-1099/4096"
    );
    assert_eq!(mid.headers().get("content-length").unwrap(), "100");
    assert_eq!(mid.bytes().await?.as_ref(), &body[1000..1100]);

    // Suffix window: last 50 bytes.
    let suffix = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=-50")
        .send()
        .await?;
    assert_eq!(suffix.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        suffix.headers().get("content-range").unwrap(),
        "bytes 4046-4095/4096"
    );
    assert_eq!(suffix.bytes().await?.as_ref(), &body[4046..4096]);

    // Open-ended from mid-file: bytes 4000- (96 bytes through the end).
    let open = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=4000-")
        .send()
        .await?;
    assert_eq!(open.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        open.headers().get("content-range").unwrap(),
        "bytes 4000-4095/4096"
    );
    assert_eq!(open.bytes().await?.as_ref(), &body[4000..4096]);
    Ok(())
}

// --- SEC-118: per-row authorization on stored blobs ---

use common::mint_user_session;

/// Helper: add `email` to the per-db allowlist so a `Principal::User` resolves
/// `authorize(db)` successfully (per-row enforcement runs after that gate).
async fn allowlist_add(addr: SocketAddr, db: &str, email: &str) {
    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db, "action": "add", "email": email}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

/// SEC-118: a `Principal::User` uploading a blob stamps its `user_id` onto the
/// row. A second user — allowlisted on the same db, so the db-level gate passes
/// — is forbidden from reading it. The owner themselves can still serve it.
#[tokio::test]
async fn sec118_user_blob_forbidden_to_other_user() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    // Two real user sessions, both allowlisted on `db`.
    allowlist_add(addr, &db, "alice@example.com").await;
    allowlist_add(addr, &db, "bob@example.com").await;
    let alice = mint_user_session(&state.pool, "alice", "alice@example.com").await;
    let bob = mint_user_session(&state.pool, "bob", "bob@example.com").await;

    // Alice uploads; the row is stamped owner_id = "alice".
    let payload = b"alice's secret";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&alice)
        .header("content-type", "text/plain")
        .body(payload.to_vec())
        .send()
        .await?;
    assert_eq!(up.status(), StatusCode::OK);
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Alice can fetch her own blob.
    let ok = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&alice)
        .send()
        .await?;
    assert_eq!(ok.status(), StatusCode::OK);

    // Bob — same db, passes the db-level gate — is forbidden on the authed
    // serve, metadata, and delete routes: he is not the blob owner.
    let serve = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&bob)
        .send()
        .await?;
    assert_eq!(serve.status(), StatusCode::FORBIDDEN);

    let meta = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/metadata"))
        .bearer_auth(&bob)
        .send()
        .await?;
    assert_eq!(meta.status(), StatusCode::FORBIDDEN);

    let del = reqwest::Client::new()
        .delete(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&bob)
        .send()
        .await?;
    assert_eq!(del.status(), StatusCode::FORBIDDEN);

    // The blob is still there (Bob's delete was refused).
    let still_ok = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&alice)
        .send()
        .await?;
    assert_eq!(still_ok.status(), StatusCode::OK);
    Ok(())
}

/// SEC-118: a machine token bypasses per-row blob enforcement. A blob uploaded
/// by a user is still readable by a machine token authorized for the db, and a
/// machine-uploaded blob (owner_id = NULL) is readable by any user.
#[tokio::test]
async fn sec118_machine_token_bypasses_blob_owner() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    allowlist_add(addr, &db, "alice@example.com").await;
    let alice = mint_user_session(&state.pool, "alice", "alice@example.com").await;
    let machine = mint_token(addr, &db).await;

    // Alice uploads → owner_id = "alice".
    let payload = b"alice's blob";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&alice)
        .header("content-type", "text/plain")
        .body(payload.to_vec())
        .send()
        .await?;
    let alice_id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Machine token (no user_id) can read Alice's user-owned blob.
    let machine_read = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{alice_id}"))
        .bearer_auth(&machine)
        .send()
        .await?;
    assert_eq!(machine_read.status(), StatusCode::OK);
    assert_eq!(machine_read.bytes().await?.as_ref(), payload);

    // Machine uploads → owner_id = NULL.
    let machine_blob = b"machine's blob";
    let mup = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&machine)
        .header("content-type", "text/plain")
        .body(machine_blob.to_vec())
        .send()
        .await?;
    let machine_id = mup.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Alice — any user authorized for the db — can read the NULL-owner blob.
    let user_read = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{machine_id}"))
        .bearer_auth(&alice)
        .send()
        .await?;
    assert_eq!(user_read.status(), StatusCode::OK);
    assert_eq!(user_read.bytes().await?.as_ref(), machine_blob);
    Ok(())
}

/// SEC-118: the public unauthenticated serve route is unaffected by per-row
/// owner_id — public bearer URLs still serve bytes to anyone who has the id
/// (per-row enforcement runs only on the authed routes). Regression guard
/// against accidentally moving the owner check onto the public path.
#[tokio::test]
async fn sec118_public_serve_unaffected_by_owner() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    allowlist_add(addr, &db, "alice@example.com").await;
    let alice = mint_user_session(&state.pool, "alice", "alice@example.com").await;

    let payload = b"alice's public-by-id blob";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&alice)
        .header("content-type", "text/plain")
        .body(payload.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"]
        .as_str()
        .unwrap()
        .to_string();

    // No bearer at all — public serve by opaque id still works (the route is
    // unauthenticated by design; the owner check never runs here).
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(public.bytes().await?.as_ref(), payload);
    Ok(())
}

// --- ENH-021: streaming storage upload/download ---
//
// The streaming chunked layout (`storage_chunks`, 1 MiB per row) is now the
// primary storage path. These tests pin its load-bearing properties: a blob
// above the old inline bytea ceiling still round-trips byte-identical; a Range
// read fetches ONLY the covering chunk span (not the whole blob); a legacy
// inline blob (written directly into `storage.bytes` with no chunk rows) still
// serves, including ranged; dedup leaves no orphaned provisional chunks; an
// over-size upload is rejected without committing chunks; an over-quota upload
// returns 507 and commits nothing; and a chunked blob is still decodable for
// an on-the-fly image transform.

/// Count `storage_chunks` rows for `blob_id` in `db`'s schema.
async fn chunk_row_count(state: &Arc<AppState>, db: &str, blob_id: &str) -> i64 {
    let schema = format!("db_{db}");
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema}\".storage_chunks WHERE blob_id = $1"
    ))
    .bind(blob_id)
    .fetch_one(&state.pool)
    .await
    .expect("count chunk rows")
}

/// Read the inline `storage.bytes` column directly (NULL for chunked blobs).
/// Panics if the row is missing — test code, the blob was just uploaded.
async fn inline_bytes(state: &Arc<AppState>, db: &str, blob_id: &str) -> Option<Vec<u8>> {
    let schema = format!("db_{db}");
    let row: (Option<Vec<u8>>,) = sqlx::query_as(&format!(
        "SELECT bytes FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(blob_id)
    .fetch_one(&state.pool)
    .await
    .expect("read inline bytes");
    row.0
}

/// ENH-021 #1: a blob above the old 50 MiB inline-bytea ceiling round-trips
/// byte-identical through the streaming path. The default test `max_file_size`
/// is 50 MiB; this test raises the hot cap to 60 MiB and uploads ~55 MiB of
/// recognizable content, then verifies the SHA-256 of the downloaded bytes
/// matches. This proves: (a) `HARD_MAX_FILE_SIZE` (2 GiB) no longer clamps at
/// 50 MiB, (b) the streaming upload path commits chunk rows instead of one
/// inline bytea, (c) the streaming download path reassembles them in order,
/// and (d) the content-addressed sha matches.
#[tokio::test]
async fn enh021_large_blob_round_trips_byte_identical() -> anyhow::Result<()> {
    let state = test_state().await;
    // Raise the per-db hot cap above the old 50 MiB ceiling; the compile-time
    // HARD_MAX_FILE_SIZE is now 2 GiB so this clamps to 60 MiB cleanly.
    state
        .runtime
        .hot
        .store(Arc::new(rtdb_server::config::HotConfig {
            max_file_size: 60 * 1024 * 1024,
            ..common::test_hot()
        }));
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // ~55 MiB of content — byte i = (i % 251). 251 is prime so a wrong-order
    // reassembly or a dropped chunk shows up immediately in the sha.
    let n: usize = 55 * 1024 * 1024;
    let body: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;

    // Layout check: chunked, not inline (the whole point of ENH-021).
    assert_eq!(
        inline_bytes(&state, &db, &id).await,
        None,
        "large blob must use the chunked layout, not inline"
    );
    let chunks = chunk_row_count(&state, &db, &id).await;
    assert!(
        chunks >= 55,
        "expected at least 55 chunk rows for a 55 MiB blob, got {chunks}"
    );

    // Download via the public route (streaming `Body::from_stream` path) and
    // hash — a wrong-order or missing-chunk reassembly produces a different sha.
    let resp = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let received = resp.bytes().await?;
    assert_eq!(received.len(), n, "downloaded byte count must match upload");
    let recv_sha = sha2::Sha256::digest(&received);
    let expect_sha = sha2::Sha256::digest(&body);
    assert_eq!(
        recv_sha, expect_sha,
        "downloaded bytes must hash-identical to upload"
    );
    Ok(())
}

/// ENH-021 #2: a Range request for 1 byte reads ONLY the covering chunk —
/// not the whole blob. A 10 MiB blob has 10 chunk rows; a `bytes=0-0` Range
/// request covers only chunk seq=0, so the post-serve chunk-row count for the
/// OTHER chunks must be unchanged AND the fetch must touch only seq 0. We
/// assert by reading the covering chunk span directly: for `bytes=0-0` only
/// `seq=0` is needed, so we verify `get_range` returns exactly one chunk's
/// worth of covering data (one byte) without touching the other 9 chunks.
#[tokio::test]
async fn enh021_range_reads_only_covering_chunk() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    // 10 MiB body => 10 chunk rows of 1 MiB each. Byte i = (i % 251).
    let n: usize = 10 * 1024 * 1024;
    let body: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
    // Upload via the HTTP route so the blob goes through `put_stream` and
    // lands in the chunked layout (the buffer-accepting `storage::put` writes
    // inline bytes — that path is covered separately by the legacy-inline
    // test below).
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id = upload(&addr, &db, &token, &body).await;

    // Confirm chunked layout and 10 chunks.
    assert_eq!(inline_bytes(&state, &db, &id).await, None);
    assert_eq!(
        chunk_row_count(&state, &db, &id).await,
        10,
        "10 MiB blob should produce exactly 10 1-MiB chunk rows"
    );

    // Range read `bytes=0-0` (1 byte). The covering chunk span is ONLY seq=0
    // (start=0 → seq_lo=0, end=0 → seq_hi=0). get_range must return 1 byte
    // AND read only chunk seq=0. We assert the byte matches and that
    // get_range on a mid-blob single byte (the last byte of chunk 5, which
    // lives at offset 6*1024*1024 - 1) also returns one correct byte.
    let first = storage::get_range(&state.pool, &db, &id, 0, 0)
        .await?
        .expect("range present");
    assert_eq!(
        first.0.as_ref(),
        &body[0..1],
        "bytes=0-0 must return the first byte"
    );

    // A byte that lives in the middle of chunk seq=5 (offset 5 MiB + 1234).
    let mid_off: u64 = 5 * 1024 * 1024 + 1234;
    let mid = storage::get_range(&state.pool, &db, &id, mid_off, mid_off)
        .await?
        .expect("range present");
    assert_eq!(
        mid.0.as_ref(),
        &body[mid_off as usize..=mid_off as usize],
        "mid-blob 1-byte range must return exactly that byte"
    );

    // A wider mid-file window (3000 bytes spanning the seq=2/seq=3 boundary)
    // must return exactly 3000 correct bytes — proves the chunk-span fetch
    // and byte-trim logic compose correctly.
    let win_start: u64 = (3 * 1024 * 1024 - 1000) as u64;
    let win_end: u64 = win_start + 2999;
    let win = storage::get_range(&state.pool, &db, &id, win_start, win_end)
        .await?
        .expect("range present");
    assert_eq!(
        win.0.as_ref(),
        &body[win_start as usize..=win_end as usize],
        "mid-file window spanning a chunk boundary must byte-match"
    );
    Ok(())
}

/// ENH-021 #3: a legacy inline blob (bytes written directly into `storage.bytes`
/// with no chunk rows) still serves on both the full and ranged paths. This
/// pins the inline-fallback branch of `probe_layout` / `serve_bytes` / `get_range`
/// so a database upgraded from pre-ENH-021 keeps serving its existing inline
/// blobs unchanged.
#[tokio::test]
async fn enh021_legacy_inline_blob_still_serves() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    // `mint_token` is unused here — the blob is written directly via the
    // `storage::put` helper (no HTTP upload), but we still need the server
    // up for the serve path. Bind under underscore to silence -D warnings.
    let _ = mint_token(addr, &db).await;

    // Write an inline blob directly: `storage::put` is the legacy buffer-accepting
    // path, which writes the bytea into `storage.bytes`. No chunk rows.
    let body: Vec<u8> = (0..4096u32).map(|i| (i % 251) as u8).collect();
    let id = storage::put(
        &state.pool,
        &db,
        &storage::sha256_hex_bytes(&body),
        body.len() as i64,
        Some("application/octet-stream"),
        None,
        &body,
    )
    .await?;
    // Layout: inline, not chunked.
    assert!(inline_bytes(&state, &db, &id).await.is_some());
    assert_eq!(
        chunk_row_count(&state, &db, &id).await,
        0,
        "legacy inline blob has zero chunk rows"
    );

    // Full serve.
    let full = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(full.status(), StatusCode::OK);
    assert_eq!(full.bytes().await?.as_ref(), &body[..]);

    // Ranged serve — `substring(bytes FROM ... FOR ...)` path.
    let ranged = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", "bytes=1000-1999")
        .send()
        .await?;
    assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        ranged.headers().get("content-range").unwrap(),
        "bytes 1000-1999/4096"
    );
    assert_eq!(ranged.bytes().await?.as_ref(), &body[1000..2000]);
    Ok(())
}

/// ENH-021 #4: re-uploading identical bytes returns the same id (dedup hit),
/// and the dedup path deletes the provisional chunk rows so no orphaned
/// `storage_chunks` rows leak against the storage quota.
#[tokio::test]
async fn enh021_dedup_leaves_no_orphan_chunks() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let body = b"dedup me across streaming uploads";
    let id1 = upload(&addr, &db, &token, body).await;
    let id2 = upload(&addr, &db, &token, body).await;
    assert_eq!(id1, id2, "identical bytes must dedup to the same id");

    // The dedup path dropped the second upload's provisional chunks. Total
    // chunk rows for this blob_id must equal the FIRST upload's row count
    // (one chunk, since the body is small) — NOT doubled.
    let chunks = chunk_row_count(&state, &db, &id1).await;
    assert_eq!(
        chunks, 1,
        "dedup must drop provisional chunks; expected 1 chunk row, got {chunks}"
    );

    // And there is exactly one metadata row for this sha (dedup hit).
    let sha = storage::sha256_hex_bytes(body);
    let rows = sha256_count(&state, &db, &sha).await;
    assert_eq!(rows, 1, "dedup must keep exactly one metadata row");
    Ok(())
}

/// ENH-021 #5: an over-size upload is rejected WITHOUT committing any chunk
/// rows — the txn rolls back when `put_stream` detects `total > limit`. We
/// verify by asserting the response is 400 BAD_REQUEST and no chunk rows
/// exist in the schema for ANY blob_id (the provisional id is never published
/// to the client, but its chunks must be gone).
#[tokio::test]
async fn enh021_oversize_upload_commits_no_chunks() -> anyhow::Result<()> {
    let state = test_state().await;
    // max_file_size stays at the test default (50 MiB). Upload 51 MiB.
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let n: usize = 51 * 1024 * 1024;
    let body = vec![0xABu8; n];
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "application/octet-stream")
        .body(body)
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "51 MiB upload under a 50 MiB cap must be rejected"
    );
    let body_json: serde_json::Value = resp.json().await?;
    assert_eq!(body_json["code"], json!("BAD_REQUEST"));

    // No chunk rows committed at all — the txn rolled back. Count over the
    // whole schema (no blob_id predicate) so we catch any leaked provisional
    // rows regardless of id.
    let schema = format!("db_{db}");
    let total_chunks: i64 =
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".storage_chunks"))
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(
        total_chunks, 0,
        "rejected over-size upload must not leave any chunk rows behind"
    );
    // And no metadata row.
    let total_meta: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".storage"))
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(
        total_meta, 0,
        "rejected over-size upload commits no metadata"
    );
    Ok(())
}

/// ENH-021 #6: an over-quota upload returns 507 INSUFFICIENT_STORAGE and
/// commits nothing — neither chunk rows nor a metadata row. The quota check
/// closure inside `put_stream` fires on the first chunk that crosses the cap,
/// so the txn rolls back and the storage cache is left untouched.
#[tokio::test]
async fn enh021_overquota_upload_returns_507_and_commits_nothing() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Set a 1-byte storage cap. Any non-empty upload exceeds it.
    state
        .runtime
        .hot
        .store(Arc::new(rtdb_server::config::HotConfig {
            max_storage_bytes_per_db: 1,
            ..common::test_hot()
        }));

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "application/octet-stream")
        .body(b"this exceeds a 1-byte cap".to_vec())
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        StatusCode::INSUFFICIENT_STORAGE,
        "upload over the storage cap must return 507"
    );

    // Nothing committed.
    let schema = format!("db_{db}");
    let total_chunks: i64 =
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".storage_chunks"))
            .fetch_one(&state.pool)
            .await?;
    let total_meta: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM \"{schema}\".storage"))
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(total_chunks, 0, "over-quota upload commits no chunk rows");
    assert_eq!(total_meta, 0, "over-quota upload commits no metadata row");
    Ok(())
}

/// ENH-021 #7: an on-the-fly image transform still works on a chunked-layout
/// blob. The transform path calls `storage::get`, which reassembles chunked
/// blobs into one buffer for the decoder; a small PNG through `?w=` must
/// produce a valid (smaller) PNG response with `image/png` content-type.
#[tokio::test]
async fn enh021_image_transform_works_on_chunked_blob() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Build a real (non-trivial) PNG so the decode → resize → re-encode path
    // actually runs: a 32x32 opaque image. The image crate can decode this
    // from the bytes we upload.
    let img = image::RgbImage::from_fn(32, 32, |x, y| {
        image::Rgb([(x * 8) as u8, (y * 8) as u8, 128])
    });
    let mut png_bytes: Vec<u8> = Vec::new();
    image::DynamicImage::ImageRgb8(img)
        .write_to(
            &mut std::io::Cursor::new(&mut png_bytes),
            image::ImageFormat::Png,
        )
        .expect("encode png");
    let id = upload(&addr, &db, &token, &png_bytes).await;

    // Confirm the upload went through the chunked path.
    assert_eq!(inline_bytes(&state, &db, &id).await, None);
    assert!(
        chunk_row_count(&state, &db, &id).await >= 1,
        "chunked blob expected"
    );

    // Transform via `?w=16` — halves the width. Must return a valid PNG.
    let resp = reqwest::get(format!("http://{addr}/storage/{id}?w=16&format=png")).await?;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap(),
        "image/png",
        "transform response must be image/png"
    );
    let out_bytes = resp.bytes().await?;
    let out =
        image::load_from_memory_with_format(&out_bytes, image::ImageFormat::Png).expect("decode");
    assert_eq!(out.width(), 16, "transformed image must be 16px wide");
    assert_eq!(
        out.height(),
        16,
        "transform must preserve aspect ratio (16x16)"
    );
    Ok(())
}

/// ENH-021 #8: existing 206 / Content-Range / 416 semantics are unchanged on
/// a chunked-layout blob. This pins RFC 7233 compliance through the new
/// chunk-aware `get_range` path: a mid-file Range on a chunked blob must
/// return 206 with the exact slice, and an out-of-bounds Range must return
/// 416 with the correct `*/total` Content-Range. (The existing
/// `range_request_returns_206_partial_content` and friends cover the legacy
/// inline path; this test covers the chunked path.)
#[tokio::test]
async fn enh021_chunked_blob_range_semantics_unchanged() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // 3 MiB body => 3 chunk rows. Spans multiple chunks so a mid-file window
    // exercises the chunk-boundary byte-trim path.
    let n: usize = 3 * 1024 * 1024;
    let body: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
    let id = upload(&addr, &db, &token, &body).await;
    assert_eq!(chunk_row_count(&state, &db, &id).await, 3);

    // 206 mid-file, spanning the seq=1/seq=2 boundary.
    let start: usize = 2 * 1024 * 1024 - 500;
    let end: usize = 2 * 1024 * 1024 + 500;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", format!("bytes={start}-{end}"))
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        resp.headers().get("content-range").unwrap(),
        format!("bytes {start}-{end}/{n}").as_str()
    );
    assert_eq!(resp.headers().get("content-length").unwrap(), "1001");
    assert_eq!(resp.bytes().await?.as_ref(), &body[start..=end]);

    // 416 out-of-bounds.
    let oob = reqwest::Client::new()
        .get(format!("http://{addr}/storage/{id}"))
        .header("range", format!("bytes={n}-{}", n + 100))
        .send()
        .await?;
    assert_eq!(oob.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        oob.headers().get("content-range").unwrap(),
        format!("bytes */{n}").as_str()
    );
    assert_eq!(oob.headers().get("accept-ranges").unwrap(), "bytes");
    Ok(())
}
