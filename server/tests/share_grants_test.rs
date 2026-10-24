//! Integration tests for per-row public share grants
//! (`http_api::share`, gated on `RTDB_SHARE_GRANTS_ENABLED`).
//!
//! Positive: an unauthenticated holder of a minted share token reads EXACTLY
//! the granted row, and can mint a signed URL for EXACTLY the granted blob.
//! Negative (criterion 2 of the card): principal-free access to any other row,
//! ownerless-table enumeration, and db-wide minting are all rejected with the
//! same generic failure — invalid token, flag off, wrong row, deleted row,
//! revoked, expired, and blob-less grant are byte-identical envelopes, so the
//! public surface is enumeration-free.

use crate::common::{
    admin_post, fresh_db, spawn_app, test_config, test_hot, test_pool, test_state,
    test_state_with_share_grants,
};
use rtdb_server::{AppState, db, storage};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

/// The exact generic envelope every unauthenticated rejection must return.
const GENERIC: &str = "{\"code\":\"UNAUTHORIZED\",\"message\":\"invalid share token\"}";

async fn assert_generic(resp: reqwest::Response) {
    assert_eq!(resp.status(), 401);
    let body = resp.text().await.expect("body");
    assert_eq!(body, GENERIC, "every rejection must be byte-identical");
}

/// Mint a machine token authorized for `db`.
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(addr, "/admin/mint-token", json!({ "db": db, "name": "t" })).await;
    let body: serde_json::Value = resp.json().await.expect("mint-token");
    body["token"].as_str().expect("token").to_string()
}

/// Insert a projects row and return its id.
async fn insert_project(addr: SocketAddr, db: &str, token: &str, name: &str) -> String {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(
            &json!({"db": db, "txn": {"steps": [{"op": "insert", "table": "projects", "doc": {
                "name": name,
                "description": "payload",
                "status": "active",
                "tags": [],
                "updatedAt": 1.0
            }}]}}),
        )
        .send()
        .await
        .expect("insert");
    assert_eq!(resp.status(), 200, "insert must succeed");
    let body: serde_json::Value = resp.json().await.expect("body");
    body["results"][0]["id"].as_str().expect("id").to_string()
}

/// Mint a share grant over HTTP and return the token.
async fn mint_grant(
    addr: SocketAddr,
    db: &str,
    token: &str,
    body: serde_json::Value,
) -> serde_json::Value {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db}/share-grants"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("mint grant");
    assert_eq!(resp.status(), 200, "grant mint must succeed: {:?}", body);
    resp.json().await.expect("grant body")
}

/// Seed a blob directly and return its id + bytes. `tag` must differ between
/// calls — blob ids are content-addressed, so identical bytes yield one id.
async fn seed_blob(state: &Arc<AppState>, db: &str, tag: &[u8]) -> (String, Vec<u8>) {
    let bytes = [b"share grant payload: ", tag].concat();
    let sha = storage::sha256_hex_bytes(&bytes);
    let id = storage::put(
        &state.pool,
        db,
        &sha,
        bytes.len() as i64,
        Some("text/plain"),
        None,
        &bytes,
    )
    .await
    .expect("seed blob");
    (id, bytes)
}

// --- Criterion 1: the token grants exactly the row/blob ---

#[tokio::test]
async fn share_grant_read_returns_exactly_the_granted_row() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id_a = insert_project(addr, &db, &token, "row-a").await;
    let _id_b = insert_project(addr, &db, &token, "row-b").await;

    let grant = mint_grant(addr, &db, &token, json!({"table": "projects", "id": id_a})).await;
    let share = grant["token"].as_str().expect("token");

    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(resp.status(), 200);
    let doc: serde_json::Value = resp.json().await?;
    assert_eq!(doc["name"], "row-a");
    assert_eq!(doc["_id"], id_a.as_str());
    // The response is the granted row only — row-b's content never appears.
    assert!(doc.to_string().find("row-b").is_none());
    Ok(())
}

#[tokio::test]
async fn share_signed_url_mints_for_exactly_the_granted_blob() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let (blob_a, bytes) = seed_blob(&state, &db, b"a").await;
    let blob_b = seed_blob(&state, &db, b"b").await.0;
    let id = insert_project(addr, &db, &token, "with-blob").await;

    let grant = mint_grant(
        addr,
        &db,
        &token,
        json!({"table": "projects", "id": id, "blobId": blob_a}),
    )
    .await;
    let share = grant["token"].as_str().expect("token");

    let resp = reqwest::get(format!("http://{addr}/share/{share}/signed-url")).await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    let url = body["url"].as_str().expect("url").to_string();
    assert!(url.contains(blob_a.as_str()), "signed for blob_a: {url}");
    assert!(!url.contains(blob_b.as_str()));
    let path = &url[url.find("/storage/").expect("path")..];
    let served = reqwest::get(format!("http://{addr}{path}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);

    // The signature is bound to blob_a: replaying it against blob_b's id is a
    // cross-blob rejection (existing signed-URL binding, proven here for the
    // share-minted signature).
    let exp = body["expiresAt"].as_i64().expect("exp");
    let sig = url.rsplit("sig=").next().expect("sig").to_string();
    let cross = reqwest::get(format!(
        "http://{addr}/storage/{blob_b}?exp={exp}&sig={sig}"
    ))
    .await?;
    assert_eq!(
        cross.status(),
        403,
        "blob_a's signature must not serve blob_b"
    );
    Ok(())
}

// --- Criterion 2: the negatives are each rejected, indistinguishably ---

#[tokio::test]
async fn flag_off_rejects_mint_and_reads_generic() -> anyhow::Result<()> {
    let state = test_state().await; // share_grants_enabled = false (the default)
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;

    let mint = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db}/share-grants"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"table": "projects", "id": "whatever"}))
        .send()
        .await?;
    assert_eq!(mint.status(), 403, "mint is Forbidden with the flag off");

    // A perfectly valid-looking token reads as the generic rejection — the
    // flag state is not observable through the public surface.
    let read = reqwest::get(format!("http://{addr}/share/{}", "a".repeat(64))).await?;
    assert_generic(read).await;
    Ok(())
}

#[tokio::test]
async fn rejections_are_indistinguishable_no_enumeration_oracle() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id = insert_project(addr, &db, &token, "row").await;
    let grant = mint_grant(addr, &db, &token, json!({"table": "projects", "id": id})).await;
    let share = grant["token"].as_str().expect("token").to_string();

    // Sanity: the valid token reads its row (200) — the shapes below must all
    // collapse to the SAME envelope.
    let ok = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(ok.status(), 200);

    // Collect status+body for every failure shape.
    let mut shapes: Vec<(u16, String)> = Vec::new();
    for path in [
        format!("/share/{}", "0".repeat(64)), // unknown 64-hex token
        "/share/short-garbage".to_string(),   // malformed token
    ] {
        let resp = reqwest::get(format!("http://{addr}{path}")).await?;
        shapes.push((resp.status().as_u16(), resp.text().await?));
    }
    // Flag-off state, valid token: same envelope.
    let off_state = test_state().await;
    let off_addr = spawn_app(off_state.clone()).await;
    let resp = reqwest::get(format!("http://{off_addr}/share/{share}")).await?;
    shapes.push((resp.status().as_u16(), resp.text().await?));

    let expected = (401u16, GENERIC.to_string());
    assert!(
        shapes.iter().all(|s| *s == expected),
        "all rejections must be byte-identical: {shapes:?}"
    );
    Ok(())
}

#[tokio::test]
async fn grant_scopes_to_one_row_other_rows_and_enumeration_rejected() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id_a = insert_project(addr, &db, &token, "row-a").await;
    let _id_b = insert_project(addr, &db, &token, "row-b").await;
    let grant = mint_grant(addr, &db, &token, json!({"table": "projects", "id": id_a})).await;
    let share = grant["token"].as_str().expect("token").to_string();

    // Sanity: the granted row reads.
    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(resp.status(), 200);

    // Delete the granted row: the token must NOT fall back to table-level
    // access — row-b still exists in the same ownerless table, and the token
    // reaches it no more than it reached any other row before.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"db": db, "txn": {"steps": [{"op": "delete", "table": "projects", "id": id_a}]}}))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    // Enumeration attempts (unknown 64-hex tokens) and the scoped-out grant
    // are the same generic rejection — an ownerless table exposes nothing to
    // a tokenless caller.
    let mut shapes: Vec<(u16, String)> = Vec::new();
    for path in [
        format!("/share/{share}"),            // granted row deleted
        format!("/share/{}", "f".repeat(64)), // unknown token
        format!("/share/{}", "0".repeat(64)), // unknown token
    ] {
        let resp = reqwest::get(format!("http://{addr}{path}")).await?;
        shapes.push((resp.status().as_u16(), resp.text().await?));
    }
    let expected = (401u16, GENERIC.to_string());
    assert!(
        shapes.iter().all(|s| *s == expected),
        "scoped-out grant and enumeration attempts must be identical: {shapes:?}"
    );
    Ok(())
}

#[tokio::test]
async fn blobless_grant_rejects_db_wide_minting() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    seed_blob(&state, &db, b"unused").await;
    let id = insert_project(addr, &db, &token, "no-blob").await;
    let grant = mint_grant(addr, &db, &token, json!({"table": "projects", "id": id})).await;
    let share = grant["token"].as_str().expect("token").to_string();

    // A grant without blobId cannot mint ANY blob URL — db-wide minting is
    // rejected with the same generic envelope as an invalid token.
    let resp = reqwest::get(format!("http://{addr}/share/{share}/signed-url")).await?;
    assert_generic(resp).await;
    Ok(())
}

#[tokio::test]
async fn revoked_grant_reads_as_generic_rejection() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id = insert_project(addr, &db, &token, "row").await;
    let grant = mint_grant(addr, &db, &token, json!({"table": "projects", "id": id})).await;
    let share = grant["token"].as_str().expect("token").to_string();

    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(resp.status(), 200);

    // Revoke (machine token authorized for this db).
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/api/db/{db}/share-grants/{share}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);

    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_generic(resp).await;

    // Idempotent second revoke.
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/api/db/{db}/share-grants/{share}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    Ok(())
}

#[tokio::test]
async fn expired_grant_reads_as_generic_rejection() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let id = insert_project(addr, &db, &token, "row").await;
    let grant = mint_grant(
        addr,
        &db,
        &token,
        json!({"table": "projects", "id": id, "ttlSeconds": 1}),
    )
    .await;
    let share = grant["token"].as_str().expect("token").to_string();

    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(resp.status(), 200);
    let ok = crate::common::wait_until(Duration::from_secs(5), || async {
        matches!(
            reqwest::get(format!("http://{addr}/share/{share}"))
                .await
                .map(|r| r.status() == reqwest::StatusCode::UNAUTHORIZED),
            Ok(true)
        )
    })
    .await;
    assert!(ok, "grant must expire to the generic rejection within 5s");
    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_generic(resp).await;
    Ok(())
}

// --- Mint authorization ---

#[tokio::test]
async fn mint_requires_db_authorization() -> anyhow::Result<()> {
    let state = test_state_with_share_grants().await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token_a = mint_token(addr, &db_a).await;
    let token_b = mint_token(addr, &db_b).await;
    let id_a = insert_project(addr, &db_a, &token_a, "row").await;

    // No bearer → 401.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db_a}/share-grants"))
        .json(&json!({"table": "projects", "id": id_a}))
        .send()
        .await?;
    assert_eq!(resp.status(), 401);

    // A db-B principal cannot mint grants against db-A rows.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db_a}/share-grants"))
        .header("Authorization", format!("Bearer {token_b}"))
        .json(&json!({"table": "projects", "id": id_a}))
        .send()
        .await?;
    assert_eq!(resp.status(), 403);

    // Unknown table / unknown doc are specific 404s (authed surface).
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db_a}/share-grants"))
        .header("Authorization", format!("Bearer {token_a}"))
        .json(&json!({"table": "nope", "id": id_a}))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/db/{db_a}/share-grants"))
        .header("Authorization", format!("Bearer {token_a}"))
        .json(&json!({"table": "projects", "id": "does-not-exist"}))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);
    Ok(())
}

// --- Require-signed-URL interplay ---

// A share-minted signed URL serves under RTDB_STORAGE_REQUIRE_SIGNED_URLS=true
// — the coherent composition: the token grants the doc, the signed URL grants
// the bytes, both single-target capabilities.
#[tokio::test]
async fn share_signed_url_serves_under_require_signed_mode() -> anyhow::Result<()> {
    let mut config = test_config();
    config.storage.share_grants_enabled = true;
    config.storage.require_signed_urls = true;
    let pool = test_pool(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    let state: Arc<AppState> = AppState::new(pool, config, test_hot());
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;
    let (blob, bytes) = seed_blob(&state, &db, b"require").await;
    let id = insert_project(addr, &db, &token, "row").await;
    let grant = mint_grant(
        addr,
        &db,
        &token,
        json!({"table": "projects", "id": id, "blobId": blob}),
    )
    .await;
    let share = grant["token"].as_str().expect("token").to_string();

    // Doc read is unaffected by the storage flag.
    let resp = reqwest::get(format!("http://{addr}/share/{share}")).await?;
    assert_eq!(resp.status(), 200);

    // Opaque-id blob fetch is rejected in require mode; the share-minted
    // signed URL serves the granted blob.
    let no_sig = reqwest::get(format!("http://{addr}/storage/{blob}")).await?;
    assert_eq!(no_sig.status(), 403);
    let resp = reqwest::get(format!("http://{addr}/share/{share}/signed-url")).await?;
    assert_eq!(resp.status(), 200);
    let url = resp.json::<serde_json::Value>().await?["url"]
        .as_str()
        .expect("url")
        .to_string();
    let path = &url[url.find("/storage/").expect("path")..];
    let served = reqwest::get(format!("http://{addr}{path}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}
