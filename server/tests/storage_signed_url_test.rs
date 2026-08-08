//! Integration tests for signed, time-limited storage URLs.
//!
//! Covers the mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) and the
//! additive signature verification on the public serve route
//! (`GET /storage/{id}?exp=&sig=`). The mint endpoint's returned `url` embeds
//! `config.public_url` (`http://localhost:0` in tests), so tests extract the
//! `/storage/...?...` path+query and re-fetch against the ephemeral test addr.

mod common;

use common::{admin_post, fresh_db, spawn_app, test_state};
use rtdb_server::signed_url;
use rtdb_server::storage;
use serde_json::json;
use std::net::SocketAddr;

/// Mint a machine token authorized for `db` (mirrors `storage_test`'s helper).
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({ "db": db, "name": "test-token" }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token present").to_string()
}

/// Seed a blob directly and return its id + the bytes written.
async fn seed(state: &std::sync::Arc<rtdb_server::AppState>, db: &str) -> (String, Vec<u8>) {
    let bytes = b"signed-url payload".to_vec();
    let sha = storage::sha256_hex_bytes(&bytes);
    let id = storage::put(
        &state.pool,
        db,
        &sha,
        bytes.len() as i64,
        Some("text/plain"),
        &bytes,
    )
    .await
    .expect("seed blob");
    (id, bytes)
}

#[tokio::test]
async fn mint_then_serve_returns_bytes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;

    // Mint via the endpoint.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    let url = body["url"].as_str().expect("url present");
    let exp = body["expiresAt"].as_i64().expect("expiresAt present");
    assert!(exp > rtdb_server::db::now_ms());

    // The returned URL embeds the test public_url host; swap to the live addr.
    let path = &url[url.find("/storage/").expect("path present")..];
    let served = reqwest::get(format!("http://{addr}{path}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

#[tokio::test]
async fn expired_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let past = rtdb_server::db::now_ms() - 1000;
    let sig = signed_url::sign(&state.signed_url_key, &id, past);
    let resp = reqwest::get(format!("http://{addr}/storage/{id}?exp={past}&sig={sig}")).await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn tampered_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    let mut chars: Vec<char> = sig.chars().collect();
    let last_idx = chars.len() - 1;
    let last = chars[last_idx];
    chars[last_idx] = if last == '0' { '1' } else { '0' };
    let bad: String = chars.into_iter().collect();

    let resp = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}&sig={bad}")).await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn tampered_id_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    // Valid sig for `id`, fetched against a different id in the path.
    let resp = reqwest::get(format!(
        "http://{addr}/storage/other-id?exp={exp}&sig={sig}"
    ))
    .await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn tampered_exp_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);
    // The sig was computed over `exp`; fetching with a different exp fails verify.
    let tampered_exp = exp + 1;
    let resp = reqwest::get(format!(
        "http://{addr}/storage/{id}?exp={tampered_exp}&sig={sig}"
    ))
    .await?;
    assert_eq!(resp.status(), 403);
    Ok(())
}

#[tokio::test]
async fn partial_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let sig = signed_url::sign(&state.signed_url_key, &id, exp);

    // exp without sig
    let r1 = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}")).await?;
    assert_eq!(r1.status(), 403);
    // sig without exp
    let r2 = reqwest::get(format!("http://{addr}/storage/{id}?sig={sig}")).await?;
    assert_eq!(r2.status(), 403);
    Ok(())
}

#[tokio::test]
async fn no_signature_remains_public() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;

    let served = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

#[tokio::test]
async fn ttl_is_clamped() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;
    let now = rtdb_server::db::now_ms();

    // Over the cap clamps to MAX (7d); ttl=0 clamps to the 1s minimum.
    let over = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/{id}/signed-url?ttlSeconds=999999999"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let exp_over = over.json::<serde_json::Value>().await?["expiresAt"]
        .as_i64()
        .unwrap();
    // `now` was captured before the HTTP round-trip, while the server computes
    // `exp_over` after it, so allow a few seconds of slack. The clamp is still
    // proven: unclamped, a 999999999s ttl would land ~31 years out, not 7d+5s.
    assert!(exp_over <= now + (signed_url::MAX_SIGNED_URL_TTL_SECS as i64 + 5) * 1000);

    let zero = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/{id}/signed-url?ttlSeconds=0"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    let exp_zero = zero.json::<serde_json::Value>().await?["expiresAt"]
        .as_i64()
        .unwrap();
    assert!(exp_zero >= now + 1000); // at least the 1s floor
    Ok(())
}

#[tokio::test]
async fn negative_ttl_clamps_to_minimum() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;
    let now = rtdb_server::db::now_ms();

    // A negative ttl clamps to the 1s floor (not the 1h default a naive
    // u64 parse would fall back to). Allow ~2s of HTTP round-trip slack.
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/{id}/signed-url?ttlSeconds=-5"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let exp = resp.json::<serde_json::Value>().await?["expiresAt"]
        .as_i64()
        .unwrap();
    assert!(exp <= now + 3000); // clamped to ~1s, well under the 3600s default
    assert!(exp >= now + 1000); // at least the 1s floor
    Ok(())
}

#[tokio::test]
async fn mint_requires_db_authorization() -> anyhow::Result<()> {
    let state = test_state().await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id_a, _) = seed(&state, &db_a).await;
    let token_b = mint_token(addr, &db_b).await; // authorized for db_b, not db_a

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db_a}/{id_a}/signed-url"
        ))
        .header("Authorization", format!("Bearer {token_b}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 403);

    // No bearer at all → 401.
    let noauth = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db_a}/{id_a}/signed-url"
        ))
        .send()
        .await?;
    assert_eq!(noauth.status(), 401);
    Ok(())
}
