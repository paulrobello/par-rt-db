//! Integration tests for signed, time-limited storage URLs.
//!
//! Covers the mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) and the
//! additive signature verification on the public serve route
//! (`GET /storage/{id}?exp=&sig=`). The mint endpoint's returned `url` embeds
//! `config.public_url` (`http://localhost:0` in tests), so tests extract the
//! `/storage/...?...` path+query and re-fetch against the ephemeral test addr.

mod common;

use common::{admin_post, fresh_db, spawn_app, test_state, test_state_with_require_signed_urls};
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
        None,
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
async fn signature_is_bound_to_the_transform_it_was_minted_for() -> anyhow::Result<()> {
    // SEC-003: the signature covers the canonical transform params, so one
    // signature authorizes exactly one render. A URL minted for `w=100` must
    // not serve `w=200`, nor the un-transformed blob, and a plain signature
    // must not authorize a transform.
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/{id}/signed-url?w=100"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    let url = body["url"].as_str().expect("url present").to_string();
    assert!(
        url.contains("w=100"),
        "the mint must echo the render it signed: {url}"
    );
    let path = url[url.find("/storage/").expect("path present")..].to_string();
    let exp = body["expiresAt"].as_i64().expect("expiresAt present");
    let sig = url.rsplit("sig=").next().expect("sig in url").to_string();

    // Widening the render invalidates the signature.
    let widened = path.replace("w=100", "w=200");
    let resp = reqwest::get(format!("http://{addr}{widened}")).await?;
    assert_eq!(
        resp.status(),
        403,
        "a signature minted for w=100 must not serve w=200"
    );

    // Dropping the transform (full-resolution fetch) invalidates it too.
    let resp = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}&sig={sig}")).await?;
    assert_eq!(
        resp.status(),
        403,
        "a signature minted for a thumbnail must not serve the original"
    );

    // And the reverse: a plain signature must not authorize a transform.
    let plain_sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, "");
    let resp = reqwest::get(format!(
        "http://{addr}/storage/{id}?w=100&exp={exp}&sig={plain_sig}"
    ))
    .await?;
    assert_eq!(
        resp.status(),
        403,
        "an un-transformed signature must not authorize a render"
    );
    Ok(())
}

#[tokio::test]
async fn expired_signature_returns_403() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let past = rtdb_server::db::now_ms() - 1000;
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, past, "");
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
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, "");
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
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, "");
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
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, "");
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
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, "");

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

// --- SEC-113: require-signature mode + db-scope the mint ---

// SEC-113: when `RTDB_STORAGE_REQUIRE_SIGNED_URLS=true`, the public serve
// route rejects an opaque-id-only fetch (no `?exp=&sig=`) with 403. A valid
// signed URL still serves.
#[tokio::test]
async fn sec113_require_signed_mode_rejects_opaque_id_only() -> anyhow::Result<()> {
    let state = test_state_with_require_signed_urls().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;
    let token = mint_token(addr, &db).await;

    // Opaque id alone is rejected — the prior behavior (anyone with the id
    // fetches the bytes) is disabled in this mode.
    let no_sig = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(no_sig.status(), 403);

    // A valid signed URL still serves.
    let mint = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/signed-url"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(mint.status(), 200);
    let url = mint.json::<serde_json::Value>().await?["url"]
        .as_str()
        .expect("url")
        .to_string();
    let path = &url[url.find("/storage/").expect("path")..];
    let served = reqwest::get(format!("http://{addr}{path}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

// SEC-113: a partial signature (only `exp`, no `sig`) is rejected even in
// require-signed mode — both must be present.
#[tokio::test]
async fn sec113_require_signed_mode_rejects_partial_signature() -> anyhow::Result<()> {
    let state = test_state_with_require_signed_urls().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, _) = seed(&state, &db).await;

    let exp = rtdb_server::db::now_ms() + 60_000;
    let exp_only = reqwest::get(format!("http://{addr}/storage/{id}?exp={exp}")).await?;
    assert_eq!(exp_only.status(), 403);
    Ok(())
}

// SEC-113: in default (require-signed=false) mode, the opaque id alone still
// serves — guards against an accidental default flip that would break every
// existing public bearer URL.
#[tokio::test]
async fn sec113_default_mode_still_allows_opaque_id() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id, bytes) = seed(&state, &db).await;

    let served = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(served.status(), 200);
    assert_eq!(served.bytes().await?.as_ref(), &bytes[..]);
    Ok(())
}

// SEC-113: the mint endpoint must reject a caller authorized for db A asking
// to sign an id that lives in db B. Cross-db mismatch returns 404 (matching
// the authed-serve behavior for a foreign id — existence of an id in another
// db is not disclosed).
#[tokio::test]
async fn sec113_mint_rejects_cross_db_id() -> anyhow::Result<()> {
    let state = test_state().await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let (id_a, _) = seed(&state, &db_a).await;
    let token_b = mint_token(addr, &db_b).await;

    // token_b is authorized for db_b but id_a lives in db_a — the mint must
    // refuse, since minting grants a capability against the public serve route.
    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db_b}/{id_a}/signed-url"
        ))
        .header("Authorization", format!("Bearer {token_b}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);

    // Minting the same id against its real owner db_a still works.
    let token_a = mint_token(addr, &db_a).await;
    let ok = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db_a}/{id_a}/signed-url"
        ))
        .header("Authorization", format!("Bearer {token_a}"))
        .send()
        .await?;
    assert_eq!(ok.status(), 200);
    Ok(())
}

// SEC-113: minting an id that does not exist anywhere returns 404 (the
// storage_index lookup misses), not 403 — keeps the not-found vs forbidden
// distinction consistent with the authed serve route.
#[tokio::test]
async fn sec113_mint_unknown_id_returns_404() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/api/storage/{db}/never-existed/signed-url"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), 404);
    Ok(())
}
