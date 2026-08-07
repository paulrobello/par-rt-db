//! Integration tests for the Microsoft + Apple OAuth providers (ENH-001).
//!
//! Microsoft and Apple have hardcoded IdP endpoints (unlike GitHub, whose base
//! URLs are configurable), so — like the Google provider — they are exercised
//! through their `/auth/{provider}/begin` route wiring end-to-end, not through a
//! full wiremock code-exchange. The `begin` handler only *builds* an authorize
//! URL and returns it as JSON; it makes no outbound call, so no mock is needed
//! and no real IdP is hit. The token-exchange / userinfo *parsing* logic is
//! covered by unit tests in `auth/microsoft.rs` and `auth/apple.rs`; the shared
//! callback/state/poll machinery is covered by the GitHub tests in
//! `oauth_test.rs` (same generic handlers).
//!
//! One thing the Microsoft/Apple unit tests genuinely cannot cover is routing:
//! that each provider's routes are mounted, and — for Apple specifically — that
//! the dedicated POST `/auth/apple/callback` (Apple's `response_mode=form_post`)
//! is reachable and parses the form body into the state-claim flow. That is the
//! focus of the Apple callback test below.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use rtdb_server::config::Config;
use rtdb_server::{AppState, db};
use serde_json::Value;

use common::{spawn_app, test_config, test_hot};

const ALLOWED_ORIGIN: &str = "http://localhost:5173";

/// Spawns an app from an explicit `Config` (bootstrapping `rtdb_auth`), so each
/// provider's helper can configure just its own fields. Mirrors
/// `oauth_test.rs::google_configured_state`.
async fn configured_state(cfg: Config) -> (Arc<AppState>, SocketAddr) {
    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    let state = AppState::new(pool, cfg, test_hot());
    let addr = spawn_app(state.clone()).await;
    (state, addr)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build client")
}

async fn begin(addr: SocketAddr, provider: &str, origin: &str) -> reqwest::Response {
    client()
        .get(format!(
            "http://{addr}/auth/{provider}/begin?origin={origin}"
        ))
        .send()
        .await
        .expect("send begin")
}

async fn ms_configured_state() -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.microsoft_client_id = Some("ms-client".into());
    cfg.microsoft_client_secret = Some("ms-secret".into());
    configured_state(cfg).await
}

async fn apple_configured_state() -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.apple_client_id = Some("com.example.svc".into());
    cfg.apple_team_id = Some("TEAM".into());
    cfg.apple_key_id = Some("KEY".into());
    // `begin` never touches the key; any non-None value satisfies from_config.
    cfg.apple_private_key = Some("dummy".into());
    configured_state(cfg).await
}

// ---------------------------------------------------------------------------
// Microsoft
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(non_snake_case)]
async fn microsoft_begin_returns_microsoft_authorizeUrl() -> anyhow::Result<()> {
    let (_state, addr) = ms_configured_state().await;

    let resp = begin(addr, "microsoft", ALLOWED_ORIGIN).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    let url = body["authorizeUrl"]
        .as_str()
        .expect("authorizeUrl in begin response");
    assert!(url.starts_with("https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"));
    assert!(url.contains("client_id=ms-client"));
    assert!(url.contains("response_type=code"));
    assert!(url.contains("response_mode=query"));
    assert!(url.contains("scope=openid%20email%20profile"));
    assert!(url.contains("redirect_uri="));
    assert!(!extract_query_param(url, "state").is_empty());
    assert!(!body["state"].as_str().unwrap_or("").is_empty());
    Ok(())
}

#[tokio::test]
async fn microsoft_begin_honors_configured_tenant() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.microsoft_client_id = Some("ms-client".into());
    cfg.microsoft_client_secret = Some("ms-secret".into());
    cfg.microsoft_tenant = "contoso.onmicrosoft.com".into();
    let (_state, addr) = configured_state(cfg).await;

    let resp = begin(addr, "microsoft", ALLOWED_ORIGIN).await;
    let url = resp.json::<Value>().await?["authorizeUrl"]
        .as_str()
        .expect("authorizeUrl")
        .to_string();
    assert!(url.starts_with(
        "https://login.microsoftonline.com/contoso.onmicrosoft.com/oauth2/v2.0/authorize?"
    ));
    Ok(())
}

#[tokio::test]
async fn microsoft_begin_with_disallowed_origin_returns_forbidden() -> anyhow::Result<()> {
    let (_state, addr) = ms_configured_state().await;

    let resp = begin(addr, "microsoft", "http://evil.example").await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], "FORBIDDEN");
    Ok(())
}

#[tokio::test]
async fn microsoft_begin_unconfigured_returns_service_unavailable() -> anyhow::Result<()> {
    // test_config() leaves microsoft_client_id/secret as None.
    let state = common::test_state().await;
    let addr = spawn_app(state).await;

    let resp = begin(addr, "microsoft", ALLOWED_ORIGIN).await;
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

// ---------------------------------------------------------------------------
// Apple
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(non_snake_case)]
async fn apple_begin_returns_apple_authorizeUrl() -> anyhow::Result<()> {
    let (_state, addr) = apple_configured_state().await;

    let resp = begin(addr, "apple", ALLOWED_ORIGIN).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    let url = body["authorizeUrl"]
        .as_str()
        .expect("authorizeUrl in begin response");
    assert!(url.starts_with("https://appleid.apple.com/auth/authorize?"));
    assert!(url.contains("client_id=com.example.svc"));
    assert!(url.contains("response_type=code"));
    // Apple mandates form_post — the regression guard for the dedicated POST
    // callback route this provider depends on.
    assert!(url.contains("response_mode=form_post"));
    assert!(url.contains("scope=name%20email"));
    assert!(url.contains("redirect_uri="));
    assert!(!extract_query_param(url, "state").is_empty());
    assert!(!body["state"].as_str().unwrap_or("").is_empty());
    Ok(())
}

#[tokio::test]
async fn apple_begin_with_disallowed_origin_returns_forbidden() -> anyhow::Result<()> {
    let (_state, addr) = apple_configured_state().await;

    let resp = begin(addr, "apple", "http://evil.example").await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], "FORBIDDEN");
    Ok(())
}

#[tokio::test]
async fn apple_begin_unconfigured_returns_service_unavailable() -> anyhow::Result<()> {
    // test_config() leaves all apple_* fields as None.
    let state = common::test_state().await;
    let addr = spawn_app(state).await;

    let resp = begin(addr, "apple", ALLOWED_ORIGIN).await;
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

/// Apple POSTs `code` + `state` to the redirect URI (`response_mode=form_post`),
/// so the callback is a dedicated POST handler — the one piece of routing the
/// unit tests can't reach. With the login-CSRF check off (to isolate the state
/// path), a POST whose `state` matches no pending entry must reach
/// `claim_pending` and return 400 "invalid or expired state". That proves: the
/// POST route is mounted, the form body is parsed, and the state-claim flow runs
/// — not a 404/405 (route missing) and not a silent accept.
#[tokio::test]
async fn apple_callback_post_route_parses_form_and_requires_state() -> anyhow::Result<()> {
    let mut cfg = test_config();
    cfg.apple_client_id = Some("com.example.svc".into());
    cfg.apple_team_id = Some("TEAM".into());
    cfg.apple_key_id = Some("KEY".into());
    cfg.apple_private_key = Some("dummy".into());
    cfg.oauth_login_csrf = false; // isolate the state check from the CSRF gate
    let (_state, addr) = configured_state(cfg).await;

    let resp = client()
        .post(format!("http://{addr}/auth/apple/callback"))
        .form(&[("code", "abc"), ("state", "bogus-not-pending")])
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::BAD_REQUEST,
        "POST callback must reach the state check"
    );
    let body: Value = resp.json().await?;
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("invalid or expired state"),
        "expected the state-claim rejection, got: {body}"
    );
    Ok(())
}

/// The Apple callback is POST-only. A GET must be rejected (405 Method Not
/// Allowed), confirming the route is registered for POST — not, say, mounted on
/// the GET generic by mistake.
#[tokio::test]
async fn apple_callback_rejects_get() -> anyhow::Result<()> {
    let (_state, addr) = apple_configured_state().await;

    let resp = client()
        .get(format!(
            "http://{addr}/auth/apple/callback?code=abc&state=st"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::METHOD_NOT_ALLOWED);
    Ok(())
}

fn extract_query_param(url: &str, key: &str) -> String {
    let query = url.split('?').nth(1).unwrap_or("");
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if it.next() == Some(key) {
            return it.next().unwrap_or("").to_string();
        }
    }
    String::new()
}
