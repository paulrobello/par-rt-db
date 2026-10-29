//! Bearer-delivery OAuth mode + fenced `POST /auth/state/revoke`.
//! Bearer begin alone returns the client-only revokeSecret. Only its sha256
//! verifier is stored. A lost begin response means no popup navigation and
//! the unused pending row expires via TTL. Failed callbacks and revoked
//! callbacks without durable settlement evidence remain unconfirmed, including
//! retries after a pre-marker crash. Token absence never proves settlement.
//!
//! Contract under test:
//! - `mode=bearer` at `/auth/{provider}/begin` persists a per-row delivery of
//!   `bearer`; unknown modes still reject `400` (the SEC-207 declared-value
//!   rule).
//! - A bearer callback sets NO `rtdb_session` cookie and NO admin-CSRF nonce,
//!   still clears the login-CSRF nonce, and the poll delivers the token
//!   one-shot (second poll answers `expired`).
//! - Default and cookie-mode flows are regression-identical to pre-change
//!   behavior.
//! - `/auth/state/revoke` fences all four interleavings (before-claim,
//!   during-mint, after-outcome, after-consumption): a `revoked` row can never
//!   deliver a token, and no live session survives a recorded revoke.
//!
//! CROSS-TAB SCOPE (inherited, fail-closed — card addendum): the login-CSRF
//! nonce and `rtdb-oauth-state` cookies are per-origin, so a browser carries
//! ONE concurrent login flow at a time; a second begin poisons the first tab's
//! in-flight flow — its callback fails the CSRF gate, its poll answers
//! `expired` WITHOUT consuming the state row (no token delivered), and no
//! failure path touches `rtdb_session`, so a pre-existing cookie-backed
//! session is preserved untouched. Bearer mode inherits this unchanged; the
//! `revoke_leaves_preexisting_cookie_session_valid` test pins the
//! pre-existing-session preservation end to end at the HTTP level. The card's
//! criterion 4 (a real-browser fixture proving the same) remains with the
//! orchestrator.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{spawn_app, test_config, test_hot, test_pool};
use rtdb_server::{AppState, db};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Builds an `AppState` whose GitHub URLs point at `mock`, OAuth credentials
/// configured, login-CSRF on — mirroring `oauth_test`'s fixture.
async fn bearer_state(mock: &MockServer) -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.oauth.github.base_url = mock.uri();
    cfg.oauth.github.api_url = mock.uri();
    cfg.oauth.github.client_id = Some("test-client".into());
    cfg.oauth.github.client_secret = Some("test-secret".into());
    cfg.oauth_login_csrf = true;

    let pool = test_pool(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");

    let state = AppState::new(pool, cfg, test_hot());
    let addr = spawn_app(state.clone()).await;
    (state, addr)
}

/// Mounts the three GitHub endpoints the callback hits, each delayed by
/// `delay` (a non-zero delay widens the claim→set_outcome window so the
/// during-mint revoke test can land deterministically inside it).
async fn mount_github_mocks(
    mock: &MockServer,
    github_id: i64,
    login: &str,
    email: &str,
    delay: Duration,
    expect_each: u64,
) {
    Mock::given(method("POST"))
        .and(path("/login/oauth/access_token"))
        .and(header("accept", "application/json"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "access_token": "gh-access-token",
                    "token_type": "bearer"
                }))
                .set_delay(delay),
        )
        .expect(expect_each)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({
                    "id": github_id,
                    "login": login,
                    "name": "Paul"
                }))
                .set_delay(delay),
        )
        .expect(expect_each)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user/emails"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([
                    {"email": email, "primary": true, "verified": true}
                ]))
                .set_delay(delay),
        )
        .expect(expect_each)
        .mount(mock)
        .await;
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()
        .expect("build client")
}

/// A unique user identity for one test — `rtdb_auth.users` enforces
/// `github_id` uniqueness across the shared test Postgres.
fn unique_github_id() -> i64 {
    i64::from_str_radix(&db::random_token()[..15], 16).expect("parse hex as i64")
}

fn unique_email(label: &str) -> String {
    format!("bearer-{label}-{}@example.com", db::random_token())
}

/// `None` for the historical default flow, `Some(m)` appends `&mode=<m>`.
fn mode_query(mode: Option<&str>) -> String {
    match mode {
        Some(m) => format!("&mode={m}"),
        None => String::new(),
    }
}

async fn begin_login(client: &reqwest::Client, addr: SocketAddr, mode: Option<&str>) -> String {
    begin_full(client, addr, mode)
        .await
        .0
        .expect("state field in begin response")
}

/// Begin + full response body: `(state, revokeSecret)`. Bearer begins carry
/// exactly `{authorizeUrl, state, revokeSecret}`; default/cookie begins carry
/// `{authorizeUrl, state}` with no revokeSecret field.
async fn begin_full(
    client: &reqwest::Client,
    addr: SocketAddr,
    mode: Option<&str>,
) -> (Option<String>, Option<String>) {
    let resp = client
        .get(format!(
            "http://{addr}/auth/github/begin?origin=http://localhost:5173{}",
            mode_query(mode)
        ))
        .send()
        .await
        .expect("send github begin");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("begin json");
    let keys: Vec<&str> = body
        .as_object()
        .expect("begin body object")
        .keys()
        .map(String::as_str)
        .collect();
    match mode {
        Some("bearer") => assert_eq!(keys, vec!["authorizeUrl", "revokeSecret", "state"]),
        _ => assert_eq!(keys, vec!["authorizeUrl", "state"]),
    }
    (
        body["state"].as_str().map(str::to_string),
        body["revokeSecret"].as_str().map(str::to_string),
    )
}

/// Bearer begin: returns `(state, revokeSecret)` — the begin response is the
/// ONLY place the client receives the revoke secret.
async fn begin_bearer(client: &reqwest::Client, addr: SocketAddr) -> (String, String) {
    let (state, secret) = begin_full(client, addr, Some("bearer")).await;
    (
        state.expect("bearer begin carries state"),
        secret.expect("bearer begin carries revokeSecret"),
    )
}

async fn callback(
    client: &reqwest::Client,
    addr: SocketAddr,
    state_token: &str,
) -> reqwest::Response {
    client
        .get(format!(
            "http://{addr}/auth/callback?code=abc&state={state_token}"
        ))
        .send()
        .await
        .expect("send callback")
}

async fn poll_state(client: &reqwest::Client, addr: SocketAddr, state_token: &str) -> Value {
    let resp = client
        .get(format!("http://{addr}/auth/state?state={state_token}"))
        .send()
        .await
        .expect("poll");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("poll json");
    assert!(body.get("revokeSecret").is_none());
    assert!(body.get("revoke_secret").is_none());
    body
}

/// Bare (cookie-jar-less) revoke with the capability pair. `None` secret
/// simulates the missing-secret case.
async fn revoke_state(
    addr: SocketAddr,
    state_token: &str,
    revoke_secret: Option<&str>,
) -> reqwest::Response {
    let body = match revoke_secret {
        Some(secret) => json!({"state": state_token, "revokeSecret": secret}),
        None => json!({"state": state_token}),
    };
    reqwest::Client::new()
        .post(format!("http://{addr}/auth/state/revoke"))
        .json(&body)
        .send()
        .await
        .expect("send revoke")
}

/// Sessions belonging to this test's user (scoped by its unique email so
/// concurrent tests sharing `rtdb_auth.sessions` never cross-contaminate).
async fn session_count_for_email(state: &Arc<AppState>, email: &str) -> i64 {
    let (count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM rtdb_auth.sessions s \
         JOIN rtdb_auth.users u ON u.id = s.user_id \
         WHERE u.email = $1",
    )
    .bind(email)
    .fetch_one(&state.pool)
    .await
    .expect("count sessions");
    count
}

/// Bounded wait until the state row reaches `want` (used to catch the callback
/// mid-mint). Panics after the deadline rather than hanging.
async fn wait_for_status(state: &Arc<AppState>, state_token: &str, want: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM rtdb_auth.oauth_states WHERE state = $1")
                .bind(state_token)
                .fetch_optional(&state.pool)
                .await
                .expect("read state status");
        if status.as_deref() == Some(want) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "state never reached {want}: {status:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// --- begin: opt-in mode parsing -------------------------------------------

#[tokio::test]
async fn begin_accepts_bearer_mode() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (_state, addr) = bearer_state(&mock).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/auth/github/begin?origin=http://localhost:5173&mode=bearer"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert!(body["state"].as_str().is_some());
    assert!(body["authorizeUrl"].as_str().is_some());
    Ok(())
}

// SEC-207 declared-value rule holds: anything but cookie/bearer rejects with
// the same 400 envelope shape as before.
#[tokio::test]
async fn begin_unknown_mode_still_rejects() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (_state, addr) = bearer_state(&mock).await;

    let resp = reqwest::Client::new()
        .get(format!(
            "http://{addr}/auth/github/begin?origin=http://localhost:5173&mode=bogus"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    Ok(())
}

// --- bearer callback: no cookies, poll-only credential ----------------------

#[tokio::test]
async fn bearer_callback_sets_no_cookies_and_poll_delivers_one_shot() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("happy");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, _bearer_secret) = begin_bearer(&client, addr).await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        resp.headers()
            .get("content-security-policy")
            .expect("csp header")
            .to_str()?
            .starts_with("default-src 'none'"),
        "same popup-closing HTML envelope as the default flow"
    );

    // NO rtdb_session and NO admin-CSRF Set-Cookie; the login-CSRF nonce IS
    // cleared (its Set-Cookie carries an empty value + Max-Age=0).
    let set_cookies: Vec<String> = resp
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    assert!(
        !set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb_session=") && !c.contains("Max-Age=0")),
        "bearer callback must not set rtdb_session: {set_cookies:?}"
    );
    assert!(
        !set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb-admin-csrf=")),
        "bearer callback must not mint the admin-CSRF nonce: {set_cookies:?}"
    );
    assert!(
        set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb-oauth-csrf=;") && c.contains("Max-Age=0")),
        "login-CSRF nonce must still be cleared: {set_cookies:?}"
    );

    let body = resp.text().await?;
    assert!(body.contains("window.close();"));

    // Poll delivers the token one-shot; the token works as a bearer.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    let token = pv["token"]
        .as_str()
        .expect("bearer poll must carry the token")
        .to_string();
    assert!(pv["user"]["email"].as_str().is_some());
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let me = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);

    // The poll delivers token + user only — the revoke capability is NOT
    // re-exposed here (it rode the begin response once).
    assert!(
        pv.get("revokeSecret").is_none(),
        "poll must not carry revokeSecret"
    );

    // One-shot: the second poll is expired.
    let pv2 = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv2["status"], json!("expired"));
    Ok(())
}

// --- default + cookie-mode regressions --------------------------------------

#[tokio::test]
async fn default_flow_regression_unchanged() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("default");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let state_token = begin_login(&client, addr, None).await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let set_cookies: Vec<String> = resp
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    let session_cookie = set_cookies
        .iter()
        .find(|c| c.starts_with("rtdb_session=") && !c.contains("Max-Age=0"))
        .expect("default callback must set rtdb_session");
    let token = session_cookie
        .split(';')
        .next()
        .expect("cookie pair")
        .strip_prefix("rtdb_session=")
        .expect("token value")
        .to_string();
    assert!(
        set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb-admin-csrf=") && !c.contains("Max-Age=0")),
        "default callback must mint the admin-CSRF nonce"
    );
    assert!(
        set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb-oauth-csrf=;") && c.contains("Max-Age=0")),
        "default callback must clear the login-CSRF nonce"
    );

    // Poll delivers the SAME token one-shot; the entry is then gone.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    assert_eq!(pv["token"].as_str(), Some(token.as_str()));
    let pv2 = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv2["status"], json!("expired"));
    assert_eq!(session_count_for_email(&state, &email).await, 1);
    Ok(())
}

#[tokio::test]
async fn cookie_mode_regression_unchanged() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("cookie");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let state_token = begin_login(&client, addr, Some("cookie")).await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let set_cookies: Vec<String> = resp
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok().map(str::to_string))
        .collect();
    assert!(
        set_cookies
            .iter()
            .any(|c| c.starts_with("rtdb_session=") && !c.contains("Max-Age=0")),
        "cookie-mode callback still sets the session cookie: {set_cookies:?}"
    );

    // SEC-207: the poll body carries NO token; the cookie authenticates.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    assert!(
        pv.get("token").is_none(),
        "SEC-207: cookie-mode poll body must not carry the token"
    );
    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    let me_body: Value = me.json().await?;
    assert_eq!(me_body["user"]["email"], json!(email));
    assert_eq!(session_count_for_email(&state, &email).await, 1);
    Ok(())
}

// --- state/revoke barrier tests (all four interleavings) --------------------

#[tokio::test]
async fn revoke_before_claim_blocks_callback() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("preclaim");
    // expect(0): the callback must never reach the provider.
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 0).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("revoked"));

    // The callback can no longer claim — the pending row is fenced.
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::BAD_REQUEST);

    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("expired"));
    assert_eq!(session_count_for_email(&state, &email).await, 0);
    Ok(())
}

#[tokio::test]
async fn revoke_during_mint_fences_the_fresh_session() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("duringmint");
    // 500ms per provider response widens the claim→set_outcome window to
    // ~1.5s so the revoke lands deterministically mid-mint.
    mount_github_mocks(
        &mock,
        unique_github_id(),
        "paul",
        &email,
        Duration::from_millis(500),
        1,
    )
    .await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;

    // Drive the callback concurrently and revoke once it is claiming —
    // i.e. between claim_pending and set_outcome.
    let cb_client = client.clone();
    let cb_state_token = state_token.clone();
    let cb_task = tokio::spawn(async move { callback(&cb_client, addr, &cb_state_token).await });
    wait_for_status(&state, &state_token, "claiming").await;

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unconfirmed"));

    // The callback still returns its popup HTML (the fence is invisible to
    // the popup), but the mint rolls forward into the cancellation.
    let early = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(early.json::<Value>().await?["status"], json!("unconfirmed"));
    let cb = cb_task.await.expect("join callback");
    assert_eq!(cb.status(), reqwest::StatusCode::OK);

    // NO live session survives, and the poll can never deliver a token.
    assert_eq!(session_count_for_email(&state, &email).await, 0);
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("expired"));
    Ok(())
}

#[tokio::test]
async fn revoke_after_outcome_deletes_session_and_kills_token() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("afteroutcome");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    // The mint happened (outcome recorded, token not yet polled).
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("revoked"));
    assert_eq!(session_count_for_email(&state, &email).await, 0);

    // The token can never be delivered — the poll is dead for a revoked row.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("expired"));
    Ok(())
}

#[tokio::test]
async fn revoke_after_consumption_kills_delivered_token() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("afterconsumption");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);

    // Consume: the poll delivered the token; the caller now holds a live
    // session token and asks for cleanup.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    let token = pv["token"].as_str().expect("delivered token").to_string();

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("revoked"));
    assert_eq!(session_count_for_email(&state, &email).await, 0);

    // The delivered token is dead, and the row is gone for revoke purposes
    // (a repeat answers unknown).
    let me = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(me.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Retryable confirm: an already-revoked row re-answers `revoked` (not
    // unknown) so a client can confirm cleanup after any partial failure.
    let again = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    let again_body: Value = again.json().await?;
    assert_eq!(again_body["status"], json!("revoked"));
    Ok(())
}

#[tokio::test]
async fn revoke_unknown_state_returns_unknown() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (state, addr) = bearer_state(&mock).await;

    // Never minted.
    let resp = revoke_state(addr, "no-such-state", None).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unknown"));

    // TTL-swept: expire the row, run the sweep, then revoke.
    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;
    sqlx::query("UPDATE rtdb_auth.oauth_states SET expires_at = 1 WHERE state = $1")
        .bind(&state_token)
        .execute(&state.pool)
        .await
        .expect("expire row");
    let swept = rtdb_server::auth::provider::sweep_oauth_states(&state.pool).await?;
    assert_eq!(swept, 1);
    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unknown"));
    Ok(())
}

// The-cube cross-tab ask: a bearer-only login's cleanup must leave a
// PRE-EXISTING cookie-backed session valid, and the revoke endpoint itself
// must emit no Set-Cookie headers at all (unlike POST /auth/logout, which
// still attaches them unconditionally — the deferred follow-up card).
#[tokio::test]
async fn revoke_leaves_preexisting_cookie_session_valid() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("crosstab");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 2).await;
    let (state, addr) = bearer_state(&mock).await;

    // Tab 1: default login — the jar now holds the cookie-backed session.
    let client = no_redirect_client();
    let default_state = begin_login(&client, addr, None).await;
    let cb = callback(&client, addr, &default_state).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    // Tab 2 (same browser jar): a bearer login — its callback must NOT
    // clobber the pre-existing rtdb_session cookie.
    let (bearer_state_token, bearer_secret) = begin_bearer(&client, addr).await;
    let cb = callback(&client, addr, &bearer_state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    let me_body: Value = me.json().await?;
    assert_eq!(me_body["user"]["email"], json!(email));
    assert_eq!(session_count_for_email(&state, &email).await, 2);

    let pv = poll_state(&client, addr, &bearer_state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    let bearer_token = pv["token"].as_str().expect("bearer token").to_string();

    // Cleanup with credentials omitted (bare client): the revoke carries NO
    // Set-Cookie headers, kills the bearer session, and leaves the
    // cookie-backed session untouched.
    let resp = revoke_state(addr, &bearer_state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .count(),
        0,
        "state/revoke must never emit Set-Cookie headers"
    );
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("revoked"));
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let me = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {bearer_token}"))
        .send()
        .await?;
    assert_eq!(me.status(), reqwest::StatusCode::UNAUTHORIZED);

    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    let me_body: Value = me.json().await?;
    assert_eq!(me_body["user"]["email"], json!(email));
    Ok(())
}

// Partial-failure recovery: a row left `revoked` with its session_token still
// recorded (the pre-transactional partial state, or the marker a crashed
// roll-forward leaves) is cleaned up by the next revoke — the session is
// deleted, the token cleared, and BOTH calls answer confirmed `revoked`.
#[tokio::test]
async fn revoke_retry_after_partial_failure_recovers() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("partial");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    // Simulate the partial state: fence committed, cleanup lost.
    sqlx::query("UPDATE rtdb_auth.oauth_states SET status = 'revoked' WHERE state = $1")
        .bind(&state_token)
        .execute(&state.pool)
        .await
        .expect("mark revoked out-of-band");

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("revoked"));
    assert_eq!(session_count_for_email(&state, &email).await, 0);

    // The recorded plaintext was cleared, and a second revoke re-confirms
    // rather than answering unknown.
    let (token_left,): (Option<String>,) =
        sqlx::query_as("SELECT session_token FROM rtdb_auth.oauth_states WHERE state = $1")
            .bind(&state_token)
            .fetch_one(&state.pool)
            .await
            .expect("read token back");
    assert!(token_left.is_none());
    let again = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    let again_body: Value = again.json().await?;
    assert_eq!(again_body["status"], json!("revoked"));
    Ok(())
}

// Delayed-callback barrier: revoke while claiming, let the callback settle,
// then retry — the retry confirms and no live session survived the fence +
// roll-forward.
#[tokio::test]
async fn revoke_retry_after_callback_settlement_confirms() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("settle");
    mount_github_mocks(
        &mock,
        unique_github_id(),
        "paul",
        &email,
        Duration::from_millis(500),
        1,
    )
    .await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let (state_token, bearer_secret) = begin_bearer(&client, addr).await;
    let cb_client = client.clone();
    let cb_state_token = state_token.clone();
    let cb_task = tokio::spawn(async move { callback(&cb_client, addr, &cb_state_token).await });
    wait_for_status(&state, &state_token, "claiming").await;

    let resp = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unconfirmed"));

    let early = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    assert_eq!(early.json::<Value>().await?["status"], json!("unconfirmed"));
    let cb = cb_task.await.expect("join callback");
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    assert_eq!(session_count_for_email(&state, &email).await, 0);

    // The post-settlement retry confirms (idempotent), no session revived.
    let retry = revoke_state(addr, &state_token, Some(&bearer_secret)).await;
    let retry_body: Value = retry.json().await?;
    assert_eq!(retry_body["status"], json!("revoked"));
    assert_eq!(session_count_for_email(&state, &email).await, 0);
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("expired"));
    Ok(())
}

// Scope restriction: state/revoke is a BEARER-flow capability. A default-flow
// state is refused with the unknown-shaped body, deletes nothing, and the
// flow proceeds untouched — the poll still delivers and the cookie session
// still authenticates.
#[tokio::test]
async fn revoke_of_default_mode_state_is_refused() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("refused-default");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let state_token = begin_login(&client, addr, None).await;
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let resp = revoke_state(addr, &state_token, None).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unknown"));
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    // Untouched: the poll still delivers one-shot and the cookie works.
    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    assert!(pv["token"].as_str().is_some());
    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    Ok(())
}

// Same refusal for a cookie-mode state: unknown-shaped, session preserved,
// cookie authenticates, poll completes tokenless.
#[tokio::test]
async fn revoke_of_cookie_mode_state_is_refused() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let email = unique_email("refused-cookie");
    mount_github_mocks(&mock, unique_github_id(), "paul", &email, Duration::ZERO, 1).await;
    let (state, addr) = bearer_state(&mock).await;

    let client = no_redirect_client();
    let state_token = begin_login(&client, addr, Some("cookie")).await;
    let cb = callback(&client, addr, &state_token).await;
    assert_eq!(cb.status(), reqwest::StatusCode::OK);
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let resp = revoke_state(addr, &state_token, None).await;
    let body: Value = resp.json().await?;
    assert_eq!(body["status"], json!("unknown"));
    assert_eq!(session_count_for_email(&state, &email).await, 1);

    let pv = poll_state(&client, addr, &state_token).await;
    assert_eq!(pv["status"], json!("complete"));
    assert!(pv.get("token").is_none());
    let me = client.get(format!("http://{addr}/auth/me")).send().await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    let me_body: Value = me.json().await?;
    assert_eq!(me_body["user"]["email"], json!(email));
    Ok(())
}

#[tokio::test]
async fn revoke_requires_secret_and_pending_cancel_remains_retryable() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (state, addr) = bearer_state(&mock).await;
    let client = no_redirect_client();
    let (state_token, secret) = begin_bearer(&client, addr).await;
    let verifier: Option<String> = sqlx::query_scalar(
        "SELECT revoke_secret_verifier FROM rtdb_auth.oauth_states WHERE state = $1",
    )
    .bind(&state_token)
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(verifier.as_deref(), Some(db::sha256_hex(&secret).as_str()));
    for supplied in [None, Some("wrong")] {
        let response = revoke_state(addr, &state_token, supplied).await;
        assert_eq!(response.json::<Value>().await?, json!({"status":"unknown"}));
        let status: String =
            sqlx::query_scalar("SELECT status FROM rtdb_auth.oauth_states WHERE state = $1")
                .bind(&state_token)
                .fetch_one(&state.pool)
                .await?;
        assert_eq!(status, "pending");
    }
    let response = revoke_state(addr, &state_token, Some(&secret)).await;
    assert_eq!(response.json::<Value>().await?, json!({"status":"revoked"}));
    let wrong = revoke_state(addr, &state_token, Some("wrong")).await;
    assert_eq!(wrong.json::<Value>().await?, json!({"status":"unknown"}));
    let retry = revoke_state(addr, &state_token, Some(&secret)).await;
    assert_eq!(retry.json::<Value>().await?, json!({"status":"revoked"}));
    Ok(())
}

#[tokio::test]
async fn failed_bearer_revoke_is_unconfirmed() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (state, addr) = bearer_state(&mock).await;
    let client = no_redirect_client();
    let (state_token, secret) = begin_bearer(&client, addr).await;
    sqlx::query("UPDATE rtdb_auth.oauth_states SET status = 'failed', callback_settled_at = $1 WHERE state = $2")
        .bind(db::now_ms()).bind(&state_token).execute(&state.pool).await?;
    for _ in 0..2 {
        let response = revoke_state(addr, &state_token, Some(&secret)).await;
        assert_eq!(
            response.json::<Value>().await?,
            json!({"status":"unconfirmed"})
        );
    }
    Ok(())
}

#[tokio::test]
async fn pre_marker_crash_retries_remain_unconfirmed() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (state, addr) = bearer_state(&mock).await;
    let client = no_redirect_client();
    let (state_token, secret) = begin_bearer(&client, addr).await;
    // Callback claimed, then crashed before writing its mint/settlement marker.
    sqlx::query("UPDATE rtdb_auth.oauth_states SET status = 'claiming' WHERE state = $1")
        .bind(&state_token)
        .execute(&state.pool)
        .await?;
    for _ in 0..3 {
        let response = revoke_state(addr, &state_token, Some(&secret)).await;
        assert_eq!(
            response.json::<Value>().await?,
            json!({"status":"unconfirmed"})
        );
    }
    let settled: Option<i64> = sqlx::query_scalar(
        "SELECT callback_settled_at FROM rtdb_auth.oauth_states WHERE state = $1",
    )
    .bind(&state_token)
    .fetch_one(&state.pool)
    .await?;
    assert!(settled.is_none());
    Ok(())
}

/// Raw-body revoke for shape-gate cases: sends `body` verbatim.
async fn revoke_raw(addr: SocketAddr, body: String) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/auth/state/revoke"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("send raw revoke")
}

/// Body-shape gate + per-IP rate budget, both uniform: every refusal answers
/// the same unknown-shaped 200 (limiter denial, oversize, wrong types, bad
/// JSON) and none of them reaches the fence — the state row is untouched
/// throughout.
#[tokio::test]
async fn revoke_shape_gate_and_rate_budget_are_uniform() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (state, addr) = bearer_state(&mock).await;
    let client = no_redirect_client();
    let (state_token, secret) = begin_bearer(&client, addr).await;

    let oversized_field = format!("\"{}\"", "x".repeat(300));
    let cases = [
        ("non-object body", "hello".to_string()),
        (
            "state not a string",
            format!("{{\"state\": 123, \"revokeSecret\": \"{secret}\"}}"),
        ),
        (
            "state over field bound",
            format!("{{\"state\": {oversized_field}, \"revokeSecret\": \"{secret}\"}}"),
        ),
        (
            "oversized total payload",
            format!(
                "{{\"state\": \"{}\", \"revokeSecret\": \"{}\", \"pad\": \"{}\"}}",
                "x".repeat(43),
                secret,
                "y".repeat(5000)
            ),
        ),
    ];
    for (label, body) in cases {
        let resp = revoke_raw(addr, body).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "{label}");
        assert_eq!(
            resp.json::<Value>().await?,
            json!({"status": "unknown"}),
            "{label}"
        );
    }
    let status: String =
        sqlx::query_scalar("SELECT status FROM rtdb_auth.oauth_states WHERE state = $1")
            .bind(&state_token)
            .fetch_one(&state.pool)
            .await
            .expect("read row status");
    assert_eq!(
        status, "pending",
        "shape-gate refusals must not touch the row"
    );

    // Rate budget: 30 shape-valid requests fill the fixed window; everything
    // past it is refused with the SAME unknown-shaped 200 whether the
    // refusal was the limiter or a wrong capability, and the row survives
    // untouched either way.
    for _ in 0..31 {
        let resp = revoke_state(addr, &state_token, Some("wrong")).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        assert_eq!(resp.json::<Value>().await?, json!({"status": "unknown"}));
    }
    let status: String =
        sqlx::query_scalar("SELECT status FROM rtdb_auth.oauth_states WHERE state = $1")
            .bind(&state_token)
            .fetch_one(&state.pool)
            .await
            .expect("read row status");
    assert_eq!(
        status, "pending",
        "limiter refusals must not reach the fence"
    );

    // Recovery is per-IP and window-scoped, never a lockout: a fresh key is
    // admitted immediately (white-box probe of the same limiter the route
    // uses). The fixed window itself refills at the next wall-clock minute —
    // structural to the limiter's minute-bucket key, not sleep-tested here.
    let fresh = state
        .limits
        .rate_limiter
        .check(
            rtdb_server::rate_limit::RateKey::Ip {
                route: "state_revoke",
                ip: "203.0.113.7".into(),
            },
            30,
        )
        .await;
    assert!(
        matches!(fresh, rtdb_server::rate_limit::RateDecision::Allowed),
        "a fresh per-IP key must not be locked out by another key's exhaustion"
    );
    Ok(())
}
