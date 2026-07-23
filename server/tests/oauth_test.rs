mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use common::{admin_post, fresh_db, spawn_app, test_config, test_state};
use futures_util::{SinkExt, StreamExt};
use rtdb_server::{AppState, db};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Builds an `AppState` whose GitHub URLs point at `mock`, with OAuth
/// credentials configured, and spawns it. Bootstrap runs on a fresh
/// connection to the shared test Postgres, matching `test_state()`'s setup.
async fn oauth_state(mock: &MockServer) -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.github_base_url = mock.uri();
    cfg.github_api_url = mock.uri();
    cfg.github_client_id = Some("test-client".into());
    cfg.github_client_secret = Some("test-secret".into());

    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");

    let state = AppState::new(pool, cfg);
    let addr = spawn_app(state.clone()).await;
    (state, addr)
}

/// Mounts the three GitHub endpoints the callback hits, each asserting the
/// exact headers the brief requires and expected to be called exactly once —
/// if the handler ever omits `Accept`/`User-Agent`/`Authorization`, or
/// re-fetches after a replay, `MockServer`'s drop-time verification fails
/// the test.
async fn mount_github_mocks(mock: &MockServer, email_body: Value) {
    mount_github_user_mocks(mock, 42, "paul", email_body).await;
}

/// Parameterized variant of `mount_github_mocks` so cross-provider tests can
/// use a distinct `github_id`/`login` (the default helper's fixed `id: 42`
/// would collide in the shared `rtdb_auth.users` table across parallel tests).
async fn mount_github_user_mocks(
    mock: &MockServer,
    github_id: i64,
    login: &str,
    email_body: Value,
) {
    Mock::given(method("POST"))
        .and(path("/login/oauth/access_token"))
        .and(header("accept", "application/json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "gh-access-token",
            "token_type": "bearer"
        })))
        .expect(1)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": github_id,
            "login": login,
            "name": "Paul"
        })))
        .expect(1)
        .mount(mock)
        .await;

    Mock::given(method("GET"))
        .and(path("/user/emails"))
        .and(header("user-agent", "par-rt-db"))
        .and(header("authorization", "Bearer gh-access-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(email_body))
        .expect(1)
        .mount(mock)
        .await;
}

fn extract_query_param(url: &str, key: &str) -> String {
    let query = url.split('?').nth(1).expect("query string");
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=')
            && k == key
        {
            return v.to_string();
        }
    }
    panic!("missing query param {key} in {url}");
}

fn extract_token_from_html(body: &str) -> String {
    let marker = "token:\"";
    let start = body.find(marker).expect("token marker in html") + marker.len();
    let rest = &body[start..];
    let end = rest.find('"').expect("closing quote after token");
    rest[..end].to_string()
}

fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("build client")
}

async fn start_login(client: &reqwest::Client, addr: SocketAddr, origin: &str) -> String {
    let resp = client
        .get(format!("http://{addr}/auth/github?origin={origin}"))
        .send()
        .await
        .expect("send github start");
    assert_eq!(resp.status(), reqwest::StatusCode::FOUND);
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("location header")
        .to_str()
        .expect("location header is valid utf8")
        .to_string();
    extract_query_param(&location, "state")
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

/// Drives a full `/auth/github` -> `/auth/callback` round trip, asserting
/// the callback's content type, CSP header, and origin, and returns the
/// session token embedded in the response HTML.
async fn login_flow(addr: SocketAddr, origin: &str) -> String {
    let client = no_redirect_client();
    let state_token = start_login(&client, addr, origin).await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(
        resp.headers()
            .get(reqwest::header::CONTENT_TYPE)
            .expect("content-type header")
            .to_str()
            .expect("utf8")
            .starts_with("text/html")
    );
    assert_eq!(
        resp.headers()
            .get("content-security-policy")
            .expect("csp header")
            .to_str()
            .expect("utf8"),
        "default-src 'none'; script-src 'unsafe-inline'"
    );

    let body = resp.text().await.expect("read callback body");
    assert!(body.contains(origin));

    extract_token_from_html(&body)
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_connect(addr: SocketAddr) -> WsStream {
    let (ws, _) = connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn ws_send_json(ws: &mut WsStream, msg: Value) {
    ws.send(WsMessage::Text(msg.to_string().into()))
        .await
        .expect("send frame");
}

async fn ws_recv_json(ws: &mut WsStream) -> Value {
    match ws.next().await.expect("stream ended").expect("frame ok") {
        WsMessage::Text(text) => serde_json::from_str(&text).expect("parse json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn ws_auth(ws: &mut WsStream, token: &str, db: &str) -> Value {
    ws_send_json(ws, json!({"type": "auth", "token": token, "db": db})).await;
    ws_recv_json(ws).await
}

fn verified_primary_email(email: &str) -> Value {
    json!([{"email": email, "primary": true, "verified": true}])
}

/// A pseudo-random positive `i64` for `github_id`, unique across parallel tests
/// sharing the `rtdb_auth.users` table (which enforces `github_id` uniqueness).
/// 15 hex nibbles max out well under `i64::MAX`.
fn unique_github_id() -> i64 {
    i64::from_str_radix(&db::random_token()[..15], 16).expect("parse hex as i64")
}

fn insert_work_item_txn() -> Value {
    json!({"steps": [{"op": "insert", "table": "workItems", "doc": {
        "projectId": "0".repeat(32),
        "title": "item",
        "status": "backlog",
        "order": 1.0,
        "completedAt": null
    }}]})
}

// (a) full flow: start -> callback -> 200 HTML containing the token and the exact origin.
#[tokio::test]
async fn full_oauth_flow_returns_html_with_session_token() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (_state, addr) = oauth_state(&mock).await;

    let token = login_flow(addr, "http://localhost:5173").await;
    assert!(!token.is_empty());
    Ok(())
}

// (b) /auth/me with the session token -> email correct, plus the GitHub
// identity (login + id) surfaced from the resolved session's user row.
#[tokio::test]
async fn me_with_session_token_returns_email() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (_state, addr) = oauth_state(&mock).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["user"]["kind"], json!("user"));
    assert_eq!(body["user"]["email"], json!("probello@gmail.com"));
    assert_eq!(body["user"]["githubLogin"], json!("paul"));
    assert_eq!(body["user"]["githubId"], json!(42));
    Ok(())
}

// (b2) /auth/validate with a real session token returns the authed user with
// GitHub identity — same machinery as /auth/me, but available to a trusted
// backend validating a player's token rather than the connection's own.
#[tokio::test]
async fn validate_with_session_token_returns_user() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (_state, addr) = oauth_state(&mock).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/validate"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["user"]["kind"], json!("user"));
    assert_eq!(body["user"]["email"], json!("probello@gmail.com"));
    assert_eq!(body["user"]["githubLogin"], json!("paul"));
    assert_eq!(body["user"]["githubId"], json!(42));
    Ok(())
}

// (b3) /auth/validate rejects an invalid/expired token with the standard
// Unauthorized envelope rather than a 500, keeping 500s generic. No OAuth
// flow is exercised, so no GitHub mocks are mounted.
#[tokio::test]
async fn validate_rejects_invalid_token() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (_state, addr) = oauth_state(&mock).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/validate"))
        .header("Authorization", "Bearer not-a-real-token")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
    Ok(())
}

// (b4) /auth/validate requires a bearer token — missing header is a 401, not a 500.
#[tokio::test]
async fn validate_rejects_missing_token() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (_state, addr) = oauth_state(&mock).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/validate"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

// (c) allowlist the OAuth'd email -> WS auth with the session token succeeds
// with user.kind == "user"; remove from the allowlist -> new WS auth -> authErr FORBIDDEN.
#[tokio::test]
async fn session_token_authorizes_ws_only_while_allowlisted() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (state, addr) = oauth_state(&mock).await;
    let db_name = fresh_db(&state).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let add_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "add", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(add_resp.status(), reqwest::StatusCode::OK);

    let mut ws = ws_connect(addr).await;
    let msg = ws_auth(&mut ws, &token, &db_name).await;
    assert_eq!(msg["type"], json!("authOk"));
    assert_eq!(msg["user"]["kind"], json!("user"));

    let remove_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "remove", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(remove_resp.status(), reqwest::StatusCode::OK);

    let mut ws2 = ws_connect(addr).await;
    let msg2 = ws_auth(&mut ws2, &token, &db_name).await;
    assert_eq!(msg2["type"], json!("authErr"));
    assert_eq!(msg2["error"]["code"], json!("FORBIDDEN"));
    Ok(())
}

// (c2) A3: live authz on every WS op. Subscribe and mutate succeed while allowlisted;
// after an admin allowlist removal, a mutate on the SAME open connection gets mutateErr
// FORBIDDEN (not a close) and the connection stays usable (a following ping still pongs).
#[tokio::test]
async fn allowlist_removal_mid_session_fails_mutate_without_closing_connection()
-> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (state, addr) = oauth_state(&mock).await;
    let db_name = fresh_db(&state).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let add_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "add", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(add_resp.status(), reqwest::StatusCode::OK);

    let mut ws = ws_connect(addr).await;
    let auth_msg = ws_auth(&mut ws, &token, &db_name).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    ws_send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let sub_msg = ws_recv_json(&mut ws).await;
    assert_eq!(sub_msg["type"], json!("queryUpdate"));

    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_mutate_ok = false;
    for _ in 0..2 {
        let msg = ws_recv_json(&mut ws).await;
        if msg["type"] == json!("mutateOk") {
            assert_eq!(msg["mutId"], json!("m1"));
            saw_mutate_ok = true;
        }
    }
    assert!(saw_mutate_ok, "expected mutateOk before allowlist removal");

    let remove_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "remove", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(remove_resp.status(), reqwest::StatusCode::OK);

    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let err_msg = ws_recv_json(&mut ws).await;
    assert_eq!(err_msg["type"], json!("mutateErr"));
    assert_eq!(err_msg["mutId"], json!("m2"));
    assert_eq!(err_msg["error"]["code"], json!("FORBIDDEN"));

    // Connection stays open (not closed by the authz failure): a subsequent
    // ping still round-trips.
    ws_send_json(&mut ws, json!({"type": "ping"})).await;
    let pong = ws_recv_json(&mut ws).await;
    assert_eq!(pong["type"], json!("pong"));

    Ok(())
}

// (c3) rank 8: live session-expiry enforcement on every WS op. `authorize()`
// checks the session's `expires_at` as captured once by the connection's
// `Principal` at auth time (see `auth::authorize` doc comment), so mid-
// connection expiry can only be simulated by shortening the session's window
// *before* connecting and then letting real wall-clock time actually cross
// it while the connection stays open — mutating the row after connecting
// would be invisible to the already-cached principal. Subscribe and mutate
// succeed while still inside that window; once real time passes it, the next
// subscribe AND the next mutate on the SAME open connection get
// subscribeErr/mutateErr UNAUTHORIZED (not a close) and the connection stays
// usable (a following ping still pongs), so a client can retry with a fresh
// token.
#[tokio::test]
async fn session_expiry_mid_connection_denies_operations_but_keeps_connection_usable()
-> anyhow::Result<()> {
    const SHORT_WINDOW_MS: i64 = 1_500;

    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (state, addr) = oauth_state(&mock).await;
    let db_name = fresh_db(&state).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let add_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "add", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(add_resp.status(), reqwest::StatusCode::OK);

    // Shorten the just-minted session's window before connecting: still
    // valid right now, but expiring soon enough for the test to wait it out.
    sqlx::query("UPDATE rtdb_auth.sessions SET expires_at = $1 WHERE token_hash = $2")
        .bind(db::now_ms() + SHORT_WINDOW_MS)
        .bind(db::sha256_hex(&token))
        .execute(&state.pool)
        .await?;

    let mut ws = ws_connect(addr).await;
    let auth_msg = ws_auth(&mut ws, &token, &db_name).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // While the session is valid: subscribe and mutate both succeed.
    ws_send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let sub_msg = ws_recv_json(&mut ws).await;
    assert_eq!(sub_msg["type"], json!("queryUpdate"));

    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_mutate_ok = false;
    for _ in 0..2 {
        let msg = ws_recv_json(&mut ws).await;
        if msg["type"] == json!("mutateOk") {
            assert_eq!(msg["mutId"], json!("m1"));
            saw_mutate_ok = true;
        }
    }
    assert!(saw_mutate_ok, "expected mutateOk while session is valid");

    // Let real wall-clock time actually cross the connect-time-cached
    // expires_at while the connection stays open the whole time.
    tokio::time::sleep(std::time::Duration::from_millis(
        SHORT_WINDOW_MS as u64 + 500,
    ))
    .await;

    // The next subscribe on the same open connection is rejected.
    ws_send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q2", "query": {"table": "workItems"}}),
    )
    .await;
    let sub_err = ws_recv_json(&mut ws).await;
    assert_eq!(sub_err["type"], json!("subscribeErr"));
    assert_eq!(sub_err["queryId"], json!("q2"));
    assert_eq!(sub_err["error"]["code"], json!("UNAUTHORIZED"));

    // And so is the next mutate.
    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut_err = ws_recv_json(&mut ws).await;
    assert_eq!(mut_err["type"], json!("mutateErr"));
    assert_eq!(mut_err["mutId"], json!("m2"));
    assert_eq!(mut_err["error"]["code"], json!("UNAUTHORIZED"));

    // Connection stays open (not closed by the expiry failure): a subsequent
    // ping still round-trips.
    ws_send_json(&mut ws, json!({"type": "ping"})).await;
    let pong = ws_recv_json(&mut ws).await;
    assert_eq!(pong["type"], json!("pong"));

    Ok(())
}

// (d) disallowed origin -> 403.
#[tokio::test]
async fn github_start_with_disallowed_origin_returns_forbidden() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    let (_state, addr) = oauth_state(&mock).await;

    let resp = no_redirect_client()
        .get(format!(
            "http://{addr}/auth/github?origin=http://evil.example"
        ))
        .send()
        .await?;

    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("FORBIDDEN"));
    Ok(())
}

// (e) replayed state -> 400.
#[tokio::test]
async fn replayed_state_returns_bad_request() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (_state, addr) = oauth_state(&mock).await;

    let client = no_redirect_client();
    let state_token = start_login(&client, addr, "http://localhost:5173").await;

    let first = callback(&client, addr, &state_token).await;
    assert_eq!(first.status(), reqwest::StatusCode::OK);

    let second = callback(&client, addr, &state_token).await;
    assert_eq!(second.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = second.json().await?;
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    Ok(())
}

// (f) logout -> /auth/me 401.
#[tokio::test]
async fn logout_invalidates_session_for_me() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("probello@gmail.com")).await;
    let (_state, addr) = oauth_state(&mock).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let logout_resp = reqwest::Client::new()
        .post(format!("http://{addr}/auth/logout"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(logout_resp.status(), reqwest::StatusCode::OK);
    let logout_body: Value = logout_resp.json().await?;
    assert_eq!(logout_body["ok"], json!(true));

    let me_resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(me_resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

// (g) expired session (row inserted directly with a past expires_at) -> 401.
#[tokio::test]
async fn expired_session_returns_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;

    let user_id = db::new_id();
    let github_id = i64::from_str_radix(&db::random_token()[..15], 16).expect("parse hex as i64");
    let email = format!("expired-{}@example.com", db::new_id());
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&user_id)
    .bind(github_id)
    .bind("expired-user")
    .bind(&email)
    .bind(db::now_ms())
    .execute(&state.pool)
    .await?;

    let token = db::random_token();
    let hash = db::sha256_hex(&token);
    sqlx::query(
        "INSERT INTO rtdb_auth.sessions (token_hash, user_id, expires_at, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&hash)
    .bind(&user_id)
    .bind(db::now_ms() - 1_000)
    .bind(db::now_ms())
    .execute(&state.pool)
    .await?;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

// --- Google provider (the provider abstraction's second implementation) ---
//
// These exercise the new `/auth/google` route wiring and the generic
// `provider_start` handler end-to-end through the real router. The start
// handler only *builds* an authorize URL and 302s — it makes no outbound call
// to Google — so no mock is needed and no real Google endpoint is hit. The
// token-exchange / userinfo *parsing* logic is covered by unit tests in
// `auth/google.rs`; the shared callback/state/HTML/logout machinery is
// covered by the GitHub tests above (same generic handlers).

/// Spawns an app with Google OAuth configured. Endpoints stay at the real
/// Google constants (not configurable, unlike GitHub's GHE-overrideable URLs).
async fn google_configured_state() -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.google_client_id = Some("g-client".into());
    cfg.google_client_secret = Some("g-secret".into());
    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    let state = AppState::new(pool, cfg);
    let addr = spawn_app(state.clone()).await;
    (state, addr)
}

// (h) configured Google provider -> 302 to Google's authorize URL with the
// expected OIDC params.
#[tokio::test]
async fn google_start_redirects_to_google_authorize_url() -> anyhow::Result<()> {
    let (_state, addr) = google_configured_state().await;

    let resp = no_redirect_client()
        .get(format!(
            "http://{addr}/auth/google?origin=http://localhost:5173"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::FOUND);
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("location header")
        .to_str()
        .expect("utf8")
        .to_string();
    assert!(location.starts_with("https://accounts.google.com/o/oauth2/v2/auth?"));
    assert!(location.contains("client_id=g-client"));
    assert!(location.contains("response_type=code"));
    assert!(location.contains("scope=openid%20email%20profile"));
    assert!(location.contains("redirect_uri="));
    assert!(!extract_query_param(&location, "state").is_empty());
    Ok(())
}

// (i) disallowed origin -> 403 (origin check runs after the configured check).
#[tokio::test]
async fn google_start_with_disallowed_origin_returns_forbidden() -> anyhow::Result<()> {
    let (_state, addr) = google_configured_state().await;

    let resp = no_redirect_client()
        .get(format!(
            "http://{addr}/auth/google?origin=http://evil.example"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("FORBIDDEN"));
    Ok(())
}

// (j) route mounted but provider unconfigured (no client_id/secret) -> 503.
#[tokio::test]
async fn google_start_unconfigured_returns_service_unavailable() -> anyhow::Result<()> {
    // test_state() leaves google_client_id/secret as None.
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = no_redirect_client()
        .get(format!(
            "http://{addr}/auth/google?origin=http://localhost:5173"
        ))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    Ok(())
}

// --- Cross-provider same-email resolution (no 500) ---
//
// GitHub upserts by `github_id` while Google upserts by `email`. A GitHub
// login whose email already exists on a Google-created account (github_id
// NULL) used to INSERT and hit the email UNIQUE constraint -> 500. It now
// links the existing account in place.

// (k) a Google-created account (github_id NULL) is linked when the same email
// later signs in via GitHub — no 500, the row is reused, github_id is set.
#[tokio::test]
async fn github_login_links_existing_email_account_without_500() -> anyhow::Result<()> {
    let email = format!("link-{}@example.com", db::new_id());
    let github_id = unique_github_id();
    let mock = MockServer::start().await;
    mount_github_user_mocks(&mock, github_id, "alice", verified_primary_email(&email)).await;
    let (state, addr) = oauth_state(&mock).await;

    // Pre-existing account keyed by email with no GitHub link yet (as a Google
    // login would leave it).
    let existing_id = db::new_id();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, NULL, $2, $3, $4)",
    )
    .bind(&existing_id)
    .bind("Alice Google")
    .bind(&email)
    .bind(db::now_ms())
    .execute(&state.pool)
    .await?;

    // No 500: login_flow asserts a 200 HTML callback and returns a session token.
    let token = login_flow(addr, "http://localhost:5173").await;

    // Linked in place: same id, github_id now set, still exactly one row.
    let (id, gh): (String, Option<i64>) =
        sqlx::query_as("SELECT id, github_id FROM rtdb_auth.users WHERE email = $1")
            .bind(&email)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(id, existing_id);
    assert_eq!(gh, Some(github_id));
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE email = $1")
        .bind(&email)
        .fetch_one(&state.pool)
        .await?;
    assert_eq!(count, 1);

    // The session resolves and now carries the linked GitHub identity.
    let me = reqwest::Client::new()
        .get(format!("http://{addr}/auth/me"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await?;
    assert_eq!(me.status(), reqwest::StatusCode::OK);
    let body: Value = me.json().await?;
    assert_eq!(body["user"]["githubId"], json!(github_id));
    assert_eq!(body["user"]["email"], json!(email));
    Ok(())
}

// (l) single-provider GitHub flow: a returning GitHub user reuses their account
// and refreshes a changed email — no second row, no fork.
#[tokio::test]
async fn github_returning_user_reuses_account_and_refreshes_email() -> anyhow::Result<()> {
    let github_id = unique_github_id();
    let old_email = format!("ret-old-{}@example.com", db::new_id());
    let new_email = format!("ret-new-{}@example.com", db::new_id());
    let mock = MockServer::start().await;
    mount_github_user_mocks(&mock, github_id, "bob", verified_primary_email(&new_email)).await;
    let (state, addr) = oauth_state(&mock).await;

    let existing_id = db::new_id();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&existing_id)
    .bind(github_id)
    .bind("bob-old")
    .bind(&old_email)
    .bind(db::now_ms())
    .execute(&state.pool)
    .await?;

    let _token = login_flow(addr, "http://localhost:5173").await;

    let (id, gh, em): (String, Option<i64>, String) =
        sqlx::query_as("SELECT id, github_id, email FROM rtdb_auth.users WHERE github_id = $1")
            .bind(github_id)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(id, existing_id);
    assert_eq!(gh, Some(github_id));
    assert_eq!(em, new_email);
    // The old email is gone (updated in place, not duplicated).
    let (count_old,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE email = $1")
            .bind(&old_email)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(count_old, 0);
    Ok(())
}

// (m) single-provider GitHub flow: a brand-new user inserts exactly one row.
#[tokio::test]
async fn github_new_user_inserts_one_row() -> anyhow::Result<()> {
    let github_id = unique_github_id();
    let email = format!("new-{}@example.com", db::new_id());
    let mock = MockServer::start().await;
    mount_github_user_mocks(&mock, github_id, "carol", verified_primary_email(&email)).await;
    let (state, addr) = oauth_state(&mock).await;

    let _token = login_flow(addr, "http://localhost:5173").await;

    let (count,): (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM rtdb_auth.users WHERE github_id = $1")
            .bind(github_id)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(count, 1);
    Ok(())
}

// (n) the email is already linked to a *different* GitHub account -> a
// deliberate 409 conflict, never a 500, and the existing account is untouched.
#[tokio::test]
async fn github_login_email_linked_elsewhere_returns_conflict_not_500() -> anyhow::Result<()> {
    let email = format!("conflict-{}@example.com", db::new_id());
    let other_github_id = unique_github_id();
    let login_github_id = unique_github_id();
    let mock = MockServer::start().await;
    mount_github_user_mocks(
        &mock,
        login_github_id,
        "dave",
        verified_primary_email(&email),
    )
    .await;
    let (state, addr) = oauth_state(&mock).await;

    // Email already claimed by a different GitHub account.
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(db::new_id())
    .bind(other_github_id)
    .bind("other")
    .bind(&email)
    .bind(db::now_ms())
    .execute(&state.pool)
    .await?;

    // Drive the callback directly — login_flow asserts 200, but we expect a 409.
    let client = no_redirect_client();
    let state_token = start_login(&client, addr, "http://localhost:5173").await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("PRECONDITION_FAILED"));

    // The pre-existing account is untouched.
    let (gh,): (Option<i64>,) =
        sqlx::query_as("SELECT github_id FROM rtdb_auth.users WHERE email = $1")
            .bind(&email)
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(gh, Some(other_github_id));
    Ok(())
}
