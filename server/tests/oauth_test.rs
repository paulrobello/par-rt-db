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
            "id": 42,
            "login": "paul",
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

// (b) /auth/me with the session token -> email correct.
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
