//! Per-database read-only freeze: `GET|PATCH /admin/db/{db}/readonly` toggles
//! a persisted flag on `rtdb_auth.databases`; the committer's Mutate arm
//! checks it fresh on every client write, so every transport (WS, HTTP one-
//! shot, mutate-batch, admin direct mutate) and every principal (machine
//! token, OAuth user, admin) is covered by one gate.
//!
//! Contract pinned here (the documented ruling):
//! - REJECTED while frozen: document mutations, new scheduled jobs, new
//!   workflow starts, workflow signal delivery — on every transport and
//!   principal, admin included.
//! - STILL WORK while frozen: reads and subscriptions, system writes
//!   (scheduled fires, workflow advances, TTL reaping), admin surfaces
//!   (schema push/migrate/restore, backups, the toggle itself).
//! - Error: `READ_ONLY` (HTTP 409). An idempotent retry of an
//!   already-committed write still replays its cached result while frozen.
//!
//! The exempted system arms are what keep the freeze from stranding work:
//! freezing them mid-run would strand in-flight workflows and stop TTL
//! expiry, and the schema-surgery use case needs the admin DDL plane open.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::common::{
    admin_get, admin_post, fresh_db, spawn_app, test_state, test_state_with_ttl_sweep,
};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const TOKEN_HEADER: &str = "Authorization";

async fn admin_patch(addr: SocketAddr, path: &str, body: serde_json::Value) -> reqwest::Response {
    reqwest::Client::new()
        .patch(format!("http://{addr}{path}"))
        .header(TOKEN_HEADER, "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .expect("send admin patch")
}

/// Mints a machine token for the db via the admin surface (the normal
/// application path) and returns it.
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "freeze-test"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

async fn ws_connect(addr: SocketAddr) -> WsStream {
    let (ws, _) = connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn send_json(ws: &mut WsStream, msg: Value) {
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .expect("send frame");
}

async fn recv_json(ws: &mut WsStream) -> Value {
    match ws.next().await.expect("stream ended").expect("frame ok") {
        Message::Text(text) => serde_json::from_str(&text).expect("parse json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

fn insert_work_item_txn() -> Value {
    json!({"steps": [{"op": "insert", "table": "workItems", "doc": {
        "projectId": "0".repeat(32),
        "title": "freeze probe",
        "status": "backlog",
        "order": 1.0,
        "completedAt": null
    }}]})
}

/// POST /api/mutate with a fresh minted-token bearer.
async fn http_mutate(addr: SocketAddr, token: &str, db: &str, txn: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db, "txn": txn}))
        .send()
        .await
        .expect("send mutate")
}

async fn freeze(addr: SocketAddr, db: &str, frozen: bool) {
    let resp = admin_patch(
        addr,
        &format!("/admin/db/{db}/readonly"),
        json!({"readOnly": frozen}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "PATCH must succeed");
}

async fn http_query(addr: SocketAddr, token: &str, db: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db, "query": {"table": "workItems"}}))
        .send()
        .await
        .expect("send query")
}

/// Criterion 1 (admin surface): the toggle round-trips, persists, and 404s
/// for an unregistered database.
#[tokio::test]
async fn readonly_admin_surface_roundtrip() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let get = |addr: SocketAddr, db: &str| {
        let db = db.to_string();
        async move { admin_get(addr, &format!("/admin/db/{db}/readonly")).await }
    };

    let resp = get(addr, &db).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["readOnly"], json!(false), "default is unfrozen");

    freeze(addr, &db, true).await;
    let body: Value = get(addr, &db).await.json().await?;
    assert_eq!(body["readOnly"], json!(true), "frozen after PATCH");

    freeze(addr, &db, false).await;
    let body: Value = get(addr, &db).await.json().await?;
    assert_eq!(body["readOnly"], json!(false), "unfrozen again");

    let resp = admin_patch(
        addr,
        &format!("/admin/db/nope_{}/readonly", "0".repeat(24)),
        json!({"readOnly": true}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}

/// Criterion 1 (data plane): while frozen, HTTP and WS document writes are
/// rejected with READ_ONLY while reads and subscriptions keep working;
/// unfreezing restores writes. Criterion 3's replay carve-out is pinned in
/// `readonly_new_starts_rejected_and_replay_wins`.
#[tokio::test]
async fn readonly_freeze_rejects_writes_but_reads_and_subs_work() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // A write before the freeze lands normally.
    let resp = http_mutate(addr, &token, &db, insert_work_item_txn()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["results"].as_array().expect("results").len(), 1);

    freeze(addr, &db, true).await;

    // HTTP mutate: 409 with the READ_ONLY code.
    let resp = http_mutate(addr, &token, &db, insert_work_item_txn()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("READ_ONLY"));

    // WS mutate: error frame carries READ_ONLY; the connection stays open.
    let mut ws = ws_connect(addr).await;
    send_json(
        &mut ws,
        json!({"type": "auth", "token": token, "db": db.as_str()}),
    )
    .await;
    let ack = recv_json(&mut ws).await;
    assert_eq!(ack["type"], json!("authOk"));
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let reply = recv_json(&mut ws).await;
    assert_eq!(reply["type"], json!("mutateErr"));
    assert_eq!(reply["error"]["code"], json!("READ_ONLY"));

    // Reads still work over both transports.
    let resp = http_query(addr, &token, &db).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["result"].as_array().expect("docs").len(), 1);

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let snap = recv_json(&mut ws).await;
    assert_eq!(snap["type"], json!("queryUpdate"));
    assert_eq!(snap["result"].as_array().expect("docs").len(), 1);

    // Unfreeze restores writes (and the WS connection was usable throughout).
    freeze(addr, &db, false).await;
    let resp = http_mutate(addr, &token, &db, insert_work_item_txn()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    Ok(())
}

/// Criterion 2 (system writes): a scheduled job created before the freeze
/// still fires — and its document lands — while the database is frozen. The
/// fire path is the exempt system arm, so freezing must not strand work
/// already committed to the system.
#[tokio::test]
async fn readonly_scheduled_fire_continues_while_frozen() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Create the future write BEFORE freezing: due in 600ms.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/schedule"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({
            "db": db.as_str(),
            "when": {"type": "afterMs", "ms": 600},
            "txn": insert_work_item_txn()
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "schedule created");

    freeze(addr, &db, true).await;

    // The scheduler's fire path is the exempt system arm: the job fires
    // while frozen and its document write lands.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(6);
    let mut landed = false;
    while tokio::time::Instant::now() < deadline {
        let body: Value = http_query(addr, &token, &db).await.json().await?;
        if !body["result"].as_array().expect("docs").is_empty() {
            landed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        landed,
        "a pre-freeze scheduled job must still fire while the database is frozen"
    );

    // And a NEW schedule under the same freeze is rejected.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/schedule"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({
            "db": db.as_str(),
            "when": {"type": "afterMs", "ms": 600},
            "txn": insert_work_item_txn()
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("READ_ONLY"));

    Ok(())
}

/// Criterion 2 (system writes): TTL reaping continues while frozen — an
/// expired document is deleted by the reaper (the exempt system arm) even
/// though direct writes are rejected.
#[tokio::test]
async fn readonly_ttl_reap_continues_while_frozen() -> anyhow::Result<()> {
    let state = test_state_with_ttl_sweep(1).await;
    let addr = spawn_app(state.clone()).await;
    // A bare database (no fixture push): the sessions schema below is the
    // db's FIRST push, so the additive-only push rule is satisfied.
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create bare database");
    let db = crate::common::wrap_test_db(name);
    let token = mint_token(addr, &db).await;

    // Push a TTL table: inserts without `expiresAt` are stamped with the
    // schema's default duration (1000ms here).
    let schema = json!({"tables": {"sessions": {
        "fields": {
            "name": {"type": "string"},
            "expiresAt": {"type": "number"}
        },
        "indexes": [{"name": "by_expiresAt", "fields": ["expiresAt"]}],
        "ttl": {"field": "expiresAt", "defaultDurationMs": 1000}
    }}});
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/push-schema"))
        .header(TOKEN_HEADER, "Bearer test-admin-key")
        .json(&json!({"db": db.as_str(), "schema": schema}))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "push-schema: {}",
        resp.text().await?
    );

    // Insert a session (pre-freeze); confirm it is visible.
    let insert = json!({"steps": [{"op": "insert", "table": "sessions", "doc": {
        "name": "s1"
    }}]});
    let resp = http_mutate(addr, &token, &db, insert).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    freeze(addr, &db, true).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut reaped = false;
    while tokio::time::Instant::now() < deadline {
        let body: Value = reqwest::Client::new()
            .post(format!("http://{addr}/api/query"))
            .header(TOKEN_HEADER, format!("Bearer {token}"))
            .json(&json!({"db": db.as_str(), "query": {"table": "sessions"}}))
            .send()
            .await?
            .json()
            .await?;
        if body["result"].as_array().expect("docs").is_empty() {
            reaped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(
        reaped,
        "an expired document must still be reaped while the database is frozen"
    );

    Ok(())
}

/// Criterion 1/3 (new starts + replay carve-out): under freeze, new scheduled
/// jobs (WS arm covered in `readonly_scheduled_fire_continues_while_frozen`
/// via HTTP; here the HTTP gate), workflow starts, and workflow signal
/// delivery are rejected; an idempotent retry of an already-committed write
/// replays its cached result instead of erroring.
#[tokio::test]
async fn readonly_new_starts_rejected_and_replay_wins() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Pre-freeze: one committed write carrying an idempotency key.
    let txn = insert_work_item_txn();
    let key = "freeze-replay-1";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db.as_str(), "txn": txn, "idempotencyKey": key}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let first: Value = resp.json().await?;

    freeze(addr, &db, true).await;

    // New starts are rejected...
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/workflows"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({
            "db": db.as_str(),
            "spec": {"name": "wf", "steps": [
                {"txn": {"steps": [
                    {"op": "insert", "table": "workItems", "doc": {
                        "projectId": "0".repeat(32), "title": "wf", "status": "backlog",
                        "order": 1.0, "completedAt": null}}]}}]}
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("READ_ONLY"));

    // ...and so is signal delivery (the gate fires before the typed 404, so
    // an unknown id still surfaces READ_ONLY, not NOT_FOUND).
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/workflows/nope/signal"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db.as_str(), "name": "go"}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("READ_ONLY"));

    // A retry of the pre-freeze committed write replays its cached result —
    // the freeze gate sits AFTER the idempotency lookup.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db.as_str(), "txn": txn, "idempotencyKey": key}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "replay must win");
    let second: Value = resp.json().await?;
    assert_eq!(first, second, "replay returns the original outcome");

    Ok(())
}
