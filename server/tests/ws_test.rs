mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{admin_post, fresh_db, spawn_app, test_state};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_connect(addr: SocketAddr) -> WsStream {
    let (ws, _) = connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
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

async fn auth(ws: &mut WsStream, token: &str, db: &str) -> Value {
    send_json(ws, json!({"type": "auth", "token": token, "db": db})).await;
    recv_json(ws).await
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

/// Expects the connection to close with an explicit close frame carrying
/// `expected_code` (A4: auth failures close with `4401`, distinct from
/// `4400` for a generic protocol violation). A missing close frame (`None`
/// or `Close(None)`) fails the assertion — a future regression that drops
/// the frame must not pass silently.
async fn expect_close_with_code(ws: &mut WsStream, expected_code: u16) {
    match ws.next().await {
        Some(Ok(Message::Close(Some(frame)))) => {
            assert_eq!(u16::from(frame.code), expected_code);
        }
        other => panic!("expected a close frame with code {expected_code}, got {other:?}"),
    }
}

// (a) auth -> authOk with user.kind == "machine".
#[tokio::test]
async fn auth_with_valid_machine_token_returns_auth_ok() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, &token, &db).await;

    assert_eq!(msg["type"], json!("authOk"));
    assert_eq!(msg["user"]["kind"], json!("machine"));
    Ok(())
}

// (a2) C1: a pre-auth protocol-level Ping is tolerated (doesn't consume the
// "first message must be auth" slot) and the following auth frame still succeeds. The
// transport layer auto-replies with a Pong to our Ping before any app data; drain that
// first (tungstenite's `_write`/`read_message_frame` queues a Pong reply on any Ping read,
// independent of the app-level handling this test exercises).
#[tokio::test]
async fn protocol_ping_before_auth_frame_is_tolerated() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    ws.send(Message::Ping(Vec::new().into()))
        .await
        .expect("send ping frame");

    match ws.next().await.expect("stream ended").expect("frame ok") {
        Message::Pong(_) => {}
        other => panic!("expected transport-layer auto pong reply, got {other:?}"),
    }

    let msg = auth(&mut ws, &token, &db).await;
    assert_eq!(msg["type"], json!("authOk"));
    Ok(())
}

// (b) bad token -> authErr then stream closes.
#[tokio::test]
async fn auth_with_bad_token_returns_auth_err_and_closes() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, "bogus-token", &db).await;
    assert_eq!(msg["type"], json!("authErr"));

    expect_close_with_code(&mut ws, 4401).await;
    Ok(())
}

// (c) first message not auth -> closed.
#[tokio::test]
async fn first_message_not_auth_closes_connection() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;

    let mut ws = ws_connect(addr).await;
    send_json(&mut ws, json!({"type": "ping"})).await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("authErr"));

    expect_close_with_code(&mut ws, 4401).await;
    Ok(())
}

// (d) subscribe -> initial queryUpdate.
#[tokio::test]
async fn subscribe_returns_initial_query_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("queryUpdate"));
    assert_eq!(msg["queryId"], json!("q1"));
    assert_eq!(msg["result"].as_array().expect("docs array").len(), 0);
    Ok(())
}

// (e) mutate over the SAME connection -> mutateOk AND a fresh queryUpdate.
#[tokio::test]
async fn mutate_on_same_connection_returns_mutate_ok_and_query_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws).await; // initial queryUpdate

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;

    let mut saw_mutate_ok = false;
    let mut saw_query_update = false;
    for _ in 0..2 {
        let msg = recv_json(&mut ws).await;
        match msg["type"].as_str() {
            Some("mutateOk") => {
                assert_eq!(msg["mutId"], json!("m1"));
                saw_mutate_ok = true;
            }
            Some("queryUpdate") => {
                assert_eq!(msg["queryId"], json!("q1"));
                saw_query_update = true;
            }
            other => panic!("unexpected message type {other:?}"),
        }
    }
    assert!(saw_mutate_ok && saw_query_update);
    Ok(())
}

// (new) two mutates with the same idempotencyKey dedupe: the second replays
// the first's cached results and produces no queryUpdate (fan-out is
// skipped on a cache hit). mutId stays distinct per call (m1, m2) — proving
// it is independent of the dedup key, not the same value.
#[tokio::test]
async fn mutate_with_same_idempotency_key_dedupes_and_skips_fan_out() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws).await; // initial queryUpdate

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "idempotencyKey": "retry-key", "txn": insert_work_item_txn()}),
    )
    .await;

    let mut first_results = None;
    for _ in 0..2 {
        let msg = recv_json(&mut ws).await;
        match msg["type"].as_str() {
            Some("mutateOk") => {
                assert_eq!(msg["mutId"], json!("m1"));
                first_results = Some(msg["results"].clone());
            }
            Some("queryUpdate") => {
                assert_eq!(msg["queryId"], json!("q1"));
            }
            other => panic!("unexpected message type {other:?}"),
        }
    }
    let first_results = first_results.expect("first mutateOk");

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "idempotencyKey": "retry-key", "txn": insert_work_item_txn()}),
    )
    .await;
    let second = recv_json(&mut ws).await;
    assert_eq!(second["type"], json!("mutateOk"));
    assert_eq!(second["mutId"], json!("m2"));
    assert_eq!(second["results"], first_results);

    let drained = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
    assert!(
        drained.is_err(),
        "expected no further message after deduped mutate, got {drained:?}"
    );
    Ok(())
}

// (f) mutate from a SECOND authed connection -> first connection receives queryUpdate.
#[tokio::test]
async fn mutate_from_second_connection_pushes_update_to_first() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws1 = ws_connect(addr).await;
    auth(&mut ws1, &token, &db).await;
    send_json(
        &mut ws1,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws1).await; // initial queryUpdate

    let mut ws2 = ws_connect(addr).await;
    auth(&mut ws2, &token, &db).await;
    send_json(
        &mut ws2,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mutate_ok = recv_json(&mut ws2).await;
    assert_eq!(mutate_ok["type"], json!("mutateOk"));

    let update = recv_json(&mut ws1).await;
    assert_eq!(update["type"], json!("queryUpdate"));
    assert_eq!(update["queryId"], json!("q1"));
    Ok(())
}

// (g) subscribe to unknown index -> subscribeErr with BAD_REQUEST.
#[tokio::test]
async fn subscribe_to_unknown_index_returns_subscribe_err() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems", "index": "no_such_index"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("subscribeErr"));
    assert_eq!(msg["queryId"], json!("q1"));
    assert_eq!(msg["error"]["code"], json!("BAD_REQUEST"));
    Ok(())
}

// (h) unsubscribe then mutate -> no further queryUpdate (drain with timeout).
#[tokio::test]
async fn unsubscribe_then_mutate_sends_no_further_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws).await; // initial queryUpdate

    send_json(&mut ws, json!({"type": "unsubscribe", "queryId": "q1"})).await;
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;

    let mutate_ok = recv_json(&mut ws).await;
    assert_eq!(mutate_ok["type"], json!("mutateOk"));

    let drained = tokio::time::timeout(Duration::from_millis(300), ws.next()).await;
    assert!(
        drained.is_err(),
        "expected no further message, got {drained:?}"
    );
    Ok(())
}

// (i) ping -> pong.
#[tokio::test]
async fn ping_returns_pong() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(&mut ws, json!({"type": "ping"})).await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("pong"));
    Ok(())
}

// F1: revoking a machine token mid-session denies its next mutate on the SAME
// open connection (MutateErr UNAUTHORIZED) without closing it — the
// connection stays usable afterward (verified via a subsequent ping/pong).
#[tokio::test]
async fn revoked_machine_token_denies_mutate_but_keeps_connection_usable() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("parse mint-token response");
    let token_id = body["tokenId"].as_str().expect("tokenId").to_string();
    let token = body["token"].as_str().expect("token").to_string();

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("queryUpdate"));

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_mutate_ok = false;
    let mut saw_query_update = false;
    for _ in 0..2 {
        let msg = recv_json(&mut ws).await;
        match msg["type"].as_str() {
            Some("mutateOk") => saw_mutate_ok = true,
            Some("queryUpdate") => saw_query_update = true,
            other => panic!("unexpected message type {other:?}"),
        }
    }
    assert!(
        saw_mutate_ok && saw_query_update,
        "subscribe+mutate OK before revocation"
    );

    let resp = admin_post(addr, "/admin/revoke-token", json!({"tokenId": token_id})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Same open connection, revoked token: mutate is denied but the
    // connection is not closed.
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("mutateErr"));
    assert_eq!(msg["mutId"], json!("m2"));
    assert_eq!(msg["error"]["code"], json!("UNAUTHORIZED"));

    // Connection stays usable: a ping on the same socket still gets a pong.
    send_json(&mut ws, json!({"type": "ping"})).await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("pong"));
    Ok(())
}

// Rate limit: >200 messages within the rolling 10s window closes the connection.
#[tokio::test]
async fn rate_limit_exceeded_closes_connection() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    for _ in 0..201 {
        send_json(&mut ws, json!({"type": "ping"})).await;
    }

    let mut saw_auth_err = false;
    for _ in 0..201 {
        match ws.next().await {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text)?;
                if value["type"] == json!("authErr") {
                    saw_auth_err = true;
                    break;
                }
            }
            Some(Ok(Message::Close(_))) | None => break,
            Some(Ok(_)) => {}
            Some(Err(_)) => break,
        }
    }
    assert!(
        saw_auth_err,
        "expected an authErr for exceeding the rate limit"
    );
    Ok(())
}
