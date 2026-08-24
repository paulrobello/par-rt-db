use std::net::SocketAddr;
use std::time::Duration;

use crate::common::{
    admin_post, fresh_db, mint_user_session, spawn_app, test_state, test_state_with_rate_limits,
};
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
    // ARC-013: a client that never mentions `protocolVersion` gets an
    // `authOk` with no `protocolVersion` key — byte-identical to a
    // pre-ARC-013 server for a pre-ARC-013 client.
    assert!(msg.get("protocolVersion").is_none());
    Ok(())
}

// ARC-013: a client that sends `protocolVersion` on `Auth` gets the server's
// `PROTOCOL_VERSION` echoed back on `authOk`.
#[tokio::test]
async fn auth_with_protocol_version_echoes_server_version() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    send_json(
        &mut ws,
        json!({"type": "auth", "token": token, "db": db, "protocolVersion": 1}),
    )
    .await;
    let msg = recv_json(&mut ws).await;

    assert_eq!(msg["type"], json!("authOk"));
    assert_eq!(msg["protocolVersion"], json!(1));
    Ok(())
}

// ARC-013: a client requesting a protocol version newer than the server's
// gets `authErr { error: { code: "UNSUPPORTED_PROTOCOL" } }` and the
// connection closes with the auth-failure close code.
#[tokio::test]
async fn auth_with_unsupported_protocol_version_returns_auth_err_and_closes() -> anyhow::Result<()>
{
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    send_json(
        &mut ws,
        json!({"type": "auth", "token": token, "db": db, "protocolVersion": 999}),
    )
    .await;
    let msg = recv_json(&mut ws).await;

    assert_eq!(msg["type"], json!("authErr"));
    assert_eq!(msg["error"]["code"], json!("UNSUPPORTED_PROTOCOL"));
    // Not a credential failure — closes with the generic protocol-violation
    // code (4400), not the auth-failure code (4401).
    expect_close_with_code(&mut ws, 4400).await;
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

// ENH-005 Task 4 (h): a machine token scoped to `["projects"]` cannot subscribe
// to `workItems` — the table-scope gate fires inside `execute_query` (the
// subscribe path's initial run in `handle_subscribe`) AND again inside
// `subs::register` as defense-in-depth. Either way the client sees a
// `subscribeErr` carrying `FORBIDDEN`. Minted directly via the token helper
// because the admin HTTP route doesn't yet accept `tables`.
#[tokio::test]
async fn subscribe_on_forbidden_table_returns_subscribe_err_for_scoped_token() -> anyhow::Result<()>
{
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let (_, scoped) = rtdb_server::auth::tokens::mint_token(
        &state.pool,
        &db,
        "scoped",
        None,
        false,
        Some(&["projects".to_string()]),
    )
    .await
    .expect("mint scoped token");

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &scoped, &db).await;

    // Subscribe to the forbidden table → subscribeErr / FORBIDDEN.
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("subscribeErr"));
    assert_eq!(msg["queryId"], json!("q1"));
    assert_eq!(msg["error"]["code"], json!("FORBIDDEN"));

    // Subscribe to the allowed table → initial queryUpdate (empty Docs).
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q2", "query": {"table": "projects"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("queryUpdate"));
    assert_eq!(msg["queryId"], json!("q2"));
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

// F2: schedule a one-shot over WS -> scheduleOk with an id; cancel it over the
// same connection -> scheduleAck ok:true. `afterMs` is set well into the
// future so the scheduler loop cannot claim+fire the job before our cancel
// arrives (a 0ms job races the scheduler and flakes).
#[tokio::test]
async fn schedule_one_shot_over_ws() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({
            "type": "schedule", "scheduleId": "s1",
            "when": {"type": "afterMs", "ms": 60_000},
            "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 5}}]}
        }),
    )
    .await;
    let reply = recv_json(&mut ws).await;
    assert_eq!(reply["type"], json!("scheduleOk"));
    assert_eq!(reply["scheduleId"], json!("s1"));
    let id = reply["id"].as_str().expect("id").to_string();

    send_json(
        &mut ws,
        json!({"type": "cancelSchedule", "scheduleId": "s2", "id": id}),
    )
    .await;
    let ack = recv_json(&mut ws).await;
    assert_eq!(ack["type"], json!("scheduleAck"));
    assert_eq!(ack["scheduleId"], json!("s2"));
    assert_eq!(ack["ok"], json!(true));
    Ok(())
}

// F3: a garbage cron expr is rejected at resolve_when time -> scheduleErr.
#[tokio::test]
async fn schedule_rejects_bad_cron() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({
            "type": "schedule", "scheduleId": "s1",
            "when": {"type": "cron", "expr": "garbage"},
            "txn": {"steps": []}
        }),
    )
    .await;
    let reply = recv_json(&mut ws).await;
    assert_eq!(reply["type"], json!("scheduleErr"));
    assert_eq!(reply["scheduleId"], json!("s1"));
    Ok(())
}

// F4: listSchedules -> listSchedulesOk echoing the caller's scheduleId.
#[tokio::test]
async fn list_schedules_over_ws() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    send_json(
        &mut ws,
        json!({"type": "listSchedules", "scheduleId": "l1"}),
    )
    .await;
    let reply = recv_json(&mut ws).await;
    assert_eq!(reply["type"], json!("listSchedulesOk"));
    assert_eq!(reply["scheduleId"], json!("l1"));
    assert!(reply["schedules"].is_array());
    Ok(())
}

// F5: pause and resume round-trip over WS. Schedules a far-future one-shot,
// pauses it (pending -> paused, ack ok:true), then resumes it (paused ->
// pending, ack ok:true), then cancels to clean up. The large afterMs keeps the
// scheduler from claiming/firing the job mid-test, mirroring F2's anti-flake
// guard. Covers the reactive WS path directly — pause/resume were previously
// exercised only at the scheduler (set_paused) and client layers.
#[tokio::test]
async fn pause_and_resume_over_ws() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    // Schedule a one-shot far enough out that the scheduler cannot fire it first.
    send_json(
        &mut ws,
        json!({
            "type": "schedule", "scheduleId": "s1",
            "when": {"type": "afterMs", "ms": 3_600_000},
            "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 1}}]}
        }),
    )
    .await;
    let reply = recv_json(&mut ws).await;
    assert_eq!(reply["type"], json!("scheduleOk"));
    assert_eq!(reply["scheduleId"], json!("s1"));
    let id = reply["id"].as_str().expect("id").to_string();

    // Pause: pending -> paused.
    send_json(
        &mut ws,
        json!({"type": "pauseSchedule", "scheduleId": "p1", "id": id}),
    )
    .await;
    let ack = recv_json(&mut ws).await;
    assert_eq!(ack["type"], json!("scheduleAck"));
    assert_eq!(ack["scheduleId"], json!("p1"));
    assert_eq!(ack["ok"], json!(true));

    // Resume: paused -> pending.
    send_json(
        &mut ws,
        json!({"type": "resumeSchedule", "scheduleId": "r1", "id": id}),
    )
    .await;
    let ack = recv_json(&mut ws).await;
    assert_eq!(ack["type"], json!("scheduleAck"));
    assert_eq!(ack["scheduleId"], json!("r1"));
    assert_eq!(ack["ok"], json!(true));

    // Clean up so the far-future job does not linger.
    send_json(
        &mut ws,
        json!({"type": "cancelSchedule", "scheduleId": "c1", "id": id}),
    )
    .await;
    let ack = recv_json(&mut ws).await;
    assert_eq!(ack["ok"], json!(true));
    Ok(())
}

// --- Phase 5: /sync admin bypass -----------------------------------------

// An admin OAuth session is admitted to a database it is NOT allowlisted for
// (is_admin bypasses authorize at the handshake); a non-admin is rejected.
#[tokio::test]
async fn admin_ws_bypasses_authorize_for_any_db() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let suffix = uuid::Uuid::now_v7().simple();
    let admin_email = format!("wsadmin-{suffix}@example.com");
    let admin_tok = mint_user_session(&pool, &format!("u-wsadmin-{suffix}"), &admin_email).await;
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&admin_email)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;

    // Admin: admitted even though not allowlisted for `db`.
    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, &admin_tok, &db).await;
    assert_eq!(msg["type"], json!("authOk"));

    // Non-admin (no admins row, not allowlisted): rejected + closed 4401.
    let stranger_tok = mint_user_session(
        &pool,
        &format!("u-stranger-{suffix}"),
        &format!("stranger-{suffix}@example.com"),
    )
    .await;
    let mut ws2 = ws_connect(addr).await;
    let msg = auth(&mut ws2, &stranger_tok, &db).await;
    assert_eq!(msg["type"], json!("authErr"));
    expect_close_with_code(&mut ws2, 4401).await;
    Ok(())
}

// An admin can Subscribe over /sync to a database they're not allowlisted for
// (the per-op authorize is bypassed): the subscription returns its initial
// queryUpdate, not a subscribeErr.
#[tokio::test]
async fn admin_ws_subscribe_bypasses_authorize() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let suffix = uuid::Uuid::now_v7().simple();
    let admin_email = format!("wssub-{suffix}@example.com");
    let admin_tok = mint_user_session(&pool, &format!("u-wssub-{suffix}"), &admin_email).await;
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&admin_email)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;

    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, &admin_tok, &db).await;
    assert_eq!(msg["type"], json!("authOk"));

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("queryUpdate"));
    assert_eq!(msg["queryId"], json!("q1"));
    Ok(())
}

// Per-token/per-db rate limiting (shared with HTTP via rate_limit::evaluate):
// with per_token_rpm = 3, the first 3 mutates succeed and the 4th in the same
// minute returns a mutateErr RATE_LIMITED with a positive retryAfter — and the
// connection stays open (a subsequent ping still pongs). Mirrors the HTTP
// assertions in rate_limit_test over the WS transport.
#[tokio::test]
async fn ws_mutate_rate_limited_keeps_connection_open() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(3, 0).await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    let txn = insert_work_item_txn();
    for i in 1..=3 {
        send_json(
            &mut ws,
            json!({"type": "mutate", "mutId": format!("ok-{i}"), "txn": txn.clone()}),
        )
        .await;
        let msg = recv_json(&mut ws).await;
        assert_eq!(
            msg["type"],
            json!("mutateOk"),
            "mutate {i} under the per-token limit should succeed: {msg}"
        );
    }

    // 4th in the same minute → mutateErr RATE_LIMITED with a retryAfter hint.
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "limited", "txn": txn}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("mutateErr"));
    assert_eq!(msg["mutId"], json!("limited"));
    assert_eq!(msg["error"]["code"], json!("RATE_LIMITED"));
    let retry_after = msg["error"]["retryAfter"]
        .as_u64()
        .expect("retryAfter present");
    assert!(
        (1..=60).contains(&retry_after),
        "retryAfter within one fixed-window minute: got {retry_after}"
    );

    // Connection stays open: a ping on the same socket still pongs.
    send_json(&mut ws, json!({"type": "ping"})).await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("pong"));
    Ok(())
}

// Same gate on Subscribe: 3 subscribes return their initial queryUpdate, the
// 4th returns subscribeErr RATE_LIMITED, and the connection stays open.
#[tokio::test]
async fn ws_subscribe_rate_limited_keeps_connection_open() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(3, 0).await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    for i in 1..=3 {
        send_json(
            &mut ws,
            json!({"type": "subscribe", "queryId": format!("q{i}"), "query": {"table": "workItems"}}),
        )
        .await;
        let msg = recv_json(&mut ws).await;
        assert_eq!(
            msg["type"],
            json!("queryUpdate"),
            "subscribe {i} should return its initial queryUpdate: {msg}"
        );
    }

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "qlim", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("subscribeErr"));
    assert_eq!(msg["queryId"], json!("qlim"));
    assert_eq!(msg["error"]["code"], json!("RATE_LIMITED"));
    let retry_after = msg["error"]["retryAfter"]
        .as_u64()
        .expect("retryAfter present");
    assert!(
        (1..=60).contains(&retry_after),
        "retryAfter within one fixed-window minute: got {retry_after}"
    );

    send_json(&mut ws, json!({"type": "ping"})).await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("pong"));
    Ok(())
}

// (ro1) A read-only machine token connecting over WS is authed (authOk), but
// its Mutate frame is rejected with mutateErr FORBIDDEN. The WS Mutate
// read-only gate landed in ENH-005 Task 3 but had no WS test coverage — this
// establishes the read-only-over-WS pattern (mint directly with read_only=true
// via auth::tokens::mint_token, since the /admin/mint-token helper can't set
// the flag, then connect with the shared `auth` helper).
#[tokio::test]
async fn read_only_token_ws_mutate_is_forbidden() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let (_id, ro_token) =
        rtdb_server::auth::tokens::mint_token(&state.pool, &db, "ro", None, true, None)
            .await
            .expect("mint read-only token");

    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, &ro_token, &db).await;
    assert_eq!(msg["type"], json!("authOk"));

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("mutateErr"));
    assert_eq!(msg["mutId"], json!("m1"));
    assert_eq!(msg["error"]["code"], json!("FORBIDDEN"));

    Ok(())
}

// (ro2) A read-only machine token cannot manage scheduled jobs over WS: each
// of cancelSchedule/pauseSchedule/resumeSchedule returns a scheduleAck with
// ok:false and error.code FORBIDDEN. The job is created on a separate
// full-access connection, then the read-only token attempts to tamper with it
// — covering the new WS gate in run_simple_schedule (ENH-005 follow-up: the
// schedule-create arm was gated, but the manage arms were not).
#[tokio::test]
async fn read_only_token_ws_cannot_manage_schedule() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let full_token = mint_token(addr, &db).await;

    // Connection 1 (full-access): schedule a far-future one-shot so the
    // scheduler loop cannot claim+fire it before the manage frames land.
    let mut ws_full = ws_connect(addr).await;
    auth(&mut ws_full, &full_token, &db).await;
    send_json(
        &mut ws_full,
        json!({
            "type": "schedule", "scheduleId": "s1",
            "when": {"type": "afterMs", "ms": 3_600_000},
            "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 1}}]}
        }),
    )
    .await;
    let reply = recv_json(&mut ws_full).await;
    assert_eq!(reply["type"], json!("scheduleOk"));
    let id = reply["id"].as_str().expect("id").to_string();

    // Connection 2 (read-only): each manage arm is forbidden.
    let (_id, ro_token) =
        rtdb_server::auth::tokens::mint_token(&state.pool, &db, "ro", None, true, None)
            .await
            .expect("mint read-only token");
    let mut ws = ws_connect(addr).await;
    let msg = auth(&mut ws, &ro_token, &db).await;
    assert_eq!(msg["type"], json!("authOk"));

    for (frame_ty, sched_id) in [
        ("pauseSchedule", "p1"),
        ("resumeSchedule", "r1"),
        ("cancelSchedule", "c1"),
    ] {
        send_json(
            &mut ws,
            json!({"type": frame_ty, "scheduleId": sched_id, "id": id}),
        )
        .await;
        let ack = recv_json(&mut ws).await;
        assert_eq!(ack["type"], json!("scheduleAck"), "for {frame_ty}");
        assert_eq!(ack["scheduleId"], json!(sched_id), "for {frame_ty}");
        assert_eq!(ack["ok"], json!(false), "for {frame_ty}");
        assert_eq!(ack["error"]["code"], json!("FORBIDDEN"), "for {frame_ty}");
    }

    Ok(())
}

// Live session revocation: a session deleted mid-connection is rejected on the
// NEXT mutate over the SAME open socket (UNAUTHORIZED, not a close), and the
// connection stays usable (a following ping still pongs). Revoke is done by the
// same row DELETE the admin endpoint performs — proving the per-op check.
#[tokio::test]
async fn revoked_session_is_rejected_on_next_ws_op_over_open_connection() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let suffix = uuid::Uuid::now_v7().simple();
    let user_id = format!("u-rev-{suffix}");
    let email = format!("rev-{suffix}@example.com");

    let token = mint_user_session(&state.pool, &user_id, &email).await;
    let add = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db.as_str(), "action": "add", "email": email}),
    )
    .await;
    assert_eq!(add.status(), reqwest::StatusCode::OK);

    let mut ws = ws_connect(addr).await;
    let auth_msg = auth(&mut ws, &token, db.as_str()).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // Subscribe so the mutate produces both mutateOk and a queryUpdate (the
    // loop below drains both). Mirrors the (e) test pattern.
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws).await; // initial queryUpdate

    // mutate succeeds while the session is live
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_ok = false;
    for _ in 0..2 {
        let m = recv_json(&mut ws).await;
        if m["type"] == json!("mutateOk") {
            assert_eq!(m["mutId"], json!("m1"));
            saw_ok = true;
        }
    }
    assert!(saw_ok, "expected mutateOk before revocation");

    // revoke the session directly (exactly what DELETE /admin/sessions/{hash} does)
    let hash = rtdb_server::db::sha256_hex(&token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(&state.pool)
        .await?;

    // the NEXT mutate on the SAME open connection is now rejected
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let err_msg = recv_json(&mut ws).await;
    assert_eq!(err_msg["type"], json!("mutateErr"));
    assert_eq!(err_msg["mutId"], json!("m2"));
    assert_eq!(err_msg["error"]["code"], json!("UNAUTHORIZED"));

    // connection stays open (revocation errors the op, does not close)
    send_json(&mut ws, json!({"type": "ping"})).await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("pong"));

    Ok(())
}

// An admin OAuth session bypasses per-db `authorize` (the WS Subscribe/Mutate
// arms take the `if admin` branch), so the per-op session-liveness check must
// run on the admin branch too: revoking an admin's session rejects the next
// mutate over the SAME open connection (UNAUTHORIZED, not a close).
#[tokio::test]
async fn revoked_admin_session_is_rejected_on_next_ws_op() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    // uuid suffix: `mint_user_session` writes the GLOBAL rtdb_auth.users row
    // (PK on id); tests don't clean those rows, so a literal id collides on the
    // second run.
    let suffix = uuid::Uuid::now_v7().simple();
    let user_id = format!("u-adm-{suffix}");
    let email = format!("adm-{suffix}@example.com");

    let token = mint_user_session(&state.pool, &user_id, &email).await;
    // make the user a dashboard admin (server-wide) so `is_admin` returns true
    // and the WS admin branch runs.
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&email)
        .bind(rtdb_server::db::now_ms())
        .execute(&state.pool)
        .await?;

    let mut ws = ws_connect(addr).await;
    let auth_msg = auth(&mut ws, &token, db.as_str()).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // Subscribe so the mutate produces both mutateOk and a queryUpdate (the
    // loop below drains both). Without this the second recv_json would block
    // until the server's liveness timer fires a Ping the helper panics on.
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    recv_json(&mut ws).await; // initial queryUpdate

    // admin mutate succeeds (bypasses authorize) while the session is live
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_ok = false;
    for _ in 0..2 {
        let m = recv_json(&mut ws).await;
        if m["type"] == json!("mutateOk") {
            assert_eq!(m["mutId"], json!("m1"));
            saw_ok = true;
        }
    }
    assert!(saw_ok, "expected mutateOk before revocation");

    // revoke the session directly (exactly what DELETE /admin/sessions/{hash}
    // does — Task 3 wires that endpoint).
    let hash = rtdb_server::db::sha256_hex(&token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(&state.pool)
        .await?;

    // the NEXT mutate on the SAME open connection is now rejected — proves the
    // admin branch runs session_still_valid per op.
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let err_msg = recv_json(&mut ws).await;
    assert_eq!(err_msg["type"], json!("mutateErr"));
    assert_eq!(err_msg["mutId"], json!("m2"));
    assert_eq!(err_msg["error"]["code"], json!("UNAUTHORIZED"));

    // connection stays open (revocation errors the op, does not close)
    send_json(&mut ws, json!({"type": "ping"})).await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("pong"));

    Ok(())
}

// SEC-105: the /sync WS upgrade must reject an Origin that is neither on the
// hot-reloaded allowlist nor equal to public_url, since the cookie
// authenticates a browser-opened handshake and CORS does not apply to WS.
// `connect_async` surfaces a non-101 response (403 here) as a connect error.
#[tokio::test]
async fn sync_upgrade_rejects_disallowed_origin() -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state().await;
    let addr = spawn_app(state).await;

    let mut req = format!("ws://{addr}/sync").into_client_request()?;
    req.headers_mut().insert(
        "origin",
        reqwest::header::HeaderValue::from_static("https://evil.example"),
    );
    let result = connect_async(req).await;
    assert!(
        result.is_err(),
        "a disallowed Origin must be rejected before the WS upgrade"
    );
    Ok(())
}

// SEC-105: an allowed Origin upgrades normally. The test hot allowlist seeds
// `http://localhost:5173`. The upgrade succeeds even though no Auth frame has
// been sent — `/sync` authenticates post-upgrade, so the handshake itself only
// gates on Origin.
#[tokio::test]
async fn sync_upgrade_allows_allowlisted_origin() -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state().await;
    let addr = spawn_app(state).await;

    let mut req = format!("ws://{addr}/sync").into_client_request()?;
    req.headers_mut().insert(
        "origin",
        reqwest::header::HeaderValue::from_static("http://localhost:5173"),
    );
    let (_ws, resp) = connect_async(req).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SWITCHING_PROTOCOLS,
        "an allowlisted Origin must upgrade to 101"
    );
    Ok(())
}

// SEC-105: no Origin header = non-browser client (CLI/SDK/machine token); the
// existing auth gates (post-upgrade Auth frame) validate the credential
// regardless, so the upgrade itself admits the connection.
#[tokio::test]
async fn sync_upgrade_admits_absent_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    // connect_async does not set Origin by default.
    let (_ws, resp) = connect_async(format!("ws://{addr}/sync")).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SWITCHING_PROTOCOLS,
        "an absent Origin (non-browser client) must upgrade to 101"
    );
    Ok(())
}

// SEC-006: /admin/stream authenticates once at the handshake, so without a
// periodic re-check a revoked session keeps reading the op feed until the
// client disconnects. The gauge tick re-validates the credential and closes
// the socket with 4401.
#[tokio::test]
async fn admin_stream_closes_when_session_is_revoked() -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state().await;
    let addr = spawn_app(state).await;

    // Mint an admin-key login session (SEC-120) and use its opaque token as the
    // stream's subprotocol bearer — the dashboard's path.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()?;
    let resp = client
        .post(format!("http://{addr}/admin/login"))
        .json(&json!({"adminKey": "test-admin-key"}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
    let session = resp
        .headers()
        .get_all(reqwest::header::SET_COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .find_map(|s| s.strip_prefix("rtdb_session="))
        .map(|s| s.split(';').next().unwrap_or_default().to_string())
        .expect("login set rtdb_session cookie");

    let mut req = format!("ws://{addr}/admin/stream").into_client_request()?;
    req.headers_mut().insert(
        "sec-websocket-protocol",
        reqwest::header::HeaderValue::from_str(&format!("rtdb-admin.{session}"))?,
    );
    let (mut ws, _) = connect_async(req).await.expect("admin stream upgrades");

    // Revoke exactly this session's row (sibling tests in this binary hold
    // their own admin-key sessions against the shared rtdb_auth).
    let hash = rtdb_server::db::sha256_hex(&session);
    let resp = crate::common::admin_delete(addr, &format!("/admin/sessions/{hash}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // The next gauge tick (~1s) must tear the socket down.
    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = ws.next().await {
            match frame {
                Ok(Message::Close(_)) | Err(_) => return true,
                Ok(_) => continue,
            }
        }
        true
    })
    .await;
    assert!(
        closed.unwrap_or(false),
        "SEC-006: a revoked admin session must not keep an open /admin/stream"
    );
    Ok(())
}

// SEC-105: the /admin/stream WS upgrade applies the same Origin check. A
// disallowed Origin is rejected before WS negotiation begins, regardless of
// whether the bearer is offered via the subprotocol.
#[tokio::test]
async fn admin_stream_rejects_disallowed_origin() -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = test_state().await;
    let addr = spawn_app(state).await;

    let mut req = format!("ws://{addr}/admin/stream").into_client_request()?;
    req.headers_mut().insert(
        "origin",
        reqwest::header::HeaderValue::from_static("https://evil.example"),
    );
    req.headers_mut().insert(
        "sec-websocket-protocol",
        reqwest::header::HeaderValue::from_static("rtdb-admin.test-admin-key"),
    );
    let result = connect_async(req).await;
    assert!(
        result.is_err(),
        "a disallowed Origin must be rejected before the /admin/stream upgrade, \
         even with a valid subprotocol bearer"
    );
    Ok(())
}
