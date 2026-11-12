//! ARC-005 background bulk delete: chunked committer turns, interleave with
//! interactive writes, status surface. Setup goes through the HTTP API on the
//! test's own fresh database; no direct datastore access.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use rtdb_server::AppState;
use rtdb_server::committer::{BulkStartResult, CommitterRequest};
use rtdb_server::db;
use rtdb_server::schema::SchemaDef;
use serde_json::{Value, json};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::common::{admin_post, spawn_app, test_state, wait_until, wrap_test_db};

async fn fresh(state: &Arc<AppState>) -> (crate::common::TestDb, String) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    (wrap_test_db(name.clone()), name)
}

/// The bulk fixture: one plain table (no softDelete, no onDelete children —
/// the chunk path mirrors a DeleteByQuery step over it).
fn schema() -> SchemaDef {
    serde_json::from_value(json!({"tables": {
        "items": {
            "fields": {"kind": {"type": "string"}, "n": {"type": "number"}},
            "indexes": [{"name": "by_kind", "fields": ["kind"]}]
        }
    }}))
    .expect("parse schema")
}

async fn push_schema(addr: std::net::SocketAddr, db_name: &str) {
    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db_name, "schema": schema()}),
    )
    .await;
    let status = push.status();
    let body = push.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "push-schema failed: {body}"
    );
}

async fn mint_token(addr: std::net::SocketAddr, db_name: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db_name, "name": "bulk-test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "mint-token failed");
    let body: Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

/// Token-authed POST to a /api route.
async fn api_post(
    addr: std::net::SocketAddr,
    path: &str,
    token: &str,
    body: Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("send api request")
}

/// Token-authed GET to a /api route.
async fn api_get(addr: std::net::SocketAddr, path: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("send api request")
}

/// Inserts `count` rows in batches of 100 (the per-txn mutation-impact cap) through the admin mutate surface.
async fn seed_rows(addr: std::net::SocketAddr, db_name: &str, count: usize) {
    let mut remaining = count;
    while remaining > 0 {
        let batch = remaining.min(100);
        let steps: Vec<Value> = (0..batch)
            .map(|i| {
                json!({"op": "insert", "table": "items",
                       "doc": {"kind": "bulk", "n": i as f64}})
            })
            .collect();
        let resp = admin_post(
            addr,
            &format!("/admin/db/{db_name}/mutate"),
            json!({"txn": {"steps": steps}}),
        )
        .await;
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        assert_eq!(
            status,
            reqwest::StatusCode::OK,
            "seed mutate failed: {body}"
        );
        remaining -= batch;
    }
}

async fn count_rows(addr: std::net::SocketAddr, db_name: &str) -> i64 {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db_name}/query"),
        json!({"query": {"table": "items", "count": true}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "count query failed");
    resp.json::<Value>().await.expect("count body")["result"]
        .as_i64()
        .expect("count result is a number")
}

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> WsStream {
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

/// 1. The acceptance core: a 1500-row bulk delete splits into chunk turns; an
/// interactive insert submitted mid-op completes in a fraction of the op's
/// wall time (it must not queue behind the whole op), and the subscriber sees
/// progressive per-chunk pushes (at least two updates beyond the initial).
#[tokio::test]
async fn bulk_delete_chunks_and_interleaves() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db_name) = fresh(&state).await;
    push_schema(addr, &db_name).await;
    let token = mint_token(addr, &db_name).await;

    seed_rows(addr, &db_name, 1500).await;
    assert_eq!(count_rows(addr, &db_name).await, 1500);

    // Subscriber on a count query over the table. Pushes accumulate in a
    // spawned reader task while the main task drives the op + the insert.
    let mut ws = ws_connect(addr).await;
    send_json(
        &mut ws,
        json!({"type": "auth", "token": token, "db": db_name}),
    )
    .await;
    let auth_ok = recv_json(&mut ws).await;
    assert_eq!(auth_ok["type"], json!("authOk"));
    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "items", "count": true}}),
    )
    .await;
    let initial = recv_json(&mut ws).await;
    assert_eq!(initial["type"], json!("queryUpdate"));

    let updates = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let updates_reader = updates.clone();
    let reader = tokio::spawn(async move {
        let mut ws = ws;
        while let Some(msg) = ws.next().await {
            let Ok(Message::Text(text)) = msg else {
                continue;
            };
            let parsed: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
            if parsed["type"] == json!("queryUpdate") {
                updates_reader.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    });

    let op_started = Instant::now();
    let resp = api_post(
        addr,
        "/api/bulk/delete",
        &token,
        json!({"db": db_name, "table": "items",
               "filter": {"op": "eq", "field": "kind", "value": "bulk"}}),
    )
    .await;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "bulk delete start failed: {body_text}"
    );
    let start_body: Value = serde_json::from_str(&body_text).expect("start body json");
    assert_eq!(start_body["state"], json!("running"));
    assert_eq!(start_body["matched"], json!(1500));
    let op_id = start_body["opId"].as_str().expect("opId").to_string();

    // Interactive insert submitted WHILE the op runs; its round trip is the
    // latency the split exists to protect. It must not wait for the whole op
    // (well under half the op's total wall time).
    let insert_started = Instant::now();
    let insert_resp = admin_post(
        addr,
        &format!("/admin/db/{db_name}/mutate"),
        json!({"txn": {"steps": [
            {"op": "insert", "table": "items", "doc": {"kind": "live", "n": 1.0}}
        ]}}),
    )
    .await;
    let insert_status = insert_resp.status();
    let insert_body = insert_resp.text().await.unwrap_or_default();
    assert_eq!(
        insert_status,
        reqwest::StatusCode::OK,
        "interactive insert failed: {insert_body}"
    );
    let insert_elapsed = insert_started.elapsed();

    // Poll status until Complete (50ms interval, bounded).
    let mut final_status = Value::Null;
    let poll_deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < poll_deadline {
        let resp = api_get(addr, &format!("/api/bulk/{op_id}?db={db_name}"), &token).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "status GET failed");
        let body: Value = resp.json().await.expect("status body json");
        if body["state"] == json!("complete") {
            final_status = body;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let op_wall = op_started.elapsed();
    assert_eq!(
        final_status["state"],
        json!("complete"),
        "op did not complete in time: {final_status}"
    );
    assert_eq!(final_status["matched"], json!(1500));
    assert_eq!(final_status["deleted"], json!(1500));
    assert_eq!(count_rows(addr, &db_name).await, 1);

    assert!(
        insert_elapsed.as_millis() * 2 < op_wall.as_millis().max(1),
        "interactive insert ({insert_elapsed:?}) must complete in well under the op wall time ({op_wall:?})"
    );

    // Progressive per-chunk pushes: each chunk commit fans out; drain briefly
    // for the tail push, then assert.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let pushed = updates.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        pushed >= 2,
        "expected at least 2 queryUpdate pushes, saw {pushed}"
    );

    reader.abort();
}

/// 2. Unknown op id -> NotFound.
#[tokio::test]
async fn bulk_delete_status_unknown_id() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db_name) = fresh(&state).await;
    let token = mint_token(addr, &db_name).await;
    let resp = api_get(
        addr,
        &format!("/api/bulk/does-not-exist?db={db_name}"),
        &token,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

/// 3. A filter matching nothing completes immediately with matched 0.
#[tokio::test]
async fn bulk_delete_no_match_completes() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db_name) = fresh(&state).await;
    push_schema(addr, &db_name).await;
    let token = mint_token(addr, &db_name).await;
    seed_rows(addr, &db_name, 3).await;

    let resp = api_post(
        addr,
        "/api/bulk/delete",
        &token,
        json!({"db": db_name, "table": "items",
               "filter": {"op": "eq", "field": "kind", "value": "nothing-matches"}}),
    )
    .await;
    let status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::OK, "start failed: {body_text}");
    let start_body: Value = serde_json::from_str(&body_text).expect("start body json");
    assert_eq!(start_body["matched"], json!(0));
    let op_id = start_body["opId"].as_str().expect("opId").to_string();

    let completed = wait_until(Duration::from_secs(10), || async {
        let resp = api_get(addr, &format!("/api/bulk/{op_id}?db={db_name}"), &token).await;
        let body: Value = resp.json().await.unwrap_or(Value::Null);
        body["state"] == json!("complete")
    })
    .await;
    assert!(completed, "zero-match op must complete");
    assert_eq!(count_rows(addr, &db_name).await, 3);
}

/// 4. Over-cap: the start arm's reply carries `matched_over_cap` (which the
/// HTTP handler maps to 400 "narrow the filter"). Exercised by submitting the
/// arm directly with a tiny test-only cap — the HTTP handler pins the
/// production cap of 100k, impractical to seed.
#[tokio::test]
async fn bulk_delete_over_cap_fails() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db_name) = fresh(&state).await;
    push_schema(addr, &db_name).await;
    seed_rows(addr, &db_name, 5).await;

    let admission_epoch = db::admission_epoch(&state.pool)
        .await
        .expect("admission epoch");
    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    state
        .realtime
        .committers
        .submit(
            &db_name,
            CommitterRequest::RunBulkDeleteStart {
                table: "items".to_string(),
                filter: Some(
                    serde_json::from_value::<rtdb_server::query::FilterExpr>(json!(
                        {"op": "eq", "field": "kind", "value": "bulk"}
                    ))
                    .expect("parse filter"),
                ),
                principal_ctx: rtdb_server::auth::PrincipalCtx::bypass(),
                id_cap: 2,
                admission_epoch,
                reply,
            },
        )
        .await
        .expect("submit start arm");
    let result: BulkStartResult = reply_rx.await.expect("reply").expect("start ok");
    assert!(result.matched_over_cap, "5 rows must exceed a cap of 2");
    assert_eq!(result.ids.len(), 3);
}
