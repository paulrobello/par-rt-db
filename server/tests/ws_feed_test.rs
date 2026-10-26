//! WebSocket change-feed channel (`feedSubscribe`) — resume, at-least-once,
//! server-side table filter, typed cursor expiry, and authorization.

use std::time::Duration;

use crate::common::cluster::WsClient;
use crate::common::{
    admin_post, fresh_db, kanban_schema_json, mint_user_session, spawn_app, test_state,
};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::change_log;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::{ClientMessage, ServerMessage};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

const WAIT: Duration = Duration::from_secs(10);

async fn write(pool: &sqlx::PgPool, db: &str, table: &str, doc: serde_json::Value) -> String {
    let schema: SchemaDef = serde_json::from_value(kanban_schema_json()).expect("schema");
    let doc = doc.as_object().expect("object").clone();
    let outcome = execute_txn(
        pool,
        db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: table.to_string(),
                doc,
            }],
        },
        &PrincipalCtx::bypass(),
        None,
    )
    .await
    .expect("insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

async fn project(pool: &sqlx::PgPool, db: &str, name: &str) -> String {
    let doc = serde_json::json!({
        "name": name, "description": null, "status": "active", "tags": [], "updatedAt": 1.0
    });
    write(pool, db, "projects", doc).await
}

async fn item(pool: &sqlx::PgPool, db: &str, project_id: &str, title: &str) {
    let doc = serde_json::json!({
        "projectId": project_id, "title": title, "status": "backlog", "order": 1.0
    });
    write(pool, db, "workItems", doc).await;
}

async fn mint(addr: std::net::SocketAddr, db: &str, tables: Option<Vec<&str>>) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({ "db": db, "name": "ws-feed-test", "tables": tables }),
    )
    .await;
    let body: serde_json::Value = resp.json().await.expect("mint json");
    body["token"].as_str().expect("token").to_string()
}

fn subscribe(since: Option<i64>, epoch: Option<&str>, tables: &[&str]) -> ClientMessage {
    ClientMessage::FeedSubscribe {
        feed_id: "f1".to_string(),
        since,
        feed_epoch: epoch.map(str::to_string),
        tables: tables.iter().map(|t| t.to_string()).collect(),
    }
}

/// Collects `(epoch, seqs)` until `want` ops arrived (the confirmation frame
/// may carry zero ops).
async fn collect(ws: &mut WsClient, want: usize) -> (String, Vec<i64>) {
    let mut epoch = String::new();
    let mut seqs = Vec::new();
    while seqs.len() < want {
        match ws.recv_timeout(WAIT).await {
            Some(ServerMessage::FeedEvents {
                feed_epoch, ops, ..
            }) => {
                epoch = feed_epoch;
                seqs.extend(ops.iter().map(|o| o.seq));
            }
            other => panic!("expected feedEvents, got {other:?}"),
        }
    }
    (epoch, seqs)
}

#[tokio::test]
async fn resume_replays_exactly_ops_after_cursor_at_least_once() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint(addr, &db.0, None).await;
    for n in ["a", "b", "c"] {
        project(&state.pool, &db.0, n).await;
    }

    let mut ws = WsClient::connect(addr, &db.0, &token).await;
    ws.send(&subscribe(Some(1), None, &[])).await;
    let (epoch, seqs) = collect(&mut ws, 2).await;
    assert_eq!(seqs, vec![2, 3], "exactly the ops after the cursor");

    // Live push after the replay.
    project(&state.pool, &db.0, "d").await;
    let (_, live) = collect(&mut ws, 1).await;
    assert_eq!(live, vec![4]);
    drop(ws);

    // The consumer only processed through seq 2 before dying: reconnecting
    // with that cursor redelivers 3 and 4 (at-least-once), same epoch.
    let mut ws = WsClient::connect(addr, &db.0, &token).await;
    ws.send(&subscribe(Some(2), Some(&epoch), &[])).await;
    let (epoch2, again) = collect(&mut ws, 2).await;
    assert_eq!(again, vec![3, 4]);
    assert_eq!(epoch2, epoch, "(feedEpoch, seq) is a stable dedupe key");
}

#[tokio::test]
async fn table_filter_drops_non_matching_ops_before_send() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint(addr, &db.0, None).await;
    let pid = project(&state.pool, &db.0, "p1").await; // seq 1
    item(&state.pool, &db.0, &pid, "i1").await; // seq 2
    project(&state.pool, &db.0, "p2").await; // seq 3

    let mut ws = WsClient::connect(addr, &db.0, &token).await;
    ws.send(&subscribe(Some(0), None, &["workItems"])).await;
    let (_, seqs) = collect(&mut ws, 1).await;
    assert_eq!(seqs, vec![2]);
    project(&state.pool, &db.0, "p3").await; // seq 4: filtered
    item(&state.pool, &db.0, &pid, "i2").await; // seq 5
    let (_, seqs) = collect(&mut ws, 1).await;
    assert_eq!(seqs, vec![5], "projects ops never reach the consumer");
    assert!(
        ws.recv_timeout(Duration::from_millis(300)).await.is_none(),
        "no further frames"
    );
}

#[tokio::test]
async fn token_table_scope_applies_and_user_principal_is_forbidden() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let scoped = mint(addr, &db.0, Some(vec!["workItems"])).await;
    let pid = project(&state.pool, &db.0, "p1").await;
    item(&state.pool, &db.0, &pid, "i1").await;

    let mut ws = WsClient::connect(addr, &db.0, &scoped).await;
    ws.send(&subscribe(Some(0), None, &[])).await;
    let (_, seqs) = collect(&mut ws, 1).await;
    assert_eq!(seqs, vec![2], "scoped token never sees projects");

    // An admitted (admin) user session is not a machine token.
    let email = "feed-admin-ws@example.com";
    let session = mint_user_session(&state.pool, "feed-admin-ws", email).await;
    sqlx::query(
        "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)
         ON CONFLICT DO NOTHING",
    )
    .bind(email)
    .bind(rtdb_server::db::now_ms())
    .execute(&state.pool)
    .await
    .expect("admin row");
    let mut user_ws = WsClient::connect(addr, &db.0, &session).await;
    user_ws.send(&subscribe(Some(0), None, &[])).await;
    match user_ws.recv_timeout(WAIT).await {
        Some(ServerMessage::FeedErr { error, .. }) => {
            assert_eq!(error.code, ErrorCode::Forbidden);
        }
        other => panic!("expected feedErr, got {other:?}"),
    }
}

#[tokio::test]
async fn evicted_or_foreign_cursor_is_typed_cursor_expired() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint(addr, &db.0, None).await;
    for n in ["a", "b", "c"] {
        project(&state.pool, &db.0, n).await;
    }
    // Keep only the newest row (seq 3): cursor 0 now predates the window.
    change_log::trim_expired(&state.pool, &db.0, 1)
        .await
        .expect("trim");

    for msg in [
        subscribe(Some(0), None, &[]),
        subscribe(Some(99), None, &[]),
        subscribe(Some(2), Some("not-the-log-id"), &[]),
    ] {
        let mut ws = WsClient::connect(addr, &db.0, &token).await;
        ws.send(&msg).await;
        match ws.recv_timeout(WAIT).await {
            Some(ServerMessage::FeedErr { error, .. }) => {
                assert_eq!(error.code, ErrorCode::CursorExpired, "{msg:?}");
            }
            other => panic!("expected feedErr for {msg:?}, got {other:?}"),
        }
    }

    // A cursor inside the window still works.
    let mut ws = WsClient::connect(addr, &db.0, &token).await;
    ws.send(&subscribe(Some(2), None, &[])).await;
    let (_, seqs) = collect(&mut ws, 1).await;
    assert_eq!(seqs, vec![3]);
}
