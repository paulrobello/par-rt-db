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
#[cfg(feature = "test-support")]
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
#[cfg(feature = "test-support")]
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::common::{
    admin_get, admin_post, fresh_db, spawn_app, test_config, test_hot, test_shared_pool,
    test_state, test_state_with_ttl_sweep,
};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::txn::{Step, Transaction};

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

/// Revokes a minted machine token via the admin surface (the normal
/// application path).
async fn revoke_machine_token(
    addr: &SocketAddr,
    state: &Arc<rtdb_server::AppState>,
    db: &str,
    token: &str,
) {
    let principal = rtdb_server::auth::resolve_bearer(&state.pool, token)
        .await
        .expect("resolve token");
    let token_id = match principal {
        rtdb_server::auth::Principal::Machine {
            token_id,
            db: token_db,
            ..
        } => {
            assert_eq!(token_db, db, "token not minted for this db");
            token_id
        }
        _ => panic!("expected machine principal"),
    };
    let resp = admin_post(*addr, "/admin/revoke-token", json!({"tokenId": token_id})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
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

fn insert_work_item_transaction(title: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: serde_json::json!({
                "projectId": "0".repeat(32),
                "title": title,
                "status": "backlog",
                "order": 1.0,
                "completedAt": null
            })
            .as_object()
            .expect("work item document")
            .clone(),
        }],
    }
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

async fn quiesce(addr: SocketAddr, db: &str, active: bool, generation: Option<i64>) -> Option<i64> {
    let response = if active {
        admin_post(addr, &format!("/admin/db/{db}/quiesce"), json!({})).await
    } else {
        reqwest::Client::new()
            .delete(format!("http://{addr}/admin/db/{db}/quiesce"))
            .header(TOKEN_HEADER, "Bearer test-admin-key")
            .header(
                "x-rtdb-quiesce-generation",
                generation.expect("quiesce generation"),
            )
            .send()
            .await
            .expect("send quiesce delete")
    };
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    if active {
        Some(
            response.json::<Value>().await.expect("quiesce response")["generation"]
                .as_i64()
                .expect("generation"),
        )
    } else {
        None
    }
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

#[tokio::test]
async fn quiesce_barrier_rejects_new_writes_and_reopens() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    assert_eq!(
        http_mutate(addr, &token, &db, insert_work_item_txn())
            .await
            .status(),
        reqwest::StatusCode::OK
    );

    let generation = quiesce(addr, &db, true, None).await;
    let readback = admin_get(addr, &format!("/admin/db/{db}/quiesce")).await;
    assert_eq!(readback.status(), reqwest::StatusCode::OK);
    let body: Value = readback.json().await?;
    assert_eq!(body, json!({"quiesced": true, "generation": generation}));
    let frozen = http_mutate(addr, &token, &db, insert_work_item_txn()).await;
    assert_eq!(frozen.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(frozen.json::<Value>().await?["code"], json!("READ_ONLY"));

    quiesce(addr, &db, false, generation).await;
    let readback = admin_get(addr, &format!("/admin/db/{db}/quiesce")).await;
    assert_eq!(readback.status(), reqwest::StatusCode::OK);
    let body: Value = readback.json().await?;
    assert_eq!(body, json!({"quiesced": false, "generation": generation}));
    assert_eq!(
        http_mutate(addr, &token, &db, insert_work_item_txn())
            .await
            .status(),
        reqwest::StatusCode::OK
    );
    Ok(())
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn stale_http_admission_is_rejected_after_body_is_released() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let probe_marker = uuid::Uuid::now_v7().to_string();
    let entered = Arc::new(Notify::new());
    let entered_for_probe = Arc::clone(&entered);
    let release = Arc::new(Notify::new());
    let release_for_probe = Arc::clone(&release);
    let marker_for_probe = probe_marker.clone();
    let _probe =
        rtdb_server::http_api::install_admission_probe(Arc::new(move |request, _epoch| {
            let matches = request
                .headers()
                .get("x-rtdb-test-probe")
                .and_then(|value| value.to_str().ok())
                == Some(marker_for_probe.as_str());
            let entered = Arc::clone(&entered_for_probe);
            let release = Arc::clone(&release_for_probe);
            Box::pin(async move {
                if matches {
                    entered.notify_one();
                    release.notified().await;
                }
            })
        }));

    let request_addr = addr;
    let request_token = token.clone();
    let request_marker = probe_marker.clone();
    let request_db = db.to_string();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://{request_addr}/api/mutate"))
            .header(TOKEN_HEADER, format!("Bearer {request_token}"))
            .header("x-rtdb-test-probe", request_marker)
            .json(&json!({"db": request_db, "txn": insert_work_item_txn()}))
            .send()
            .await
    });

    entered.notified().await;
    let generation = quiesce(addr, &db, true, None).await;
    release.notify_one();
    let response = request.await??;
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(response.json::<Value>().await?["code"], json!("READ_ONLY"));
    quiesce(addr, &db, false, generation).await;
    Ok(())
}

/// A token revoked while its request sits parked at the admission probe must
/// come out safely rejected, never committed, and leave the drain unblocked.
#[tokio::test]
async fn revoked_token_write_is_rejected_while_quiesced() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    http_mutate(addr, &token, &db, insert_work_item_txn()).await;

    let probe_marker = uuid::Uuid::now_v7().to_string();
    let entered = Arc::new(Notify::new());
    let entered_for_probe = Arc::clone(&entered);
    let release = Arc::new(Notify::new());
    let release_for_probe = Arc::clone(&release);
    let marker_for_probe = probe_marker.clone();
    let _probe =
        rtdb_server::http_api::install_admission_probe(Arc::new(move |request, _epoch| {
            let matches = request
                .headers()
                .get("x-rtdb-test-probe")
                .and_then(|value| value.to_str().ok())
                == Some(marker_for_probe.as_str());
            let entered = Arc::clone(&entered_for_probe);
            let release = Arc::clone(&release_for_probe);
            Box::pin(async move {
                if matches {
                    entered.notify_one();
                    release.notified().await;
                }
            })
        }));

    let request_db = db.to_string();
    let request_token = token.clone();
    let request_marker = probe_marker.clone();
    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("http://{addr}/api/mutate"))
            .header(TOKEN_HEADER, format!("Bearer {request_token}"))
            .header("x-rtdb-test-probe", request_marker)
            .json(&json!({"db": request_db, "txn": insert_work_item_txn()}))
            .send()
            .await
    });

    entered.notified().await;
    let generation = quiesce(addr, &db, true, None).await;
    revoke_machine_token(&addr, &state, &db, &token).await;
    release.notify_one();
    let response = request.await??;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    let persisted: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db}\".t_workItems"
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(persisted, 1, "the revoked write must not commit");
    quiesce(addr, &db, false, generation).await;
    Ok(())
}

/// Authorization is admission-time: a write already inside the committer still
/// drains to a durable commit even if its token is revoked mid-flight.
#[tokio::test]
async fn admitted_write_survives_token_revocation_during_drain() -> anyhow::Result<()> {
    let config = test_config();
    let pool = test_shared_pool(&config.database_url, 4).await?;
    db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool.clone(), config, test_hot());
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let db_name = db.to_string();
    let token = mint_token(addr, &db_name).await;
    let admission_epoch = db::admission_epoch(&pool).await?;
    state
        .realtime
        .committers
        .mutate(
            &db_name,
            None,
            insert_work_item_transaction("warmup"),
            PrincipalCtx::bypass(),
        )
        .await?;

    let principal = rtdb_server::auth::resolve_bearer(&pool, &token).await?;
    let row_ctx = principal.row_ctx();

    let mut blocker = pool.begin().await?;
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT seq FROM \"db_{db_name}\".change_head WHERE ok = TRUE FOR UPDATE"
    )))
    .execute(&mut *blocker)
    .await?;

    let committers = state.realtime.committers.clone();
    let write_db = db_name.clone();
    let write = tokio::spawn(async move {
        committers
            .mutate_with_admission(
                &write_db,
                None,
                insert_work_item_transaction("in-flight"),
                row_ctx,
                Some(admission_epoch),
            )
            .await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut waiter: Option<(i32, String)> = None;
    while tokio::time::Instant::now() < deadline {
        waiter = sqlx::query_as::<_, (i32, String)>(
            "SELECT a.pid, a.query
             FROM pg_stat_activity AS a
             WHERE a.wait_event_type = 'Lock'
               AND $1 = ANY(pg_blocking_pids(a.pid))
               AND a.query LIKE '%change_head%'
             LIMIT 1",
        )
        .bind(blocker_pid)
        .fetch_optional(&pool)
        .await?;
        if waiter.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let Some((waiter_pid, waiter_statement)) = waiter else {
        panic!("mutation never reached the change_head lock");
    };
    assert_ne!(waiter_pid, blocker_pid);
    assert!(waiter_statement.contains("change_head"));

    let quiesce_committers = state.realtime.committers.clone();
    let quiesce_db = db_name.clone();
    let quiesce = tokio::spawn(async move { quiesce_committers.quiesce(&quiesce_db).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut quiesced = false;
    while tokio::time::Instant::now() < deadline {
        quiesced = db::quiesce_state(&pool, &db_name)
            .await?
            .is_some_and(|(active, _)| active);
        if quiesced {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(quiesced, "quiesce did not close admission");
    assert!(
        !quiesce.is_finished(),
        "quiesce returned before the blocked mutation drained"
    );

    revoke_machine_token(&addr, &state, &db_name, &token).await;
    blocker.rollback().await?;
    write.await??;
    let generation = quiesce.await??;
    assert_eq!(
        db::quiesce_state(&pool, &db_name).await?,
        Some((true, generation))
    );
    let persisted: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db_name}\".t_workItems WHERE doc->>'title' = 'in-flight'"
    )))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        persisted, 1,
        "the admitted mutation must commit despite the mid-flight revocation"
    );
    state
        .realtime
        .committers
        .unquiesce(&db_name, generation)
        .await?;
    Ok(())
}

#[tokio::test]
async fn existing_websocket_rejects_writes_after_quiesce_and_reopens() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let mut ws = ws_connect(addr).await;
    send_json(
        &mut ws,
        json!({"type": "auth", "token": token, "db": db.as_str()}),
    )
    .await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("authOk"));

    let generation = quiesce(addr, &db, true, None).await;
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "quiesced", "txn": insert_work_item_txn()}),
    )
    .await;
    let rejected = recv_json(&mut ws).await;
    assert_eq!(rejected["type"], json!("mutateErr"));
    assert_eq!(rejected["error"]["code"], json!("READ_ONLY"));

    quiesce(addr, &db, false, generation).await;
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "reopened", "txn": insert_work_item_txn()}),
    )
    .await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("mutateOk"));
    Ok(())
}

#[tokio::test]
async fn quiesce_state_survives_process_restart_until_owner_release() -> anyhow::Result<()> {
    let config = test_config();
    let pool = test_shared_pool(&config.database_url, 8).await?;
    db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool.clone(), config.clone(), test_hot());
    let db = fresh_db(&state).await;
    let db_name = db.to_string();
    let stale_epoch = db::admission_epoch(&pool).await?;
    let generation = state.realtime.committers.quiesce(&db_name).await?;
    let epoch = db::admission_epoch(&pool).await?;
    state.realtime.committers.drop_db(&db_name).await;
    state.background.shutdown(Duration::from_secs(1)).await;
    drop(state);

    let replacement = rtdb_server::AppState::new(pool.clone(), config, test_hot());
    let rejected = replacement
        .realtime
        .committers
        .mutate_with_admission(
            &db_name,
            None,
            insert_work_item_transaction("after-restart"),
            PrincipalCtx::bypass(),
            Some(epoch),
        )
        .await
        .expect_err("restart must not reopen a quiesced database");
    assert_eq!(rejected.code, rtdb_server::error::ErrorCode::ReadOnly);
    // Release recovery: the old owner's Postgres session died with the
    // process, so the quiesce advisory lock is free — the replacement takes
    // it transiently, validates the generation, and reopens the database.
    replacement
        .realtime
        .committers
        .unquiesce(&db_name, generation)
        .await
        .expect("release after the owning session is gone must succeed");
    assert_eq!(
        db::quiesce_state(&pool, &db_name).await?,
        Some((false, generation))
    );
    // Release does not re-admit stale authorities: the admission fence set by
    // the quiesce persists, so the pre-quiesce epoch stays rejected while a
    // fresh admission goes through.
    let stale = replacement
        .realtime
        .committers
        .mutate_with_admission(
            &db_name,
            None,
            insert_work_item_transaction("stale-after-release"),
            PrincipalCtx::bypass(),
            Some(stale_epoch),
        )
        .await
        .expect_err("release must not re-admit the pre-quiesce epoch");
    assert_eq!(stale.code, rtdb_server::error::ErrorCode::ReadOnly);
    replacement
        .realtime
        .committers
        .mutate(
            &db_name,
            None,
            insert_work_item_transaction("after-release"),
            PrincipalCtx::bypass(),
        )
        .await
        .expect("fresh admission works after release");
    replacement
        .background
        .shutdown(Duration::from_secs(1))
        .await;
    Ok(())
}

#[tokio::test]
async fn stale_epoch_cannot_write_after_database_delete_and_recreate() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let stale_epoch = db::admission_epoch(&state.pool).await?;

    db::drop_database(&state.pool, &db)
        .await
        .expect("drop before recreate");
    db::create_database(&state.pool, &db)
        .await
        .expect("recreate after drop");

    let current_epoch = db::admission_epoch(&state.pool).await?;
    let mut connection = state.pool.acquire().await?;
    let stale = db::ensure_admission_epoch(&mut connection, &db, stale_epoch)
        .await
        .expect_err("old epoch must not cross delete/recreate");
    assert_eq!(stale.code, rtdb_server::error::ErrorCode::ReadOnly);
    db::ensure_admission_epoch(&mut connection, &db, current_epoch).await?;
    Ok(())
}

#[tokio::test]
async fn admitted_mutation_finishes_after_quiesce_starts() -> anyhow::Result<()> {
    let config = test_config();
    let pool = test_shared_pool(&config.database_url, 4).await?;
    db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool.clone(), config, test_hot());
    let db = fresh_db(&state).await;
    let db_name = db.to_string();
    let admission_epoch = db::admission_epoch(&pool).await?;
    state
        .realtime
        .committers
        .mutate(
            &db_name,
            None,
            insert_work_item_transaction("warmup"),
            PrincipalCtx::bypass(),
        )
        .await?;
    let work_items_exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("db_{db_name}.t_workItems"))
        .fetch_one(&pool)
        .await?;
    assert!(work_items_exists, "workItems fixture table is missing");
    let change_head_oid: i64 = sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(format!("db_{db_name}.change_head"))
        .fetch_one(&pool)
        .await?;

    let mut blocker = pool.begin().await?;
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT seq FROM \"db_{db_name}\".change_head WHERE ok = TRUE FOR UPDATE"
    )))
    .execute(&mut *blocker)
    .await?;

    let committers = state.realtime.committers.clone();
    let write_db = db_name.clone();
    let write = tokio::spawn(async move {
        committers
            .mutate_with_admission(
                &write_db,
                None,
                insert_work_item_transaction("in-flight"),
                PrincipalCtx::bypass(),
                Some(admission_epoch),
            )
            .await
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut waiter: Option<(i32, String)> = None;
    while tokio::time::Instant::now() < deadline {
        waiter = sqlx::query_as::<_, (i32, String)>(
            "SELECT a.pid, a.query
             FROM pg_stat_activity AS a
             WHERE a.wait_event_type = 'Lock'
               AND $1 = ANY(pg_blocking_pids(a.pid))
               AND a.query LIKE '%change_head%'
             LIMIT 1",
        )
        .bind(blocker_pid)
        .fetch_optional(&pool)
        .await?;
        if waiter.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if waiter.is_none() {
        let finished = write.is_finished();
        let outcome = if finished {
            Some(format!("{:?}", write.await))
        } else {
            None
        };
        panic!(
            "mutation did not reach change_head lock: blocker_pid={blocker_pid}, change_head_oid={change_head_oid}, finished={finished}, outcome={outcome:?}"
        );
    }
    let (waiter_pid, waiter_statement) = waiter.expect("waiter checked above");
    assert_ne!(waiter_pid, blocker_pid);
    assert!(waiter_statement.contains("change_head"));

    let quiesce_committers = state.realtime.committers.clone();
    let quiesce_db = db_name.clone();
    let quiesce = tokio::spawn(async move { quiesce_committers.quiesce(&quiesce_db).await });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut quiesced = false;
    while tokio::time::Instant::now() < deadline {
        quiesced = db::quiesce_state(&pool, &db_name)
            .await?
            .is_some_and(|(active, _)| active);
        if quiesced {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(quiesced, "quiesce did not close admission");
    assert!(
        !quiesce.is_finished(),
        "quiesce returned before the blocked mutation drained"
    );
    blocker.rollback().await?;
    write.await??;
    let generation = quiesce.await??;
    assert_eq!(
        db::quiesce_state(&pool, &db_name).await?,
        Some((true, generation))
    );
    let persisted: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db_name}\".t_workItems WHERE doc->>'title' = 'in-flight'"
    )))
    .fetch_one(&pool)
    .await?;
    assert_eq!(persisted, 1, "the admitted mutation was durably committed");

    let rejected = state
        .realtime
        .committers
        .mutate_with_admission(
            &db_name,
            None,
            insert_work_item_transaction("after-quiesce"),
            PrincipalCtx::bypass(),
            Some(db::admission_epoch(&pool).await?),
        )
        .await
        .expect_err("new target write must be rejected while quiesced");
    assert_eq!(rejected.code, rtdb_server::error::ErrorCode::ReadOnly);

    let unrelated = fresh_db(&state).await;
    state
        .realtime
        .committers
        .mutate(
            &unrelated,
            None,
            insert_work_item_transaction("unrelated"),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .unquiesce(&db_name, generation)
        .await?;
    Ok(())
}

#[tokio::test]
async fn quiesce_stale_generation_cannot_release_or_replace_the_active_barrier()
-> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let generation = quiesce(addr, &db, true, None).await.expect("generation");
    let stale = reqwest::Client::new()
        .delete(format!("http://{addr}/admin/db/{db}/quiesce"))
        .header(TOKEN_HEADER, "Bearer test-admin-key")
        .header("x-rtdb-quiesce-generation", generation + 1)
        .send()
        .await?;
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    let duplicate = admin_post(addr, &format!("/admin/db/{db}/quiesce"), json!({})).await;
    assert_eq!(duplicate.status(), reqwest::StatusCode::CONFLICT);
    assert!(quiesce(addr, &db, false, Some(generation)).await.is_none());
    Ok(())
}

/// The cutover fence is one-way for old admissions: quiesce bumps the global
/// admission epoch and the database's persisted fence to it, and release
/// clears the `quiesced` flag without rolling the fence back. An admission
/// captured before the quiesce therefore stays rejected on the direct
/// committer and storage paths even after release, while fresh admissions go
/// through. The transport-level version of this race is pinned by
/// `stale_http_admission_is_rejected_after_body_is_released`.
#[tokio::test]
async fn stale_admission_epoch_stays_rejected_after_release_for_direct_paths() -> anyhow::Result<()>
{
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let db_name = db.to_string();
    let stale_epoch = db::admission_epoch(&state.pool).await?;

    let generation = state.realtime.committers.quiesce(&db_name).await?;
    state
        .realtime
        .committers
        .unquiesce(&db_name, generation)
        .await?;
    assert_eq!(
        db::quiesce_state(&state.pool, &db_name).await?,
        Some((false, generation)),
        "release cleared the flag"
    );

    let rejected = state
        .realtime
        .committers
        .mutate_with_admission(
            &db_name,
            None,
            insert_work_item_transaction("stale-after-release"),
            PrincipalCtx::bypass(),
            Some(stale_epoch),
        )
        .await
        .expect_err("stale epoch must stay rejected after release");
    assert_eq!(rejected.code, rtdb_server::error::ErrorCode::ReadOnly);

    let put = rtdb_server::storage::put_with_epoch(
        &state.pool,
        &db_name,
        "stale-blob",
        5,
        None,
        None,
        b"bytes",
        Some(stale_epoch),
    )
    .await;
    assert_eq!(
        put.expect_err("stale storage put must be fenced").code,
        rtdb_server::error::ErrorCode::ReadOnly
    );

    let delete = rtdb_server::storage::delete_with_epoch(
        &state.pool,
        &db_name,
        "no-such-id",
        Some(stale_epoch),
    )
    .await;
    assert_eq!(
        delete
            .expect_err("stale storage delete must be fenced")
            .code,
        rtdb_server::error::ErrorCode::ReadOnly
    );

    state
        .realtime
        .committers
        .mutate(
            &db_name,
            None,
            insert_work_item_transaction("fresh-after-release"),
            PrincipalCtx::bypass(),
        )
        .await
        .expect("fresh admission works after release");
    Ok(())
}

/// Maintenance writers pause with everything else: the mutation-log cleanup
/// sweep (which also carries change-log trimming) skips a quiesced database
/// and resumes on release. Deterministic — the expiry is backdated with a
/// direct UPDATE, no timing.
#[tokio::test]
async fn quiesce_pauses_mutation_log_cleanup() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let db_name = db.to_string();
    rtdb_server::mutation_log::ensure_table(&state.pool, &db_name).await?;
    rtdb_server::mutation_log::store(
        &state.pool,
        &db_name,
        "mut-quiesce-1",
        &[json!({"ok": true})],
        60_000,
    )
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE \"db_{db_name}\".mutation_cache_v2 SET expires_at = 0 WHERE mut_id = 'mut-quiesce-1'"
    )))
    .execute(&state.pool)
    .await?;

    let generation = rtdb_server::db::quiesce_database(&state.pool, &db_name)
        .await?
        .expect("database registered");
    let removed = rtdb_server::mutation_log::cleanup_expired(&state.pool, &db_name).await?;
    assert_eq!(removed, 0, "cleanup must skip a quiesced database");
    let kept: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db_name}\".mutation_cache_v2"
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(kept, 1, "the expired row survives the quiesced sweep");

    assert!(rtdb_server::db::clear_quiesce(&state.pool, &db_name, generation).await?);
    let removed = rtdb_server::mutation_log::cleanup_expired(&state.pool, &db_name).await?;
    assert_eq!(removed, 1, "cleanup resumes after release");
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

/// Release criterion: a scheduled fire due after a quiesce/unquiesce cycle
/// lands on release — the scheduler's claim captures a fresh admission epoch
/// and the freeze-exempt system arm executes under the lifted fence. Paired
/// with `readonly_scheduled_fire_continues_while_frozen`, which pins the
/// same system arm against the read-only freeze.
#[tokio::test]
async fn scheduled_fire_lands_after_quiesce_release_cycle() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Due comfortably after the lifecycle round trip below.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/schedule"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({
            "db": db.as_str(),
            "when": {"type": "afterMs", "ms": 2000},
            "txn": insert_work_item_txn()
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "schedule created");

    let generation = quiesce(addr, &db, true, None).await;
    quiesce(addr, &db, false, generation).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(12);
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
        "a scheduled fire must land after the quiesce is released"
    );
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

/// ENH-048: the documented order is pinned at the new combined-preamble level
/// too — a frozen database's mutate carrying an already-committed idempotency
/// key replays the cached result instead of erroring `READ_ONLY`. The replay
/// decision now happens inside `execute_txn_with_side`'s preamble statement,
/// so this guards the in-transaction ordering (cached-result branch before
/// the freeze branch).
#[tokio::test]
async fn readonly_frozen_replay_returns_cached_result_not_read_only() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Commit one keyed write BEFORE the freeze so the cached result exists.
    let txn = insert_work_item_txn();
    let key = "frozen-replay-preamble";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db.as_str(), "txn": txn, "idempotencyKey": key}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let first: Value = resp.json().await?;

    freeze(addr, &db, true).await;

    // Same key again while frozen: the preamble's cached-result branch must
    // fire before the freeze branch — cached outcome, HTTP 200, not 409.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header(TOKEN_HEADER, format!("Bearer {token}"))
        .json(&json!({"db": db.as_str(), "txn": txn, "idempotencyKey": key}))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "replay must win over READ_ONLY"
    );
    let second: Value = resp.json().await?;
    assert_eq!(first, second, "replay returns the original outcome");

    // And a fresh (unkeyed) write under the same freeze is still rejected,
    // proving the freeze itself is live.
    let resp = http_mutate(addr, &token, db.as_str(), insert_work_item_txn()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await?;
    assert_eq!(body["code"], json!("READ_ONLY"));

    Ok(())
}
