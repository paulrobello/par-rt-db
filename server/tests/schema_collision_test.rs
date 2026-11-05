//! Regression tests for the 2026-11-05 production incident: tenant schema
//! pushes colliding with the 03:00Z `pg_dump` backup queued ALTERs on the
//! dump's ACCESS SHARE table locks, each queued ALTER held a sqlx pool
//! connection, and the shared query pool exhausted (fleet-wide 500s) while
//! Postgres itself stayed healthy.
//!
//! Covers the four shipped mechanisms (see `server/src/schema_gate.rs` and
//! `ddl::push_schema_gated`):
//! 1. **Push short-circuit** — a push whose tables map matches the committed
//!    schema issues no DDL and no schema-history capture.
//! 2. **Single-flight** — N concurrent pushes of the same NEW schema issue
//!    exactly one DDL pass (the winner); the rest short-circuit against the
//!    winner's committed result.
//! 3. **Pool protection** — a simulated backup (an open transaction holding
//!    ACCESS SHARE, exactly what pg_dump holds while it reads) blocks pushes
//!    in DDL, but the schema gate caps how many pool connections those waits
//!    can pin, and ordinary API traffic is served throughout the collision.
//! 4. **Backup-window serialization** — while a backup holds the gate, a
//!    changed push defers with a retryable 409; an unchanged push still
//!    succeeds (it never needs DDL admission).

use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    TestDb, admin_get, admin_post, connect_and_bootstrap_pooled, spawn_app, test_config, test_hot,
    test_state, wrap_test_db,
};
use rtdb_server::schema_gate::MAX_CONCURRENT_SCHEMA_DDL;
use rtdb_server::{AppState, db, ddl};
use serde_json::{Value, json};
use sqlx::Connection;

/// The incident shape: `holdme` gains an indexed `score` column in v2 (the
/// `ALTER TABLE ADD COLUMN` that queued behind the backup), `writes` is the
/// ordinary-traffic table that must keep being served throughout.
fn collision_schema(with_score: bool) -> Value {
    let mut holdme_fields = json!({"name": {"type": "string"}});
    let mut holdme_indexes = json!([{"name": "by_name", "fields": ["name"]}]);
    if with_score {
        holdme_fields["score"] = json!({"type": "number"});
        holdme_indexes
            .as_array_mut()
            .expect("indexes array")
            .push(json!({"name": "by_score", "fields": ["score"]}));
    }
    json!({"tables": {
        "holdme": {"fields": holdme_fields, "indexes": holdme_indexes},
        "writes": {"fields": {"value": {"type": "string"}},
                    "indexes": [{"name": "by_value", "fields": ["value"]}]}
    }})
}

async fn push_via_api(addr: std::net::SocketAddr, db: &str, schema: Value) -> reqwest::Response {
    admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": schema}),
    )
    .await
}

async fn history_count(addr: std::net::SocketAddr, db: &str) -> usize {
    let resp = admin_get(addr, &format!("/admin/db/{db}/schema/history")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("history body");
    body["entries"].as_array().expect("entries array").len()
}

/// Creates a bare test database and pushes the base (v1) schema DIRECTLY
/// (outside the committer — setup only; the API drives every assertion below).
/// Returns the name plus the RAII `TestDb` whose Drop schedules cleanup — the
/// caller MUST keep it alive for the whole test or the database is dropped
/// the moment this helper returns.
async fn create_collision_db(state: &Arc<AppState>) -> (String, TestDb) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create collision db");
    let schema: rtdb_server::schema::SchemaDef =
        serde_json::from_value(collision_schema(false)).expect("parse v1 schema");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push v1 schema");
    let guard = wrap_test_db(name.clone());
    (name, guard)
}

/// Opens the simulated backup: a dedicated connection with an open
/// transaction that has read `t_holdme`, holding ACCESS SHARE on it until
/// `release_simulated_backup` — exactly the lock posture a `pg_dump` has on
/// the table it is currently reading, and what the incident's ALTERs queued
/// behind. Returns the connection; the caller releases by ROLLBACK/drop.
async fn hold_backup_lock(database_url: &str, db: &str) -> sqlx::PgConnection {
    let mut conn = sqlx::PgConnection::connect(database_url)
        .await
        .expect("connect holder");
    sqlx::query("BEGIN")
        .execute(&mut conn)
        .await
        .expect("BEGIN");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"SELECT count(*) FROM "db_{db}"."t_holdme""#
    )))
    .execute(&mut conn)
    .await
    .expect("hold ACCESS SHARE on t_holdme");
    conn
}

/// One ordinary-traffic insert into `writes` via the public API, with the
/// bearer token the API requires.
async fn api_insert(
    client: &reqwest::Client,
    addr: std::net::SocketAddr,
    db: &str,
    token: &str,
    value: &str,
) -> anyhow::Result<()> {
    let resp = client
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"db": db, "txn": {"steps": [
            {"op": "insert", "table": "writes", "doc": {"value": value}}
        ]}}))
        .timeout(Duration::from_secs(15))
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    if status != reqwest::StatusCode::OK {
        anyhow::bail!("insert failed: {status} {body}");
    }
    Ok(())
}

async fn mint_token(addr: std::net::SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("mint-token body");
    body["token"].as_str().expect("token").to_string()
}

/// 1 + 2: unchanged pushes short-circuit (no DDL, no history capture), and a
/// concurrent storm of the same NEW schema coalesces into exactly one DDL
/// pass — the single-flight property, observed through `schema_history`
/// (only a DDL-executing push captures a row).
#[tokio::test]
async fn unchanged_pushes_short_circuit_and_storms_single_flight() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (db, _guard) = create_collision_db(&state).await;

    // The first API push is a no-op against the directly-pushed v1: it must
    // succeed and capture nothing new (DDL never runs).
    let resp = push_via_api(addr, &db, collision_schema(false)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        history_count(addr, &db).await,
        0,
        "no-op push captures no history"
    );

    // 8 concurrent IDENTICAL no-op pushes: all succeed, still no DDL.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (addr, db) = (addr, db.clone());
        handles.push(tokio::spawn(async move {
            push_via_api(addr, &db, collision_schema(false))
                .await
                .status()
        }));
    }
    for handle in handles {
        assert_eq!(
            handle.await.expect("push task"),
            reqwest::StatusCode::OK,
            "no-op push must succeed"
        );
    }
    assert_eq!(
        history_count(addr, &db).await,
        0,
        "identical re-push storm must issue no DDL passes"
    );

    // 8 concurrent pushes of the SAME new schema (the incident's shape):
    // exactly one DDL pass (one history capture), every push succeeds.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (addr, db) = (addr, db.clone());
        handles.push(tokio::spawn(async move {
            push_via_api(addr, &db, collision_schema(true))
                .await
                .status()
        }));
    }
    for handle in handles {
        assert_eq!(
            handle.await.expect("push task"),
            reqwest::StatusCode::OK,
            "single-flighted push must succeed"
        );
    }
    assert_eq!(
        history_count(addr, &db).await,
        1,
        "N concurrent identical pushes must coalesce into ONE DDL pass"
    );
    Ok(())
}

/// 3: during a simulated backup (ACCESS SHARE held on `t_holdme`), a changed
/// push blocks in DDL — but ordinary API traffic keeps being served on the
/// same server (same shared pool), and only ONE pool connection is pinned by
/// the blocked DDL.
#[tokio::test]
async fn blocked_push_cannot_starve_the_query_pool() -> anyhow::Result<()> {
    let config = test_config();
    let url = config.database_url.clone();
    // A pool big enough to be representative but small enough that one or two
    // pinned connections would matter: 6 connections, of which the gate may
    // pin at most MAX_CONCURRENT_SCHEMA_DDL for DDL lock waits.
    let pool = connect_and_bootstrap_pooled(&url, 6).await?;
    let state = AppState::new(pool, config, test_hot());
    let addr = spawn_app(state.clone()).await;

    let (pushed_db, _pushed_guard) = create_collision_db(&state).await;
    // Ordinary traffic flows to a DIFFERENT database (its committer is free —
    // the pushed db's committer is occupied by the blocked push by design).
    let (traffic_db, _traffic_guard) = create_collision_db(&state).await;
    let token = mint_token(addr, &traffic_db).await;
    let client = reqwest::Client::new();

    let mut backup = hold_backup_lock(&url, &pushed_db).await;

    // The changed push blocks: its ALTER on t_holdme queues behind the held
    // ACCESS SHARE (pg_dump posture) until the "backup" rolls back.
    let push_task = {
        let (addr, db) = (addr, pushed_db.clone());
        tokio::spawn(async move { push_via_api(addr, &db, collision_schema(true)).await })
    };

    // Wait until the push is parked in DDL holding exactly one gate slot.
    let gate = state.runtime.schema_gate.clone();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while gate.ddl_in_flight() == 0 {
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "push never reached DDL"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(gate.ddl_in_flight(), 1, "one blocked DDL pass, one slot");

    // THE incident assertion: ordinary queries are served WHILE the DDL is
    // blocked on the backup's lock — on the shared pool, not a side channel.
    for i in 0..6 {
        api_insert(
            &client,
            addr,
            &traffic_db,
            &token,
            &format!("during-collision-{i}"),
        )
        .await
        .expect("ordinary insert served during the DDL/backup collision");
    }

    // Release the backup; the parked push completes within its lock_timeout.
    sqlx::query("ROLLBACK")
        .execute(&mut backup)
        .await
        .expect("release simulated backup");
    drop(backup);
    let resp = tokio::time::timeout(Duration::from_secs(45), push_task)
        .await
        .expect("push completes after backup release")
        .expect("push task join");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(gate.ddl_in_flight(), 0, "gate slot released after commit");
    Ok(())
}

/// 3 (storm variant): five databases' pushes collide with five simulated
/// backups at once. The gate caps concurrent DDL passes at
/// MAX_CONCURRENT_SCHEMA_DDL — the many-lock-wait pool exhaustion of the
/// incident is structurally impossible — and ordinary traffic keeps flowing
/// the whole time.
#[tokio::test]
async fn multi_db_push_storm_is_capped_and_queries_served() -> anyhow::Result<()> {
    let config = test_config();
    let url = config.database_url.clone();
    let pool = connect_and_bootstrap_pooled(&url, 6).await?;
    let state = AppState::new(pool, config, test_hot());
    let addr = spawn_app(state.clone()).await;

    let mut dbs = Vec::new();
    let mut guards = Vec::new();
    for _ in 0..5 {
        let (db, guard) = create_collision_db(&state).await;
        dbs.push(db);
        guards.push(guard);
    }
    let (traffic_db, _traffic_guard) = create_collision_db(&state).await;
    let token = mint_token(addr, &traffic_db).await;
    let client = reqwest::Client::new();

    // Every db's t_holdme is under a simulated backup lock.
    let mut backups = Vec::new();
    for db in &dbs {
        backups.push(hold_backup_lock(&url, db).await);
    }

    // Fire all five changed pushes at once.
    let mut push_tasks = Vec::new();
    for db in &dbs {
        let (addr, db) = (addr, db.clone());
        push_tasks.push(tokio::spawn(async move {
            push_via_api(addr, &db, collision_schema(true)).await
        }));
    }

    // Observe the gate through the collision: the DDL cap is never exceeded,
    // whatever the queue depth behind it, and the storm reaches DDL at all.
    let gate = state.runtime.schema_gate.clone();
    let mut max_observed = 0;
    let observe_until = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < observe_until {
        max_observed = max_observed.max(gate.ddl_in_flight());
        assert!(
            max_observed <= MAX_CONCURRENT_SCHEMA_DDL,
            "DDL concurrency cap violated: {max_observed}"
        );
        // Ordinary traffic during the storm, not only after it.
        api_insert(&client, addr, &traffic_db, &token, "storm-traffic").await?;
        if max_observed == MAX_CONCURRENT_SCHEMA_DDL {
            break; // saturated; nothing more to learn
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        max_observed >= 1,
        "the storm never reached DDL under the simulated backups"
    );
    assert!(
        max_observed <= MAX_CONCURRENT_SCHEMA_DDL,
        "DDL concurrency cap violated: {max_observed}"
    );

    // Release every backup; all five pushes complete.
    for mut backup in backups {
        sqlx::query("ROLLBACK")
            .execute(&mut backup)
            .await
            .expect("release simulated backup");
    }
    for task in push_tasks {
        let resp = tokio::time::timeout(Duration::from_secs(45), task)
            .await
            .expect("push completes after backups release")
            .expect("push task join");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }
    assert_eq!(gate.ddl_in_flight(), 0);
    Ok(())
}

/// 4: while a backup holds the schema gate, a CHANGED push defers with a
/// retryable 409 naming the backup; an UNCHANGED push still succeeds (it
/// never needs DDL admission); after the backup, the changed push applies.
#[tokio::test]
async fn backup_window_defers_changed_pushes_only() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (db, _guard) = create_collision_db(&state).await;

    let backup_hold = state.runtime.schema_gate.clone().begin_backup().await;
    assert!(state.runtime.schema_gate.backup_active());

    // Unchanged push: short-circuits before DDL admission — succeeds.
    let resp = push_via_api(addr, &db, collision_schema(false)).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "unchanged push must succeed during a backup (no DDL needed)"
    );

    // Changed push: defers with a retryable 409 that names the backup.
    let resp = push_via_api(addr, &db, collision_schema(true)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: Value = resp.json().await.expect("error body");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("backup is in flight")),
        "deferral message names the backup: {body}"
    );

    drop(backup_hold);
    assert!(!state.runtime.schema_gate.backup_active());

    let resp = push_via_api(addr, &db, collision_schema(true)).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
