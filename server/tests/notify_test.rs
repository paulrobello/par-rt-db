//! Integration tests for cross-instance op-feed fan-out via Postgres
//! LISTEN/NOTIFY (ENH-022 Stage 2).
//!
//! These tests build two `AppState`s (replica A + replica B) sharing one
//! Postgres pool, both with `multi_instance = true` and distinct instance ids.
//! A write committed on replica A emits a `pg_notify('rtdb_ops', …)` at the
//! committer tap site; replica B's LISTEN task mirrors it into its local
//! op-feed ring. The self-dedupe contract (instance-id tag) keeps A from
//! double-publishing its own writes into its own ring.

use crate::common::{admin_get, admin_post, spawn_app, test_config, test_hot};
use rtdb_server::AppState;
use serde_json::json;

/// Build a multi-instance `AppState` with the given instance id, sharing `pool`.
async fn multi_instance_state(pool: sqlx::PgPool, instance_id: &str) -> std::sync::Arc<AppState> {
    let mut cfg = test_config();
    cfg.multi_instance = true;
    cfg.instance_id = Some(instance_id.to_string());
    AppState::new(pool, cfg, test_hot())
}

/// POST `/admin/create-db` with `{name}` and assert OK.
async fn create_db(addr: std::net::SocketAddr, db: &str) {
    let resp = admin_post(addr, "/admin/create-db", json!({"name": db})).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "create-db should succeed: {:?}",
        resp.text().await
    );
}

/// POST `/admin/push-schema` with `{db, schema}` for a minimal `widgets` table
/// (one indexed string field). Asserts OK.
async fn push_widgets_schema(addr: std::net::SocketAddr, db: &str) {
    let schema = json!({
        "tables": {
            "widgets": {
                "fields": {"label": {"type": "string"}},
                "indexes": [{"name": "by_label", "fields": ["label"]}]
            }
        }
    });
    let resp = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": schema}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "push-schema should succeed: {:?}",
        resp.text().await
    );
}

/// POST `{txn: {steps: [insert ...]}}` to `/admin/db/{db}/mutate` and return
/// the inserted doc's id. A single insert keeps the assertions unambiguous.
async fn insert_widget(addr: std::net::SocketAddr, db: &str, label: &str) -> String {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({
            "txn": {
                "steps": [
                    {"op": "insert", "table": "widgets", "doc": {"label": label}}
                ]
            }
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "insert mutate should succeed: {:?}",
        resp.text().await
    );
    let body: serde_json::Value = resp.json().await.expect("parse mutate response");
    body["results"][0]["id"]
        .as_str()
        .expect("insert returns doc id")
        .to_string()
}

/// Read `GET /admin/ops/recent?db=<db>&table=<table>&n=500` and return the ops
/// array (the ring contents for that db/table).
async fn ops_recent(addr: std::net::SocketAddr, db: &str, table: &str) -> Vec<serde_json::Value> {
    let resp = admin_get(
        addr,
        &format!("/admin/ops/recent?db={db}&table={table}&n=500"),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse ops/recent response");
    body["ops"].as_array().expect("ops array").clone()
}

/// A unique test database name. The cleanup worker (lazily spawned by
/// `wrap_test_db`) DROP-SCHEMAs these after the test on its own runtime, so we
/// do not need to tear them down inline.
fn fresh_name() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}

/// ENH-022 Stage 2: a write on replica A appears in replica B's op-feed ring via
/// Postgres LISTEN/NOTIFY, AND appears exactly once in A's own ring (self-
/// dedupe — the local `publish` plus the echoed `pg_notify` would otherwise
/// double-count). Both replicas share one Postgres pool, mirroring a real
/// multi-instance deploy.
#[tokio::test]
async fn cross_replica_op_feed_fan_out_and_self_dedupe() -> anyhow::Result<()> {
    let cfg = test_config();
    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool)
        .await
        .expect("bootstrap rtdb_auth");

    // Replica A and replica B: distinct AppStates, distinct instance ids, same
    // pool, both multi_instance. The LISTEN task is spawned inside AppState::new
    // because config.multi_instance is true.
    let state_a = multi_instance_state(pool.clone(), "replica-a").await;
    let state_b = multi_instance_state(pool.clone(), "replica-b").await;
    let addr_a = spawn_app(state_a.clone()).await;
    let addr_b = spawn_app(state_b.clone()).await;

    // Fresh test database + widgets schema. Create on replica A; both replicas
    // share the same Postgres so the schema is visible to B too. The `_guard`
    // must stay live for the test body so its `Drop` cleanup runs AFTER the test
    // (an unbound `wrap_test_db(...)` drops at end-of-statement, leaking the DB).
    let db_name = fresh_name();
    let _guard = crate::common::wrap_test_db(db_name.clone());
    create_db(addr_a, &db_name).await;
    push_widgets_schema(addr_a, &db_name).await;

    // Insert one widget on replica A. The committer publishes locally into A's
    // ring AND emits a pg_notify; replica B's listener mirrors it into B's ring.
    let doc_id = insert_widget(addr_a, &db_name, "gadget").await;

    // Poll replica B's op-feed ring until the write appears. NOTIFY delivery is
    // asynchronous; bound the wait at ~5s with 100ms sleeps.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut b_ops: Vec<serde_json::Value> = Vec::new();
    while std::time::Instant::now() < deadline {
        b_ops = ops_recent(addr_b, &db_name, "widgets").await;
        if b_ops.iter().any(|o| o["docId"] == doc_id) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let b_match_count = b_ops.iter().filter(|o| o["docId"] == doc_id).count();
    assert_eq!(
        b_match_count, 1,
        "replica B must see A's write exactly once in its op-feed ring \
         (got {b_match_count} occurrences in {b_ops:?})"
    );

    // Self-dedupe: replica A's own ring must contain the event exactly ONCE
    // (the local publish), not twice (local + echoed self-notification). The
    // instance-id tag on the NOTIFY payload is the dedupe mechanism.
    let a_ops = ops_recent(addr_a, &db_name, "widgets").await;
    let a_match_count = a_ops.iter().filter(|o| o["docId"] == doc_id).count();
    assert_eq!(
        a_match_count, 1,
        "replica A must see its own write exactly once (self-dedupe; \
         got {a_match_count} occurrences in {a_ops:?})"
    );

    Ok(())
}

/// ENH-022 Stage 2: when `multi_instance = false` (the default), the committer
/// must NOT emit `pg_notify`, and no LISTEN task is spawned. We assert this by
/// building two single-instance `AppState`s and verifying a write on A never
/// reaches B's ring within a bounded window — the negative of the cross-replica
/// test.
#[tokio::test]
async fn multi_instance_disabled_does_not_fan_out() -> anyhow::Result<()> {
    let cfg = test_config();
    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool)
        .await
        .expect("bootstrap rtdb_auth");

    // Both replicas default to multi_instance = false (test_config).
    let state_a = AppState::new(pool.clone(), test_config(), test_hot());
    let state_b = AppState::new(pool.clone(), test_config(), test_hot());
    let addr_a = spawn_app(state_a.clone()).await;
    let addr_b = spawn_app(state_b.clone()).await;

    let db_name = fresh_name();
    let _guard = crate::common::wrap_test_db(db_name.clone());
    create_db(addr_a, &db_name).await;
    push_widgets_schema(addr_a, &db_name).await;

    let doc_id = insert_widget(addr_a, &db_name, "gismo").await;

    // Wait long enough that a NOTIFY (had one been emitted) would have been
    // delivered and mirrored. 1s is well over the async-delivery latency
    // demonstrated in the positive test (~tens of ms).
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let b_ops = ops_recent(addr_b, &db_name, "widgets").await;
    let b_match_count = b_ops.iter().filter(|o| o["docId"] == doc_id).count();
    assert_eq!(
        b_match_count, 0,
        "with multi_instance=false, replica B must NOT see A's write \
         (no NOTIFY emitted; got {b_match_count} occurrences)"
    );

    Ok(())
}
