//! Integration tests for cross-instance op-feed fan-out via Postgres
//! LISTEN/NOTIFY (ENH-022 Stage 2).
//!
//! These tests build two `AppState`s (replica A + replica B) sharing one
//! Postgres pool, both with `multi_instance = true` and distinct instance ids.
//! A write committed on replica A emits a `pg_notify('rtdb_ops', …)` at the
//! committer tap site; replica B's LISTEN task mirrors it into its local
//! op-feed ring. The self-dedupe contract (instance-id tag) keeps A from
//! double-publishing its own writes into its own ring.

use crate::common::{admin_get, admin_post, spawn_app, test_config, test_hot, wait_until};
use rtdb_server::AppState;
use serde_json::json;

/// Build a multi-instance `AppState` with the given instance id, sharing `pool`.
async fn multi_instance_state(pool: sqlx::PgPool, instance_id: &str) -> std::sync::Arc<AppState> {
    let mut cfg = test_config();
    cfg.multi_instance.enabled = true;
    cfg.multi_instance.instance_id = Some(instance_id.to_string());
    AppState::new(pool, cfg, test_hot())
}

/// Like [`multi_instance_state`], but with `max_affected_docs` raised so a
/// large single-transaction batch write (ARC-006) isn't rejected by the
/// admin-mutate guardrail (`RTDB_MAX_AFFECTED_DOCS`, default 100).
async fn multi_instance_state_with_cap(
    pool: sqlx::PgPool,
    instance_id: &str,
    max_affected_docs: usize,
) -> std::sync::Arc<AppState> {
    let mut cfg = test_config();
    cfg.multi_instance.enabled = true;
    cfg.multi_instance.instance_id = Some(instance_id.to_string());
    cfg.max_affected_docs = max_affected_docs;
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

/// POST a mutate txn with `n` distinct `widgets` inserts in ONE transaction
/// (ARC-006's "1000-row write" case, scaled down for test runtime) and return
/// OK/err.
async fn insert_widgets_batch(addr: std::net::SocketAddr, db: &str, n: usize) {
    let steps: Vec<serde_json::Value> = (0..n)
        .map(|i| json!({"op": "insert", "table": "widgets", "doc": {"label": format!("w{i}")}}))
        .collect();
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({"txn": {"steps": steps}}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "batch insert mutate should succeed: {:?}",
        resp.text().await
    );
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
    // asynchronous; bound the wait at ~5s.
    let b_ops: std::cell::RefCell<Vec<serde_json::Value>> = std::cell::RefCell::new(Vec::new());
    wait_until(std::time::Duration::from_secs(5), || async {
        let ops = ops_recent(addr_b, &db_name, "widgets").await;
        let found = ops.iter().any(|o| o["docId"] == doc_id);
        *b_ops.borrow_mut() = ops;
        found
    })
    .await;
    let b_ops = b_ops.into_inner();
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

/// ARC-006: a large multi-op write must batch its `pg_notify`s (one per
/// `OP_NOTIFY_CHUNK_LIMIT`-sized chunk of ops, not one per op), and every op
/// must still reach a peer replica's op-feed ring despite the batching.
///
/// The test opens its own raw `PgListener` on `rtdb_ops` (a fourth party,
/// distinct from replica A/B) to count actual NOTIFY messages, since the
/// admin `/ops/recent` API only exposes the *decoded* per-op events, not how
/// many wire notifications produced them. Postgres NOTIFY is scoped to the
/// physical Postgres database, which every test in this binary shares, so
/// messages are filtered to this test's unique `db_name` to avoid
/// cross-test contamination.
#[tokio::test]
async fn large_batch_write_coalesces_notifications() -> anyhow::Result<()> {
    let cfg = test_config();
    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool)
        .await
        .expect("bootstrap rtdb_auth");

    const N: usize = 250;
    let state_a = multi_instance_state_with_cap(pool.clone(), "replica-a", N).await;
    let state_b = multi_instance_state(pool.clone(), "replica-b").await;
    let addr_a = spawn_app(state_a.clone()).await;
    let addr_b = spawn_app(state_b.clone()).await;

    let db_name = fresh_name();
    let _guard = crate::common::wrap_test_db(db_name.clone());
    create_db(addr_a, &db_name).await;
    push_widgets_schema(addr_a, &db_name).await;

    // A raw listener on the same channel the server uses, so the test can
    // count wire-level pg_notify messages independent of how the listener
    // task decodes them.
    let mut raw_listener = sqlx::postgres::PgListener::connect_with(&pool)
        .await
        .expect("connect raw notify listener");
    raw_listener
        .listen_all([rtdb_server::notify::OP_FEED_CHANNEL])
        .await
        .expect("listen on rtdb_ops");

    insert_widgets_batch(addr_a, &db_name, N).await;

    // Collect every notification scoped to this test's db within a bounded
    // window, tracking both the number of wire messages and the total op
    // count they carry (a message may be a single object or a batch array).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut message_count = 0usize;
    let mut op_count = 0usize;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let notif = match tokio::time::timeout(remaining, raw_listener.recv()).await {
            Ok(Ok(n)) => n,
            _ => break,
        };
        let payload: serde_json::Value =
            serde_json::from_str(notif.payload()).expect("valid JSON notify payload");
        let items: Vec<&serde_json::Value> = match &payload {
            serde_json::Value::Array(items) => items.iter().collect(),
            other => vec![other],
        };
        let matching: Vec<&&serde_json::Value> =
            items.iter().filter(|v| v["db"] == db_name).collect();
        if matching.is_empty() {
            continue; // notification from a concurrently-running test's db
        }
        message_count += 1;
        op_count += matching.len();
        if op_count >= N {
            break;
        }
    }

    assert_eq!(
        op_count, N,
        "every op must reach the raw listener across the batched notifications"
    );
    assert!(
        message_count < N / 4,
        "a {N}-op write must coalesce into far fewer than {N} pg_notify \
         messages (ARC-006); got {message_count} messages for {op_count} ops"
    );

    // And the decoded side: replica B's op-feed ring must contain every op
    // (batching must not drop or truncate anything the listener decodes).
    let b_ops: std::cell::RefCell<Vec<serde_json::Value>> = std::cell::RefCell::new(Vec::new());
    wait_until(std::time::Duration::from_secs(5), || async {
        let ops = ops_recent(addr_b, &db_name, "widgets").await;
        let done = ops.len() >= N;
        *b_ops.borrow_mut() = ops;
        done
    })
    .await;
    let b_ops = b_ops.into_inner();
    assert_eq!(
        b_ops.len(),
        N,
        "replica B's op-feed ring must contain every op from the batched write"
    );

    Ok(())
}
