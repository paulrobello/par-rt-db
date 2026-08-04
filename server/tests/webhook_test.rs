//! Integration tests for the webhook delivery registry.
//!
//! Covers:
//! - **Enqueue + matching**: admin-create webhooks for a db, mutate a doc, and
//!   assert the committer's tap enqueued a `webhook_deliveries` row only for the
//!   webhook whose (table, events) filter matched the op. Verifies the payload
//!   JSON shape (camelCase, OpEvent-equivalent + source).
//! - **Delivery end-to-end**: a tiny axum receiver on an ephemeral port records
//!   POSTed bodies; after registering it as a webhook, mutating, and calling
//!   `webhook::drain_once`, the receiver has one POST matching the payload.
//! - **Admin CRUD**: create → list → delete → gone. A non-admin bearer gets 403;
//!   a missing/invalid bearer gets 401.

mod common;

use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Json, Router};
use common::{admin_delete, admin_get, admin_post, fresh_db, spawn_app, test_state_with_webhooks};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// POSTs `{txn: {steps}}` to `/admin/db/{db}/mutate` and returns `results`.
async fn mutate(addr: SocketAddr, db: &str, steps: Value) -> Value {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({ "txn": { "steps": steps } }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "mutate should succeed: {:?}",
        resp.text().await
    );
    let body: Value = resp.json().await.expect("parse mutate response");
    body["results"].clone()
}

/// POSTs a webhook create body and returns the assigned id.
async fn create_webhook(addr: SocketAddr, db: &str, body: Value) -> i64 {
    let resp = admin_post(addr, &format!("/admin/db/{db}/webhooks"), body).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "create webhook should succeed: {:?}",
        resp.text().await
    );
    let body: Value = resp.json().await.expect("parse create webhook response");
    body["id"].as_i64().expect("id is an integer")
}

/// Counts deliveries for `webhook_id`.
async fn delivery_count(pool: &sqlx::PgPool, webhook_id: i64) -> i64 {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rtdb.webhook_deliveries WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(pool)
            .await
            .expect("count deliveries");
    count
}

/// Fetches the newest payload for `webhook_id`.
async fn newest_payload(pool: &sqlx::PgPool, webhook_id: i64) -> Value {
    let payload: Value = sqlx::query_scalar(
        "SELECT payload FROM rtdb.webhook_deliveries \
         WHERE webhook_id = $1 ORDER BY id DESC LIMIT 1",
    )
    .bind(webhook_id)
    .fetch_one(pool)
    .await
    .expect("fetch payload");
    payload
}

// (a) Enqueue: a webhook matching the (table, event) gets one pending row with
// the right payload; webhooks filtered out by table or by event get none.
#[tokio::test]
async fn webhook_enqueue_matches_table_and_event_filters() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Three webhooks: A matches (projects/insert); B is filtered by table
    // (workItems/insert); C is filtered by event (projects/delete); D matches
    // everything (all-tables/*).
    let a = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/a", "table": "projects", "events": ["insert"]}),
    )
    .await;
    let b = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/b", "table": "workItems", "events": ["insert"]}),
    )
    .await;
    let c = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/c", "table": "projects", "events": ["delete"]}),
    )
    .await;
    let d = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/d", "events": ["*"]}),
    )
    .await;

    // Insert one project — should fire A and D only.
    mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {
            "name": "alpha", "status": "active", "tags": [], "updatedAt": 0
        }}]),
    )
    .await;

    assert_eq!(
        delivery_count(&pool, a).await,
        1,
        "matching webhook (projects/insert) gets a delivery"
    );
    assert_eq!(
        delivery_count(&pool, b).await,
        0,
        "table-filtered webhook (workItems) gets no delivery"
    );
    assert_eq!(
        delivery_count(&pool, c).await,
        0,
        "event-filtered webhook (delete) gets no delivery for an insert"
    );
    assert_eq!(
        delivery_count(&pool, d).await,
        1,
        "all-events/all-tables webhook gets a delivery"
    );

    // Payload shape for the matching webhook: camelCase OpEvent + source.
    let payload = newest_payload(&pool, a).await;
    assert_eq!(payload["db"], name.as_str());
    assert_eq!(payload["table"], "projects");
    assert_eq!(payload["kind"], "insert");
    assert_eq!(payload["source"], "mutate");
    assert!(
        payload["ts"].as_i64().is_some(),
        "payload carries a numeric ts"
    );
    assert!(
        payload["docId"].as_str().is_some(),
        "payload carries docId (camelCase)"
    );
    // Admin mutate passes owner = None.
    assert!(payload["owner"].is_null());
    // No snake_case leakage.
    assert!(payload.get("doc_id").is_none());
    assert!(payload.get("created_at").is_none());

    Ok(())
}

// (b) Delivery end-to-end: a real axum receiver on an ephemeral port records
// POSTed bodies; registering it as a webhook, mutating, and calling
// `webhook::drain_once` proves the worker path delivers exactly one POST whose
// body matches the OpEvent payload, and the delivery row is marked delivered.
async fn receive_hook(
    State(received): State<Arc<Mutex<Vec<Value>>>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    received.lock().await.push(body);
    Json(json!({"ok": true}))
}

#[tokio::test]
async fn webhook_delivery_end_to_end_posts_payload() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Tiny axum receiver recording every POST body.
    let received: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let receiver_app = Router::new()
        .route("/hook", axum::routing::post(receive_hook))
        .with_state(received.clone());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind receiver");
    let rx_addr = listener.local_addr().expect("receiver local_addr");
    tokio::spawn(axum::serve(listener, receiver_app).into_future());
    let url = format!("http://{rx_addr}/hook");

    let webhook_id = create_webhook(
        addr,
        &name,
        json!({"url": url, "table": "projects", "events": ["insert"]}),
    )
    .await;

    // Insert one project — enqueues one delivery.
    let results = mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {
            "name": "beta", "status": "active", "tags": [], "updatedAt": 0
        }}]),
    )
    .await;
    let doc_id = results[0]["id"]
        .as_str()
        .expect("insert returns id")
        .to_string();

    // Confirm enqueued before drain.
    assert_eq!(delivery_count(&pool, webhook_id).await, 1);

    let processed = rtdb_server::webhook::drain_once(&pool)
        .await
        .expect("drain_once");
    // `drain_once` processes every due row in the shared `webhook_deliveries`
    // table (other tests, running in parallel, may have enqueued their own), so
    // we only assert this test's row was among them — the receiver-side count
    // and the row-status check below are the precise per-webhook assertions.
    assert!(
        processed >= 1,
        "drain_once processed at least the one due delivery for this webhook"
    );

    // Receiver got exactly one POST matching the payload (other webhooks POST to
    // their own URLs, never this receiver, so its count is precisely ours).
    let got = received.lock().await.clone();
    assert_eq!(got.len(), 1, "receiver got exactly one POST");
    let body = &got[0];
    assert_eq!(body["db"], name.as_str());
    assert_eq!(body["table"], "projects");
    assert_eq!(body["kind"], "insert");
    assert_eq!(body["docId"], doc_id);
    assert_eq!(body["source"], "mutate");
    assert!(body["ts"].as_i64().is_some());

    // The delivery row is now marked delivered.
    let status: String =
        sqlx::query_scalar("SELECT status FROM rtdb.webhook_deliveries WHERE webhook_id = $1")
            .bind(webhook_id)
            .fetch_one(&pool)
            .await
            .expect("fetch status");
    assert_eq!(status, "delivered");

    Ok(())
}

// (c) Admin CRUD: create → list shows it → delete → gone. A non-admin bearer
// is forbidden (403); a missing bearer is unauthorized (401).
#[tokio::test]
async fn webhook_admin_crud_and_non_admin_forbidden() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Create.
    let id = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/hook", "table": "projects"}),
    )
    .await;
    // events omitted → defaults to ["*"].
    let list_body: Value = admin_get(addr, &format!("/admin/db/{name}/webhooks"))
        .await
        .json()
        .await
        .expect("parse list");
    let webhooks = list_body["webhooks"].as_array().expect("webhooks array");
    assert_eq!(webhooks.len(), 1);
    assert_eq!(webhooks[0]["id"], id);
    assert_eq!(webhooks[0]["url"], "http://example.com/hook");
    assert_eq!(webhooks[0]["table"], "projects");
    assert_eq!(webhooks[0]["events"], json!(["*"]));

    // Delete.
    let resp = admin_delete(addr, &format!("/admin/db/{name}/webhooks/{id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.expect("parse delete response");
    assert_eq!(body["ok"], json!(true));

    // Gone.
    let list_body: Value = admin_get(addr, &format!("/admin/db/{name}/webhooks"))
        .await
        .json()
        .await
        .expect("parse list");
    assert!(
        list_body["webhooks"].as_array().unwrap().is_empty(),
        "webhook list empty after delete"
    );

    // Non-admin bearer → 403. Mint a real user session that is NOT on the admin
    // allowlist; `authenticate_admin` resolves it then rejects with Forbidden.
    // The user id/email carry a UUID suffix because `rtdb_auth.users` enforces
    // uniqueness on both and is shared across the whole suite (a fixed value
    // would collide with prior runs).
    let suffix = uuid::Uuid::now_v7().simple();
    let session = common::mint_user_session(
        &state.pool,
        &format!("u-{suffix}"),
        &format!("u-{suffix}@example.com"),
    )
    .await;
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{name}/webhooks"))
        .header("Authorization", format!("Bearer {session}"))
        .json(&json!({"url": "http://example.com"}))
        .send()
        .await
        .expect("send non-admin request");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "non-admin bearer is forbidden"
    );

    // Missing bearer → 401.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{name}/webhooks"))
        .json(&json!({"url": "http://example.com"}))
        .send()
        .await
        .expect("send no-auth request");
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::UNAUTHORIZED,
        "missing bearer is unauthorized"
    );

    Ok(())
}
