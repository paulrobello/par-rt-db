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
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::response::IntoResponse;
use axum::{Json, Router};
use common::{
    admin_delete, admin_get, admin_post, admin_put, fresh_db, spawn_app, test_state_with_webhooks,
};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// One captured delivery: the raw body bytes plus the `X-Rtdb-Signature` header
/// value (if present). Captured by `receive_signed_hook` for the SEC-115 test.
#[derive(Clone)]
struct CapturedDelivery {
    body: Vec<u8>,
    signature: Option<String>,
}

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

// (a.2) Disabled webhooks do not enqueue deliveries; enabled ones on the same
// db/table/event still do. The `enabled` flag round-trips on create + list.
#[tokio::test]
async fn webhook_disabled_does_not_enqueue() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Two webhooks with identical (table, event) filters; one is created
    // disabled, the other is created enabled (the default when omitted).
    let disabled_id = create_webhook(
        addr,
        &name,
        json!({
            "url": "http://example.com/disabled",
            "table": "projects",
            "events": ["insert"],
            "enabled": false
        }),
    )
    .await;
    let enabled_id = create_webhook(
        addr,
        &name,
        json!({
            "url": "http://example.com/enabled",
            "table": "projects",
            "events": ["insert"]
        }),
    )
    .await;

    // The `enabled` flag round-trips on list.
    let list_body: Value = admin_get(addr, &format!("/admin/db/{name}/webhooks"))
        .await
        .json()
        .await
        .expect("parse list");
    let webhooks = list_body["webhooks"].as_array().expect("webhooks array");
    let by_id = |id: i64| -> &Value {
        webhooks
            .iter()
            .find(|w| w["id"].as_i64() == Some(id))
            .unwrap_or_else(|| panic!("webhook {id} missing from list"))
    };
    assert_eq!(by_id(disabled_id)["enabled"], json!(false));
    assert_eq!(by_id(enabled_id)["enabled"], json!(true));

    // Insert one project — fires the enabled webhook only.
    mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {
            "name": "alpha", "status": "active", "tags": [], "updatedAt": 0
        }}]),
    )
    .await;

    assert_eq!(
        delivery_count(&pool, disabled_id).await,
        0,
        "disabled webhook enqueues no delivery"
    );
    assert_eq!(
        delivery_count(&pool, enabled_id).await,
        1,
        "enabled webhook still enqueues on the same op"
    );

    // Flipping the disabled webhook back on (directly, since the PUT edit
    // endpoint lands in Task 2) lets subsequent ops enqueue for it too — this
    // guards against the regression where the enqueue filter reads the wrong
    // column or the column defaulted incorrectly.
    sqlx::query("UPDATE rtdb.webhooks SET enabled = true WHERE id = $1")
        .bind(disabled_id)
        .execute(&pool)
        .await
        .expect("re-enable webhook");
    mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {
            "name": "beta", "status": "active", "tags": [], "updatedAt": 0
        }}]),
    )
    .await;
    assert_eq!(
        delivery_count(&pool, disabled_id).await,
        1,
        "re-enabled webhook enqueues on a fresh matching op"
    );

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

    // Drain the shared outbox until *our* delivery is delivered. A single
    // `drain_once` is not enough under parallel test load: it pulls a bounded
    // batch ordered by `next_attempt` from the shared `webhook_deliveries`
    // table, so rows other tests enqueued earlier can fill the batch and defer
    // ours to a later pass — the receiver then sees zero POSTs even though
    // `drain_once` returned a nonzero count. Loop (bounded by a deadline) until
    // our row reaches `delivered`; the receiver-side and body assertions below
    // remain the precise per-webhook checks. The deadline is generous because
    // under full-suite parallel test load the shared delivery worker + Postgres
    // contend, and this test cares about CORRECTNESS (exactly one matching POST),
    // not speed.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        rtdb_server::webhook::drain_once(&pool)
            .await
            .expect("drain_once");
        let delivered: bool = sqlx::query_scalar(
            "SELECT status = 'delivered' FROM rtdb.webhook_deliveries WHERE webhook_id = $1",
        )
        .bind(webhook_id)
        .fetch_one(&pool)
        .await
        .expect("fetch delivery status");
        if delivered || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

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
    // `enabled` defaults to true when omitted on create and round-trips on list.
    assert_eq!(webhooks[0]["enabled"], json!(true));

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

// (d) PUT edit round-trip: present fields are updated, absent fields are left
// alone, `table: null` clears the table filter to all-tables, and a PUT on a
// non-existent id is a 404. Covers both the dynamic-SET path (multiple fields)
// and the nested `Option<Option<String>>` table semantics.
async fn list_webhook_by_id(addr: SocketAddr, db: &str, id: i64) -> Value {
    let body: Value = admin_get(addr, &format!("/admin/db/{db}/webhooks"))
        .await
        .json()
        .await
        .expect("parse list");
    body["webhooks"]
        .as_array()
        .expect("webhooks array")
        .iter()
        .find(|w| w["id"].as_i64() == Some(id))
        .cloned()
        .unwrap_or_else(|| panic!("webhook {id} missing from list"))
}

#[tokio::test]
async fn webhook_put_edits_fields_and_clears_table() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Start: all-tables, events ["*"], enabled (default).
    let id = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/hook", "events": ["insert"]}),
    )
    .await;
    let before = list_webhook_by_id(addr, &name, id).await;
    assert_eq!(before["url"], "http://example.com/hook");
    assert_eq!(before["table"], json!(null), "table omitted → all-tables");
    assert_eq!(before["events"], json!(["insert"]));
    assert_eq!(before["enabled"], json!(true));

    // PUT: change url + events + toggle enabled false. table absent → unchanged.
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}"),
        json!({
            "url": "http://example.com/updated",
            "events": ["insert", "patch"],
            "enabled": false
        }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "edit should succeed"
    );
    let updated: Value = resp.json().await.expect("parse edit response");
    assert_eq!(updated["id"], id);
    assert_eq!(updated["url"], "http://example.com/updated");
    assert_eq!(updated["events"], json!(["insert", "patch"]));
    assert_eq!(updated["enabled"], json!(false));
    assert_eq!(updated["table"], json!(null), "table absent → unchanged");

    // The list view agrees with the returned row.
    let listed = list_webhook_by_id(addr, &name, id).await;
    assert_eq!(listed["url"], updated["url"]);
    assert_eq!(listed["events"], updated["events"]);
    assert_eq!(listed["enabled"], updated["enabled"]);

    // PUT: set table to a specific table.
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}"),
        json!({"table": "projects"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: Value = resp.json().await.expect("parse edit response");
    assert_eq!(updated["table"], "projects");

    // PUT: `table: null` clears the filter back to all-tables.
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}"),
        json!({"table": null}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: Value = resp.json().await.expect("parse edit response");
    assert_eq!(updated["table"], json!(null), "table:null → all-tables");

    // PUT with an empty body is a no-op edit that returns the current row
    // (the helper short-circuits to a SELECT rather than synthesizing empty
    // SET SQL).
    let resp = admin_put(addr, &format!("/admin/db/{name}/webhooks/{id}"), json!({})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: Value = resp.json().await.expect("parse edit response");
    assert_eq!(updated["id"], id);
    assert_eq!(updated["url"], "http://example.com/updated");

    // PUT on a non-existent id → 404 (the WHERE id AND db scope matches no row).
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/999999"),
        json!({"url": "http://example.com/x"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // PUT with an unknown event name → 400 (validation runs before the update).
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}"),
        json!({"events": ["bogus"]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // PUT scoping is per-db: the same id on a different db is a 404, proving
    // the `WHERE id AND db` clause prevents a cross-db edit.
    let other = fresh_db(&state).await;
    let resp = admin_put(
        addr,
        &format!("/admin/db/{other}/webhooks/{id}"),
        json!({"url": "http://example.com/cross"}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::NOT_FOUND,
        "webhook id scoped to its db"
    );

    Ok(())
}

// (e) GET deliveries: seeded outbox rows are returned newest-first, the
// `status` filter excludes non-matching rows, and limit/offset page. The row
// shape is camelCase (`nextAttempt`/`lastError`) with the raw payload passed
// through. Seeds rows directly to avoid coupling this endpoint test to the
// enqueue/drain machinery exercised above.
#[tokio::test]
async fn webhook_deliveries_filter_sort_and_paginate() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let id = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/hook", "events": ["*"]}),
    )
    .await;

    // Seed three deliveries with distinct statuses and staggered next_attempt
    // so the ORDER BY (next_attempt DESC, id DESC) is observable. ids ascend
    // with insertion order; next_attempt ascends so the newest (largest ts)
    // is first.
    let payload = json!({"db": name, "docId": "d1", "kind": "insert"});
    let mut next = 1_700_000_000_000_i64;
    for (status, attempts, last_err) in [
        ("pending", 0_i32, None::<&str>),
        ("retrying", 2, Some("HTTP 503")),
        ("delivered", 1, None),
    ] {
        sqlx::query(
            "INSERT INTO rtdb.webhook_deliveries \
             (webhook_id, payload, attempts, next_attempt, status, last_error) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(id)
        .bind(&payload)
        .bind(attempts)
        .bind(next)
        .bind(status)
        .bind(last_err)
        .execute(&pool)
        .await
        .expect("seed delivery");
        next += 5_000;
    }

    // No filter: all three rows, newest next_attempt first.
    let body: Value = admin_get(addr, &format!("/admin/db/{name}/webhooks/{id}/deliveries"))
        .await
        .json()
        .await
        .expect("parse deliveries");
    let deliveries = body["deliveries"].as_array().expect("deliveries array");
    assert_eq!(deliveries.len(), 3);
    // Ordering: descending next_attempt (third-seeded "delivered" has the
    // largest ts).
    assert_eq!(deliveries[0]["status"], "delivered");
    assert_eq!(deliveries[1]["status"], "retrying");
    assert_eq!(deliveries[2]["status"], "pending");
    // Row shape: camelCase keys, raw payload passed through.
    let retrying = &deliveries[1];
    assert_eq!(retrying["attempts"], 2);
    assert_eq!(retrying["status"], "retrying");
    assert!(
        retrying["nextAttempt"].as_i64().is_some(),
        "camelCase nextAttempt"
    );
    assert_eq!(retrying["lastError"], "HTTP 503");
    assert_eq!(retrying["payload"]["docId"], "d1", "payload passed through");
    // No snake_case leakage.
    assert!(retrying.get("next_attempt").is_none());
    assert!(retrying.get("last_error").is_none());

    // Status filter: only retrying rows.
    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}/deliveries?status=retrying"),
    )
    .await
    .json()
    .await
    .expect("parse filtered deliveries");
    let deliveries = body["deliveries"].as_array().unwrap();
    assert_eq!(deliveries.len(), 1, "status filter excludes non-matching");
    assert_eq!(deliveries[0]["status"], "retrying");

    // Status filter matching nothing → empty list (not an error).
    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}/deliveries?status=failed"),
    )
    .await
    .json()
    .await
    .expect("parse empty deliveries");
    assert!(
        body["deliveries"].as_array().unwrap().is_empty(),
        "no matching status → empty"
    );

    // Pagination: limit=1 offset=0 → newest one; offset=1 → next; offset=3 →
    // empty. Proves LIMIT/OFFSET apply after the ORDER BY.
    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}/deliveries?limit=1&offset=0"),
    )
    .await
    .json()
    .await
    .expect("parse page 1");
    let page1 = body["deliveries"].as_array().unwrap();
    assert_eq!(page1.len(), 1);
    assert_eq!(page1[0]["status"], "delivered");

    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}/deliveries?limit=1&offset=1"),
    )
    .await
    .json()
    .await
    .expect("parse page 2");
    let page2 = body["deliveries"].as_array().unwrap();
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0]["status"], "retrying");

    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}/deliveries?limit=1&offset=3"),
    )
    .await
    .json()
    .await
    .expect("parse past-end page");
    assert!(
        body["deliveries"].as_array().unwrap().is_empty(),
        "offset past end → empty"
    );

    // Deliveries are scoped by webhook_id: a different webhook sees none.
    let other = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/other", "events": ["*"]}),
    )
    .await;
    let body: Value = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/{other}/deliveries"),
    )
    .await
    .json()
    .await
    .expect("parse other deliveries");
    assert!(
        body["deliveries"].as_array().unwrap().is_empty(),
        "deliveries scoped by webhook_id"
    );

    // Non-numeric id → 400.
    let resp = admin_get(
        addr,
        &format!("/admin/db/{name}/webhooks/notanint/deliveries"),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

// SEC-134: delete_webhook and fetch_deliveries must scope on (id, db) the same
// way edit_webhook does, so a caller can't act on another database's webhook by
// guessing its numeric id. Both functions are exercised through their admin
// routes: a cross-db DELETE reports ok:false and leaves the row intact, and a
// cross-db deliveries listing sees nothing.
#[tokio::test]
async fn webhook_delete_and_deliveries_are_scoped_per_db() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let other = fresh_db(&state).await;

    let id = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/hook", "events": ["*"]}),
    )
    .await;

    // Seed one delivery so a cross-db listing that *ignored* the db scope would
    // return it (and fail this test).
    sqlx::query(
        "INSERT INTO rtdb.webhook_deliveries \
         (webhook_id, payload, attempts, next_attempt, status) \
         VALUES ($1, $2, 0, $3, 'pending')",
    )
    .bind(id)
    .bind(serde_json::json!({"db": name, "docId": "x", "kind": "insert"}))
    .bind(1_700_000_000_000_i64)
    .execute(&state.pool)
    .await
    .expect("seed delivery");

    // Cross-db deliveries listing: the webhook belongs to `name`, so `other`
    // sees none (the JOIN on w.db = $2 excludes it).
    let body: Value = admin_get(addr, &format!("/admin/db/{other}/webhooks/{id}/deliveries"))
        .await
        .json()
        .await
        .expect("parse cross-db deliveries");
    assert!(
        body["deliveries"].as_array().unwrap().is_empty(),
        "cross-db deliveries must be scoped out (SEC-134)"
    );

    // Cross-db delete: ok:false because the (id, other) pair matches no row.
    let body: Value = admin_delete(addr, &format!("/admin/db/{other}/webhooks/{id}"))
        .await
        .json()
        .await
        .expect("parse cross-db delete");
    assert_eq!(body["ok"], false, "cross-db delete must not remove the row");

    // The webhook survives: deleting it from its own db succeeds.
    let body: Value = admin_delete(addr, &format!("/admin/db/{name}/webhooks/{id}"))
        .await
        .json()
        .await
        .expect("parse same-db delete");
    assert_eq!(body["ok"], true, "same-db delete removes the row");

    Ok(())
}

// (f) SEC-115: each delivery carries `X-Rtdb-Signature: t=<ts>,v1=<hmac>` and
// the tag verifies against the webhook's secret over the exact body bytes the
// receiver observed. Also asserts the secret is generated server-side on
// create (present in the list response), is not the literal a client might
// have supplied, and rotates on `rotateSecret: true`.
async fn receive_signed_hook(
    State(captured): State<Arc<Mutex<Vec<CapturedDelivery>>>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let signature = headers
        .get("x-rtdb-signature")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    captured.lock().await.push(CapturedDelivery {
        body: body.to_vec(),
        signature,
    });
    Json(json!({"ok": true}))
}

#[tokio::test]
async fn webhook_delivery_carries_verifiable_signature() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let captured: Arc<Mutex<Vec<CapturedDelivery>>> = Arc::new(Mutex::new(Vec::new()));
    let receiver_app = Router::new()
        .route("/hook", axum::routing::post(receive_signed_hook))
        .with_state(captured.clone());
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

    // The webhook's secret is generated server-side and visible in the list
    // response so an operator can copy it to the receiver.
    let list_body: Value = admin_get(addr, &format!("/admin/db/{name}/webhooks"))
        .await
        .json()
        .await
        .expect("parse list");
    let secret = list_body["webhooks"][0]["secret"]
        .as_str()
        .expect("secret present on create")
        .to_string();
    assert_eq!(secret.len(), 64, "secret is 64 hex chars (256 bits)");
    assert!(
        secret.chars().all(|c| c.is_ascii_hexdigit()),
        "secret is hex: {secret}"
    );

    // Mutate to enqueue one delivery.
    let results = mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {
            "name": "sig", "status": "active", "tags": [], "updatedAt": 0
        }}]),
    )
    .await;
    let doc_id = results[0]["id"]
        .as_str()
        .expect("insert returns id")
        .to_string();
    assert_eq!(delivery_count(&pool, webhook_id).await, 1);

    // Drain until OUR delivery reaches `delivered` (matches the e2e test's
    // bounded-loop pattern: a single `drain_once` can defer our row under
    // parallel test load because the shared outbox is drained in bounded
    // batches ordered by `next_attempt`).
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        rtdb_server::webhook::drain_once(&pool)
            .await
            .expect("drain_once");
        let delivered: bool = sqlx::query_scalar(
            "SELECT status = 'delivered' FROM rtdb.webhook_deliveries WHERE webhook_id = $1",
        )
        .bind(webhook_id)
        .fetch_one(&pool)
        .await
        .expect("fetch delivery status");
        if delivered || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // Find OUR captured POST by matching the payload's docId. Under parallel
    // test load a stale retrying webhook from another test whose URL happened
    // to land on our recycled ephemeral port can produce an extra POST to our
    // receiver — filtering by docId keeps the assertion precise about *our*
    // delivery without coupling to total received count.
    let cap = {
        let got = captured.lock().await.clone();
        let mut ours: Vec<CapturedDelivery> = Vec::new();
        for entry in got.iter() {
            if let Ok(parsed) = serde_json::from_slice::<Value>(&entry.body)
                && parsed["docId"].as_str() == Some(doc_id.as_str())
            {
                ours.push(entry.clone());
            }
        }
        ours.pop()
            .expect("our delivery reached the receiver and was captured")
    };

    // Header present and well-formed: `t=<ts>,v1=<hex>`.
    let sig_header = cap
        .signature
        .as_ref()
        .expect("X-Rtdb-Signature header present")
        .clone();
    let (ts_str, v1) = sig_header
        .split_once(',')
        .and_then(|(t, v)| {
            Some((
                t.strip_prefix("t=")?.to_string(),
                v.strip_prefix("v1=")?.to_string(),
            ))
        })
        .unwrap_or_else(|| panic!("signature header malformed: {sig_header}"));
    let ts: i64 = ts_str
        .parse()
        .unwrap_or_else(|_| panic!("timestamp numeric: {ts_str}"));

    // The signature verifies against the webhook's secret over the EXACT body
    // bytes the receiver observed — proving the receiver can authenticate the
    // delivery and that no bytes were mangled in flight.
    assert!(
        rtdb_server::webhook::verify_signature(&secret, ts, &cap.body, &v1),
        "signature verifies against the webhook secret and observed body"
    );
    // A tampered body must NOT verify (defense against forgery / capture-replay
    // without the secret).
    assert!(
        !rtdb_server::webhook::verify_signature(&secret, ts, b"tampered", &v1),
        "tampered body must not verify"
    );

    Ok(())
}

// (g) SEC-115 rotation: `PUT {rotateSecret: true}` generates a new secret,
// different from the prior one. The secret value itself is never accepted from
// the client (no `secret` field on the request body).
#[tokio::test]
async fn webhook_rotate_secret_generates_new_value() -> anyhow::Result<()> {
    let state = test_state_with_webhooks().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let id = create_webhook(
        addr,
        &name,
        json!({"url": "http://example.com/hook", "events": ["*"]}),
    )
    .await;
    let before = list_webhook_by_id(addr, &name, id).await;
    let secret_before = before["secret"]
        .as_str()
        .expect("secret present on create")
        .to_string();
    assert_eq!(secret_before.len(), 64);

    // Rotate.
    let resp = admin_put(
        addr,
        &format!("/admin/db/{name}/webhooks/{id}"),
        json!({"rotateSecret": true}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: Value = resp.json().await.expect("parse edit response");
    let secret_after = updated["secret"]
        .as_str()
        .expect("secret present after rotation")
        .to_string();
    assert_eq!(secret_after.len(), 64);
    assert_ne!(
        secret_after, secret_before,
        "rotation produces a new secret value"
    );

    // The list view agrees with the rotated value.
    let listed = list_webhook_by_id(addr, &name, id).await;
    assert_eq!(listed["secret"], json!(secret_after));

    Ok(())
}
