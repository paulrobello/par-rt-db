//! Admin purge route (`POST /admin/db/{db}/purge`): two-phase reclamation of
//! insertOnly rows by predicate — preview returns the matching count and a
//! confirm token; the confirmed call deletes up to `limit` rows through the
//! committer's `RunPurge` arm (four-tap publication, audit `source="purge"`).
//! The card's criteria pinned here: (1) the TTL sweep reclaims expired rows
//! (`ttl_test.rs` covers the reaper end to end; this file's sibling evidence
//! is the before/after counts below), (2) the admin purge removes matching
//! rows with a confirm token and writes audit rows.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::db;
use rtdb_server::schema::SchemaDef;
use serde_json::{Value, json};

use crate::common::{TestDb, admin_post, spawn_app, test_state, test_state_with_audit, wait_until};

fn schema() -> SchemaDef {
    serde_json::from_value(json!({"tables": {
        "events": {
            "fields": {"kind": {"type": "string"}, "n": {"type": "number"}},
            "indexes": [{"name": "by_kind", "fields": ["kind"]}]
        },
        "feed": {
            "fields": {"kind": {"type": "string"}},
            "indexes": [{"name": "by_kind", "fields": ["kind"]}],
            "insertOnly": true
        }
    }}))
    .expect("parse schema")
}

async fn fresh(state: &Arc<AppState>) -> (TestDb, String) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    (crate::common::wrap_test_db(name.clone()), name)
}

async fn push_schema(addr: std::net::SocketAddr, db: &str) {
    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": schema()}),
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

async fn insert_rows(
    addr: std::net::SocketAddr,
    db: &str,
    table: &str,
    kinds: &[&str],
) -> Vec<String> {
    let docs: Vec<Value> = kinds
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            // `events` declares `n`; `feed` declares only `kind`.
            if table == "feed" {
                json!({"kind": kind})
            } else {
                json!({"kind": kind, "n": i})
            }
        })
        .collect();
    let steps: Vec<Value> = docs
        .into_iter()
        .map(|doc| json!({"op": "insert", "table": table, "doc": doc}))
        .collect();
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({"txn": {"steps": steps}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "seed mutate failed");
    resp.json::<Value>().await.expect("mutate body")["results"]
        .as_array()
        .expect("results array")
        .iter()
        .map(|r| r["id"].as_str().expect("id").to_string())
        .collect()
}

async fn count_rows(pool: &sqlx::PgPool, db: &str, table: &str) -> i64 {
    // Physical names follow the ddl mapping: schema `db_{name}`, table
    // `t_{table}`. Identifiers are this file's own fixed literals ("feed"),
    // never request data — the AssertSqlSafe audit note covers the
    // interpolated identifiers.
    sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db}\".\"t_{table}\""
    )))
    .fetch_one(pool)
    .await
    .expect("count rows")
}

#[tokio::test]
async fn purge_requires_insert_only_table() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_db, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    let ids = insert_rows(addr, &db, "events", &["keep", "keep"]).await;
    assert_eq!(ids.len(), 2);

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({"table": "events", "filter": {"op": "eq", "field": "kind", "value": "keep"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    // Preview phase of a non-insertOnly table must not delete anything.
    assert_eq!(count_rows(&state.pool, &db, "events").await, 2);
}

#[tokio::test]
async fn purge_preview_wrong_token_then_confirm_deletes_matching_rows() {
    let state = test_state_with_audit().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    let mut kinds: Vec<&str> = vec!["wake"; 5];
    kinds.extend_from_slice(&["keep", "keep", "keep"]);
    insert_rows(addr, &db, "feed", &kinds).await;
    assert_eq!(count_rows(&pool, &db, "feed").await, 8);

    // Preview: returns the matching count and a token, deletes nothing.
    let preview = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({"table": "feed", "filter": {"op": "eq", "field": "kind", "value": "wake"}}),
    )
    .await;
    assert_eq!(preview.status(), reqwest::StatusCode::OK);
    let body = preview.json::<Value>().await.expect("preview body");
    assert_eq!(body["matching"], json!(5));
    let token = body["confirmToken"]
        .as_str()
        .expect("confirmToken")
        .to_string();

    // Wrong token: stale/mismatched confirm refuses without deleting.
    let bad = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": "deadbeef"
        }),
    )
    .await;
    assert_eq!(bad.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows(&pool, &db, "feed").await, 8);

    // Confirmed purge deletes exactly the matching rows.
    let purge = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(purge.status(), reqwest::StatusCode::OK);
    let body = purge.json::<Value>().await.expect("purge body");
    assert_eq!(body["deleted"], json!(5));
    assert_eq!(body["truncated"], json!(false));
    let after = count_rows(&pool, &db, "feed").await;
    assert_eq!(
        after, 3,
        "before=8 after=3: only the non-matching rows remain"
    );

    // The durable write is auditable: one delete row per purged doc with
    // source = "purge" (four-tap publication, best-effort post-commit —
    // same discipline as the TTL reaper's taps).
    let audit_landed = wait_until(std::time::Duration::from_secs(10), || async {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1 AND source = 'purge'",
        )
        .bind(db.as_str())
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
        count == 5
    })
    .await;
    assert!(audit_landed, "audit rows with source='purge' did not land");
}

#[tokio::test]
async fn purge_limit_reports_truncated() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    insert_rows(addr, &db, "feed", &["wake"; 5]).await;

    // Preview + confirm with limit=2 deletes exactly 2 and reports more.
    let preview = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({"table": "feed", "filter": {"op": "eq", "field": "kind", "value": "wake"}, "limit": 2}),
    )
    .await;
    assert_eq!(preview.status(), reqwest::StatusCode::OK);
    let token = preview.json::<Value>().await.expect("preview body")["confirmToken"]
        .as_str()
        .expect("confirmToken")
        .to_string();
    let purge = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "limit": 2,
            "confirm": token
        }),
    )
    .await;
    assert_eq!(purge.status(), reqwest::StatusCode::OK);
    let body = purge.json::<Value>().await.expect("purge body");
    assert_eq!(body["deleted"], json!(2));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(count_rows(&state.pool, &db, "feed").await, 3);
}

#[tokio::test]
async fn purge_requires_admin() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    insert_rows(addr, &db, "feed", &["wake"]).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/purge"))
        .json(&json!({"table": "feed"}))
        .send()
        .await
        .expect("send unauthenticated purge");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(count_rows(&state.pool, &db, "feed").await, 1);
}

#[tokio::test]
async fn purge_rejected_while_db_read_only() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    insert_rows(addr, &db, "feed", &["wake"; 2]).await;

    let freeze = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/db/{db}/readonly"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&json!({"readOnly": true}))
        .send()
        .await
        .expect("send freeze");
    assert_eq!(freeze.status(), reqwest::StatusCode::OK, "freeze failed");
    let preview = admin_post(
        addr,
        &format!("/admin/db/{db}/purge"),
        json!({"table": "feed"}),
    )
    .await;
    assert_eq!(preview.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows(&pool, &db, "feed").await, 2);
}
