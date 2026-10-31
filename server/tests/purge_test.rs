//! Admin purge route (`POST /admin/db/{db}/purge`): two-phase reclamation of
//! insertOnly rows by predicate. All application-data setup, counts, audit
//! assertions, and change-feed assertions go through the HTTP API (admin
//! query/mutate/purge/changes/audit routes) — no direct datastore access.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::db;
use rtdb_server::schema::SchemaDef;
use serde_json::{Value, json};

use crate::common::{
    admin_get, admin_post, mint_user_session, spawn_app, test_state, test_state_with_audit,
};

async fn fresh(state: &Arc<AppState>) -> (crate::common::TestDb, String) {
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
    assert_eq!(push.status(), reqwest::StatusCode::OK, "push-schema failed");
}

/// The base fixture: `events` (not insertOnly), `feed` (insertOnly).
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

/// A second fixture: parent + onDelete child tables for cascade tests, a
/// softDelete insertOnly table, and a ttl insertOnly table.
fn extended_schema() -> SchemaDef {
    serde_json::from_value(json!({"tables": {
        "feed": {
            "fields": {"kind": {"type": "string"}},
            "indexes": [{"name": "by_kind", "fields": ["kind"]}],
            "insertOnly": true
        },
        "links": {
            "fields": {"parent": {"type": "id", "table": "feed", "onDelete": "cascade"}},
            "indexes": [{"name": "by_parent", "fields": ["parent"]}],
            "insertOnly": true
        },
        "pins": {
            "fields": {"parent": {"type": "id", "table": "feed", "onDelete": "restrict"}},
            "indexes": [{"name": "by_parent", "fields": ["parent"]}],
            "insertOnly": true
        },
        "arch": {
            "fields": {"kind": {"type": "string"}},
            "insertOnly": true,
            "softDelete": true
        },
        "feedttl": {
            "fields": {"kind": {"type": "string"}, "expiresAt": {"type": "number"}},
            "indexes": [
                {"name": "by_kind", "fields": ["kind"]},
                {"name": "by_expires", "fields": ["expiresAt"]}
            ],
            "insertOnly": true,
            "ttl": {"field": "expiresAt", "defaultDurationMs": 86_400_000}
        }
    }}))
    .expect("parse extended schema")
}

async fn push_extended_schema(addr: std::net::SocketAddr, db: &str) {
    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": extended_schema()}),
    )
    .await;
    let status = push.status();
    let body = push.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "push extended failed: {body}"
    );
}

async fn insert_rows(
    addr: std::net::SocketAddr,
    db: &str,
    table: &str,
    docs: Vec<Value>,
) -> Vec<String> {
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
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(
        status,
        reqwest::StatusCode::OK,
        "seed mutate failed: {body}"
    );
    let body: Value = serde_json::from_str(&body).expect("mutate body json");
    body["results"]
        .as_array()
        .expect("results array")
        .iter()
        .map(|r| r["id"].as_str().expect("id").to_string())
        .collect()
}

async fn kind_rows(
    addr: std::net::SocketAddr,
    db: &str,
    table: &str,
    kinds: &[&str],
) -> Vec<String> {
    let docs = kinds.iter().map(|k| json!({"kind": k})).collect();
    insert_rows(addr, db, table, docs).await
}

/// Rows in `table` whose `kind` equals `kind`, via the admin count terminal.
async fn count_matching_kind(addr: std::net::SocketAddr, db: &str, table: &str, kind: &str) -> i64 {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/query"),
        json!({
            "query": {
                "table": table,
                "count": true,
                "filter": {"op": "eq", "field": "kind", "value": kind}
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "kind count failed");
    resp.json::<Value>().await.expect("count body")["result"]
        .as_i64()
        .expect("count result is a number")
}

/// Total rows in `table` via the admin query count terminal. When
/// `include_deleted` is set the count includes soft-deleted (stamped) rows.
async fn count_rows_total(
    addr: std::net::SocketAddr,
    db: &str,
    table: &str,
    include_deleted: bool,
) -> i64 {
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/query"),
        json!({"query": {"table": table, "count": true}, "includeDeleted": include_deleted}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "count query failed");
    resp.json::<Value>().await.expect("count body")["result"]
        .as_i64()
        .expect("count result is a number")
}

async fn purge(addr: std::net::SocketAddr, db: &str, body: Value) -> reqwest::Response {
    admin_post(addr, &format!("/admin/db/{db}/purge"), body).await
}

async fn preview_body(
    addr: std::net::SocketAddr,
    db: &str,
    table: &str,
    filter: Value,
    limit: Option<u32>,
) -> (i64, String, u32) {
    let mut body = json!({"table": table, "filter": filter});
    if let Some(l) = limit {
        body["limit"] = json!(l);
    }
    let resp = purge(addr, db, body).await;
    let status = resp.status();
    let raw = resp.text().await.unwrap_or_default();
    assert_eq!(status, reqwest::StatusCode::OK, "preview failed: {raw}");
    let body: Value = serde_json::from_str(&raw).expect("preview json");
    (
        body["matching"].as_i64().expect("matching"),
        body["confirmToken"]
            .as_str()
            .expect("confirmToken")
            .to_string(),
        body["limit"].as_u64().expect("limit") as u32,
    )
}

/// The current head seq of the db's durable change feed, via the admin route.
async fn change_head(addr: std::net::SocketAddr, db: &str) -> i64 {
    let resp = admin_get(addr, &format!("/admin/db/{db}/changes?since=0&limit=1")).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "changes head failed"
    );
    resp.json::<Value>().await.expect("changes body")["head"]
        .as_i64()
        .expect("head")
}

#[tokio::test]
async fn purge_requires_insert_only_table() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    insert_rows(
        addr,
        &db,
        "events",
        vec![
            json!({"kind": "keep", "n": 1}),
            json!({"kind": "keep", "n": 2}),
        ],
    )
    .await;
    assert_eq!(count_rows_total(addr, &db, "events", false).await, 2);

    let resp = purge(
        addr,
        &db,
        json!({"table": "events", "filter": {"op": "eq", "field": "kind", "value": "keep"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(count_rows_total(addr, &db, "events", false).await, 2);
}

#[tokio::test]
async fn purge_preview_is_read_only_then_wrong_token_refuses_then_confirm_deletes() {
    let state = test_state_with_audit().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    let mut kinds: Vec<&str> = vec!["wake"; 5];
    kinds.extend_from_slice(&["keep", "keep", "keep"]);
    kind_rows(addr, &db, "feed", &kinds).await;
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 8);

    // Preview returns the count and token and writes nothing.
    let (matching, token, limit) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    assert_eq!(matching, 5);
    assert_eq!(limit, 10_000);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 8);

    // Wrong token: refuses without deleting.
    let bad = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": "deadbeef"
        }),
    )
    .await;
    assert_eq!(bad.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 8);

    // Confirm deletes exactly the matching rows; non-matching rows survive.
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    let body = done.json::<Value>().await.expect("purge body");
    assert_eq!(body["deleted"], json!(5));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 3);
    assert_eq!(
        count_matching_kind(addr, &db, "feed", "wake").await,
        0,
        "no wake rows survive"
    );
    assert_eq!(
        count_matching_kind(addr, &db, "feed", "keep").await,
        3,
        "predicate survival: keep rows untouched"
    );
}

/// The purge's deletes are durably recorded: audit rows with source=purge
/// land through GET /admin/audit, and the durable change feed records the
/// delete ops with an advanced head.
#[tokio::test]
async fn purge_writes_audit_rows_and_change_feed_deletes() {
    let state = test_state_with_audit().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 4]).await;

    let head_before = change_head(addr, &db).await;
    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(4)
    );

    // Audit: written in the purge's own transaction, so the rows are already
    // committed when the response returns.
    let audit = admin_get(
        addr,
        &format!("/admin/audit?db={db}&source=purge&limit=100"),
    )
    .await;
    assert_eq!(audit.status(), reqwest::StatusCode::OK);
    let entries = audit.json::<Value>().await.expect("audit body")["entries"]
        .as_array()
        .expect("entries")
        .len() as i64;
    assert_eq!(entries, 4, "one audit row per purged doc");

    // Durable change feed: every delete op lands after the pre-purge head.
    let feed = admin_get(
        addr,
        &format!("/admin/db/{db}/changes?since={head_before}&limit=1000"),
    )
    .await;
    assert_eq!(feed.status(), reqwest::StatusCode::OK);
    let feed_body = feed.json::<Value>().await.expect("changes body");
    let ops = feed_body["ops"].as_array().expect("ops");
    assert_eq!(ops.len(), 4, "one change row per purged doc");
    for op in ops {
        assert_eq!(op["table"], json!("feed"));
        assert_eq!(op["kind"], json!("delete"));
    }
    assert!(feed_body["head"].as_i64().unwrap() > head_before);
}

#[tokio::test]
async fn purge_limit_reports_truncated() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 5]).await;

    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        Some(2),
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "limit": 2,
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    let body = done.json::<Value>().await.expect("purge body");
    assert_eq!(body["deleted"], json!(2));
    assert_eq!(body["truncated"], json!(true));
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 3);
}

/// Confirming with a different limit than the preview refuses (the limit is
/// part of the token identity), and no rows are deleted.
#[tokio::test]
async fn purge_confirm_with_changed_limit_refuses() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 4]).await;

    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        Some(4),
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "limit": 2,
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 4);
}

/// A confirm whose preview predates an insert that changes the matching set
/// refuses (stale token), then a re-preview + confirm succeeds.
#[tokio::test]
async fn purge_stale_token_after_insert_refuses() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 3]).await;
    let (_, stale_token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    kind_rows(addr, &db, "feed", &["wake"]).await;

    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": stale_token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 4);

    let (_, fresh_token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": fresh_token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(4)
    );
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 0);
}

/// Access control: unauthenticated 401; a data-plane machine token cannot
/// reach the route; an OAuth admin-allowlist session is rejected by the
/// key-only handler gate.
#[tokio::test]
async fn purge_access_control() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"]).await;

    let unauth = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/purge"))
        .json(&json!({"table": "feed"}))
        .send()
        .await
        .expect("send unauthenticated purge");
    assert_eq!(unauth.status(), reqwest::StatusCode::UNAUTHORIZED);

    let mint = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "purge-probe"}),
    )
    .await;
    assert_eq!(mint.status(), reqwest::StatusCode::OK);
    let token = mint.json::<Value>().await.expect("mint body")["token"]
        .as_str()
        .expect("token")
        .to_string();
    let machine = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/purge"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&json!({"table": "feed"}))
        .send()
        .await
        .expect("send machine-token purge");
    assert_eq!(machine.status(), reqwest::StatusCode::FORBIDDEN);

    // OAuth admin-allowlist session: seeded the same way dashboard_test does
    // (auth fixture rows are not reachable over HTTP). The email is unique
    // per run because the shared dev Postgres keeps the allowlist across
    // processes.
    let email = format!("purge-oauth-{}@example.com", uuid::Uuid::now_v7().simple());
    let session = mint_user_session(&state.pool, "purge-oauth-admin", &email).await;
    let oauth = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/purge"))
        .header("Authorization", format!("Bearer {session}"))
        .json(&json!({"table": "feed"}))
        .send()
        .await
        .expect("send oauth-admin purge");
    assert_eq!(oauth.status(), reqwest::StatusCode::FORBIDDEN);

    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 1);
}

#[tokio::test]
async fn purge_rejected_while_db_read_only() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 2]).await;

    let freeze = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/db/{db}/readonly"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&json!({"readOnly": true}))
        .send()
        .await
        .expect("send freeze");
    assert_eq!(freeze.status(), reqwest::StatusCode::OK, "freeze failed");
    let resp = purge(addr, &db, json!({"table": "feed"})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 2);
}

/// Quiesce fences the purge in both phases; unquiesce restores it.
#[tokio::test]
async fn purge_rejected_while_quiesced() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"]).await;

    let q = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/quiesce"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await
        .expect("send quiesce");
    assert_eq!(q.status(), reqwest::StatusCode::OK);
    let generation = q.json::<Value>().await.expect("quiesce body")["generation"]
        .as_i64()
        .expect("generation");

    let resp = purge(addr, &db, json!({"table": "feed"})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 1);

    let uq = reqwest::Client::new()
        .delete(format!("http://{addr}/admin/db/{db}/quiesce"))
        .header("Authorization", "Bearer test-admin-key")
        .header("x-rtdb-quiesce-generation", generation.to_string())
        .send()
        .await
        .expect("send unquiesce");
    assert_eq!(uq.status(), reqwest::StatusCode::OK, "unquiesce failed");

    let (matching, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    assert_eq!(matching, 1);
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 0);
}

/// With audit logging disabled the purge still runs (and still records audit
/// rows in-transaction); GET /admin/audit keeps its documented empty-while-
/// disabled contract. The stored audit rows themselves are not observable
/// over HTTP while the route is gated — noted as a coverage gap for the
/// primary session.
#[tokio::test]
async fn purge_runs_with_audit_disabled() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_schema(addr, &db).await;
    kind_rows(addr, &db, "feed", &["wake"; 2]).await;

    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(2)
    );
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 0);

    let audit = admin_get(addr, &format!("/admin/audit?db={db}&source=purge")).await;
    assert_eq!(audit.status(), reqwest::StatusCode::OK);
    let entries = audit.json::<Value>().await.expect("audit body")["entries"]
        .as_array()
        .expect("entries")
        .len();
    assert_eq!(
        entries, 0,
        "audit route stays empty while audit is disabled"
    );
}

/// force_hard: purge physically removes (not stamps) the rows it deletes on a
/// softDelete insertOnly table. Soft-deleted rows are invisible to the
/// predicate scan (FM-33 — by-query semantics match visible rows), so a
/// stamped row survives the purge and remains reaper territory.
#[tokio::test]
async fn purge_hard_deletes_soft_delete_table() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_extended_schema(addr, &db).await;
    let ids = kind_rows(addr, &db, "arch", &["gone", "gone"]).await;

    // Soft-delete one row via admin mutate (admin bypasses insertOnly).
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        json!({"txn": {"steps": [{"op": "delete", "table": "arch", "id": ids[0]}]}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "admin soft delete");
    // Live view: 1; including stamped rows: 2.
    assert_eq!(count_rows_total(addr, &db, "arch", false).await, 1);
    assert_eq!(count_rows_total(addr, &db, "arch", true).await, 2);

    let (_, token, _) = preview_body(addr, &db, "arch", json!(null), None).await;
    let done = purge(addr, &db, json!({"table": "arch", "confirm": token})).await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(1),
        "only live rows match the predicate (FM-33)"
    );
    assert_eq!(count_rows_total(addr, &db, "arch", false).await, 0);
    assert_eq!(
        count_rows_total(addr, &db, "arch", true).await,
        1,
        "the stamped row survives the purge (invisible to the scan) and is not re-stamped: \
         the deleted row was physically removed, not stamped"
    );
}

/// onDelete restrict: a live child blocks the purge and NOTHING is deleted
/// (single-transaction rollback).
#[tokio::test]
async fn purge_restrict_child_rolls_back_everything() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_extended_schema(addr, &db).await;
    let parents = kind_rows(addr, &db, "feed", &["p"]).await;
    insert_rows(addr, &db, "pins", vec![json!({"parent": parents[0]})]).await;

    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "p"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "p"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 1);
    assert_eq!(count_rows_total(addr, &db, "pins", false).await, 1);
}

/// onDelete cascade: purging the parent removes live child rows; the change
/// feed records both the parent and child deletes.
#[tokio::test]
async fn purge_cascades_children() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_extended_schema(addr, &db).await;
    let parents = kind_rows(addr, &db, "feed", &["p"]).await;
    insert_rows(
        addr,
        &db,
        "links",
        vec![json!({"parent": parents[0]}), json!({"parent": parents[0]})],
    )
    .await;

    let head_before = change_head(addr, &db).await;
    let (_, token, _) = preview_body(
        addr,
        &db,
        "feed",
        json!({"op": "eq", "field": "kind", "value": "p"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feed",
            "filter": {"op": "eq", "field": "kind", "value": "p"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(1)
    );
    assert_eq!(count_rows_total(addr, &db, "feed", false).await, 0);
    assert_eq!(count_rows_total(addr, &db, "links", false).await, 0);

    let feed = admin_get(
        addr,
        &format!("/admin/db/{db}/changes?since={head_before}&limit=1000"),
    )
    .await;
    let ops = feed.json::<Value>().await.expect("changes body")["ops"]
        .as_array()
        .expect("ops")
        .to_vec();
    assert_eq!(ops.len(), 3, "parent + 2 cascade children recorded");
}

/// A ttl-bearing insertOnly table purges by predicate before expiry; the
/// surviving rows are untouched and remain visible.
#[tokio::test]
async fn purge_ttl_table_before_after_counts() {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let (_guard, db) = fresh(&state).await;
    push_extended_schema(addr, &db).await;
    let future = rtdb_server::db::now_ms() + 86_400_000;
    insert_rows(
        addr,
        &db,
        "feedttl",
        vec![
            json!({"kind": "wake", "expiresAt": future}),
            json!({"kind": "wake", "expiresAt": future}),
            json!({"kind": "keep", "expiresAt": future}),
        ],
    )
    .await;
    assert_eq!(count_rows_total(addr, &db, "feedttl", false).await, 3);

    let (_, token, _) = preview_body(
        addr,
        &db,
        "feedttl",
        json!({"op": "eq", "field": "kind", "value": "wake"}),
        None,
    )
    .await;
    let done = purge(
        addr,
        &db,
        json!({
            "table": "feedttl",
            "filter": {"op": "eq", "field": "kind", "value": "wake"},
            "confirm": token
        }),
    )
    .await;
    assert_eq!(done.status(), reqwest::StatusCode::OK);
    assert_eq!(
        done.json::<Value>().await.expect("body")["deleted"],
        json!(2)
    );
    assert_eq!(count_rows_total(addr, &db, "feedttl", false).await, 1);
    assert_eq!(
        count_matching_kind(addr, &db, "feedttl", "keep").await,
        1,
        "ttl rows outside the predicate survive"
    );
}
