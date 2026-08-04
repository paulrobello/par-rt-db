//! Integration tests for the durable audit log (`rtdb.audit_log`).
//!
//! Covers: every durable `DocOp` is recorded (insert/patch/replace/delete) when
//! audit is enabled; `GET /admin/audit` returns them newest-first and respects
//! `limit`/`offset`; with audit disabled no rows are written and the endpoint
//! returns an empty list (and never touches a table that may not exist).
//!
//! All tests filter by the uniquely-named test database so they remain
//! deterministic despite sharing the global `rtdb.audit_log` table with every
//! other concurrent test in the suite.

mod common;

use common::{admin_get, admin_post, fresh_db, spawn_app, test_state, test_state_with_audit};
use serde_json::json;

/// POSTs `{txn: {steps}}` to `/admin/db/{db}/mutate` and returns the outcome's
/// `results` array. Used to drive durable writes through the real committer
/// (where the audit tap lives).
async fn mutate(
    addr: std::net::SocketAddr,
    db: &str,
    steps: serde_json::Value,
) -> serde_json::Value {
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
    let body: serde_json::Value = resp.json().await.expect("parse mutate response");
    body["results"].clone()
}

/// Reads `GET /admin/audit?db=<db>` into the entries array.
async fn audit_for_db(addr: std::net::SocketAddr, db: &str) -> serde_json::Value {
    let resp = admin_get(addr, &format!("/admin/audit?db={db}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse audit response");
    body["entries"].clone()
}

// (a) Enabled: insert / patch / replace / delete each write one audit row, with
// the right db / table / docId / op / source. The admin mutate path passes
// `owner = None`, so `principal` is null. `GET /admin/audit?db=<db>` returns
// them newest-first.
#[tokio::test]
async fn audit_enabled_records_each_doc_op_newest_first() -> anyhow::Result<()> {
    let state = test_state_with_audit().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Insert two projects; capture their ids so we can patch/replace/delete.
    let results = mutate(
        addr,
        &name,
        json!([
            {"op": "insert", "table": "projects", "doc": {"name": "alpha", "status": "active", "tags": [], "updatedAt": 0}},
            {"op": "insert", "table": "projects", "doc": {"name": "beta", "status": "active", "tags": [], "updatedAt": 0}}
        ]),
    )
    .await;
    let alpha_id = results[0]["id"]
        .as_str()
        .expect("insert returns doc id")
        .to_string();
    let beta_id = results[1]["id"]
        .as_str()
        .expect("insert returns doc id")
        .to_string();

    // patch alpha, replace beta, then delete beta.
    mutate(
        addr,
        &name,
        json!([
            {"op": "patch", "table": "projects", "id": alpha_id, "fields": {"status": "paused"}},
            {"op": "replace", "table": "projects", "id": beta_id, "doc": {"name": "beta", "status": "active", "tags": [], "updatedAt": 0}},
            {"op": "delete", "table": "projects", "id": beta_id}
        ]),
    )
    .await;

    // Five durable writes → five rows for this db. Filter by db so the assertion
    // is isolated from any other concurrent test in the shared table.
    type AuditProbe = (
        String,
        String,
        Option<String>,
        String,
        Option<String>,
        String,
    );
    let rows: Vec<AuditProbe> = sqlx::query_as(
        "SELECT db, tbl, op, doc_id, principal, source \
         FROM rtdb.audit_log WHERE db = $1 ORDER BY id ASC",
    )
    .bind(name.as_str())
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 5, "five durable writes: {rows:?}");
    let expected: [(&str, &str, &str); 5] = [
        ("projects", &alpha_id, "insert"),
        ("projects", &beta_id, "insert"),
        ("projects", &alpha_id, "patch"),
        ("projects", &beta_id, "replace"),
        ("projects", &beta_id, "delete"),
    ];
    for (i, (tbl, doc_id, op)) in expected.iter().enumerate() {
        assert_eq!(rows[i].0, name.as_str(), "row {i} db");
        assert_eq!(rows[i].1, *tbl, "row {i} table");
        assert_eq!(rows[i].2.as_deref(), Some(*op), "row {i} op");
        assert_eq!(rows[i].3, *doc_id, "row {i} doc_id");
        assert!(
            rows[i].4.is_none(),
            "row {i} principal null (admin owner=None)"
        );
        assert_eq!(rows[i].5, "mutate", "row {i} source");
    }

    // Endpoint returns newest-first. The last op (delete beta) has the highest
    // id (and ts), so it is first in the response.
    let entries = audit_for_db(addr, &name).await;
    let arr = entries.as_array().expect("entries array");
    assert_eq!(arr.len(), 5, "endpoint returns all 5: {arr:?}");
    assert_eq!(arr[0]["op"], json!("delete"), "newest op first");
    assert_eq!(arr[0]["table"], json!("projects"));
    assert_eq!(arr[0]["docId"], json!(beta_id));
    assert_eq!(arr[0]["db"], json!(name));
    assert_eq!(arr[0]["source"], json!("mutate"));
    assert!(arr[0]["principal"].is_null(), "principal null");

    // tsMs is present and monotonic-non-increasing across the newest-first list.
    let ts: Vec<i64> = arr
        .iter()
        .map(|e| e["tsMs"].as_i64().expect("tsMs i64"))
        .collect();
    assert!(ts.windows(2).all(|w| w[0] >= w[1]), "newest-first: {ts:?}");

    Ok(())
}

// (b) `limit`/`offset` page: with three inserts (six durable ops would also
// work, but three keeps the math obvious), limit=2 returns the two newest and
// offset=2 returns the oldest.
#[tokio::test]
async fn audit_endpoint_paginates_with_limit_and_offset() -> anyhow::Result<()> {
    let state = test_state_with_audit().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    mutate(
        addr,
        &name,
        json!([
            {"op": "insert", "table": "projects", "doc": {"name": "one", "status": "active", "tags": [], "updatedAt": 0}},
            {"op": "insert", "table": "projects", "doc": {"name": "two", "status": "active", "tags": [], "updatedAt": 0}},
            {"op": "insert", "table": "projects", "doc": {"name": "three", "status": "active", "tags": [], "updatedAt": 0}}
        ]),
    )
    .await;

    let page1 = admin_get(addr, &format!("/admin/audit?db={name}&limit=2"))
        .await
        .json::<serde_json::Value>()
        .await
        .expect("parse page1")["entries"]
        .clone();
    let p1 = page1.as_array().expect("page1 array");
    assert_eq!(p1.len(), 2, "limit=2: {p1:?}");

    let page2 = admin_get(addr, &format!("/admin/audit?db={name}&limit=2&offset=2"))
        .await
        .json::<serde_json::Value>()
        .await
        .expect("parse page2")["entries"]
        .clone();
    let p2 = page2.as_array().expect("page2 array");
    assert_eq!(p2.len(), 1, "offset=2 returns the remaining 1: {p2:?}");

    // No overlap: the docIds paginated must be distinct.
    let mut ids: Vec<String> = p1
        .iter()
        .chain(p2.iter())
        .map(|e| e["docId"].as_str().expect("docId").to_string())
        .collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3, "limit/offset cover all 3 distinct docs");

    Ok(())
}

// (c) Disabled (the default `test_state`): a mutation writes nothing to
// `rtdb.audit_log` for this db, and `GET /admin/audit` short-circuits to empty
// without requiring the table to exist on a fresh DB.
#[tokio::test]
async fn audit_disabled_writes_nothing_and_endpoint_returns_empty() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    mutate(
        addr,
        &name,
        json!([{"op": "insert", "table": "projects", "doc": {"name": "x", "status": "active", "tags": [], "updatedAt": 0}}]),
    )
    .await;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1")
        .bind(name.as_str())
        .fetch_one(&pool)
        .await
        .expect("audit_log query (table exists from concurrent enabled tests; filtered by db)");
    assert_eq!(count, 0, "disabled audit writes no rows for this db");

    let entries = audit_for_db(addr, &name).await;
    assert!(
        entries.as_array().map(|a| a.is_empty()).unwrap_or(false),
        "disabled endpoint returns empty entries: {entries:?}"
    );

    Ok(())
}
