use crate::common::{fresh_db, spawn_app, test_state};
use rtdb_server::auth::{self, Principal};
use rtdb_server::error::ErrorCode;
use serde_json::json;
use std::net::SocketAddr;

/// ENH-005 Task 1: `mint_token` must persist the new capability columns
/// (`expires_at`, `read_only`, `tables`) so they can be read back verbatim.
/// Legacy callers pass `None` / `false` / `None`; this test exercises the
/// populated path to lock in the INSERT contract.
#[tokio::test]
async fn mint_token_records_capabilities() {
    let state = test_state().await;
    let tables = vec!["users".to_string()];
    let (id, _plaintext) = rtdb_server::auth::tokens::mint_token(
        &state.pool,
        "dbx",
        "scraper",
        Some(1_700_000_000_000),
        true,
        Some(&tables),
    )
    .await
    .expect("mint token");

    let row: (Option<i64>, bool, Option<Vec<String>>) = sqlx::query_as(
        "SELECT expires_at, read_only, tables FROM rtdb_auth.machine_tokens WHERE id = $1",
    )
    .bind(&id)
    .fetch_one(&state.pool)
    .await
    .expect("read back token row");

    assert_eq!(row.0, Some(1_700_000_000_000));
    assert!(row.1);
    assert_eq!(row.2, Some(vec!["users".to_string()]));
}

/// ENH-005 Task 1: the legacy full-access path (`None`, `false`, `None`) must
/// still mint a usable token and store NULL/false/NULL for the new columns,
/// preserving pre-migration behavior.
#[tokio::test]
async fn mint_token_legacy_full_access_defaults() {
    let state = test_state().await;
    let (id, _plaintext) =
        rtdb_server::auth::tokens::mint_token(&state.pool, "dbx", "legacy", None, false, None)
            .await
            .expect("mint legacy token");

    let row: (Option<i64>, bool, Option<Vec<String>>) = sqlx::query_as(
        "SELECT expires_at, read_only, tables FROM rtdb_auth.machine_tokens WHERE id = $1",
    )
    .bind(&id)
    .fetch_one(&state.pool)
    .await
    .expect("read back legacy token row");

    assert_eq!(row.0, None);
    assert!(!row.1);
    assert_eq!(row.2, None);
}

/// ENH-005 Task 2: `resolve_bearer` must thread `read_only` + `tables` from
/// the machine-token row onto `Principal::Machine`, and `authorize` must
/// reject expired tokens live (re-queried per op, so a token that crossed
/// its expiry since resolution is denied on its next use).
#[tokio::test]
async fn resolve_bearer_threads_capabilities_and_expiry() {
    let state = test_state().await;

    // Capabilities thread through onto the Principal.
    let (_id, plaintext) = rtdb_server::auth::tokens::mint_token(
        &state.pool,
        "dbx",
        "ro",
        None,
        true,
        Some(&["users".to_string()]),
    )
    .await
    .expect("mint ro token");

    let p = auth::resolve_bearer(&state.pool, &plaintext)
        .await
        .expect("resolve ro token");
    match p {
        Principal::Machine {
            read_only, tables, ..
        } => {
            assert!(read_only);
            assert_eq!(tables, Some(vec!["users".to_string()]));
        }
        _ => panic!("expected Machine principal"),
    }

    // Expired token still resolves (so its principal is constructed), but
    // `authorize` rejects it live. `Some(1)` ⇒ expired at epoch 1ms, in the
    // past relative to `now_ms()`.
    let (_eid, ept) =
        rtdb_server::auth::tokens::mint_token(&state.pool, "dbx", "exp", Some(1), false, None)
            .await
            .expect("mint expired token");
    let ep = auth::resolve_bearer(&state.pool, &ept)
        .await
        .expect("resolve expired token (resolution does NOT enforce expiry)");
    let err = auth::authorize(&state.pool, &ep, "dbx")
        .await
        .expect_err("expired token must be denied");
    assert!(matches!(err.code, ErrorCode::Unauthorized));
}

/// ENH-005 Task 4: a machine token with a non-empty `tables` allowlist cannot
/// reach a table not on the list through any executor surface. This test mints
/// a token scoped to `["projects"]` against the kanban schema (`fresh_db`) and
/// asserts: mutate+query on `projects` succeed; mutate+query on `workItems`
/// return 403 Forbidden. The gate is enforced at the executor boundary
/// (`query::execute_query`, `txn::execute_txn`), not at HTTP routing — the
/// request reaches the executor and is rejected there with `Forbidden`.
async fn api_post(
    addr: SocketAddr,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("send api request")
}

#[tokio::test]
async fn table_scoped_token_restricted() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Mint directly via the token helper — the admin HTTP route doesn't yet
    // accept `tables`, so this bypasses the route for direct DB seeding.
    let (_, scoped) = auth::tokens::mint_token(
        &state.pool,
        &name,
        "scoped",
        None,
        false,
        Some(&["projects".to_string()]),
    )
    .await
    .expect("mint scoped token");

    let projects_txn = json!({"steps": [{"op": "insert", "table": "projects", "doc": {
        "name": "p", "status": "active", "tags": [], "updatedAt": 0
    }}]});
    let work_items_txn = json!({"steps": [{"op": "insert", "table": "workItems", "doc": {
        "projectId": "0".repeat(32), "title": "w", "status": "backlog",
        "order": 1.0, "completedAt": null
    }}]});

    // Mutate on allowed table succeeds; on forbidden table → 403.
    let resp = api_post(
        addr,
        "/api/mutate",
        &scoped,
        json!({"db": name, "txn": projects_txn}),
    )
    .await;
    assert_eq!(
        resp.status(),
        200,
        "scoped token can mutate an allowed table"
    );

    let resp = api_post(
        addr,
        "/api/mutate",
        &scoped,
        json!({"db": name, "txn": work_items_txn}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "scoped token cannot mutate a forbidden table"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("FORBIDDEN"));

    // Query on forbidden table → 403; on allowed table → 200.
    let resp = api_post(
        addr,
        "/api/query",
        &scoped,
        json!({"db": name, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "scoped token cannot query a forbidden table"
    );

    let resp = api_post(
        addr,
        "/api/query",
        &scoped,
        json!({"db": name, "query": {"table": "projects"}}),
    )
    .await;
    assert_eq!(
        resp.status(),
        200,
        "scoped token can query an allowed table"
    );

    Ok(())
}

/// ENH-005 Task 4 regression: a full-access machine token (`tables = None`)
/// and a `tables = Some([])` (empty ⇒ no restriction, per the mint-time
/// contract) must behave exactly as before — unrestricted on every table. This
/// locks in that the gate only narrows scope for `Some(non-empty)`.
#[tokio::test]
async fn full_access_token_unrestricted_regression() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // `tables = None` — legacy full-access token.
    let (_, full) = auth::tokens::mint_token(&state.pool, &name, "full", None, false, None)
        .await
        .expect("mint full-access token");
    // `tables = Some([])` — empty list treated as "no restriction".
    let (_, empty) = auth::tokens::mint_token(&state.pool, &name, "empty", None, false, Some(&[]))
        .await
        .expect("mint empty-list token");

    for (label, token) in [("full", full.as_str()), ("empty", empty.as_str())] {
        let resp = api_post(
            addr,
            "/api/query",
            token,
            json!({"db": name, "query": {"table": "projects"}}),
        )
        .await;
        assert_eq!(resp.status(), 200, "{label}: full-access reads projects");

        let resp = api_post(
            addr,
            "/api/query",
            token,
            json!({"db": name, "query": {"table": "workItems"}}),
        )
        .await;
        assert_eq!(resp.status(), 200, "{label}: full-access reads workItems");
    }

    Ok(())
}
