use crate::common::{admin_get, admin_post, fresh_db, mint_user_session, spawn_app, test_state};
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

/// SEC-007 regression set: the shared `authed` prologue must reject an HTTP
/// data-route request whose `Origin` is neither `public_url` nor on the hot
/// `allowed_origins` list when the ambient session cookie is the credential
/// (CORS does not stop the request from *executing*, and the storage upload
/// is preflight-free), while an allowlisted Origin, `public_url` itself, an
/// absent Origin (CLI/SDK/machine token), and an explicit bearer header all
/// stay admitted. Cookie cases ride a real OAuth session — fresh
/// `rtdb_auth.users`/`sessions` rows whose email is allowlisted for the db
/// via the admin API (the `per_row_auth_test` fixture shape) — because a
/// machine token only authenticates via `Authorization` and can never
/// exercise the cookie path.
async fn cookie_session_for(
    state: &std::sync::Arc<rtdb_server::AppState>,
    addr: SocketAddr,
    db: &str,
) -> String {
    let suffix = uuid::Uuid::now_v7().simple();
    let email = format!("sec007-{suffix}@example.com");
    let session = mint_user_session(&state.pool, &format!("sec007-{suffix}"), &email).await;
    let r = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db, "action": "add", "email": email}),
    )
    .await;
    assert_eq!(r.status(), reqwest::StatusCode::OK, "allowlist add failed");
    session
}

fn mutate_body(db: &str) -> serde_json::Value {
    json!({"db": db, "txn": {"steps": [{"op": "insert", "table": "projects", "doc": {
        "name": "sec007", "status": "active", "tags": [], "updatedAt": 0
    }}]}})
}

#[tokio::test]
async fn http_data_route_rejects_disallowed_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let session = cookie_session_for(&state, addr, &name).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Origin", "https://evil.example")
        .header("Cookie", format!("rtdb_session={session}"))
        .json(&mutate_body(&name))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a cookie-authenticated mutate from a disallowed Origin must be rejected"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("FORBIDDEN"));

    // Same credential, no Origin header: admitted, so the 403 above was the
    // Origin gate alone — and the rejected write left no document behind.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header("Cookie", format!("rtdb_session={session}"))
        .json(&json!({"db": name, "query": {"table": "projects"}}))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        200,
        "the session cookie without an Origin must stay admitted"
    );
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["result"]
            .as_array()
            .is_some_and(|docs| docs.is_empty()),
        "the rejected mutate must leave no document"
    );

    Ok(())
}

#[tokio::test]
async fn storage_upload_rejects_disallowed_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let session = cookie_session_for(&state, addr, &name).await;

    // The upload takes a raw text/plain body — the preflight-free CSRF shape
    // the JSON routes don't have.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{name}"))
        .header("Origin", "https://evil.example")
        .header("Cookie", format!("rtdb_session={session}"))
        .header("Content-Type", "text/plain")
        .body("csrf-bytes")
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "a cookie-authenticated upload from a disallowed Origin must be rejected"
    );

    // The rejected upload left no blob.
    let body: serde_json::Value = admin_get(addr, &format!("/admin/db/{name}/storage"))
        .await
        .json()
        .await?;
    assert!(
        body["files"]
            .as_array()
            .is_some_and(|files| files.is_empty()),
        "the rejected upload must leave no blob"
    );

    Ok(())
}

#[tokio::test]
async fn http_data_route_allows_allowlisted_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let session = cookie_session_for(&state, addr, &name).await;

    // The test hot allowlist seeds `http://localhost:5173` (common::test_hot).
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Origin", "http://localhost:5173")
        .header("Cookie", format!("rtdb_session={session}"))
        .json(&mutate_body(&name))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        200,
        "an allowlisted Origin with a valid session cookie must mutate"
    );

    // And the write landed: one projects doc.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header("Cookie", format!("rtdb_session={session}"))
        .json(&json!({"db": name, "query": {"table": "projects"}}))
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await?;
    let docs = body["result"].as_array().expect("docs array");
    assert_eq!(docs.len(), 1, "the admitted mutate must have written");
    assert_eq!(docs[0]["name"], json!("sec007"));

    Ok(())
}

#[tokio::test]
async fn http_data_route_allows_public_url_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let session = cookie_session_for(&state, addr, &name).await;

    // `common::test_config` seeds `public_url = "http://localhost:0"`, the
    // deploy's own origin — always allowed.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Origin", "http://localhost:0")
        .header("Cookie", format!("rtdb_session={session}"))
        .json(&mutate_body(&name))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        200,
        "the public_url Origin with a valid session cookie must mutate"
    );

    Ok(())
}

#[tokio::test]
async fn http_data_route_allows_absent_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = auth::tokens::mint_token(&state.pool, &name, "sec7", None, false, None)
        .await
        .expect("mint token");

    // No Origin header at all: the CLI/SDK/machine-token shape. Admitted —
    // the resolve/authorize gates still validate the credential.
    let resp = api_post(addr, "/api/mutate", &token, mutate_body(&name)).await;
    assert_eq!(
        resp.status(),
        200,
        "an absent Origin (non-browser client) must keep mutating"
    );

    Ok(())
}

#[tokio::test]
async fn http_data_route_allows_explicit_bearer_with_disallowed_origin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = auth::tokens::mint_token(&state.pool, &name, "sec7", None, false, None)
        .await
        .expect("mint token");

    // A non-ambient credential cannot be CSRF-forged (a foreign page cannot
    // set the Authorization header), so the Origin check does not apply.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Origin", "https://evil.example")
        .header("Authorization", format!("Bearer {token}"))
        .json(&mutate_body(&name))
        .send()
        .await?;
    assert_eq!(
        resp.status(),
        200,
        "an explicit bearer header is non-ambient and must stay admitted"
    );

    Ok(())
}

/// Empty-Authorization semantics, pinned against the current parser
/// (`http_api::bearer_token` / `explicit_bearer`, inspected): an explicitly
/// empty `Bearer` is NOT a credential — the prefix match filters empty tokens
/// and falls through to the ambient cookie. With no cookie present the
/// request is 401 either way. This asserts only that floor (the vault rule
/// from the go-client F7 fix: an explicitly-presented-but-empty credential
/// must never authenticate on its own); the empty-bearer-WITH-cookie
/// fallback is deliberately not pinned while the main session may revise it.
/// Both wire spellings are covered — OWS trimming can deliver `Bearer`
/// without the trailing space, and both fail the parser identically.
#[tokio::test]
async fn empty_bearer_without_cookie_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    for header in ["Bearer ", "Bearer"] {
        let resp = reqwest::Client::new()
            .post(format!("http://{addr}/api/mutate"))
            .header("Authorization", header)
            .json(&mutate_body(&name))
            .send()
            .await?;
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "{header:?}: an explicitly empty bearer must not authenticate"
        );
    }

    Ok(())
}
