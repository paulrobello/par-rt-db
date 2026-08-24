use std::net::SocketAddr;

use crate::common::{admin_post, fresh_db, spawn_app, test_state};
use rtdb_server::auth::{Principal, PrincipalCtx, authorize};
use rtdb_server::db;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::Query;
use rtdb_server::subs::next_conn_id;
use serde_json::json;

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

async fn mint_token(addr: SocketAddr, db: &str) -> (String, String) {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    (
        body["tokenId"].as_str().expect("tokenId").to_string(),
        body["token"].as_str().expect("token").to_string(),
    )
}

fn insert_work_item_txn() -> serde_json::Value {
    json!({"steps": [{"op": "insert", "table": "workItems", "doc": {
        "projectId": "0".repeat(32),
        "title": "item",
        "status": "backlog",
        "order": 1.0,
        "completedAt": null
    }}]})
}

// (a) mint token for fresh_db -> /api/mutate insert -> /api/query reads it back.
#[tokio::test]
async fn mint_token_mutate_then_query_round_trips() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": insert_work_item_txn()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 1);
    let id = results[0]["id"].as_str().expect("inserted id").to_string();

    let resp = api_post(
        addr,
        "/api/query",
        &token,
        json!({"db": name, "query": {"table": "workItems", "get": id}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["result"]["title"], json!("item"));

    Ok(())
}

// (new) two /api/mutate calls with the same idempotencyKey dedupe.
#[tokio::test]
async fn http_mutate_with_same_idempotency_key_dedupes() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": insert_work_item_txn(), "idempotencyKey": "retry-key"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let first_body: serde_json::Value = resp.json().await?;

    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": insert_work_item_txn(), "idempotencyKey": "retry-key"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let second_body: serde_json::Value = resp.json().await?;

    assert_eq!(first_body, second_body);

    let resp = api_post(
        addr,
        "/api/query",
        &token,
        json!({"db": name, "query": {"table": "workItems"}}),
    )
    .await;
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["result"].as_array().expect("results array").len(), 1);

    Ok(())
}

// (b) missing bearer -> 401.
#[tokio::test]
async fn missing_bearer_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .json(&json!({"db": name, "query": {"table": "workItems"}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}

// ARC-013: `X-Rtdb-Protocol` newer than the server's `PROTOCOL_VERSION` ->
// 400 `UNSUPPORTED_PROTOCOL`, checked before the bearer token even resolves.
#[tokio::test]
async fn unsupported_protocol_header_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", "Bearer bogus-token")
        .header("X-Rtdb-Protocol", "999")
        .json(&json!({"db": name, "query": {"table": "workItems"}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNSUPPORTED_PROTOCOL");

    Ok(())
}

// ARC-013: `X-Rtdb-Protocol` at or below the current version doesn't
// interfere with normal auth failures (bogus token still 401).
#[tokio::test]
async fn supported_protocol_header_does_not_block_auth() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", "Bearer bogus-token")
        .header("X-Rtdb-Protocol", "1")
        .json(&json!({"db": name, "query": {"table": "workItems"}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    Ok(())
}

// (c) bogus token -> 401.
#[tokio::test]
async fn bogus_token_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = api_post(
        addr,
        "/api/query",
        "bogus-token",
        json!({"db": name, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    Ok(())
}

// (d) token minted for db A used against db B -> 403.
#[tokio::test]
async fn token_for_other_db_is_forbidden() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name_a = fresh_db(&state).await;
    let name_b = fresh_db(&state).await;

    let (_, token) = mint_token(addr, &name_a).await;

    let resp = api_post(
        addr,
        "/api/query",
        &token,
        json!({"db": name_b, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    Ok(())
}

// (e) revoked token -> 401.
#[tokio::test]
async fn revoked_token_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let (token_id, token) = mint_token(addr, &name).await;
    let resp = admin_post(addr, "/admin/revoke-token", json!({"tokenId": token_id})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = api_post(
        addr,
        "/api/query",
        &token,
        json!({"db": name, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    Ok(())
}

// (f) allowlist add/list/remove round-trip via admin routes; email is lowercased.
#[tokio::test]
async fn allowlist_add_list_remove_round_trips() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "add", "email": "Foo@Example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/allowlist?db={name}"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["emails"], json!(["foo@example.com"]));

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "remove", "email": "foo@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/allowlist?db={name}"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["emails"], json!([]));

    Ok(())
}

// (g) unknown allowlist action -> 400.
#[tokio::test]
async fn allowlist_unknown_action_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "bogus", "email": "foo@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

// (h) malformed txn JSON (unknown op) -> 400.
#[tokio::test]
async fn malformed_txn_unknown_op_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": {"steps": [{"op": "bogus", "table": "workItems"}]}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    Ok(())
}

// (i) mutate via HTTP triggers a registered subscription.
#[tokio::test]
async fn http_mutate_triggers_registered_subscription() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    let query: Query = serde_json::from_value(json!({"table": "workItems"})).expect("parse query");
    state
        .realtime
        .committers
        .subscribe(
            &name,
            conn,
            "q1".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": insert_work_item_txn()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let msg = rx.try_recv().expect("update after http mutate");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert_eq!(result.as_array().expect("docs array").len(), 1);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }

    Ok(())
}

// (j) authorize's User branch matches the allowlist case-insensitively, and
// forbids a non-allowlisted email.
#[tokio::test]
async fn authorize_user_branch_matches_allowlist_case_insensitively() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "add", "email": "user@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let allowed = Principal::User {
        user_id: "u1".to_string(),
        email: Some("User@Example.com".to_string()),
        name: None,
        expires_at: i64::MAX,
        anonymous: false,
        github_id: None,
        github_login: None,
        session_hash: None,
    };
    authorize(&state.pool, &allowed, &name)
        .await
        .expect("mixed-case email of an allowlisted user should authorize");

    let not_allowed = Principal::User {
        user_id: "u2".to_string(),
        email: Some("someone-else@example.com".to_string()),
        name: None,
        expires_at: i64::MAX,
        anonymous: false,
        github_id: None,
        github_login: None,
        session_hash: None,
    };
    let err = authorize(&state.pool, &not_allowed, &name)
        .await
        .expect_err("non-allowlisted user should be forbidden");
    assert_eq!(err.code, ErrorCode::Forbidden);

    Ok(())
}

// (k) POST /api/schedule creates a job, /api/schedules lists it, and the
// three :id manage routes (pause/resume/cancel) each return {ok:true}. Uses
// a far-future afterMs so the row is never drained by the scheduler (which
// isn't spawned for this db during the test).
#[tokio::test]
async fn schedule_and_manage_over_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": name,
            "when": {"type": "afterMs", "ms": 3_600_000},
            "txn": insert_work_item_txn(),
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("schedule id").to_string();

    let resp = api_post(addr, "/api/schedules", &token, json!({"db": name})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let listed = body["schedules"].as_array().expect("schedules array");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["id"], json!(id));
    assert_eq!(listed[0]["kind"], json!("oneshot"));
    assert_eq!(listed[0]["status"], json!("pending"));

    for op in &["pause", "resume", "cancel"] {
        let resp = api_post(
            addr,
            &format!("/api/schedule/{id}/{op}"),
            &token,
            json!({"db": name}),
        )
        .await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await?;
        assert_eq!(body["ok"], json!(true));
    }

    // Cancel deleted the row, so a subsequent list is empty.
    let resp = api_post(addr, "/api/schedules", &token, json!({"db": name})).await;
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["schedules"]
            .as_array()
            .expect("schedules array")
            .is_empty()
    );

    Ok(())
}

// (l) negative afterMs is rejected before any row is written.
#[tokio::test]
async fn schedule_rejects_negative_after_ms_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": name,
            "when": {"type": "afterMs", "ms": -1},
            "txn": {"steps": []},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");

    // No row was written.
    let resp = api_post(addr, "/api/schedules", &token, json!({"db": name})).await;
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["schedules"]
            .as_array()
            .expect("schedules array")
            .is_empty()
    );

    Ok(())
}

// (l2) a non-positive or over-cap everyMs on an interval schedule is rejected
// before any row is written.
#[tokio::test]
async fn schedule_rejects_bad_every_ms_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    for bad in [0i64, -1, rtdb_server::scheduler::MAX_EVERY_MS + 1] {
        let resp = api_post(
            addr,
            "/api/schedule",
            &token,
            json!({
                "db": name,
                "when": {"type": "interval", "everyMs": bad},
                "txn": {"steps": []},
            }),
        )
        .await;
        assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: serde_json::Value = resp.json().await?;
        assert_eq!(body["code"], "BAD_REQUEST");
    }

    // No row was written.
    let resp = api_post(addr, "/api/schedules", &token, json!({"db": name})).await;
    let body: serde_json::Value = resp.json().await?;
    assert!(
        body["schedules"]
            .as_array()
            .expect("schedules array")
            .is_empty()
    );

    Ok(())
}

// (j2) authorize's User branch rejects a session whose expiry has passed,
// even for an allowlisted email — the email is added to the allowlist first
// so expiry is the only possible reason for rejection.
#[tokio::test]
async fn authorize_user_branch_rejects_expired_session() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "add", "email": "user@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let expired = Principal::User {
        user_id: "u1".to_string(),
        email: Some("user@example.com".to_string()),
        name: None,
        expires_at: db::now_ms() - 1,
        anonymous: false,
        github_id: None,
        github_login: None,
        session_hash: None,
    };
    let err = authorize(&state.pool, &expired, &name)
        .await
        .expect_err("expired session should be unauthorized even when allowlisted");
    assert_eq!(err.code, ErrorCode::Unauthorized);

    Ok(())
}

// (m) POST /api/query-batch runs all queries against the same db as the same
// principal and returns aligned per-slot outcomes. Per-query execution errors
// land in their own slot (`ok:false` + envelope) and don't fail the batch; the
// db-level bearer/authorize gate still returns the normal status for the whole
// request.
#[tokio::test]
async fn batch_query_returns_aligned_results_and_isolates_per_query_errors() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    // Seed one workItem + one project to read back.
    let resp = api_post(
        addr,
        "/api/mutate",
        &token,
        json!({"db": name, "txn": {"steps": [
            {"op": "insert", "table": "workItems", "doc": {
                "projectId": "0".repeat(32), "title": "first", "status": "backlog",
                "order": 1.0, "completedAt": null}},
            {"op": "insert", "table": "projects", "doc": {
                "name": "P1", "status": "active", "tags": [], "updatedAt": 0}}
        ]}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Two valid queries → aligned ok slots with expected raw results.
    let resp = api_post(
        addr,
        "/api/query-batch",
        &token,
        json!({"db": name, "queries": [
            {"table": "workItems"},
            {"table": "projects", "count": true}
        ]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["ok"], json!(true));
    assert_eq!(results[0]["result"].as_array().expect("docs").len(), 1);
    assert_eq!(results[0]["result"][0]["title"], json!("first"));
    assert_eq!(results[1]["ok"], json!(true));
    assert_eq!(results[1]["result"], json!(1));
    // omit-when-None: `error` is absent on an ok slot.
    assert!(results[0].as_object().unwrap().get("error").is_none());

    // Middle query references an unknown table → its slot is `ok:false` with the
    // standard envelope, the surrounding slots are still `ok:true`, and the
    // batch itself still returns 200.
    let resp = api_post(
        addr,
        "/api/query-batch",
        &token,
        json!({"db": name, "queries": [
            {"table": "workItems"},
            {"table": "noSuchTable"},
            {"table": "projects", "count": true}
        ]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let results = body["results"].as_array().expect("results array");
    assert_eq!(results.len(), 3);
    assert_eq!(results[0]["ok"], json!(true));
    assert_eq!(results[1]["ok"], json!(false));
    let err = &results[1]["error"];
    assert!(err["code"].is_string(), "error slot carries a code: {err}");
    assert!(
        err["message"].is_string(),
        "error slot carries a message: {err}"
    );
    // omit-when-None: `result` is absent on an errored slot.
    assert!(results[1].as_object().unwrap().get("result").is_none());
    assert_eq!(results[2]["ok"], json!(true));
    assert_eq!(results[2]["result"], json!(1));

    Ok(())
}

// (m2) Empty `queries` is rejected with 400 before any query runs.
#[tokio::test]
async fn batch_query_rejects_empty_queries() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let resp = api_post(
        addr,
        "/api/query-batch",
        &token,
        json!({"db": name, "queries": []}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");

    Ok(())
}

// (m3) A bad bearer fails the whole batch with 401 — the db-level auth gate is
// request-scoped (resolved once, before any query runs), not per-slot.
#[tokio::test]
async fn batch_query_bad_token_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = api_post(
        addr,
        "/api/query-batch",
        "bogus-token",
        json!({"db": name, "queries": [{"table": "workItems"}]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    Ok(())
}

// (m4) SEC-119: a batch with more than MAX_BATCH_QUERIES (64) entries is
// rejected with 400 BEFORE any work runs — including before the bearer/authorize
// gate, so an unauthenticated abuser cannot pin a worker with a 10k-query fan-out.
// A batch at exactly the cap is still accepted (boundary check).
#[tokio::test]
async fn batch_query_rejects_oversized_batch() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // 65 queries — one over the cap. Built without a token to prove the cap
    // fires before the bearer gate (the empty-token path would otherwise 401).
    let queries: Vec<serde_json::Value> = (0..65)
        .map(|_| serde_json::json!({"table": "workItems"}))
        .collect();
    let resp = api_post(
        addr,
        "/api/query-batch",
        "ignored-no-matter-what",
        json!({"db": name, "queries": queries}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");
    // Error message names the cap so an operator reading the response knows
    // the limit exists and what it is.
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("exceeds maximum"),
        "response should mention the cap: {body}"
    );

    // 64 queries (exactly the cap) is accepted — proves the cap is `>`, not
    // `>=`. The queries don't need to resolve (workItems may not even exist);
    // we only assert the request is not 400'd at the gate.
    let (_, token) = mint_token(addr, &name).await;
    let queries: Vec<serde_json::Value> = (0..64)
        .map(|_| serde_json::json!({"table": "workItems"}))
        .collect();
    let resp = api_post(
        addr,
        "/api/query-batch",
        &token,
        json!({"db": name, "queries": queries}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["results"].as_array().expect("results").len(), 64);

    Ok(())
}

// (n) A read-only machine token cannot mutate (403 Forbidden) but can still
// query the same db. The token is minted directly with read_only=true via
// auth::tokens::mint_token (the /admin/mint-token endpoint doesn't yet expose
// the flag), then exercised over the real HTTP path.
#[tokio::test]
async fn read_only_token_cannot_mutate_but_can_query() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let (_id, ro_token) =
        rtdb_server::auth::tokens::mint_token(&state.pool, &name, "ro", None, true, None)
            .await
            .expect("mint read-only token");

    // Mutate is forbidden.
    let resp = api_post(
        addr,
        "/api/mutate",
        &ro_token,
        json!({"db": name, "txn": insert_work_item_txn()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "FORBIDDEN");

    // Query is allowed (no rows, but must not be 403).
    let resp = api_post(
        addr,
        "/api/query",
        &ro_token,
        json!({"db": name, "query": {"table": "workItems"}}),
    )
    .await;
    assert_ne!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    Ok(())
}

// (n2) A read-only machine token cannot upload to storage (403 Forbidden).
// Verifies the storage-upload write gate. Uses a raw body (storage uploads
// don't use ApiJson) and the same directly-minted read-only token.
#[tokio::test]
async fn read_only_token_cannot_upload_storage() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let (_id, ro_token) =
        rtdb_server::auth::tokens::mint_token(&state.pool, &name, "ro", None, true, None)
            .await
            .expect("mint read-only token");

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{name}"))
        .header("Authorization", format!("Bearer {ro_token}"))
        .header("Content-Type", "application/octet-stream")
        .body(b"hello".to_vec())
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);

    Ok(())
}

// (n3) A read-only machine token cannot manage scheduled jobs: each of the
// three :id manage routes (pause/resume/cancel) returns 403 FORBIDDEN. The
// job is created by a full-access token, then a directly-minted read-only
// token (read_only=true via auth::tokens::mint_token, since /admin/mint-token
// doesn't expose the flag) attempts to tamper with it. Gates the
// scheduler-management path that ENH-005 left open after gating schedule-create.
#[tokio::test]
async fn read_only_token_cannot_manage_schedule() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    // Create the job with a full-access token (far-future afterMs so the
    // scheduler, which isn't spawned for this db during the test, never drains it).
    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": name,
            "when": {"type": "afterMs", "ms": 3_600_000},
            "txn": insert_work_item_txn(),
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("schedule id").to_string();

    let (_id, ro_token) =
        rtdb_server::auth::tokens::mint_token(&state.pool, &name, "ro", None, true, None)
            .await
            .expect("mint read-only token");

    for op in &["pause", "resume", "cancel"] {
        let resp = api_post(
            addr,
            &format!("/api/schedule/{id}/{op}"),
            &ro_token,
            json!({"db": name}),
        )
        .await;
        assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
        let body: serde_json::Value = resp.json().await?;
        assert_eq!(body["code"], "FORBIDDEN");
    }

    Ok(())
}
