mod common;

use std::net::SocketAddr;

use common::{admin_post, fresh_db, spawn_app, test_state};
use rtdb_server::auth::{Principal, authorize};
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
        .committers
        .subscribe(&name, conn, "q1".to_string(), query, tx)
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
        json!({"db": name, "action": "add", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let allowed = Principal::User {
        user_id: "u1".to_string(),
        email: "Probello@Gmail.com".to_string(),
        name: None,
        expires_at: i64::MAX,
        github_id: None,
        github_login: None,
    };
    authorize(&state.pool, &allowed, &name)
        .await
        .expect("mixed-case email of an allowlisted user should authorize");

    let not_allowed = Principal::User {
        user_id: "u2".to_string(),
        email: "someone-else@example.com".to_string(),
        name: None,
        expires_at: i64::MAX,
        github_id: None,
        github_login: None,
    };
    let err = authorize(&state.pool, &not_allowed, &name)
        .await
        .expect_err("non-allowlisted user should be forbidden");
    assert_eq!(err.code, ErrorCode::Forbidden);

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
        json!({"db": name, "action": "add", "email": "probello@gmail.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let expired = Principal::User {
        user_id: "u1".to_string(),
        email: "probello@gmail.com".to_string(),
        name: None,
        expires_at: db::now_ms() - 1,
        github_id: None,
        github_login: None,
    };
    let err = authorize(&state.pool, &expired, &name)
        .await
        .expect_err("expired session should be unauthorized even when allowlisted");
    assert_eq!(err.code, ErrorCode::Unauthorized);

    Ok(())
}
