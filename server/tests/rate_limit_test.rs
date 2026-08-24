//! Integration tests for the in-memory HTTP rate limiter (`server/src/rate_limit.rs`).
//! Exercises the per-token and per-db fixed-window ceilings end-to-end through
//! the real `/api/query` HTTP route, the `Retry-After` header on the 429, and
//! the disabled-by-default (0) escape hatch.
//!
//! Determinism: every request in a single test fires within the same wall-clock
//! minute (tests run in milliseconds), so the fixed-window bucket is the same
//! across all N requests — the assertions are deterministic, not flaky.

use std::net::SocketAddr;

use crate::common::{admin_post, fresh_db, spawn_app, test_state_with_rate_limits};
use serde_json::json;

/// POSTs `body` to `path` with a Bearer token; mirrors `http_api_test::api_post`.
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

/// Mints a real machine token for `db` via the admin route. Returns
/// `(token_id, token)` — the id is unused here but mirrors `http_api_test`.
async fn mint_token(addr: SocketAddr, db: &str) -> (String, String) {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": db, "name": "rate-limit-test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    (
        body["tokenId"].as_str().expect("tokenId").to_string(),
        body["token"].as_str().expect("token").to_string(),
    )
}

// (a) Per-token ceiling: with limit 3, three requests succeed and the fourth is
// 429 with a `Retry-After` header and a `RATE_LIMITED` body code.
// SEC-005: the route namespacing must hold on the Postgres-backed path too —
// that is the branch multi-instance deploys actually run, and it folds the
// route into `key_text` rather than into the enum's hash. A unique IP literal
// per run keeps this safe against the shared `rtdb_auth.rate_counters` table
// that sibling tests in this binary also write to.
#[tokio::test]
async fn sec005_pg_backed_ip_buckets_are_route_namespaced() -> anyhow::Result<()> {
    use rtdb_server::rate_limit::{RateDecision, RateKey, RateLimiter};

    let state = crate::common::test_state().await;
    let limiter = RateLimiter::new_pg(state.pool.clone());
    let ip = format!("198.51.100.{}", rtdb_server::db::new_id());
    let key = |route: &'static str| RateKey::Ip {
        route,
        ip: ip.clone(),
    };

    for _ in 0..11 {
        let _ = limiter.check(key("storage"), 10).await;
    }
    assert!(
        matches!(
            limiter.check(key("storage"), 10).await,
            RateDecision::Denied { .. }
        ),
        "the storage budget for this IP must be exhausted"
    );
    assert_eq!(
        limiter.check(key("admin_login"), 10).await,
        RateDecision::Allowed,
        "SEC-005: exhausting one route's per-IP budget must not spend another's"
    );
    assert_eq!(
        limiter.check(key("anon_mint"), 10).await,
        RateDecision::Allowed
    );
    Ok(())
}

#[tokio::test]
async fn per_token_limit_allows_n_then_denials_carry_retry_after() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(3, 0).await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let query = json!({"db": name, "query": {"table": "workItems"}});
    for i in 1..=3 {
        let resp = api_post(addr, "/api/query", &token, query.clone()).await;
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "request {i} under the per-token limit should succeed"
        );
    }

    // 4th in the same minute → 429.
    let resp = api_post(addr, "/api/query", &token, query).await;
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // Retry-After header present and parses as a positive seconds count.
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .expect("Retry-After header on 429");
    let secs: u32 = retry_after
        .to_str()
        .expect("ascii Retry-After")
        .parse()
        .expect("numeric Retry-After");
    assert!(
        (1..=60).contains(&secs),
        "Retry-After within one fixed-window minute: got {secs}"
    );

    // Body is the standard wire envelope with code RATE_LIMITED.
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("RATE_LIMITED"));
    assert!(
        body["message"].is_string(),
        "rate-limited body carries a message: {body}"
    );

    Ok(())
}

// (b) Per-db ceiling is shared across tokens: with limit 2, two requests from
// token A exhaust the db budget; a third request from a *different* token for
// the same db is denied — the per-db bucket doesn't care which token spent it.
#[tokio::test]
async fn per_db_limit_is_shared_across_tokens() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(0, 2).await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let (_, token_a) = mint_token(addr, &name).await;
    let (_, token_b) = mint_token(addr, &name).await;

    let query = json!({"db": name, "query": {"table": "workItems"}});
    let resp = api_post(addr, "/api/query", &token_a, query.clone()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = api_post(addr, "/api/query", &token_a, query.clone()).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Token B is a fresh principal, but the db-wide budget is exhausted.
    let resp = api_post(addr, "/api/query", &token_b, query).await;
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    assert!(
        resp.headers().get(reqwest::header::RETRY_AFTER).is_some(),
        "Retry-After header on per-db 429"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("RATE_LIMITED"));

    Ok(())
}

// (c) Disabled (the default): with both ceilings 0, a rapid burst never hits a
// 429. Guards the default-config behavior the rest of the test suite relies on.
#[tokio::test]
async fn disabled_limits_never_throttle() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(0, 0).await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;
    let (_, token) = mint_token(addr, &name).await;

    let query = json!({"db": name, "query": {"table": "workItems"}});
    for _ in 0..25 {
        let resp = api_post(addr, "/api/query", &token, query.clone()).await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    Ok(())
}

// (d) Per-db budget is per-db: db A exhausting its budget does not consume db
// B's. With per_db_rpm = 1, after one request to db A, db A is denied but db B
// still serves its first request. (Also exercises that per-db is independent of
// per-token, which is 0 here.)
#[tokio::test]
async fn per_db_budget_does_not_leak_across_dbs() -> anyhow::Result<()> {
    // per_db_rpm=1 makes the cross-db contrast crisp: one request saturates a
    // db's bucket for the minute, so the second db is the only place a fresh
    // request can still succeed.
    let state = test_state_with_rate_limits(0, 1).await;
    let addr = spawn_app(state.clone()).await;
    let name_a = fresh_db(&state).await;
    let name_b = fresh_db(&state).await;
    let (_, token_a) = mint_token(addr, &name_a).await;
    let (_, token_b) = mint_token(addr, &name_b).await;

    // First request against db A succeeds and exhausts its per-db budget.
    let resp = api_post(
        addr,
        "/api/query",
        &token_a,
        json!({"db": name_a, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let resp = api_post(
        addr,
        "/api/query",
        &token_a,
        json!({"db": name_a, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    // db B has its own budget; its first request still succeeds.
    let resp = api_post(
        addr,
        "/api/query",
        &token_b,
        json!({"db": name_b, "query": {"table": "workItems"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    Ok(())
}
