mod common;

/// Serializes tests that touch the global single-row `rtdb_config` table
/// (id = 1). That row is shared across the whole dev Postgres, so without this
/// guard two such tests running in parallel race — one test's `DELETE` can be
/// undone by another's `PATCH`/`save_hot` in the window between the `DELETE`
/// and the following `load_hot` assertion, making the suite flaky. Per-db test
/// isolation doesn't help here because `rtdb_config` is global (not per test db).
static RTDB_CONFIG_GUARD: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Seeding lowercases emails, is idempotent, and stores them with a NULL github_id.
#[tokio::test]
async fn seed_admin_emails_lowercases_and_is_idempotent() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();

    rtdb_server::auth::seed_admin_emails(
        &pool,
        &[
            "Foo@Bar.com".to_string(),
            "  ".to_string(),
            "a@b.com".to_string(),
        ],
    )
    .await?;
    // Re-seed the same address: ON CONFLICT DO NOTHING keeps it a single row.
    rtdb_server::auth::seed_admin_emails(&pool, &["foo@bar.com".to_string()]).await?;

    // Assert the two seeded rows exist with NULL github_id. `rtdb_auth.admins` is
    // a shared global table (other tests insert their own rows), so we probe by
    // email rather than asserting the whole table — matching the isolation
    // convention in `per_row_auth_test` / `oauth_test`.
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT email, github_id FROM rtdb_auth.admins WHERE email IN ($1, $2) ORDER BY email",
    )
    .bind("a@b.com")
    .bind("foo@bar.com")
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            ("a@b.com".to_string(), None),
            ("foo@bar.com".to_string(), None),
        ]
    );
    let blank_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM rtdb_auth.admins WHERE email = '  '")
            .fetch_one(&pool)
            .await?;
    assert_eq!(blank_count, 0);
    Ok(())
}

// Create an OAuth user + session and return the plaintext session bearer.
async fn user_session(
    state: &std::sync::Arc<rtdb_server::AppState>,
    email: &str,
    github_id: Option<i64>,
) -> String {
    let pool = &state.pool;
    let user_id = format!("u{}", uuid::Uuid::now_v7().simple());
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&user_id)
    .bind(github_id)
    .bind(email)
    .bind(email)
    .bind(rtdb_server::db::now_ms())
    .execute(pool)
    .await
    .unwrap();
    rtdb_server::auth::session::create_session(pool, &user_id, 30)
        .await
        .unwrap()
}

async fn resolve_principal(
    state: &std::sync::Arc<rtdb_server::AppState>,
    token: &str,
) -> rtdb_server::auth::Principal {
    rtdb_server::auth::resolve_bearer(&state.pool, token)
        .await
        .unwrap()
}

// is_admin is false with no admins; true when the user's email is allowlisted;
// true when the user's github_id is allowlisted (independently of email).
//
// Emails + github_id are unique per run: `rtdb_auth.users` enforces UNIQUE on
// both and `rtdb_auth.admins.email` is the PK, all shared across test runs
// against the persistent dev Postgres (same convention as `per_row_auth_test` /
// `oauth_test`, which suffix emails with a uuid).
#[tokio::test]
async fn is_admin_matches_email_or_github_id() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let uid = uuid::Uuid::now_v7().simple().to_string();
    let gh_id: i64 = i64::from_str_radix(&uid[..15], 16).expect("parse hex as i64");
    let email_addr = format!("owner-{uid}@example.com");
    let gh_addr = format!("ghonly-{uid}@example.com");
    let stranger_addr = format!("stranger-{uid}@example.com");

    let email_tok = user_session(&state, &email_addr, None).await;
    let gh_tok = user_session(&state, &gh_addr, Some(gh_id)).await;
    let stranger_tok = user_session(&state, &stranger_addr, None).await;

    // No admins yet → nobody is admin.
    assert!(
        !rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &email_tok).await).await
    );

    // Add owner by email.
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&email_addr)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;
    assert!(rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &email_tok).await).await);

    // gh user is not yet admin (no email match, github_id not listed).
    assert!(!rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &gh_tok).await).await);

    // Add an admin row keyed on gh_id under an unrelated email → gh user matches.
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, $2, $3)")
        .bind(format!("someone-else-{uid}@example.com"))
        .bind(gh_id)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;
    assert!(rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &gh_tok).await).await);

    // A user matched by neither is not admin.
    assert!(
        !rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &stranger_tok).await).await
    );
    Ok(())
}

// The admin-key path still authorizes after the require_admin rewrite.
#[tokio::test]
async fn admin_key_path_still_authorizes() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let resp = common::admin_get(addr, "/admin/dbs").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

// DELETE with an admin-key bearer + JSON body.
async fn admin_delete(
    addr: std::net::SocketAddr,
    path: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .delete(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn admins_crud_round_trip() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    // Add by email.
    let resp = common::admin_post(
        addr,
        "/admin/admins",
        serde_json::json!({"email": "Crew@Example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Listed, lowercased. (Other parallel tests may add rows; assert membership.)
    let body: serde_json::Value = common::admin_get(addr, "/admin/admins")
        .await
        .json()
        .await?;
    let emails: Vec<String> = body["admins"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["email"].as_str().map(String::from))
        .collect();
    assert!(emails.iter().any(|e| e == "crew@example.com"));

    // Remove.
    let resp = admin_delete(
        addr,
        "/admin/admins",
        serde_json::json!({"email": "crew@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value = common::admin_get(addr, "/admin/admins")
        .await
        .json()
        .await?;
    let emails: Vec<String> = body["admins"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["email"].as_str().map(String::from))
        .collect();
    assert!(!emails.iter().any(|e| e == "crew@example.com"));
    Ok(())
}

// Adding an admin with a blank email is a 400, not a silent no-op.
#[tokio::test]
async fn add_admin_requires_email() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;
    let resp = common::admin_post(addr, "/admin/admins", serde_json::json!({"email": "  "})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

// GET with an arbitrary bearer (for the session-path tests below).
async fn bearer_get(addr: std::net::SocketAddr, path: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
}

// An allowlisted OAuth session is admitted to an admin route.
//
// Email + admin row are UUID-suffixed: `rtdb_auth.users.email` is UNIQUE and
// `rtdb_auth.admins.email` is the PK, both shared across test runs against the
// persistent dev Postgres (same convention as `is_admin_matches_email_or_github_id`
// above, and `per_row_auth_test` / `oauth_test`).
#[tokio::test]
async fn oauth_admin_session_is_admitted() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let email = format!("dash-{}@example.com", uuid::Uuid::now_v7().simple());

    let token = user_session(&state, &email, None).await;
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&email)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;

    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

// A valid OAuth session NOT on the admin allowlist is rejected (403).
#[tokio::test]
async fn non_admin_session_is_forbidden() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let email = format!("nobody-{}@example.com", uuid::Uuid::now_v7().simple());

    let token = user_session(&state, &email, None).await;
    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    Ok(())
}

// A missing bearer is rejected (401).
#[tokio::test]
async fn missing_bearer_is_unauthorized() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/admins"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

// A machine token is never an admin, even if it reaches an admin route.
#[tokio::test]
async fn machine_token_is_not_admin() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    // Create a db + mint a machine token through the admin API.
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    common::admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;
    let resp: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": name, "name": "tok"}),
    )
    .await
    .json()
    .await?;
    let token = resp["token"].as_str().unwrap().to_string();

    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    Ok(())
}

// GET /admin/dbs/{db}/schema returns the pushed schema back.
#[tokio::test]
async fn get_schema_returns_pushed_schema() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let resp = common::admin_get(addr, &format!("/admin/dbs/{db}/schema")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    // fresh_db pushes the kanban fixture, which has a `projects` table.
    assert!(
        body["tables"].get("projects").is_some(),
        "schema missing projects table: {body}"
    );
    // Unknown db → 404 (NotFound), not 500.
    let resp = common::admin_get(addr, "/admin/dbs/does-not-exist/schema").await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

// GET /admin/tokens?db= lists tokens by id/name/revoked and never exposes the secret hash.
#[tokio::test]
async fn list_tokens_omits_secret() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let _resp = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "ci"}),
    )
    .await;

    let resp = common::admin_get(addr, &format!("/admin/tokens?db={db}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let tokens = body["tokens"].as_array().expect("tokens array");
    assert!(
        tokens.iter().any(|t| t["name"] == "ci"),
        "minted token missing: {body}"
    );
    // The response must never carry the secret or its hash.
    let body_str = body.to_string();
    assert!(
        !body_str.contains("token_hash") && !body_str.contains("hash"),
        "secret leaked: {body_str}"
    );
    // And the DB's stored hash is not equal to any field value in the response.
    let (stored_hash,): (String,) =
        sqlx::query_as("SELECT token_hash FROM rtdb_auth.machine_tokens WHERE db_name = $1")
            .bind(&db)
            .fetch_one(&pool)
            .await?;
    assert!(
        !body_str.contains(&stored_hash),
        "token hash leaked: {stored_hash}"
    );
    Ok(())
}

// GET /admin/dbs/{db}/stats returns one row per document table (logical names from the
// schema) with an integer rowCount + integer sizeBytes, plus a positive total. COUNT(*)
// correctness is inherent; this verifies the endpoint enumerates tables and queries each.
#[tokio::test]
async fn db_stats_reports_table_counts_and_sizes() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let resp = common::admin_get(addr, &format!("/admin/dbs/{db}/stats")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let tables = body["tables"].as_array().expect("tables array");
    let names: Vec<String> = tables
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect();
    // The kanban fixture has exactly these two document tables (logical schema names).
    assert!(
        names.contains(&"projects".to_string()) && names.contains(&"workItems".to_string()),
        "expected projects+workItems: {body}"
    );
    for t in tables {
        assert!(
            t["rowCount"].as_i64().is_some(),
            "rowCount not an integer: {t}"
        );
        assert!(
            t["sizeBytes"].as_i64().is_some(),
            "sizeBytes not an integer: {t}"
        );
    }
    assert!(
        body["totalSizeBytes"].as_i64().unwrap_or(0) > 0,
        "total size not positive: {body}"
    );

    // Unknown db → 404.
    let resp = common::admin_get(addr, "/admin/dbs/does-not-exist/stats").await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

// Metrics counters increment and snapshot reflects them; count() reports active subs.
#[tokio::test]
async fn metrics_counters_and_subs_count() -> anyhow::Result<()> {
    let m = rtdb_server::metrics::Metrics::new();
    m.record_query();
    m.record_query();
    m.record_mutation();

    let state = common::test_state().await;
    // count() is 0 before any subscribe on a fresh manager.
    assert_eq!(state.realtime.subs.count().await, 0);

    let snap = m
        .snapshot(&state.pool, &state.realtime.subs, state.runtime.started_at)
        .await;
    assert_eq!(snap.queries_total, 2);
    assert_eq!(snap.mutations_total, 1);
    assert_eq!(snap.uploads_total, 0);
    assert_eq!(snap.ws_connections, 0);
    assert!(snap.pool_size >= 0);
    Ok(())
}

// GET /admin/metrics returns the snapshot; a real mutation bumps mutationsTotal.
#[tokio::test]
async fn metrics_endpoint_reflects_a_mutation() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let before: serde_json::Value = common::admin_get(addr, "/admin/metrics")
        .await
        .json()
        .await?;
    assert_eq!(
        before["mutationsTotal"], 0,
        "fresh state should have 0 mutations: {before}"
    );

    // Mint a token + run one insert via /api/mutate.
    let mint: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "t"}),
    )
    .await
    .json()
    .await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "db": db,
            "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let after: serde_json::Value = common::admin_get(addr, "/admin/metrics")
        .await
        .json()
        .await?;
    assert_eq!(
        after["mutationsTotal"], 1,
        "one mutation should be counted: {after}"
    );
    assert!(after["queriesTotal"].as_i64().unwrap_or(-1) >= 0);
    Ok(())
}

// GET /admin/metrics exposes query/mutate/subscribe latency percentiles that
// are well-formed (and non-zero at p50) once a real query has run. Exact
// values are nondeterministic; assert structure + ordering + presence.
#[tokio::test]
async fn metrics_endpoint_reflects_query_latency() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    // Mint a token + insert one doc so a query has something to read.
    let mint: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "t"}),
    )
    .await
    .json()
    .await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let client = reqwest::Client::new();
    let insert = client
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "db": db,
            "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}
        }))
        .send()
        .await?;
    assert_eq!(insert.status(), reqwest::StatusCode::OK);

    // Run several queries so the latency ring has multiple samples.
    for _ in 0..3 {
        let q = client
            .post(format!("http://{addr}/api/query"))
            .header("Authorization", format!("Bearer {token}"))
            .json(&serde_json::json!({"db": db, "query": {"table": "projects"}}))
            .send()
            .await?;
        assert_eq!(q.status(), reqwest::StatusCode::OK);
    }

    let snap: serde_json::Value = common::admin_get(addr, "/admin/metrics")
        .await
        .json()
        .await?;

    // queryLatency: object with p50/p95/p99, monotonically ordered, non-zero
    // after real queries (a real db round-trip takes measurable time).
    let ql = &snap["queryLatency"];
    assert!(ql.is_object(), "queryLatency missing/malformed: {snap}");
    let p50 = ql["p50"].as_i64().unwrap_or(-1);
    let p95 = ql["p95"].as_i64().unwrap_or(-1);
    let p99 = ql["p99"].as_i64().unwrap_or(-1);
    assert!(
        p50 > 0,
        "queryLatency.p50 should reflect a real query: {snap}"
    );
    assert!(p95 >= p50, "queryLatency.p95 >= p50: {snap}");
    assert!(p99 >= p95, "queryLatency.p99 >= p95: {snap}");

    // mutateLatency: present + well-formed (one mutation ran above).
    let ml = &snap["mutateLatency"];
    assert!(ml.is_object(), "mutateLatency missing/malformed: {snap}");
    let m50 = ml["p50"].as_i64().unwrap_or(-1);
    let m95 = ml["p95"].as_i64().unwrap_or(-1);
    let m99 = ml["p99"].as_i64().unwrap_or(-1);
    assert!(m50 >= 0, "mutateLatency.p50 should be non-negative: {snap}");
    assert!(m95 >= m50, "mutateLatency.p95 >= p50: {snap}");
    assert!(m99 >= m95, "mutateLatency.p99 >= p95: {snap}");

    // subscribeLatency: present + well-formed (no WS sub here → all zeros is
    // the expected empty-state; only assert the shape and ordering).
    let sl = &snap["subscribeLatency"];
    assert!(sl.is_object(), "subscribeLatency missing/malformed: {snap}");
    let s50 = sl["p50"].as_i64().unwrap_or(-1);
    let s95 = sl["p95"].as_i64().unwrap_or(-1);
    let s99 = sl["p99"].as_i64().unwrap_or(-1);
    assert!(
        s50 >= 0,
        "subscribeLatency.p50 should be non-negative: {snap}"
    );
    assert!(s95 >= s50, "subscribeLatency.p95 >= p50: {snap}");
    assert!(s99 >= s95, "subscribeLatency.p99 >= p95: {snap}");
    Ok(())
}

// GET /metrics returns Prometheus text-exposition (no Accept header → text
// branch), and a prior /api/query is reflected in the rtdb_queries_total sample.
#[tokio::test]
async fn metrics_prometheus_endpoint() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    // Mint a token, insert one doc, then run one query (bumps queries_total).
    let mint: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "t"}),
    )
    .await
    .json()
    .await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let client = reqwest::Client::new();
    let insert = client
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "db": db,
            "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}
        }))
        .send()
        .await?;
    assert_eq!(insert.status(), reqwest::StatusCode::OK);
    let query = client
        .post(format!("http://{addr}/api/query"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"db": db, "query": {"table": "projects"}}))
        .send()
        .await?;
    assert_eq!(query.status(), reqwest::StatusCode::OK);

    // No Accept header → Prometheus text (API-only test mode, no RTDB_STATIC_DIR).
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.starts_with("text/plain"),
        "expected text/plain content-type, got: {content_type}"
    );
    let body = resp.text().await?;
    assert!(
        body.contains("# TYPE rtdb_queries_total counter"),
        "missing counter TYPE line: {body}"
    );
    assert!(
        body.contains("rtdb_queries_total "),
        "missing rtdb_queries_total sample line: {body}"
    );
    assert!(
        body.contains("# TYPE rtdb_ws_connections gauge"),
        "missing gauge TYPE line: {body}"
    );
    assert!(
        body.contains("rtdb_build_info{version="),
        "missing build_info line: {body}"
    );
    // The query we ran must be reflected: sample value >= 1.
    let qline = body
        .lines()
        .find(|l| l.starts_with("rtdb_queries_total "))
        .unwrap_or("");
    let val: u64 = qline
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    assert!(val >= 1, "rtdb_queries_total sample < 1: {qline}");
    Ok(())
}

// OpFeed publishes one event per DocOp (with kind), replays from the ring, broadcasts live.
#[tokio::test]
async fn op_feed_publishes_and_replays() -> anyhow::Result<()> {
    use rtdb_server::txn::{DocOp, OpKind};
    let feed = rtdb_server::op_feed::OpFeed::new(64, 32);
    let ops = vec![
        DocOp {
            table: "projects".into(),
            id: "id1".into(),
            kind: OpKind::Insert,
        },
        DocOp {
            table: "projects".into(),
            id: "id2".into(),
            kind: OpKind::Patch,
        },
    ];
    let mut rx = feed.subscribe();
    feed.publish("dbA", Some("user-1"), &ops).await;

    let recent = feed.recent(Some("dbA"), None, 10).await;
    let ids: Vec<&str> = recent.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(
        ids.contains(&"id1") && ids.contains(&"id2"),
        "ring missing events: {recent:?}"
    );
    assert_eq!(
        recent.iter().find(|e| e.doc_id == "id1").unwrap().kind,
        OpKind::Insert
    );
    assert!(feed.recent(Some("dbB"), None, 10).await.is_empty());

    let mut got = 0;
    while let Ok(_ev) = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await
    {
        got += 1;
    }
    assert_eq!(got, 2);
    Ok(())
}

// A committed insert publishes an op event WITH its kind; /admin/ops/recent returns it.
#[tokio::test]
async fn op_feed_tapped_on_commit() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let mint: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "t"}),
    )
    .await
    .json()
    .await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let resp = reqwest::Client::new().post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"db": db, "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}}))
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value =
        common::admin_get(addr, &format!("/admin/ops/recent?db={db}&n=10"))
            .await
            .json()
            .await?;
    let ops = body["ops"].as_array().expect("ops array");
    let ours = ops
        .iter()
        .find(|o| o["table"] == "projects")
        .expect("projects op event missing");
    assert_eq!(ours["kind"], "insert", "kind should be 'insert': {ours}");
    Ok(())
}

// /admin/stream rejects a missing bearer at the upgrade (no 101).
#[tokio::test]
async fn admin_stream_requires_admin() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/stream"))
        .send()
        .await?;
    let status = resp.status();
    assert!(
        status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN,
        "missing bearer must be rejected, got {status}"
    );
    Ok(())
}

// /admin/stream authenticates via the `rtdb-admin.<token>` WebSocket
// subprotocol — the browser dashboard's path, since browsers cannot set the
// Authorization header on a WS handshake. A valid admin key upgrades (101) and
// the stream actually pushes a frame (ring replay or the ~1s gauge snapshot).
#[tokio::test]
async fn admin_stream_authenticates_via_subprotocol() -> anyhow::Result<()> {
    use futures_util::StreamExt;
    use std::time::Duration;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let mut req = format!("ws://{addr}/admin/stream").into_client_request()?;
    req.headers_mut().insert(
        "sec-websocket-protocol",
        reqwest::header::HeaderValue::from_str("rtdb-admin.test-admin-key")?,
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await?;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::SWITCHING_PROTOCOLS,
        "a valid subprotocol bearer must upgrade to 101"
    );

    let frame = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .expect("timed out waiting for a stream frame")
        .expect("stream closed")
        .expect("ws frame error");
    match frame {
        tokio_tungstenite::tungstenite::Message::Text(t) => {
            let v: serde_json::Value = serde_json::from_str(&t)?;
            assert!(
                v["kind"] == "op" || v["kind"] == "gauges",
                "expected an op|gauges frame, got {v}"
            );
        }
        other => panic!("expected a text frame, got {other:?}"),
    }
    Ok(())
}

// /admin/stream rejects an invalid bearer offered via the subprotocol: the
// server returns 401 before the upgrade, which tokio-tungstenite surfaces as a
// connect error.
#[tokio::test]
async fn admin_stream_rejects_bad_subprotocol() -> anyhow::Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let mut req = format!("ws://{addr}/admin/stream").into_client_request()?;
    req.headers_mut().insert(
        "sec-websocket-protocol",
        reqwest::header::HeaderValue::from_str("rtdb-admin.not-the-real-key")?,
    );
    let result = tokio_tungstenite::connect_async(req).await;
    assert!(
        result.is_err(),
        "an invalid subprotocol bearer must be rejected before the upgrade"
    );
    Ok(())
}

// The persisted hot-config row round-trips through load_hot/save_hot, and a
// missing row loads None. `rtdb_config` is a global table shared across tests on
// the dev Postgres, so this test cleans up its row to avoid polluting others.
#[tokio::test]
async fn hot_config_round_trips_through_rtdb_config() -> anyhow::Result<()> {
    let _rtdb_config_guard = RTDB_CONFIG_GUARD.lock().await;
    let state = common::test_state().await;

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    assert!(
        rtdb_server::config::load_hot(&state.pool, &common::test_hot())
            .await?
            .is_none()
    );

    let hot = rtdb_server::config::HotConfig {
        allowed_origins: vec![
            "https://one.example.com".into(),
            "https://two.example.com".into(),
        ],
        session_ttl_days: 7,
        max_file_size: 12345,
        idempotency_ttl_ms: rtdb_server::mutation_log::DEFAULT_DEDUP_TTL_MS,
    };
    rtdb_server::config::save_hot(&state.pool, &hot).await?;
    let loaded = rtdb_server::config::load_hot(&state.pool, &common::test_hot())
        .await?
        .unwrap();
    assert_eq!(loaded.allowed_origins, hot.allowed_origins);
    assert_eq!(loaded.session_ttl_days, 7);
    assert_eq!(loaded.max_file_size, 12345);
    assert_eq!(
        loaded.idempotency_ttl_ms,
        rtdb_server::mutation_log::DEFAULT_DEDUP_TTL_MS
    );

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    Ok(())
}

// A row persisted BEFORE a `HotConfig` field existed must still load, keeping
// its own values and taking the env seed only for the absent field. This is the
// regression guard for the prod defect found 2026-07-29: the strict decode
// failed on `missing field idempotencyTtlMs`, so every boot silently threw the
// operator's persisted PATCH away and reverted to env defaults.
#[tokio::test]
async fn hot_config_row_missing_a_newer_field_still_loads() -> anyhow::Result<()> {
    let _rtdb_config_guard = RTDB_CONFIG_GUARD.lock().await;
    let state = common::test_state().await;

    // Write the row shape as it actually existed in prod: no idempotencyTtlMs.
    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    sqlx::query("INSERT INTO rtdb_config (id, hot) VALUES (1, $1)")
        .bind(serde_json::json!({
            "allowedOrigins": ["https://persisted.example.com"],
            "sessionTtlDays": 14,
            "maxFileSize": 999
        }))
        .execute(&state.pool)
        .await?;

    let defaults = common::test_hot();
    let loaded = rtdb_server::config::load_hot(&state.pool, &defaults)
        .await?
        .expect("an older row must load, not error");
    // Persisted values survive...
    assert_eq!(
        loaded.allowed_origins,
        vec!["https://persisted.example.com".to_string()]
    );
    assert_eq!(loaded.session_ttl_days, 14);
    assert_eq!(loaded.max_file_size, 999);
    // ...and only the absent field comes from the env seed.
    assert_eq!(loaded.idempotency_ttl_ms, defaults.idempotency_ttl_ms);

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    Ok(())
}

// GET /admin/config redacts secrets (admin key, OAuth, database_url) into
// configured-bools and shows hot values + version/commit + the allowlist.
// PATCH updates a hot value, persists it, and rejects invalid/unknown fields.
// The global rtdb_config row is cleaned up so other tests on the shared pool
// are unaffected.
#[tokio::test]
async fn config_get_and_patch_round_trip() -> anyhow::Result<()> {
    let _rtdb_config_guard = RTDB_CONFIG_GUARD.lock().await;
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let bearer = "Bearer test-admin-key";

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;

    // GET: secrets are configured-bools, never values; hot + version + admins present.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/config"))
        .header("Authorization", bearer)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["adminKeyConfigured"], true);
    assert_eq!(body["databaseUrlConfigured"], true);
    assert_eq!(body["githubConfigured"], false);
    assert_eq!(body["googleConfigured"], false);
    assert!(!body.to_string().contains("test-admin-key"));
    assert!(body["hot"]["allowedOrigins"].is_array());
    assert!(body["hot"]["sessionTtlDays"].is_i64());
    assert!(body["version"].is_string());
    assert!(body["gitCommit"].is_string());
    assert!(body["admins"].is_array());

    // PATCH a hot value; response reflects it and it persisted to the table.
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "sessionTtlDays": 7 }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["hot"]["sessionTtlDays"],
        7
    );
    let loaded = rtdb_server::config::load_hot(&state.pool, &common::test_hot())
        .await?
        .unwrap();
    assert_eq!(loaded.session_ttl_days, 7);

    // PATCH idempotencyTtlMs; response reflects it, GET returns it, and it
    // persisted to the table. Proves the dedup TTL is a hot-config value.
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "idempotencyTtlMs": 60000 }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["hot"]["idempotencyTtlMs"],
        60000
    );
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/config"))
        .header("Authorization", bearer)
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["hot"]["idempotencyTtlMs"],
        60000
    );
    let loaded = rtdb_server::config::load_hot(&state.pool, &common::test_hot())
        .await?
        .unwrap();
    assert_eq!(loaded.idempotency_ttl_ms, 60000);

    // Invalid value -> 400.
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "sessionTtlDays": 0 }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // idempotencyTtlMs <= 0 -> 400 (must be positive).
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "idempotencyTtlMs": 0 }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Unknown (immutable) field -> 400 (deny_unknown_fields).
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "port": 9999 }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // No auth -> 401/403.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/config"))
        .send()
        .await?;
    assert!(
        resp.status() == reqwest::StatusCode::UNAUTHORIZED
            || resp.status() == reqwest::StatusCode::FORBIDDEN
    );

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    Ok(())
}

// CORS `allowed_origins` hot-reloads: after PATCH adds an origin, a preflight
// from that origin echoes access-control-allow-origin; a non-listed origin does
// not. Proves the AllowOrigin::predicate reads live HotConfig, not a startup
// snapshot.
#[tokio::test]
async fn config_cors_hot_reloads_allowed_origins() -> anyhow::Result<()> {
    let _rtdb_config_guard = RTDB_CONFIG_GUARD.lock().await;
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let bearer = "Bearer test-admin-key";

    // Add an origin via the hot-reload path (replaces the test_hot seed entirely).
    let origin = "https://dashboard.example.com";
    let resp = reqwest::Client::new()
        .request(
            reqwest::Method::PATCH,
            format!("http://{addr}/admin/config"),
        )
        .header("Authorization", bearer)
        .json(&serde_json::json!({ "allowedOrigins": [origin] }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Preflight from the now-allowed origin echoes ACAO.
    let resp = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/admin/dbs"))
        .header("Origin", origin)
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await?;
    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .unwrap()
            .to_str()
            .unwrap(),
        origin
    );

    // A non-listed origin receives no ACAO header.
    let resp = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/admin/dbs"))
        .header("Origin", "https://evil.example")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await?;
    assert!(resp.headers().get("access-control-allow-origin").is_none());

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await?;
    Ok(())
}

// --- Phase 5: admin document access --------------------------------------

fn owner_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{"notes":{"fields":{
        "title":{"type":"string"},
        "userId":{"type":"string"}
    },"indexes":[{"name":"by_user","fields":["userId"]}],"ownerField":"userId"}}})
}

fn items_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{"items":{"fields":{
        "name":{"type":"string"}
    },"indexes":[{"name":"by_name","fields":["name"]}]}}})
}

/// Seeds a `notes` row whose `userId` (the owner field) is `uid`, via the
/// test-direct executor (owner=None; the field value carries the ownership).
async fn seed_owned_note(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &rtdb_server::schema::SchemaDef,
    title: &str,
    uid: &str,
) {
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), title.into());
    doc.insert("userId".into(), uid.into());
    rtdb_server::txn::execute_txn(
        pool,
        db,
        schema,
        &rtdb_server::txn::Transaction {
            steps: vec![rtdb_server::txn::Step::Insert {
                table: "notes".into(),
                doc,
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
}

// Admin document read uses owner=None, so it sees every row on an ownerField
// table regardless of owner (a scoped user query would see only their own).
#[tokio::test]
async fn admin_query_bypasses_per_row_owner() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let schema =
        rtdb_server::ddl::push_schema(&pool, &db, serde_json::from_value(owner_schema_json())?)
            .await?;
    seed_owned_note(&pool, &db, &schema, "alice's note", "alice").await;
    seed_owned_note(&pool, &db, &schema, "bob's note", "bob").await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/query"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&serde_json::json!({"query":{"table":"notes"}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let mut titles: Vec<String> = body["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["title"].as_str().unwrap().to_string())
        .collect();
    titles.sort();
    assert_eq!(
        titles,
        vec!["alice's note".to_string(), "bob's note".to_string()]
    );
    Ok(())
}

// Admin document write goes through the committer (owner=None); an over-cap
// mutation (steps > RTDB_MAX_AFFECTED_DOCS, default 100) is rejected before it
// reaches the committer, so it writes nothing.
#[tokio::test]
async fn admin_mutate_writes_and_cap_rejects() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    rtdb_server::ddl::push_schema(&pool, &db, serde_json::from_value(items_schema_json())?).await?;
    let bearer = "Bearer test-admin-key";

    // A single-step insert succeeds and is durable.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/mutate"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"txn":{"steps":[{"op":"insert","table":"items","doc":{"name":"first"}}]}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 101 insert steps > default cap 100 -> 400.
    let steps: Vec<serde_json::Value> = (0..101)
        .map(|i| serde_json::json!({"op":"insert","table":"items","doc":{"name":format!("n{i}")}}))
        .collect();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/mutate"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"txn":{"steps":steps}}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // The over-cap batch wrote nothing: still exactly one item.
    let q: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/query"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"query":{"table":"items"}}))
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(q["result"].as_array().unwrap().len(), 1);
    Ok(())
}

// --- Phase 6: static hosting -------------------------------------------------

// ServeDir serves the SPA same-origin: index.html (no-cache), hashed assets
// (immutable), and index.html again for unknown GET paths (SPA fallback).
#[tokio::test]
async fn static_dir_serves_index_and_assets() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rtdb-static-{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(dir.join("assets"))?;
    std::fs::write(
        dir.join("index.html"),
        "<html><body>dashboard</body></html>",
    )?;
    std::fs::write(dir.join("assets").join("app.js"), "console.log(1)")?;

    let mut cfg = common::test_config();
    cfg.static_dir = Some(dir.to_string_lossy().to_string());
    let pool = sqlx::PgPool::connect(&cfg.database_url).await?;
    rtdb_server::db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool, cfg, common::test_hot());
    let addr = common::spawn_app(state).await;

    // GET / -> index.html, no-cache.
    let resp = reqwest::get(format!("http://{addr}/")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-cache, no-store, must-revalidate"
    );
    assert!(resp.text().await?.contains("dashboard"));

    // GET /assets/app.js -> immutable.
    let resp = reqwest::get(format!("http://{addr}/assets/app.js")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "public, max-age=31536000, immutable"
    );

    // SPA fallback: an unknown GET path returns index.html (no-cache), not 404.
    let resp = reqwest::get(format!("http://{addr}/dbs/some-db/tables")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .unwrap()
            .to_str()
            .unwrap(),
        "no-cache, no-store, must-revalidate"
    );
    assert!(resp.text().await?.contains("dashboard"));

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

// With no RTDB_STATIC_DIR, the server is API-only: GET / is a 404 (not
// index.html) and the API routes are unaffected.
#[tokio::test]
async fn no_static_dir_is_api_only() -> anyhow::Result<()> {
    let state = common::test_state().await; // static_dir = None
    let addr = common::spawn_app(state.clone()).await;

    let resp = reqwest::get(format!("http://{addr}/")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // API still works.
    let resp = common::admin_get(addr, "/admin/dbs").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
