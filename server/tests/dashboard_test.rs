mod common;

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
    assert_eq!(state.subs.count().await, 0);

    let snap = m.snapshot(&state.pool, &state.subs, state.started_at).await;
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
