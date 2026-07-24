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
