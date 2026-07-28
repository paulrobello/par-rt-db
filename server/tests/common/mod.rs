use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;

use rtdb_server::config::HotConfig;
use rtdb_server::schema::SchemaDef;
use rtdb_server::{AppState, build_router, config::Config, db, ddl};

pub fn test_config() -> Config {
    Config {
        port: 0,
        database_url: std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into()),
        admin_key: "test-admin-key".into(),
        public_url: "http://localhost:0".into(),
        github_client_id: None,
        github_client_secret: None,
        github_base_url: "https://github.com".into(),
        github_api_url: "https://api.github.com".into(),
        google_client_id: None,
        google_client_secret: None,
        max_affected_docs: 100,
        static_dir: None,
        pool_max_connections: 75,
    }
}

/// Hot-config seed for tests: mirrors the pre-split `test_config` defaults, so
/// the existing CORS/origin tests (which rely on `http://localhost:5173` being
/// allowed) keep passing now that origins live behind `state.runtime.hot`.
#[allow(dead_code)]
pub fn test_hot() -> HotConfig {
    HotConfig {
        allowed_origins: vec!["http://localhost:5173".into()],
        session_ttl_days: 30,
        max_file_size: 50 * 1024 * 1024,
        idempotency_ttl_ms: rtdb_server::mutation_log::DEFAULT_DEDUP_TTL_MS,
    }
}

pub async fn test_state() -> Arc<AppState> {
    let pool = sqlx::PgPool::connect(&test_config().database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, test_config(), test_hot())
}

#[allow(dead_code)]
pub async fn spawn_app(state: Arc<AppState>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    tokio::spawn(axum::serve(listener, build_router(state)).into_future());
    addr
}

#[allow(dead_code)]
pub fn kanban_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
      "projects":{
        "fields":{
          "name":{"type":"string"},
          "description":{"type":"optional","inner":{"type":"string"}},
          "status":{"type":"union","variants":[
            {"type":"literal","value":"active"},{"type":"literal","value":"paused"},
            {"type":"literal","value":"dormant"},{"type":"literal","value":"archived"}]},
          "tags":{"type":"array","element":{"type":"string"}},
          "updatedAt":{"type":"number"}},
        "indexes":[{"name":"by_name","fields":["name"]},{"name":"by_status","fields":["status"]}]},
      "workItems":{
        "fields":{
          "projectId":{"type":"id","table":"projects"},
          "title":{"type":"string"},
          "status":{"type":"union","variants":[
            {"type":"literal","value":"backlog"},{"type":"literal","value":"in_progress"},
            {"type":"literal","value":"blocked"},{"type":"literal","value":"done"}]},
          "order":{"type":"number"},
          "completedAt":{"type":"optional","inner":{"type":"number"}}},
        "indexes":[{"name":"by_project","fields":["projectId"]},
                   {"name":"by_status","fields":["status"]},
                   {"name":"by_project_and_status","fields":["projectId","status"]},
                   {"name":"by_project_and_order","fields":["projectId","order"]},
                   {"name":"by_project_status_order","fields":["projectId","status","order"]}]}
    }})
}

#[allow(dead_code)]
pub async fn fresh_db(state: &Arc<AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create fresh database");

    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema fixture");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push kanban schema");

    name
}

#[allow(dead_code)]
pub async fn admin_post(
    addr: SocketAddr,
    path: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .expect("send admin request")
}

#[allow(dead_code)]
pub async fn admin_get(addr: SocketAddr, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await
        .expect("send admin request")
}

#[allow(dead_code)]
pub async fn admin_post_raw(addr: SocketAddr, path: &str, body: String) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .body(body)
        .send()
        .await
        .expect("send admin request")
}

/// Seeds a real `rtdb_auth.users` + `rtdb_auth.sessions` row for `user_id` /
/// `email` and returns a bearer session token that `resolve_bearer` maps to
/// `Principal::User { user_id, .. }`. Caller must allowlist `email` for the
/// target db separately. Distinct `user_id`s produce distinct `github_id`s
/// (the `users` table enforces uniqueness) via a stable hash of the id, so two
/// calls with different ids never collide. Mirrors `oauth_test.rs`'s direct
/// seed (column names / `sha256_hex` / `now_ms` / `random_token`).
#[allow(dead_code)]
pub async fn mint_user_session(pool: &sqlx::PgPool, user_id: &str, email: &str) -> String {
    // Stable distinct github_id per user_id: sha256 of the id gives distinct
    // bytes; 15 hex nibbles = 60 bits, always non-negative and under i64::MAX
    // (same approach oauth_test uses with random_token, but deterministic).
    let github_id: i64 =
        i64::from_str_radix(&db::sha256_hex(user_id)[..15], 16).expect("parse hex as i64");
    let now = db::now_ms();
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(user_id)
    .bind(github_id)
    .bind(email)
    .bind(email)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert rtdb_auth.users row");

    let token = db::random_token();
    let hash = db::sha256_hex(&token);
    sqlx::query(
        "INSERT INTO rtdb_auth.sessions (token_hash, user_id, expires_at, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&hash)
    .bind(user_id)
    .bind(now + 31 * 86_400 * 1_000)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert rtdb_auth.sessions row");

    token
}
