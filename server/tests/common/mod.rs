use std::future::IntoFuture;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::OnceLock;

use rtdb_server::config::HotConfig;
use rtdb_server::schema::SchemaDef;
use rtdb_server::{AppState, build_router, config::Config, db, ddl};
use tokio::sync::mpsc::{self, UnboundedSender};

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
        gitlab_client_id: None,
        gitlab_client_secret: None,
        gitlab_base_url: "https://gitlab.com".into(),
        oidc_client_id: None,
        oidc_client_secret: None,
        oidc_authorize_url: None,
        oidc_token_url: None,
        oidc_userinfo_url: None,
        max_affected_docs: 100,
        static_dir: None,
        pool_max_connections: 75,
        rate_limit_per_token_rpm: 0,
        rate_limit_per_db_rpm: 0,
        audit_log_enabled: false,
        oauth_login_csrf: true,
        webhooks_enabled: false,
        backup_enabled: false,
        backup_cron: "0 3 * * *".into(),
        backup_dir: "./backups".into(),
        backup_retention: 7,
        subs_verify_skip_every: 0,
        ttl_sweep_interval_secs: 60,
        ttl_batch: 5000,
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

#[allow(dead_code)]
pub async fn test_state() -> Arc<AppState> {
    let pool = sqlx::PgPool::connect(&test_config().database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, test_config(), test_hot())
}

/// Like `test_state` but with non-zero HTTP rate-limit ceilings. Used by
/// `tests/rate_limit_test.rs` to exercise the per-token and per-db fixed-window
/// limiter without touching env vars: the limiter reads
/// `state.config.rate_limit_per_{token,db}_rpm`, so we set them on the Config
/// before constructing AppState. This is the cleanest override path the
/// codebase exposes — `test_config()` is already a public helper.
#[allow(dead_code)]
pub async fn test_state_with_rate_limits(per_token_rpm: u32, per_db_rpm: u32) -> Arc<AppState> {
    let mut config = test_config();
    config.rate_limit_per_token_rpm = per_token_rpm;
    config.rate_limit_per_db_rpm = per_db_rpm;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state` but with `audit_log_enabled = true` and the
/// `rtdb.audit_log` table ensured. Used by `tests/audit_test.rs` to exercise
/// the durable audit log end-to-end without touching env vars. Mirrors the
/// `test_state_with_rate_limits` override pattern.
#[allow(dead_code)]
pub async fn test_state_with_audit() -> Arc<AppState> {
    let mut config = test_config();
    config.audit_log_enabled = true;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    db::ensure_audit_table(&pool)
        .await
        .expect("ensure rtdb.audit_log");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state` but with `webhooks_enabled = true` and the
/// `rtdb.webhooks` / `rtdb.webhook_deliveries` tables ensured. Used by
/// `tests/webhook_test.rs` to exercise the registry end-to-end without touching
/// env vars. Mirrors the `test_state_with_audit` override pattern.
#[allow(dead_code)]
pub async fn test_state_with_webhooks() -> Arc<AppState> {
    let mut config = test_config();
    config.webhooks_enabled = true;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    db::ensure_webhooks_tables(&pool)
        .await
        .expect("ensure rtdb.webhooks tables");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state` but with subscription skip-verification on at `every`
/// (1 = verify EVERY skip). Used by `tests/sub_invalidation_test.rs` to run the
/// invalidation soundness matrix in verified mode: every skip is shadow-checked
/// against a real re-run, so `subsMissedPushesTotal` must stay 0. Mirrors the
/// `test_state_with_audit` override pattern.
#[allow(dead_code)]
pub async fn test_state_with_skip_verification(every: u64) -> Arc<AppState> {
    let mut config = test_config();
    config.subs_verify_skip_every = every;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state` but with the TTL reaper's per-db sweep interval overridden
/// to `secs` (default is 60s). Used by `tests/ttl_test.rs` reaper tests so a
/// sweep lands within the test's poll window. The reaper only acts on tables
/// that declare `ttl`, so the shorter cadence is harmless for other tables.
/// Mirrors the `test_state_with_rate_limits` override pattern.
#[allow(dead_code)]
pub async fn test_state_with_ttl_sweep(secs: u64) -> Arc<AppState> {
    let mut config = test_config();
    config.ttl_sweep_interval_secs = secs;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state` but with `backup_dir` overridden. Used by ENH-002 Task 3's
/// `/admin/backup` trigger test to point at a tempdir — so the spawned `pg_dump`
/// (which calls `tokio::fs::create_dir_all` on `backup_dir` before running) does
/// not pollute the default `./backups` and break the parallel
/// `admin_list_backups_returns_empty_when_dir_missing` test, which asserts that
/// dir does not exist. Mirrors the `test_state_with_*` override pattern.
#[allow(dead_code)]
pub async fn test_state_with_backup_dir(dir: String) -> Arc<AppState> {
    let mut config = test_config();
    config.backup_dir = dir;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

/// Like `test_state_with_ttl_sweep` but ALSO enables the durable audit log and
/// webhook registry (ensuring their tables), so a reaper delete can be asserted
/// to publish through all four tap sites with `source = "ttl"`. Combines
/// `test_state_with_audit` + `test_state_with_webhooks` + the sweep override.
/// Used by `tests/ttl_test.rs`'s audit/webhook coverage test.
#[allow(dead_code)]
pub async fn test_state_with_ttl_audit_webhooks(secs: u64) -> Arc<AppState> {
    let mut config = test_config();
    config.ttl_sweep_interval_secs = secs;
    config.audit_log_enabled = true;
    config.webhooks_enabled = true;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");
    db::ensure_audit_table(&pool)
        .await
        .expect("ensure rtdb.audit_log");
    db::ensure_webhooks_tables(&pool)
        .await
        .expect("ensure rtdb.webhooks tables");
    AppState::new(pool, config, test_hot())
}

/// A DELETE helper, mirroring `admin_get`/`admin_post`. Used by webhook CRUD
/// tests.
#[allow(dead_code)]
pub async fn admin_delete(addr: SocketAddr, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .delete(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await
        .expect("send admin request")
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
pub async fn fresh_db(state: &Arc<AppState>) -> TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create fresh database");

    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema fixture");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push kanban schema");

    ensure_cleanup_worker(&state.config.database_url);
    TestDb(name)
}

/// Channel handle minted once per process by `ensure_cleanup_worker`; `TestDb`'s
/// `Drop` sends the database name here. Held in a `OnceLock` so the worker is
/// lazily, idempotently spawned on first use.
static CLEANUP_TX: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// A par-rt-db database created for a test. `Drop` schedules best-effort
/// teardown (DROP SCHEMA + registry deletes via `db::drop_database`) on a
/// dedicated worker thread that owns its own runtime + pool — independent of
/// the test's current-thread runtime, so cleanup runs after the test returns.
/// Behaves like a string via `Deref`/`AsRef`/`Display`.
#[derive(Debug)]
pub struct TestDb(pub String);

impl TestDb {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Serialize as a string so `serde_json::json!({"db": db, ...})` works without
/// an explicit `.as_str()` at every call site — consistent with "behaves like a
/// string". RAII is unaffected: the `TestDb` retains its name and still drops.
impl serde::Serialize for TestDb {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0)
    }
}

impl std::ops::Deref for TestDb {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TestDb {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Clone for TestDb {
    fn clone(&self) -> Self {
        TestDb(self.0.clone())
    }
}

impl From<TestDb> for String {
    fn from(mut t: TestDb) -> String {
        std::mem::take(&mut t.0)
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if let Some(tx) = CLEANUP_TX.get() {
            let name = std::mem::take(&mut self.0);
            // Skip when the name was already extracted (e.g. via `String::from`):
            // sending "" would queue a pointless `drop_database("")` that logs an error.
            if !name.is_empty() {
                let _ = tx.send(name);
            }
        }
    }
}

/// Wrap a database name created outside `fresh_db` (e.g. an inline
/// `db::create_database` call that pushes a custom schema instead of the kanban
/// fixture) in a `TestDb` so its `Drop` schedules cleanup. Ensures the worker
/// is running first — idempotent, so safe to call even if `fresh_db` or
/// `test_state` already initialized it. Use when a test cannot use `fresh_db`
/// because it needs a non-kanban schema or a bare (schema-less) database.
#[allow(dead_code)]
pub fn wrap_test_db(name: String) -> TestDb {
    ensure_cleanup_worker(&test_config().database_url);
    TestDb(name)
}

/// Lazily spawn the cleanup worker once per process on its own OS thread, with
/// its own runtime + pool built from `database_url`. Idempotent: the second and
/// later calls are no-ops. The worker owns its own `tokio::runtime::Runtime`
/// and `PgPool` so it survives the test's current-thread runtime shutting down
/// at return — cleanup proceeds after the test.
pub fn ensure_cleanup_worker(database_url: &str) {
    CLEANUP_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let url = database_url.to_string();
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("test-db cleanup worker: runtime build failed: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                let pool = match sqlx::postgres::PgPoolOptions::new()
                    .max_connections(4)
                    .connect(&url)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("test-db cleanup worker: connect failed: {e}");
                        return;
                    }
                };
                while let Some(name) = rx.recv().await {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        if let Err(e) = db::drop_database(&pool, &name).await {
                            eprintln!("test-db cleanup: drop {name} failed: {e}");
                        }
                    });
                }
            });
        });
        tx
    });
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
