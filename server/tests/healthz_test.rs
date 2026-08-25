use rtdb_server::config::HotConfig;
use rtdb_server::{
    AppState, build_router,
    config::{Config, OAuthConfig},
};

fn test_config() -> Config {
    Config {
        port: 0,
        admin_key: "canary-secret-admin-key".into(),
        public_url: "http://localhost:0".into(),
        oauth: OAuthConfig::default(),
        max_affected_docs: 100,
        static_dir: None,
        pool_max_connections: 75,
        schema_cache_max_entries: 1024,
        slow_query_ms: 0,
        slow_query_capacity: 200,
        slow_query_log_params: false,
        audit_log_enabled: false,
        oauth_login_csrf: true,
        webhooks_enabled: false,
        webhook_allow_http: false,
        subs_verify_skip_every: 0,
        ttl_sweep_interval_secs: 60,
        ttl_batch: 5000,
        presence_enabled: false,
        presence_max_state_bytes: 1024,
        presence_max_room_size: 100,
        presence_max_rooms_per_conn: 32,
        presence_max_room_bytes: 256,
        presence_broadcast_interval_ms: 50,
        presence_update_limit_per_sec: 20,
        presence_max_ttl_ms: 300_000,
        presence_beat_interval_ms: 5000,
        presence_beat_timeout_ms: 15000,
        auth_anonymous_enabled: false,
        anonymous_session_ttl_days: 1,
        quota_cache_ttl_secs: 60,
        db_idle_reclaim_secs: 0,
        cookie_secure: false,
        trusted_proxy: false,
        otel_enabled: false,
        otel_endpoint: String::new(),
        otel_service_name: String::new(),
        otel_sample_ratio: 0.0,
        limits: rtdb_server::config::LimitsConfig {
            per_token_rpm: 0,
            per_db_rpm: 0,
            exact: false,
            sync_ms: 1000,
            storage_per_ip_rpm: 0,
            anonymous_per_ip_rpm: 0,
            admin_per_ip_rpm: 0,
        },
        storage: rtdb_server::config::StorageConfig {
            require_signed_urls: false,
            image: rtdb_server::config::ImageTransformConfig {
                enabled: true,
                max_dim: 2048,
                max_pixels: 25_000_000,
                cache_bytes: 256 * 1024 * 1024,
                concurrency: 4,
                default_quality: 80,
            },
        },
        backup: rtdb_server::config::BackupConfig {
            enabled: false,
            cron: "0 3 * * *".into(),
            dir: "./backups".into(),
            retention: 7,
        },
        multi_instance: rtdb_server::config::MultiInstanceConfig {
            enabled: false,
            instance_id: None,
            forward_timeout_ms: 5000,
            forward_concurrency: 64,
        },
        database_url: std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into()),
    }
}

fn test_hot() -> HotConfig {
    HotConfig {
        allowed_origins: vec!["http://localhost:5173".into()],
        session_ttl_days: 30,
        max_file_size: 50 * 1024 * 1024,
        idempotency_ttl_ms: rtdb_server::mutation_log::DEFAULT_DEDUP_TTL_MS,
        max_tables_per_db: 0,
        max_storage_bytes_per_db: 0,
        max_subs_per_db: 0,
    }
}

#[tokio::test]
async fn healthz_returns_diagnostics() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&test_config().database_url).await?;
    let state = AppState::new(pool, test_config(), test_hot());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(axum::serve(listener, build_router(state)).into_future());

    let resp = reqwest::get(format!("http://{addr}/healthz")).await?;
    assert_eq!(resp.status(), 200);
    let text = resp.text().await?;
    // The route is unauthenticated: no configured secret may appear in the body.
    assert!(!text.contains("canary"), "healthz leaked a secret: {text}");

    let body: serde_json::Value = serde_json::from_str(&text)?;
    assert_eq!(body["status"], "ok");
    assert_eq!(body["postgres"], true);
    // SEC-129: the build fingerprint (version/git_commit/build_timestamp) is
    // gated behind admin auth; an unauthenticated request must not leak it.
    assert!(
        body["version"].is_null(),
        "version leaked unauthenticated: {text}"
    );
    assert!(
        body["git_commit"].is_null(),
        "git_commit leaked unauthenticated: {text}"
    );
    assert!(
        body["build_timestamp"].is_null(),
        "build_timestamp leaked unauthenticated: {text}"
    );
    assert!(body["started_at"].is_string());
    assert!(
        body["uptime_seconds"].as_u64().unwrap() < 120,
        "uptime should be near zero for a freshly spawned server"
    );

    // An admin-authed request DOES surface the fingerprint (SEC-129).
    let admin_resp = reqwest::Client::new()
        .get(format!("http://{addr}/healthz"))
        .header("Authorization", "Bearer canary-secret-admin-key")
        .send()
        .await?;
    assert_eq!(admin_resp.status(), 200);
    let admin_body: serde_json::Value = serde_json::from_str(&admin_resp.text().await?)?;
    assert_eq!(admin_body["version"], env!("CARGO_PKG_VERSION"));
    assert!(admin_body["git_commit"].is_string());
    assert!(admin_body["build_timestamp"].is_string());
    Ok(())
}

async fn spawn_for_cors() -> anyhow::Result<std::net::SocketAddr> {
    let pool = sqlx::PgPool::connect(&test_config().database_url).await?;
    let state = AppState::new(pool, test_config(), test_hot());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(axum::serve(listener, build_router(state)).into_future());
    Ok(addr)
}

#[tokio::test]
async fn cors_preflight_echoes_allowed_origin() -> anyhow::Result<()> {
    let addr = spawn_for_cors().await?;

    let resp = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/api/query"))
        .header("Origin", "http://localhost:5173")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await?;

    assert_eq!(
        resp.headers()
            .get("access-control-allow-origin")
            .map(|v| v.to_str().unwrap()),
        Some("http://localhost:5173")
    );
    Ok(())
}

// ARC-013: `X-Rtdb-Protocol` rides every SDK HTTP call, so it must survive the
// CORS preflight — a cross-origin browser caller lists it in
// `Access-Control-Request-Headers`, and an `allow_headers` set that omits it
// fails the preflight and blocks the request before any handler runs.
#[tokio::test]
async fn cors_preflight_allows_the_protocol_version_header() -> anyhow::Result<()> {
    let addr = spawn_for_cors().await?;

    let resp = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/api/query"))
        .header("Origin", "http://localhost:5173")
        .header("Access-Control-Request-Method", "POST")
        .header("Access-Control-Request-Headers", "x-rtdb-protocol")
        .send()
        .await?;

    let allowed = resp
        .headers()
        .get("access-control-allow-headers")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    assert!(
        allowed.contains("x-rtdb-protocol"),
        "preflight must allow the protocol-version header, got: {allowed:?}"
    );
    Ok(())
}

#[tokio::test]
async fn cors_preflight_omits_header_for_disallowed_origin() -> anyhow::Result<()> {
    let addr = spawn_for_cors().await?;

    let resp = reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("http://{addr}/api/query"))
        .header("Origin", "http://evil.example")
        .header("Access-Control-Request-Method", "POST")
        .send()
        .await?;

    assert!(resp.headers().get("access-control-allow-origin").is_none());
    Ok(())
}
