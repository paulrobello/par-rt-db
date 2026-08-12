use rtdb_server::config::HotConfig;
use rtdb_server::{AppState, build_router, config::Config};

fn test_config() -> Config {
    Config {
        port: 0,
        database_url: std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into()),
        admin_key: "canary-secret-admin-key".into(),
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
        microsoft_client_id: None,
        microsoft_client_secret: None,
        microsoft_tenant: "common".into(),
        apple_client_id: None,
        apple_team_id: None,
        apple_key_id: None,
        apple_private_key: None,
        max_affected_docs: 100,
        static_dir: None,
        pool_max_connections: 75,
        schema_cache_max_entries: 1024,
        slow_query_ms: 0,
        slow_query_capacity: 200,
        slow_query_log_params: false,
        rate_limit_per_token_rpm: 0,
        rate_limit_per_db_rpm: 0,
        audit_log_enabled: false,
        oauth_login_csrf: true,
        webhooks_enabled: false,
        webhook_allow_http: false,
        storage_rate_limit_per_ip_rpm: 0,
        storage_require_signed_urls: false,
        backup_enabled: false,
        backup_cron: "0 3 * * *".into(),
        backup_dir: "./backups".into(),
        backup_retention: 7,
        subs_verify_skip_every: 0,
        ttl_sweep_interval_secs: 60,
        ttl_batch: 5000,
        image_transforms_enabled: true,
        image_max_dim: 2048,
        image_max_pixels: 25_000_000,
        image_cache_bytes: 256 * 1024 * 1024,
        image_concurrency: 4,
        image_default_quality: 80,
        presence_enabled: false,
        presence_max_state_bytes: 1024,
        presence_max_room_size: 100,
        presence_max_rooms_per_conn: 32,
        presence_max_room_bytes: 256,
        presence_broadcast_interval_ms: 50,
        presence_update_limit_per_sec: 20,
        presence_max_ttl_ms: 300_000,
        auth_anonymous_enabled: false,
        anonymous_session_ttl_days: 1,
        anonymous_rate_limit_per_ip_rpm: 0,
        quota_cache_ttl_secs: 60,
        db_idle_reclaim_secs: 0,
        admin_rate_limit_per_ip_rpm: 0,
        cookie_secure: false,
        otel_enabled: false,
        otel_endpoint: String::new(),
        otel_service_name: String::new(),
        otel_sample_ratio: 0.0,
        multi_instance: false,
        instance_id: None,
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
