use rtdb_server::{AppState, build_router, config::Config};

fn test_config() -> Config {
    Config {
        port: 0,
        database_url: std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into()),
        admin_key: "test-admin-key".into(),
        public_url: "http://localhost:0".into(),
        allowed_origins: vec!["http://localhost:5173".into()],
        github_client_id: None,
        github_client_secret: None,
        github_base_url: "https://github.com".into(),
        github_api_url: "https://api.github.com".into(),
        session_ttl_days: 30,
    }
}

#[tokio::test]
async fn healthz_returns_ok() -> anyhow::Result<()> {
    let pool = sqlx::PgPool::connect(&test_config().database_url).await?;
    let state = AppState::new(pool, test_config());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(axum::serve(listener, build_router(state)).into_future());
    let body = reqwest::get(format!("http://{addr}/healthz"))
        .await?
        .text()
        .await?;
    assert_eq!(body, "ok");
    Ok(())
}
