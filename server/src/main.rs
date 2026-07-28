use rtdb_server::{AppState, auth, build_router, config::Config, db};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let config = Config::from_env().unwrap_or_else(|err| {
        eprintln!("failed to load configuration: {err}");
        std::process::exit(1);
    });

    let pool = PgPoolOptions::new()
        .max_connections(config.pool_max_connections)
        // A small warm pool keeps first-of-burst requests off the connect
        // critical path; the committer-per-db model holds connections during
        // fan-out re-runs so a non-zero floor reduces latency variance.
        .min_connections(5)
        // Fail fast under saturation (the sqlx default is 30s, which is a
        // long stall before the eventual `PoolAcquireTimeout` surfaces). 10s
        // is long enough to ride out a brief fan-out spike, short enough that
        // a saturated pool surfaces a diagnosable error to the client.
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&config.database_url)
        .await
        .unwrap_or_else(|err| {
            eprintln!("failed to connect to database: {err}");
            std::process::exit(1);
        });

    db::bootstrap(&pool).await.unwrap_or_else(|err| {
        eprintln!("failed to bootstrap database: {err}");
        std::process::exit(1);
    });

    // Durable audit log table: only ensured when the feature is enabled at
    // boot. When off the table is permitted to not exist, and the
    // `GET /admin/audit` endpoint returns an empty list.
    if config.audit_log_enabled {
        db::ensure_audit_table(&pool).await.unwrap_or_else(|err| {
            tracing::warn!(error = %err, "failed to ensure rtdb.audit_log table");
        });
    }

    // Webhook registry + delivery outbox: only ensured when the feature is
    // enabled at boot. When on, also spawn the single delivery worker that
    // drains the outbox. The worker is best-effort (never panics on transient
    // errors) and runs until the process exits.
    if config.webhooks_enabled {
        db::ensure_webhooks_tables(&pool)
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "failed to ensure rtdb.webhooks tables");
            });
        tokio::spawn(rtdb_server::webhook::run_delivery_worker(pool.clone()));
    }

    // Managed pg_dump backup task: off by default. When on, a single
    // background task runs `pg_dump` on the configured cron into `backup_dir`,
    // retaining the newest `backup_retention` dumps. The task sleeps in bounded
    // chunks and never aborts the server on a pg_dump/prune failure (logged +
    // continued). The connection string is passed as PG* env vars, never argv.
    if config.backup_enabled {
        let db_url = config.database_url.clone();
        let dir = config.backup_dir.clone();
        let cron = config.backup_cron.clone();
        let retention = config.backup_retention;
        tokio::spawn(rtdb_server::backup::run_backup_task(
            db_url, dir, cron, retention,
        ));
    }

    let admin_emails: Vec<String> = match std::env::var("RTDB_ADMIN_EMAILS") {
        Ok(v) if !v.is_empty() => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    };
    auth::seed_admin_emails(&pool, &admin_emails)
        .await
        .unwrap_or_else(|err| {
            tracing::warn!(error = %err, "failed to seed admin emails");
        });

    // Hot config: load the persisted row if present, else seed from env. A
    // missing row is normal (first boot); a malformed row or DB error is logged
    // and falls back to env defaults rather than blocking startup.
    let hot = match rtdb_server::config::load_hot(&pool).await {
        Ok(Some(h)) => h,
        Ok(None) => rtdb_server::config::HotConfig::from_env(),
        Err(e) => {
            tracing::warn!(error = %e, "failed to load rtdb_config; falling back to env defaults");
            rtdb_server::config::HotConfig::from_env()
        }
    };

    let port = config.port;
    let state = AppState::new(pool, config, hot);
    let router = build_router(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|err| {
            eprintln!("failed to bind to port {port}: {err}");
            std::process::exit(1);
        });

    tracing::info!("listening on 0.0.0.0:{port}");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|err| {
            eprintln!("server error: {err}");
            std::process::exit(1);
        });
}

/// Resolves on SIGINT or SIGTERM, whichever arrives first. If a signal
/// handler fails to install, that branch never resolves (logged once) rather
/// than panicking — the other signal (or an operator's stronger signal) still
/// terminates the process.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "failed to install SIGINT handler");
            std::future::pending::<()>().await;
        }
    };

    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }

    tracing::info!("shutting down");
}
