use rtdb_server::{AppState, build_router, config::Config, db};
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
        .max_connections(10)
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

    let port = config.port;
    let state = AppState::new(pool, config);
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
