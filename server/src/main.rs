//! Binary entry point: loads `Config` from the `RTDB_*` environment, connects
//! the Postgres pool, builds `AppState` and the router (API routes, admin
//! plane, storage, SPA fallback), and serves HTTP with graceful shutdown on
//! SIGTERM (the OTel exporter, if enabled, flushes on the way down).

use rtdb_server::{AppState, auth, build_router, config::Config, db};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() {
    // Config is parsed BEFORE the tracing subscriber is installed so the
    // runtime RTDB_OTEL_* knobs (master switch, endpoint, sample ratio) can
    // gate the OTLP layer at boot (ENH-018). Any boot error from config is
    // printed directly — there is no subscriber to log through yet.
    let config = Config::from_env().unwrap_or_else(|err| {
        eprintln!("failed to load configuration: {err}");
        std::process::exit(1);
    });

    // ENH-018: install the global tracing subscriber. Default build → stdout
    // fmt layer (byte-compatible with pre-ENH-018 behavior). `otel` feature +
    // RTDB_OTEL_ENABLED=true → composes an OpenTelemetryLayer exporting OTLP
    // spans to a collector. The guard flushes the exporter on shutdown_signal
    // resolution (held for the process lifetime below).
    let _otel_guard = rtdb_server::tracing_setup::init(&config);

    // ARC-126 / ENH-022: multi-instance status. With `RTDB_MULTI_INSTANCE=true`
    // (Stages 1–4), OAuth login state, the op-feed, presence, rate-limit
    // budgets (`rtdb_auth.rate_counters`), and per-database write ownership
    // (advisory-lock lease on a dedicated connection; the owner runs the db's
    // committer on it, kill -9 hands the lease to the next taker) all
    // coordinate across replicas. Remaining limitation (Stage 4c): a
    // non-owner replica replies CONFLICT to writes until it takes the lease —
    // writes must reach the owning replica; automatic forwarding is the
    // follow-up. INFO (not WARN): the constraint is loud (CONFLICT errors, not
    // silent corruption) and a single instance needs no action.
    if !config.multi_instance {
        tracing::info!(
            "single-instance topology: multi-instance coordination (login \
             state, op-feed, presence, shared rate budgets, per-db write \
             ownership) activates with RTDB_MULTI_INSTANCE=true (ENH-022)"
        );
    }

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

    // ENH-022 Stage 1: gated background sweep of expired OAuth state rows.
    // `rtdb_auth.oauth_states` is the cross-replica login-state table; rows
    // live for the 10-minute login TTL and are also pruned opportunistically at
    // `/begin`, but this task bounds the table even when `/begin` is idle
    // (closes the SEC-132 unbounded-map note). It writes nothing to document
    // tables — a pure reader+DELETE on a side table — so the committer's
    // single-writer invariant is untouched. Runs for the process lifetime;
    // best-effort (errors logged, never aborts).
    {
        let sweep_pool = pool.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                match auth::provider::sweep_oauth_states(&sweep_pool).await {
                    Ok(n) if n > 0 => {
                        tracing::debug!(expired = n, "oauth state sweep: pruned rows");
                    }
                    Ok(_) => {}
                    Err(err) => tracing::warn!(error = %err, "oauth state sweep failed"),
                }
            }
        });
    }

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
        // SEC-115: every webhook needs a signing secret before the delivery
        // worker drains, so backfill any legacy NULL-secret rows now.
        rtdb_server::webhook::backfill_webhook_secrets(&pool)
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "failed to backfill webhook secrets");
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
    } else {
        tracing::info!(
            "managed backups are disabled (RTDB_BACKUP_ENABLED is false); \
             no scheduled pg_dump will run — set RTDB_BACKUP_ENABLED=true to enable"
        );
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
    // missing row is normal (first boot). A row missing newer fields is NOT an
    // error — `load_hot` overlays it onto the env seed field by field, so an
    // operator's persisted PATCH survives a release that adds a setting. Only a
    // DB error or a structurally invalid row falls back wholesale, logged.
    let env_hot = rtdb_server::config::HotConfig::from_env();
    let hot = match rtdb_server::config::load_hot(&pool, &env_hot).await {
        Ok(Some(h)) => h,
        Ok(None) => env_hot,
        Err(e) => {
            tracing::warn!(error = %e, "failed to load rtdb_config; falling back to env defaults");
            env_hot
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

    // `into_make_service_with_connect_info` makes the peer `SocketAddr`
    // available to handlers via the `ConnectInfo<SocketAddr>` extractor — used
    // by the per-IP rate limit on the unauthenticated public storage serve
    // route (SEC-004). When deploying behind a trusted proxy (the Cloudflare
    // tunnel in production) the IP is read from `X-Forwarded-For` first, with
    // ConnectInfo as the fallback for direct connections.
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
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
