pub mod admin;
pub mod audit;
pub mod auth;
pub mod backup;
pub mod committer;
pub mod config;
pub mod db;
pub mod ddl;
pub mod error;
pub mod health;
pub mod http_api;
pub mod image_transform;
pub mod metrics;
pub mod migrate;
pub mod mutation_log;
pub mod op_feed;
pub mod pagination;
pub mod privacy;
pub mod protocol;
pub mod query;
pub mod rate_limit;
pub mod reaper;
pub mod scheduler;
pub mod schema;
pub mod schema_diff;
pub mod schema_history;
pub mod snapshot;
pub mod storage;
pub mod subs;
pub mod txn;
pub mod webhook;
pub mod ws;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::{Router, routing::get};
use committer::Committers;
use config::{Config, HotConfig};
use db::SchemaCache;
use subs::SubscriptionManager;
use tower::ServiceBuilder;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

use auth::provider::OAuthStateEntry;

/// Realtime execution core: subscription state, the per-db committer tasks,
/// and the live op-feed tap they publish to. Grouped so handlers that only
/// need the reactive surface can reach for `state.realtime` as a unit.
pub struct Realtime {
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
    pub op_feed: Arc<op_feed::OpFeed>,
}

/// Runtime-wide mutable/process state: hot-reloaded config, metrics, and the
/// server's boot timestamp. Grouped so the `/admin/*` and health surfaces can
/// reach for `state.runtime` as a unit.
pub struct Runtime {
    pub hot: Arc<ArcSwap<HotConfig>>,
    pub metrics: Arc<metrics::Metrics>,
    pub started_at: SystemTime,
}

/// Per-instance auth bookkeeping that is neither a config value nor a realtime
/// concern. Grouped so `state.auth` is the only OAuth-session seam.
pub struct Auth {
    pub oauth_states: tokio::sync::Mutex<HashMap<String, OAuthStateEntry>>,
}

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub realtime: Realtime,
    pub runtime: Runtime,
    pub auth: Auth,
    pub rate_limiter: Arc<rate_limit::RateLimiter>,
    /// In-progress flag for the manual `/admin/backup` trigger. Set
    /// synchronously in the handler before the spawned `pg_dump` task and
    /// cleared on completion (success or failure). Read by `GET /admin/backups`
    /// to populate `running`, and checked-and-set by the trigger to enforce
    /// "one manual backup at a time" (409 on conflict). Atomic because the
    /// trigger handler, the spawned dump task, and the listing handler all
    /// touch it without serialization — the same way the cron backup task
    /// (which never touches this flag) runs alongside them.
    pub backup_running: Arc<AtomicBool>,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config, hot: HotConfig) -> Arc<Self> {
        let schemas = SchemaCache::new();
        let metrics = metrics::Metrics::new();
        // The subscription manager records invalidation effectiveness on the
        // same `Metrics` the dashboard reads, and owns the skip-verification
        // sampler — so it is built after metrics and before the committers.
        let subs = SubscriptionManager::with_instrumentation(
            Some(metrics.clone()),
            config.subs_verify_skip_every,
        );
        let op_feed = op_feed::OpFeed::new(1024, 500);
        let hot = Arc::new(ArcSwap::from_pointee(hot));
        let committers = Committers::new(
            pool.clone(),
            subs.clone(),
            schemas.clone(),
            op_feed.clone(),
            hot.clone(),
            config.audit_log_enabled,
            config.webhooks_enabled,
            config.ttl_sweep_interval_secs,
            config.ttl_batch,
            metrics.clone(),
        );
        Arc::new(Self {
            pool,
            config,
            schemas,
            realtime: Realtime {
                subs,
                committers,
                op_feed,
            },
            runtime: Runtime {
                hot,
                metrics,
                started_at: SystemTime::now(),
            },
            auth: Auth {
                oauth_states: tokio::sync::Mutex::new(HashMap::new()),
            },
            rate_limiter: rate_limit::RateLimiter::new(),
            backup_running: Arc::new(AtomicBool::new(false)),
        })
    }
}

/// Origins are decided per request from live `HotConfig`, so `PATCH /admin/config`
/// can add an origin and have it take effect without a restart. The layer itself
/// is still constructed once at router build time; only the origin decision is
/// dynamic. WS is CORS-exempt (Origin is enforced at OAuth start, unchanged).
fn cors_layer(hot: Arc<ArcSwap<HotConfig>>) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _parts| {
            let h = hot.load();
            match origin.to_str() {
                Ok(val) => h
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed.as_str() == val),
                Err(_) => false,
            }
        }))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
        .allow_credentials(true)
}

/// Sets `Cache-Control` on static responses from their Content-Type: the SPA
/// shell (text/html — including the index served for unknown paths by the SPA
/// fallback) is `no-cache` so a new deploy's index.html is always fetched (and
/// then references the newest hashed assets); every other static asset is
/// `immutable`. Wraps only the static `ServeDir`, never the API/admin/WS/auth
/// routes.
async fn set_static_cache_headers(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let no_cache = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/html"));
    let cc = if no_cache {
        "no-cache, no-store, must-revalidate"
    } else {
        "public, max-age=31536000, immutable"
    };
    if let Ok(value) = HeaderValue::from_str(cc) {
        resp.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    resp
}

/// `GET /metrics`: unauthenticated Prometheus text-exposition scrape endpoint.
///
/// Content-negotiates on `Accept` so the operator dashboard's browser route at
/// `/metrics` (SPA) still works: a browser (`Accept: text/html,...`) is served
/// the SPA's `index.html` when `RTDB_STATIC_DIR` is configured; everything else
/// (Prometheus's scraper sends `application/openmetrics-text;...,text/plain;...`,
/// curl, API-only deploys) gets Prometheus text. Safe to expose without auth —
/// `MetricsSnapshot` is aggregate-only (no per-db, no principal data), same
/// posture as `/healthz`. The existing admin JSON snapshot stays at
/// `/admin/metrics` (admin-gated); this endpoint does not replace it.
async fn prometheus_metrics_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Response {
    let wants_html = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/html"));

    if wants_html
        && let Some(dir) = state.config.static_dir.as_deref()
        && let Ok(index) = tokio::fs::read_to_string(format!("{dir}/index.html")).await
    {
        let mut h = HeaderMap::new();
        h.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/html; charset=utf-8"),
        );
        h.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("no-cache, no-store, must-revalidate"),
        );
        return (StatusCode::OK, h, index).into_response();
    }

    let snap = state
        .runtime
        .metrics
        .snapshot(&state.pool, &state.realtime.subs, state.runtime.started_at)
        .await;
    let body =
        metrics::render_prometheus(&snap, env!("CARGO_PKG_VERSION"), env!("BUILD_GIT_COMMIT"));
    let mut h = HeaderMap::new();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    (StatusCode::OK, h, body).into_response()
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.runtime.hot.clone());

    let mut router = Router::new()
        .route("/healthz", get(health::handler))
        .route("/privacy", get(privacy::handler))
        .route("/metrics", get(prometheus_metrics_handler))
        .merge(admin::admin_routes())
        .merge(http_api::http_api_routes())
        .merge(ws::ws_routes())
        .merge(auth::provider::auth_routes());

    // Static SPA hosting, mounted LAST as the fallback so it can never shadow a
    // real route. Only when RTDB_STATIC_DIR is set and the directory exists;
    // otherwise the server is API-only (today's behavior). `static_dir` is
    // cloned out so `state` can still move into `with_state` below.
    if let Some(dir) = state.config.static_dir.clone()
        && Path::new(&dir).is_dir()
    {
        let serve_dir =
            ServeDir::new(dir.clone()).fallback(ServeFile::new(format!("{dir}/index.html")));
        router = router.fallback_service(
            ServiceBuilder::new()
                .layer(from_fn(set_static_cache_headers))
                .service(serve_dir),
        );
    }

    router
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
