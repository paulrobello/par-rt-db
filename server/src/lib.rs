pub mod admin;
pub mod auth;
pub mod committer;
pub mod config;
pub mod db;
pub mod ddl;
pub mod error;
pub mod health;
pub mod http_api;
pub mod metrics;
pub mod mutation_log;
pub mod op_feed;
pub mod pagination;
pub mod protocol;
pub mod query;
pub mod scheduler;
pub mod schema;
pub mod snapshot;
pub mod storage;
pub mod subs;
pub mod txn;
pub mod ws;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use arc_swap::ArcSwap;
use axum::extract::Request;
use axum::http::{HeaderValue, Method, header};
use axum::middleware::{Next, from_fn};
use axum::response::Response;
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

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub hot: Arc<ArcSwap<HotConfig>>,
    pub schemas: SchemaCache,
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
    pub oauth_states: tokio::sync::Mutex<HashMap<String, OAuthStateEntry>>,
    pub started_at: SystemTime,
    pub metrics: Arc<metrics::Metrics>,
    pub op_feed: Arc<op_feed::OpFeed>,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config, hot: HotConfig) -> Arc<Self> {
        let schemas = SchemaCache::new();
        let subs = SubscriptionManager::new();
        let op_feed = op_feed::OpFeed::new(1024, 500);
        let committers =
            Committers::new(pool.clone(), subs.clone(), schemas.clone(), op_feed.clone());
        let metrics = metrics::Metrics::new();
        Arc::new(Self {
            pool,
            config,
            hot: Arc::new(ArcSwap::from_pointee(hot)),
            schemas,
            subs,
            committers,
            oauth_states: tokio::sync::Mutex::new(HashMap::new()),
            started_at: SystemTime::now(),
            metrics,
            op_feed,
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

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.hot.clone());

    let mut router = Router::new()
        .route("/healthz", get(health::handler))
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
