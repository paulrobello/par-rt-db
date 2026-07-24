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
use std::sync::Arc;
use std::time::SystemTime;

use axum::http::{HeaderValue, Method, header};
use axum::{Router, routing::get};
use committer::Committers;
use config::Config;
use db::SchemaCache;
use subs::SubscriptionManager;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use auth::provider::OAuthStateEntry;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
    pub oauth_states: tokio::sync::Mutex<HashMap<String, OAuthStateEntry>>,
    pub started_at: SystemTime,
    pub metrics: Arc<metrics::Metrics>,
    pub op_feed: Arc<op_feed::OpFeed>,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config) -> Arc<Self> {
        let schemas = SchemaCache::new();
        let subs = SubscriptionManager::new();
        let op_feed = op_feed::OpFeed::new(1024, 500);
        let committers =
            Committers::new(pool.clone(), subs.clone(), schemas.clone(), op_feed.clone());
        let metrics = metrics::Metrics::new();
        Arc::new(Self {
            pool,
            config,
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

/// Origins allowed to send bearer tokens over CORS to `/auth/*` and the HTTP
/// one-shot API. Origins that fail `HeaderValue` parsing are skipped (and
/// logged) rather than rejecting startup — WS is exempt from CORS (Origin is
/// already enforced at OAuth start, see `auth::provider::provider_start`).
fn cors_layer(allowed_origins: &[String]) -> CorsLayer {
    let origins: Vec<HeaderValue> = allowed_origins
        .iter()
        .filter_map(|origin| match HeaderValue::from_str(origin) {
            Ok(value) => Some(value),
            Err(err) => {
                tracing::warn!(origin = %origin, error = %err, "skipping invalid CORS origin");
                None
            }
        })
        .collect();

    CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(&state.config.allowed_origins);

    Router::new()
        .route("/healthz", get(health::handler))
        .merge(admin::admin_routes())
        .merge(http_api::http_api_routes())
        .merge(ws::ws_routes())
        .merge(auth::provider::auth_routes())
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
