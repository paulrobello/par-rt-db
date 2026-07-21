pub mod admin;
pub mod auth;
pub mod committer;
pub mod config;
pub mod db;
pub mod ddl;
pub mod error;
pub mod http_api;
pub mod protocol;
pub mod query;
pub mod schema;
pub mod subs;
pub mod txn;
pub mod ws;

use std::collections::HashMap;
use std::sync::Arc;

use axum::{Router, routing::get};
use committer::Committers;
use config::Config;
use db::SchemaCache;
use subs::SubscriptionManager;

use auth::github::OAuthStateEntry;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
    pub oauth_states: tokio::sync::Mutex<HashMap<String, OAuthStateEntry>>,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config) -> Arc<Self> {
        let schemas = SchemaCache::new();
        let subs = SubscriptionManager::new();
        let committers = Committers::new(pool.clone(), subs.clone(), schemas.clone());
        Arc::new(Self {
            pool,
            config,
            schemas,
            subs,
            committers,
            oauth_states: tokio::sync::Mutex::new(HashMap::new()),
        })
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(admin::admin_routes())
        .merge(http_api::http_api_routes())
        .merge(ws::ws_routes())
        .merge(auth::github::auth_routes())
        .with_state(state)
}
