pub mod admin;
pub mod committer;
pub mod config;
pub mod db;
pub mod ddl;
pub mod error;
pub mod protocol;
pub mod query;
pub mod schema;
pub mod subs;
pub mod txn;

use std::sync::Arc;

use axum::{Router, routing::get};
use committer::Committers;
use config::Config;
use db::SchemaCache;
use subs::SubscriptionManager;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
    pub subs: Arc<SubscriptionManager>,
    pub committers: Committers,
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
        })
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(admin::admin_routes())
        .with_state(state)
}
