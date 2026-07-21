pub mod admin;
pub mod config;
pub mod db;
pub mod ddl;
pub mod error;
pub mod schema;
pub mod txn;

use std::sync::Arc;

use axum::{Router, routing::get};
use config::Config;
use db::SchemaCache;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
    pub schemas: SchemaCache,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config) -> Arc<Self> {
        Arc::new(Self {
            pool,
            config,
            schemas: SchemaCache::new(),
        })
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(admin::admin_routes())
        .with_state(state)
}
