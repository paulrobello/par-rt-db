pub mod config;
pub mod error;
pub mod schema;

use std::sync::Arc;

use axum::{Router, routing::get};
use config::Config;

pub struct AppState {
    pub pool: sqlx::PgPool,
    pub config: Config,
}

impl AppState {
    pub fn new(pool: sqlx::PgPool, config: Config) -> Arc<Self> {
        Arc::new(Self { pool, config })
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}
