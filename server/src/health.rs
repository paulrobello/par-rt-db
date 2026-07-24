//! Unauthenticated `/healthz` diagnostics. Returns a JSON object identifying
//! which build is live and whether Postgres is reachable, so an operator can
//! answer "what's running, and is it healthy?" without SSH.
//!
//! Security: the route is **unauthenticated** — the body is built only from
//! build identity and reachability, never from `Config` (no tokens, secrets,
//! OAuth credentials, allowlists, or internal hostnames). Reachability is
//! probed with a bounded `SELECT 1`, never a value-bearing query.
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::AppState;

/// Git short sha injected by `build.rs`. `"unknown"` when neither
/// `RTDB_BUILD_COMMIT` nor `git` was available at build time.
const GIT_COMMIT: &str = env!("BUILD_GIT_COMMIT");
/// Unix seconds the binary was built, injected by `build.rs` as a string so
/// the const-eval is a plain `&str`; parsed to RFC3339 per request.
const BUILD_TIMESTAMP_SECS: &str = env!("BUILD_TIMESTAMP_SECS");

/// Cap the Postgres probe so a hung DB can never stall the health check.
const POSTGRES_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Serialize)]
pub struct HealthReport {
    pub status: &'static str,
    pub version: &'static str,
    pub git_commit: &'static str,
    pub build_timestamp: String,
    pub started_at: String,
    pub uptime_seconds: u64,
    pub postgres: bool,
}

/// `GET /healthz`: 200 + `{status:"ok"}` when Postgres is reachable, 503 +
/// `{status:"degraded"}` otherwise. The body shape is identical either way.
pub async fn handler(State(state): State<Arc<AppState>>) -> Response {
    let postgres = postgres_reachable(&state.pool).await;
    let now = std::time::SystemTime::now();
    let uptime_seconds = now
        .duration_since(state.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let build_secs: u64 = BUILD_TIMESTAMP_SECS.parse().unwrap_or(0);

    let report = HealthReport {
        status: if postgres { "ok" } else { "degraded" },
        version: env!("CARGO_PKG_VERSION"),
        git_commit: GIT_COMMIT,
        build_timestamp: fmt_secs(build_secs),
        started_at: DateTime::<Utc>::from(state.started_at).to_rfc3339(),
        uptime_seconds,
        postgres,
    };

    let code = if postgres {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(report)).into_response()
}

async fn postgres_reachable(pool: &sqlx::PgPool) -> bool {
    matches!(
        tokio::time::timeout(
            POSTGRES_PROBE_TIMEOUT,
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(pool),
        )
        .await,
        Ok(Ok(1))
    )
}

fn fmt_secs(secs: u64) -> String {
    DateTime::<Utc>::from_timestamp(secs as i64, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}
