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
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::AppState;

/// Unix seconds the binary was built, injected by `build.rs` as a string so
/// the const-eval is a plain `&str`; parsed to RFC3339 per request.
const BUILD_TIMESTAMP_SECS: &str = env!("BUILD_TIMESTAMP_SECS");

/// Cap the Postgres probe so a hung DB can never stall the health check.
const POSTGRES_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Serialize)]
pub struct HealthReport {
    pub status: &'static str,
    // SEC-129: build fingerprint fields are admin-only — omitted from the
    // unauthenticated response so an anonymous prober cannot pin the exact
    // deployed version / commit. Populated only when the request carries a
    // valid admin bearer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_timestamp: Option<String>,
    pub started_at: String,
    pub uptime_seconds: u64,
    pub postgres: bool,
}

/// `GET /healthz`: 200 + `{status:"ok"}` when Postgres is reachable, 503 +
/// `{status:"degraded"}` otherwise. The body shape is identical either way
/// for the liveness fields; the build fingerprint is included only for an
/// admin-authenticated request (SEC-129).
pub async fn handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let postgres = postgres_reachable(&state.pool).await;
    let now = std::time::SystemTime::now();
    let uptime_seconds = now
        .duration_since(state.runtime.started_at)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // SEC-129: include the build fingerprint only when admin-authed.
    let fingerprint = crate::admin_fingerprint(&state, &headers).await;
    let (version, git_commit, build_timestamp) = match fingerprint {
        Some((v, c)) => (
            Some(v),
            Some(c),
            Some(fmt_secs(BUILD_TIMESTAMP_SECS.parse().unwrap_or(0))),
        ),
        None => (None, None, None),
    };

    let report = HealthReport {
        status: if postgres { "ok" } else { "degraded" },
        version,
        git_commit,
        build_timestamp,
        started_at: DateTime::<Utc>::from(state.runtime.started_at).to_rfc3339(),
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
