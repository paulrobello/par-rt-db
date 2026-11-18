//! Unauthenticated `/healthz` diagnostics. Returns a JSON object identifying
//! which build is live and whether Postgres is reachable, so an operator can
//! answer "what's running, and is it healthy?" without SSH.
//!
//! Security: the route is **unauthenticated** — the body is built only from
//! build identity, reachability, aggregate committer queue gauges, and the
//! pool-probe latency, never from `Config` (no tokens, secrets, OAuth
//! credentials, allowlists, or internal hostnames). Reachability is probed
//! with a bounded `SELECT 1`, never a value-bearing query.
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
    /// Committer-watchdog signal: why the write path is degraded, when the
    /// sustained queue-wait rule trips (see `watchdog.rs`). With the pool
    /// watchdog below this is the PRIMARY reason — the first element of
    /// `degraded_reasons`, pool-stall first because a postgres I/O stall
    /// (the 2026-11-16 episode class) trips the pool flag first and the
    /// committer backlog second. The HTTP status stays bound to `postgres`
    /// so probers and the container healthcheck keep treating this route as
    /// alive. INWARIANT (additive-only wire surface): existing consumers
    /// parse `status`/`postgres`/`degraded_reason`; new watchdog reasons
    /// arrive as new values here plus the `degraded_reasons` list, never by
    /// reshaping the body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub degraded_reason: Option<&'static str>,
    /// Every currently-tripped degradation reason, in stable order
    /// (pool-stall first, committer second). Additive superset of
    /// `degraded_reason` for consumers that must not lose the committer
    /// signal when both watchdogs fire during the same episode. Empty when
    /// healthy (the field is omitted).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub degraded_reasons: Vec<&'static str>,
    /// Last completed pool-probe latency (bounded acquire + `SELECT 1`
    /// through the connection pool), microseconds — the DB-stall signal
    /// (see `pool_watch.rs`). Absent until the monitor's first probe
    /// completes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_probe_wait_us: Option<u64>,
    /// Last watchdog sample of the committer queue-wait p99, microseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committer_queue_wait_p99_us: Option<u64>,
    /// Last sampled per-db committer queue depth (best-effort; see the
    /// gauge docs in `metrics`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub committer_queue_depth: Option<u64>,
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

    // Watchdog samples: aggregate queue gauges + the pool probe latency only
    // (no config or secrets), so they stay inside the unauthenticated body
    // contract. `degraded_reasons` lists every tripped watchdog, pool-stall
    // first (the postgres I/O episode class trips it before the committer
    // backlog builds); `degraded_reason` mirrors the first element for the
    // existing single-reason consumers.
    let watch = state.runtime.metrics.committer_watch_sample();
    let mut degraded_reasons: Vec<&'static str> = Vec::new();
    if state.runtime.metrics.pool_stalled() {
        degraded_reasons.push("pool_stalled");
    }
    if state.runtime.metrics.committer_degraded() {
        degraded_reasons.push("committer_queue_sustained_wait");
    }
    let report = HealthReport {
        status: if postgres { "ok" } else { "degraded" },
        version,
        git_commit,
        build_timestamp,
        started_at: DateTime::<Utc>::from(state.runtime.started_at).to_rfc3339(),
        uptime_seconds,
        postgres,
        degraded_reason: degraded_reasons.first().copied(),
        degraded_reasons,
        pool_probe_wait_us: state.runtime.metrics.pool_probe_wait_us(),
        committer_queue_wait_p99_us: Some(watch.queue_wait_p99_us),
        committer_queue_depth: Some(watch.queue_depth),
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
