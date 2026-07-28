//! Process-wide operational metrics for the dashboard: lock-free atomic
//! counters incremented at the transport boundary (HTTP + WS handlers), snapshotted
//! on demand by `GET /admin/metrics`. Rates are derived client-side from successive
//! snapshots; the realtime push stream + op feed live in Phase 3b.
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::SystemTime;

use serde::Serialize;
use sqlx::PgPool;

use crate::subs::SubscriptionManager;

#[derive(Default)]
pub struct Metrics {
    queries_total: AtomicU64,
    mutations_total: AtomicU64,
    uploads_total: AtomicU64,
    /// Current open `/sync` WebSocket connections (inc on auth, dec on close).
    ws_connections: AtomicI64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_mutation(&self) {
        self.mutations_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_upload(&self) {
        self.uploads_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ws_connect(&self) {
        self.ws_connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ws_disconnect(&self) {
        self.ws_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub async fn snapshot(
        &self,
        pool: &PgPool,
        subs: &SubscriptionManager,
        started_at: SystemTime,
    ) -> MetricsSnapshot {
        MetricsSnapshot {
            queries_total: self.queries_total.load(Ordering::Relaxed),
            mutations_total: self.mutations_total.load(Ordering::Relaxed),
            uploads_total: self.uploads_total.load(Ordering::Relaxed),
            ws_connections: self.ws_connections.load(Ordering::Relaxed),
            active_subscriptions: subs.count().await,
            pool_size: pool.size() as i64,
            pool_idle: pool.num_idle() as i64,
            uptime_seconds: started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSnapshot {
    pub queries_total: u64,
    pub mutations_total: u64,
    pub uploads_total: u64,
    pub ws_connections: i64,
    pub active_subscriptions: usize,
    pub pool_size: i64,
    pub pool_idle: i64,
    pub uptime_seconds: u64,
}

/// Render the snapshot as Prometheus text-exposition format (version 0.0.4).
///
/// Pure (no I/O) so it is trivially unit-testable. `version` and `git_commit`
/// populate a `rtdb_build_info` gauge so a scrape also records which build is
/// live — mirroring the identity fields on `/healthz` and `/admin/config`. Both
/// are build-time constants (semver / hex sha or `"unknown"`), so they are
/// interpolated into the label value without escaping.
pub fn render_prometheus(snap: &MetricsSnapshot, version: &str, git_commit: &str) -> String {
    let mut s = String::with_capacity(1024);
    // Counters — monotonic totals incremented at the transport boundary.
    s.push_str("# HELP rtdb_queries_total Total query requests served (HTTP /api/query + WS).\n");
    s.push_str("# TYPE rtdb_queries_total counter\n");
    s.push_str(&format!("rtdb_queries_total {}\n", snap.queries_total));

    s.push_str(
        "# HELP rtdb_mutations_total Total mutation transactions committed (HTTP /api/mutate + WS).\n",
    );
    s.push_str("# TYPE rtdb_mutations_total counter\n");
    s.push_str(&format!("rtdb_mutations_total {}\n", snap.mutations_total));

    s.push_str("# HELP rtdb_uploads_total Total file storage uploads (POST /api/storage/{db}).\n");
    s.push_str("# TYPE rtdb_uploads_total counter\n");
    s.push_str(&format!("rtdb_uploads_total {}\n", snap.uploads_total));

    // Gauges — point-in-time process/runtime state.
    s.push_str("# HELP rtdb_ws_connections Current open /sync WebSocket connections.\n");
    s.push_str("# TYPE rtdb_ws_connections gauge\n");
    s.push_str(&format!("rtdb_ws_connections {}\n", snap.ws_connections));

    s.push_str("# HELP rtdb_active_subscriptions Current active live-query subscriptions.\n");
    s.push_str("# TYPE rtdb_active_subscriptions gauge\n");
    s.push_str(&format!(
        "rtdb_active_subscriptions {}\n",
        snap.active_subscriptions
    ));

    s.push_str("# HELP rtdb_pool_size Postgres connection pool size (total connections).\n");
    s.push_str("# TYPE rtdb_pool_size gauge\n");
    s.push_str(&format!("rtdb_pool_size {}\n", snap.pool_size));

    s.push_str("# HELP rtdb_pool_idle Postgres connection pool idle connections.\n");
    s.push_str("# TYPE rtdb_pool_idle gauge\n");
    s.push_str(&format!("rtdb_pool_idle {}\n", snap.pool_idle));

    s.push_str("# HELP rtdb_uptime_seconds Server uptime in seconds since boot.\n");
    s.push_str("# TYPE rtdb_uptime_seconds gauge\n");
    s.push_str(&format!("rtdb_uptime_seconds {}\n", snap.uptime_seconds));

    // build_info: constant 1 gauge carrying version + git_commit labels.
    s.push_str("# HELP rtdb_build_info Build identity (version, git_commit).\n");
    s.push_str("# TYPE rtdb_build_info gauge\n");
    s.push_str(&format!(
        "rtdb_build_info{{version=\"{version}\",git_commit=\"{git_commit}\"}} 1\n"
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_prometheus_includes_counters_gauges_and_build_info() {
        let snap = MetricsSnapshot {
            queries_total: 5,
            mutations_total: 3,
            uploads_total: 1,
            ws_connections: 2,
            active_subscriptions: 7,
            pool_size: 10,
            pool_idle: 4,
            uptime_seconds: 99,
        };
        let body = render_prometheus(&snap, "0.0.0", "abc");
        assert!(
            body.contains("# TYPE rtdb_queries_total counter"),
            "missing counter TYPE: {body}"
        );
        assert!(
            body.contains("# TYPE rtdb_ws_connections gauge"),
            "missing gauge TYPE: {body}"
        );
        assert!(
            body.contains("rtdb_build_info{version=\"0.0.0\",git_commit=\"abc\"} 1"),
            "missing build_info sample: {body}"
        );
    }
}
