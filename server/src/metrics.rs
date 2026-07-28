//! Process-wide operational metrics for the dashboard: lock-free atomic
//! counters incremented at the transport boundary (HTTP + WS handlers), snapshotted
//! on demand by `GET /admin/metrics`. Rates are derived client-side from successive
//! snapshots; the realtime push stream + op feed live in Phase 3b.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use serde::Serialize;
use sqlx::PgPool;

use crate::subs::SubscriptionManager;

/// Ring-buffer capacity for [`LatencySamples`] (1024 micros samples per bucket).
const LATENCY_BUF_CAP: usize = 1024;

/// p50/p95/p99 latency in microseconds. The field names are already lowercase,
/// so they serialize as `p50`/`p95`/`p99` verbatim; the surrounding
/// [`MetricsSnapshot`] carries the `queryLatency`/`mutateLatency`/
/// `subscribeLatency` camelCase keys via its own `rename_all`.
#[derive(Default, Serialize, Clone, Copy, Debug, PartialEq, Eq)]
pub struct LatencyStats {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
}

/// Fixed-capacity ring buffer of latency samples (microseconds). `record`
/// pushes while the buffer has room, then overwrites in insertion order once
/// full (cap [`LATENCY_BUF_CAP`]). `percentiles` clones the filled samples,
/// sorts, and returns nearest-rank p50/p95/p99 (1-indexed position
/// `ceil(p * len)`, i.e. 0-indexed `ceil(p * len) - 1` clamped to `len - 1`).
/// Held under a `std::sync::Mutex` inside [`Metrics`]; the clone + sort runs
/// inside the lock because 1024 `u64`s sort in microseconds and the
/// alternative (clone under lock, sort outside) doubles the copy.
#[derive(Default)]
pub struct LatencySamples {
    buf: Vec<u64>,
    next: usize,
    len: usize,
}

impl LatencySamples {
    fn record(&mut self, us: u64) {
        if self.buf.len() < LATENCY_BUF_CAP {
            self.buf.push(us);
            self.next = self.buf.len() % LATENCY_BUF_CAP;
            self.len = self.buf.len();
        } else {
            self.buf[self.next] = us;
            self.next = (self.next + 1) % LATENCY_BUF_CAP;
            self.len = LATENCY_BUF_CAP;
        }
    }

    fn percentiles(&self) -> LatencyStats {
        if self.buf.is_empty() {
            return LatencyStats::default();
        }
        let mut v = self.buf.clone();
        v.sort_unstable();
        let rank = |p: f64| -> usize {
            let n = v.len();
            let i = (p * n as f64).ceil() as usize;
            i.saturating_sub(1).min(n - 1)
        };
        LatencyStats {
            p50: v[rank(0.5)],
            p95: v[rank(0.95)],
            p99: v[rank(0.99)],
        }
    }
}

#[derive(Default)]
pub struct Metrics {
    queries_total: AtomicU64,
    mutations_total: AtomicU64,
    uploads_total: AtomicU64,
    /// Current open `/sync` WebSocket connections (inc on auth, dec on close).
    ws_connections: AtomicI64,
    query_latency: Mutex<LatencySamples>,
    mutate_latency: Mutex<LatencySamples>,
    subscribe_latency: Mutex<LatencySamples>,
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

    /// Record a successful query's duration (microseconds). HTTP `/api/query`
    /// and per-query `/api/query-batch` both feed this; the WS Subscribe
    /// initial query feeds `record_subscribe_duration` instead.
    pub fn record_query_duration(&self, us: u64) {
        if let Ok(mut s) = self.query_latency.lock() {
            s.record(us);
        }
    }
    /// Record a successful mutation's duration (microseconds). HTTP
    /// `/api/mutate` feeds this; WS Mutate currently feeds the counter only.
    pub fn record_mutation_duration(&self, us: u64) {
        if let Ok(mut s) = self.mutate_latency.lock() {
            s.record(us);
        }
    }
    /// Record a successful WS Subscribe's initial-query duration
    /// (microseconds) — the `committers.subscribe().await` resolution covers
    /// channel submit + committer queue + `execute_query` + result send +
    /// `subs.register`.
    pub fn record_subscribe_duration(&self, us: u64) {
        if let Ok(mut s) = self.subscribe_latency.lock() {
            s.record(us);
        }
    }

    pub async fn snapshot(
        &self,
        pool: &PgPool,
        subs: &SubscriptionManager,
        started_at: SystemTime,
    ) -> MetricsSnapshot {
        let query_latency = self
            .query_latency
            .lock()
            .map(|s| s.percentiles())
            .unwrap_or_default();
        let mutate_latency = self
            .mutate_latency
            .lock()
            .map(|s| s.percentiles())
            .unwrap_or_default();
        let subscribe_latency = self
            .subscribe_latency
            .lock()
            .map(|s| s.percentiles())
            .unwrap_or_default();
        MetricsSnapshot {
            queries_total: self.queries_total.load(Ordering::Relaxed),
            mutations_total: self.mutations_total.load(Ordering::Relaxed),
            uploads_total: self.uploads_total.load(Ordering::Relaxed),
            ws_connections: self.ws_connections.load(Ordering::Relaxed),
            active_subscriptions: subs.count().await,
            pool_size: pool.size() as i64,
            pool_idle: pool.num_idle() as i64,
            uptime_seconds: started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0),
            query_latency,
            mutate_latency,
            subscribe_latency,
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
    pub query_latency: LatencyStats,
    pub mutate_latency: LatencyStats,
    pub subscribe_latency: LatencyStats,
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
            query_latency: LatencyStats::default(),
            mutate_latency: LatencyStats::default(),
            subscribe_latency: LatencyStats::default(),
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

    #[test]
    fn latency_samples_percentiles_nearest_rank() {
        let mut s = LatencySamples::default();
        for us in [100_u64, 200, 300, 400, 500, 600, 700, 800, 900, 1000] {
            s.record(us);
        }
        // len=10, sorted [100..=1000]; nearest-rank index = ceil(p*len) - 1.
        // p50 -> ceil(5.0)-1   = 4 -> v[4]=500
        // p95 -> ceil(9.5)-1   = 9 -> v[9]=1000
        // p99 -> ceil(9.9)-1   = 9 -> v[9]=1000
        let p = s.percentiles();
        assert_eq!(p.p50, 500);
        assert_eq!(p.p95, 1000);
        assert_eq!(p.p99, 1000);
    }

    #[test]
    fn latency_samples_empty_returns_zeros() {
        let s = LatencySamples::default();
        let p = s.percentiles();
        assert_eq!(p, LatencyStats::default());
        assert_eq!((p.p50, p.p95, p.p99), (0, 0, 0));
    }

    #[test]
    fn latency_samples_ring_overwrite_evicts_oldest() {
        let mut s = LatencySamples::default();
        // Fill the buffer (cap 1024) with 1..=1024.
        for us in 1..=1024_u64 {
            s.record(us);
        }
        assert_eq!(s.len, LATENCY_BUF_CAP);
        assert_eq!(s.buf.len(), LATENCY_BUF_CAP);
        assert_eq!(s.next, 0, "next wraps to 0 once the buffer is exactly full");

        // One more record: must overwrite buf[0] (oldest = value 1), not grow.
        s.record(9999);
        assert_eq!(s.len, LATENCY_BUF_CAP, "len stays capped after overwrite");
        assert_eq!(
            s.buf.len(),
            LATENCY_BUF_CAP,
            "buffer must not grow past cap"
        );
        assert_eq!(s.buf[0], 9999, "overwrite lands at position 0 (FIFO)");
        assert_eq!(s.next, 1, "next advances past the overwritten slot");
        assert!(!s.buf.contains(&1_u64), "value 1 (oldest) must be evicted");
        assert!(s.buf.contains(&9999_u64), "new sample must be present");

        // A second overwrite lands at the next slot (value 2 evicted).
        s.record(8888);
        assert_eq!(s.buf[1], 8888, "second overwrite advances to position 1");
        assert_eq!(s.next, 2);
        assert!(!s.buf.contains(&2_u64), "value 2 must be evicted");

        // Percentile invariant still holds on the post-overwrite sample set.
        let p = s.percentiles();
        assert!(p.p50 <= p.p95);
        assert!(p.p95 <= p.p99);
    }

    #[test]
    fn latency_samples_single_sample_is_all_percentiles() {
        let mut s = LatencySamples::default();
        s.record(4242);
        let p = s.percentiles();
        // len=1: rank(p) = ceil(p*1) - 1 = 0 for any p in (0, 1].
        assert_eq!((p.p50, p.p95, p.p99), (4242, 4242, 4242));
    }
}
