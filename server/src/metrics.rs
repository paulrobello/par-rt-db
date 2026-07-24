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
