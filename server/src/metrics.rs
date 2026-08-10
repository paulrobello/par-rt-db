//! Process-wide operational metrics for the dashboard: lock-free atomic
//! counters incremented at the transport boundary (HTTP + WS handlers), snapshotted
//! on demand by `GET /admin/metrics`. Rates are derived client-side from successive
//! snapshots; the realtime push stream + op feed live in Phase 3b.
use std::collections::HashMap;
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

/// Which `subs::ReadSet` class decided a fan-out skip. Only the classes that
/// CAN skip are represented — `Table` always re-runs, so it has no skip
/// counter (its work shows up in `subs_reruns_total`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SkipClass {
    Point,
    Indexed,
    Ordered,
}

/// Per-database subscription-invalidation counters — the per-db breakdown of
/// the global skip/re-run/missed counters on [`Metrics`]. Stored inside
/// [`Metrics::per_db_subs`], a `Mutex<HashMap<String, Self>>`; the mutex is held
/// only across a single atomic increment (no I/O), so it does not reintroduce
/// the cross-db serialization the sharded registry (`crate::subs`) removed —
/// that concern is about holding locks across Postgres round-trips, not
/// nanosecond atomic adds. Exposed only in the JSON metrics snapshot and the
/// `/admin/subscriptions` inspector; deliberately absent from the Prometheus
/// scrape to avoid per-db label cardinality on the public export.
#[derive(Default)]
struct DbSubCounters {
    reruns: AtomicU64,
    skips_point: AtomicU64,
    skips_indexed: AtomicU64,
    skips_ordered: AtomicU64,
    missed: AtomicU64,
}

/// One db's subscription-invalidation counters in a serializable, sorted form
/// (the JSON snapshot + inspector shape). camelCase on the wire.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbSubCounterRow {
    pub db: String,
    pub reruns: u64,
    pub skips_point: u64,
    pub skips_indexed: u64,
    pub skips_ordered: u64,
    pub missed: u64,
}

/// Which resource quota was exceeded. Mirrors the `SkipClass` pattern: the
/// kind becomes a Prometheus label on the aggregate counter, so a new quota
/// dimension is a new variant rather than a new metric name.
#[derive(Debug, Clone, Copy)]
pub enum QuotaKind {
    Tables,
    Storage,
    Subs,
}

/// Per-database resource-quota rejection counters — the per-db breakdown of the
/// global `quota_rejections_*_total` counters on [`Metrics`]. Same shape and
/// same concurrency posture as [`DbSubCounters`]: held under a
/// `Mutex<HashMap<String, Self>>`, the mutex held only across a single atomic
/// increment. Exposed only in the JSON metrics snapshot; deliberately absent
/// from the Prometheus scrape to avoid per-db label cardinality on the public
/// export (see `lib.rs:239` — the `/metrics` scrape is aggregate-only).
#[derive(Default)]
struct QuotaCounters {
    tables: AtomicU64,
    storage: AtomicU64,
    subs: AtomicU64,
}

/// One db's quota-rejection counters in a serializable, sorted form (the JSON
/// snapshot shape). camelCase on the wire.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbQuotaCounterRow {
    pub db: String,
    pub tables: u64,
    pub storage: u64,
    pub subs: u64,
}

#[derive(Default)]
pub struct Metrics {
    queries_total: AtomicU64,
    mutations_total: AtomicU64,
    uploads_total: AtomicU64,
    // ---- Image transforms (ENH-014) ----
    image_transforms_hit_total: AtomicU64,
    image_transforms_miss_total: AtomicU64,
    image_transforms_error_total: AtomicU64,
    image_transform_bytes_total: AtomicU64,
    // ---- Presence (ENH-015) ----
    /// Total `presence/update` frames processed (one per inbound client update).
    presence_updates_total: AtomicU64,
    /// Total presence-state broadcasts fanned out to interested subscribers.
    /// Incremented once per `flush_once` even when multiple peers receive it,
    /// so it counts fan-out decisions, not delivered frames.
    presence_broadcasts_total: AtomicU64,
    /// Total presence sessions whose per-state TTL expired (state cleared to null).
    presence_ttl_expiries_total: AtomicU64,
    /// Current open `/sync` WebSocket connections (inc on auth, dec on close).
    ws_connections: AtomicI64,
    query_latency: Mutex<LatencySamples>,
    mutate_latency: Mutex<LatencySamples>,
    subscribe_latency: Mutex<LatencySamples>,
    // ---- Subscription invalidation (see `subs::fan_out`) ----
    // Counted per subscription whose table WAS written; subscriptions on
    // untouched tables are the trivial fast path and are not counted, so
    // `skips + reruns` is the number of read-set decisions actually made.
    /// Re-runs performed: the subscription's read set could not prove the write
    /// irrelevant (includes every `Table`-class subscription).
    subs_reruns_total: AtomicU64,
    subs_skips_point_total: AtomicU64,
    subs_skips_indexed_total: AtomicU64,
    subs_skips_ordered_total: AtomicU64,
    /// Skips that were shadow-verified (the query was re-run anyway and its
    /// result compared against the last pushed one). Sampled — see
    /// `Config::subs_verify_skip_every`.
    subs_skip_verifications_total: AtomicU64,
    /// **The alarm.** Verified skips whose result had actually changed: the
    /// invalidation logic under-approximated and would have dropped a realtime
    /// update. Any non-zero value is a correctness defect, not a tuning signal.
    subs_missed_pushes_total: AtomicU64,
    // ---- TTL reaper ----
    /// Total expired documents deleted by the per-db TTL reaper across all
    /// dbs/tables. Global (no db/table labels) to match the neighboring counters.
    ttl_expired_total: AtomicU64,
    // ---- Per-db subscription-invalidation counters (ENH-010) ----
    /// Per-db breakdown of the skip/re-run/missed counters above, keyed by db
    /// name. Updated alongside the globals in `fan_out`; read by the JSON
    /// metrics snapshot and `/admin/subscriptions`. See [`DbSubCounters`].
    per_db_subs: Mutex<HashMap<String, DbSubCounters>>,
    // ---- Per-db resource-quota counters (ENH-011) ----
    /// Per-db breakdown of the `quota_rejections_*_total` counters below,
    /// keyed by db name. JSON-snapshot only — the Prometheus scrape carries
    /// the aggregate-by-kind totals (no per-db labels). See [`QuotaCounters`].
    per_db_quota: Mutex<HashMap<String, QuotaCounters>>,
    quota_rejections_tables_total: AtomicU64,
    quota_rejections_storage_total: AtomicU64,
    quota_rejections_subs_total: AtomicU64,
    /// SEC-109: admin-key login failures (wrong key guesses at `POST /admin/login`).
    /// Monotonic counter for brute-force detection — a spike signals an attack.
    admin_auth_failures_total: AtomicU64,
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
    pub fn record_image_transform_hit(&self) {
        self.image_transforms_hit_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_image_transform_miss(&self, out_bytes: u64) {
        self.image_transforms_miss_total
            .fetch_add(1, Ordering::Relaxed);
        self.image_transform_bytes_total
            .fetch_add(out_bytes, Ordering::Relaxed);
    }
    pub fn record_image_transform_error(&self) {
        self.image_transforms_error_total
            .fetch_add(1, Ordering::Relaxed);
    }
    /// SEC-109: record an admin-key login failure for brute-force detection.
    pub fn record_admin_auth_failure(&self) {
        self.admin_auth_failures_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_presence_update(&self) {
        self.presence_updates_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_presence_broadcast(&self) {
        self.presence_broadcasts_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_presence_ttl_expiry(&self) {
        self.presence_ttl_expiries_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn presence_updates_total(&self) -> u64 {
        self.presence_updates_total.load(Ordering::Relaxed)
    }
    pub fn presence_broadcasts_total(&self) -> u64 {
        self.presence_broadcasts_total.load(Ordering::Relaxed)
    }
    pub fn presence_ttl_expiries_total(&self) -> u64 {
        self.presence_ttl_expiries_total.load(Ordering::Relaxed)
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

    /// A subscription on a written table was re-run by `fan_out`. Records both
    /// the global counter and the per-db counter for `db`.
    pub fn record_subs_rerun(&self, db: &str) {
        self.subs_reruns_total.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_db_subs.lock() {
            map.entry(db.to_string())
                .or_default()
                .reruns
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A subscription on a written table was skipped: its read set proved every
    /// written document irrelevant. Records both the global counter and the
    /// per-db counter for `db` (by `class`).
    pub fn record_subs_skip(&self, db: &str, class: SkipClass) {
        let counter = match class {
            SkipClass::Point => &self.subs_skips_point_total,
            SkipClass::Indexed => &self.subs_skips_indexed_total,
            SkipClass::Ordered => &self.subs_skips_ordered_total,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_db_subs.lock() {
            let entry = map.entry(db.to_string()).or_default();
            let per_db = match class {
                SkipClass::Point => &entry.skips_point,
                SkipClass::Indexed => &entry.skips_indexed,
                SkipClass::Ordered => &entry.skips_ordered,
            };
            per_db.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A skip was shadow-verified. Call once per verification regardless of the
    /// outcome; `record_subs_missed_push` records the failures.
    pub fn record_subs_skip_verification(&self) {
        self.subs_skip_verifications_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A shadow-verified skip turned out to be wrong — the result HAD changed.
    /// See `subs_missed_pushes_total`: non-zero means a correctness defect.
    /// Records both the global counter and the per-db counter for `db`.
    pub fn record_subs_missed_push(&self, db: &str) {
        self.subs_missed_pushes_total
            .fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_db_subs.lock() {
            map.entry(db.to_string())
                .or_default()
                .missed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A TTL reaper sweep deleted an expired document. Call once per deleted doc.
    pub fn record_ttl_expired(&self) {
        self.ttl_expired_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot of the per-db subscription-invalidation counters, sorted by db
    /// name for stable output. Empty until a `fan_out` records a decision.
    pub fn per_db_subs_snapshot(&self) -> Vec<DbSubCounterRow> {
        let Ok(map) = self.per_db_subs.lock() else {
            return Vec::new();
        };
        let mut rows: Vec<DbSubCounterRow> = map
            .iter()
            .map(|(db, c)| DbSubCounterRow {
                db: db.clone(),
                reruns: c.reruns.load(Ordering::Relaxed),
                skips_point: c.skips_point.load(Ordering::Relaxed),
                skips_indexed: c.skips_indexed.load(Ordering::Relaxed),
                skips_ordered: c.skips_ordered.load(Ordering::Relaxed),
                missed: c.missed.load(Ordering::Relaxed),
            })
            .collect();
        rows.sort_by(|a, b| a.db.cmp(&b.db));
        rows
    }

    /// A per-db resource quota rejection (ENH-011). Records the global per-kind
    /// counter + a per-db breakdown. The per-db breakdown is JSON-snapshot only;
    /// the Prometheus scrape carries the aggregate-by-kind totals (no per-db
    /// labels on the public export — same convention as `per_db_subs`).
    pub fn record_quota_rejection(&self, db: &str, kind: QuotaKind) {
        let global = match kind {
            QuotaKind::Tables => &self.quota_rejections_tables_total,
            QuotaKind::Storage => &self.quota_rejections_storage_total,
            QuotaKind::Subs => &self.quota_rejections_subs_total,
        };
        global.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_db_quota.lock() {
            let entry = map.entry(db.to_string()).or_default();
            let per_db = match kind {
                QuotaKind::Tables => &entry.tables,
                QuotaKind::Storage => &entry.storage,
                QuotaKind::Subs => &entry.subs,
            };
            per_db.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Snapshot of the per-db quota-rejection counters, sorted by db name for
    /// stable output. Empty until a `record_quota_rejection` lands.
    pub fn per_db_quota_snapshot(&self) -> Vec<DbQuotaCounterRow> {
        let Ok(map) = self.per_db_quota.lock() else {
            return Vec::new();
        };
        let mut rows: Vec<DbQuotaCounterRow> = map
            .iter()
            .map(|(db, c)| DbQuotaCounterRow {
                db: db.clone(),
                tables: c.tables.load(Ordering::Relaxed),
                storage: c.storage.load(Ordering::Relaxed),
                subs: c.subs.load(Ordering::Relaxed),
            })
            .collect();
        rows.sort_by(|a, b| a.db.cmp(&b.db));
        rows
    }

    pub async fn snapshot(
        &self,
        pool: &PgPool,
        subs: &SubscriptionManager,
        started_at: SystemTime,
        presence_rooms: usize,
        presence_sessions: usize,
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
            subs_reruns_total: self.subs_reruns_total.load(Ordering::Relaxed),
            subs_skips_point_total: self.subs_skips_point_total.load(Ordering::Relaxed),
            subs_skips_indexed_total: self.subs_skips_indexed_total.load(Ordering::Relaxed),
            subs_skips_ordered_total: self.subs_skips_ordered_total.load(Ordering::Relaxed),
            subs_skip_verifications_total: self
                .subs_skip_verifications_total
                .load(Ordering::Relaxed),
            subs_missed_pushes_total: self.subs_missed_pushes_total.load(Ordering::Relaxed),
            ttl_expired_total: self.ttl_expired_total.load(Ordering::Relaxed),
            image_transforms_hit_total: self.image_transforms_hit_total.load(Ordering::Relaxed),
            image_transforms_miss_total: self.image_transforms_miss_total.load(Ordering::Relaxed),
            image_transforms_error_total: self.image_transforms_error_total.load(Ordering::Relaxed),
            image_transform_bytes_total: self.image_transform_bytes_total.load(Ordering::Relaxed),
            presence_updates_total: self.presence_updates_total.load(Ordering::Relaxed),
            presence_broadcasts_total: self.presence_broadcasts_total.load(Ordering::Relaxed),
            presence_ttl_expiries_total: self.presence_ttl_expiries_total.load(Ordering::Relaxed),
            presence_rooms,
            presence_sessions,
            per_db_subs: self.per_db_subs_snapshot(),
            quota_rejections_tables_total: self
                .quota_rejections_tables_total
                .load(Ordering::Relaxed),
            quota_rejections_storage_total: self
                .quota_rejections_storage_total
                .load(Ordering::Relaxed),
            quota_rejections_subs_total: self.quota_rejections_subs_total.load(Ordering::Relaxed),
            admin_auth_failures_total: self.admin_auth_failures_total.load(Ordering::Relaxed),
            per_db_quota: self.per_db_quota_snapshot(),
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
    /// Subscription-invalidation effectiveness: how many read-set decisions
    /// ended in a re-run vs. a proven skip, split by the class that proved it.
    /// A class stuck at zero while its subscriptions exist means its derivation
    /// isn't firing.
    pub subs_reruns_total: u64,
    pub subs_skips_point_total: u64,
    pub subs_skips_indexed_total: u64,
    pub subs_skips_ordered_total: u64,
    /// Sampled shadow verifications of skips, and the ones that found the skip
    /// was wrong. `subsMissedPushesTotal > 0` is a correctness defect — see
    /// `Metrics::subs_missed_pushes_total`.
    pub subs_skip_verifications_total: u64,
    pub subs_missed_pushes_total: u64,
    /// Total expired documents deleted by the TTL reaper (all dbs/tables).
    pub ttl_expired_total: u64,
    /// Image-transform cache lookups, by outcome (ENH-014).
    pub image_transforms_hit_total: u64,
    pub image_transforms_miss_total: u64,
    pub image_transforms_error_total: u64,
    /// Total bytes emitted by image transforms (miss path only; hits serve
    /// from the cache and aren't re-encoded).
    pub image_transform_bytes_total: u64,
    /// Presence `presence/update` frames processed (ENH-015). Counted once per
    /// inbound client update, before fan-out.
    pub presence_updates_total: u64,
    /// Presence-state broadcasts fanned out to interested subscribers
    /// (ENH-015). One per `flush_once`, regardless of recipient count.
    pub presence_broadcasts_total: u64,
    /// Presence sessions whose per-state TTL expired (ENH-015 follow-up).
    pub presence_ttl_expiries_total: u64,
    /// Distinct presence rooms across all dbs at snapshot time (ENH-015). Gauge,
    /// not counter — membership is per-shard HashMap state tallied at read time
    /// by `PresenceManager::counts`.
    pub presence_rooms: usize,
    /// Total presence sessions across all rooms at snapshot time (ENH-015).
    pub presence_sessions: usize,
    /// Per-database breakdown of the subscription-invalidation counters above
    /// (ENH-010). Empty until a `fan_out` records a decision; sorted by db.
    pub per_db_subs: Vec<DbSubCounterRow>,
    /// Aggregate quota rejections by kind (ENH-011), surfaced in BOTH the JSON
    /// snapshot and the Prometheus scrape (`rtdb_quota_rejections_total{kind=…}`).
    pub quota_rejections_tables_total: u64,
    pub quota_rejections_storage_total: u64,
    pub quota_rejections_subs_total: u64,
    /// SEC-109: total admin-key login failures (brute-force signal).
    pub admin_auth_failures_total: u64,
    /// Per-database breakdown of the quota-rejection counters above (ENH-011).
    /// JSON-snapshot only — deliberately absent from the Prometheus scrape
    /// (per-db labels would blow up cardinality on the public export).
    pub per_db_quota: Vec<DbQuotaCounterRow>,
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

    // Subscription invalidation. Skips carry a `class` label (the read set that
    // proved the write irrelevant) so one metric covers every class and a new
    // class is a new label rather than a new metric name.
    s.push_str(
        "# HELP rtdb_subs_skips_total Subscription re-runs skipped because the read set proved every written document irrelevant.\n",
    );
    s.push_str("# TYPE rtdb_subs_skips_total counter\n");
    s.push_str(&format!(
        "rtdb_subs_skips_total{{class=\"point\"}} {}\n",
        snap.subs_skips_point_total
    ));
    s.push_str(&format!(
        "rtdb_subs_skips_total{{class=\"indexed\"}} {}\n",
        snap.subs_skips_indexed_total
    ));
    s.push_str(&format!(
        "rtdb_subs_skips_total{{class=\"ordered\"}} {}\n",
        snap.subs_skips_ordered_total
    ));

    s.push_str(
        "# HELP rtdb_subs_reruns_total Subscriptions re-run by fan_out (read set could not prove irrelevance).\n",
    );
    s.push_str("# TYPE rtdb_subs_reruns_total counter\n");
    s.push_str(&format!(
        "rtdb_subs_reruns_total {}\n",
        snap.subs_reruns_total
    ));

    s.push_str(
        "# HELP rtdb_subs_skip_verifications_total Skips shadow-verified by re-running the query and comparing (sampled).\n",
    );
    s.push_str("# TYPE rtdb_subs_skip_verifications_total counter\n");
    s.push_str(&format!(
        "rtdb_subs_skip_verifications_total {}\n",
        snap.subs_skip_verifications_total
    ));

    s.push_str(
        "# HELP rtdb_subs_missed_pushes_total Verified skips whose result had changed — invalidation under-approximated. ALERT ON ANY INCREASE.\n",
    );
    s.push_str("# TYPE rtdb_subs_missed_pushes_total counter\n");
    s.push_str(&format!(
        "rtdb_subs_missed_pushes_total {}\n",
        snap.subs_missed_pushes_total
    ));

    // Resource quota rejections (ENH-011). Aggregate-by-kind only — no per-db
    // labels (cardinality). The per-db breakdown lives in the JSON snapshot.
    s.push_str("# HELP rtdb_quota_rejections_total Resource-quota rejections by kind.\n");
    s.push_str("# TYPE rtdb_quota_rejections_total counter\n");
    s.push_str(&format!(
        "rtdb_quota_rejections_total{{kind=\"tables\"}} {}\n",
        snap.quota_rejections_tables_total
    ));
    s.push_str(&format!(
        "rtdb_quota_rejections_total{{kind=\"storage\"}} {}\n",
        snap.quota_rejections_storage_total
    ));
    s.push_str(&format!(
        "rtdb_quota_rejections_total{{kind=\"subs\"}} {}\n",
        snap.quota_rejections_subs_total
    ));

    // SEC-109: admin-key login failures (brute-force detection).
    s.push_str(
        "# HELP rtdb_admin_auth_failures_total Admin-key login failures at POST /admin/login (brute-force signal).\n",
    );
    s.push_str("# TYPE rtdb_admin_auth_failures_total counter\n");
    s.push_str(&format!(
        "rtdb_admin_auth_failures_total {}\n",
        snap.admin_auth_failures_total
    ));

    s.push_str(
        "# HELP rtdb_ttl_expired_total Total expired documents deleted by the TTL reaper across all dbs/tables.\n",
    );
    s.push_str("# TYPE rtdb_ttl_expired_total counter\n");
    s.push_str(&format!(
        "rtdb_ttl_expired_total {}\n",
        snap.ttl_expired_total
    ));

    // Image transforms (ENH-014). `result` label mirrors the `class` label on
    // subs_skips_total: one metric name, one sample per outcome.
    s.push_str("# HELP rtdb_image_transforms_total Image transforms served, by result.\n");
    s.push_str("# TYPE rtdb_image_transforms_total counter\n");
    s.push_str(&format!(
        "rtdb_image_transforms_total{{result=\"hit\"}} {}\n",
        snap.image_transforms_hit_total
    ));
    s.push_str(&format!(
        "rtdb_image_transforms_total{{result=\"miss\"}} {}\n",
        snap.image_transforms_miss_total
    ));
    s.push_str(&format!(
        "rtdb_image_transforms_total{{result=\"error\"}} {}\n",
        snap.image_transforms_error_total
    ));
    s.push_str(
        "# HELP rtdb_image_transform_bytes_total Total bytes emitted by image transforms.\n",
    );
    s.push_str("# TYPE rtdb_image_transform_bytes_total counter\n");
    s.push_str(&format!(
        "rtdb_image_transform_bytes_total {}\n",
        snap.image_transform_bytes_total
    ));

    // Presence (ENH-015). Two monotonic counters plus two gauges computed at
    // snapshot time by `PresenceManager::counts` (membership is per-shard
    // HashMap state, not an atomic counter).
    s.push_str("# HELP rtdb_presence_updates_total Inbound presence/update frames processed.\n");
    s.push_str("# TYPE rtdb_presence_updates_total counter\n");
    s.push_str(&format!(
        "rtdb_presence_updates_total {}\n",
        snap.presence_updates_total
    ));
    s.push_str(
        "# HELP rtdb_presence_broadcasts_total Presence-state broadcasts fanned out to subscribers.\n",
    );
    s.push_str("# TYPE rtdb_presence_broadcasts_total counter\n");
    s.push_str(&format!(
        "rtdb_presence_broadcasts_total {}\n",
        snap.presence_broadcasts_total
    ));
    s.push_str(
        "# HELP rtdb_presence_ttl_expiries_total Presence sessions whose per-state TTL expired.\n",
    );
    s.push_str("# TYPE rtdb_presence_ttl_expiries_total counter\n");
    s.push_str(&format!(
        "rtdb_presence_ttl_expiries_total {}\n",
        snap.presence_ttl_expiries_total
    ));
    s.push_str("# HELP rtdb_presence_rooms Distinct presence rooms across all dbs.\n");
    s.push_str("# TYPE rtdb_presence_rooms gauge\n");
    s.push_str(&format!("rtdb_presence_rooms {}\n", snap.presence_rooms));
    s.push_str("# HELP rtdb_presence_sessions Total presence sessions across all rooms.\n");
    s.push_str("# TYPE rtdb_presence_sessions gauge\n");
    s.push_str(&format!(
        "rtdb_presence_sessions {}\n",
        snap.presence_sessions
    ));

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
            subs_reruns_total: 11,
            subs_skips_point_total: 12,
            subs_skips_indexed_total: 13,
            subs_skips_ordered_total: 14,
            subs_skip_verifications_total: 15,
            subs_missed_pushes_total: 0,
            ttl_expired_total: 0,
            image_transforms_hit_total: 0,
            image_transforms_miss_total: 0,
            image_transforms_error_total: 0,
            image_transform_bytes_total: 0,
            presence_updates_total: 0,
            presence_broadcasts_total: 0,
            presence_ttl_expiries_total: 0,
            presence_rooms: 0,
            presence_sessions: 0,
            per_db_subs: Vec::new(),
            quota_rejections_tables_total: 0,
            quota_rejections_storage_total: 0,
            quota_rejections_subs_total: 0,
            admin_auth_failures_total: 0,
            per_db_quota: Vec::new(),
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
            body.contains("# TYPE rtdb_presence_rooms gauge"),
            "missing presence_rooms gauge TYPE: {body}"
        );
        assert!(
            body.contains("# TYPE rtdb_presence_sessions gauge"),
            "missing presence_sessions gauge TYPE: {body}"
        );
        assert!(
            body.contains("rtdb_build_info{version=\"0.0.0\",git_commit=\"abc\"} 1"),
            "missing build_info sample: {body}"
        );
    }

    #[test]
    fn render_prometheus_includes_invalidation_counters_with_class_labels() {
        let snap = MetricsSnapshot {
            queries_total: 0,
            mutations_total: 0,
            uploads_total: 0,
            ws_connections: 0,
            active_subscriptions: 0,
            pool_size: 0,
            pool_idle: 0,
            uptime_seconds: 0,
            query_latency: LatencyStats::default(),
            mutate_latency: LatencyStats::default(),
            subscribe_latency: LatencyStats::default(),
            subs_reruns_total: 4,
            subs_skips_point_total: 1,
            subs_skips_indexed_total: 2,
            subs_skips_ordered_total: 3,
            subs_skip_verifications_total: 7,
            subs_missed_pushes_total: 9,
            ttl_expired_total: 0,
            image_transforms_hit_total: 0,
            image_transforms_miss_total: 0,
            image_transforms_error_total: 0,
            image_transform_bytes_total: 0,
            presence_updates_total: 0,
            presence_broadcasts_total: 0,
            presence_ttl_expiries_total: 0,
            presence_rooms: 0,
            presence_sessions: 0,
            per_db_subs: Vec::new(),
            quota_rejections_tables_total: 0,
            quota_rejections_storage_total: 0,
            quota_rejections_subs_total: 0,
            admin_auth_failures_total: 0,
            per_db_quota: Vec::new(),
        };
        let body = render_prometheus(&snap, "0.0.0", "abc");
        // One metric name, one sample per skip class.
        assert!(
            body.contains("# TYPE rtdb_subs_skips_total counter"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_subs_skips_total{class=\"point\"} 1"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_subs_skips_total{class=\"indexed\"} 2"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_subs_skips_total{class=\"ordered\"} 3"),
            "{body}"
        );
        assert!(body.contains("rtdb_subs_reruns_total 4"), "{body}");
        assert!(
            body.contains("rtdb_subs_skip_verifications_total 7"),
            "{body}"
        );
        assert!(body.contains("rtdb_subs_missed_pushes_total 9"), "{body}");
    }

    #[test]
    fn skip_and_verification_counters_land_in_the_snapshot() {
        let m = Metrics::default();
        m.record_subs_skip("db-a", SkipClass::Point);
        m.record_subs_skip("db-a", SkipClass::Indexed);
        m.record_subs_skip("db-b", SkipClass::Indexed);
        m.record_subs_skip("db-a", SkipClass::Ordered);
        m.record_subs_rerun("db-a");
        m.record_subs_skip_verification();
        m.record_subs_missed_push("db-b");
        assert_eq!(m.subs_skips_point_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.subs_skips_indexed_total.load(Ordering::Relaxed), 2);
        assert_eq!(m.subs_skips_ordered_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.subs_reruns_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.subs_skip_verifications_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.subs_missed_pushes_total.load(Ordering::Relaxed), 1);
        // Per-db breakdown (ENH-010): globals fan out into per-db rows.
        let rows = m.per_db_subs_snapshot();
        assert_eq!(rows.len(), 2, "two dbs recorded");
        // Sorted by db name: db-a first, then db-b.
        assert_eq!(rows[0].db, "db-a");
        assert_eq!(rows[0].skips_point, 1);
        assert_eq!(rows[0].skips_indexed, 1);
        assert_eq!(rows[0].skips_ordered, 1);
        assert_eq!(rows[0].reruns, 1);
        assert_eq!(rows[0].missed, 0);
        assert_eq!(rows[1].db, "db-b");
        assert_eq!(rows[1].skips_indexed, 1);
        assert_eq!(rows[1].missed, 1);
        // Globals are the sum across dbs.
        assert_eq!(
            rows.iter().map(|r| r.skips_indexed).sum::<u64>(),
            m.subs_skips_indexed_total.load(Ordering::Relaxed)
        );
    }

    #[test]
    fn quota_rejections_land_in_snapshot_and_prometheus() {
        // Mirrors `skip_and_verification_counters_land_in_the_snapshot` — the
        // per-db breakdown fans out into rows, the globals are the sum across
        // dbs, and the aggregate-by-kind totals appear in the Prometheus scrape
        // under one metric name with a `kind` label (no per-db labels).
        let m = Metrics::default();
        m.record_quota_rejection("db-a", QuotaKind::Tables);
        m.record_quota_rejection("db-a", QuotaKind::Storage);
        m.record_quota_rejection("db-b", QuotaKind::Storage);
        m.record_quota_rejection("db-a", QuotaKind::Subs);
        assert_eq!(m.quota_rejections_tables_total.load(Ordering::Relaxed), 1);
        assert_eq!(m.quota_rejections_storage_total.load(Ordering::Relaxed), 2);
        assert_eq!(m.quota_rejections_subs_total.load(Ordering::Relaxed), 1);
        // Per-db breakdown: db-a owns 3 rejections across kinds, db-b owns 1.
        let rows = m.per_db_quota_snapshot();
        assert_eq!(rows.len(), 2, "two dbs recorded");
        assert_eq!(rows[0].db, "db-a");
        assert_eq!(rows[0].tables, 1);
        assert_eq!(rows[0].storage, 1);
        assert_eq!(rows[0].subs, 1);
        assert_eq!(rows[1].db, "db-b");
        assert_eq!(rows[1].storage, 1);
        assert_eq!(rows[1].tables, 0);
        assert_eq!(rows[1].subs, 0);
        // Globals = sum across dbs.
        assert_eq!(
            rows.iter().map(|r| r.storage).sum::<u64>(),
            m.quota_rejections_storage_total.load(Ordering::Relaxed)
        );
        // Prometheus renders the three aggregate-by-kind totals (no per-db).
        let snap = MetricsSnapshot {
            queries_total: 0,
            mutations_total: 0,
            uploads_total: 0,
            ws_connections: 0,
            active_subscriptions: 0,
            pool_size: 0,
            pool_idle: 0,
            uptime_seconds: 0,
            query_latency: LatencyStats::default(),
            mutate_latency: LatencyStats::default(),
            subscribe_latency: LatencyStats::default(),
            subs_reruns_total: 0,
            subs_skips_point_total: 0,
            subs_skips_indexed_total: 0,
            subs_skips_ordered_total: 0,
            subs_skip_verifications_total: 0,
            subs_missed_pushes_total: 0,
            ttl_expired_total: 0,
            image_transforms_hit_total: 0,
            image_transforms_miss_total: 0,
            image_transforms_error_total: 0,
            image_transform_bytes_total: 0,
            presence_updates_total: 0,
            presence_broadcasts_total: 0,
            presence_ttl_expiries_total: 0,
            presence_rooms: 0,
            presence_sessions: 0,
            per_db_subs: Vec::new(),
            quota_rejections_tables_total: 1,
            quota_rejections_storage_total: 2,
            quota_rejections_subs_total: 3,
            admin_auth_failures_total: 0,
            per_db_quota: Vec::new(),
        };
        let body = render_prometheus(&snap, "0.0.0", "abc");
        assert!(
            body.contains("# TYPE rtdb_quota_rejections_total counter"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_quota_rejections_total{kind=\"tables\"} 1"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_quota_rejections_total{kind=\"storage\"} 2"),
            "{body}"
        );
        assert!(
            body.contains("rtdb_quota_rejections_total{kind=\"subs\"} 3"),
            "{body}"
        );
        // Per-db breakdown deliberately absent from the scrape (cardinality).
        assert!(!body.contains("db-a"), "per-db leaked into scrape: {body}");
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

    #[test]
    fn presence_counters_record() {
        // Assert via the public getters rather than `snapshot()` — the snapshot
        // builder needs a PgPool + SubscriptionManager (heavyweight for a unit
        // test), and coupling the test to its signature would break every time
        // the builder's deps change.
        let m = Metrics::default();
        m.record_presence_update();
        m.record_presence_update();
        m.record_presence_broadcast();
        m.record_presence_ttl_expiry();
        assert_eq!(m.presence_updates_total(), 2);
        assert_eq!(m.presence_broadcasts_total(), 1);
        assert_eq!(m.presence_ttl_expiries_total(), 1);
    }
}
