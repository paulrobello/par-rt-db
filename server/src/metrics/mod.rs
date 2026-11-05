//! Process-wide operational metrics for the dashboard: lock-free atomic
//! counters incremented at the transport boundary (HTTP + WS handlers), snapshotted
//! on demand by `GET /admin/metrics`. Rates are derived client-side from successive
//! snapshots; the realtime push stream + op feed live in Phase 3b.
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use sqlx::PgPool;

use crate::subs::SubscriptionManager;

use crate::protocol::WorkflowStatus;

mod counters;
mod snapshot;

#[cfg(test)]
mod tests;

pub use counters::{DbSubCounterRow, QuotaKind, SkipClass, SlowQueryRecord, WorkflowStepOutcome};
pub use snapshot::{MetricsSnapshot, render_prometheus};

use counters::{
    DbQuotaCounterRow, DbSubCounters, DbWorkflowStatusRow, LatencySamples, QuotaCounters,
    SlowQueryLog,
};

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
    // ---- Slow-query log (ENH-019) ----
    /// Bounded ring buffer of recent slow-query records. Populated at the
    /// HTTP query timing call sites when a query's wall-clock duration
    /// exceeds `Config::slow_query_ms`. Default cap is 0 (off); a non-zero
    /// `RTDB_SLOW_QUERY_CAPACITY` enables it. The buffer is constructed with
    /// the configured capacity at [`Metrics::with_capacity`].
    slow_queries: Mutex<SlowQueryLog>,
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
    // ---- Scheduled/cron missed windows ----
    /// Cumulative count of recurring-job windows that elapsed before a fire
    /// (e.g. the process was down across one or more fire times), across all
    /// dbs/tables. Global (no db/table labels) to match the neighboring
    /// counters. Recurring jobs skip missed windows by design (see
    /// `scheduler::missed_windows_interval`/`missed_windows_cron`) — this
    /// counts how often that skip happened, it does not change the skip
    /// policy.
    scheduled_missed_total: AtomicU64,
    // ---- Anon→real merge (FM-27) ----
    /// Total documents re-stamped by the anon→real user merge across all
    /// dbs/tables. Global (no db/table labels) to match the neighboring counters.
    merge_docs_total: AtomicU64,
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
    // ---- Durable workflow steps (FM-29) ----
    /// Workflow step attempts by outcome, across all dbs (no db/table labels,
    /// matching the neighboring counters; the per-db status breakdown is a
    /// live side-table read on the admin metrics JSON instead).
    workflow_steps_success_total: AtomicU64,
    workflow_steps_retry_total: AtomicU64,
    workflow_steps_fail_total: AtomicU64,
    /// SEC-109: admin-key login failures (wrong key guesses at `POST /admin/login`).
    /// Monotonic counter for brute-force detection — a spike signals an attack.
    admin_auth_failures_total: AtomicU64,
    // ---- Ownership-lease observability (ENH-049) ----
    // Prometheus-only: rendered by `render_prometheus` from the live `Metrics`,
    // deliberately absent from `MetricsSnapshot` so the admin JSON wire (mirrored
    // in four clients, Python's extra="forbid") stays byte-identical.
    /// Whether multi-instance mode is on. Set once at boot from the config.
    multi_instance: AtomicI64,
    /// Databases whose ownership lease this replica currently holds. Inc when a
    /// lease-holding channel entry is inserted, dec on every removal of such an
    /// entry — all removal paths go through `remove_channel_entry`
    /// (committer/mod.rs) so the gauge cannot drift from the map state.
    leases_owned: AtomicI64,
    /// Successful `pg_try_advisory_lock` acquisitions (first spawn or takeover).
    lease_acquired_total: AtomicU64,
    /// Acquisitions that lost the race and produced a shadow.
    lease_contended_total: AtomicU64,
    /// Leases lost after being held (backend replacement or per-turn
    /// `verify_lease` failure).
    lease_lost_total: AtomicU64,
    /// Forwards to an owner that hit the timeout with no reply (the takeover
    /// trigger). Notify-send failures are not timeouts and are not counted.
    forward_timeouts_total: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Construct with a non-default slow-query log capacity (ENH-019). Used by
    /// `Runtime::new` to thread `Config::slow_query_capacity` through. A cap of
    /// 0 leaves the log inert (records are short-circuited at the call site).
    pub fn with_slow_query_capacity(slow_query_cap: usize) -> Arc<Self> {
        Arc::new(Self {
            slow_queries: Mutex::new(SlowQueryLog::with_capacity(slow_query_cap)),
            ..Self::default()
        })
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

    /// Append a slow-query record to the bounded log (ENH-019). Callers
    /// (`http_api.rs`) short-circuit on `Config::slow_query_ms == 0` before
    /// constructing the record; the buffer here also defends against a 0 cap.
    /// The mutex is held only across the in-memory push (no I/O).
    pub fn record_slow_query(&self, entry: SlowQueryRecord) {
        if let Ok(mut log) = self.slow_queries.lock() {
            log.record(entry);
        }
    }

    /// Newest-first snapshot of the retained slow-query records. Empty when
    /// slow-query logging is off (`RTDB_SLOW_QUERY_CAPACITY=0`) or no query
    /// has crossed the threshold yet. The `GET /admin/slow-queries` handler
    /// applies the `?limit=` truncation after this returns.
    pub fn recent_slow_queries(&self) -> Vec<SlowQueryRecord> {
        self.slow_queries
            .lock()
            .map(|log| log.recent())
            .unwrap_or_default()
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

    /// A recurring job's fire detected `n` elapsed windows before it (missed
    /// while the process was down, or otherwise skipped). Call once per fire
    /// that found `n > 0`; a fire with `n == 0` (the common case) never calls
    /// this.
    pub fn record_scheduled_missed(&self, n: u64) {
        self.scheduled_missed_total.fetch_add(n, Ordering::Relaxed);
    }

    /// Documents re-stamped by the anon→real merge (FM-27), across all dbs/tables.
    pub fn record_merge_doc(&self) {
        self.merge_docs_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Flag multi-instance mode (ENH-049). Called once at boot from the
    /// resolved config; the gauge never changes for the process's lifetime.
    pub fn set_multi_instance(&self, on: bool) {
        self.multi_instance.store(i64::from(on), Ordering::Relaxed);
    }
    /// This replica acquired `db`'s ownership lease (`pg_try_advisory_lock`
    /// returned true) — at first spawn or on takeover.
    pub fn record_lease_acquired(&self) {
        self.lease_acquired_total.fetch_add(1, Ordering::Relaxed);
    }
    /// Another replica held the lease, so this replica spawned a shadow.
    pub fn record_lease_contended(&self) {
        self.lease_contended_total.fetch_add(1, Ordering::Relaxed);
    }
    /// A held lease was lost (lease backend replaced, or the per-turn
    /// `verify_lease` probe found the lock gone).
    pub fn record_lease_lost(&self) {
        self.lease_lost_total.fetch_add(1, Ordering::Relaxed);
    }
    /// A forward to the lease owner hit the timeout with no owner reply —
    /// the trigger for the takeover path.
    pub fn record_forward_timeout(&self) {
        self.forward_timeouts_total.fetch_add(1, Ordering::Relaxed);
    }
    /// A lease-holding channel entry was inserted (this replica now owns a db).
    pub fn lease_owned_inc(&self) {
        self.leases_owned.fetch_add(1, Ordering::Relaxed);
    }
    /// A lease-holding channel entry was removed, by any removal path.
    pub fn lease_owned_dec(&self) {
        self.leases_owned.fetch_sub(1, Ordering::Relaxed);
    }

    /// Snapshot of the per-db subscription-invalidation counters, sorted by db
    /// name for stable output. Empty until a `fan_out` records a decision.
    /// `skips` is the class sum and `rerun_ratio` is `reruns / max(1, reruns +
    /// skips)` — computed here so both admin surfaces (`/admin/metrics`
    /// `perDbSubs` and `/admin/subscriptions` `perDb`) serve the same shape.
    pub fn per_db_subs_snapshot(&self) -> Vec<DbSubCounterRow> {
        let Ok(map) = self.per_db_subs.lock() else {
            return Vec::new();
        };
        let mut rows: Vec<DbSubCounterRow> = map
            .iter()
            .map(|(db, c)| {
                let reruns = c.reruns.load(Ordering::Relaxed);
                let skips_point = c.skips_point.load(Ordering::Relaxed);
                let skips_indexed = c.skips_indexed.load(Ordering::Relaxed);
                let skips_ordered = c.skips_ordered.load(Ordering::Relaxed);
                let skips = skips_point + skips_indexed + skips_ordered;
                DbSubCounterRow {
                    db: db.clone(),
                    reruns,
                    skips_point,
                    skips_indexed,
                    skips_ordered,
                    missed: c.missed.load(Ordering::Relaxed),
                    skips,
                    rerun_ratio: reruns as f64 / reruns.saturating_add(skips).max(1) as f64,
                }
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

    /// One workflow step attempt ended (FM-29): committed (`Success`), failed
    /// with retries left (`Retry`), or exhausted its retries (`Fail`). Global
    /// counter — the per-db run counts by status are served by
    /// [`per_db_workflows_rows`] on the admin metrics JSON instead.
    pub fn record_workflow_step(&self, outcome: WorkflowStepOutcome) {
        let counter = match outcome {
            WorkflowStepOutcome::Success => &self.workflow_steps_success_total,
            WorkflowStepOutcome::Retry => &self.workflow_steps_retry_total,
            WorkflowStepOutcome::Fail => &self.workflow_steps_fail_total,
        };
        counter.fetch_add(1, Ordering::Relaxed);
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
            scheduled_missed_total: self.scheduled_missed_total.load(Ordering::Relaxed),
            merge_docs_total: self.merge_docs_total.load(Ordering::Relaxed),
            image_transforms_hit_total: self.image_transforms_hit_total.load(Ordering::Relaxed),
            image_transforms_miss_total: self.image_transforms_miss_total.load(Ordering::Relaxed),
            image_transforms_error_total: self.image_transforms_error_total.load(Ordering::Relaxed),
            image_transform_bytes_total: self.image_transform_bytes_total.load(Ordering::Relaxed),
            presence_updates_total: self.presence_updates_total.load(Ordering::Relaxed),
            presence_broadcasts_total: self.presence_broadcasts_total.load(Ordering::Relaxed),
            presence_ttl_expiries_total: self.presence_ttl_expiries_total.load(Ordering::Relaxed),
            presence_rooms,
            presence_sessions,
            presence_detail: Vec::new(),
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
            workflow_steps_success_total: self.workflow_steps_success_total.load(Ordering::Relaxed),
            workflow_steps_retry_total: self.workflow_steps_retry_total.load(Ordering::Relaxed),
            workflow_steps_fail_total: self.workflow_steps_fail_total.load(Ordering::Relaxed),
            per_db_workflows: Vec::new(),
        }
    }
}

/// Live per-db workflow-run counts by status for the `/admin/metrics` JSON
/// (FM-29). Reads `workflows::count_by_status` per registered database — a
/// gauge, not a counter, so there is no in-memory map to drain. Sorted by db
/// name (`list_databases` already orders them). Best-effort per db: a database
/// whose per-db tasks have not yet lazily ensured the `workflows` table
/// (created but never touched by a client) is skipped rather than failing the
/// whole snapshot. JSON-snapshot only — deliberately absent from the Prometheus
/// scrape to avoid per-db label cardinality on the public export.
pub async fn per_db_workflows_rows(pool: &PgPool) -> Vec<DbWorkflowStatusRow> {
    let dbs = match crate::db::list_databases(pool).await {
        Ok(dbs) => dbs,
        Err(err) => {
            tracing::warn!(error = %err, "failed to list databases for workflow counts");
            return Vec::new();
        }
    };
    let mut rows = Vec::with_capacity(dbs.len());
    for db in dbs {
        match crate::workflows::count_by_status(pool, &db).await {
            Ok(counts) => {
                let mut row = DbWorkflowStatusRow {
                    db,
                    pending: 0,
                    running: 0,
                    waiting: 0,
                    success: 0,
                    failed: 0,
                    cancelled: 0,
                };
                for (status, n) in counts {
                    match status {
                        WorkflowStatus::Pending => row.pending = n,
                        WorkflowStatus::Running => row.running = n,
                        WorkflowStatus::Waiting => row.waiting = n,
                        WorkflowStatus::Success => row.success = n,
                        WorkflowStatus::Failed => row.failed = n,
                        WorkflowStatus::Cancelled => row.cancelled = n,
                    }
                }
                rows.push(row);
            }
            Err(err) => {
                tracing::debug!(
                    error = %err,
                    db,
                    "skipping per-db workflow counts (table not ensured yet)"
                );
            }
        }
    }
    rows
}
