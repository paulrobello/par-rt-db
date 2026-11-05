//! Metrics counter primitives (QA-003 split): latency ring buffers, the
//! slow-query log, skip classes, and the per-db counter/quota/workflow row
//! types the registry in `super::Metrics` accumulates and the snapshot in
//! `super::snapshot` serializes.

use std::sync::atomic::AtomicU64;

use serde::Serialize;

/// Ring-buffer capacity for [`LatencySamples`] (1024 micros samples per bucket).
pub(super) const LATENCY_BUF_CAP: usize = 1024;

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
    pub(super) buf: Vec<u64>,
    pub(super) next: usize,
    pub(super) len: usize,
}

impl LatencySamples {
    pub(super) fn record(&mut self, us: u64) {
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

    pub(super) fn percentiles(&self) -> LatencyStats {
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

/// One entry in the slow-query log (ENH-019). Recorded at the existing query
/// timing call sites in `http_api.rs` when a query's wall-clock duration
/// exceeds `Config::slow_query_ms`. `sql` is the exact compiled string the
/// real query path executed (the same string `/explain` returns); `params`
/// is included only when `Config::slow_query_log_params` is set, to avoid
/// capturing document content by default. `terminal` is the wire terminal
/// (`get`/`count`/`collect`/…) for at-a-glance grouping.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlowQueryRecord {
    /// When the query started, as epoch milliseconds. i64 to match
    /// [`crate::db::now_ms`]; negative values are not expected for realistic
    /// wall-clock time but the type matches the rest of the codebase.
    pub started_at_ms: i64,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    pub db: String,
    pub table: String,
    pub terminal: String,
    pub sql: String,
    /// Bound parameters, included only when `RTDB_SLOW_QUERY_LOG_PARAMS=true`.
    /// `None` otherwise to keep document content out of the log by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<Vec<String>>,
}

/// Bounded ring buffer of recent slow-query records (ENH-019). `record`
/// pushes while the buffer has room, then overwrites in insertion order once
/// full (cap set at construction from `Config::slow_query_capacity`).
/// `recent` clones the filled slice newest-first for the `/admin/slow-queries`
/// handler. Held under a `std::sync::Mutex` inside [`Metrics`]; the clone runs
/// under the lock because the typical capacity (200) is a few-KB copy and the
/// alternative (clone-under-lock vs. iterate-under-lock) makes no measurable
/// difference at that size.
#[derive(Default)]
pub struct SlowQueryLog {
    buf: Vec<SlowQueryRecord>,
    /// Write cursor — next slot to overwrite when `buf.len() == cap`.
    next: usize,
    /// Maximum records to retain; `0` disables logging entirely (records are
    /// discarded at the call site when the cap is 0, so this struct only ever
    /// sees a non-zero cap in practice).
    cap: usize,
}

impl SlowQueryLog {
    /// Construct a log with the given bounded capacity. `cap == 0` produces an
    /// inert log (no allocation, no retention) — the caller is expected to
    /// short-circuit recording when the cap is 0; we still defend here.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
            next: 0,
            cap,
        }
    }

    pub(super) fn record(&mut self, entry: SlowQueryRecord) {
        if self.cap == 0 {
            return;
        }
        if self.buf.len() < self.cap {
            self.buf.push(entry);
            self.next = self.buf.len() % self.cap;
        } else {
            self.buf[self.next] = entry;
            self.next = (self.next + 1) % self.cap;
        }
    }

    /// Newest-first snapshot of the retained records. Iteration order in the
    /// ring is insertion order with wrap-around, so we walk from `next` (the
    /// oldest live slot once full) backwards to produce newest-first.
    pub(super) fn recent(&self) -> Vec<SlowQueryRecord> {
        if self.buf.is_empty() {
            return Vec::new();
        }
        let n = self.buf.len();
        let mut out = Vec::with_capacity(n);
        // Once full, `next` points at the oldest entry; before full, index 0
        // is the oldest and `next == n`. Walk oldest -> newest, then reverse
        // for newest-first.
        for i in 0..n {
            let idx = (self.next + i) % n;
            out.push(self.buf[idx].clone());
        }
        out.reverse();
        out
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
pub(super) struct DbSubCounters {
    pub(super) reruns: AtomicU64,
    pub(super) skips_point: AtomicU64,
    pub(super) skips_indexed: AtomicU64,
    pub(super) skips_ordered: AtomicU64,
    pub(super) missed: AtomicU64,
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
    /// Total skips across the three classes, so consumers can read the rerun
    /// ratio without re-summing the class breakdown (ENH-024).
    pub skips: u64,
    /// `reruns / max(1, reruns + skips)` — always in [0, 1]. Sustained above
    /// 0.5 means re-runs dominate this db's fan-out and its writes queue
    /// behind subscriber load (see deploy/README.md "Monitoring the
    /// invalidation fan-out" for the alert and remediation levers).
    pub rerun_ratio: f64,
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

/// How one workflow step attempt ended (FM-29). Same label-not-name pattern
/// as `QuotaKind`: the outcome becomes the `outcome` label on the aggregate
/// `rtdb_workflow_steps_total` counter.
#[derive(Debug, Clone, Copy)]
pub enum WorkflowStepOutcome {
    Success,
    Retry,
    Fail,
}

/// Per-database resource-quota rejection counters — the per-db breakdown of the
/// global `quota_rejections_*_total` counters on [`Metrics`]. Same shape and
/// same concurrency posture as [`DbSubCounters`]: held under a
/// `Mutex<HashMap<String, Self>>`, the mutex held only across a single atomic
/// increment. Exposed only in the JSON metrics snapshot; deliberately absent
/// from the Prometheus scrape to avoid per-db label cardinality on the public
/// export (see `lib.rs:239` — the `/metrics` scrape is aggregate-only).
#[derive(Default)]
pub(super) struct QuotaCounters {
    pub(super) tables: AtomicU64,
    pub(super) storage: AtomicU64,
    pub(super) subs: AtomicU64,
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

/// One db's workflow-run counts by status (FM-29), the JSON-snapshot shape.
/// camelCase on the wire. Populated by a live `workflows::count_by_status`
/// read (status counts are gauges, not monotonic counters), so unlike the
/// counter-backed per-db sections there is no in-memory map behind it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DbWorkflowStatusRow {
    pub db: String,
    pub pending: i64,
    pub running: i64,
    pub waiting: i64,
    pub success: i64,
    pub failed: i64,
    pub cancelled: i64,
}
