//! Metrics exposition (QA-003 split): the admin JSON snapshot shape and the
//! Prometheus text renderer.

use std::sync::atomic::Ordering;

use serde::Serialize;

use super::Metrics;
use super::counters::{DbQuotaCounterRow, DbSubCounterRow, DbWorkflowStatusRow, LatencyStats};

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
    /// Cumulative count of recurring-job windows that elapsed before a fire
    /// (all dbs/tables). See `Metrics::scheduled_missed_total`.
    pub scheduled_missed_total: u64,
    /// Total documents re-stamped by the anon→real user merge (all dbs/tables).
    pub merge_docs_total: u64,
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
    /// Per-room presence inspector rows (GET /admin/presence). Populated in
    /// the `/admin/metrics` handler rather than `snapshot` so the periodic
    /// stream gauge ticks don't enumerate rooms every second.
    pub presence_detail: Vec<crate::presence::RoomInspect>,
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
    /// Workflow step attempts by outcome (FM-29), surfaced in BOTH the JSON
    /// snapshot and the Prometheus scrape
    /// (`rtdb_workflow_steps_total{outcome=…}`).
    pub workflow_steps_success_total: u64,
    pub workflow_steps_retry_total: u64,
    pub workflow_steps_fail_total: u64,
    /// Per-database workflow-run counts by status (FM-29). Populated only by
    /// the `/admin/metrics` handler (a live side-table read — see
    /// [`per_db_workflows_rows`](crate::metrics::per_db_workflows_rows)), so the periodic `/admin/stream` gauge ticks
    /// and other `snapshot()` callers see an empty list. JSON-snapshot only —
    /// deliberately absent from the Prometheus scrape (per-db labels would
    /// blow up cardinality on the public export).
    pub per_db_workflows: Vec<DbWorkflowStatusRow>,
}

/// Render the snapshot as Prometheus text-exposition format (version 0.0.4).
///
/// Pure (no I/O) so it is trivially unit-testable. When `fingerprint` is
/// `Some((version, git_commit))`, a `rtdb_build_info` gauge records which build
/// is live — mirroring the identity fields on `/healthz` and `/admin/config`.
/// Pass `None` on an unauthenticated request so the exact build version and
/// commit sha are not disclosed to an unauthenticated scraper (SEC-129 —
/// liveness/metrics stay unauthenticated, build fingerprint is admin-gated).
/// Both fields are build-time constants (semver / hex sha or `"unknown"`), so
/// they are interpolated into the label value without escaping.
///
/// ENH-049: the ownership-lease series render from the live [`Metrics`]
/// counters rather than the snapshot — they are Prometheus-only by design, so
/// `MetricsSnapshot` (the admin JSON wire, mirrored in four clients) stays
/// untouched. `instance_id` labels `rtdb_build_info` so a fleet-wide scrape
/// can attribute lease churn to a replica.
pub fn render_prometheus(
    snap: &MetricsSnapshot,
    m: &Metrics,
    fingerprint: Option<(&str, &str)>,
    instance_id: &str,
) -> String {
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

    // Durable workflow steps (FM-29). Aggregate-by-outcome only — no per-db
    // labels (cardinality). The per-db status counts live in the JSON snapshot.
    s.push_str("# HELP rtdb_workflow_steps_total Workflow step attempts, by outcome.\n");
    s.push_str("# TYPE rtdb_workflow_steps_total counter\n");
    s.push_str(&format!(
        "rtdb_workflow_steps_total{{outcome=\"success\"}} {}\n",
        snap.workflow_steps_success_total
    ));
    s.push_str(&format!(
        "rtdb_workflow_steps_total{{outcome=\"retry\"}} {}\n",
        snap.workflow_steps_retry_total
    ));
    s.push_str(&format!(
        "rtdb_workflow_steps_total{{outcome=\"fail\"}} {}\n",
        snap.workflow_steps_fail_total
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

    s.push_str(
        "# HELP rtdb_scheduled_missed_total Recurring-job windows that elapsed before a fire (e.g. process downtime), across all dbs/tables.\n",
    );
    s.push_str("# TYPE rtdb_scheduled_missed_total counter\n");
    s.push_str(&format!(
        "rtdb_scheduled_missed_total {}\n",
        snap.scheduled_missed_total
    ));

    // Anon→real merge (FM-27).
    s.push_str(
        "# HELP rtdb_merge_docs_total Total documents re-stamped by the anon-to-real user merge across all dbs/tables.\n",
    );
    s.push_str("# TYPE rtdb_merge_docs_total counter\n");
    s.push_str(&format!(
        "rtdb_merge_docs_total {}\n",
        snap.merge_docs_total
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
    // Omitted entirely on an unauthenticated request (SEC-129). ENH-049:
    // `instance_id` joins the label set so fleet-wide scrapes can attribute
    // lease churn; the value is a boot-resolved config scalar or a generated
    // short hex id (never per-request data), so no escaping is needed.
    if let Some((version, git_commit)) = fingerprint {
        s.push_str("# HELP rtdb_build_info Build identity (version, git_commit, instance_id).\n");
        s.push_str("# TYPE rtdb_build_info gauge\n");
        s.push_str(&format!(
            "rtdb_build_info{{version=\"{version}\",git_commit=\"{git_commit}\",instance_id=\"{instance_id}\"}} 1\n"
        ));
    }

    // Ownership-lease observability (ENH-049). Aggregate gauges + counters —
    // per-db detail belongs in the tracing logs (each lease event carries the
    // `db` field), not metric labels (cardinality).
    s.push_str(
        "# HELP rtdb_multi_instance Whether multi-instance mode is enabled on this replica.\n",
    );
    s.push_str("# TYPE rtdb_multi_instance gauge\n");
    s.push_str(&format!(
        "rtdb_multi_instance {}\n",
        m.multi_instance.load(Ordering::Relaxed)
    ));

    s.push_str(
        "# HELP rtdb_leases_owned Databases whose ownership lease this replica currently holds.\n",
    );
    s.push_str("# TYPE rtdb_leases_owned gauge\n");
    s.push_str(&format!(
        "rtdb_leases_owned {}\n",
        m.leases_owned.load(Ordering::Relaxed)
    ));

    s.push_str(
        "# HELP rtdb_lease_acquired_total Successful ownership-lease acquisitions (first spawn or takeover).\n",
    );
    s.push_str("# TYPE rtdb_lease_acquired_total counter\n");
    s.push_str(&format!(
        "rtdb_lease_acquired_total {}\n",
        m.lease_acquired_total.load(Ordering::Relaxed)
    ));

    s.push_str(
        "# HELP rtdb_lease_contended_total Acquisitions that lost the race and produced a shadow.\n",
    );
    s.push_str("# TYPE rtdb_lease_contended_total counter\n");
    s.push_str(&format!(
        "rtdb_lease_contended_total {}\n",
        m.lease_contended_total.load(Ordering::Relaxed)
    ));

    s.push_str(
        "# HELP rtdb_lease_lost_total Leases lost after being held (lease backend replaced, lock revoked). ALERT ON ANY INCREASE.\n",
    );
    s.push_str("# TYPE rtdb_lease_lost_total counter\n");
    s.push_str(&format!(
        "rtdb_lease_lost_total {}\n",
        m.lease_lost_total.load(Ordering::Relaxed)
    ));

    s.push_str(
        "# HELP rtdb_forward_timeouts_total Forwards to the lease owner that timed out with no owner reply (the takeover trigger).\n",
    );
    s.push_str("# TYPE rtdb_forward_timeouts_total counter\n");
    s.push_str(&format!(
        "rtdb_forward_timeouts_total {}\n",
        m.forward_timeouts_total.load(Ordering::Relaxed)
    ));
    s
}
