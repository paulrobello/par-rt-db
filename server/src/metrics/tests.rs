use super::counters::{LATENCY_BUF_CAP, LatencySamples, LatencyStats};
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
        scheduled_missed_total: 0,
        merge_docs_total: 0,
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
        workflow_steps_success_total: 0,
        workflow_steps_retry_total: 0,
        workflow_steps_fail_total: 0,
        per_db_workflows: Vec::new(),
        presence_detail: Vec::new(),
    };
    let body = render_prometheus(
        &snap,
        &Metrics::default(),
        Some(("0.0.0", "abc")),
        "test-instance",
    );
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
        body.contains(
            "rtdb_build_info{version=\"0.0.0\",git_commit=\"abc\",instance_id=\"test-instance\"} 1"
        ),
        "missing build_info sample: {body}"
    );
    // ENH-049: the six ownership-lease series render (zero defaults).
    assert!(
        body.contains("# TYPE rtdb_multi_instance gauge") && body.contains("rtdb_multi_instance 0"),
        "missing multi_instance gauge: {body}"
    );
    assert!(
        body.contains("rtdb_leases_owned 0"),
        "missing leases_owned gauge: {body}"
    );
    assert!(
        body.contains("rtdb_lease_acquired_total 0"),
        "missing lease_acquired counter: {body}"
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
        scheduled_missed_total: 0,
        merge_docs_total: 0,
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
        workflow_steps_success_total: 0,
        workflow_steps_retry_total: 0,
        workflow_steps_fail_total: 0,
        per_db_workflows: Vec::new(),
        presence_detail: Vec::new(),
    };
    let body = render_prometheus(
        &snap,
        &Metrics::default(),
        Some(("0.0.0", "abc")),
        "test-instance",
    );
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
    // ENH-024 totals: skips sums the classes, ratio = reruns / max(1,
    // reruns + skips) — db-a is 1/(1+3) = 0.25, db-b 0/(0+1) = 0.
    assert_eq!(rows[0].skips, 3);
    assert_eq!(rows[0].rerun_ratio, 0.25);
    assert_eq!(rows[1].skips, 1);
    assert_eq!(rows[1].rerun_ratio, 0.0);
    for row in &rows {
        assert!((0.0..=1.0).contains(&row.rerun_ratio));
    }
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
        scheduled_missed_total: 0,
        merge_docs_total: 0,
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
        workflow_steps_success_total: 0,
        workflow_steps_retry_total: 0,
        workflow_steps_fail_total: 0,
        per_db_workflows: Vec::new(),
        presence_detail: Vec::new(),
    };
    let body = render_prometheus(
        &snap,
        &Metrics::default(),
        Some(("0.0.0", "abc")),
        "test-instance",
    );
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
fn workflow_step_counters_record_and_render() {
    // Mirrors `quota_rejections_land_in_snapshot_and_prometheus`: the
    // three outcome counters increment via `record_workflow_step`, and the
    // aggregate-by-outcome totals appear in the Prometheus scrape under one
    // metric name with an `outcome` label (no per-db labels).
    let m = Metrics::default();
    m.record_workflow_step(WorkflowStepOutcome::Success);
    m.record_workflow_step(WorkflowStepOutcome::Success);
    m.record_workflow_step(WorkflowStepOutcome::Retry);
    m.record_workflow_step(WorkflowStepOutcome::Fail);
    assert_eq!(
        m.workflow_steps_success_total.load(Ordering::Relaxed),
        2,
        "two successes"
    );
    assert_eq!(
        m.workflow_steps_retry_total.load(Ordering::Relaxed),
        1,
        "one retry"
    );
    assert_eq!(
        m.workflow_steps_fail_total.load(Ordering::Relaxed),
        1,
        "one terminal failure"
    );
    // Prometheus renders the three aggregate-by-outcome totals.
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
        scheduled_missed_total: 0,
        merge_docs_total: 0,
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
        workflow_steps_success_total: 2,
        workflow_steps_retry_total: 1,
        workflow_steps_fail_total: 1,
        per_db_workflows: Vec::new(),
        presence_detail: Vec::new(),
    };
    let body = render_prometheus(
        &snap,
        &Metrics::default(),
        Some(("0.0.0", "abc")),
        "test-instance",
    );
    assert!(
        body.contains("# TYPE rtdb_workflow_steps_total counter"),
        "{body}"
    );
    assert!(
        body.contains("rtdb_workflow_steps_total{outcome=\"success\"} 2"),
        "{body}"
    );
    assert!(
        body.contains("rtdb_workflow_steps_total{outcome=\"retry\"} 1"),
        "{body}"
    );
    assert!(
        body.contains("rtdb_workflow_steps_total{outcome=\"fail\"} 1"),
        "{body}"
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

#[test]
fn lease_series_record_and_render() {
    // ENH-049: the ownership-lease counters/gauge land on the live Metrics
    // and render in the Prometheus scrape, while MetricsSnapshot (the
    // admin JSON wire) carries none of them — the compile-time proof is
    // that this test constructs a snapshot with no lease fields.
    let m = Metrics::default();
    m.set_multi_instance(true);
    m.record_lease_acquired();
    m.record_lease_contended();
    m.record_lease_contended();
    m.lease_owned_inc();
    m.record_forward_timeout();

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
        scheduled_missed_total: 0,
        merge_docs_total: 0,
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
        workflow_steps_success_total: 0,
        workflow_steps_retry_total: 0,
        workflow_steps_fail_total: 0,
        per_db_workflows: Vec::new(),
        presence_detail: Vec::new(),
    };
    let body = render_prometheus(&snap, &m, Some(("0.0.0", "abc")), "inst-1");
    assert!(body.contains("rtdb_multi_instance 1"), "{body}");
    assert!(body.contains("rtdb_leases_owned 1"), "{body}");
    assert!(body.contains("rtdb_lease_acquired_total 1"), "{body}");
    assert!(body.contains("rtdb_lease_contended_total 2"), "{body}");
    assert!(body.contains("rtdb_lease_lost_total 0"), "{body}");
    assert!(body.contains("rtdb_forward_timeouts_total 1"), "{body}");
    assert!(
        body.contains(
            "rtdb_build_info{version=\"0.0.0\",git_commit=\"abc\",instance_id=\"inst-1\"} 1"
        ),
        "{body}"
    );

    // Removal decrements the gauge; a lost lease is recorded.
    m.lease_owned_dec();
    m.record_lease_lost();
    let body = render_prometheus(&snap, &m, None, "inst-1");
    assert!(body.contains("rtdb_leases_owned 0"), "{body}");
    assert!(body.contains("rtdb_lease_lost_total 1"), "{body}");
    // No fingerprint => no build_info line at all (SEC-129 unchanged).
    assert!(!body.contains("rtdb_build_info"), "{body}");
}
