//! Integration tests for the `scheduled_txns` side table and its accessors
//! (Task 2 of the scheduled/cron transactions feature). The harness mirrors
//! `tests/txn_test.rs`: the shared `common` module bootstraps `rtdb_auth` and
//! hands out a pool; each test creates a uniquely-named `t<uuid>` database via
//! `db::create_database` (which now also creates the `scheduled_txns` table).
//! `ensure_table` is still called per test to exercise the idempotent
//! pre-feature migration path.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use sqlx::PgPool;

use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::{CommitterConfig, Committers};
use rtdb_server::db::SchemaCache;
use rtdb_server::ddl;
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::protocol::{ScheduleKind, ScheduleStatus};
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::quota;
use rtdb_server::scheduler;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::SubscriptionManager;
use rtdb_server::txn::{Step, Transaction};

/// Mirrors `common::test_state()`'s pool setup: connect to the shared dev
/// Postgres and bootstrap `rtdb_auth`. Each test gets its own connection so
/// they don't share a `PgPool`'s lifetime.
async fn test_pool() -> PgPool {
    let state = common::test_state().await;
    state.pool.clone()
}

/// Mirrors `common::fresh_db()`'s naming + creation path, minus the schema
/// push (these tests never query user tables, so no schema is needed).
async fn unique_db(pool: &PgPool) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    common::wrap_test_db(name)
}

fn empty_txn() -> Transaction {
    Transaction { steps: vec![] }
}

#[tokio::test]
async fn insert_list_cancel_roundtrip() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();

    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "oneshot", 123, &txn, None, None)
        .await
        .unwrap();
    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].kind, ScheduleKind::Oneshot);
    assert_eq!(listed[0].status, ScheduleStatus::Pending);
    assert_eq!(listed[0].due_at, 123);

    assert!(scheduler::cancel(&pool, &db, &id).await.unwrap());
    assert!(scheduler::list(&pool, &db).await.unwrap().is_empty());
    assert!(!scheduler::cancel(&pool, &db, &id).await.unwrap()); // already gone
}

#[tokio::test]
async fn pause_resume_cron_recomputes_due() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"), None)
        .await
        .unwrap();

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, ScheduleStatus::Paused);
    // A paused job must not be claimed even if due_at is in the past.
    let claimed = scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    assert!(claimed.is_empty());

    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, ScheduleStatus::Pending);
    assert!(info.due_at > 1); // recomputed forward from now
}

#[tokio::test]
async fn claim_due_and_finalize() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let one = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None, None)
        .await
        .unwrap();
    let cron = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"), None)
        .await
        .unwrap();

    let claimed = scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    scheduler::finalize_one_shot_done(&pool, &db, &one)
        .await
        .unwrap();
    let next = scheduler::next_fire("*/5 * * * *", rtdb_server::db::now_ms()).unwrap();
    scheduler::finalize_recurring_next(&pool, &db, &cron, next)
        .await
        .unwrap();

    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1); // one-shot deleted, cron remains
    assert_eq!(listed[0].id, cron);
    assert_eq!(listed[0].fired_count, 1);
    assert_eq!(listed[0].status, ScheduleStatus::Pending);
}

#[tokio::test]
async fn reset_running_recovers_orphans() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let _id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None, None)
        .await
        .unwrap();
    // Simulate a crash mid-fire: the committer claimed but never finalized.
    scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    let n = scheduler::reset_running(&pool, &db).await.unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        scheduler::list(&pool, &db).await.unwrap()[0].status,
        ScheduleStatus::Pending
    );
}

#[tokio::test]
async fn next_due_and_mark_error() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();

    // Empty table → nothing due.
    assert!(scheduler::next_due(&pool, &db).await.unwrap().is_none());

    let _a = scheduler::insert(&pool, &db, "oneshot", 50, &txn, None, None)
        .await
        .unwrap();
    let b = scheduler::insert(&pool, &db, "oneshot", 10, &txn, None, None)
        .await
        .unwrap();
    let _c = scheduler::insert(&pool, &db, "oneshot", 90, &txn, None, None)
        .await
        .unwrap();

    // Min due_at among pending rows is 10.
    assert_eq!(scheduler::next_due(&pool, &db).await.unwrap(), Some(10));

    // Marking an error removes the row from the pending pool, bumping next_due.
    scheduler::mark_error(&pool, &db, &b, "boom").await.unwrap();
    let info = scheduler::list(&pool, &db)
        .await
        .unwrap()
        .into_iter()
        .find(|s| s.id == b)
        .unwrap();
    assert_eq!(info.status, ScheduleStatus::Error);
    assert_eq!(info.last_error.as_deref(), Some("boom"));
    assert_eq!(scheduler::next_due(&pool, &db).await.unwrap(), Some(50));
}

#[tokio::test]
async fn pause_resume_one_shot_keeps_due_at() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "oneshot", 42, &txn, None, None)
        .await
        .unwrap();

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, ScheduleStatus::Pending);
    assert_eq!(info.due_at, 42); // unchanged by resume

    // Resuming a non-paused job is a no-op (returns false).
    assert!(!scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
}

// --- Task 3: end-to-end firing tests -------------------------------------
//
// These exercise the full scheduler→committer path: the per-db scheduler timer
// claims a due row and enqueues a fire-and-forget `RunScheduled`, the committer
// runs the txn through `execute_txn` + `fan_out`, then finalizes the row. The
// committer (and thus the scheduler) are lazily spawned on the first request to
// a db, so each test warms the committer up with a no-op mutate before relying
// on the scheduler having started.

/// Pushes a one-table schema (`items` with an indexed number field `n`) so a
/// firing job has an observable effect. Mirrors `common::fresh_db`'s push path.
async fn push_simple_schema(pool: &PgPool, db: &str) -> SchemaDef {
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({"tables":{
        "items":{
            "fields":{"n":{"type":"number"}},
            "indexes":[{"name":"by_n","fields":["n"]}]}
    }}))
    .expect("parse items schema");
    let (applied, _) = ddl::push_schema(pool, db, schema)
        .await
        .expect("push items schema");
    applied
}

/// Triggers lazy spawn of `db`'s committer + scheduler tasks by submitting a
/// no-op mutate. Both spawn inside `channel_for` on first use.
async fn warm_up_committer(committers: &Committers, db: &str) {
    committers
        .mutate(
            db,
            None,
            Transaction { steps: vec![] },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("warm-up mutate");
}

/// Polls `items` by `n` until a doc with that value appears or `timeout`
/// elapses. Returns true if observed. Used instead of a fixed sleep so the
/// test is not timing-sensitive: due_at is already in the past, so the
/// scheduler claims on its first wake (within MAX_SLEEP) and the committer
/// executes immediately; 5s is far above the real latency.
async fn poll_for_n(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    n: i64,
    timeout: Duration,
) -> bool {
    let query = Query {
        table: "items".to_string(),
        get: None,
        index: Some("by_n".to_string()),
        eq: vec![serde_json::json!(n)],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    };
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(QueryResult::Docs(docs)) =
            execute_query(pool, db, schema, &query, &PrincipalCtx::bypass(), false).await
            && !docs.is_empty()
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Polls `scheduler::list` until `pred` returns `Some`, or `timeout`
/// elapses. Used to wait for a job's finalized row state (deleted for a
/// one-shot; pending+fired_count for a cron): finalize runs after `fan_out`,
/// so observing the doc via `poll_for_n` then immediately reading the row
/// would race the finalize step.
async fn poll_list<T, F>(pool: &PgPool, db: &str, timeout: Duration, pred: F) -> Option<T>
where
    F: Fn(&[scheduler::ScheduleInfo]) -> Option<T>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(listed) = scheduler::list(pool, db).await
            && let Some(t) = pred(&listed)
        {
            return Some(t);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn one_shot_fires_and_writes() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // Schedule a one-shot due in the past so it fires on the scheduler's first
    // wake. The txn inserts a doc the test can observe.
    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "n": 42 }).as_object().unwrap().clone(),
        }],
    };
    let _id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None, None)
        .await
        .unwrap();

    // Spawn the committer + scheduler for this db.
    warm_up_committer(&committers, &db).await;

    // The doc appears once the scheduler claims and the committer executes.
    let appeared = poll_for_n(&pool, &db, &schema, 42, Duration::from_secs(15)).await;
    assert!(appeared, "scheduled one-shot never wrote its doc");

    // A one-shot row is deleted after a successful fire. Poll for the delete:
    // finalize (the DELETE) runs after `fan_out`, so reading the row the
    // instant the doc appears would race it.
    let deleted = poll_list(&pool, &db, Duration::from_secs(8), |l| {
        if l.is_empty() { Some(()) } else { None }
    })
    .await;
    assert!(
        deleted.is_some(),
        "one-shot row should be gone after firing"
    );
}

#[tokio::test]
async fn cron_fires_and_stays_pending() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // `* * * * *` = every minute. Scheduled due in the past, so it fires once
    // immediately, then `handle_scheduled` recomputes the next fire and sets
    // the row back to pending with fired_count = 1.
    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "n": 7 }).as_object().unwrap().clone(),
        }],
    };
    let _id = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("* * * * *"), None)
        .await
        .unwrap();

    warm_up_committer(&committers, &db).await;

    let appeared = poll_for_n(&pool, &db, &schema, 7, Duration::from_secs(15)).await;
    assert!(appeared, "scheduled cron never wrote its doc");

    // After firing, the cron row returns to pending with fired_count >= 1.
    // Poll for that finalized state: finalize (status→pending, fired_count++)
    // runs after `fan_out`, so reading the row the instant the doc appears
    // would observe the intermediate 'running' row instead.
    let info = poll_list(&pool, &db, Duration::from_secs(8), |l| {
        l.iter()
            .find(|i| {
                i.kind == ScheduleKind::Cron
                    && i.status == ScheduleStatus::Pending
                    && i.fired_count >= 1
            })
            .cloned()
    })
    .await
    .expect("cron row should be pending with fired_count >= 1");
    assert_eq!(info.kind, ScheduleKind::Cron);
    assert_eq!(info.status, ScheduleStatus::Pending);
}

#[tokio::test]
async fn failing_cron_reschedules_anyway() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let _schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // A cron whose txn FAILS every fire: `ExpectVersion` against a document
    // that does not exist returns NotFound, so `execute_txn` rejects the job.
    // Per spec, a failing cron must log the error and reschedule (keep firing)
    // rather than stick in `status='error'`. `fired_count` counts successful
    // fires only, so it must stay 0.
    let txn = Transaction {
        steps: vec![Step::ExpectVersion {
            table: "items".to_string(),
            id: "no-such-doc".to_string(),
            version: 999,
        }],
    };
    let _id = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("* * * * *"), None)
        .await
        .unwrap();

    let before = rtdb_server::db::now_ms();
    warm_up_committer(&committers, &db).await;

    // The scheduler claims the due row and the committer runs the txn, which
    // fails; `handle_scheduled`'s failure branch must reschedule rather than
    // mark_error. Poll for the rescheduled state: status='pending' (NOT
    // 'error'), last_error set, fired_count 0, due_at advanced beyond now.
    let info = poll_list(&pool, &db, Duration::from_secs(15), |l| {
        l.iter()
            .find(|i| {
                i.kind == ScheduleKind::Cron
                    && i.status == ScheduleStatus::Pending
                    && i.last_error.is_some()
            })
            .cloned()
    })
    .await
    .expect("failing cron should be rescheduled (pending with last_error)");

    assert_eq!(
        info.status,
        ScheduleStatus::Pending,
        "failing cron must keep firing"
    );
    assert!(
        info.last_error.is_some(),
        "failure must be recorded in last_error"
    );
    assert_eq!(
        info.fired_count, 0,
        "fired_count counts successful fires only"
    );
    assert!(
        info.due_at > before,
        "due_at must advance to the next fire, not stay in the past"
    );
}

// --- Task 7: scheduler catch-up / no-backfill / one-shot failure semantics --

#[tokio::test]
async fn one_shot_catches_up_after_being_past_due() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // Warm the committer+scheduler up FIRST so the per-db scheduler loop is
    // already running, THEN insert a one-shot whose due_at is ~1 hour in the
    // past. The running scheduler must catch the newly-past-due row on its
    // next sweep (within MAX_SLEEP=2s) and fire it, not drop it. This is a
    // distinct timing path from `one_shot_fires_and_writes`, which inserts
    // before warming up (testing startup discovery of stale rows); this test
    // catches the bug class where a running scheduler fails to re-read
    // `next_due` and misses rows inserted after it started.
    warm_up_committer(&committers, &db).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "n": 5 }).as_object().unwrap().clone(),
        }],
    };
    let one_hour_ago = rtdb_server::db::now_ms() - 3_600_000;
    let _id = scheduler::insert(&pool, &db, "oneshot", one_hour_ago, &txn, None, None)
        .await
        .unwrap();

    let appeared = poll_for_n(&pool, &db, &schema, 5, Duration::from_secs(15)).await;
    assert!(
        appeared,
        "running scheduler must catch up a newly-inserted past-due one-shot"
    );
}

#[tokio::test]
async fn cron_skips_missed_windows() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // `* * * * *` (every minute) with due_at ~1 hour in the past. A naive
    // backfilling scheduler would fire ~60 times for the missed hour; the spec
    // requires exactly ONE fire on the next sweep, after which `due_at` jumps
    // to the next minute boundary after now (because `handle_scheduled`
    // computes `next_fire(expr, now_ms())`, not `next_fire(prev_due_at)`).
    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "n": 77 }).as_object().unwrap().clone(),
        }],
    };
    let one_hour_ago = rtdb_server::db::now_ms() - 3_600_000;
    let _id = scheduler::insert(
        &pool,
        &db,
        "cron",
        one_hour_ago,
        &txn,
        Some("* * * * *"),
        None,
    )
    .await
    .unwrap();

    let before = rtdb_server::db::now_ms();
    warm_up_committer(&committers, &db).await;

    // Poll for the finalized state after exactly one fire: status back to
    // pending with fired_count == 1. If the scheduler were backfilling the
    // missed hour, fired_count would jump past 1 within milliseconds and this
    // predicate would never match (test times out -> fails).
    let info = poll_list(&pool, &db, Duration::from_secs(10), |l| {
        l.iter()
            .find(|i| {
                i.kind == ScheduleKind::Cron
                    && i.status == ScheduleStatus::Pending
                    && i.fired_count == 1
            })
            .cloned()
    })
    .await
    .expect("cron should fire exactly once and return to pending");

    assert_eq!(
        info.fired_count, 1,
        "cron must not backfill the ~60 missed minutes"
    );
    assert!(
        info.due_at > before,
        "due_at must advance past now (next minute boundary), not stay in the past"
    );
    // For `* * * * *`, next_fire(now) is at most 60s in the future. `before`
    // was captured just before warmup, so allow margin for warmup + scheduler
    // wake + fire latency.
    assert!(
        info.due_at - before < 75_000,
        "due_at must be the next minute boundary after now, not far in the future"
    );

    // The doc was written exactly once (one fire, one insert).
    let appeared = poll_for_n(&pool, &db, &schema, 77, Duration::from_secs(8)).await;
    assert!(
        appeared,
        "cron should have written its doc on the single fire"
    );
}

#[tokio::test]
async fn failing_txn_marks_error_one_shot() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    // A one-shot whose txn is set up to FAIL: step 1 inserts a doc, step 2 is
    // an ExpectVersion against a nonexistent row (NotFound). Because
    // `execute_txn` runs all steps in one Postgres transaction, step 1's
    // insert is rolled back when step 2 fails. Per spec, a failing one-shot
    // records the error and STOPS — status='error', row kept (not deleted like
    // a successful one-shot, not rescheduled like a failing cron). This is the
    // one-shot counterpart of `failing_cron_reschedules_anyway`.
    let txn = Transaction {
        steps: vec![
            Step::Insert {
                table: "items".to_string(),
                doc: serde_json::json!({ "n": 123 }).as_object().unwrap().clone(),
            },
            Step::ExpectVersion {
                table: "items".to_string(),
                id: "no-such-doc".to_string(),
                version: 999,
            },
        ],
    };
    let _id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None, None)
        .await
        .unwrap();

    warm_up_committer(&committers, &db).await;

    // Poll for the error state: status='error' with last_error set (NOT
    // deleted like a successful one-shot, NOT pending like a rescheduled
    // cron). `mark_error` runs after `execute_txn` fails, so observing this
    // state means the txn has already been attempted and rolled back.
    let info = poll_list(&pool, &db, Duration::from_secs(15), |l| {
        l.iter()
            .find(|i| i.kind == ScheduleKind::Oneshot && i.status == ScheduleStatus::Error)
            .cloned()
    })
    .await
    .expect("failing one-shot should be marked error, not deleted or pending");

    assert_eq!(info.status, ScheduleStatus::Error);
    assert!(
        info.last_error.is_some(),
        "failure must be recorded in last_error"
    );
    assert_eq!(
        info.fired_count, 0,
        "fired_count counts successful fires only"
    );

    // The (failing) write did NOT occur: `execute_txn`'s atomicity rolled back
    // step 1's insert when step 2 failed. By the time we observe status
    // ='error' the txn has already been attempted and failed, so the doc will
    // never appear; the short poll confirms non-existence rather than asserting
    // it instantaneously (which would race the mark_error UPDATE).
    let appeared = poll_for_n(&pool, &db, &schema, 123, Duration::from_millis(300)).await;
    assert!(
        !appeared,
        "failing txn's write must be rolled back — no doc with n=123 should exist"
    );
}

/// Unit-level: resuming a paused interval job shifts `due_at` to
/// `now + every_ms` — windows elapsed while paused are skipped, never
/// backfilled (a resume that kept the stale `due_at` would fire immediately
/// for every missed window).
#[tokio::test]
async fn pause_resume_interval_shifts_due_from_resume() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "interval", 1, &txn, None, Some(60_000))
        .await
        .unwrap();

    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.kind, ScheduleKind::Interval);
    assert_eq!(info.every_ms, Some(60_000));

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, ScheduleStatus::Pending);
    let now = rtdb_server::db::now_ms();
    // One full interval out from the resume instant — not the stale due_at
    // (1), not a catch-up burst.
    assert!(
        info.due_at >= now + 59_000 && info.due_at <= now + 60_000,
        "resume must shift due_at to now + everyMs, got {} vs now {}",
        info.due_at,
        now
    );
}

/// E2E (decision-pinning): an interval job fires repeatedly, re-arming from
/// each fire; pause stops fires outright; resume shifts the next fire one
/// full interval from the resume instant instead of backfilling the windows
/// that elapsed while paused.
#[tokio::test]
async fn interval_fires_repeatedly_and_skips_paused_windows() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let _schema = push_simple_schema(&pool, &db).await;
    let committers = Committers::new(
        pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
        Metrics::new(),
        CommitterConfig {
            quotas: Arc::new(quota::UsageCache::new()),
            audit_log_enabled: false,
            webhooks_enabled: false,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            quota_cache_ttl_secs: 60,
            idle_reclaim_secs: 0,
            instance_id: String::new(),
            multi_instance: false,
            forwarder: None,
        },
    );

    const EVERY_MS: i64 = 400;
    // Due in the past so the first fire is immediate (catch-up path); every
    // later fire re-arms one interval out from its fire time.
    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "interval", 1, &txn, None, Some(EVERY_MS))
        .await
        .unwrap();

    warm_up_committer(&committers, &db).await;

    // Fires repeatedly: fired_count reaches >= 2 (two fires one interval
    // apart) and the row stays pending — the recurring shape.
    let info = poll_list(&pool, &db, Duration::from_secs(15), |l| {
        l.iter()
            .find(|i| {
                i.id == id
                    && i.kind == ScheduleKind::Interval
                    && i.status == ScheduleStatus::Pending
                    && i.fired_count >= 2
            })
            .cloned()
    })
    .await
    .expect("interval job should fire repeatedly and stay pending");
    assert_eq!(info.every_ms, Some(EVERY_MS));

    // Pause. set_paused only transitions pending→paused, so a fire claimed
    // just before the call makes it return false (the row is 'running') —
    // retry until it lands. Once it returns true the row is quiescent: any
    // earlier claim would have made the UPDATE miss, and later claims only
    // take pending rows.
    let pause_deadline = Instant::now() + Duration::from_secs(8);
    let mut paused_landed = false;
    while Instant::now() < pause_deadline {
        if scheduler::set_paused(&pool, &db, &id, true).await.unwrap() {
            paused_landed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(paused_landed, "failed to pause the interval job");
    let f1 = scheduler::list(&pool, &db)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.id == id)
        .map(|i| i.fired_count)
        .expect("paused row must still be listed");

    // Let >= 3 windows elapse while paused — no fires.
    tokio::time::sleep(Duration::from_millis(1_400)).await;
    let f_still = scheduler::list(&pool, &db)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(
        f_still.fired_count, f1,
        "a paused interval job must not fire"
    );
    assert_eq!(f_still.status, ScheduleStatus::Paused);

    // Resume: the next fire shifts one full interval from the resume instant
    // — it must NOT be immediately due (that would be backfill).
    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let resumed_at = rtdb_server::db::now_ms();
    let info = scheduler::list(&pool, &db)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.id == id)
        .unwrap();
    assert_eq!(info.status, ScheduleStatus::Pending);
    assert!(
        info.due_at >= resumed_at + EVERY_MS - 100,
        "resume must shift due_at one interval out (>= resume + {}ms), got due_at {} vs resumed_at {}",
        EVERY_MS - 100,
        info.due_at,
        resumed_at
    );

    // And it fires exactly once more from the shifted due — observed as
    // fired_count reaching f1 + 1 (no burst of missed windows).
    let f2 = poll_list(&pool, &db, Duration::from_secs(10), |l| {
        l.iter()
            .find(|i| i.id == id && i.fired_count > f1)
            .map(|i| i.fired_count)
    })
    .await;
    assert!(f2.is_some(), "interval job should fire again after resume");
}
