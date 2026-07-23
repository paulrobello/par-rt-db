//! Integration tests for the `scheduled_txns` side table and its accessors
//! (Task 2 of the scheduled/cron transactions feature). The harness mirrors
//! `tests/txn_test.rs`: the shared `common` module bootstraps `rtdb_auth` and
//! hands out a pool; each test creates a uniquely-named `t<uuid>` database via
//! `db::create_database` (which now also creates the `scheduled_txns` table).
//! `ensure_table` is still called per test to exercise the idempotent
//! pre-feature migration path.

mod common;

use sqlx::PgPool;

use rtdb_server::scheduler;
use rtdb_server::txn::Transaction;

/// Mirrors `common::test_state()`'s pool setup: connect to the shared dev
/// Postgres and bootstrap `rtdb_auth`. Each test gets its own connection so
/// they don't share a `PgPool`'s lifetime.
async fn test_pool() -> PgPool {
    let state = common::test_state().await;
    state.pool.clone()
}

/// Mirrors `common::fresh_db()`'s naming + creation path, minus the schema
/// push (these tests never query user tables, so no schema is needed).
async fn unique_db(pool: &PgPool) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    name
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
    let id = scheduler::insert(&pool, &db, "oneshot", 123, &txn, None)
        .await
        .unwrap();
    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].kind, "oneshot");
    assert_eq!(listed[0].status, "pending");
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
    let id = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"))
        .await
        .unwrap();

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, "paused");
    // A paused job must not be claimed even if due_at is in the past.
    let claimed = scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    assert!(claimed.is_empty());

    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, "pending");
    assert!(info.due_at > 1); // recomputed forward from now
}

#[tokio::test]
async fn claim_due_and_finalize() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let one = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None)
        .await
        .unwrap();
    let cron = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"))
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
    scheduler::finalize_cron_next(&pool, &db, &cron, next)
        .await
        .unwrap();

    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1); // one-shot deleted, cron remains
    assert_eq!(listed[0].id, cron);
    assert_eq!(listed[0].fired_count, 1);
    assert_eq!(listed[0].status, "pending");
}

#[tokio::test]
async fn reset_running_recovers_orphans() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let _id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None)
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
        "pending"
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

    let _a = scheduler::insert(&pool, &db, "oneshot", 50, &txn, None)
        .await
        .unwrap();
    let b = scheduler::insert(&pool, &db, "oneshot", 10, &txn, None)
        .await
        .unwrap();
    let _c = scheduler::insert(&pool, &db, "oneshot", 90, &txn, None)
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
    assert_eq!(info.status, "error");
    assert_eq!(info.last_error.as_deref(), Some("boom"));
    assert_eq!(scheduler::next_due(&pool, &db).await.unwrap(), Some(50));
}

#[tokio::test]
async fn pause_resume_one_shot_keeps_due_at() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = empty_txn();
    let id = scheduler::insert(&pool, &db, "oneshot", 42, &txn, None)
        .await
        .unwrap();

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, "pending");
    assert_eq!(info.due_at, 42); // unchanged by resume

    // Resuming a non-paused job is a no-op (returns false).
    assert!(!scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
}
