//! Regression for the database-deletion lifecycle fix: a per-db scheduler whose
//! database is deleted out from under it must detect the missing schema on its
//! next poll and exit cleanly, rather than logging an error every `MAX_SLEEP`
//! (2s) forever.

use std::time::Duration;

use rtdb_server::{committer::CommitterRequest, db, scheduler, txn::Transaction};

#[tokio::test]
async fn scheduler_exits_when_its_database_is_deleted() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db).await?;

    // Insert a far-future job so the scheduler uses the 2s poll cadence
    // (MAX_SLEEP) instead of the 60s idle cadence (IDLE_SLEEP, ARC-102).
    // Without a pending job, next_due returns None and the scheduler idles at
    // 60s — too slow for this test's deletion-detection window. A far-future
    // due_at keeps the scheduler polling at 2s so the deletion Err surfaces
    // within the test timeout.
    let far_future = i64::MAX - 1_000_000;
    let txn = Transaction { steps: vec![] };
    scheduler::insert(
        &pool, &db, "oneshot", far_future, &txn, None, None, None, false, None,
    )
    .await?;

    // No consumer is needed: the scheduler only sends on this channel when it
    // claims due rows, and the far-future job never becomes due, so it never
    // blocks.
    let (tx, _rx) = tokio::sync::mpsc::channel::<CommitterRequest>(8);
    let handle = tokio::spawn(scheduler::run_scheduler(pool.clone(), db.clone(), tx));

    // Let it run startup + one poll cycle (MAX_SLEEP = 2s), then delete its db.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    db::drop_database(&pool, &db).await?;

    // The next poll hits the now-missing schema; the existence check must make
    // the scheduler exit instead of erroring every 2s forever.
    match tokio::time::timeout(Duration::from_secs(6), handle).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(anyhow::anyhow!("scheduler task failed: {e}")),
        Err(_) => Err(anyhow::anyhow!(
            "scheduler did not exit after its database was deleted (would log every 2s)"
        )),
    }
}

/// Regression for the 2026-11-12 prod livelock (card 01a1d0899): request-path
/// cold guards must not re-run the eleven-statement exclusive-lock DDL on
/// every call. External claim pollers hit them every poll; at prod cadence the
/// churn starved per-db scheduler queries for up to 604s. The cached variant
/// memoizes per process — a second call runs no DDL — while the uncached boot
/// path still recreates a table dropped behind the memo (the self-heal a
/// scheduler respawn relies on).
#[tokio::test]
async fn cold_guard_ensure_memoizes_per_process() -> anyhow::Result<()> {
    use rtdb_server::ddl::pg_schema;
    use sqlx::Row;

    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db).await?;

    scheduler::ensure_table_cached(&pool, &db).await?;
    // Drop the table BEHIND the memo. The cached call must stay a no-op (no
    // DDL, table stays dropped); the uncached boot-path ensure recreates it.
    let schema = pg_schema(&db);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE \"{schema}\".scheduled_txns"
    )))
    .execute(&pool)
    .await?;
    scheduler::ensure_table_cached(&pool, &db).await?;
    let missing = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT to_regclass('\"{schema}\".scheduled_txns') IS NULL AS absent"
    )))
    .fetch_one(&pool)
    .await?
    .get::<bool, _>("absent");
    assert!(missing, "cached ensure must not have recreated the table");

    scheduler::ensure_table(&pool, &db).await?;
    let present = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT to_regclass('\"{schema}\".scheduled_txns') IS NOT NULL AS present"
    )))
    .fetch_one(&pool)
    .await?
    .get::<bool, _>("present");
    assert!(present, "uncached boot-path ensure must self-heal");
    Ok(())
}
