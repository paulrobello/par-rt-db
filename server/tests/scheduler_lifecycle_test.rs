//! Regression for the database-deletion lifecycle fix: a per-db scheduler whose
//! database is deleted out from under it must detect the missing schema on its
//! next poll and exit cleanly, rather than logging an error every `MAX_SLEEP`
//! (2s) forever.

mod common;

use std::time::Duration;

use rtdb_server::{committer::CommitterRequest, db, scheduler, txn::Transaction};

#[tokio::test]
async fn scheduler_exits_when_its_database_is_deleted() -> anyhow::Result<()> {
    let state = common::test_state().await;
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
    scheduler::insert(&pool, &db, "oneshot", far_future, &txn, None).await?;

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
