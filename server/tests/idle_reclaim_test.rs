//! Idle-database reclamation (ARC-102 step 4).
//!
//! A database whose committer was spawned is retired by `reclaim_idle_once`
//! once it has had no client activity for `RTDB_DB_IDLE_RECLAIM_SECS` AND has no
//! live subscriptions AND no pending scheduled jobs. These cover the three
//! gates plus the respawn contract:
//!
//! 1. a genuinely-idle db is reclaimed (its committer exits) and the next
//!    request respawns a fresh committer cleanly;
//! 2. a db with a live subscription is NOT reclaimed (the audit's load-bearing
//!    guard — a db with listeners is never idle);
//! 3. a db with a pending (future-due) scheduled job is NOT reclaimed (the cron
//!    / one-shot still needs to fire).
//!
//! The sweep is driven directly via `Committers::reclaim_idle_once` for
//! determinism; the background sweep loop spawned by `test_state_with_idle_reclaim`
//! is also running but is a no-op for the protected cases and reaches the same
//! outcome for the idle case, so the assertions are robust either way.

use std::time::{Duration, Instant};

use crate::common::{fresh_db, test_state_with_idle_reclaim};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::now_ms;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::Query;
use rtdb_server::scheduler;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction};

/// `RTDB_DB_IDLE_RECLAIM_SECS` for these tests: 1s so a db goes idle inside the
/// test window. `idle()` sleeps just past this before driving `reclaim_idle_once`.
const IDLE_SECS: u64 = 1;

fn insert_work_item() -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: serde_json::json!({
                "projectId": "0123456789abcdef0123456789abcdef",
                "title": "item",
                "status": "backlog",
                "order": 1.0,
                "completedAt": null,
            })
            .as_object()
            .expect("json object")
            .clone(),
        }],
    }
}

fn count_work_items() -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": ["backlog"],
        "count": true
    }))
    .expect("parse query")
}

/// Sleeps just past the reclaim threshold so a db last touched before this is
/// stale from the sweep's point of view.
async fn idle() {
    tokio::time::sleep(Duration::from_secs(IDLE_SECS) + Duration::from_millis(500)).await;
}

/// Polls until `db`'s committer is no longer spawned (the draining task has
/// exited and the supervisor cleared its entry). The drain is fast for a
/// genuinely-idle db (no queued work), so this resolves well inside `timeout`.
async fn await_not_spawned(state: &rtdb_server::AppState, db: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if !state.realtime.committers.is_spawned(db).await {
            return;
        }
        if Instant::now() >= deadline {
            panic!("db {db} still spawned after {:?}", timeout);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A genuinely-idle db (no subs, no pending jobs) is reclaimed, and the next
/// request respawns its committer cleanly — reclamation loses nothing.
#[tokio::test]
async fn idle_db_is_reclaimed_and_respawns() -> anyhow::Result<()> {
    let state = test_state_with_idle_reclaim(IDLE_SECS).await;
    let db = fresh_db(&state).await;

    // A mutate spawns the committer (+ the four pollers).
    state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item(), PrincipalCtx::bypass())
        .await?;
    assert!(
        state.realtime.committers.is_spawned(&db).await,
        "db is spawned after a mutate"
    );

    idle().await;
    // Drive one sweep pass directly. The background loop may also have run;
    // either way the outcome is the same, so assert the outcome, not the count.
    let _ = state.realtime.committers.reclaim_idle_once().await;
    await_not_spawned(&state, &db, Duration::from_secs(2)).await;

    // The next request respawns the committer — channel_for waits out the drain
    // (if still in progress) then spawns a fresh task, so the single-writer
    // invariant holds and the write succeeds.
    state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item(), PrincipalCtx::bypass())
        .await?;
    assert!(
        state.realtime.committers.is_spawned(&db).await,
        "db respawns on the next request"
    );

    Ok(())
}

/// A db with a live subscription is NOT reclaimed regardless of idle time — the
/// audit's load-bearing guard (a db with listeners is never idle).
#[tokio::test]
async fn live_subscription_protects_db() -> anyhow::Result<()> {
    let state = test_state_with_idle_reclaim(IDLE_SECS).await;
    let db = fresh_db(&state).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item(), PrincipalCtx::bypass())
        .await?;
    // Register a live subscription. The receiver is held for the test's scope so
    // the sub stays registered (count_for_db > 0).
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<ServerMessage>();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "q".to_string(),
            count_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;

    idle().await;
    let reclaimed = state.realtime.committers.reclaim_idle_once().await;
    assert_eq!(
        reclaimed, 0,
        "a db with a live subscription must not be reclaimed"
    );
    assert!(
        state.realtime.committers.is_spawned(&db).await,
        "sub-protected db stays spawned"
    );

    Ok(())
}

/// A db with a pending (future-due) scheduled job is NOT reclaimed — the cron /
/// one-shot still needs to fire, and reclaiming would stall it until the next
/// client request.
#[tokio::test]
async fn pending_scheduled_job_protects_db() -> anyhow::Result<()> {
    let state = test_state_with_idle_reclaim(IDLE_SECS).await;
    let db = fresh_db(&state).await;

    state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item(), PrincipalCtx::bypass())
        .await?;
    // Ensure the scheduled_txns side table exists (the scheduler task ensures it
    // too, but do it explicitly to avoid a startup race), then insert a
    // future-due one-shot so next_due reports pending work.
    scheduler::ensure_table(&state.pool, &db).await?;
    scheduler::insert(
        &state.pool,
        &db,
        "oneshot",
        now_ms() + 3_600_000,
        &insert_work_item(),
        None,
        None,
    )
    .await?;

    idle().await;
    let reclaimed = state.realtime.committers.reclaim_idle_once().await;
    assert_eq!(
        reclaimed, 0,
        "a db with a pending scheduled job must not be reclaimed"
    );
    assert!(
        state.realtime.committers.is_spawned(&db).await,
        "job-protected db stays spawned"
    );

    Ok(())
}
