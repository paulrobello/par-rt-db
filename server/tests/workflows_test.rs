//! Integration tests for durable workflows (FM-29). Task 2 covers the
//! side-table ops (insert/claim/reset/list/cancel/get/delete); Task 3 adds
//! the advancement-engine tests (real Committers so the per-db scheduler
//! claims rows and the committer's `RunWorkflowAdvance` arm advances them —
//! the harness pattern of `schedule_step_test.rs::
//! chained_schedule_fires_and_enqueues_follow_up`); Tasks 4–5 grow this file
//! with step-surface coverage.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use common::{fresh_db, test_hot, test_state};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::Committers;
use rtdb_server::db::{SchemaCache, now_ms};
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::protocol::{
    OutcomeStatus, WorkflowInfo, WorkflowInfoFull, WorkflowSpec, WorkflowStatus,
};
use rtdb_server::quota;
use rtdb_server::subs::SubscriptionManager;
use rtdb_server::txn::Transaction;
use rtdb_server::workflows;

fn one_step_spec(name: &str) -> WorkflowSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "steps": [ { "txn": { "steps": [ { "op": "insert", "table": "projects",
            "doc": { "name": "W", "description": null, "status": "active",
                     "tags": [], "updatedAt": 1.0 } } ] } } ]
    }))
    .expect("parse one-step workflow spec")
}

#[tokio::test]
async fn insert_claim_reset_roundtrip() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let id = workflows::insert(&pool, &db, &one_step_spec("rt"))
        .await
        .unwrap();
    let due = workflows::next_due(&pool, &db).await.unwrap();
    assert!(due.is_some());
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].status, WorkflowStatus::Running);
    // Nothing further claims while running:
    assert!(
        workflows::claim_due(&pool, &db, now_ms() + 10_000, 10)
            .await
            .unwrap()
            .is_empty()
    );
    // Crash recovery path:
    assert_eq!(workflows::reset_running(&pool, &db).await.unwrap(), 1);
    // list/get/cancel/delete shape:
    let listed = workflows::list(&pool, &db, None, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, WorkflowStatus::Pending);
    assert!(workflows::cancel(&pool, &db, &id).await.unwrap());
    let full = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Cancelled);
    assert!(full.step_outcomes.is_empty());
    assert!(workflows::delete(&pool, &db, &id).await.unwrap());
}

// --- Task 3: advancement engine (committer arm + scheduler dual poll) ------

/// A workflow step whose txn inserts one kanban-valid `projects` doc named
/// `name` — the durable side effect each engine test observes.
fn insert_step(name: &str) -> serde_json::Value {
    serde_json::json!({ "txn": { "steps": [ { "op": "insert", "table": "projects",
        "doc": { "name": name, "description": null, "status": "active",
                 "tags": [], "updatedAt": 1.0 } } ] } })
}

/// Mirrors `schedule_step_test.rs::make_committers`: a real `Committers` so
/// `channel_for` spawns the per-db committer + scheduler tasks.
async fn make_committers(state: &Arc<AppState>) -> Arc<Committers> {
    Arc::new(Committers::new(
        state.pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(test_hot())),
        false,
        false,
        60,
        5000,
        Metrics::new(),
        Arc::new(quota::UsageCache::new()),
        60,
        0,
        String::new(),
        false,
    ))
}

/// Triggers lazy spawn of `db`'s committer + scheduler tasks by submitting a
/// no-op mutate (`schedule_step_test.rs::warm_up`).
async fn warm_up(committers: &Arc<Committers>, db: &str) {
    committers
        .mutate(
            db,
            None,
            Transaction { steps: vec![] },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("warm up committer");
}

/// Polls the run until `pred(&info)` holds or ~10s elapse (the scheduler's
/// wake is ≤2s per gate). Panics on timeout with the last-known status.
async fn await_status(
    pool: &sqlx::PgPool,
    db: &str,
    id: &str,
    pred: impl Fn(&WorkflowInfo) -> bool,
) -> WorkflowInfoFull {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(Some(full)) = workflows::get(pool, db, id).await
            && pred(&full.info)
        {
            return full;
        }
        assert!(
            Instant::now() < deadline,
            "workflow never reached expected status (still not met after 10s)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Row count of `db`'s `projects` table — the durable side effect of every
/// step in these tests. Uses `ddl::pg_table` for the physical table name.
async fn projects_count(pool: &sqlx::PgPool, db: &str) -> i64 {
    let table = rtdb_server::ddl::pg_table("projects");
    sqlx::query_scalar::<_, i64>(&format!("SELECT COUNT(*) FROM \"db_{db}\".\"{table}\""))
        .fetch_one(pool)
        .await
        .expect("count projects")
}

/// (1) Happy path: a 3-step no-sleep chain advances to `success` in one
/// committer turn, appends one successful outcome per step, and leaves all
/// three docs durable.
#[tokio::test]
async fn three_step_workflow_advances_to_success() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;

    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "chain",
        "steps": [insert_step("S0"), insert_step("S1"), insert_step("S2")]
    }))
    .expect("parse 3-step workflow spec");
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.info.step_count, 3);
    assert_eq!(full.step_outcomes.len(), 3);
    assert!(
        full.step_outcomes
            .iter()
            .all(|o| o.status == OutcomeStatus::Success)
    );
    assert_eq!(projects_count(&pool, &db).await, 3);
}

/// (2) Exhausted retries: an always-failing step (`expectVersion` on a row
/// that never exists) with `maxAttempts: 2` ends `failed` with the full
/// attempt trail, and no later step ever runs.
#[tokio::test]
async fn exhausted_retries_mark_failed_with_trail() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;

    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "doomed",
        "steps": [
            { "txn": { "steps": [ { "op": "expectVersion", "table": "projects",
                "id": "nope", "version": 7 } ] },
              "retry": { "maxAttempts": 2, "initialRetryMs": 50, "maxRetryMs": 100 } },
            insert_step("never")
        ]
    }))
    .expect("parse doomed workflow spec");
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Failed).await;
    assert_eq!(full.info.current_step, 0);
    assert_eq!(full.info.attempts, 2);
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(full.step_outcomes[0].status, OutcomeStatus::Failed);
    assert_eq!(full.step_outcomes[0].attempts, 2);
    assert!(full.info.last_error.is_some());
    // Step 1 never ran:
    assert_eq!(projects_count(&pool, &db).await, 0);
}

/// (3) Sleep gate: step 1's `sleepBeforeMs` parks the run as `pending` with a
/// future gate — step 0's doc is durable while step 1's is not yet — before
/// the run resumes and completes.
#[tokio::test]
async fn sleep_before_ms_gates_next_step() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;

    let gated: serde_json::Value = {
        let mut s = insert_step("G1");
        s["sleepBeforeMs"] = serde_json::json!(1500);
        s
    };
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "gated", "steps": [insert_step("G0"), gated]
    }))
    .expect("parse gated workflow spec");
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();

    // Wait for step 0's doc, then observe the gate still closed: only one
    // doc, run back to `pending` with a future `sleepUntil`. (300ms is safely
    // inside the 1500ms gate from the moment step 0's doc becomes visible.)
    let deadline = Instant::now() + Duration::from_secs(5);
    while projects_count(&pool, &db).await < 1 {
        assert!(Instant::now() < deadline, "step 0 never executed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        projects_count(&pool, &db).await,
        1,
        "step 1 must not run early"
    );
    let mid = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(mid.info.status, WorkflowStatus::Pending);
    assert!(mid.info.sleep_until.unwrap() > now_ms());
    assert_eq!(mid.info.current_step, 1);

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.step_outcomes.len(), 2);
    assert_eq!(projects_count(&pool, &db).await, 2);
}

/// (4) Crash resume (at-least-once): a row orphaned as `running` — the state
/// a crashed advance leaves behind — is recovered by the scheduler startup's
/// `reset_running` and re-advanced to `success` once the tasks spawn.
#[tokio::test]
async fn orphaned_running_row_resumes_to_success() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();

    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "resume", "steps": [insert_step("R")]
    }))
    .expect("parse resume workflow spec");
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();

    // Simulate the crash: claim the row (→ `running`) but never advance it.
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    let mid = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(mid.info.status, WorkflowStatus::Running);

    // Spawning the per-db tasks AFTER the orphan exists exercises the real
    // recovery path: scheduler startup's `reset_running` returns it to
    // `pending`, the loop re-claims it, and the committer advances it.
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(projects_count(&pool, &db).await, 1);
}
