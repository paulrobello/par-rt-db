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
use common::{fresh_db, kanban_schema_json, test_hot, test_state, test_state_with_audit};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::Committers;
use rtdb_server::db::{SchemaCache, now_ms};
use rtdb_server::error::ErrorCode;
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::protocol::{
    OutcomeStatus, ScheduleWhen, WorkflowInfo, WorkflowInfoFull, WorkflowSpec, WorkflowStatus,
};
use rtdb_server::quota;
use rtdb_server::scheduler;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::SubscriptionManager;
use rtdb_server::txn::{Step, Transaction, execute_txn};
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

// --- Task 4: `Step::StartWorkflow` / `Step::CancelWorkflow` txn steps -------
// The harness mirrors `tests/schedule_step_test.rs`: `fresh_db` pushes the
// kanban fixture, each test drives `execute_txn` directly with
// `PrincipalCtx::bypass()`, and `ensure_table` stands in for the scheduler
// startup's lazy ensure (no scheduler runs in these tests unless spawned).

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).expect("parse kanban schema")
}

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

fn valid_project_doc() -> serde_json::Map<String, serde_json::Value> {
    doc(serde_json::json!({
        "name": "Alpha",
        "description": null,
        "status": "active",
        "tags": ["a", "b"],
        "updatedAt": 1.0
    }))
}

/// A kanban-valid `workItems` doc. `projectId` is typed `id` (32-char hex);
/// the type check is format-only (no cross-table existence check), so a fixed
/// placeholder keeps the helper independent of any inserted project.
fn valid_work_item_doc(title: &str) -> serde_json::Map<String, serde_json::Value> {
    doc(serde_json::json!({
        "projectId": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
        "title": title,
        "status": "backlog",
        "order": 1.0,
        "completedAt": null
    }))
}

/// (5) The StartWorkflow step commits atomically with the enclosing txn's
/// writes, and a failing later step rolls the run row back with the txn —
/// no orphan run survives.
#[tokio::test]
async fn start_workflow_step_is_atomic_with_writes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();
    workflows::ensure_table(&pool, &db).await?;

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "projects".to_string(),
                    doc: valid_project_doc(),
                },
                Step::StartWorkflow {
                    spec: Box::new(one_step_spec("from-step")),
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert!(
        outcome.results[1]["workflowId"].as_str().is_some(),
        "startWorkflow result must carry a workflowId"
    );

    let listed = workflows::list(&pool, &db, None, 10).await?;
    assert_eq!(listed.len(), 1, "exactly one run row");

    // Rollback: a failing later step removes the run row too.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::StartWorkflow {
                    spec: Box::new(one_step_spec("rolled-back")),
                },
                Step::ExpectVersion {
                    table: "projects".to_string(),
                    id: "missing".to_string(),
                    version: 9,
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("ExpectVersion on a missing doc must fail");
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(
        workflows::list(&pool, &db, None, 10).await?.len(),
        1,
        "rolled-back start must leave no orphan run row"
    );

    Ok(())
}

/// (6) CancelWorkflow step result shape + idempotence: first cancel reports
/// true, a repeat reports false (already terminal).
#[tokio::test]
async fn cancel_workflow_step_result_shape() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();
    workflows::ensure_table(&pool, &db).await?;
    let id = workflows::insert(&pool, &db, &one_step_spec("cancelme")).await?;

    let cancel = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::CancelWorkflow { id: id.clone() }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(
        cancel.results,
        vec![serde_json::json!({ "cancelled": true })]
    );

    // Cancelling again is a no-op, not an error.
    let again = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::CancelWorkflow { id }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(
        again.results,
        vec![serde_json::json!({ "cancelled": false })]
    );

    Ok(())
}

/// (7) Submit-time validation and recursive table scoping: an empty spec is
/// `BadRequest` before anything is written; a scoped machine token cannot
/// smuggle a future write into a forbidden table via a workflow step that
/// fires later as bypass — directly, or nested inside a `Schedule` payload.
#[tokio::test]
async fn spec_bounds_and_allowlist_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();
    workflows::ensure_table(&pool, &db).await?;

    let mut empty = one_step_spec("x");
    empty.steps.clear();
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::StartWorkflow {
                spec: Box::new(empty),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("empty workflow spec must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        workflows::list(&pool, &db, None, 10).await?.is_empty(),
        "no run row may survive a rejected spec"
    );

    let scoped = PrincipalCtx {
        user_id: None,
        email: None,
        tables: Some(vec!["projects".to_string()]),
    };

    // The smuggle attempt: a spec whose step txn writes the forbidden table.
    let mut smuggle = one_step_spec("scoped");
    smuggle.steps[0].txn.steps = vec![Step::Insert {
        table: "workItems".to_string(),
        doc: valid_work_item_doc("smuggled"),
    }];
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::StartWorkflow {
                spec: Box::new(smuggle),
            }],
        },
        &scoped,
    )
    .await
    .expect_err("scoped token must not start a workflow writing workItems");
    assert_eq!(err.code, ErrorCode::Forbidden);
    assert!(
        workflows::list(&pool, &db, None, 10).await?.is_empty(),
        "no run row may survive the Forbidden"
    );

    // The Schedule-nesting bypass: every top-level step is control flow (no
    // table of its own), but the scheduled txn starts a workflow writing the
    // forbidden table — the `authorize_txn_tables` → `authorize_spec_tables`
    // recursion blocks it.
    let mut nested = one_step_spec("nested");
    nested.steps[0].txn.steps = vec![Step::Insert {
        table: "workItems".to_string(),
        doc: valid_work_item_doc("nested"),
    }];
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Schedule {
                when: ScheduleWhen::RunAt {
                    ms: now_ms() + 600_000,
                },
                txn: Box::new(Transaction {
                    steps: vec![Step::StartWorkflow {
                        spec: Box::new(nested),
                    }],
                }),
            }],
        },
        &scoped,
    )
    .await
    .expect_err("scoped token must not smuggle a workflow via Schedule");
    assert_eq!(err.code, ErrorCode::Forbidden);
    assert!(
        scheduler::list(&pool, &db).await?.is_empty(),
        "no job row may survive the Forbidden"
    );

    Ok(())
}

/// (8) Op-feed tap coverage (spec §Testing item 11): a workflow started via
/// the txn step publishes each step's writes through the committer's tap
/// sites — one `rtdb.audit_log` row per step write with `source = 'workflow'`
/// and no principal (steps fire as the system bypass principal). Uses
/// `test_state_with_audit` so `state.realtime.committers` carries
/// `audit_log_enabled = true` without touching env vars.
#[tokio::test]
async fn workflow_step_writes_publish_to_audit_tap() -> anyhow::Result<()> {
    let state = test_state_with_audit().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;

    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "audited",
        "steps": [insert_step("A0"), insert_step("A1")]
    }))
    .expect("parse audited workflow spec");
    let outcome = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::StartWorkflow {
                    spec: Box::new(spec),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;
    let id = outcome.results[0]["workflowId"]
        .as_str()
        .expect("workflowId string")
        .to_string();

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.step_outcomes.len(), 2);

    // The tap writes land in the same committer turn AFTER each step txn
    // commits, each on its own await — poll for the audit rows before
    // asserting their content (ttl_test.rs's pattern).
    let mut count: i64 = 0;
    for _ in 0..100 {
        count = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1 AND source = 'workflow'",
        )
        .bind(db.as_str())
        .fetch_one(&pool)
        .await?;
        if count == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(count, 2, "one audit row per workflow step write");

    // owner = None in the tap payload ⇒ `principal` is NULL on every row.
    let principals: Vec<(Option<String>,)> = sqlx::query_as(
        "SELECT principal FROM rtdb.audit_log WHERE db = $1 AND source = 'workflow'",
    )
    .bind(db.as_str())
    .fetch_all(&pool)
    .await?;
    assert_eq!(principals.len(), 2);
    assert!(
        principals.iter().all(|(p,)| p.is_none()),
        "workflow step audit rows carry no principal"
    );

    Ok(())
}
