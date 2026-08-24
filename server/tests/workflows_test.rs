//! Integration tests for durable workflows (FM-29). Task 2 covers the
//! side-table ops (insert/claim/reset/list/cancel/get/delete); Task 3 adds
//! the advancement-engine tests (real Committers so the per-db scheduler
//! claims rows and the committer's `RunWorkflowAdvance` arm advances them —
//! the harness pattern of `schedule_step_test.rs::
//! chained_schedule_fires_and_enqueues_follow_up`); Tasks 4–5 grow this file
//! with step-surface coverage.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::{
    admin_delete, admin_get, admin_post, fresh_db, kanban_schema_json, spawn_app, test_hot,
    test_state, test_state_with_audit,
};
use arc_swap::ArcSwap;
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::{CommitterConfig, Committers};
use rtdb_server::db::{SchemaCache, now_ms};
use rtdb_server::error::ErrorCode;
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::protocol::{
    OutcomeStatus, ScheduleWhen, StepOutcome, WorkflowInfo, WorkflowInfoFull, WorkflowSpec,
    WorkflowStatus,
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

/// Task 2: the awaitSignal side-table lifecycle — park (visibility columns +
/// gate), deliver (latest-wins slot write + wake flip), consume (atomic step
/// boundary), and the typed delivery classification. No committer needed.
#[tokio::test]
async fn await_signal_side_table_lifecycle() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "gate", "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 50 } } ]
    }))
    .unwrap();
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();
    // Only the advance arm parks, on a row it holds `running` — claim first
    // (the `status = 'running'` guard on `park_waiting`).
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    // Park: waiting + visibility columns; not claimable before the gate.
    workflows::park_waiting(&pool, &db, &id, 0, "approve", now_ms() + 60_000)
        .await
        .unwrap();
    let full = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Waiting);
    assert_eq!(full.info.waiting_for.as_deref(), Some("approve"));
    assert!(full.info.waited_since.is_some());
    assert!(
        workflows::claim_due(&pool, &db, now_ms(), 10)
            .await
            .unwrap()
            .is_empty()
    );
    // next_due sees the waiting gate:
    assert!(workflows::next_due(&pool, &db).await.unwrap().is_some());

    // Delivery: latest-wins + wake flip.
    let d1 = workflows::deliver_signal(&pool, &db, &id, "wrong", None)
        .await
        .unwrap();
    assert!(matches!(d1, workflows::SignalDelivery::NameMismatch { .. }));
    let d2 = workflows::deliver_signal(
        &pool,
        &db,
        &id,
        "approve",
        Some(serde_json::json!({"v": 1})),
    )
    .await
    .unwrap();
    assert!(matches!(d2, workflows::SignalDelivery::Delivered));
    let d3 = workflows::deliver_signal(
        &pool,
        &db,
        &id,
        "approve",
        Some(serde_json::json!({"v": 2})),
    )
    .await
    .unwrap();
    assert!(matches!(d3, workflows::SignalDelivery::Delivered));
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].signal_payload, Some(serde_json::json!({"v": 2})));

    // Consume + boundary clears the wait columns.
    let outcome = StepOutcome {
        step_index: 0,
        status: OutcomeStatus::Success,
        attempts: 1,
        at: now_ms(),
        error: None,
        signal: Some(serde_json::json!({"v": 2})),
    };
    workflows::record_signal_success(&pool, &db, &id, 1, &outcome)
        .await
        .unwrap();
    let full = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Running);
    assert!(full.info.waiting_for.is_none() && full.info.waited_since.is_none());
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(
        full.step_outcomes[0].signal,
        Some(serde_json::json!({"v": 2}))
    );

    // Typed classification against a fresh parked row + unknown id.
    let id2 = workflows::insert(&pool, &db, &spec).await.unwrap();
    assert!(matches!(
        workflows::deliver_signal(&pool, &db, "nope", "approve", None)
            .await
            .unwrap(),
        workflows::SignalDelivery::NotFound
    ));
    workflows::cancel(&pool, &db, &id2).await.unwrap();
    assert!(matches!(
        workflows::deliver_signal(&pool, &db, &id2, "approve", None)
            .await
            .unwrap(),
        workflows::SignalDelivery::NotWaiting
    ));
    let full2 = workflows::get(&pool, &db, &id2).await.unwrap().unwrap();
    assert!(
        full2.info.waiting_for.is_none(),
        "cancel clears wait columns"
    );
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
    smuggle.steps[0].txn = Some(Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: valid_work_item_doc("smuggled"),
        }],
    });
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
    nested.steps[0].txn = Some(Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: valid_work_item_doc("nested"),
        }],
    });
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

// --- Task 5: WS frames + HTTP one-shot start/cancel/list surfaces -----------
// The harness mirrors `schedule_step_test.rs`'s HTTP/WS sections: `fresh_db`
// pushes the kanban fixture, `ensure_table` stands in for the scheduler
// startup's lazy ensure. The start surfaces themselves now ensure the table
// and spawn the per-db tasks (FM-29 cold-db liveness), so these tests start
// gated specs (`gated_projects_spec_json`) to keep runs deterministically
// `pending` for assertions.

/// Raw bearer POST to a per-db API route (mirrors `schedule_step_test.rs::
/// api_post`).
async fn api_post(
    addr: std::net::SocketAddr,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("send api request")
}

/// Mints a machine token scoped to `tables` (pattern: `admin_test.rs`
/// mint-and-list capabilities test) and returns the raw bearer secret.
async fn mint_scoped_token(addr: std::net::SocketAddr, db: &str, tables: &[&str]) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({
            "db": db,
            "name": "scoped",
            "tables": tables,
            "readOnly": false,
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

/// A one-step projects-insert workflow spec as raw JSON (the kanban-valid
/// shape `one_step_spec` builds, for HTTP bodies and WS frames).
fn projects_spec_json(name: &str) -> serde_json::Value {
    serde_json::to_value(one_step_spec(name)).expect("serialize projects spec")
}

/// `projects_spec_json` with step 0 gated far into the future: the start
/// surfaces spawn the per-db tasks (FM-29 cold-db liveness), so an
/// immediately-due run would advance mid-test. The gate keeps these
/// surface round-trip tests on a deterministically-`pending` run;
/// advancement is covered by the engine tests and (14).
fn gated_projects_spec_json(name: &str) -> serde_json::Value {
    let mut spec = projects_spec_json(name);
    spec["steps"][0]["sleepBeforeMs"] = serde_json::json!(600_000);
    spec
}

/// The smuggle shape: a one-step spec whose txn inserts into `workItems`.
fn workitems_spec_json(name: &str) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "steps": [ { "txn": { "steps": [ { "op": "insert", "table": "workItems",
            "doc": serde_json::Value::Object(valid_work_item_doc("smuggled")) } ] } } ]
    })
}

/// (9) HTTP happy path: `POST /api/workflows` returns `{id}`;
/// `/api/workflows/list` round-trips the run (unfiltered and status-filtered);
/// `/api/workflows/{id}/cancel` flips it (`true` once, then `false`).
#[tokio::test]
async fn http_start_list_cancel_roundtrip() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let resp = api_post(
        addr,
        "/api/workflows",
        &token,
        serde_json::json!({ "db": db, "spec": gated_projects_spec_json("http-drip") }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("workflow id").to_string();
    assert!(
        body.get("spec").is_none(),
        "start reply carries only the id"
    );

    let listed = workflows::list(&pool, &db, None, 10).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    let resp = api_post(
        addr,
        "/api/workflows/list",
        &token,
        serde_json::json!({ "db": db }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["workflows"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["workflows"][0]["id"], serde_json::json!(id));
    assert_eq!(body["workflows"][0]["name"], serde_json::json!("http-drip"));
    assert_eq!(body["workflows"][0]["status"], serde_json::json!("pending"));
    assert_eq!(body["workflows"][0]["stepCount"], serde_json::json!(1));

    // Status filter: the pending run does not match `success`.
    let resp = api_post(
        addr,
        "/api/workflows/list",
        &token,
        serde_json::json!({ "db": db, "status": "success" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["workflows"].as_array().map(Vec::len), Some(0));

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/cancel"),
        &token,
        serde_json::json!({ "db": db }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "cancelled": true }));

    // Cancelling again: already terminal, still 200 with false.
    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/cancel"),
        &token,
        serde_json::json!({ "db": db }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "cancelled": false }));

    let listed = workflows::list(&pool, &db, None, 10).await?;
    assert_eq!(listed[0].status, WorkflowStatus::Cancelled);

    Ok(())
}

/// (10) HTTP submit-time gates: an empty spec is `BadRequest` and a scoped
/// token cannot start a workflow writing a forbidden table — both before any
/// run row is written.
#[tokio::test]
async fn http_start_rejects_bad_spec_and_forbidden_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let empty = serde_json::json!({ "name": "empty", "steps": [] });
    let resp = api_post(
        addr,
        "/api/workflows",
        &token,
        serde_json::json!({ "db": db, "spec": empty }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    assert!(
        workflows::list(&pool, &db, None, 10).await?.is_empty(),
        "no run row may survive a rejected spec"
    );

    let resp = api_post(
        addr,
        "/api/workflows",
        &token,
        serde_json::json!({ "db": db, "spec": workitems_spec_json("smuggle") }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));
    assert!(
        workflows::list(&pool, &db, None, 10).await?.is_empty(),
        "no run row may survive the Forbidden"
    );

    Ok(())
}

// (11) WS frames: the `/sync` StartWorkflow/CancelWorkflow/ListWorkflows arms
// (the plumbing mirrors `schedule_step_test.rs`'s WS section).
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> WsStream {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn ws_send_json(ws: &mut WsStream, msg: serde_json::Value) {
    use futures_util::SinkExt as _;
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        msg.to_string().into(),
    ))
    .await
    .expect("send frame");
}

async fn ws_recv_json(ws: &mut WsStream) -> serde_json::Value {
    use futures_util::StreamExt as _;
    match ws.next().await.expect("stream ended").expect("frame ok") {
        tokio_tungstenite::tungstenite::Message::Text(text) => {
            serde_json::from_str(&text).expect("parse json")
        }
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn ws_auth(ws: &mut WsStream, token: &str, db: &str) -> serde_json::Value {
    ws_send_json(
        ws,
        serde_json::json!({"type": "auth", "token": token, "db": db}),
    )
    .await;
    ws_recv_json(ws).await
}

#[tokio::test]
async fn ws_start_list_cancel_roundtrip() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let mut ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ws, &token, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));

    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "startWorkflow", "workflowId": "c1",
            "spec": gated_projects_spec_json("ws-drip"),
        }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("startWorkflowOk"));
    assert_eq!(reply["workflowId"], serde_json::json!("c1"));
    let id = reply["info"]["id"].as_str().expect("info.id").to_string();
    assert_eq!(reply["info"]["name"], serde_json::json!("ws-drip"));
    assert_eq!(reply["info"]["status"], serde_json::json!("pending"));

    // A bad spec replies startWorkflowErr without dropping the connection.
    let empty = serde_json::json!({ "name": "empty", "steps": [] });
    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "startWorkflow", "workflowId": "c2", "spec": empty,
        }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("startWorkflowErr"));
    assert_eq!(reply["workflowId"], serde_json::json!("c2"));
    assert_eq!(reply["error"]["code"], serde_json::json!("BAD_REQUEST"));

    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "listWorkflows", "workflowId": "c3", "status": "pending",
        }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("listWorkflowsOk"));
    assert_eq!(reply["workflowId"], serde_json::json!("c3"));
    assert_eq!(reply["workflows"].as_array().map(Vec::len), Some(1));
    assert_eq!(reply["workflows"][0]["id"], serde_json::json!(id));

    ws_send_json(
        &mut ws,
        serde_json::json!({ "type": "cancelWorkflow", "workflowId": "c4", "id": id }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("workflowAck"));
    assert_eq!(reply["workflowId"], serde_json::json!("c4"));
    assert_eq!(reply["ok"], serde_json::json!(true));
    assert!(reply.get("error").is_none(), "clean ack omits error");

    ws_send_json(
        &mut ws,
        serde_json::json!({ "type": "cancelWorkflow", "workflowId": "c5", "id": id }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("workflowAck"));
    assert_eq!(reply["ok"], serde_json::json!(false));

    let listed = workflows::list(&pool, &db, None, 10).await?;
    assert_eq!(listed.len(), 1, "the rejected spec left exactly one run");
    assert_eq!(listed[0].status, WorkflowStatus::Cancelled);

    Ok(())
}

// --- Task 6: admin routes + step metrics -------------------------------------
// Same harness as the Task 5 HTTP section: `fresh_db` pushes the kanban
// fixture, `ensure_table` stands in for the scheduler startup's lazy ensure,
// and the started run is gated so it stays `pending` for assertions (the
// admin start surface spawns the per-db tasks — see (14) for that coverage).

/// (12) Admin round-trip: create → list (unfiltered + status-filtered, with
/// the bad-status `BadRequest`) → get (`WorkflowInfoFull`, 404 on unknown id)
/// → cancel (`ok` true once then false) → delete (`ok` true once then false),
/// plus the per-db status counts on the `/admin/metrics` JSON.
#[tokio::test]
async fn admin_routes_roundtrip() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let addr = spawn_app(state).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows"),
        gated_projects_spec_json("admin-run"),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("workflow id").to_string();

    let listed = workflows::list(&pool, &db, None, 10).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    // List: unfiltered shows the run; the status filter narrows it; a bad
    // status value is a `BadRequest` envelope, not a 500.
    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["workflows"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["workflows"][0]["id"], serde_json::json!(id));
    assert_eq!(body["workflows"][0]["name"], serde_json::json!("admin-run"));
    assert_eq!(body["workflows"][0]["status"], serde_json::json!("pending"));
    assert_eq!(body["workflows"][0]["stepCount"], serde_json::json!(1));

    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows?status=success")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["workflows"].as_array().map(Vec::len), Some(0));

    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows?status=bogus")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));

    // An invalid spec is rejected at the surface before any run row exists.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows"),
        serde_json::json!({ "name": "empty", "steps": [] }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        workflows::list(&pool, &db, None, 10).await?.len(),
        1,
        "no second run row may survive the rejected spec"
    );

    // Get: the full row (info flattened + stepOutcomes); unknown id 404s.
    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows/{id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["id"], serde_json::json!(id));
    assert_eq!(body["status"], serde_json::json!("pending"));
    assert_eq!(body["stepOutcomes"].as_array().map(Vec::len), Some(0));

    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows/nope")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));

    // Per-db status counts ride the /admin/metrics JSON (one pending run).
    let resp = admin_get(addr, "/admin/metrics").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let ours = body["perDbWorkflows"]
        .as_array()
        .expect("perDbWorkflows array")
        .iter()
        .find(|row| row["db"] == serde_json::json!(db))
        .cloned()
        .unwrap_or_else(|| panic!("no perDbWorkflows row for {db}"));
    assert_eq!(ours["pending"], serde_json::json!(1));

    // Cancel: true once, then false (already terminal).
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/{id}/cancel"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "ok": true }));

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/{id}/cancel"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "ok": false }));

    // Delete: hard-removes the row — true once, then false, list empty after.
    let resp = admin_delete(addr, &format!("/admin/db/{db}/workflows/{id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "ok": true }));

    let resp = admin_delete(addr, &format!("/admin/db/{db}/workflows/{id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "ok": false }));

    assert!(
        workflows::list(&pool, &db, None, 10).await?.is_empty(),
        "deleted run must not list"
    );

    Ok(())
}

// --- Cold-db liveness (FM-29 review) ------------------------------------------
// Unlike every test above, these deliberately skip `workflows::ensure_table`
// and `warm_up`/`mutate`: `fresh_db` creates document tables directly on the
// pool and spawns nothing, so the db is "cold" — no `workflows` table (its
// only ensure runs at per-db scheduler startup) and no committer/scheduler
// tasks. That is the state of an admin-created db before its first client op.

/// (13) The admin routes serve a cold db: list must not 500 on the missing
/// `workflows` table (the `admin_storage_list` ensure-inline precedent).
#[tokio::test]
async fn admin_list_serves_cold_db() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;

    let resp = admin_get(addr, &format!("/admin/db/{db}/workflows")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        body["workflows"].as_array().map(Vec::len),
        Some(0),
        "cold db lists zero runs, not an error"
    );

    Ok(())
}

/// (14) A run started via the admin surface on a cold db must advance: the
/// start surface has to spawn the per-db tasks (steps fire from the per-db
/// scheduler, which only exists after that spawn) or the row sits `pending`
/// forever with nothing to claim it.
#[tokio::test]
async fn admin_start_on_cold_db_advances_to_success() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows"),
        projects_spec_json("cold-start"),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("workflow id").to_string();

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(projects_count(&pool, &db).await, 1);

    Ok(())
}

/// (15) The client-facing list route serves a cold db (no workflows table, no
/// per-db tasks) — ensure-inline like the admin surface, not a 500.
#[tokio::test]
async fn http_list_serves_cold_db() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let resp = api_post(
        addr,
        "/api/workflows/list",
        &token,
        serde_json::json!({ "db": db }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        body["workflows"].as_array().map(Vec::len),
        Some(0),
        "cold db lists zero runs, not an error"
    );

    Ok(())
}

/// (16) A cold-db client start succeeds on the FIRST attempt: `ensure_spawned`
/// only queues the spawned scheduler's startup ensure, which is not ordered
/// against this request's own insert — the start arm must also ensure the
/// table inline or a cold-db insert can lose that race and error once. Pre-fix
/// this is a race, not a deterministic failure; this test pins the post-fix
/// first-try contract (the cold-list test (15) is the deterministic RED).
/// Advancement is asserted to prove the spawned tasks claim the run.
#[tokio::test]
async fn http_start_on_cold_db_first_try_advances() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let resp = api_post(
        addr,
        "/api/workflows",
        &token,
        serde_json::json!({ "db": db, "spec": projects_spec_json("cold-http") }),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "first attempt must succeed on a cold db"
    );
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("workflow id").to_string();

    let full = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(projects_count(&pool, &db).await, 1);

    Ok(())
}

// --- awaitSignal (approval gates): park / deliver / timeout integration ------
// Engine tests follow the make_committers pattern above (the per-db scheduler
// claims rows and the committer's three-way awaitSignal branch advances them);
// surface tests add spawn_app for the HTTP/WS/admin delivery routes. All
// polling goes through `await_status` — the scheduler's wake is ≤2 s per gate,
// so fixed sleeps would flake.

/// The approval-gate shape every awaitSignal test uses: insert "pre", wait for
/// `signal`, insert "post". `timeout_ms = None` parks with no gate (only a
/// delivery or cancel wakes the run). The retry backoff window is far below
/// every timeout used here, so a FRESH-timeout re-park is distinguishable from
/// a backoff re-park by the gate distance alone.
fn await_gate_spec(
    name: &str,
    signal: &str,
    timeout_ms: Option<u64>,
    max_attempts: u32,
) -> WorkflowSpec {
    let mut gate = serde_json::json!({
        "awaitSignal": { "name": signal },
        "retry": { "maxAttempts": max_attempts, "initialRetryMs": 10, "maxRetryMs": 50 }
    });
    if let Some(ms) = timeout_ms {
        gate["awaitSignal"]["timeoutMs"] = serde_json::json!(ms);
    }
    serde_json::from_value(serde_json::json!({
        "name": name,
        "steps": [insert_step("pre"), gate, insert_step("post")]
    }))
    .expect("parse await-gate workflow spec")
}

/// (17) Happy path: the run parks at the await step (`waiting` + visibility
/// columns), an HTTP signal delivers (`{"delivered": true}`), the run resumes
/// and completes, and the outcome trail carries the payload verbatim.
#[tokio::test]
async fn await_signal_parks_delivers_and_advances_with_payload() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("park", "approve", Some(60_000), 5),
    )
    .await?;

    // Step 0's doc is durable; the run parks at step 1 with the wait visible.
    let parked = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Waiting).await;
    assert_eq!(parked.info.current_step, 1);
    assert_eq!(parked.info.waiting_for.as_deref(), Some("approve"));
    assert!(parked.info.waited_since.is_some());
    assert_eq!(projects_count(&pool, &db).await, 1);

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "approve", "payload": { "ok": true } }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "delivered": true }));

    let done = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(done.step_outcomes.len(), 3);
    assert!(
        done.step_outcomes
            .iter()
            .all(|o| o.status == OutcomeStatus::Success)
    );
    assert_eq!(
        done.step_outcomes[1].signal,
        Some(serde_json::json!({ "ok": true })),
        "the outcome trail carries the payload verbatim"
    );
    assert_eq!(done.info.waiting_for, None);
    assert_eq!(projects_count(&pool, &db).await, 2);

    Ok(())
}

/// (17b) A payload-less delivery consumes the gate: the omitted payload is
/// delivered as JSON null (never SQL NULL, which is the no-signal state), so
/// the run completes and the outcome records `signal: null` — present, not
/// omitted — without burning a timeout attempt.
#[tokio::test]
async fn await_signal_payloadless_delivery_consumes_gate() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("payloadless", "approve", Some(60_000), 5),
    )
    .await?;

    let parked = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Waiting).await;
    assert_eq!(parked.info.current_step, 1);

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "delivered": true }));

    let done = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(done.step_outcomes.len(), 3);
    assert_eq!(done.step_outcomes[1].attempts, 1);
    // The committer records `signal: null` in the stored trail — but the
    // typed `signal: Option<Value>` collapses JSON null to None on every read
    // (deserialize null → None, re-serialize → omitted), so present-vs-absent
    // is asserted on the raw jsonb column (a missing key would parse to Null
    // too — `.get` is what distinguishes them).
    let stored: serde_json::Value = sqlx::query_scalar(&format!(
        "SELECT step_outcomes FROM \"db_{db}\".workflows WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        stored[1].get("signal"),
        Some(&serde_json::Value::Null),
        "an omitted payload records signal: null — present, not absent"
    );
    assert_eq!(projects_count(&pool, &db).await, 2);

    Ok(())
}

/// (18) Timeout retry waits a FRESH full timeoutMs (never backoff): the first
/// re-park carries attempts == 1 and a gate ≈ timeoutMs past the re-park
/// (backoff at initialRetryMs 10 would leave ~10 ms); a delivery into the
/// re-parked wait then succeeds with the attempts accounting (outcome
/// attempts == 2 — one timed-out attempt plus the delivered one).
#[tokio::test]
async fn await_signal_timeout_retries_with_fresh_timeout_then_succeeds() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("fresh-timeout", "approve", Some(100), 3),
    )
    .await?;

    let repark = await_status(&pool, &db, &id, |i| {
        i.status == WorkflowStatus::Waiting && i.attempts >= 1
    })
    .await;
    assert_eq!(repark.info.attempts, 1);
    assert_eq!(repark.info.waiting_for.as_deref(), Some("approve"));
    let gate = repark
        .info
        .sleep_until
        .expect("waiting rows carry their gate");
    assert!(
        gate - repark.info.updated_at >= 90,
        "retry must wait the full timeoutMs again, not backoff (gate {gate}, updated_at {}, distance {})",
        repark.info.updated_at,
        gate - repark.info.updated_at
    );

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "approve", "payload": { "after": "timeout" } }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "delivered": true }));

    let done = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(
        done.step_outcomes[1].attempts, 2,
        "one timed-out attempt + the delivered one"
    );
    assert_eq!(
        done.step_outcomes[1].signal,
        Some(serde_json::json!({ "after": "timeout" }))
    );
    assert_eq!(projects_count(&pool, &db).await, 2);

    Ok(())
}

/// (19) Timeout exhaustion fails typed: maxAttempts 1 with no delivery ends
/// `failed` with `last_error == "awaitSignal 'approve' timed out"`, the
/// outcome trail marks the await step Failed, and the "post" step never runs.
#[tokio::test]
async fn await_signal_timeout_exhaustion_fails_typed() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("exhaust", "approve", Some(100), 1),
    )
    .await?;

    let failed = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Failed).await;
    assert_eq!(
        failed.info.last_error.as_deref(),
        Some("awaitSignal 'approve' timed out")
    );
    assert_eq!(failed.info.current_step, 1);
    assert_eq!(failed.info.attempts, 1);
    assert_eq!(failed.step_outcomes.len(), 2);
    assert_eq!(failed.step_outcomes[0].status, OutcomeStatus::Success);
    assert_eq!(failed.step_outcomes[1].status, OutcomeStatus::Failed);
    assert_eq!(
        failed.step_outcomes[1].error.as_deref(),
        Some("awaitSignal 'approve' timed out")
    );
    assert_eq!(failed.step_outcomes[1].signal, None);
    assert_eq!(
        projects_count(&pool, &db).await,
        1,
        "\"post\" must never insert after exhaustion"
    );

    Ok(())
}

/// (20) Typed delivery errors on all three surfaces: wrong name → 409 CONFLICT
/// naming both signals; unknown id → 404 NOT_FOUND; after cancel → 409
/// not-waiting. Same classification via the WS ack's error envelope and the
/// admin route. A read-only token is rejected (403 HTTP / FORBIDDEN ack)
/// before any delivery attempt — the wait survives every failed delivery
/// above, and only the cancel ends it.
#[tokio::test]
async fn signal_delivery_typed_errors_on_all_three_surfaces() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;
    // A read-only machine token (the mint-scoped pattern with readOnly).
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({
            "db": db, "name": "ro", "tables": ["projects"], "readOnly": true
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let ro: serde_json::Value = resp.json().await?;
    let ro = ro["token"].as_str().expect("read-only token").to_string();

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("typed", "approve", Some(60_000), 5),
    )
    .await?;
    await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Waiting).await;

    // HTTP: wrong name → 409 CONFLICT naming both signals.
    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "reject" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        body["message"],
        serde_json::json!("workflow waiting on 'approve', got 'reject'")
    );

    // HTTP: unknown id → 404 NOT_FOUND.
    let resp = api_post(
        addr,
        "/api/workflows/nope/signal",
        &token,
        serde_json::json!({ "db": db, "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    assert_eq!(body["message"], serde_json::json!("unknown workflow"));

    // WS: the same classifications ride the workflowAck error envelope.
    let mut ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ws, &token, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));
    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "signalWorkflow", "workflowId": "w1",
            "id": id, "name": "reject"
        }),
    )
    .await;
    let ack = ws_recv_json(&mut ws).await;
    assert_eq!(ack["type"], serde_json::json!("workflowAck"));
    assert_eq!(ack["workflowId"], serde_json::json!("w1"));
    assert_eq!(ack["ok"], serde_json::json!(false));
    assert_eq!(ack["error"]["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        ack["error"]["message"],
        serde_json::json!("workflow waiting on 'approve', got 'reject'")
    );
    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "signalWorkflow", "workflowId": "w2",
            "id": "nope", "name": "approve"
        }),
    )
    .await;
    let ack = ws_recv_json(&mut ws).await;
    assert_eq!(ack["workflowId"], serde_json::json!("w2"));
    assert_eq!(ack["ok"], serde_json::json!(false));
    assert_eq!(ack["error"]["code"], serde_json::json!("NOT_FOUND"));
    assert_eq!(
        ack["error"]["message"],
        serde_json::json!("unknown workflow")
    );

    // Admin route: same two classifications.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/{id}/signal"),
        serde_json::json!({ "name": "reject" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        body["message"],
        serde_json::json!("workflow waiting on 'approve', got 'reject'")
    );
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/nope/signal"),
        serde_json::json!({ "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));

    // Read-only token: rejected on HTTP (403) and WS (ack error) — before any
    // delivery attempt, so the check does not need a deliverable wait.
    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &ro,
        serde_json::json!({ "db": db, "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));
    assert_eq!(
        body["message"],
        serde_json::json!("read-only token cannot mutate")
    );
    let mut ro_ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ro_ws, &ro, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));
    ws_send_json(
        &mut ro_ws,
        serde_json::json!({
            "type": "signalWorkflow", "workflowId": "w3",
            "id": id, "name": "approve"
        }),
    )
    .await;
    let ack = ws_recv_json(&mut ro_ws).await;
    assert_eq!(ack["type"], serde_json::json!("workflowAck"));
    assert_eq!(ack["ok"], serde_json::json!(false));
    assert_eq!(ack["error"]["code"], serde_json::json!("FORBIDDEN"));
    assert_eq!(
        ack["error"]["message"],
        serde_json::json!("read-only token cannot mutate")
    );

    // None of the failed deliveries consumed the wait; cancel ends it, and a
    // late signal on the terminal run conflicts on all three surfaces.
    let parked = workflows::get(&pool, &db, &id).await?.expect("row");
    assert_eq!(parked.info.status, WorkflowStatus::Waiting);
    assert!(workflows::cancel(&pool, &db, &id).await?);

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        body["message"],
        serde_json::json!("workflow is not waiting for a signal")
    );

    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "signalWorkflow", "workflowId": "w4",
            "id": id, "name": "approve"
        }),
    )
    .await;
    let ack = ws_recv_json(&mut ws).await;
    assert_eq!(ack["workflowId"], serde_json::json!("w4"));
    assert_eq!(ack["ok"], serde_json::json!(false));
    assert_eq!(ack["error"]["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        ack["error"]["message"],
        serde_json::json!("workflow is not waiting for a signal")
    );

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/{id}/signal"),
        serde_json::json!({ "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        body["message"],
        serde_json::json!("workflow is not waiting for a signal")
    );
    assert_eq!(
        projects_count(&pool, &db).await,
        1,
        "the cancelled run never advances to \"post\""
    );

    Ok(())
}

/// (21) No timeoutMs: the wait is indefinite — the run stays `waiting` across
/// several scheduler claim sweeps and the gate is not claimable even 10 s out;
/// a signal still advances it.
#[tokio::test]
async fn await_signal_no_timeout_waits_indefinitely() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;

    let id = workflows::insert(&pool, &db, &await_gate_spec("forever", "approve", None, 5)).await?;

    let parked = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Waiting).await;
    assert_eq!(parked.info.waiting_for.as_deref(), Some("approve"));
    assert_eq!(projects_count(&pool, &db).await, 1);

    // Several claim sweeps (the scheduler loop wakes at least once per its
    // 2 s MAX_SLEEP) leave the run parked; the gate is never due.
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let still = workflows::get(&pool, &db, &id).await?.expect("row");
    assert_eq!(still.info.status, WorkflowStatus::Waiting);
    assert_eq!(still.info.current_step, 1);
    assert!(
        workflows::claim_due(&pool, &db, now_ms() + 10_000, 10)
            .await?
            .is_empty(),
        "no timeoutMs ⇒ the gate is never claimable"
    );
    assert_eq!(projects_count(&pool, &db).await, 1);

    assert!(matches!(
        workflows::deliver_signal(
            &pool,
            &db,
            &id,
            "approve",
            Some(serde_json::json!({"late": true}))
        )
        .await?,
        workflows::SignalDelivery::Delivered
    ));
    let done = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(
        done.step_outcomes[1].signal,
        Some(serde_json::json!({ "late": true }))
    );
    assert_eq!(projects_count(&pool, &db).await, 2);

    Ok(())
}

/// (22) Latest-wins payload: two deliveries into an unconsumed wait both ack
/// `{"delivered": true}` and the consumed signal is the SECOND payload. The
/// park is manual and the engine spawns only after both deliveries — a live
/// scheduler could otherwise claim between them and turn the second into a
/// not-waiting 409 (the side-table-lifecycle + crash-resume patterns).
#[tokio::test]
async fn await_signal_latest_wins_payload() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    // The await step at index 0: claim → park, no engine running yet.
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "latest",
        "steps": [
            { "awaitSignal": { "name": "approve", "timeoutMs": 60_000 },
              "retry": { "maxAttempts": 5, "initialRetryMs": 10, "maxRetryMs": 50 } },
            insert_step("post")
        ]
    }))
    .expect("parse latest-wins workflow spec");
    let id = workflows::insert(&pool, &db, &spec).await?;
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10).await?;
    assert_eq!(claimed.len(), 1);
    workflows::park_waiting(&pool, &db, &id, 0, "approve", now_ms() + 60_000).await?;

    for payload in [serde_json::json!({ "v": 1 }), serde_json::json!({ "v": 2 })] {
        let resp = api_post(
            addr,
            &format!("/api/workflows/{id}/signal"),
            &token,
            serde_json::json!({ "db": db, "name": "approve", "payload": payload }),
        )
        .await;
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        let body: serde_json::Value = resp.json().await?;
        assert_eq!(body, serde_json::json!({ "delivered": true }));
    }

    // Spawn the engine after the slot is final: the claim consumes {"v": 2}.
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let done = await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(done.step_outcomes.len(), 2);
    assert_eq!(
        done.step_outcomes[0].signal,
        Some(serde_json::json!({ "v": 2 })),
        "latest delivery wins the consumed slot"
    );
    assert_eq!(projects_count(&pool, &db).await, 1);

    Ok(())
}

/// (23) Cancel while waiting: the run flips `cancelled` (wait columns drop
/// with the projection), and a late signal for the right name conflicts
/// instead of resurrecting it.
#[tokio::test]
async fn cancel_while_waiting_then_late_signal_conflicts() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("cancel-wait", "approve", Some(60_000), 5),
    )
    .await?;
    await_status(&pool, &db, &id, |i| i.status == WorkflowStatus::Waiting).await;

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/cancel"),
        &token,
        serde_json::json!({ "db": db }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "cancelled": true }));

    let row = workflows::get(&pool, &db, &id).await?.expect("row");
    assert_eq!(row.info.status, WorkflowStatus::Cancelled);
    assert_eq!(
        row.info.waiting_for, None,
        "leaving waiting clears the wait"
    );
    assert!(row.info.finished_at.is_some());

    let resp = api_post(
        addr,
        &format!("/api/workflows/{id}/signal"),
        &token,
        serde_json::json!({ "db": db, "name": "approve" }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::CONFLICT);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CONFLICT"));
    assert_eq!(
        body["message"],
        serde_json::json!("workflow is not waiting for a signal")
    );
    let row = workflows::get(&pool, &db, &id).await?.expect("row");
    assert_eq!(row.info.status, WorkflowStatus::Cancelled);
    assert_eq!(
        projects_count(&pool, &db).await,
        1,
        "\"post\" must never insert after the cancel"
    );

    Ok(())
}

/// (24) The WS frame and the admin route each deliver a signal that advances a
/// parked run — both surfaces drive the same `deliver_signal` wake the HTTP
/// route covers in (17), each landing its own payload in the outcome trail.
#[tokio::test]
async fn await_signal_ws_and_admin_surfaces_deliver() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let committers = make_committers(&state).await;
    warm_up(&committers, &db).await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let ws_id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("ws-gate", "approve", Some(60_000), 5),
    )
    .await?;
    let admin_id = workflows::insert(
        &pool,
        &db,
        &await_gate_spec("admin-gate", "approve", Some(60_000), 5),
    )
    .await?;
    await_status(&pool, &db, &ws_id, |i| i.status == WorkflowStatus::Waiting).await;
    await_status(&pool, &db, &admin_id, |i| {
        i.status == WorkflowStatus::Waiting
    })
    .await;

    let mut ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ws, &token, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));
    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "signalWorkflow", "workflowId": "w1",
            "id": ws_id, "name": "approve", "payload": { "via": "ws" }
        }),
    )
    .await;
    let ack = ws_recv_json(&mut ws).await;
    assert_eq!(ack["type"], serde_json::json!("workflowAck"));
    assert_eq!(ack["workflowId"], serde_json::json!("w1"));
    assert_eq!(ack["ok"], serde_json::json!(true));
    assert!(ack.get("error").is_none(), "clean ack omits error");

    let ws_done = await_status(&pool, &db, &ws_id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(
        ws_done.step_outcomes[1].signal,
        Some(serde_json::json!({ "via": "ws" }))
    );

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/workflows/{admin_id}/signal"),
        serde_json::json!({ "name": "approve", "payload": { "via": "admin" } }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({ "ok": true }));

    let admin_done = await_status(&pool, &db, &admin_id, |i| {
        i.status == WorkflowStatus::Success
    })
    .await;
    assert_eq!(
        admin_done.step_outcomes[1].signal,
        Some(serde_json::json!({ "via": "admin" }))
    );
    assert_eq!(projects_count(&pool, &db).await, 4);

    Ok(())
}

/// (25) The 64 KiB payload cap, pinned server-side (until now only the
/// python harness covered it): a delivery whose serialized payload exceeds
/// `MAX_SIGNAL_PAYLOAD_BYTES` is `BadRequest` — the exact `deliver_signal`
/// message — BEFORE any slot write, so the row is still `waiting` with the
/// signal slot empty: a failed delivery consumes nothing. Side-table only
/// (the lifecycle test's claim-then-park setup — no committer, no spawn).
#[tokio::test]
async fn await_signal_payload_cap_rejects_without_consuming() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await?;
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "cap", "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 60_000 } } ]
    }))?;
    let id = workflows::insert(&pool, &db, &spec).await?;
    // Only the advance arm parks, on a row it holds `running` — claim first
    // (the `status = 'running'` guard on `park_waiting`).
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10).await?;
    assert_eq!(claimed.len(), 1);
    workflows::park_waiting(&pool, &db, &id, 0, "approve", now_ms() + 60_000).await?;

    // A JSON string of exactly MAX_SIGNAL_PAYLOAD_BYTES chars serializes to
    // MAX + 2 bytes (the quotes), so it is over the cap.
    let oversized = serde_json::Value::String("x".repeat(workflows::MAX_SIGNAL_PAYLOAD_BYTES));
    let err = workflows::deliver_signal(&pool, &db, &id, "approve", Some(oversized))
        .await
        .expect_err("an over-cap payload must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(err.message, "signal payload exceeds 65536 bytes");

    let full = workflows::get(&pool, &db, &id).await?.expect("row");
    assert_eq!(full.info.status, WorkflowStatus::Waiting);
    assert_eq!(full.info.waiting_for.as_deref(), Some("approve"));
    let slot: Option<serde_json::Value> = sqlx::query_scalar(&format!(
        "SELECT signal_payload FROM \"db_{db}\".workflows WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert!(
        slot.is_none(),
        "a failed delivery consumed nothing — the slot stays empty"
    );

    Ok(())
}
