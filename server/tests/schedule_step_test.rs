//! Integration tests for the `Step::Schedule`/`Step::CancelSchedule` txn steps
//! (FM-28 Task 2). The harness mirrors `tests/txn_test.rs`: `fresh_db` pushes
//! the kanban fixture (`projects` + `workItems`), and each test drives
//! `execute_txn` directly with `PrincipalCtx::bypass()`. The fire test
//! (`chained_schedule_fires_and_enqueues_follow_up`) additionally mirrors
//! `tests/scheduled_test.rs`: a real `Committers` so the per-db scheduler
//! claims the enqueued row and the committer executes it end-to-end.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use sqlx::PgPool;

use common::{TestDb, admin_get, admin_post, fresh_db, kanban_schema_json, spawn_app, test_state};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::committer::Committers;
use rtdb_server::db::SchemaCache;
use rtdb_server::error::ErrorCode;
use rtdb_server::metrics::Metrics;
use rtdb_server::op_feed::OpFeed;
use rtdb_server::protocol::{ScheduleStatus, ScheduleWhen};
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::quota;
use rtdb_server::scheduler;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::SubscriptionManager;
use rtdb_server::txn::{Step, Transaction, execute_txn};

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

fn insert_project_step() -> Step {
    Step::Insert {
        table: "projects".to_string(),
        doc: valid_project_doc(),
    }
}

/// A no-index `.take(100)` (collect) over `table` — the query-based observer
/// the tests use instead of adding new server surface.
fn take_query(table: &str) -> Query {
    Query {
        table: table.to_string(),
        get: None,
        index: None,
        eq: vec![],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: Some(100),
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

/// Scalar `count` over `table` (needs no index) — for assertions beyond the
/// take(100) cap of `count_docs`.
async fn table_count(
    pool: &PgPool,
    db: &TestDb,
    schema: &SchemaDef,
    table: &str,
) -> anyhow::Result<i64> {
    let mut q = take_query(table);
    q.take = None;
    q.count = true;
    match execute_query(pool, db, schema, &q, &PrincipalCtx::bypass(), false).await? {
        QueryResult::Count(n) => Ok(n),
        other => panic!("expected Count variant, got {other:?}"),
    }
}

async fn count_docs(
    pool: &PgPool,
    db: &TestDb,
    schema: &SchemaDef,
    table: &str,
) -> anyhow::Result<usize> {
    let result = execute_query(
        pool,
        db,
        schema,
        &take_query(table),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match result {
        QueryResult::Docs(docs) => Ok(docs.len()),
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

/// A `RunAt` safely in the future so the scheduler never fires the job while
/// the test asserts on its pending row.
fn future_run_at() -> ScheduleWhen {
    ScheduleWhen::RunAt {
        ms: rtdb_server::db::now_ms() + 600_000,
    }
}

async fn make_committers(state: &Arc<AppState>) -> Arc<Committers> {
    Arc::new(Committers::new(
        state.pool.clone(),
        SubscriptionManager::new(),
        SchemaCache::new(),
        OpFeed::new(64, 32),
        Arc::new(ArcSwap::from_pointee(common::test_hot())),
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
/// no-op mutate. Both spawn inside `channel_for` on first use.
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

/// Polls a no-index take(100) on `table` until it returns at least `want`
/// docs or `timeout` elapses. Returns true if observed. Modeled on
/// `scheduled_test.rs::poll_for_n`: deadline loop, 50ms between attempts.
async fn poll_until(
    pool: &PgPool,
    db: &TestDb,
    schema: &SchemaDef,
    table: &str,
    want: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(n) = count_docs(pool, db, schema, table).await
            && n >= want
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// (1) The Schedule step commits atomically with the enclosing txn's writes:
// one txn produces the projects doc AND the pending scheduled_txns row.
#[tokio::test]
async fn schedule_step_commits_atomically_with_writes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                insert_project_step(),
                Step::Schedule {
                    when: future_run_at(),
                    txn: Box::new(Transaction {
                        steps: vec![Step::Insert {
                            table: "workItems".to_string(),
                            doc: valid_work_item_doc("nested"),
                        }],
                    }),
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;

    let schedule_id = outcome.results[1]["scheduleId"]
        .as_str()
        .expect("scheduleId string")
        .to_string();
    assert!(!schedule_id.is_empty());

    let listed = scheduler::list(&pool, &db).await?;
    assert_eq!(listed.len(), 1, "exactly one pending job");
    assert_eq!(listed[0].id, schedule_id);
    assert_eq!(listed[0].status, ScheduleStatus::Pending);

    // The same txn's document write is durable and queryable.
    assert_eq!(count_docs(&pool, &db, &schema, "projects").await?, 1);

    Ok(())
}

// (2) A failed step AFTER the Schedule rolls the enqueue back with the txn:
// no orphan job survives a NotFound from a later step.
#[tokio::test]
async fn schedule_step_rolls_back_with_failed_txn() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Schedule {
                    when: future_run_at(),
                    txn: Box::new(Transaction { steps: vec![] }),
                },
                Step::ExpectVersion {
                    table: "projects".to_string(),
                    id: "missing".to_string(),
                    version: 1,
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("ExpectVersion on a missing doc must fail");
    assert_eq!(err.code, ErrorCode::NotFound);

    let listed = scheduler::list(&pool, &db).await?;
    assert!(
        listed.is_empty(),
        "failed txn must not leave an orphan scheduled job"
    );
    assert_eq!(count_docs(&pool, &db, &schema, "projects").await?, 0);

    Ok(())
}

// (3) An invalid `when` rejects the whole txn: the doc write before it is
// rolled back and no job row exists. Both bad shapes covered: negative
// AfterMs and an unparseable cron expression.
#[tokio::test]
async fn bad_when_rolls_back_writes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    for when in [
        ScheduleWhen::AfterMs { ms: -1 },
        ScheduleWhen::Cron {
            expr: "not a cron".to_string(),
        },
    ] {
        let err = execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![
                    insert_project_step(),
                    Step::Schedule {
                        when,
                        txn: Box::new(Transaction { steps: vec![] }),
                    },
                ],
            },
            &PrincipalCtx::bypass(),
        )
        .await
        .expect_err("invalid when must reject the txn");
        assert_eq!(err.code, ErrorCode::BadRequest);

        assert_eq!(
            count_docs(&pool, &db, &schema, "projects").await?,
            0,
            "doc write must roll back with the bad schedule"
        );
        assert!(
            scheduler::list(&pool, &db).await?.is_empty(),
            "no job row may exist after a bad when"
        );
    }

    Ok(())
}

// (4) CancelSchedule result shape + idempotence: first cancel reports true,
// repeats report false, and the standalone scheduler::cancel agrees.
#[tokio::test]
async fn cancel_schedule_step_result_and_idempotence() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Schedule {
                when: future_run_at(),
                txn: Box::new(Transaction { steps: vec![] }),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    let id = outcome.results[0]["scheduleId"]
        .as_str()
        .expect("scheduleId string")
        .to_string();

    let cancel = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::CancelSchedule { id: id.clone() }],
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
            steps: vec![Step::CancelSchedule { id: id.clone() }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    assert_eq!(
        again.results,
        vec![serde_json::json!({ "cancelled": false })]
    );

    // The standalone cancel op on the same (already-cancelled) id also false.
    assert!(!scheduler::cancel(&pool, &db, &id).await?);

    Ok(())
}

// (5) A CancelSchedule for a missing job is NOT an error: the txn succeeds,
// reports cancelled:false positionally, and the accompanying write commits.
#[tokio::test]
async fn cancel_step_commits_atomically_with_writes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::CancelSchedule {
                    id: "no-such-job".to_string(),
                },
                insert_project_step(),
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;

    assert_eq!(
        outcome.results[0],
        serde_json::json!({ "cancelled": false })
    );
    assert!(
        outcome.results[1]["id"].as_str().is_some(),
        "insert result expected at position 1"
    );
    assert_eq!(count_docs(&pool, &db, &schema, "projects").await?, 1);

    Ok(())
}

// (6) A scoped machine token cannot smuggle a future write into a forbidden
// table via a nested Schedule txn: the recursive table-scope check runs at
// ENQUEUE time inside the same sqlx tx, so the whole txn (including the doc
// write before it) rolls back.
#[tokio::test]
async fn scoped_token_cannot_enqueue_forbidden_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let scoped = PrincipalCtx {
        user_id: None,
        email: None,
        tables: Some(vec!["projects".to_string()]),
    };

    // (a) Regression guard: a top-level Insert into workItems is Forbidden
    // (pre-existing per-step gate behavior).
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "workItems".to_string(),
                doc: valid_work_item_doc("direct"),
            }],
        },
        &scoped,
    )
    .await
    .expect_err("scoped token must not write workItems directly");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // (b) The smuggle attempt: allowed projects write first, then a Schedule
    // whose NESTED txn writes workItems. Forbidden, and nothing commits.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                insert_project_step(),
                Step::Schedule {
                    when: future_run_at(),
                    txn: Box::new(Transaction {
                        steps: vec![Step::Insert {
                            table: "workItems".to_string(),
                            doc: valid_work_item_doc("smuggled"),
                        }],
                    }),
                },
            ],
        },
        &scoped,
    )
    .await
    .expect_err("scoped token must not enqueue a workItems write");
    assert_eq!(err.code, ErrorCode::Forbidden);
    assert_eq!(
        count_docs(&pool, &db, &schema, "projects").await?,
        0,
        "whole txn must roll back, including the allowed projects write"
    );
    assert!(
        scheduler::list(&pool, &db).await?.is_empty(),
        "no job row may survive the Forbidden"
    );

    // (c) Nested txn writing only the ALLOWED table enqueues fine.
    let ok = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                insert_project_step(),
                Step::Schedule {
                    when: future_run_at(),
                    txn: Box::new(Transaction {
                        steps: vec![insert_project_step()],
                    }),
                },
            ],
        },
        &scoped,
    )
    .await?;
    assert!(ok.results[1]["scheduleId"].as_str().is_some());
    assert_eq!(scheduler::list(&pool, &db).await?.len(), 1);
    assert_eq!(count_docs(&pool, &db, &schema, "projects").await?, 1);

    Ok(())
}

// (7) The recursive step budget: 513 top-level Inserts + 1 Schedule nesting
// 512 = 1026 recursive steps exceeds MAX_STEPS (1024) and is rejected BEFORE
// any step executes (table stays empty). 511 + 1 + 512 = 1024 exactly is Ok.
#[tokio::test]
async fn recursive_step_budget() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let nested_512 = || Transaction {
        steps: (0..512).map(|_| insert_project_step()).collect(),
    };

    // Over budget: 513 + 1 + 512 = 1026 > 1024.
    let over = Transaction {
        steps: (0..513)
            .map(|_| insert_project_step())
            .chain(std::iter::once(Step::Schedule {
                when: future_run_at(),
                txn: Box::new(nested_512()),
            }))
            .collect(),
    };
    let err = execute_txn(&pool, &db, &schema, &over, &PrincipalCtx::bypass())
        .await
        .expect_err("1026 recursive steps must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(
        count_docs(&pool, &db, &schema, "projects").await?,
        0,
        "over-budget txn must execute no steps"
    );
    assert!(scheduler::list(&pool, &db).await?.is_empty());

    // Exactly at budget: 511 + 1 + 512 = 1024 is allowed.
    let exact = Transaction {
        steps: (0..511)
            .map(|_| insert_project_step())
            .chain(std::iter::once(Step::Schedule {
                when: future_run_at(),
                txn: Box::new(nested_512()),
            }))
            .collect(),
    };
    let outcome = execute_txn(&pool, &db, &schema, &exact, &PrincipalCtx::bypass()).await?;
    assert!(outcome.results[511]["scheduleId"].as_str().is_some());
    assert_eq!(table_count(&pool, &db, &schema, "projects").await?, 511);
    assert_eq!(scheduler::list(&pool, &db).await?.len(), 1);

    Ok(())
}

// (8) End-to-end chained fire: a Schedule step whose nested txn itself
// contains a Schedule step. The first job (RunAt in the past = immediate)
// fires via the real Committers + scheduler, and the FIRE path must execute
// the nested Schedule step — enqueueing the follow-up — which then fires too
// and writes its doc. Polling until BOTH workItems docs exist proves the
// full chain scheduled → fired → scheduled → fired.
#[tokio::test]
async fn chained_schedule_fires_and_enqueues_follow_up() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();
    let committers = make_committers(&state).await;

    warm_up(&committers, &db).await;

    let nested = Transaction {
        steps: vec![
            Step::Insert {
                table: "workItems".to_string(),
                doc: valid_work_item_doc("chained-a"),
            },
            Step::Schedule {
                when: ScheduleWhen::AfterMs { ms: 0 },
                txn: Box::new(Transaction {
                    steps: vec![Step::Insert {
                        table: "workItems".to_string(),
                        doc: valid_work_item_doc("chained-b"),
                    }],
                }),
            },
        ],
    };
    let outcome = committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Schedule {
                    when: ScheduleWhen::RunAt { ms: 1 }, // past ⇒ immediate
                    txn: Box::new(nested),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;
    assert!(
        outcome.results[0]["scheduleId"].as_str().is_some(),
        "top-level Schedule step must report its id"
    );

    let both = poll_until(&pool, &db, &schema, "workItems", 2, Duration::from_secs(15)).await;
    assert!(
        both,
        "chained schedules must both fire: expected 2 workItems docs"
    );

    Ok(())
}

// --- Enqueue-time table scoping on the standalone Schedule surfaces --------
// (FM-28 Task 3). The `execute_txn`-level tests above (6) prove the recursive
// check inside a txn; these prove the three standalone enqueue paths apply it
// too: WS reuses the same helper (covered via the HTTP surface here since both
// route through `authorize_txn_tables` + `resolve_when`), and admin create
// proves the bypass no-op. Mirrors `http_api_test.rs`'s `api_post`/
// `mint_token` helpers.

/// Raw bearer POST to a per-db API route (mirrors `http_api_test.rs::api_post`).
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

/// Mints a machine token scoped to `tables` (pattern: admin_test.rs
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

/// A one-step insert txn over the kanban wire shape, for `POST /api/schedule`.
fn insert_txn_json(table: &str) -> serde_json::Value {
    let doc = match table {
        "workItems" => valid_work_item_doc("http"),
        _ => valid_project_doc(),
    };
    serde_json::json!({"steps": [{
        "op": "insert",
        "table": table,
        "doc": serde_json::Value::Object(doc),
    }]})
}

/// A `POST /api/schedule` body with a `runAt` safely in the future (the job
/// stays pending for the test's assertions; tests cancel what they create).
fn schedule_body(db: &str, txn: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "db": db,
        "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() + 600_000},
        "txn": txn,
    })
}

// (9) HTTP surface: a scoped machine token cannot smuggle a future write into
// a forbidden table via POST /api/schedule — 403 FORBIDDEN before any row is
// written.
#[tokio::test]
async fn scoped_token_cannot_schedule_forbidden_table_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        schedule_body(&db, insert_txn_json("workItems")),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));

    assert!(
        scheduler::list(&pool, &db).await?.is_empty(),
        "no job row may survive the Forbidden"
    );

    Ok(())
}

// (10) HTTP surface: the same scoped token scheduling into its ALLOWED table
// enqueues fine — the tightening does not over-block.
#[tokio::test]
async fn scoped_token_can_schedule_allowed_table_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        schedule_body(&db, insert_txn_json("projects")),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("schedule id").to_string();

    let listed = scheduler::list(&pool, &db).await?;
    assert_eq!(listed.len(), 1, "exactly one pending job");
    assert_eq!(listed[0].id, id);

    // Cancel so the pending row is not left behind.
    assert!(scheduler::cancel(&pool, &db, &id).await?);

    Ok(())
}

// (11) Admin create keeps cross-table scheduling: the enqueue-time check is a
// no-op for the bypass principal, so a txn touching tables no single scoped
// token could combine (projects write + nested Schedule writing workItems)
// still enqueues.
#[tokio::test]
async fn admin_create_schedule_still_allows_cross_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;

    let cross_table_txn = serde_json::json!({
        "steps": [
            {"op": "insert", "table": "projects",
             "doc": serde_json::Value::Object(valid_project_doc())},
            {"op": "schedule",
             "when": {"type": "afterMs", "ms": 3_600_000},
             "txn": insert_txn_json("workItems")},
        ]
    });
    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schedules"),
        serde_json::json!({
            "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() + 600_000},
            "txn": cross_table_txn,
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("schedule id").to_string();

    let listed = scheduler::list(&pool, &db).await?;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);

    assert!(scheduler::cancel(&pool, &db, &id).await?);

    Ok(())
}

// (12) NESTED smuggling at the HTTP surface (review fix round 1): every
// TOP-LEVEL step is allowed for the scoped token, but the txn carries a
// Step::Schedule nesting an Insert into the forbidden table — the exact
// two-level-deep bypass the recursive `authorize_txn_tables` exists to block.
#[tokio::test]
async fn scoped_token_cannot_schedule_nested_forbidden_table_http() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let nested_smuggle = serde_json::json!({
        "steps": [
            {"op": "insert", "table": "projects",
             "doc": serde_json::Value::Object(valid_project_doc())},
            {"op": "schedule",
             "when": {"type": "afterMs", "ms": 3_600_000},
             "txn": insert_txn_json("workItems")},
        ]
    });
    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        schedule_body(&db, nested_smuggle),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));

    assert!(
        scheduler::list(&pool, &db).await?.is_empty(),
        "no job row may survive the Forbidden"
    );

    Ok(())
}

// (13) The restructured WS `handle_schedule` arm (review fix round 1): the
// same nested-smuggling shape over a `/sync` Schedule frame — `scheduleErr`
// Forbidden, no job row. The WS plumbing mirrors `tests/ws_test.rs` (the
// dev-dependencies are shared across test binaries).
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
async fn scoped_token_cannot_schedule_nested_forbidden_table_ws() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let token = mint_scoped_token(addr, &db, &["projects"]).await;

    let mut ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ws, &token, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));

    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "schedule", "scheduleId": "s1",
            "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() + 600_000},
            "txn": {"steps": [
                {"op": "insert", "table": "projects",
                 "doc": serde_json::Value::Object(valid_project_doc())},
                {"op": "schedule",
                 "when": {"type": "afterMs", "ms": 3_600_000},
                 "txn": insert_txn_json("workItems")},
            ]},
        }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("scheduleErr"));
    assert_eq!(reply["scheduleId"], serde_json::json!("s1"));
    assert_eq!(reply["error"]["code"], serde_json::json!("FORBIDDEN"));

    assert!(
        scheduler::list(&pool, &db).await?.is_empty(),
        "no job row may survive the Forbidden"
    );

    Ok(())
}

// (14) Cold-db family (FM-29's workflows fix mirrored onto schedules): a db
// with no data-plane traffic since creation has no spawned per-db tasks, and
// the side table's creation-time rollout does not cover dbs created before it.
// Simulate that legacy shape by dropping `scheduled_txns` outright — the admin
// surfaces must still serve list (200 + empty) and manage (200 + `ok:false`)
// by ensuring the table inline, not 500.
#[tokio::test]
async fn admin_schedules_family_serves_cold_db_without_side_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;

    let schema_name = rtdb_server::ddl::pg_schema(&db);
    sqlx::query(&format!(
        "DROP TABLE IF EXISTS \"{schema_name}\".scheduled_txns"
    ))
    .execute(&pool)
    .await?;

    let resp = admin_get(addr, &format!("/admin/db/{db}/schedules")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse list response");
    assert_eq!(body["schedules"], serde_json::json!([]));

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schedules/no-such-job/cancel"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse cancel response");
    assert_eq!(body["ok"], serde_json::json!(false));

    Ok(())
}

// (15) A one-shot created via ADMIN create on a cold db (no Mutate/Subscribe
// since creation, so no per-db tasks) with a past `runAt` must FIRE without
// any prior data-plane op — the create surface spawns the per-db tasks and
// ensures the table before insert, so the scheduler claims the row.
#[tokio::test]
async fn admin_created_one_shot_on_cold_db_fires() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let schema = kanban_schema();

    let resp = admin_post(
        addr,
        &format!("/admin/db/{db}/schedules"),
        serde_json::json!({
            "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() - 1_000},
            "txn": insert_txn_json("workItems"),
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert!(
        poll_until(&pool, &db, &schema, "workItems", 1, Duration::from_secs(15)).await,
        "cold-db admin-created one-shot must fire without a prior data-plane op"
    );

    Ok(())
}

// (16) Same liveness contract through the HTTP surface (`POST /api/schedule`)
// with a scoped machine token: a past-due one-shot on a cold db fires.
#[tokio::test]
async fn http_created_one_shot_on_cold_db_fires() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let schema = kanban_schema();
    let token = mint_scoped_token(addr, &db, &["workItems"]).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        serde_json::json!({
            "db": db.to_string(),
            "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() - 1_000},
            "txn": insert_txn_json("workItems"),
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    assert!(
        poll_until(&pool, &db, &schema, "workItems", 1, Duration::from_secs(15)).await,
        "cold-db HTTP-created one-shot must fire without a prior data-plane op"
    );

    Ok(())
}

// (17) Same liveness contract through the WS `Schedule` frame: a past-due
// one-shot on a cold db is acked `scheduleOk` and then fires.
#[tokio::test]
async fn ws_created_one_shot_on_cold_db_fires() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let addr = spawn_app(state).await;
    let schema = kanban_schema();
    let token = mint_scoped_token(addr, &db, &["workItems"]).await;

    let mut ws = ws_connect(addr).await;
    let hello = ws_auth(&mut ws, &token, &db).await;
    assert_eq!(hello["type"], serde_json::json!("authOk"));

    ws_send_json(
        &mut ws,
        serde_json::json!({
            "type": "schedule", "scheduleId": "cold1",
            "when": {"type": "runAt", "ms": rtdb_server::db::now_ms() - 1_000},
            "txn": insert_txn_json("workItems"),
        }),
    )
    .await;
    let reply = ws_recv_json(&mut ws).await;
    assert_eq!(reply["type"], serde_json::json!("scheduleOk"));

    assert!(
        poll_until(&pool, &db, &schema, "workItems", 1, Duration::from_secs(15)).await,
        "cold-db WS-created one-shot must fire without a prior data-plane op"
    );

    Ok(())
}
