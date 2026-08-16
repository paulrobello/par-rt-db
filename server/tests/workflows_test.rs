//! Integration tests for durable workflows (FM-29). Task 2 covers the
//! side-table ops (insert/claim/reset/list/cancel/get/delete); Tasks 3–5
//! grow this file with committer-advance and step-surface coverage.

mod common;

use common::{fresh_db, test_state};
use rtdb_server::db::now_ms;
use rtdb_server::protocol::{WorkflowSpec, WorkflowStatus};
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
