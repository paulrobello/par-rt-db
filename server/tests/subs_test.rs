mod common;

use common::{fresh_db, test_state};
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::Query;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction};
use tokio::sync::mpsc::error::TryRecvError;

fn work_item_doc(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "projectId": "0".repeat(32),
        "title": "item",
        "status": status,
        "order": order,
        "completedAt": null
    })
    .as_object()
    .expect("json object")
    .clone()
}

fn insert_work_item(status: &str, order: f64) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: work_item_doc(status, order),
        }],
    }
}

fn insert_project() -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: serde_json::json!({
                "name": "Alpha",
                "description": null,
                "status": "active",
                "tags": [],
                "updatedAt": 1.0
            })
            .as_object()
            .expect("json object")
            .clone(),
        }],
    }
}

fn collect_work_items() -> Query {
    serde_json::from_value(serde_json::json!({"table": "workItems"})).expect("parse query")
}

fn work_items_by_status(status: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "index": "by_status",
        "eq": [status]
    }))
    .expect("parse query")
}

fn docs_len(value: &serde_json::Value) -> usize {
    value.as_array().expect("docs array").len()
}

// (a) subscribe -> immediate initial QueryUpdate with seeded rows.
#[tokio::test]
async fn subscribe_sends_initial_query_update_with_seeded_rows() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;
    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 2.0))
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), collect_work_items(), tx)
        .await?;

    let msg = rx.try_recv().expect("initial query update");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert_eq!(docs_len(&result), 2);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }

    Ok(())
}

// (b) mutate inserting a matching workItem -> exactly one more QueryUpdate containing 3 items.
#[tokio::test]
async fn mutate_pushes_update_to_matching_subscription() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;
    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 2.0))
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), collect_work_items(), tx)
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 3.0))
        .await?;

    let msg = rx.try_recv().expect("update after mutate");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert_eq!(docs_len(&result), 3);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    Ok(())
}

// (c) mutate touching only projects -> no message for a workItems-only sub.
#[tokio::test]
async fn mutate_on_unrelated_table_sends_no_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), collect_work_items(), tx)
        .await?;
    rx.try_recv().expect("initial query update");

    state.committers.mutate(&db, None, insert_project()).await?;

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    Ok(())
}

// (d) mutate inserting a workItem NOT matching the sub's eq -> no message (result unchanged).
#[tokio::test]
async fn mutate_not_matching_eq_filter_sends_no_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            work_items_by_status("backlog"),
            tx,
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .committers
        .mutate(&db, None, insert_work_item("done", 2.0))
        .await?;

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    Ok(())
}

// (e) two subs on different queryIds both update from one txn.
#[tokio::test]
async fn two_subscriptions_both_receive_updates_from_one_mutate() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
    let conn1 = next_conn_id();
    state
        .committers
        .subscribe(&db, conn1, "q1".to_string(), collect_work_items(), tx1)
        .await?;
    rx1.try_recv().expect("initial q1");

    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
    let conn2 = next_conn_id();
    state
        .committers
        .subscribe(&db, conn2, "q2".to_string(), collect_work_items(), tx2)
        .await?;
    rx2.try_recv().expect("initial q2");

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;

    let msg1 = rx1.try_recv().expect("q1 update");
    let msg2 = rx2.try_recv().expect("q2 update");
    assert!(matches!(msg1, ServerMessage::QueryUpdate { ref query_id, .. } if query_id == "q1"));
    assert!(matches!(msg2, ServerMessage::QueryUpdate { ref query_id, .. } if query_id == "q2"));

    Ok(())
}

// (f) remove_conn then mutate -> no message.
#[tokio::test]
async fn remove_conn_stops_further_updates() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), collect_work_items(), tx)
        .await?;
    rx.try_recv().expect("initial query update");

    state.subs.remove_conn(&db, conn).await;

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;

    // No QueryUpdate is delivered: either the channel is empty, or (as here,
    // since this test keeps no other clone of `tx` alive) removing the sole
    // subscription drops the last sender and the channel disconnects. Either
    // way, no message reaches the connection.
    assert!(rx.try_recv().is_err());

    Ok(())
}

// (g) mutate on nonexistent db -> NotFound.
#[tokio::test]
async fn mutate_on_nonexistent_db_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;

    let err = state
        .committers
        .mutate("does_not_exist", None, insert_project())
        .await
        .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}
