mod common;

use common::{fresh_db, test_state};
use rtdb_server::auth::PrincipalCtx;
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
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 2.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
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
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 2.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 3.0),
            PrincipalCtx::bypass(),
        )
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
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(&db, None, insert_project(), PrincipalCtx::bypass())
        .await?;

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    Ok(())
}

// (new) a repeated idempotency key dedupes and sends no second update.
#[tokio::test]
async fn mutate_with_same_idempotency_key_sends_no_second_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    let first = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("retry-key".to_string()),
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("update after first mutate");

    let second = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("retry-key".to_string()),
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    assert_eq!(first.results, second.results);
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

    Ok(())
}

// (d) mutate inserting a workItem NOT matching the sub's eq -> no message (result unchanged).
#[tokio::test]
async fn mutate_not_matching_eq_filter_sends_no_update() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            work_items_by_status("backlog"),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("done", 2.0),
            PrincipalCtx::bypass(),
        )
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
        .realtime
        .committers
        .subscribe(
            &db,
            conn1,
            "q1".to_string(),
            collect_work_items(),
            tx1,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx1.try_recv().expect("initial q1");

    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
    let conn2 = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn2,
            "q2".to_string(),
            collect_work_items(),
            tx2,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx2.try_recv().expect("initial q2");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
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
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state.realtime.subs.remove_conn(&db, conn).await;

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
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
        .realtime
        .committers
        .mutate(
            "does_not_exist",
            None,
            insert_project(),
            PrincipalCtx::bypass(),
        )
        .await
        .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

// Fine-grained invalidation plumbing: a committed txn's write_set records the
// specific (table, id) of every written document — not just the table name —
// so point-read subscriptions can later skip re-runs that don't touch their doc.
#[tokio::test]
async fn write_set_records_written_document_ids() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert_a = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    let id_a = insert_a.results[0]["id"]
        .as_str()
        .expect("insert returns id")
        .to_string();

    let patch_a = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "workItems".to_string(),
                    id: id_a.clone(),
                    fields: serde_json::json!({ "status": "in_progress" })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;

    assert!(insert_a.write_set.tables.contains("workItems"));
    assert!(
        insert_a
            .write_set
            .docs
            .contains(&("workItems".to_string(), id_a.clone()))
    );
    assert!(
        patch_a
            .write_set
            .docs
            .contains(&("workItems".to_string(), id_a))
    );
    Ok(())
}

// A get(id) subscription still receives an update when its own document is
// written — the point-read skip must never drop a relevant update.
#[tokio::test]
async fn get_subscription_updates_when_its_doc_is_written() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    let id = insert.results[0]["id"]
        .as_str()
        .expect("insert id")
        .to_string();

    let get_query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "get": id,
    }))
    .expect("parse get query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            get_query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "workItems".to_string(),
                    id: id.clone(),
                    fields: serde_json::json!({ "status": "in_progress" })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;

    let msg = rx
        .try_recv()
        .expect("update after patching the subscribed doc");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert_eq!(result["status"], "in_progress");
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    Ok(())
}

// A get(id) subscription does NOT receive an update when an unrelated document
// on the same table is written. (Regression guard; today's canonical diff would
// also suppress this — the skip additionally avoids the re-run entirely.)
#[tokio::test]
async fn get_subscription_skips_update_for_unrelated_doc() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert_a = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    let id_a = insert_a.results[0]["id"]
        .as_str()
        .expect("insert id")
        .to_string();

    let get_query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "get": id_a,
    }))
    .expect("parse get query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            get_query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    // Write a different document on the same table.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 2.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    Ok(())
}

// A non-get subscription (collect) still re-runs on any write to its table —
// the fine-grained skip is scoped to point reads only.
#[tokio::test]
async fn collect_subscription_still_reruns_on_table_write() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    let msg = rx.try_recv().expect("collect sub re-ran on table write");
    match msg {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(docs_len(&result), 1);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    Ok(())
}

// Deleting the document a get(id) subscription reads must still push the
// (now-absent) result — the point-read skip must never drop a delete of the
// subscribed document. Locks in the soundness property directly.
#[tokio::test]
async fn get_subscription_updates_when_its_doc_is_deleted() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    let id = insert.results[0]["id"]
        .as_str()
        .expect("insert id")
        .to_string();

    let get_query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "get": id,
    }))
    .expect("parse get query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            conn,
            "q1".to_string(),
            get_query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Delete {
                    table: "workItems".to_string(),
                    id: id.clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;

    let msg = rx
        .try_recv()
        .expect("update after deleting the subscribed doc");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert!(result.is_null(), "get of a deleted doc is null");
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    Ok(())
}

// ENH-010: the live subscription inspector. Register subscriptions across two
// dbs with different query shapes (and a user-identity principal on one), then
// assert the registry snapshot lists them with the right db/table/terminal/
// read-set class/principal, the db filter narrows correctly, and a mutate
// increments the per-db re-run counter for the written db only.
#[tokio::test]
async fn inspector_snapshots_subscriptions_and_per_db_counters() -> anyhow::Result<()> {
    let state = test_state().await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    // &str views borrow the TestDb handles, which stay alive to function end so
    // their RAII cleanup (DROP SCHEMA) runs after the test, not now.
    let a = db_a.as_str();
    let b = db_b.as_str();

    // db_a: a plain collect (Table read-set, bypass principal) and an indexed
    // by_status read (Indexed read-set, an interactive-user principal).
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            a,
            next_conn_id(),
            "collect-a".to_string(),
            collect_work_items(),
            tx1,
            PrincipalCtx::bypass(),
        )
        .await?;
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            a,
            next_conn_id(),
            "by-status-a".to_string(),
            work_items_by_status("backlog"),
            tx2,
            PrincipalCtx {
                user_id: Some("user-1".to_string()),
                email: Some("user-1@example.com".to_string()),
                tables: None,
            },
        )
        .await?;
    // db_b: one plain collect (Table read-set, bypass principal).
    let (tx3, _rx3) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            b,
            next_conn_id(),
            "collect-b".to_string(),
            collect_work_items(),
            tx3,
            PrincipalCtx::bypass(),
        )
        .await?;

    // Unfiltered snapshot lists all three, sorted by (db, table).
    let all = state.realtime.subs.snapshot(None).await;
    assert_eq!(all.len(), 3, "three subscriptions across two dbs");

    let collect_a = all
        .iter()
        .find(|s| s.db == a && s.terminal == "collect" && s.read_set_class == "table")
        .expect("db_a collect (Table) subscription present");
    assert!(collect_a.principal.is_none(), "bypass principal is null");

    let by_status_a = all
        .iter()
        .find(|s| s.db == a && s.read_set_class == "indexed")
        .expect("db_a indexed subscription present");
    assert_eq!(by_status_a.terminal, "collect");
    let principal = by_status_a
        .principal
        .as_ref()
        .expect("user principal is surfaced");
    assert_eq!(principal.user_id.as_deref(), Some("user-1"));
    assert_eq!(principal.email.as_deref(), Some("user-1@example.com"));

    assert!(
        all.iter().any(|s| s.db == b && s.read_set_class == "table"),
        "db_b subscription present"
    );

    // db filter narrows to one db's subscriptions.
    let only_a = state.realtime.subs.snapshot(Some(a)).await;
    assert_eq!(only_a.len(), 2);
    assert!(only_a.iter().all(|s| s.db == a));
    let only_b = state.realtime.subs.snapshot(Some(b)).await;
    assert_eq!(only_b.len(), 1);

    // No fan_out has run yet → no per-db counter rows.
    assert!(
        state.runtime.metrics.per_db_subs_snapshot().is_empty(),
        "no counters before any fan_out"
    );

    // Mutating db_a re-runs both of its subscriptions (Table always re-runs;
    // the indexed read's window matches the inserted status). db_b is untouched.
    state
        .realtime
        .committers
        .mutate(
            a,
            None,
            insert_work_item("backlog", 9.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    let per_db = state.runtime.metrics.per_db_subs_snapshot();
    assert_eq!(per_db.len(), 1, "only the written db recorded counters");
    assert_eq!(per_db[0].db, a);
    assert_eq!(per_db[0].reruns, 2, "both db_a subscriptions re-ran");
    assert_eq!(per_db[0].missed, 0);

    // Globals move too: the metrics snapshot's rerun total reflects db_a.
    let snap = state
        .runtime
        .metrics
        .snapshot(
            &state.pool,
            &state.realtime.subs,
            state.runtime.started_at,
            0,
            0,
        )
        .await;
    assert_eq!(snap.subs_reruns_total, 2);
    Ok(())
}
