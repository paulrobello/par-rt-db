mod common;

use std::time::Duration;

use common::{fresh_db, kanban_schema_json, test_state};
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{Order, Query, QueryResult, canonical, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).expect("parse kanban schema")
}

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

async fn insert_project(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    name: &str,
) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: doc(serde_json::json!({
                    "name": name,
                    "description": null,
                    "status": "active",
                    "tags": [],
                    "updatedAt": 1.0
                })),
            }],
        },
    )
    .await?;
    Ok(outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string())
}

async fn insert_work_item(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    project_id: &str,
    status: &str,
    order: f64,
) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "workItems".to_string(),
                doc: doc(serde_json::json!({
                    "projectId": project_id,
                    "title": format!("item {order}"),
                    "status": status,
                    "order": order,
                    "completedAt": null
                })),
            }],
        },
    )
    .await?;
    // Guarantee strictly increasing `created_at` between inserts so
    // (created_at, id) ordering assertions below are deterministic.
    tokio::time::sleep(Duration::from_millis(2)).await;
    Ok(outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string())
}

/// Seeds one project and 5 workItems (2 backlog, 2 in_progress, 1 done; distinct
/// `order` values), in this creation order. Returns `(project_id, [item0..item4])`.
async fn seed_kanban(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
) -> anyhow::Result<(String, Vec<String>)> {
    let project_id = insert_project(pool, db, schema, "Alpha").await?;

    let mut items = Vec::new();
    for (status, order) in [
        ("backlog", 1.0),
        ("in_progress", 2.0),
        ("backlog", 3.0),
        ("done", 4.0),
        ("in_progress", 5.0),
    ] {
        items.push(insert_work_item(pool, db, schema, &project_id, status, order).await?);
    }

    Ok((project_id, items))
}

fn docs_ids(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Docs(docs) => docs
            .iter()
            .map(|d| d["_id"].as_str().expect("_id string").to_string())
            .collect(),
        QueryResult::Doc(_) => panic!("expected Docs variant"),
    }
}

// (a) get by id -> doc with _id/_creationTime/_version.
#[tokio::test]
async fn get_by_id_returns_doc_with_system_fields() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;
    let target = items[0].clone();

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: Some(target.clone()),
            index: None,
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    match result {
        QueryResult::Doc(Some(value)) => {
            assert_eq!(value["_id"], serde_json::json!(target));
            assert!(value["_creationTime"].is_number());
            assert_eq!(value["_version"], serde_json::json!(1));
            assert_eq!(value["projectId"], serde_json::json!(project_id));
            assert_eq!(value["status"], serde_json::json!("backlog"));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }

    Ok(())
}

// (b) get missing -> null.
#[tokio::test]
async fn get_missing_returns_null() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: Some("0".repeat(32)),
            index: None,
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert!(matches!(result, QueryResult::Doc(None)));
    Ok(())
}

// (b2) get combined with index -> BadRequest ("get present with any of
// index/eq/order/take/unique -> BadRequest").
#[tokio::test]
async fn get_combined_with_index_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: Some(items[0].clone()),
            index: Some("by_project".to_string()),
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (b3) unique combined with take -> BadRequest.
#[tokio::test]
async fn unique_combined_with_take_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    insert_project(&pool, &db, &schema, "Alpha").await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "projects".to_string(),
            get: None,
            index: Some("by_name".to_string()),
            eq: vec![serde_json::json!("Alpha")],
            order: None,
            take: Some(10),
            unique: true,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (c) full-eq compound by_project_and_status ["<pid>","backlog"] take 10 -> exactly the 2
// backlog items ordered by created_at asc.
#[tokio::test]
async fn full_eq_compound_index_orders_by_created_at_asc() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            order: None,
            take: Some(10),
            unique: false,
        },
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[0].clone(), items[2].clone()]);
    Ok(())
}

// (d) same query with order: desc -> reversed.
#[tokio::test]
async fn full_eq_compound_index_order_desc_reverses() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            order: Some(Order::Desc),
            take: Some(10),
            unique: false,
        },
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[2].clone(), items[0].clone()]);
    Ok(())
}

// (e) prefix eq ["<pid>"] on the compound index -> all 5, ordered by (status, created_at);
// status groups contiguous and alphabetically ascending (backlog < done < in_progress).
#[tokio::test]
async fn prefix_eq_on_compound_index_sorts_by_remaining_index_field_then_created_at()
-> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id)],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![
            items[0].clone(),
            items[2].clone(),
            items[3].clone(),
            items[1].clone(),
            items[4].clone(),
        ]
    );
    Ok(())
}

// (f) unique on by_name -> 1 doc.
#[tokio::test]
async fn unique_on_by_name_returns_single_doc() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let project_id = insert_project(&pool, &db, &schema, "Alpha").await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "projects".to_string(),
            get: None,
            index: Some("by_name".to_string()),
            eq: vec![serde_json::json!("Alpha")],
            order: None,
            take: None,
            unique: true,
        },
    )
    .await?;

    match result {
        QueryResult::Doc(Some(value)) => {
            assert_eq!(value["_id"], serde_json::json!(project_id));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }
    Ok(())
}

// (g) unique after inserting a duplicate name -> 409 PreconditionFailed.
#[tokio::test]
async fn unique_with_duplicate_name_is_precondition_failed() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    insert_project(&pool, &db, &schema, "Alpha").await?;
    insert_project(&pool, &db, &schema, "Alpha").await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "projects".to_string(),
            get: None,
            index: Some("by_name".to_string()),
            eq: vec![serde_json::json!("Alpha")],
            order: None,
            take: None,
            unique: true,
        },
    )
    .await
    .expect_err("expected precondition failed");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert_eq!(err.message, "unique query matched multiple documents");

    Ok(())
}

// (h) no-index collect on workItems -> 5 docs in created_at order.
#[tokio::test]
async fn no_index_collect_returns_all_docs_in_created_at_order() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: None,
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await?;

    assert_eq!(docs_ids(&result), items);
    Ok(())
}

// (i) take cap: take 5000 -> BadRequest.
#[tokio::test]
async fn take_over_cap_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: None,
            eq: vec![],
            order: None,
            take: Some(5000),
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (j) unknown index -> BadRequest.
#[tokio::test]
async fn unknown_index_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("no_such_index".to_string()),
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (k) eq longer than index fields -> BadRequest.
#[tokio::test]
async fn eq_longer_than_index_fields_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    insert_project(&pool, &db, &schema, "Alpha").await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "projects".to_string(),
            get: None,
            index: Some("by_name".to_string()),
            eq: vec![serde_json::json!("Alpha"), serde_json::json!("extra")],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (l) unknown table -> NotFound.
#[tokio::test]
async fn unknown_table_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "bogus".to_string(),
            get: None,
            index: None,
            eq: vec![],
            order: None,
            take: None,
            unique: false,
        },
    )
    .await
    .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

// (n) C5a: take: 0 -> empty Docs([]), not an error.
#[tokio::test]
async fn take_zero_returns_empty_docs() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: None,
            eq: vec![],
            order: None,
            take: Some(0),
            unique: false,
        },
    )
    .await?;

    match result {
        QueryResult::Docs(docs) => assert!(docs.is_empty()),
        other => panic!("expected Docs variant, got {other:?}"),
    }
    Ok(())
}

// (o) C5b: unique without an index scans the whole table: 0 rows -> null, 1 row -> the
// doc, >1 rows -> PreconditionFailed.
#[tokio::test]
async fn unique_without_index_scans_whole_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let unique_query = Query {
        table: "projects".to_string(),
        get: None,
        index: None,
        eq: vec![],
        order: None,
        take: None,
        unique: true,
    };

    let result = execute_query(&pool, &db, &schema, &unique_query).await?;
    assert!(matches!(result, QueryResult::Doc(None)));

    let project_id = insert_project(&pool, &db, &schema, "Alpha").await?;
    let result = execute_query(&pool, &db, &schema, &unique_query).await?;
    match result {
        QueryResult::Doc(Some(value)) => assert_eq!(value["_id"], serde_json::json!(project_id)),
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }

    insert_project(&pool, &db, &schema, "Beta").await?;
    let err = execute_query(&pool, &db, &schema, &unique_query)
        .await
        .expect_err("expected precondition failed");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);

    Ok(())
}

// (m) canonical() is a stable string form usable for change detection.
#[tokio::test]
async fn canonical_is_stable_for_identical_results() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let query = Query {
        table: "workItems".to_string(),
        get: None,
        index: None,
        eq: vec![],
        order: None,
        take: None,
        unique: false,
    };

    let first = execute_query(&pool, &db, &schema, &query).await?;
    let second = execute_query(&pool, &db, &schema, &query).await?;
    assert_eq!(canonical(&first), canonical(&second));

    Ok(())
}
