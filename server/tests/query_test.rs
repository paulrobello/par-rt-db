use std::sync::Arc;
use std::time::Duration;

use crate::common::{fresh_db, kanban_schema_json, test_state};
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::pagination::encode_cursor;
use rtdb_server::query::{
    AggregateGroup, AggregateOp, AggregateSpec, Order, Paginate, Query, QueryResult, canonical,
    execute_query,
};
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
        &PrincipalCtx::bypass(),
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
        &PrincipalCtx::bypass(),
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
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

fn count_value(result: &QueryResult) -> i64 {
    match result {
        QueryResult::Count(n) => *n,
        other => panic!("expected Count variant, got {other:?}"),
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (b2b) get combined with paginate -> BadRequest.
#[tokio::test]
async fn get_combined_with_paginate_is_bad_request() -> anyhow::Result<()> {
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
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: Some(Paginate {
                cursor: None,
                num_items: 10,
            }),
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: true,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: Some(Order::Desc),
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(5000),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(0),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: true,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    };

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &unique_query,
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert!(matches!(result, QueryResult::Doc(None)));

    let project_id = insert_project(&pool, &db, &schema, "Alpha").await?;
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &unique_query,
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match result {
        QueryResult::Doc(Some(value)) => assert_eq!(value["_id"], serde_json::json!(project_id)),
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }

    insert_project(&pool, &db, &schema, "Beta").await?;
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &unique_query,
        &PrincipalCtx::bypass(),
        false,
    )
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
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    };

    let first = execute_query(&pool, &db, &schema, &query, &PrincipalCtx::bypass(), false).await?;
    let second = execute_query(&pool, &db, &schema, &query, &PrincipalCtx::bypass(), false).await?;
    assert_eq!(canonical(&first), canonical(&second));

    Ok(())
}

// Range queries: gt/gte/lt/lte after the eq prefix.

// (range-a) gt excludes the boundary value; results still sorted by (status, created_at).
#[tokio::test]
async fn range_gt_excludes_boundary_and_sorts_by_bound_field() -> anyhow::Result<()> {
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
            gt: Some(serde_json::json!("backlog")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[3].clone(), items[1].clone(), items[4].clone()]
    );
    Ok(())
}

// (range-b) gte is inclusive: with the minimum status as the bound, every doc matches.
#[tokio::test]
async fn range_gte_includes_boundary() -> anyhow::Result<()> {
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
            gt: None,
            gte: Some(serde_json::json!("backlog")),
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
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

// (range-c) numeric gt+lt bounded range on `order`, combined with the eq prefix.
#[tokio::test]
async fn range_gt_and_lt_bounded_numeric_range() -> anyhow::Result<()> {
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
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[1].clone(), items[2].clone(), items[3].clone()]
    );
    Ok(())
}

// (range-d) same bounded range with order: desc reverses, and take limits further.
#[tokio::test]
async fn range_bounded_numeric_range_with_order_desc_and_take() -> anyhow::Result<()> {
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
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: Some(Order::Desc),
            take: Some(2),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[3].clone(), items[2].clone()]);
    Ok(())
}

// (range-e) range bound with no eq prefix applies directly to the index's first field.
#[tokio::test]
async fn range_without_eq_prefix_applies_to_first_index_field() -> anyhow::Result<()> {
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
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: Some(serde_json::json!("backlog")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[3].clone(), items[1].clone(), items[4].clone()]
    );
    Ok(())
}

// (range-f) gt and gte both set -> BadRequest.
#[tokio::test]
async fn range_gt_and_gte_both_set_is_bad_request() -> anyhow::Result<()> {
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
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: Some(serde_json::json!("backlog")),
            gte: Some(serde_json::json!("backlog")),
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-g) lt and lte both set -> BadRequest.
#[tokio::test]
async fn range_lt_and_lte_both_set_is_bad_request() -> anyhow::Result<()> {
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
            index: Some("by_status".to_string()),
            eq: vec![],
            gt: None,
            gte: None,
            lt: Some(serde_json::json!("in_progress")),
            lte: Some(serde_json::json!("in_progress")),
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-h) range bound without an index -> BadRequest.
#[tokio::test]
async fn range_without_index_is_bad_request() -> anyhow::Result<()> {
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
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-i) eq already consumes every index field -> no remaining field for the range bound.
#[tokio::test]
async fn range_with_no_remaining_index_field_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            gt: Some(serde_json::json!("x")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-j) get combined with a range bound -> BadRequest.
#[tokio::test]
async fn range_combined_with_get_is_bad_request() -> anyhow::Result<()> {
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
            index: None,
            eq: vec![],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-k) a range value of the wrong type for the field is BadRequest, same as eq typing.
#[tokio::test]
async fn range_value_wrong_type_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!("not-a-number")),
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (range-l) lte is inclusive: the boundary value itself is included.
#[tokio::test]
async fn range_lte_includes_boundary_value() -> anyhow::Result<()> {
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
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: None,
            gte: None,
            lt: None,
            lte: Some(serde_json::json!(3.0)),
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[0].clone(), items[1].clone(), items[2].clone()]
    );
    Ok(())
}

// `.first()`: sugar over take(1) returning Doc(Some) or Doc(None) instead of Docs.

// (first-a) no matching docs -> Doc(None).
#[tokio::test]
async fn first_on_no_matching_docs_returns_null() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("blocked")],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert!(matches!(result, QueryResult::Doc(None)));
    Ok(())
}

// (first-b) exactly one matching doc -> Doc(Some(that doc)).
#[tokio::test]
async fn first_with_single_matching_doc_returns_it() -> anyhow::Result<()> {
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
            eq: vec![serde_json::json!(project_id), serde_json::json!("done")],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    match result {
        QueryResult::Doc(Some(value)) => {
            assert_eq!(value["_id"], serde_json::json!(items[3].clone()));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }
    Ok(())
}

// (first-c) combined with a range bound and the default asc order -> the smallest
// matching doc by the bound field.
#[tokio::test]
async fn first_combined_with_range_bound_returns_smallest_in_ascending_order() -> anyhow::Result<()>
{
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
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    match result {
        QueryResult::Doc(Some(value)) => {
            assert_eq!(value["_id"], serde_json::json!(items[1].clone()));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }
    Ok(())
}

// (first-d) same range bound with order: desc -> the largest matching doc instead.
#[tokio::test]
async fn first_combined_with_range_bound_and_order_desc_returns_largest() -> anyhow::Result<()> {
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
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: Some(Order::Desc),
            take: None,
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    match result {
        QueryResult::Doc(Some(value)) => {
            assert_eq!(value["_id"], serde_json::json!(items[3].clone()));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }
    Ok(())
}

// (first-e) first combined with take -> BadRequest.
#[tokio::test]
async fn first_combined_with_take_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (first-f) first combined with unique -> BadRequest.
#[tokio::test]
async fn first_combined_with_unique_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (first-g) first combined with get -> BadRequest.
#[tokio::test]
async fn first_combined_with_get_is_bad_request() -> anyhow::Result<()> {
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
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: true,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// `.count()`: terminal running SELECT COUNT(*) over the same eq/range WHERE clause as every
// other terminal, returning Count(n) instead of Docs/Doc; mutually exclusive with
// get/take/unique/first/order.

// (count-a) empty table -> Count(0).
#[tokio::test]
async fn count_on_empty_table_returns_zero() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(count_value(&result), 0);
    Ok(())
}

// (count-b) filtered subset via an eq index prefix -> count of just the matching rows.
#[tokio::test]
async fn count_with_eq_prefix_counts_matching_subset() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_status".to_string()),
            eq: vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(count_value(&result), 2);
    Ok(())
}

// (count-c) filtered subset via a range bound after the eq prefix -> count of matching rows.
#[tokio::test]
async fn count_with_range_bound_counts_matching_subset() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "workItems".to_string(),
            get: None,
            index: Some("by_project_and_order".to_string()),
            eq: vec![serde_json::json!(project_id)],
            gt: Some(serde_json::json!(1.0)),
            gte: None,
            lt: Some(serde_json::json!(5.0)),
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(count_value(&result), 3);
    Ok(())
}

// (count-d) count combined with order -> BadRequest (a count has no rows to order).
#[tokio::test]
async fn count_combined_with_order_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: Some(Order::Desc),
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (count-e) count combined with take -> BadRequest.
#[tokio::test]
async fn count_combined_with_take_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (count-f) count combined with unique -> BadRequest.
#[tokio::test]
async fn count_combined_with_unique_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (count-g) count combined with first -> BadRequest.
#[tokio::test]
async fn count_combined_with_first_is_bad_request() -> anyhow::Result<()> {
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: true,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (count-h) count combined with get -> BadRequest.
#[tokio::test]
async fn count_combined_with_get_is_bad_request() -> anyhow::Result<()> {
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
            index: None,
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// =============================================================================
// Distinct terminal (`distinct: bool`) — unique values of the index field
// immediately after the eq prefix, over the same eq/range WHERE every other
// terminal builds. Mirrors `count`'s terminal-style tests.
// =============================================================================

fn distinct_values(result: &QueryResult) -> Vec<serde_json::Value> {
    match result {
        QueryResult::Distinct(values) => values.clone(),
        other => panic!("expected Distinct variant, got {other:?}"),
    }
}

/// Base query builder for distinct tests: every field defaulted except the
/// distinct-relevant ones.
fn distinct_query(
    index: Option<&str>,
    eq: Vec<serde_json::Value>,
    range: impl FnOnce(&mut Query),
) -> Query {
    let mut q = Query {
        table: "workItems".to_string(),
        get: None,
        index: index.map(str::to_string),
        eq,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: true,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    };
    range(&mut q);
    q
}

// (distinct-a) unique values of the next index field, sorted ascending.
#[tokio::test]
async fn distinct_returns_unique_values_of_next_index_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status has [projectId, status]; consuming projectId in the
    // eq prefix leaves `status` as the field to distinct on. The 5 seeded items
    // cover backlog, in_progress, done — sorted ascending as JSON strings.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id)],
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        distinct_values(&result),
        vec![
            serde_json::json!("backlog"),
            serde_json::json!("done"),
            serde_json::json!("in_progress"),
        ]
    );
    Ok(())
}

// (distinct-b) distinct over a numeric index field, with the eq prefix
// narrowing to one project. The kanban schema has no 3-field index, so the
// 1-element eq prefix on this 2-field index is the max we can consume while
// still leaving a field to distinct on; this case still demonstrates the
// numeric distinct path (vs. test (a)'s string distinct).
#[tokio::test]
async fn distinct_with_eq_prefix_narrows_distinct_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_order has [projectId, order]; consuming projectId in the
    // eq prefix leaves `order` as the distinct field. Each of the 5 seeded
    // items has a distinct order (1..5), so all 5 surface, ascending.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    // Compare via as_f64 — JSONB numeric representation of an integral float8
    // value is decoded by sqlx as an i64-backed serde_json::Number (canonical
    // JSONB text drops the `.0`), and serde_json::Number equality is variant-
    // strict (i64 ≠ f64 even when numerically equal).
    let orders: Vec<f64> = distinct_values(&result)
        .iter()
        .map(|v| v.as_f64().expect("order is numeric"))
        .collect();
    assert_eq!(orders, vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    Ok(())
}

// (distinct-c) range bounds on the distinct field restrict the matching set.
#[tokio::test]
async fn distinct_with_range_bound_restricts_matching_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_order with eq=[projectId] leaves `order` as the distinct
    // field; gt 1 / lt 5 narrow the matching set to orders 2, 3, 4.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            |q| {
                q.gt = Some(serde_json::json!(1.0));
                q.lt = Some(serde_json::json!(5.0));
            },
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    // Compare via as_f64; see test (b) for the JSONB-numeric rationale.
    let orders: Vec<f64> = distinct_values(&result)
        .iter()
        .map(|v| v.as_f64().expect("order is numeric"))
        .collect();
    assert_eq!(orders, vec![2.0, 3.0, 4.0]);
    Ok(())
}

// (distinct-d) honors MAX_TAKE: more distinct values than the cap yield at most
// MAX_TAKE rows (the SQL LIMIT).
#[tokio::test]
async fn distinct_capped_by_max_take() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let project_id = insert_project(&pool, &db, &schema, "Alpha").await?;
    // Insert MAX_TAKE + 10 distinct orders under one project; the by_project_and_order
    // index has order as the post-prefix field, so distinct yields one row per order.
    for i in 0..(4096 + 10) {
        let order = i as f64;
        insert_work_item(&pool, &db, &schema, &project_id, "backlog", order).await?;
    }

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    let values = distinct_values(&result);
    assert_eq!(values.len(), 4096);
    Ok(())
}

// (distinct-e) no index → BadRequest.
#[tokio::test]
async fn distinct_without_index_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(None, vec![], |_| {}),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (distinct-f) eq prefix consumes every index field (no field to distinct on)
// → BadRequest.
#[tokio::test]
async fn distinct_with_no_field_beyond_eq_prefix_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status has exactly two fields; consuming both leaves none.
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &distinct_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (distinct-g) distinct combined with count → BadRequest.
#[tokio::test]
async fn distinct_combined_with_count_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let mut q = distinct_query(Some("by_status"), vec![], |_| {});
    q.count = true;
    let err = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (distinct-h) distinct combined with take → BadRequest.
#[tokio::test]
async fn distinct_combined_with_take_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let mut q = distinct_query(Some("by_status"), vec![], |_| {});
    q.take = Some(10);
    let err = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// =============================================================================
// Aggregate terminal (`aggregate: {op, groupBy?}`) — SUM/AVG/MIN/MAX over the
// index field after the eq prefix, optionally grouped by that field.
// =============================================================================

/// Aggregate spec without groupBy (most common shape in the tests below).
fn agg(op: AggregateOp) -> AggregateSpec {
    AggregateSpec {
        op,
        group_by: false,
    }
}

/// Base query builder for aggregate tests: every field defaulted except the
/// aggregate-relevant ones. `eq` consumes a prefix of the named index; the
/// aggregate runs over the index field immediately after that prefix (or, with
/// `groupBy`, groups by that field and aggregates the one after it).
fn aggregate_query(
    index: Option<&str>,
    eq: Vec<serde_json::Value>,
    spec: AggregateSpec,
    range: impl FnOnce(&mut Query),
) -> Query {
    let mut q = Query {
        table: "workItems".to_string(),
        get: None,
        index: index.map(str::to_string),
        eq,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        aggregate: Some(spec),
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
    };
    range(&mut q);
    q
}

/// Pulls the scalar out of an `Aggregate` result, panicking on any other variant.
fn aggregate_scalar(result: &QueryResult) -> serde_json::Value {
    match result {
        QueryResult::Aggregate(v) => v.clone(),
        other => panic!("expected Aggregate variant, got {other:?}"),
    }
}

/// Pulls the `{key, value}` rows out of an `AggregateGroups` result.
fn aggregate_groups(result: &QueryResult) -> Vec<AggregateGroup> {
    match result {
        QueryResult::AggregateGroups(groups) => groups.clone(),
        other => panic!("expected AggregateGroups variant, got {other:?}"),
    }
}

// (agg-a) SUM of the next index field over the matching set.
#[tokio::test]
async fn aggregate_sum_over_matching_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_order has [projectId, order]; consuming projectId in the eq
    // prefix leaves `order` as the aggregate field. Seeded orders are 1+2+3+4+5 = 15.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Sum),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(aggregate_scalar(&result).as_f64(), Some(15.0));
    Ok(())
}

// (agg-b) AVG of the next index field over the matching set.
#[tokio::test]
async fn aggregate_avg_over_matching_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Avg),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    // AVG of orders 1..=5 = 3.0.
    assert_eq!(aggregate_scalar(&result).as_f64(), Some(3.0));
    Ok(())
}

// (agg-c) MIN and MAX over the matching set (numeric order field).
#[tokio::test]
async fn aggregate_min_max_over_matching_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    let min = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Min),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(aggregate_scalar(&min).as_f64(), Some(1.0));

    let max = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Max),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(aggregate_scalar(&max).as_f64(), Some(5.0));
    Ok(())
}

// (agg-d) range bound on the aggregate field narrows the matching set.
#[tokio::test]
async fn aggregate_respects_range_bound() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // gt 1 / lt 5 narrows the matching set to orders 2, 3, 4 → SUM = 9.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Sum),
            |q| {
                q.gt = Some(serde_json::json!(1.0));
                q.lt = Some(serde_json::json!(5.0));
            },
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(aggregate_scalar(&result).as_f64(), Some(9.0));
    Ok(())
}

// (agg-e) MIN over a string index field (status) — Min/Max work on any orderable field.
#[tokio::test]
async fn aggregate_min_over_string_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status: [projectId, status]. Seeded statuses are
    // {backlog, in_progress, done}; MIN lexicographically is "backlog".
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Min),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(aggregate_scalar(&result), serde_json::json!("backlog"));
    Ok(())
}

// (agg-f) SUM on a non-numeric index field → BadRequest.
#[tokio::test]
async fn aggregate_sum_on_non_numeric_field_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status's post-prefix field is `status` (string); SUM/AVG
    // require a numeric field.
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Sum),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("requires a numeric index field"),
        "unexpected message: {}",
        err.message
    );
    Ok(())
}

// (agg-g) no index → BadRequest.
#[tokio::test]
async fn aggregate_without_index_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(None, vec![], agg(AggregateOp::Sum), |_| {}),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// (agg-h) eq prefix consumes every index field (no field to aggregate over)
// → BadRequest.
#[tokio::test]
async fn aggregate_with_no_field_beyond_eq_prefix_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status has exactly two fields; consuming both leaves none.
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            agg(AggregateOp::Sum),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// (agg-i) NULL result when no rows match (SUM/AVG/MIN/MAX over zero rows is SQL NULL).
#[tokio::test]
async fn aggregate_over_empty_matching_set_returns_null() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // eq prefix narrows to a project that has no work items (32-hex id never seeded).
    let unused_project_id = "f".repeat(32);
    let _ = project_id; // suppress unused warning in case the binding is dropped
    let err_or_value = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(unused_project_id)],
            agg(AggregateOp::Sum),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await;
    let result = err_or_value?;
    assert_eq!(aggregate_scalar(&result), serde_json::Value::Null);
    Ok(())
}

// (agg-j) aggregate combined with take → BadRequest (matrix: take is incompatible).
#[tokio::test]
async fn aggregate_combined_with_take_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let mut q = aggregate_query(
        Some("by_project_and_order"),
        vec![serde_json::json!("0".repeat(32))],
        agg(AggregateOp::Sum),
        |_| {},
    );
    q.take = Some(10);
    let err = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// (agg-k) groupBy: groups by `status`, sums `order` over a 3-field index.
#[tokio::test]
async fn aggregate_group_by_returns_one_row_per_group() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_status_order has [projectId, status, order]; consuming projectId
    // in the eq prefix leaves `status` as the group key and `order` as the
    // aggregate field. Seeded orders by status:
    //   backlog     -> 1 + 3 = 4
    //   in_progress -> 2 + 5 = 7
    //   done        -> 4
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_status_order"),
            vec![serde_json::json!(project_id)],
            AggregateSpec {
                op: AggregateOp::Sum,
                group_by: true,
            },
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    let groups = aggregate_groups(&result);
    // Ordered by group key ascending — backlog, done, in_progress.
    let pairs: Vec<(String, f64)> = groups
        .iter()
        .map(|g| {
            (
                g.key.as_str().expect("status string").to_string(),
                g.value.as_f64().expect("sum is numeric"),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("backlog".to_string(), 4.0),
            ("done".to_string(), 4.0),
            ("in_progress".to_string(), 7.0),
        ]
    );
    Ok(())
}

// (agg-l) groupBy requires two fields beyond the eq prefix → BadRequest otherwise.
#[tokio::test]
async fn aggregate_group_by_with_one_field_beyond_prefix_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status has exactly two fields; consuming one leaves one —
    // groupBy needs two (one to group by, one to aggregate).
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id)],
            AggregateSpec {
                op: AggregateOp::Sum,
                group_by: true,
            },
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("requires two index fields beyond the eq prefix"),
        "unexpected message: {}",
        err.message
    );
    Ok(())
}

// (agg-count-a) scalar COUNT over the matching set (no aggregate field consumed).
#[tokio::test]
async fn aggregate_count_over_matching_set() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // 5 work items seeded for this project; count consumes no aggregate field,
    // so eq consuming `projectId` (index by_project_and_order) leaves nothing to
    // aggregate over — fine for count, which counts rows.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(project_id)],
            agg(AggregateOp::Count),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(aggregate_scalar(&result).as_f64(), Some(5.0));
    Ok(())
}

// (agg-count-b) COUNT over an empty matching set is 0 (COUNT(*) never yields NULL,
// unlike SUM/MIN/MAX).
#[tokio::test]
async fn aggregate_count_over_empty_set_returns_zero() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // A 32-hex project id that was never seeded matches nothing.
    let unused_project_id = "f".repeat(32);
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_order"),
            vec![serde_json::json!(unused_project_id)],
            agg(AggregateOp::Count),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(aggregate_scalar(&result).as_f64(), Some(0.0));
    Ok(())
}

// (agg-count-c) grouped COUNT: count per status within a project. This is the
// dashboard "items by status" use case that previously needed a sum-over-1 workaround.
#[tokio::test]
async fn aggregate_count_grouped_by_status() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_status_order is [projectId, status, order]; eq consumes
    // projectId, leaving `status` as the group key. count consumes no aggregate
    // field, so the `order` field is unused. Seeded counts:
    //   backlog=2, in_progress=2, done=1.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_status_order"),
            vec![serde_json::json!(project_id)],
            AggregateSpec {
                op: AggregateOp::Count,
                group_by: true,
            },
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    let groups = aggregate_groups(&result);
    // Ordered by group key ascending — backlog, done, in_progress.
    let pairs: Vec<(String, f64)> = groups
        .iter()
        .map(|g| {
            (
                g.key.as_str().expect("status string").to_string(),
                g.value.as_f64().expect("count is numeric"),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![
            ("backlog".to_string(), 2.0),
            ("done".to_string(), 1.0),
            ("in_progress".to_string(), 2.0),
        ]
    );
    Ok(())
}

// (agg-count-d) count needs no aggregate field beyond the eq prefix — eq may
// consume every index field. (Sum/Min/Max would BadRequest here; count does not.)
#[tokio::test]
async fn aggregate_count_with_no_field_beyond_eq_prefix_works() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _items) = seed_kanban(&pool, &db, &schema).await?;

    // by_project_and_status is [projectId, status]; consuming both leaves none.
    // Two seeded backlog items for this project.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &aggregate_query(
            Some("by_project_and_status"),
            vec![serde_json::json!(project_id), serde_json::json!("backlog")],
            agg(AggregateOp::Count),
            |_| {},
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(aggregate_scalar(&result).as_f64(), Some(2.0));
    Ok(())
}

// =============================================================================
// Cursor-based pagination (`paginate` terminal).
// =============================================================================

/// Base query builder for paginate tests: a `workItems` query with everything
/// defaulted and only the paginate-relevant fields overridable.
fn paginate_query(
    index: Option<&str>,
    eq: Vec<serde_json::Value>,
    order: Option<Order>,
    paginate: Paginate,
) -> Query {
    Query {
        table: "workItems".to_string(),
        get: None,
        index: index.map(str::to_string),
        eq,
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: Some(paginate),
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    }
}

fn paginated(result: QueryResult) -> (Vec<serde_json::Value>, Option<String>) {
    match result {
        QueryResult::Paginated(pr) => (pr.docs, pr.next_cursor),
        other => panic!("expected Paginated variant, got {other:?}"),
    }
}

fn ids_of(docs: &[serde_json::Value]) -> Vec<String> {
    docs.iter()
        .map(|d| d["_id"].as_str().expect("_id string").to_string())
        .collect()
}

// (p1) First page without a cursor: returns the first `num_items` docs by
// (created_at, id) ascending and a non-null cursor when more rows exist.
#[tokio::test]
async fn paginate_first_page_no_index() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let (docs, next_cursor) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                None,
                vec![],
                None,
                Paginate {
                    cursor: None,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );

    assert_eq!(ids_of(&docs), vec![items[0].clone(), items[1].clone()]);
    assert!(next_cursor.is_some());
    Ok(())
}

// (p2) Walking the cursor across all pages returns every doc exactly once, in
// (created_at, id) order, and terminates with a null cursor.
#[tokio::test]
async fn paginate_walks_all_pages_without_gaps_or_dupes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let (docs, next) = paginated(
            execute_query(
                &pool,
                &db,
                &schema,
                &paginate_query(
                    None,
                    vec![],
                    None,
                    Paginate {
                        cursor,
                        num_items: 2,
                    },
                ),
                &PrincipalCtx::bypass(),
                false,
            )
            .await?,
        );
        seen.extend(ids_of(&docs));
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    assert_eq!(seen, items);
    Ok(())
}

// (p3) The last page carries no next cursor. 5 items / num_items 2 -> last page
// has 1 doc and a null cursor.
#[tokio::test]
async fn paginate_last_page_has_no_cursor() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let mut cursor: Option<String> = None;
    let mut last_docs: Vec<serde_json::Value>;
    loop {
        let (docs, next) = paginated(
            execute_query(
                &pool,
                &db,
                &schema,
                &paginate_query(
                    None,
                    vec![],
                    None,
                    Paginate {
                        cursor,
                        num_items: 2,
                    },
                ),
                &PrincipalCtx::bypass(),
                false,
            )
            .await?,
        );
        last_docs = docs;
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    assert_eq!(ids_of(&last_docs), vec![items[4].clone()]);
    Ok(())
}

// (p4) paginate honors an index + eq prefix: page is scoped to the eq matches
// and ordered by (unbound index fields, created_at, id).
#[tokio::test]
async fn paginate_with_index_and_eq_prefix() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project"),
                vec![serde_json::json!(project_id)],
                None,
                Paginate {
                    cursor: None,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), vec![items[0].clone(), items[1].clone()]);
    assert!(next.is_some());

    let (docs2, next2) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project"),
                vec![serde_json::json!(project_id)],
                None,
                Paginate {
                    cursor: next,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs2), vec![items[2].clone(), items[3].clone()]);
    assert!(next2.is_some());
    Ok(())
}

// (p5) DESC order paginates in reverse: first page is the two newest docs.
#[tokio::test]
async fn paginate_desc_reverses_order() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                None,
                vec![],
                Some(Order::Desc),
                Paginate {
                    cursor: None,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), vec![items[4].clone(), items[3].clone()]);
    assert!(next.is_some());

    let (docs2, _) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                None,
                vec![],
                Some(Order::Desc),
                Paginate {
                    cursor: next,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs2), vec![items[2].clone(), items[1].clone()]);
    Ok(())
}

// (p6) Compound index with a 2-field eq prefix leaves no unbound index field,
// so the keyset runs over (created_at, id) only and still resumes correctly.
#[tokio::test]
async fn paginate_compound_index_eq_consumes_all_fields() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;
    // backlog items are items[0] and items[2].
    let backlog_eq = vec![serde_json::json!(project_id), serde_json::json!("backlog")];

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project_and_status"),
                backlog_eq.clone(),
                None,
                Paginate {
                    cursor: None,
                    num_items: 1,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), vec![items[0].clone()]);
    assert!(next.is_some());

    let (docs2, next2) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project_and_status"),
                backlog_eq.clone(),
                None,
                Paginate {
                    cursor: next,
                    num_items: 1,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs2), vec![items[2].clone()]);
    assert!(next2.is_none());
    Ok(())
}

// (p7) A num_items larger than the matching set returns everything and no cursor.
#[tokio::test]
async fn paginate_num_items_exceeds_total() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                None,
                vec![],
                None,
                Paginate {
                    cursor: None,
                    num_items: 100,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), items);
    assert!(next.is_none());
    Ok(())
}

// (p8) num_items above MAX_TAKE is silently capped (per the brief) rather than
// rejected like `take` is.
#[tokio::test]
async fn paginate_caps_num_items_at_max_take() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                None,
                vec![],
                None,
                Paginate {
                    cursor: None,
                    num_items: 100_000,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), items);
    assert!(next.is_none());
    Ok(())
}

// (p9) A cursor whose arity does not match the sort columns is a BadRequest.
#[tokio::test]
async fn paginate_bad_cursor_arity_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    // No index => sort cols are (created_at, id) => 2 values. Send 3.
    let bad = encode_cursor(&[
        serde_json::json!(0i64),
        serde_json::json!("id"),
        serde_json::json!("extra"),
    ])?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &paginate_query(
            None,
            vec![],
            None,
            Paginate {
                cursor: Some(bad),
                num_items: 10,
            },
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// (p10) A cursor that is not valid base64/JSON is a BadRequest.
#[tokio::test]
async fn paginate_garbage_cursor_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &paginate_query(
            None,
            vec![],
            None,
            Paginate {
                cursor: Some("!!!not-base64!!!".to_string()),
                num_items: 10,
            },
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// (p11) A non-system sort column (the unbound `order` field of a compound
// index) round-trips through the cursor: the keyset resumes correctly across
// pages ordered by (order, created_at, id).
#[tokio::test]
async fn paginate_index_field_value_round_trips_in_cursor() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;
    // by_project_and_order leaves `order` unbound after the eq prefix; sort
    // cols are (order, created_at, id). All 5 workItems have distinct `order`
    // values 1.0..5.0, so ascending order is items[0..4].
    let eq = vec![serde_json::json!(project_id)];

    let (docs, next) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project_and_order"),
                eq.clone(),
                None,
                Paginate {
                    cursor: None,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs), vec![items[0].clone(), items[1].clone()]);
    assert!(next.is_some());

    let (docs2, next2) = paginated(
        execute_query(
            &pool,
            &db,
            &schema,
            &paginate_query(
                Some("by_project_and_order"),
                eq.clone(),
                None,
                Paginate {
                    cursor: next,
                    num_items: 2,
                },
            ),
            &PrincipalCtx::bypass(),
            false,
        )
        .await?,
    );
    assert_eq!(ids_of(&docs2), vec![items[2].clone(), items[3].clone()]);
    assert!(next2.is_some());
    Ok(())
}

// (p12) Paginate composed with a `gte` range bound: the cursor bind-offset
// math must account for the range bind — cursor binds start AFTER eq + range
// (`cursor_start = eq_len + range_binds.len() + 1`). Walks every page of
// `by_project_and_order` with `order >= 2.0` and asserts every returned row
// stays in range, in ascending (order, created_at, id) order, with no gaps or
// duplicates — and that the below-range row (order 1.0) never appears.
#[tokio::test]
async fn paginate_composes_with_gte_range_bound_across_pages() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;
    // gte 2.0 drops items[0] (order 1.0); in-range rows are items[1..4] with
    // distinct orders 2.0..5.0. num_items 2 forces two pages so the cursor
    // resume predicate and the range bound are both applied on page 2.
    let eq = vec![serde_json::json!(project_id)];

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut q = paginate_query(
            Some("by_project_and_order"),
            eq.clone(),
            None,
            Paginate {
                cursor,
                num_items: 2,
            },
        );
        q.gte = Some(serde_json::json!(2.0));
        let (docs, next) = paginated(
            execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false).await?,
        );

        for d in &docs {
            let order = d["order"].as_f64().expect("order is a number");
            assert!(
                order >= 2.0,
                "row below gte lower bound returned: order={order}"
            );
        }
        seen.extend(ids_of(&docs));
        match next {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }

    // Every in-range row returned exactly once, ascending, no gaps or dupes;
    // items[0] (order 1.0) is excluded by the range bound on every page.
    assert_eq!(
        seen,
        vec![
            items[1].clone(),
            items[2].clone(),
            items[3].clone(),
            items[4].clone(),
        ]
    );
    Ok(())
}

// === db-side filter() expressions ===
//
// `seed_kanban` orders workItems by strictly-increasing created_at, so the
// (created_at, id) tiebreak ordering is the insert order items[0..4]:
//   items[0] backlog   order 1   "item 1"
//   items[1] in_progress order 2 "item 2"
//   items[2] backlog   order 3   "item 3"
//   items[3] done      order 4   "item 4"
//   items[4] in_progress order 5 "item 5"

/// Parses a Query from JSON so filter tests also exercise the wire shape.
fn filter_query(json: serde_json::Value) -> Query {
    serde_json::from_value(json).expect("parse filter query")
}

// filter eq on a declared-but-not-indexed field (title) -> jsonb extraction path.
#[tokio::test]
async fn filter_eq_on_jsonb_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "eq", "field": "title", "value": "item 3"}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[2].clone()]);
    Ok(())
}

// filter range (gte) on a non-indexed numeric field -> jsonb `(doc->>'f')::float8` path.
#[tokio::test]
async fn filter_range_on_jsonb_numeric_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let project_id = insert_project(&pool, &db, &schema, "Alpha").await?;
    // completedAt is optional<number> and not indexed; insert three with
    // distinct values so a jsonb range selects a clear subset.
    let mut inserted = Vec::new();
    for (title, completed) in [("c10", 10.0_f64), ("c20", 20.0), ("c30", 30.0)] {
        let outcome = execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "workItems".to_string(),
                    doc: doc(serde_json::json!({
                        "projectId": project_id,
                        "title": title,
                        "status": "backlog",
                        "order": completed,
                        "completedAt": completed
                    })),
                }],
            },
            &PrincipalCtx::bypass(),
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(2)).await;
        inserted.push(
            outcome.results[0]["id"]
                .as_str()
                .expect("id string")
                .to_string(),
        );
    }

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "gte", "field": "completedAt", "value": 20}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![inserted[1].clone(), inserted[2].clone()]
    );
    Ok(())
}

// filter eq on an indexed field (status) -> typed column path.
#[tokio::test]
async fn filter_eq_on_typed_indexed_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "eq", "field": "status", "value": "backlog"}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[0].clone(), items[2].clone()]);
    Ok(())
}

// filter range (gt) on an indexed numeric field (order) -> typed column path.
#[tokio::test]
async fn filter_range_on_typed_indexed_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "gt", "field": "order", "value": 3}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[3].clone(), items[4].clone()]);
    Ok(())
}

// filter composes with order + take: orders > 2 (3 rows), desc, take 2.
#[tokio::test]
async fn filter_composes_with_order_and_take() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "gt", "field": "order", "value": 2},
            "order": "desc",
            "take": 2
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[4].clone(), items[3].clone()]);
    Ok(())
}

// `in` on an indexed field selects a union of values.
#[tokio::test]
async fn filter_in_on_indexed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "in", "field": "status", "values": ["backlog", "done"]}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(
        docs_ids(&result),
        vec![items[0].clone(), items[2].clone(), items[3].clone()]
    );
    Ok(())
}

// `and` combinator nests two conditions; `or` is exercised the same way.
#[tokio::test]
async fn filter_and_combinator() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {
                "op": "and",
                "exprs": [
                    {"op": "eq", "field": "status", "value": "in_progress"},
                    {"op": "gt", "field": "order", "value": 2}
                ]
            }
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[4].clone()]);
    Ok(())
}

// filter also composes with an index eq prefix.
#[tokio::test]
async fn filter_composes_with_index_eq() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "index": "by_project",
            "eq": [project_id],
            "filter": {"op": "eq", "field": "status", "value": "in_progress"}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;

    assert_eq!(docs_ids(&result), vec![items[1].clone(), items[4].clone()]);
    Ok(())
}

// malformed: unknown field -> BadRequest, never a 500 / raw SQL error.
#[tokio::test]
async fn filter_unknown_field_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "eq", "field": "bogus", "value": 1}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// malformed: get + filter -> BadRequest.
#[tokio::test]
async fn filter_with_get_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (_project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "get": items[0],
            "filter": {"op": "eq", "field": "status", "value": "backlog"}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// malformed: a wrong-typed value against an indexed column -> BadRequest.
#[tokio::test]
async fn filter_wrong_typed_value_on_indexed_column_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &filter_query(serde_json::json!({
            "table": "workItems",
            "filter": {"op": "eq", "field": "order", "value": "not a number"}
        })),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}

// ---------------------------------------------------------------------------
// int64 indexable as bigint — mirrors the Number→double precision path.
// Deviation from the task brief: the brief wrote `let db = fresh_db(&state).await`,
// but `fresh_db` always pushes the kanban schema, and a subsequent `push_schema`
// of the int64-only schema would be rejected as "removed table 'projects'".
// Instead we mirror `search_test.rs`'s `fresh_search_db`: create the DB and
// push the int64 schema directly.
// ---------------------------------------------------------------------------

fn int64_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({
        "tables": {
            "events": {
                "fields": {
                    "ts": { "type": "int64" },
                    "kind": { "type": "string" }
                },
                "indexes": [{ "name": "by_ts", "fields": ["ts"] }]
            }
        }
    }))
    .expect("parse int64 schema")
}

async fn fresh_int64_db(state: &Arc<AppState>) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    ddl::push_schema(&state.pool, &name, int64_schema())
        .await
        .expect("push int64 schema");
    crate::common::wrap_test_db(name)
}

async fn insert_event(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    ts: &str,
    kind: &str,
) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "events".to_string(),
                doc: doc(serde_json::json!({ "ts": ts, "kind": kind })),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    Ok(outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string())
}

fn docs_kinds(result: QueryResult) -> Vec<String> {
    match result {
        QueryResult::Docs(docs) => docs
            .iter()
            .map(|d| d["kind"].as_str().expect("kind").to_string())
            .collect(),
        _ => panic!("expected Docs, got {result:?}"),
    }
}

#[tokio::test]
async fn int64_index_range_and_eq_compare_numerically() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_int64_db(&state).await;
    let schema = int64_schema();
    insert_event(&pool, &db, &schema, "100", "a").await?;
    insert_event(&pool, &db, &schema, "20", "b").await?;
    insert_event(&pool, &db, &schema, "3", "c").await?;

    // Numeric range [20, +inf) asc -> ["b" (20), "a" (100)] — NOT lexicographic
    // ("100" would sort before "20" as strings).
    let r = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: Some(serde_json::json!("20")),
            lt: None,
            lte: None,
            order: Some(Order::Asc),
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(docs_kinds(r), vec!["b".to_string(), "a".to_string()]);

    // eq on the int64 field matches the decimal-string value.
    let r = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![serde_json::json!("100")],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(docs_kinds(r), vec!["a".to_string()]);
    Ok(())
}

#[tokio::test]
async fn int64_index_count_and_aggregate() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_int64_db(&state).await;
    let schema = int64_schema();
    insert_event(&pool, &db, &schema, "10", "a").await?;
    insert_event(&pool, &db, &schema, "20", "b").await?;
    insert_event(&pool, &db, &schema, "30", "c").await?;

    let r = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: true,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: None,
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert!(matches!(r, QueryResult::Count(3)));

    let r = execute_query(
        &pool,
        &db,
        &schema,
        &Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            distinct: false,
            paginate: None,
            filter: None,
            search: None,
            vector_search: None,
            hybrid_search: None,
            fields: None,
            aggregate: Some(AggregateSpec {
                op: AggregateOp::Sum,
                group_by: false,
            }),
        },
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    // SUM(bigint) projects via to_jsonb -> JSON number.
    assert!(matches!(r, QueryResult::Aggregate(ref v) if v.as_f64() == Some(60.0)));
    Ok(())
}

// =====================================================================
// Field projection (Query.fields)
// =====================================================================

/// Builds a query via the wire shape (the same serde path clients exercise)
/// with only the projection-relevant pieces set.
fn projection_query(table: &str, extra: serde_json::Value, fields: &[&str]) -> Query {
    let mut value = serde_json::json!({"table": table});
    let obj = value.as_object_mut().expect("object");
    if let Some(extra_obj) = extra.as_object() {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }
    obj.insert(
        "fields".to_string(),
        serde_json::Value::Array(fields.iter().map(|f| serde_json::json!(f)).collect()),
    );
    serde_json::from_value(value).expect("parse projection query")
}

/// Sorted key set of a result doc, for exact projected-shape assertions.
fn sorted_doc_keys(doc: &serde_json::Value) -> Vec<&str> {
    let mut keys: Vec<&str> = doc
        .as_object()
        .expect("doc object")
        .keys()
        .map(|k| k.as_str())
        .collect();
    keys.sort_unstable();
    keys
}

// (a) collect + fields: docs carry exactly the system fields + the listed
// user fields; every unlisted user field is dropped.
#[tokio::test]
async fn projection_collect_subsets_user_fields_and_keeps_system() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query("workItems", serde_json::json!({}), &["title", "status"]),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match result {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 5);
            for doc in &docs {
                assert_eq!(
                    sorted_doc_keys(doc),
                    vec!["_creationTime", "_id", "_version", "status", "title"]
                );
                assert!(
                    doc["title"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("item "))
                );
                assert!(doc["status"].is_string());
            }
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
    Ok(())
}

// (b) fields: [] is meaningful — system fields only (an ids-only view).
#[tokio::test]
async fn projection_empty_fields_list_is_system_fields_only() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let result = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query("workItems", serde_json::json!({}), &[]),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match result {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 5);
            for doc in &docs {
                assert_eq!(
                    sorted_doc_keys(doc),
                    vec!["_creationTime", "_id", "_version"]
                );
            }
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
    Ok(())
}

// (c) projection composes with get and paginate: point reads and every page
// of a pagination carry the projected shape, and cursors still work.
#[tokio::test]
async fn projection_composes_with_get_and_paginate() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, items) = seed_kanban(&pool, &db, &schema).await?;

    // get: Doc(Some) carries only system fields + title.
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({"get": items[0]}),
            &["title"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match result {
        QueryResult::Doc(Some(doc)) => {
            assert_eq!(
                sorted_doc_keys(&doc),
                vec!["_creationTime", "_id", "_version", "title"]
            );
            assert_eq!(doc["_id"], serde_json::json!(items[0]));
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }

    // paginate page 1: projected docs + a next cursor minted from the
    // unprojected row (cursor building runs inside the terminal, before
    // projection).
    let page1 = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({"index": "by_project", "eq": [project_id], "paginate": {"numItems": 2}}),
            &["status"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    let cursor = match &page1 {
        QueryResult::Paginated(page) => {
            assert_eq!(page.docs.len(), 2);
            for doc in &page.docs {
                assert_eq!(
                    sorted_doc_keys(doc),
                    vec!["_creationTime", "_id", "_version", "status"]
                );
            }
            page.next_cursor.clone().expect("page 1 has a next cursor")
        }
        other => panic!("expected Paginated variant, got {other:?}"),
    };

    // Page 2 follows that cursor and is projected too.
    let page2 = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({"index": "by_project", "eq": [project_id], "paginate": {"numItems": 2, "cursor": cursor}}),
            &["status"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    match page2 {
        QueryResult::Paginated(page) => {
            assert_eq!(page.docs.len(), 2);
            for doc in &page.docs {
                assert_eq!(
                    sorted_doc_keys(doc),
                    vec!["_creationTime", "_id", "_version", "status"]
                );
            }
        }
        other => panic!("expected Paginated variant, got {other:?}"),
    }
    Ok(())
}

// (d) unknown projection field -> BadRequest; listing the system fields
// explicitly is an accepted no-op.
#[tokio::test]
async fn projection_unknown_field_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    seed_kanban(&pool, &db, &schema).await?;

    let err = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query("workItems", serde_json::json!({}), &["title", "bogus"]),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("unknown projection field must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);

    // The system fields may be listed explicitly (always kept anyway).
    let result = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({}),
            &["_id", "_creationTime", "_version", "title"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert!(matches!(result, QueryResult::Docs(ref docs) if docs.len() == 5));
    Ok(())
}

// (e) doc-less terminals are unaffected by construction: count still counts,
// aggregate still aggregates — projection neither errors nor changes them.
#[tokio::test]
async fn projection_doc_less_terminals_unaffected() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let (project_id, _) = seed_kanban(&pool, &db, &schema).await?;

    let count = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({"index": "by_status", "eq": ["backlog"], "count": true}),
            &["title"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert_eq!(count_value(&count), 2);

    // order values 1..=5 sum to 15.
    let agg = execute_query(
        &pool,
        &db,
        &schema,
        &projection_query(
            "workItems",
            serde_json::json!({"index": "by_project_and_order", "eq": [project_id], "aggregate": {"op": "sum"}}),
            &["title"],
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await?;
    assert!(matches!(agg, QueryResult::Aggregate(ref v) if v.as_f64() == Some(15.0)));
    Ok(())
}
