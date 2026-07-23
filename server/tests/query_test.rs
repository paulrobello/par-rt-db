mod common;

use std::time::Duration;

use common::{fresh_db, kanban_schema_json, test_state};
use rtdb_server::error::ErrorCode;
use rtdb_server::pagination::encode_cursor;
use rtdb_server::query::{Order, Paginate, Query, QueryResult, canonical, execute_query};
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
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: Some(Paginate {
                cursor: None,
                num_items: 10,
            }),
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: true,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: Some(Order::Desc),
            take: Some(10),
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: true,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(5000),
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: None,
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
            gt: None,
            gte: None,
            lt: None,
            lte: None,
            order: None,
            take: Some(0),
            unique: false,
            first: false,
            count: false,
            paginate: None,
            filter: None,
            search: None,
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
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: true,
        first: false,
        count: false,
        paginate: None,
        filter: None,
        search: None,
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
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        paginate: None,
        filter: None,
        search: None,
    };

    let first = execute_query(&pool, &db, &schema, &query).await?;
    let second = execute_query(&pool, &db, &schema, &query).await?;
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
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
            paginate: None,
            filter: None,
            search: None,
        },
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

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
        paginate: Some(paginate),
        filter: None,
        search: None,
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
        let (docs, next) = paginated(execute_query(&pool, &db, &schema, &q).await?);

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
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);
    Ok(())
}
