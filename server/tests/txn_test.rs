mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::{fresh_db, kanban_schema_json, test_state};
use rtdb_server::AppState;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::schema::SchemaDef;
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

// (a) insert row + typed columns populated.
#[tokio::test]
async fn insert_populates_typed_columns() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;

    assert_eq!(
        outcome.write_set.tables,
        BTreeSet::from(["projects".to_string()])
    );
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let pg_schema = format!("db_{db}");
    let row: (String, String, i64) = sqlx::query_as(&format!(
        "SELECT \"f_name\", \"f_status\", \"created_at\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "Alpha");
    assert_eq!(row.1, "active");
    assert!(row.2 > 0);

    Ok(())
}

// (b) patch merges + bumps version + updates f_status.
#[tokio::test]
async fn patch_merges_bumps_version_and_updates_indexed_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let mut fields = serde_json::Map::new();
    fields.insert("status".to_string(), serde_json::json!("paused"));
    fields.insert("updatedAt".to_string(), serde_json::json!(2.0));

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "projects".to_string(),
                id: id.clone(),
                fields,
            }],
        },
        None,
    )
    .await?;
    assert_eq!(outcome.results, vec![serde_json::Value::Null]);

    let pg_schema = format!("db_{db}");
    let row: (String, i64) = sqlx::query_as(&format!(
        "SELECT \"f_status\", \"version\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "paused");
    assert_eq!(row.1, 2);

    Ok(())
}

// (c) patch null clears optional completedAt (not an indexed field: check `doc` jsonb only).
#[tokio::test]
async fn patch_null_clears_optional_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "workItems".to_string(),
                doc: doc(serde_json::json!({
                    "projectId": "0".repeat(32),
                    "title": "Do it",
                    "status": "backlog",
                    "order": 1.0,
                    "completedAt": 5.0
                })),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let mut fields = serde_json::Map::new();
    fields.insert("completedAt".to_string(), serde_json::Value::Null);

    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "workItems".to_string(),
                id: id.clone(),
                fields,
            }],
        },
        None,
    )
    .await?;

    let pg_schema = format!("db_{db}");
    let row: (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema}\".\"t_workitems\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert!(
        !row.0
            .as_object()
            .expect("doc obj")
            .contains_key("completedAt")
    );

    Ok(())
}

// (c2) C6: an inserted explicit null for an optional field is stripped, not stored — the
// same "absent" shape a patch-to-null produces (see (c) above), both in the stored doc and
// in query results.
#[tokio::test]
async fn insert_strips_explicit_null_optional_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(), // "description": null
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let pg_schema = format!("db_{db}");
    let row: (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert!(
        !row.0
            .as_object()
            .expect("doc obj")
            .contains_key("description")
    );

    let query_result = rtdb_server::query::execute_query(
        &pool,
        &db,
        &schema,
        &rtdb_server::query::Query {
            table: "projects".to_string(),
            get: Some(id),
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
            aggregate: None,
        },
        None,
    )
    .await?;
    match query_result {
        rtdb_server::query::QueryResult::Doc(Some(value)) => {
            assert!(
                value
                    .as_object()
                    .expect("doc obj")
                    .get("description")
                    .is_none()
            );
        }
        other => panic!("expected Doc(Some(_)), got {other:?}"),
    }

    Ok(())
}

// (d) patch unknown field -> SchemaViolation (422).
#[tokio::test]
async fn patch_unknown_field_is_schema_violation() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let mut fields = serde_json::Map::new();
    fields.insert("bogus".to_string(), serde_json::json!(true));

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "projects".to_string(),
                id,
                fields,
            }],
        },
        None,
    )
    .await
    .expect_err("expected schema violation");
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    Ok(())
}

// (d2) replace fully overwrites doc, recomputes every typed column, bumps version. The
// replacement omits the optional `description` the original doc had, to prove this is a true
// full-document overwrite and not a merge: a `patch`-style merge would leave an untouched field
// alone, but `replace` must drop it since it isn't part of the new document.
#[tokio::test]
async fn replace_overwrites_doc_updates_typed_columns_and_bumps_version() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: doc(serde_json::json!({
                    "name": "Alpha",
                    "description": "original description",
                    "status": "active",
                    "tags": ["a", "b"],
                    "updatedAt": 1.0
                })),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let replacement = doc(serde_json::json!({
        "name": "Beta",
        "status": "paused",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id: id.clone(),
                doc: replacement,
            }],
        },
        None,
    )
    .await?;
    assert_eq!(outcome.results, vec![serde_json::Value::Null]);
    assert_eq!(
        outcome.write_set.tables,
        BTreeSet::from(["projects".to_string()])
    );

    let pg_schema = format!("db_{db}");
    let row: (String, String, i64, serde_json::Value) = sqlx::query_as(&format!(
        "SELECT \"f_name\", \"f_status\", \"version\", \"doc\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "Beta");
    assert_eq!(row.1, "paused");
    assert_eq!(row.2, 2);
    assert!(
        !row.3
            .as_object()
            .expect("doc obj")
            .contains_key("description")
    );

    Ok(())
}

// (d3) replace on a missing id -> NotFound (404).
#[tokio::test]
async fn replace_missing_id_returns_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id: "0".repeat(32),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await
    .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

// (d4) replace with a doc violating the schema -> SchemaViolation (422).
#[tokio::test]
async fn replace_schema_violation_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let bad_doc = doc(serde_json::json!({
        "name": "Beta",
        "description": null,
        "status": "not-a-valid-status",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "projects".to_string(),
                id,
                doc: bad_doc,
            }],
        },
        None,
    )
    .await
    .expect_err("expected schema violation");
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    Ok(())
}

// (d5) replace inside a multi-step txn rolled back by a later failed step.
#[tokio::test]
async fn replace_rolled_back_by_later_failed_step() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let replacement = doc(serde_json::json!({
        "name": "Beta",
        "description": null,
        "status": "paused",
        "tags": ["z"],
        "updatedAt": 9.0
    }));

    let txn = Transaction {
        steps: vec![
            Step::Replace {
                table: "projects".to_string(),
                id: id.clone(),
                doc: replacement,
            },
            Step::Delete {
                table: "projects".to_string(),
                id: "0".repeat(32),
            },
        ],
    };

    let result = execute_txn(&pool, &db, &schema, &txn, None).await;
    assert!(result.is_err());

    let pg_schema = format!("db_{db}");
    let row: (String, i64) = sqlx::query_as(&format!(
        "SELECT \"f_name\", \"version\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "Alpha");
    assert_eq!(row.1, 1);

    Ok(())
}

// (e) delete.
#[tokio::test]
async fn delete_removes_row() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "projects".to_string(),
                id: id.clone(),
            }],
        },
        None,
    )
    .await?;
    assert_eq!(outcome.results, vec![serde_json::Value::Null]);
    assert_eq!(
        outcome.write_set.tables,
        BTreeSet::from(["projects".to_string()])
    );

    let pg_schema = format!("db_{db}");
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 0);

    Ok(())
}

// (f) delete missing -> NotFound (404).
#[tokio::test]
async fn delete_missing_returns_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "projects".to_string(),
                id: "0".repeat(32),
            }],
        },
        None,
    )
    .await
    .expect_err("expected not found");
    assert_eq!(err.code, ErrorCode::NotFound);

    Ok(())
}

// (g) atomicity: [insert projects, patch(bad id)] -> error AND projects table has zero rows.
#[tokio::test]
async fn failed_step_rolls_back_earlier_steps_in_same_txn() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let txn = Transaction {
        steps: vec![
            Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            },
            Step::Patch {
                table: "projects".to_string(),
                id: "0".repeat(32),
                fields: serde_json::Map::new(),
            },
        ],
    };

    let result = execute_txn(&pool, &db, &schema, &txn, None).await;
    assert!(result.is_err());

    let pg_schema = format!("db_{db}");
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT count(*) FROM \"{pg_schema}\".\"t_projects\""
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(count, 0);

    Ok(())
}

// (h) expectVersion ok / mismatch (409).
#[tokio::test]
async fn expect_version_ok_and_mismatch() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::ExpectVersion {
                table: "projects".to_string(),
                id: id.clone(),
                version: 1,
            }],
        },
        None,
    )
    .await?;

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::ExpectVersion {
                table: "projects".to_string(),
                id,
                version: 2,
            }],
        },
        None,
    )
    .await
    .expect_err("expected precondition failed");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);

    Ok(())
}

// (i) expectAbsent on by_name: free, then occupied.
#[tokio::test]
async fn expect_absent_free_then_occupied() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::ExpectAbsent {
                table: "projects".to_string(),
                index: "by_name".to_string(),
                eq: vec![serde_json::json!("Alpha")],
            }],
        },
        None,
    )
    .await?;

    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::ExpectAbsent {
                table: "projects".to_string(),
                index: "by_name".to_string(),
                eq: vec![serde_json::json!("Alpha")],
            }],
        },
        None,
    )
    .await
    .expect_err("expected precondition failed");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);

    Ok(())
}

// (j) upsert insert-path then patch-path on by_name.
#[tokio::test]
async fn upsert_inserts_then_patches_on_by_name() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_doc = valid_project_doc();

    let outcome1 = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "projects".to_string(),
                index: "by_name".to_string(),
                eq: vec![serde_json::json!("Alpha")],
                insert: insert_doc.clone(),
                patch: serde_json::Map::new(),
            }],
        },
        None,
    )
    .await?;
    assert_eq!(outcome1.results[0]["inserted"], serde_json::json!(true));
    let id = outcome1.results[0]["id"].as_str().expect("id").to_string();

    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("status".to_string(), serde_json::json!("paused"));

    let outcome2 = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "projects".to_string(),
                index: "by_name".to_string(),
                eq: vec![serde_json::json!("Alpha")],
                insert: insert_doc,
                patch: patch_fields,
            }],
        },
        None,
    )
    .await?;
    assert_eq!(outcome2.results[0]["inserted"], serde_json::json!(false));
    assert_eq!(outcome2.results[0]["id"], serde_json::json!(id));

    let pg_schema = format!("db_{db}");
    let status: String = sqlx::query_scalar(&format!(
        "SELECT \"f_status\" FROM \"{pg_schema}\".\"t_projects\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(status, "paused");

    Ok(())
}

// (k) eq arity mismatch (1 value on a 2-field index) -> BadRequest (400).
#[tokio::test]
async fn eq_arity_mismatch_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::ExpectAbsent {
                table: "workItems".to_string(),
                index: "by_project_and_status".to_string(),
                eq: vec![serde_json::json!("0".repeat(32))],
            }],
        },
        None,
    )
    .await
    .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// (l) write_set correctness across a mixed txn.
#[tokio::test]
async fn write_set_reports_all_touched_tables() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let insert_project = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: valid_project_doc(),
            }],
        },
        None,
    )
    .await?;
    let project_id = insert_project.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("status".to_string(), serde_json::json!("paused"));

    let txn = Transaction {
        steps: vec![
            Step::Patch {
                table: "projects".to_string(),
                id: project_id.clone(),
                fields: patch_fields,
            },
            Step::Insert {
                table: "workItems".to_string(),
                doc: doc(serde_json::json!({
                    "projectId": project_id,
                    "title": "T",
                    "status": "backlog",
                    "order": 1.0,
                    "completedAt": null
                })),
            },
        ],
    };

    let outcome = execute_txn(&pool, &db, &schema, &txn, None).await?;
    assert_eq!(
        outcome.write_set.tables,
        BTreeSet::from(["projects".to_string(), "workItems".to_string()])
    );

    Ok(())
}

// (m) upsert matching more than one row -> PreconditionFailed, exact message. `by_name` is a
// plain (non-unique) index, so two ordinary inserts can legitimately share a name.
#[tokio::test]
async fn upsert_multiple_matches_is_precondition_failed() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    for _ in 0..2 {
        execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "projects".to_string(),
                    doc: valid_project_doc(),
                }],
            },
            None,
        )
        .await?;
    }

    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "projects".to_string(),
                index: "by_name".to_string(),
                eq: vec![serde_json::json!("Alpha")],
                insert: valid_project_doc(),
                patch: serde_json::Map::new(),
            }],
        },
        None,
    )
    .await
    .expect_err("expected precondition failed");
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert_eq!(err.message, "upsert matched multiple documents");

    Ok(())
}

// (n) MAX_STEPS boundary: 256 steps ok, 257 -> BadRequest.
#[tokio::test]
async fn max_steps_boundary() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    let schema = kanban_schema();

    let steps_256: Vec<Step> = (0..256)
        .map(|_| Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        })
        .collect();
    let outcome = execute_txn(&pool, &db, &schema, &Transaction { steps: steps_256 }, None).await?;
    assert_eq!(outcome.results.len(), 256);

    let steps_257: Vec<Step> = (0..257)
        .map(|_| Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        })
        .collect();
    let err = execute_txn(&pool, &db, &schema, &Transaction { steps: steps_257 }, None)
        .await
        .expect_err("expected bad request");
    assert_eq!(err.code, ErrorCode::BadRequest);

    Ok(())
}

// ---------------------------------------------------------------------------
// int64 index column recomputation on patch/replace — mirrors the
// Number→double precision path. Deviation from the brief: `fresh_db` always
// pushes the kanban schema, and pushing int64-only on top would be rejected
// as a destructive change, so we create the DB and push the int64 schema
// directly (same pattern as `search_test.rs::fresh_search_db`).
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

async fn fresh_int64_db(state: &Arc<AppState>) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    ddl::push_schema(&state.pool, &name, int64_schema())
        .await
        .expect("push int64 schema");
    name
}

// patch must recompute the bigint column: insert ts:"5" (outside the gte 20
// window), patch to ts:"50" (inside), and a `gte("20")` query must then match.
#[tokio::test]
async fn patch_recomputes_int64_indexed_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_int64_db(&state).await;
    let schema = int64_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "events".to_string(),
                doc: doc(serde_json::json!({ "ts": "5", "kind": "a" })),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    // Pre-patch: ts=5, below the gte 20 window -> no match.
    let pre = rtdb_server::query::execute_query(
        &pool,
        &db,
        &schema,
        &rtdb_server::query::Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: Some(serde_json::json!("20")),
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
            aggregate: None,
        },
        None,
    )
    .await?;
    assert!(matches!(
        pre,
        rtdb_server::query::QueryResult::Docs(ref ds) if ds.is_empty()
    ));

    // Patch ts -> 50; the f_ts bigint column must be recomputed.
    let mut fields = serde_json::Map::new();
    fields.insert("ts".to_string(), serde_json::json!("50"));
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "events".to_string(),
                id: id.clone(),
                fields,
            }],
        },
        None,
    )
    .await?;

    // Post-patch: ts=50, inside the gte 20 window -> match.
    let post = rtdb_server::query::execute_query(
        &pool,
        &db,
        &schema,
        &rtdb_server::query::Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: Some(serde_json::json!("20")),
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
            aggregate: None,
        },
        None,
    )
    .await?;
    match post {
        rtdb_server::query::QueryResult::Docs(ds) => {
            assert_eq!(ds.len(), 1);
            assert_eq!(ds[0]["kind"].as_str().expect("kind"), "a");
            assert_eq!(ds[0]["ts"].as_str().expect("ts"), "50");
        }
        other => panic!("expected Docs, got {other:?}"),
    }
    Ok(())
}

// replace must recompute the bigint column from the new doc body.
#[tokio::test]
async fn replace_recomputes_int64_indexed_column() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_int64_db(&state).await;
    let schema = int64_schema();

    let insert_outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "events".to_string(),
                doc: doc(serde_json::json!({ "ts": "5", "kind": "a" })),
            }],
        },
        None,
    )
    .await?;
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    // Replace with ts=50; the f_ts bigint column must be recomputed.
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "events".to_string(),
                id: id.clone(),
                doc: doc(serde_json::json!({ "ts": "50", "kind": "b" })),
            }],
        },
        None,
    )
    .await?;

    let post = rtdb_server::query::execute_query(
        &pool,
        &db,
        &schema,
        &rtdb_server::query::Query {
            table: "events".to_string(),
            get: None,
            index: Some("by_ts".to_string()),
            eq: vec![],
            gt: None,
            gte: Some(serde_json::json!("20")),
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
            aggregate: None,
        },
        None,
    )
    .await?;
    match post {
        rtdb_server::query::QueryResult::Docs(ds) => {
            assert_eq!(ds.len(), 1);
            assert_eq!(ds[0]["kind"].as_str().expect("kind"), "b");
            assert_eq!(ds[0]["ts"].as_str().expect("ts"), "50");
        }
        other => panic!("expected Docs, got {other:?}"),
    }
    Ok(())
}
