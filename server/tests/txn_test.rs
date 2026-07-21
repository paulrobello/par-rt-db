mod common;

use std::collections::BTreeSet;

use common::{fresh_db, kanban_schema_json, test_state};
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
    )
    .await?;

    assert_eq!(outcome.write_set, BTreeSet::from(["projects".to_string()]));
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
    )
    .await
    .expect_err("expected schema violation");
    assert_eq!(err.code, ErrorCode::SchemaViolation);

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
    )
    .await?;
    assert_eq!(outcome.results, vec![serde_json::Value::Null]);
    assert_eq!(outcome.write_set, BTreeSet::from(["projects".to_string()]));

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

    let result = execute_txn(&pool, &db, &schema, &txn).await;
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

    let outcome = execute_txn(&pool, &db, &schema, &txn).await?;
    assert_eq!(
        outcome.write_set,
        BTreeSet::from(["projects".to_string(), "workItems".to_string()])
    );

    Ok(())
}
