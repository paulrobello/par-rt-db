//! `Step::InsertMany` / `Step::UpsertMany`: bulk import past the per-txn step
//! cap, per-doc validation with whole-step rollback, the 1000-doc row cap, and
//! upsert resolution through a full-arity index equality.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::create_database;
use rtdb_server::ddl::{pg_schema, pg_table, push_schema};
use rtdb_server::error::{ErrorCode, RtDbError};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use serde_json::{Value, json};

use crate::common::{TestDb, test_state, wrap_test_db};

struct Fixture {
    state: Arc<AppState>,
    db: TestDb,
    schema: SchemaDef,
}

async fn fixture() -> Fixture {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    create_database(&state.pool, &name).await.unwrap();
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(json!({"tables": {
        "items": {
            "fields": {"sku": {"type": "string"}, "qty": {"type": "number"}},
            "indexes": [{"name": "by_sku", "fields": ["sku"], "unique": true}]
        }
    }}))
    .unwrap();
    push_schema(&state.pool, &db, schema.clone()).await.unwrap();
    Fixture { state, db, schema }
}

async fn run(f: &Fixture, steps: Vec<Step>) -> Result<TxnOutcome, RtDbError> {
    let ctx = PrincipalCtx {
        user_id: Some("u".into()),
        email: None,
        tables: None,
        host_capability: None,
        enqueuer: None,
        admin: false,
        session_hash: None,
    };
    execute_txn(
        &f.state.pool,
        &f.db,
        &f.schema,
        &Transaction { steps },
        &ctx,
        None,
    )
    .await
}

fn docs(range: std::ops::Range<usize>) -> Vec<serde_json::Map<String, Value>> {
    range
        .map(|n| {
            json!({"sku": format!("s{n}"), "qty": n})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect()
}

async fn count(f: &Fixture) -> i64 {
    let schema = pg_schema(&f.db);
    let ident = pg_table("items");
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"{ident}\""
    )))
    .fetch_one(&f.state.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn insert_many_inserts_more_than_sixteen_docs_in_one_step() {
    let f = fixture().await;
    let out = run(
        &f,
        vec![Step::InsertMany {
            table: "items".into(),
            docs: docs(0..40),
        }],
    )
    .await
    .unwrap();
    let ids = out.results[0]["ids"].as_array().unwrap();
    assert_eq!(ids.len(), 40);
    assert_eq!(out.write_set.ops.len(), 40);
    assert_eq!(count(&f).await, 40);
}

#[tokio::test]
async fn invalid_mid_list_doc_rolls_back_the_whole_step() {
    let f = fixture().await;
    let mut list = docs(0..20);
    list[10].insert("qty".into(), json!("not a number"));
    let err = run(
        &f,
        vec![Step::InsertMany {
            table: "items".into(),
            docs: list,
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert_eq!(count(&f).await, 0);
}

#[tokio::test]
async fn unique_conflict_mid_list_rolls_back_earlier_steps_too() {
    let f = fixture().await;
    let mut list = docs(0..5);
    list.push(list[1].clone());
    let err = run(
        &f,
        vec![
            Step::InsertMany {
                table: "items".into(),
                docs: docs(100..103),
            },
            Step::InsertMany {
                table: "items".into(),
                docs: list,
            },
        ],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
    assert_eq!(count(&f).await, 0);
}

#[tokio::test]
async fn over_row_cap_is_rejected_and_cap_exactly_is_accepted() {
    let f = fixture().await;
    let err = run(
        &f,
        vec![Step::InsertMany {
            table: "items".into(),
            docs: docs(0..1001),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(count(&f).await, 0);
    let out = run(
        &f,
        vec![Step::InsertMany {
            table: "items".into(),
            docs: docs(0..1000),
        }],
    )
    .await
    .unwrap();
    assert_eq!(out.results[0]["ids"].as_array().unwrap().len(), 1000);
    assert_eq!(count(&f).await, 1000);
}

#[tokio::test]
async fn upsert_many_inserts_new_and_patches_existing_by_index_equality() {
    let f = fixture().await;
    run(
        &f,
        vec![Step::InsertMany {
            table: "items".into(),
            docs: docs(0..3),
        }],
    )
    .await
    .unwrap();
    let mut list = docs(1..2);
    list[0].insert("qty".into(), json!(99));
    list.extend(docs(50..52));
    let out = run(
        &f,
        vec![Step::UpsertMany {
            table: "items".into(),
            index: "by_sku".into(),
            docs: list,
        }],
    )
    .await
    .unwrap();
    assert_eq!(out.results[0]["inserted"], json!([false, true, true]));
    assert_eq!(out.results[0]["ids"].as_array().unwrap().len(), 3);
    assert_eq!(count(&f).await, 5);
}

#[tokio::test]
async fn upsert_many_doc_missing_indexed_field_rolls_back() {
    let f = fixture().await;
    let mut list = docs(0..3);
    list[2].remove("sku");
    let err = run(
        &f,
        vec![Step::UpsertMany {
            table: "items".into(),
            index: "by_sku".into(),
            docs: list,
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(count(&f).await, 0);
}
