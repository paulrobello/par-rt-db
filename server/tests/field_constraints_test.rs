//! Declarative per-field write constraints (`TableDef::constraints`):
//! push-time validation (misapplied constraint / invalid regex / crossed /
//! non-integer int64 bounds reject the push naming the field) and write-path
//! enforcement — insert, patch, replace, upsert (both branches), and
//! patchByQuery fail with `SCHEMA_VIOLATION` and roll back the whole txn when
//! a non-null value violates a declared `min`/`max`/`minLength`/`maxLength`/
//! `pattern` (boundaries inclusive). Migrate interplay: a `changeType` that
//! would leave an entry misapplied is rejected at plan time, and the apply
//! path re-validates every coerced row value against the constraints.
//! Cross-engine agreement is pinned by the `constraint-*` /
//! `error-constraint-*` semantics-corpus cases.

use crate::common::{test_state, wrap_test_db};
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::migrate::{Directive, apply_migration, plan_migration};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn constraints_schema_json(constraints: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"tables":{"tasks":{
        "fields":{
            "name":{"type":"string"},
            "score":{"type":"number"},
            "counter":{"type":"int64"}},
        "indexes":[{"name":"by_name","fields":["name"]}],
        "constraints": constraints
    }}})
}

const GOOD_CONSTRAINTS: &str = r#"{"score":{"min":1,"max":10},"counter":{"min":1,"max":100},"name":{"minLength":2,"maxLength":5,"pattern":"^[a-z]+$"}}"#;

async fn fresh_db_with(
    state: &rtdb_server::AppState,
    constraints_json: serde_json::Value,
) -> (crate::common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef =
        serde_json::from_value(constraints_schema_json(constraints_json)).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");
    (db, schema)
}

/// Pushes a schema expected to FAIL at push time; returns the typed error.
async fn push_err(
    state: &rtdb_server::AppState,
    constraints_json: serde_json::Value,
) -> rtdb_server::error::RtDbError {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let _db = wrap_test_db(name.clone());
    let schema: SchemaDef =
        serde_json::from_value(constraints_schema_json(constraints_json)).expect("parse schema");
    push_schema(&state.pool, &name, schema)
        .await
        .expect_err("expected push failure")
}

async fn run(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> Result<rtdb_server::txn::TxnOutcome, rtdb_server::error::RtDbError> {
    execute_txn(
        pool,
        db,
        schema,
        &Transaction { steps },
        &rtdb_server::auth::PrincipalCtx::bypass(),
        None,
    )
    .await
}

fn doc(v: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    v.as_object().expect("json object").clone()
}

async fn seed_one(pool: &sqlx::PgPool, db: &str, schema: &SchemaDef) -> String {
    let outcome = run(
        pool,
        db,
        schema,
        vec![Step::Insert {
            table: "tasks".into(),
            doc: doc(serde_json::json!({"name": "ok", "score": 5.0, "counter": "1"})),
        }],
    )
    .await
    .expect("seed");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

async fn count_rows(pool: &sqlx::PgPool, db: &str) -> i64 {
    let (n,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM \"db_{db}\".\"t_tasks\""
    )))
    .fetch_one(pool)
    .await
    .expect("count");
    n
}

// ---- push-time validation ----

#[tokio::test]
async fn push_accepts_constraints_on_number_int64_and_string() {
    let state = test_state().await;
    let (_db, schema) =
        fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    let table = schema.tables.get("tasks").expect("table");
    assert_eq!(table.constraints.len(), 3);
    let score = table.constraints.get("score").expect("score entry");
    assert_eq!(score.min.as_ref().and_then(|n| n.as_f64()), Some(1.0));
    assert_eq!(score.max.as_ref().and_then(|n| n.as_f64()), Some(10.0));
    let name = table.constraints.get("name").expect("name entry");
    assert_eq!(name.min_length, Some(2));
    assert_eq!(name.max_length, Some(5));
    assert_eq!(name.pattern.as_deref(), Some("^[a-z]+$"));
}

#[tokio::test]
async fn push_rejects_misapplied_constraint_naming_the_field() {
    let state = test_state().await;
    // `min` on a string field
    let err = push_err(&state, serde_json::json!({"name": {"min": 1}})).await;
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("name") && err.message.contains("'min' is legal only on number/int64"),
        "unexpected error: {}",
        err.message
    );
    // `maxLength` on a number field
    let err = push_err(&state, serde_json::json!({"score": {"maxLength": 3}})).await;
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("score") && err.message.contains("string"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_invalid_pattern_naming_the_field() {
    let state = test_state().await;
    let err = push_err(&state, serde_json::json!({"name": {"pattern": "([a-z"}})).await;
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("name") && err.message.contains("invalid pattern"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_crossed_bounds() {
    let state = test_state().await;
    let err = push_err(&state, serde_json::json!({"score": {"min": 10, "max": 1}})).await;
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("'min' exceeds 'max'"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_non_integer_int64_bound() {
    let state = test_state().await;
    let err = push_err(&state, serde_json::json!({"counter": {"min": 1.5}})).await;
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("must be an integer"),
        "unexpected error: {}",
        err.message
    );
}

// ---- write-path enforcement ----

#[tokio::test]
async fn insert_below_min_fails_and_rolls_back_whole_txn() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    // Two steps: a valid insert, then a violating one — the txn is atomic, so
    // NEITHER row may survive.
    let err = run(
        &pool,
        &db,
        &schema,
        vec![
            Step::Insert {
                table: "tasks".into(),
                doc: doc(serde_json::json!({"name": "good", "score": 5.0, "counter": "1"})),
            },
            Step::Insert {
                table: "tasks".into(),
                doc: doc(serde_json::json!({"name": "bad", "score": 0.5, "counter": "1"})),
            },
        ],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("score") && err.message.contains("'min'"),
        "unexpected error: {}",
        err.message
    );
    assert_eq!(count_rows(&pool, &db).await, 0);
}

#[tokio::test]
async fn insert_boundary_values_pass_inclusive() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    run(
        &pool,
        &db,
        &schema,
        vec![
            Step::Insert {
                table: "tasks".into(),
                doc: doc(serde_json::json!({"name": "aa", "score": 1.0, "counter": "1"})),
            },
            Step::Insert {
                table: "tasks".into(),
                doc: doc(serde_json::json!({"name": "abcde", "score": 10.0, "counter": "100"})),
            },
        ],
    )
    .await
    .expect("boundary values pass");
    assert_eq!(count_rows(&pool, &db).await, 2);
}

#[tokio::test]
async fn patch_violation_fails_txn() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    let id = seed_one(&pool, &db, &schema).await;
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::Patch {
            table: "tasks".into(),
            id,
            fields: doc(serde_json::json!({"score": 50.0})),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("score"), "{}", err.message);
}

#[tokio::test]
async fn replace_violation_fails_txn() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    let id = seed_one(&pool, &db, &schema).await;
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "tasks".into(),
            id,
            doc: doc(serde_json::json!({"name": "ab", "score": 99.0, "counter": "2"})),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert_eq!(count_rows(&pool, &db).await, 1);
}

#[tokio::test]
async fn upsert_both_branches_enforce_constraints() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    // Insert branch: a violating body fails.
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("newrow")],
            insert: doc(serde_json::json!({"name": "newrow", "score": 0.0, "counter": "1"})),
            patch: serde_json::Map::new(),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    // Seed a good row via upsert, then hit the update (patch) branch.
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("ok")],
            insert: doc(serde_json::json!({"name": "ok", "score": 5.0, "counter": "1"})),
            patch: serde_json::Map::new(),
        }],
    )
    .await
    .expect("seed row");
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("ok")],
            insert: serde_json::Map::new(),
            patch: doc(serde_json::json!({"counter": "101"})),
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}

#[tokio::test]
async fn patch_by_query_violation_fails_txn() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    seed_one(&pool, &db, &schema).await;
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::PatchByQuery {
            table: "tasks".into(),
            filter: rtdb_server::dsl::FilterExpr::Eq {
                field: "name".into(),
                value: serde_json::json!("ok"),
            },
            patch: doc(serde_json::json!({"score": 99.0})),
            limit: None,
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert_eq!(count_rows(&pool, &db).await, 1);
}

// ---- migrate interplay ----

#[tokio::test]
async fn change_type_plan_rejects_constraints_left_misapplied() {
    let state = test_state().await;
    let (_db, schema) =
        fresh_db_with(&state, serde_json::from_str(GOOD_CONSTRAINTS).unwrap()).await;
    // `score` carries min/max; retyping it to string would leave the bounds
    // misapplied, so the PLAN fails with the same typed push error.
    let err = plan_migration(
        &schema,
        &[Directive::ChangeType {
            table: "tasks".into(),
            field: "score".into(),
            to: rtdb_server::schema::FieldType::String,
            cast: rtdb_server::migrate::Cast::ToString,
            default: None,
        }],
    )
    .unwrap_err();
    assert!(
        err.message.contains("score")
            && err.message.contains("'min' is legal only on number/int64"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn change_type_apply_revalidates_coerced_values_against_constraints() {
    let state = test_state().await;
    let pool = state.pool.clone();
    // `score` bounds so the seeded row's value is INSIDE them: retyping
    // `counter` (number→string, coerced "1" → "1") must pass the apply-time
    // constraint re-validation on the constrained `score` field it doesn't
    // touch, while a retype whose coerced values would violate the field's
    // own constraints fails.
    let (db, schema) =
        fresh_db_with(&state, serde_json::json!({"score": {"min": 1, "max": 10}})).await;
    seed_one(&pool, &db, &schema).await;
    let directives = vec![Directive::ChangeType {
        table: "tasks".into(),
        field: "counter".into(),
        to: rtdb_server::schema::FieldType::Number,
        cast: rtdb_server::migrate::Cast::ToNumber,
        default: None,
    }];
    let derived = plan_migration(&schema, &directives).expect("plan valid retype");
    let mut tx = pool.begin().await.expect("begin tx");
    apply_migration(&mut tx, &db, &directives, &derived, false)
        .await
        .expect("apply valid retype (coerced values satisfy constraints)");
    tx.commit().await.expect("commit migration");
}
