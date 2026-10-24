use crate::common::test_state;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::FilterExpr;
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use std::collections::BTreeMap;

/// One table `items` whose `priority` is a string-literal union indexed by
/// `by_priority` — mirrors the projects repo's `items.priority` field whose
/// widening (low|medium|high -> +critical) blocked its deploy.
fn priority_schema(variants: &[&str]) -> SchemaDef {
    let union = FieldType::Union {
        variants: variants
            .iter()
            .map(|v| FieldType::Literal {
                value: serde_json::Value::String((*v).to_string()),
            })
            .collect(),
    };
    let mut fields = BTreeMap::new();
    fields.insert("priority".to_string(), union);
    let indexes = vec![IndexDef {
        name: "by_priority".to_string(),
        fields: vec!["priority".to_string()],
        search: false,
        vector: None,
        unique: false,
        r#where: None,
        language: None,
        trgm: false,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "items".to_string(),
        TableDef {
            constraints: std::collections::BTreeMap::new(),
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes,
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,
            soft_delete: false,
        },
    );
    SchemaDef { tables }
}

async fn fresh_empty_db(state: &std::sync::Arc<rtdb_server::AppState>) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    crate::common::wrap_test_db(name)
}

#[tokio::test]
async fn widening_a_literal_union_push_succeeds() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high"]),
    )
    .await
    .expect("initial push");
    // Adding a variant is a safe widening: accepted additively, no migration,
    // and the indexed f_priority text column is unchanged.
    push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high", "critical"]),
    )
    .await
    .expect("widened push");
}

#[tokio::test]
async fn narrowing_a_literal_union_push_is_rejected() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high", "critical"]),
    )
    .await
    .expect("initial push");
    let err = push_schema(
        &state.pool,
        &db,
        priority_schema(&["low", "medium", "high"]),
    )
    .await
    .expect_err("narrowing must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("changed type of field 'items.priority'"),
        "{}",
        err.message
    );
}

/// A `users` table with an `email` index whose `unique` and `where` flags are
/// parameterized — exercises the destructive-change arms for the new partial /
/// unique index knobs.
fn email_index_schema(unique: bool, where_clause: Option<FilterExpr>) -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("email".to_string(), FieldType::String);
    fields.insert("deleted".to_string(), FieldType::Boolean);
    let indexes = vec![IndexDef {
        name: "by_email".to_string(),
        fields: vec!["email".to_string()],
        search: false,
        vector: None,
        unique,
        r#where: where_clause,
        language: None,
        trgm: false,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "users".to_string(),
        TableDef {
            constraints: std::collections::BTreeMap::new(),
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes,
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,
            soft_delete: false,
        },
    );
    SchemaDef { tables }
}

#[tokio::test]
async fn flipping_unique_on_existing_index_is_destructive() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(&state.pool, &db, email_index_schema(false, None))
        .await
        .expect("initial non-unique push");
    let err = push_schema(&state.pool, &db, email_index_schema(true, None))
        .await
        .expect_err("flipping unique must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("uniqueness"), "got: {}", err.message);
}

#[tokio::test]
async fn flipping_where_on_existing_index_is_destructive() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    let pred = FilterExpr::Eq {
        field: "deleted".to_string(),
        value: serde_json::Value::Bool(false),
    };
    push_schema(&state.pool, &db, email_index_schema(true, None))
        .await
        .expect("initial unique push with no predicate");
    let err = push_schema(&state.pool, &db, email_index_schema(true, Some(pred)))
        .await
        .expect_err("flipping where must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("partial predicate"),
        "got: {}",
        err.message
    );
}

// ---- nested optional-object widening: array<object> element shapes ----

use rtdb_server::txn::{Step, Transaction, execute_txn};

/// One table `artifacts` whose `versions` field is `array<object>` with a
/// required `tag` string; `size` is present and optional only after the
/// widening — the par-artifacts consumer shape.
fn versions_schema(with_size: bool) -> SchemaDef {
    let mut element = BTreeMap::new();
    element.insert("tag".to_string(), FieldType::String);
    if with_size {
        element.insert(
            "size".to_string(),
            FieldType::Optional {
                inner: Box::new(FieldType::Number),
            },
        );
    }
    let mut fields = BTreeMap::new();
    fields.insert("name".to_string(), FieldType::String);
    fields.insert(
        "versions".to_string(),
        FieldType::Array {
            element: Box::new(FieldType::Object { fields: element }),
        },
    );
    let mut tables = BTreeMap::new();
    tables.insert(
        "artifacts".to_string(),
        TableDef {
            constraints: BTreeMap::new(),
            defaults: BTreeMap::new(),
            computed: BTreeMap::new(),
            fields,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,
            soft_delete: false,
        },
    );
    SchemaDef { tables }
}

async fn insert_artifact(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    schema: &SchemaDef,
    versions: serde_json::Value,
) -> String {
    let outcome = execute_txn(
        &state.pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "artifacts".to_string(),
                doc: [
                    ("name", serde_json::Value::String("demo".to_string())),
                    ("versions", versions),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
        None,
    )
    .await
    .expect("insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

async fn fetch_artifact(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    id: &str,
) -> serde_json::Value {
    let (doc,): (serde_json::Value,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_artifacts\" WHERE \"id\" = $1"
    )))
    .bind(id)
    .fetch_one(&state.pool)
    .await
    .expect("fetch doc");
    doc
}

#[tokio::test]
async fn adding_optional_field_inside_array_object_elements_is_accepted() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;

    // v1: elements carry only `tag`.
    push_schema(&state.pool, &db, versions_schema(false))
        .await
        .expect("initial push");

    // A row written before the widening — its elements lack `size`.
    let old_id = insert_artifact(
        &state,
        &db,
        &versions_schema(false),
        serde_json::json!([{"tag": "v1"}]),
    )
    .await;

    // v2: elements gain an optional `size` — accepted as additive widening,
    // no destructive migrate required.
    push_schema(&state.pool, &db, versions_schema(true))
        .await
        .expect("widened push");

    // Old row still validates and reads back unchanged (optional-absent passes).
    let old_doc = fetch_artifact(&state, &db, &old_id).await;
    assert_eq!(old_doc["versions"], serde_json::json!([{"tag": "v1"}]));

    // New docs validate with and without the new field.
    let with_id = insert_artifact(
        &state,
        &db,
        &versions_schema(true),
        serde_json::json!([{"tag": "v2", "size": 1024}]),
    )
    .await;
    let without_id = insert_artifact(
        &state,
        &db,
        &versions_schema(true),
        serde_json::json!([{"tag": "v2"}]),
    )
    .await;
    assert_eq!(
        fetch_artifact(&state, &db, &with_id).await["versions"],
        serde_json::json!([{"tag": "v2", "size": 1024}])
    );
    assert_eq!(
        fetch_artifact(&state, &db, &without_id).await["versions"],
        serde_json::json!([{"tag": "v2"}])
    );
}

#[tokio::test]
async fn removing_element_object_field_is_rejected() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(&state.pool, &db, versions_schema(true))
        .await
        .expect("initial push");
    let err = push_schema(&state.pool, &db, versions_schema(false))
        .await
        .expect_err("removing an element field must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("changed type of field 'artifacts.versions'"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn retyping_element_object_field_is_rejected() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(&state.pool, &db, versions_schema(true))
        .await
        .expect("initial push");
    let mut swapped = versions_schema(true);
    swapped
        .tables
        .get_mut("artifacts")
        .expect("table")
        .fields
        .insert(
            "versions".to_string(),
            FieldType::Array {
                element: Box::new(FieldType::Object {
                    fields: BTreeMap::from([
                        ("tag".to_string(), FieldType::String),
                        (
                            "size".to_string(),
                            FieldType::Optional {
                                inner: Box::new(FieldType::String),
                            },
                        ),
                    ]),
                }),
            },
        );
    let err = push_schema(&state.pool, &db, swapped)
        .await
        .expect_err("retyping an element field must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("changed type of field 'artifacts.versions'"),
        "{}",
        err.message
    );
}

#[tokio::test]
async fn adding_required_field_inside_array_object_elements_is_rejected() {
    let state = test_state().await;
    let db = fresh_empty_db(&state).await;
    push_schema(&state.pool, &db, versions_schema(false))
        .await
        .expect("initial push");
    let mut widened = versions_schema(true);
    widened
        .tables
        .get_mut("artifacts")
        .expect("table")
        .fields
        .insert(
            "versions".to_string(),
            FieldType::Array {
                element: Box::new(FieldType::Object {
                    fields: BTreeMap::from([
                        ("tag".to_string(), FieldType::String),
                        ("size".to_string(), FieldType::Number),
                    ]),
                }),
            },
        );
    let err = push_schema(&state.pool, &db, widened)
        .await
        .expect_err("adding a REQUIRED element field must be rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("changed type of field 'artifacts.versions'"),
        "{}",
        err.message
    );
}
