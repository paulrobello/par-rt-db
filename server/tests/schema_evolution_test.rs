mod common;

use common::test_state;
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
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "items".to_string(),
        TableDef {
            defaults: std::collections::BTreeMap::new(),
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

async fn fresh_empty_db(state: &std::sync::Arc<rtdb_server::AppState>) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    common::wrap_test_db(name)
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
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "users".to_string(),
        TableDef {
            defaults: std::collections::BTreeMap::new(),
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
