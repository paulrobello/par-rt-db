//! Integration tests for `migrate::apply_migration` (Task 3).
//!
//! Each test builds a uniquely-named db, pushes a schema, inserts docs via the
//! real `txn::execute_txn` path (so typed `f_` columns are populated like in
//! production), then runs directives through `plan_migration` + `apply_migration`
//! inside a single tx and asserts against the physical tables. Later tasks
//! reuse this harness.

mod common;

use std::sync::Arc;

use common::test_state;
use rtdb_server::AppState;
use rtdb_server::db;
use rtdb_server::ddl::push_schema;
use rtdb_server::migrate::{MigrateRequest, apply_migration, plan_migration};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

/// Owns the freshly-created db and the schema that was pushed to it. Each test
/// builds one via `setup_db_with_schema` and drops it at the end.
struct Db {
    state: Arc<AppState>,
    name: String,
    schema: SchemaDef,
}

async fn setup_db_with_schema(schema_json: &str) -> Db {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let schema: SchemaDef = serde_json::from_str(schema_json).expect("parse schema json");
    push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    Db {
        state,
        name,
        schema,
    }
}

async fn insert_doc(db: &Db, table: &str, doc_json: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(doc_json).expect("parse doc json");
    let outcome = execute_txn(
        &db.state.pool,
        &db.name,
        &db.schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: table.into(),
                doc: value.as_object().expect("doc is an object").clone(),
            }],
        },
        None,
    )
    .await
    .expect("insert doc");
    outcome.results[0]["id"]
        .as_str()
        .expect("generated id")
        .to_string()
}

async fn get_doc(db: &Db, table: &str, id: &str) -> serde_json::Value {
    let schema_name = format!("db_{}", db.name);
    let table_ident = format!("t_{}", table.to_lowercase());
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT doc FROM \"{schema_name}\".\"{table_ident}\" WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch doc");
    doc
}

/// Runs the directives in `request_json` against `db` inside a single tx:
/// `plan_migration` derives the post-migration schema, then `apply_migration`
/// executes the DDL+DML and commits.
async fn migrate(db: &Db, request_json: &str) -> rtdb_server::migrate::MigrationEffects {
    let request: MigrateRequest =
        serde_json::from_str(request_json).expect("parse migrate request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan migration");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let fx = apply_migration(
        &mut tx,
        &db.name,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await
    .expect("apply migration");
    tx.commit().await.expect("commit migration tx");
    fx
}

/// `to_regclass` returns NULL when the relation does not exist.
async fn relation_exists(db: &Db, qualified_name: &str) -> bool {
    let (exists,): (Option<String>,) = sqlx::query_as("SELECT to_regclass($1)::text")
        .bind(qualified_name)
        .fetch_one(&db.state.pool)
        .await
        .expect("regclass lookup");
    exists.is_some()
}

async fn drop_db(db: &Db) {
    let _ = db::drop_database(&db.state.pool, &db.name).await;
}

// (a) renameField rewrites the jsonb key for a non-indexed field.
#[tokio::test]
async fn rename_field_rewrites_doc_key_and_column() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"renameField","table":"users","from":"name","to":"fullName"}]}"#,
    )
    .await;

    let doc = get_doc(&db, "users", &id).await;
    assert_eq!(doc["fullName"], "Ada");
    assert!(doc.get("name").is_none());
    drop_db(&db).await;
}

// (b) renameField renames the typed `f_` column when the field is indexed and
// preserves its value (a rename, not a recompute).
#[tokio::test]
async fn rename_field_renames_indexed_column_and_preserves_value() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"renameField","table":"users","from":"name","to":"fullName"}]}"#,
    )
    .await;

    let doc = get_doc(&db, "users", &id).await;
    assert_eq!(doc["fullName"], "Ada");
    // The old `f_name` column is gone; `f_fullname` holds the value.
    let schema_name = format!("db_{}", db.name);
    let (col,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch renamed column");
    assert_eq!(col, "Ada");
    assert!(!relation_exists(&db, &format!("\"{schema_name}\".\"f_name\"")).await);
    drop_db(&db).await;
}

// (c) renameTable renames the physical table; docs are untouched.
#[tokio::test]
async fn rename_table_renames_physical_table() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"renameTable","from":"users","to":"accounts"}]}"#,
    )
    .await;

    let schema_name = format!("db_{}", db.name);
    assert!(relation_exists(&db, &format!("\"{schema_name}\".\"t_accounts\"")).await);
    assert!(!relation_exists(&db, &format!("\"{schema_name}\".\"t_users\"")).await);
    // The row is unchanged under the new table name.
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT doc FROM \"{schema_name}\".\"t_accounts\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch doc from renamed table");
    assert_eq!(doc["name"], "Ada");
    drop_db(&db).await;
}

// (d) dropField removes the jsonb key for a non-indexed field.
#[tokio::test]
async fn drop_field_removes_doc_key() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"},"age":{"type":"number"}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada","age":36}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"dropField","table":"users","field":"age"}]}"#,
    )
    .await;

    let doc = get_doc(&db, "users", &id).await;
    assert_eq!(doc["name"], "Ada");
    assert!(doc.get("age").is_none());
    drop_db(&db).await;
}

// (e) dropField on an indexed field is rejected with a bad_request naming the
// blocking index — dropping an indexed column would desync the physical index.
#[tokio::test]
async fn drop_field_rejects_when_indexed() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}]}}}"#,
    )
    .await;
    insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"dropField","table":"users","field":"name"}]}"#,
    )
    .expect("parse request");
    // plan_migration still succeeds (it prunes index refs); the applier rejects.
    let derived = plan_migration(&db.schema, &request.directives).expect("plan");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let err = apply_migration(&mut tx, &db.name, &request.directives, &derived, false)
        .await
        .expect_err("indexed drop should be rejected");
    tx.rollback().await.ok();
    assert_eq!(err.code, rtdb_server::error::ErrorCode::BadRequest);
    assert!(
        err.message.contains("by_name"),
        "error should name the blocking index: {}",
        err.message
    );
    assert!(
        err.message.contains("users.name"),
        "error should name the field: {}",
        err.message
    );
    drop_db(&db).await;
}

// (f) dropTable removes the physical table and reports the deleted ids.
#[tokio::test]
async fn drop_table_removes_physical_table() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    let fx = migrate(&db, r#"{"directives":[{"op":"dropTable","name":"users"}]}"#).await;

    let schema_name = format!("db_{}", db.name);
    assert!(!relation_exists(&db, &format!("\"{schema_name}\".\"t_users\"")).await);
    assert_eq!(fx.reports[0].affected_rows, 1);
    assert!(fx.ops.iter().any(|o| o.table == "users"));
    drop_db(&db).await;
}

// (g) dropIndex drops the physical index (`i_<table>_<name>`).
#[tokio::test]
async fn drop_index_drops_physical_index() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}]}}}"#,
    )
    .await;
    let schema_name = format!("db_{}", db.name);
    let idx_qual = format!("\"{schema_name}\".\"i_users_by_name\"");
    assert!(relation_exists(&db, &idx_qual).await);

    migrate(
        &db,
        r#"{"directives":[{"op":"dropIndex","table":"users","name":"by_name"}]}"#,
    )
    .await;

    assert!(!relation_exists(&db, &idx_qual).await);
    drop_db(&db).await;
}

// (h) setDefault populates the jsonb field only on rows lacking it.
#[tokio::test]
async fn set_default_only_touches_rows_lacking_field() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"},"role":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let with_role = insert_doc(&db, "users", r#"{"name":"Ada","role":"admin"}"#).await;
    let without = insert_doc(&db, "users", r#"{"name":"Grace"}"#).await;

    let fx = migrate(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"users","field":"role","value":"member"}]}"#,
    )
    .await;

    let doc_with = get_doc(&db, "users", &with_role).await;
    let doc_without = get_doc(&db, "users", &without).await;
    assert_eq!(doc_with["role"], "admin"); // untouched
    assert_eq!(doc_without["role"], "member"); // defaulted
    assert_eq!(fx.reports[0].affected_rows, 1);
    drop_db(&db).await;
}

// (i) setDefault on an indexed field recomputes the typed `f_` column for the
// defaulted rows so the index sees the new value.
#[tokio::test]
async fn set_default_recomputes_indexed_column() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"},"score":{"type":"optional","inner":{"type":"number"}}},"indexes":[{"name":"by_score","fields":["score"]}]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await; // no `score`

    migrate(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"users","field":"score","value":0}]}"#,
    )
    .await;

    let doc = get_doc(&db, "users", &id).await;
    assert_eq!(doc["score"], 0);
    let schema_name = format!("db_{}", db.name);
    let (typed,): (f64,) = sqlx::query_as(&format!(
        "SELECT \"f_score\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_score");
    assert_eq!(typed, 0.0);
    drop_db(&db).await;
}

// (j) changeType is a placeholder until Task 4 -> internal error.
#[tokio::test]
async fn change_type_returns_unimplemented() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"age":{"type":"number"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "users", r#"{"age":36}"#).await;

    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"changeType","table":"users","field":"age","to":{"type":"string"},"cast":"toString"}]}"#,
    )
    .expect("parse request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let err = apply_migration(&mut tx, &db.name, &request.directives, &derived, false)
        .await
        .expect_err("changeType should be unimplemented");
    tx.rollback().await.ok();
    assert!(
        err.message.contains("not yet implemented"),
        "{}",
        err.message
    );
    drop_db(&db).await;
}

// (k) evalExpr is a placeholder until Task 5 -> internal error.
#[tokio::test]
async fn eval_expr_returns_unimplemented() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"evalExpr","table":"users","set":"upper","expr":"upper(doc->>'name')"}]}"#,
    )
    .expect("parse request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let err = apply_migration(&mut tx, &db.name, &request.directives, &derived, false)
        .await
        .expect_err("evalExpr should be unimplemented");
    tx.rollback().await.ok();
    assert!(
        err.message.contains("not yet implemented"),
        "{}",
        err.message
    );
    drop_db(&db).await;
}
