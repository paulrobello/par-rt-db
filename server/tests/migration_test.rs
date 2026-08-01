//! Integration tests for `migrate::apply_migration` (Task 3).
//!
//! Each test builds a uniquely-named db, pushes a schema, inserts docs via the
//! real `txn::execute_txn` path (so typed `f_` columns are populated like in
//! production), then runs directives through `plan_migration` + `apply_migration`
//! inside a single tx and asserts against the physical tables. Later tasks
//! reuse this harness.

mod common;

use std::sync::Arc;

use common::{admin_post, spawn_app, test_state, test_state_with_audit};
use rtdb_server::AppState;
use rtdb_server::db;
use rtdb_server::ddl::push_schema;
use rtdb_server::migrate::{MigrateRequest, MigrateResult, apply_migration, plan_migration};
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::Query;
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{OpKind, Step, Transaction, execute_txn};

/// Owns the freshly-created db and the schema that was pushed to it. Each test
/// builds one via `setup_db_with_schema` and drops it at the end.
struct Db {
    state: Arc<AppState>,
    name: String,
    schema: SchemaDef,
}

async fn setup_db_with_schema(schema_json: &str) -> Db {
    let state = test_state().await;
    setup_db_with_schema_in(state, schema_json).await
}

/// Like `setup_db_with_schema` but the caller supplies the `AppState`. Used by
/// the committer-arm tests that need a non-default state — e.g. audit-enabled
/// (`test_state_with_audit`) so the migrate tap writes `rtdb.audit_log` rows.
async fn setup_db_with_schema_in(state: Arc<AppState>, schema_json: &str) -> Db {
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

/// Returns every doc row for `table`. Used by evalExpr tests that need to scan
/// across rows to assert which ones a `where`-scoped rewrite touched.
async fn query_docs(db: &Db, table: &str) -> Vec<serde_json::Value> {
    let schema_name = format!("db_{}", db.name);
    let table_ident = format!("t_{}", table.to_lowercase());
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT doc FROM \"{schema_name}\".\"{table_ident}\""
    ))
    .fetch_all(&db.state.pool)
    .await
    .expect("fetch docs");
    rows.into_iter().map(|r| r.0).collect()
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

/// Like `migrate` but expects `apply_migration` to fail; rolls the tx back and
/// returns the error so the caller can assert on `code`/`message`. The doc
/// state is what it was before the tx began (the rollback undoes any partial
/// per-row writes).
async fn migrate_err(db: &Db, request_json: &str) -> rtdb_server::error::RtDbError {
    let request: MigrateRequest =
        serde_json::from_str(request_json).expect("parse migrate request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan migration");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let err = apply_migration(
        &mut tx,
        &db.name,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await
    .expect_err("expected migrate error");
    tx.rollback().await.ok();
    err
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

// (e2) dropField on a vector index's `filterFields` entry is rejected with a
// bad_request naming the index — a vector index's filter field carries a real
// `f_` column (see `ddl::indexed_fields`), so dropping it would desync the
// physical state from the derived schema the same way a btree index field would.
#[tokio::test]
async fn drop_field_rejects_when_vector_filter_field() {
    let db = setup_db_with_schema(
        r#"{"tables":{"docs":{"fields":{
            "embedding":{"type":"vector","dimensions":3},
            "userId":{"type":"string"},
            "note":{"type":"string"}
        },"indexes":[
            {"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":3,"filterFields":["userId"]}}
        ]}}}"#,
    )
    .await;
    insert_doc(
        &db,
        "docs",
        r#"{"embedding":[0.0,0.0,0.0],"userId":"u1","note":"x"}"#,
    )
    .await;

    // Dropping the vector index's filterField is blocked and names the index.
    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"dropField","table":"docs","field":"userId"}]}"#,
    )
    .expect("parse request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    let err = apply_migration(&mut tx, &db.name, &request.directives, &derived, false)
        .await
        .expect_err("vector filterField drop should be rejected");
    tx.rollback().await.ok();
    assert_eq!(err.code, rtdb_server::error::ErrorCode::BadRequest);
    assert!(
        err.message.contains("by_embedding"),
        "error should name the blocking vector index: {}",
        err.message
    );
    assert!(
        err.message.contains("docs.userId"),
        "error should name the field: {}",
        err.message
    );

    // Sanity: dropping a non-indexed field on the same table still succeeds.
    let id2 = insert_doc(
        &db,
        "docs",
        r#"{"embedding":[1.0,0.0,0.0],"userId":"u2","note":"keep-me"}"#,
    )
    .await;
    migrate(
        &db,
        r#"{"directives":[{"op":"dropField","table":"docs","field":"note"}]}"#,
    )
    .await;
    let doc = get_doc(&db, "docs", &id2).await;
    assert!(doc.get("note").is_none(), "non-indexed field dropped");
    assert_eq!(doc["userId"], "u2", "filterField untouched");
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

// (j) changeType: number→string coerces the jsonb value (and the typed column
// when the field is indexed). `age` is indexed by `by_age`, so the `f_age`
// column is recast from double precision to text and its value recomputed from
// the just-updated doc.
#[tokio::test]
async fn change_type_number_to_string_coerces() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"age":{"type":"number"}},"indexes":[{"name":"by_age","fields":["age"]}]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"age":42}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"changeType","table":"users","field":"age","to":{"type":"string"},"cast":"toString"}]}"#,
    )
    .await;

    let doc = get_doc(&db, "users", &id).await;
    assert_eq!(doc["age"], "42");
    // The typed column followed the cast: f_age is now text holding "42".
    let schema_name = format!("db_{}", db.name);
    let (col,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_age\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_age");
    assert_eq!(col, "42");
    drop_db(&db).await;
}

// (j2) changeType to number with an uncoercible row and no default fails
// atomically with a BadRequest naming the offending row, and leaves the doc
// unchanged (the tx rolls back the partial per-row rewrite). Non-indexed field,
// so only the `doc` jsonb value is in play.
#[tokio::test]
async fn change_type_to_number_atomic_fail_names_row() {
    let db =
        setup_db_with_schema(r#"{"tables":{"u":{"fields":{"v":{"type":"string"}},"indexes":[]}}}"#)
            .await;
    let id = insert_doc(&db, "u", r#"{"v":"not-a-number"}"#).await;
    let err = migrate_err(
        &db,
        r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber"}]}"#,
    )
    .await;
    assert!(
        err.message.contains(&id),
        "error should name the offending row id: {err:?}"
    );
    assert!(
        err.message.contains("not-a-number"),
        "error should name the offending value: {err:?}"
    );
    // atomic: doc unchanged
    assert_eq!(get_doc(&db, "u", &id).await["v"], "not-a-number");
    drop_db(&db).await;
}

// (j3) changeType with a default substitutes the default for uncoercible rows
// instead of failing. The default is representable in the target type (0 is a
// valid number), so the cast succeeds and the doc reflects the default.
#[tokio::test]
async fn change_type_default_substitutes_uncoercible() {
    let db =
        setup_db_with_schema(r#"{"tables":{"u":{"fields":{"v":{"type":"string"}},"indexes":[]}}}"#)
            .await;
    let id = insert_doc(&db, "u", r#"{"v":"oops"}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber","default":0}]}"#,
    )
    .await;
    assert_eq!(get_doc(&db, "u", &id).await["v"], 0);
    drop_db(&db).await;
}

// (j4) changeType on an indexed field recomputes the typed column for every row
// from the already-updated doc, including rows that took the default. Mixes one
// coercible row ("42") with one uncoercible row ("NaN") under a ToNumber cast
// with default 0; both `doc` and `f_v` must end up consistent.
#[tokio::test]
async fn change_type_indexed_column_recompute_with_default() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"v":{"type":"string"}},"indexes":[{"name":"by_v","fields":["v"]}]}}}"#,
    )
    .await;
    let good = insert_doc(&db, "u", r#"{"v":"42"}"#).await;
    let bad = insert_doc(&db, "u", r#"{"v":"NaN"}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber","default":0}]}"#,
    )
    .await;

    assert_eq!(get_doc(&db, "u", &good).await["v"], 42.0);
    assert_eq!(get_doc(&db, "u", &bad).await["v"], 0);
    // The typed column was recast double precision and recomputed from doc.
    let schema_name = format!("db_{}", db.name);
    let (good_col,): (f64,) = sqlx::query_as(&format!(
        "SELECT \"f_v\" FROM \"{schema_name}\".\"t_u\" WHERE id = $1"
    ))
    .bind(&good)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_v good");
    assert_eq!(good_col, 42.0);
    let (bad_col,): (f64,) = sqlx::query_as(&format!(
        "SELECT \"f_v\" FROM \"{schema_name}\".\"t_u\" WHERE id = $1"
    ))
    .bind(&bad)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_v bad");
    assert_eq!(bad_col, 0.0);
    drop_db(&db).await;
}

// (j5) Regression: when the default's JSON form differs from the target type's
// natural form (e.g. `default: true` under `toNumber` — coercible to 1.0, but
// its JSON text "true" is not a valid float8 literal), the doc must hold the
// **coerced** default so the `ALTER ... USING (doc->>'v')::float8` re-cast on
// the indexed column cannot reject it. Before the fix this 500'd: the doc held
// the uncoerced `true`, and `'true'::float8` is a Postgres error.
#[tokio::test]
async fn change_type_indexed_default_bool_recasts_cleanly_to_number() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"v":{"type":"string"}},"indexes":[{"name":"by_v","fields":["v"]}]}}}"#,
    )
    .await;
    let good = insert_doc(&db, "u", r#"{"v":"42"}"#).await;
    let bad = insert_doc(&db, "u", r#"{"v":"oops"}"#).await;

    migrate(
        &db,
        r#"{"directives":[{"op":"changeType","table":"u","field":"v","to":{"type":"number"},"cast":"toNumber","default":true}]}"#,
    )
    .await;

    // The uncoercible row took the default and now holds the coerced 1.0, not
    // the boolean `true`.
    assert_eq!(get_doc(&db, "u", &bad).await["v"], 1.0);
    // The coercible row's numeric value is unchanged.
    assert_eq!(get_doc(&db, "u", &good).await["v"], 42.0);
    // The indexed column recast cleanly under the coerced default.
    let schema_name = format!("db_{}", db.name);
    let (good_col,): (f64,) = sqlx::query_as(&format!(
        "SELECT \"f_v\" FROM \"{schema_name}\".\"t_u\" WHERE id = $1"
    ))
    .bind(&good)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_v good");
    assert_eq!(good_col, 42.0);
    let (bad_col,): (f64,) = sqlx::query_as(&format!(
        "SELECT \"f_v\" FROM \"{schema_name}\".\"t_u\" WHERE id = $1"
    ))
    .bind(&bad)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_v bad");
    assert_eq!(bad_col, 1.0);
    drop_db(&db).await;
}

// (k) evalExpr rewrites a doc field via a scoped SQL expression. The expr is
// interpolated as SQL text over the row's `doc`; the new value is written back
// under the `set` key.
#[tokio::test]
async fn eval_expr_rewrites_doc_field() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"upper":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"upper","expr":"upper(doc->>'name')"}]}"#,
    )
    .await;
    assert_eq!(get_doc(&db, "u", &id).await["upper"], "ADA");
    drop_db(&db).await;
}

// (l) evalExpr `where` scopes the rewrite to matching rows only.
#[tokio::test]
async fn eval_expr_where_filters() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"doubled":{"type":"optional","inner":{"type":"number"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;
    insert_doc(&db, "u", r#"{"n":2}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"doubled","expr":"(doc->>'n')::float8 * 2","where":"(doc->>'n')::float8 >= 2"}]}"#,
    )
    .await;
    let docs = query_docs(&db, "u").await;
    // only the n=2 row gets `doubled` set to 4 ...
    assert!(
        docs.iter()
            .any(|d| d.get("doubled").and_then(|v| v.as_f64()) == Some(4.0)),
        "n=2 row should have doubled=4, got {docs:?}"
    );
    // ... and the n=1 row is untouched (no `doubled` key).
    let untouched = docs
        .iter()
        .filter_map(|d| d.as_object())
        .any(|o| !o.contains_key("doubled"));
    assert!(
        untouched,
        "n=1 row should not carry `doubled`, got {docs:?}"
    );
    drop_db(&db).await;
}

// (m) evalExpr recomputes the indexed `f_` column when its source field is the
// `set` target. `upper` is indexed by `by_upper`, so `f_upper` must track the
// new doc value after the rewrite (not stay NULL).
#[tokio::test]
async fn eval_expr_recomputes_indexed_column() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"upper":{"type":"optional","inner":{"type":"string"}}},"indexes":[{"name":"by_upper","fields":["upper"]}]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"upper","expr":"upper(doc->>'name')"}]}"#,
    )
    .await;
    // The doc carries the new value ...
    assert_eq!(get_doc(&db, "u", &id).await["upper"], "ADA");
    // ... and so does the typed `f_upper` column.
    let schema_name = format!("db_{}", db.name);
    let (typed,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_upper\" FROM \"{schema_name}\".\"t_u\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_upper");
    assert_eq!(typed, "ADA");
    drop_db(&db).await;
}

// (m2) Dependent multi-directive batch: a later directive operates on an entity
// an earlier directive renamed, addressing it by its NEW name. `apply_migration`
// advances a per-directive working schema (mirroring `plan_migration`'s
// sequential fold) so the renameField resolves `accounts`; the pre-batch
// snapshot alone (`users`) would miss it and the batch would fail at apply time
// despite passing plan — a "passes dry-run, fails apply" footgun.
#[tokio::test]
async fn dependent_batch_operates_on_renamed_table() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;

    migrate(
        &db,
        r#"{"directives":[
            {"op":"renameTable","from":"users","to":"accounts"},
            {"op":"renameField","table":"accounts","from":"name","to":"fullName"}
        ]}"#,
    )
    .await;

    // The table was renamed, then the field renamed on the renamed table.
    let schema_name = format!("db_{}", db.name);
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT doc FROM \"{schema_name}\".\"t_accounts\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch doc from renamed table");
    assert_eq!(doc["fullName"], "Ada");
    assert!(doc.get("name").is_none());
    drop_db(&db).await;
}

// (n2) dropField and changeType report `affected_rows` and emit DocOps only for
// rows that carry the touched field (rows whose `doc` actually changed) — not
// every row in the table. Aligns with the spec's "DocOps for the affected rows"
// and the precise counting the other data-bearing directives already do.
#[tokio::test]
async fn drop_field_and_change_type_count_only_field_carriers() {
    // --- dropField targets an optional field; two of three rows carry `tag`. ---
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"v":{"type":"string"},"tag":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let c1 = insert_doc(&db, "u", r#"{"v":"1","tag":"a"}"#).await;
    let c2 = insert_doc(&db, "u", r#"{"v":"2","tag":"b"}"#).await;
    insert_doc(&db, "u", r#"{"v":"3"}"#).await; // lacks `tag`
    let fx = migrate(
        &db,
        r#"{"directives":[{"op":"dropField","table":"u","field":"tag"}]}"#,
    )
    .await;
    assert_eq!(fx.reports[0].affected_rows, 2, "dropField counts carriers");
    let ids: Vec<String> = fx.ops.iter().map(|o| o.id.clone()).collect();
    assert_eq!(ids.len(), 2, "one DocOp per carrier");
    assert!(
        ids.contains(&c1) && ids.contains(&c2),
        "DocOps cover carriers: {ids:?}"
    );
    drop_db(&db).await;

    // --- changeType: a required field added to the schema AFTER a row was
    // inserted leaves that older row without the key (additive push doesn't
    // backfill). Only rows carrying `w` are cast; the pre-existing row is
    // skipped. (changeType can't target an Optional field — no cast is valid for
    // one — so this additive-schema path is the way a carrier/non-carrier split
    // arises.) ---
    let mut db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"name":"old"}"#).await; // inserted before `w` existed
    let expanded = serde_json::from_str::<SchemaDef>(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"w":{"type":"string"}},"indexes":[]}}}"#,
    )
    .unwrap();
    db.schema = push_schema(&db.state.pool, &db.name, expanded)
        .await
        .expect("additive push adds `w`");
    let c1 = insert_doc(&db, "u", r#"{"name":"a","w":"1"}"#).await;
    let c2 = insert_doc(&db, "u", r#"{"name":"b","w":"2"}"#).await;
    let fx = migrate(
        &db,
        r#"{"directives":[{"op":"changeType","table":"u","field":"w","to":{"type":"number"},"cast":"toNumber"}]}"#,
    )
    .await;
    assert_eq!(fx.reports[0].affected_rows, 2, "changeType counts carriers");
    let ids: Vec<String> = fx.ops.iter().map(|o| o.id.clone()).collect();
    assert_eq!(ids.len(), 2, "one DocOp per carrier");
    assert!(
        ids.contains(&c1) && ids.contains(&c2),
        "DocOps cover carriers: {ids:?}"
    );
    drop_db(&db).await;
}

// ---------------------------------------------------------------------------
// Task 6: committer `RunMigrate` arm — the four tap sites + dry-run.
//
// The tests above exercise `apply_migration` directly (manual tx, no committer).
// These tests drive the PUBLIC `Committers::migrate` path so the per-db
// committer task runs the migration and fires the same tap sites a mutate does:
// subscription fan-out, the op-feed, the durable audit log, and (structurally)
// webhook enqueue. Each test asserts the invariant directly.
// ---------------------------------------------------------------------------

/// Drives a migration through the public `Committers::migrate` path (the
/// committer task, with its tap sites), returning the `MigrateResult`. Mirror of
/// the direct `migrate` helper above, but exercises the real committer arm.
async fn migrate_via_committer(db: &Db, request_json: &str) -> MigrateResult {
    let request: MigrateRequest =
        serde_json::from_str(request_json).expect("parse migrate request");
    db.state
        .realtime
        .committers
        .migrate(&db.name, request)
        .await
        .expect("committer migrate")
}

// (T6-a) A migration through the committer fires subscription fan-out: a live
// `collect` query on the migrated table sees the rewritten doc and is re-pushed.
// This is the load-bearing subscription invariant — a migrate that didn't tap
// `subs.fan_out` would silently leave live queries stale.
#[tokio::test]
async fn migrate_fires_subscription_fanout() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await; // no `flag` key yet

    // Subscribe collecting all docs on table `u`. The initial push carries the
    // pre-migration doc (no `flag`).
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    let query: Query =
        serde_json::from_value(serde_json::json!({"table":"u"})).expect("parse query");
    db.state
        .realtime
        .committers
        .subscribe(&db.name, conn, "q1".to_string(), query, tx, None)
        .await
        .expect("subscribe");
    let _initial = rx.try_recv().expect("initial query update");

    // setDefault populates `flag` on rows lacking it; fan_out must re-run the
    // subscription, the result changes, and a push follows.
    let res = migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}]}"#,
    )
    .await;
    assert!(res.applied, "migrate committed");

    let msg = rx.try_recv().expect("subscription push after migrate");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            let docs = result.as_array().expect("docs array");
            assert_eq!(docs.len(), 1, "one doc in the live result: {docs:?}");
            assert_eq!(docs[0]["n"], 1, "same doc, identified by n");
            assert_eq!(
                docs[0]["flag"],
                serde_json::json!(true),
                "migrate's setDefault landed and fanned out: {docs:?}"
            );
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    assert!(
        rx.try_recv().is_err(),
        "no further pushes expected after the migrate fan-out"
    );
    drop_db(&db).await;
}

// (T6-b) A migration through the committer publishes its DocOps to the op-feed
// (the live activity ring `/admin/stream` and `/admin/ops/recent` read). The
// op-feed event carries db/table/kind but no `source` field, so we assert on
// kind=Patch — the `source = "migrate"` distinction lives in audit/webhooks.
#[tokio::test]
async fn migrate_publishes_to_op_feed() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;

    migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}]}"#,
    )
    .await;

    let events = db
        .state
        .realtime
        .op_feed
        .recent(Some(&db.name), Some("u"), 16)
        .await;
    assert!(
        events.iter().any(|e| e.kind == OpKind::Patch),
        "op-feed should carry a Patch DocOp from the migrate: {events:?}"
    );
    drop_db(&db).await;
}

// (T6-c) With audit enabled, a migration through the committer writes one
// `rtdb.audit_log` row per migrated DocOp with `source = 'migrate'` and a NULL
// principal (migrate carries no interactive principal, like a scheduled job).
// Mirrors the audit assertions in `audit_test.rs` but for the migrate tap.
#[tokio::test]
async fn migrate_writes_audit_row_when_enabled() {
    let state = test_state_with_audit().await;
    let db = setup_db_with_schema_in(
        state,
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;

    migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}]}"#,
    )
    .await;

    // The physical column is `tbl` (renamed to `table` only in the serialized
    // AuditEntry). Filter by db so the shared global audit_log stays deterministic.
    let rows: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
        "SELECT db, tbl, principal, source \
         FROM rtdb.audit_log WHERE db = $1 ORDER BY id ASC",
    )
    .bind(&db.name)
    .fetch_all(&db.state.pool)
    .await
    .expect("fetch audit rows");

    assert_eq!(rows.len(), 1, "one audit row per migrated doc: {rows:?}");
    assert_eq!(rows[0].0, db.name, "db");
    assert_eq!(rows[0].1, "u", "tbl");
    assert!(
        rows[0].2.is_none(),
        "principal null (migrate owner=None): {:?}",
        rows[0]
    );
    assert_eq!(rows[0].3, "migrate", "source is migrate");
    drop_db(&db).await;
}

// (T6-d) dry_run runs the DDL+DML only to collect a preview; it commits nothing
// and publishes through no tap site. The doc is unchanged and `applied` is false.
#[tokio::test]
async fn migrate_dry_run_commits_nothing() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"n":1}"#).await;

    let res = migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}],"dryRun":true}"#,
    )
    .await;
    assert!(!res.applied, "dry_run must not mark applied");
    // The preview still reports what WOULD have happened: one row affected.
    assert_eq!(
        res.directives[0].affected_rows, 1,
        "dry-run preview reports the would-be affected row"
    );

    // The tx was rolled back: the doc has no `flag` key.
    let doc = get_doc(&db, "u", &id).await;
    assert!(
        doc.get("flag").is_none(),
        "dry-run committed nothing, doc unchanged: {doc:?}"
    );
    drop_db(&db).await;
}

// (T6-d2) dry_run fires NO tap site: not the op-feed, not the audit log.
// `migrate_dry_run_commits_nothing` above asserts the doc is unchanged; this
// asserts the tap sites themselves stay empty — the load-bearing "every durable
// write publishes here" guarantee must not fire on a rolled-back preview.
#[tokio::test]
async fn migrate_dry_run_fires_no_tap_sites() {
    let state = test_state_with_audit().await;
    let db = setup_db_with_schema_in(
        state,
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;

    let res = migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}],"dryRun":true}"#,
    )
    .await;
    assert!(!res.applied);

    // Op-feed carries nothing for this db/table.
    let feed = db
        .state
        .realtime
        .op_feed
        .recent(Some(&db.name), Some("u"), 16)
        .await;
    assert!(
        feed.is_empty(),
        "dry-run must not publish to op-feed: {feed:?}"
    );

    // Audit log (enabled here) carries no migrate row.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1")
        .bind(&db.name)
        .fetch_one(&db.state.pool)
        .await
        .expect("count audit rows");
    assert_eq!(count, 0, "dry-run must not write an audit row");
    drop_db(&db).await;
}

// (T6-e) Concurrency durability: a migrate and a concurrent mutate submitted
// against the SAME database both land — neither write is lost. The two futures
// are fired via `tokio::join!` and the assertions below are order-independent
// (`any`), so this proves no-lost-write DURABILITY under concurrent same-db
// submit, NOT strict per-operation ordering. The assertions would pass under
// any execution that doesn't drop a write; that is exactly the property under
// test.
//
// Serialization — that the committer in fact runs one op to completion (with
// fan-out) before dequeuing the next — is STRUCTURALLY guaranteed by the per-db
// single committer task: `Committers::channel_for` spawns one mpsc channel plus
// one committer task per database (committer.rs ~lines 151-196), the same
// invariant every existing mutate relies on. It is not asserted here by timing
// because a deterministic ordering probe would require a slow-migrate hook that
// doesn't exist, and the project forbids committing flaky timing assertions.
//
// (The cross-db case — a mutate on dbB is NOT blocked by a migrate on dbA — is
// structurally guaranteed the same way: independent channels/tasks per db.)
#[tokio::test]
async fn migrate_and_concurrent_mutate_both_land_same_db() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;

    let migrate_fut = migrate_via_committer(
        &db,
        r#"{"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}]}"#,
    );
    let mutate_doc: serde_json::Map<String, serde_json::Value> = serde_json::json!({"n":2})
        .as_object()
        .expect("object")
        .clone();
    let mutate_fut = async {
        db.state
            .realtime
            .committers
            .mutate(
                &db.name,
                None,
                Transaction {
                    steps: vec![Step::Insert {
                        table: "u".into(),
                        doc: mutate_doc,
                    }],
                },
                None,
            )
            .await
            .expect("concurrent mutate")
    };

    // Both go through db.name's single channel; the committer runs one to
    // completion (including fan-out) before dequeuing the next.
    let (migrate_res, _outcome) = tokio::join!(migrate_fut, mutate_fut);
    assert!(migrate_res.applied, "migrate committed");

    // Serialization didn't drop either write. Order-independent: if the migrate
    // runs first it sets `flag` only on the pre-existing doc; if the mutate
    // runs first the migrate then sets `flag` on both. Either way the table
    // holds both an n=2 row (the insert) and a flagged row (the setDefault).
    let docs = query_docs(&db, "u").await;
    assert_eq!(docs.len(), 2, "both writes landed: {docs:?}");
    assert!(
        docs.iter()
            .any(|d| d.get("n").and_then(|v| v.as_f64()) == Some(2.0)),
        "concurrent mutate's insert landed: {docs:?}"
    );
    assert!(
        docs.iter()
            .any(|d| d.get("flag").and_then(|v| v.as_bool()) == Some(true)),
        "migrate's setDefault landed: {docs:?}"
    );
    drop_db(&db).await;
}

// ---------------------------------------------------------------------------
// Task 7: HTTP route `POST /admin/db/{db}/migrate`.
//
// The admin-gated public surface over the committer's migrate arm. Mirrors the
// admin-auth pattern in tests/admin_test.rs: admin bearer applies; a missing
// bearer -> 401; an unknown db -> 404. The route runs `require_admin` before
// `database_exists`, so the auth gate rejects without touching migration state.
// ---------------------------------------------------------------------------

// (T7-a) admin bearer applies a setDefault directive through the HTTP route;
// the doc is rewritten through the committer (with fan-out + taps) and the
// response reports `applied: true` plus the per-directive affected-row count.
#[tokio::test]
async fn http_migrate_applies_with_admin_key() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"n":1}"#).await;
    let addr = spawn_app(db.state.clone()).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        serde_json::json!({"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["applied"], true, "applied: {body:?}");
    assert_eq!(
        body["directives"][0]["affectedRows"], 1,
        "one row affected: {body:?}"
    );

    // The doc was rewritten through the committer (fan-out + taps fire).
    let doc = get_doc(&db, "u", &id).await;
    assert_eq!(doc["flag"], serde_json::json!(true));
    drop_db(&db).await;
}

// (T7-b) missing admin bearer -> 401 UNAUTHORIZED. The db exists, so the only
// reason for 401 is the auth gate (`require_admin` runs before `database_exists`).
#[tokio::test]
async fn http_migrate_rejects_missing_admin() {
    let db =
        setup_db_with_schema(r#"{"tables":{"u":{"fields":{"n":{"type":"number"}},"indexes":[]}}}"#)
            .await;
    let addr = spawn_app(db.state.clone()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{}/migrate", db.name))
        .json(&serde_json::json!({"directives":[]}))
        .send()
        .await
        .expect("send migrate request");
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "UNAUTHORIZED");
    drop_db(&db).await;
}

// (T7-c) migrate against a database that was never created -> 404 NOT_FOUND
// (mirrors /admin/db/{db}/query and /admin/db/{db}/mutate).
#[tokio::test]
async fn http_migrate_unknown_db_404() {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let bogus = format!("t{}", uuid::Uuid::now_v7().simple());

    let resp = admin_post(
        addr,
        &format!("/admin/db/{bogus}/migrate"),
        serde_json::json!({"directives":[]}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "NOT_FOUND");
}

// (T7-d) dryRun commits nothing and reports `applied: false`. The doc is
// unchanged — confirms the route threads `dryRun` through to the committer,
// which rolls the migration tx back and publishes through no tap site.
#[tokio::test]
async fn http_migrate_dry_run() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"flag":{"type":"optional","inner":{"type":"boolean"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"n":1}"#).await;
    let addr = spawn_app(db.state.clone()).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        serde_json::json!({"directives":[{"op":"setDefault","table":"u","field":"flag","value":true}],"dryRun":true}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["applied"], false, "dryRun not applied: {body:?}");

    let doc = get_doc(&db, "u", &id).await;
    assert!(
        doc.get("flag").is_none(),
        "dryRun committed nothing, doc unchanged: {doc:?}"
    );
    drop_db(&db).await;
}
