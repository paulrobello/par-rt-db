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
use rtdb_server::auth::PrincipalCtx;
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
    name: common::TestDb,
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
    let name = common::wrap_test_db(name);
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
        &PrincipalCtx::bypass(),
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

// ---- ENH-020 Stage 1: typed ValueExpr grammar (SEC-107 structural close) ----
//
// The typed path replaces raw-SQL `expr`/`where` with a closed `ValueExpr` /
// `FilterExpr`. Every literal is bound `$n`; every field reads `doc->'field'`
// and is schema-validated. There is no raw-SQL node, so the SEC-107 injection
// concern cannot arise from a typed payload. The legacy string form is retained
// for one deprecation cycle (dual-accept).

// (n) A typed ValueExpr::Upper(Field) mirrors legacy test (k): `upper` is set to
// the uppercased `name`. The expr object deserializes through the `ValueExpr`
// arm of the untagged `ExprSource`, not the legacy string arm.
#[tokio::test]
async fn eval_expr_typed_upper_field() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"upper":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"upper","expr":{"op":"upper","value":{"op":"field","field":"name"}}}]}"#,
    )
    .await;
    assert_eq!(get_doc(&db, "u", &id).await["upper"], "ADA");
    drop_db(&db).await;
}

// (o) A typed ValueExpr with a typed FilterExpr `where` scopes the rewrite —
// mirrors legacy test (l). `Concat` builds "x-" + the `tag` field.
#[tokio::test]
async fn eval_expr_typed_where_filters() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"tag":{"type":"string"},"label":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1,"tag":"a"}"#).await;
    insert_doc(&db, "u", r#"{"n":2,"tag":"b"}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"label","expr":{"op":"concat","parts":[{"op":"literal","value":"x-"},{"op":"field","field":"tag"}]},"where":{"op":"gte","field":"n","value":2}}]}"#,
    )
    .await;
    let docs = query_docs(&db, "u").await;
    assert!(
        docs.iter()
            .any(|d| d.get("label").and_then(|v| v.as_str()) == Some("x-b")),
        "n=2 row should have label=x-b, got {docs:?}"
    );
    let n1 = docs
        .iter()
        .find(|d| d.get("n").and_then(|v| v.as_f64()) == Some(1.0))
        .expect("n=1 row exists");
    assert!(
        n1.get("label").is_none(),
        "n=1 row should not carry label (predicate excluded it), got {n1:?}"
    );
    drop_db(&db).await;
}

// (p) SEC-107 structural close: a hostile ValueExpr object with an UNKNOWN `op`
// fails to deserialize — it is rejected at the JSON boundary, never reaching the
// compiler or the admin gate. A denylist is not involved; the grammar is closed
// (`deny_unknown_fields` + a fixed variant set). This is the structural property
// the legacy raw-SQL path could never provide. The two verified SEC-107 bypass
// shapes — a newline before `FROM` and a bare `SELECT current_setting(...)`
// without `FROM` — were attacks against the old denylist's *string* matching; on
// the typed path neither can be expressed at all (there is no raw-SQL node), so
// they are rejected structurally here, not by pattern. The legacy string path
// still accepts them, gated to the root admin_key (see the sec107_* tests below).
#[tokio::test]
async fn eval_expr_typed_rejects_unknown_op() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    // A would-be SQL payload (either SEC-107 bypass shape) wrapped in a bogus op
    // fails to deserialize — the closed variant set is the boundary.
    let request: Result<MigrateRequest, _> = serde_json::from_str(
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"x","expr":{"op":"rawSql","sql":"(SELECT current_setting('...')\nFROM rtdb_auth.machine_tokens)"}}]}"#,
    );
    assert!(
        request.is_err(),
        "an unknown ValueExpr op (incl. any raw-SQL bypass) must fail to deserialize, got {request:?}"
    );
    drop_db(&db).await;
}

// (q) SEC-107 regression: a ValueExpr::Literal carrying SQL metacharacters lands
// as a bound DATA value, not interpolated SQL. The literal is set verbatim into
// the doc — if it were interpolated, the single quotes would break the statement
// (a syntax error) or execute. Binding proves the value is inert.
#[tokio::test]
async fn eval_expr_typed_literal_is_bound_not_interpolated() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"note":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let hostile = "'); DROP TABLE t_u; --";
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    let req = format!(
        r#"{{"directives":[{{"op":"evalExpr","table":"u","set":"note","expr":{{"op":"literal","value":"{hostile}"}}}}]}}"#
    );
    migrate(&db, &req).await;
    // The hostile string is stored verbatim as data — the table still exists and
    // the value round-trips exactly.
    assert_eq!(get_doc(&db, "u", &id).await["note"], hostile);
    let schema_name = format!("db_{}", db.name);
    assert!(
        relation_exists(&db, &format!("{schema_name}.t_u")).await,
        "table must survive a hostile literal (bound, not executed)"
    );
    drop_db(&db).await;
}

// (r) Typed expr rejects an undeclared field reference at plan time — the field
// is validated against the table's TableDef before any DB work. Rejected by
// `plan_migration` (pure, no DB), so the migration never reaches apply.
#[tokio::test]
async fn eval_expr_typed_rejects_undeclared_field() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"x","expr":{"op":"field","field":"nonexistent"}}]}"#,
    )
    .expect("parse");
    let err = plan_migration(&db.schema, &request.directives).expect_err("plan rejects");
    assert!(
        err.message.contains("undeclared field"),
        "expected undeclared-field error, got: {}",
        err.message
    );
    drop_db(&db).await;
}

// (s) Dual-accept guard: a typed `expr` with a legacy raw-SQL `where` is
// rejected — the two sources may not mix. The typed path requires a typed
// predicate so the whole statement is parameter-bound.
#[tokio::test]
async fn eval_expr_typed_rejects_legacy_where_mix() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let err = migrate_err(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"x","expr":{"op":"literal","value":1},"where":"true"}]}"#,
    )
    .await;
    assert!(
        err.message.contains("typed 'where'"),
        "expected typed-where requirement error, got: {}",
        err.message
    );
    drop_db(&db).await;
}

// (t) A typed Case expression: classify rows by a FilterExpr predicate. Proves
// the Case arm compiles its `when` predicates via the read path's compile_filter
// (field-validated, bound) and its `then`/`otherwise` via compile_value_expr.
#[tokio::test]
async fn eval_expr_typed_case() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"n":{"type":"number"},"band":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"n":1}"#).await;
    insert_doc(&db, "u", r#"{"n":5}"#).await;
    insert_doc(&db, "u", r#"{"n":20}"#).await;
    migrate(
        &db,
        r#"{"directives":[{"op":"evalExpr","table":"u","set":"band","expr":{"op":"case","whens":[{"when":{"op":"lt","field":"n","value":5},"then":{"op":"literal","value":"low"}},{"when":{"op":"lt","field":"n","value":10},"then":{"op":"literal","value":"mid"}}],"otherwise":{"op":"literal","value":"high"}}}]}"#,
    )
    .await;
    let docs = query_docs(&db, "u").await;
    let band_of = |n: f64| {
        docs.iter()
            .find(|d| d.get("n").and_then(|v| v.as_f64()) == Some(n))
            .and_then(|d| d.get("band"))
            .and_then(|v| v.as_str())
            .unwrap_or("?")
            .to_string()
    };
    assert_eq!(band_of(1.0), "low");
    assert_eq!(band_of(5.0), "mid");
    assert_eq!(band_of(20.0), "high");
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
        .subscribe(
            &db.name,
            conn,
            "q1".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
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
        .recent(Some(db.name.as_str()), Some("u"), 16)
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
    .bind(db.name.as_str())
    .fetch_all(&db.state.pool)
    .await
    .expect("fetch audit rows");

    assert_eq!(rows.len(), 1, "one audit row per migrated doc: {rows:?}");
    assert_eq!(rows[0].0, db.name.as_str(), "db");
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
        .recent(Some(db.name.as_str()), Some("u"), 16)
        .await;
    assert!(
        feed.is_empty(),
        "dry-run must not publish to op-feed: {feed:?}"
    );

    // Audit log (enabled here) carries no migrate row.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1")
        .bind(db.name.as_str())
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
                PrincipalCtx::bypass(),
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

// Regression for BUG 1: `dropIndex` used to drop the Postgres index but leave
// its backing `f_<field>` column orphaned. The next `push_schema` then treated
// the field as newly-indexed and ran `ALTER TABLE ADD COLUMN f_<field>` (no
// `IF NOT EXISTS`), so Postgres rejected "column already exists" → 500
// INTERNAL. The fix both drops the orphan column in `dropIndex` AND makes
// `ADD COLUMN` tolerant. This test reproduces the full sequence.
#[tokio::test]
async fn drop_index_then_re_push_does_not_collide() {
    // 1. Push a table with an indexed field and insert a row so the backing
    //    `f_name` column is real (not just schema metadata).
    let mut db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada"}"#).await;
    let schema_name = format!("db_{}", db.name);

    // 2. migrate dropIndex. Done inline (rather than via the `migrate` helper)
    //    so we also persist the derived schema to the `meta` table, mirroring
    //    production `committer::handle_migrate` — otherwise a later push_schema
    //    would still see the pre-migration schema and the test would not
    //    reproduce the real bug. Should drop the index AND its orphan `f_name`
    //    column (the fix); before the fix the column was left behind.
    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"dropIndex","table":"users","name":"by_name"}]}"#,
    )
    .expect("parse migrate request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan migration");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    apply_migration(
        &mut tx,
        &db.name,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await
    .expect("apply migration");
    // Persist derived schema (same upsert handle_migrate uses).
    let schema_json = serde_json::to_value(&derived).expect("serialize derived schema");
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await
    .expect("persist derived schema");
    tx.commit().await.expect("commit migration tx");
    db.schema = derived;
    assert!(
        !relation_exists(&db, &format!("\"{schema_name}\".\"i_users_by_name\"")).await,
        "index dropped"
    );

    // 3. push_schema again with the SAME index re-added. Before the fix this
    //    500'd with "column f_name already exists"; after the fix it succeeds.
    db.schema = serde_json::from_str(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}]}}}"#,
    )
    .expect("parse re-push schema");
    push_schema(&db.state.pool, &db.name, db.schema.clone())
        .await
        .expect("re-push schema after dropIndex must not collide");

    // 4. The re-added index works: insert + query by the indexed field, proving
    //    the column is healthy (present, typed, and populated by backfill).
    let id2 = insert_doc(&db, "users", r#"{"name":"Grace"}"#).await;
    let (col,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_name\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_name for pre-migration row (backfilled)");
    assert_eq!(col, "Ada");
    let (col2,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_name\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id2)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_name for new row");
    assert_eq!(col2, "Grace");
    drop_db(&db).await;
}

// Proves the `still_indexed` guard in `Directive::DropIndex`'s orphan-column
// cleanup: when TWO btree indexes SHARE a field, dropping ONE must NOT drop the
// shared `f_` column, because the SURVIVING index still depends on it. The
// existing `drop_index_then_re_push_does_not_collide` test only exercises the
// single-index-owns-the-field case, so it would pass even if the guard
// (`!still_indexed.contains(field)`) were broken — this test makes the guard
// load-bearing. A broken guard would (a) drop `f_name`, (b) cascade-drop the
// surviving `i_users_by_name_owner` index (Postgres drops indexes that reference
// a dropped column), and (c) leave the column missing from
// `information_schema.columns`, all of which the assertions below catch.
#[tokio::test]
async fn drop_index_keeps_shared_column_when_another_index_uses_it() {
    // 1. Two btree indexes that share `name`; `owner` is the second field of
    //    the composite index.
    let mut db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{
            "name":{"type":"string"},
            "owner":{"type":"string"}
        },"indexes":[
            {"name":"by_name","fields":["name"]},
            {"name":"by_name_owner","fields":["name","owner"]}
        ]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "users", r#"{"name":"Ada","owner":"u1"}"#).await;
    let schema_name = format!("db_{}", db.name);

    // 2. migrate dropIndex ONE of the shared-column indexes (`by_name`).
    //    Inline (not via the `migrate` helper) so the derived schema is persisted
    //    to the `meta` table, mirroring production `committer::handle_migrate` —
    //    otherwise a later push_schema would see the pre-migration schema.
    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[{"op":"dropIndex","table":"users","name":"by_name"}]}"#,
    )
    .expect("parse migrate request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan migration");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    apply_migration(
        &mut tx,
        &db.name,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await
    .expect("apply migration");
    let schema_json = serde_json::to_value(&derived).expect("serialize derived schema");
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await
    .expect("persist derived schema");
    tx.commit().await.expect("commit migration tx");
    db.schema = derived;

    // 3. The dropped index is gone.
    assert!(
        !relation_exists(&db, &format!("\"{schema_name}\".\"i_users_by_name\"")).await,
        "dropped index is gone"
    );
    // 4. THE LOAD-BEARING ASSERTION: the shared `f_name` column SURVIVES because
    //    `by_name_owner` still uses `name`. A broken guard would have dropped it.
    let (f_name_count,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 't_users' AND column_name = 'f_name'",
    )
    .bind(&schema_name)
    .fetch_one(&db.state.pool)
    .await
    .expect("information_schema lookup for f_name");
    assert_eq!(
        f_name_count, 1,
        "f_name must survive dropIndex: the surviving by_name_owner index still uses it"
    );
    // The pre-migration row's `f_name` value was preserved through the dropIndex
    // (the column was not dropped, so its data is intact).
    let (name1,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_name\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_name for pre-migration row");
    assert_eq!(name1, "Ada");
    // 5. The surviving composite index is intact (Postgres would have
    //    cascade-dropped it had `f_name` been dropped).
    assert!(
        relation_exists(&db, &format!("\"{schema_name}\".\"i_users_by_name_owner\"")).await,
        "surviving index i_users_by_name_owner still exists"
    );
    // 6. The surviving index still WORKS: insert a new row and confirm its
    //    backing typed columns (`f_name`, `f_owner`) are populated by backfill.
    let id2 = insert_doc(&db, "users", r#"{"name":"Grace","owner":"u2"}"#).await;
    let (name2,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_name\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id2)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_name for new row (surviving index column)");
    assert_eq!(name2, "Grace");
    let (owner2,): (String,) = sqlx::query_as(&format!(
        "SELECT \"f_owner\" FROM \"{schema_name}\".\"t_users\" WHERE id = $1"
    ))
    .bind(&id2)
    .fetch_one(&db.state.pool)
    .await
    .expect("fetch f_owner for new row (surviving index column)");
    assert_eq!(owner2, "u2");
    // 7. Re-adding the dropped index via push_schema succeeds — the surviving
    //    `f_name` column is already present and push_schema's tolerant ADD COLUMN
    //    handles it. (Confirms no orphan-related collision either way.)
    db.schema = serde_json::from_str(
        r#"{"tables":{"users":{"fields":{
            "name":{"type":"string"},
            "owner":{"type":"string"}
        },"indexes":[
            {"name":"by_name","fields":["name"]},
            {"name":"by_name_owner","fields":["name","owner"]}
        ]}}}"#,
    )
    .expect("parse re-push schema");
    push_schema(&db.state.pool, &db.name, db.schema.clone())
        .await
        .expect("re-adding by_name must not collide with the surviving f_name column");
    drop_db(&db).await;
}

/// `information_schema.columns` membership check for a physical column on a
/// table. Used by the search/vector column-leak test below.
async fn column_exists(db: &Db, table: &str, col: &str) -> bool {
    let schema_name = format!("db_{}", db.name);
    let table_ident = format!("t_{}", table.to_lowercase());
    let (n,): (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
    )
    .bind(&schema_name)
    .bind(&table_ident)
    .bind(col)
    .fetch_one(&db.state.pool)
    .await
    .expect("information_schema lookup");
    n > 0
}

// Proves `Directive::DropIndex` drops a dropped search index's generated `s_`
// tsvector column AND a dropped vector index's `v_` vector(N) column — parity
// with `ddl::reconcile_diff`'s `drop_search_cols`/`drop_vector_cols`. Before
// the fix, migrate::DropIndex dropped only the index and its `f_` columns,
// leaving `s_`/`v_` orphaned, so the next push_schema re-creating the index
// failed with "column already exists" (the leak the reconcile path already
// fixed but the migrate path did not mirror).
#[tokio::test]
async fn drop_index_drops_search_and_vector_columns() {
    let schema_json = r#"{"tables":{"docs":{"fields":{
        "title":{"type":"string"},
        "body":{"type":"string"},
        "embedding":{"type":"vector","dimensions":3}
    },"indexes":[
        {"name":"search_body","fields":["title","body"],"search":true},
        {"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":3}}
    ]}}}"#;
    let mut db = setup_db_with_schema(schema_json).await;
    // Insert so the maintained columns are real, not just schema metadata.
    let _ = insert_doc(
        &db,
        "docs",
        r#"{"title":"t","body":"b","embedding":[1.0,0.0,0.0]}"#,
    )
    .await;
    let schema_name = format!("db_{}", db.name);

    // Both generated columns exist before the migration.
    assert!(
        column_exists(&db, "docs", "s_search_body").await,
        "search tsvector column present before dropIndex"
    );
    assert!(
        column_exists(&db, "docs", "v_by_embedding").await,
        "vector column present before dropIndex"
    );

    // migrate dropIndex both. Inline (persist the derived schema to `meta`) to
    // mirror production `committer::handle_migrate` — a later push_schema loads
    // the live schema from `meta`, so without persisting it the re-push below
    // would not exercise the collision the fix prevents.
    let request: MigrateRequest = serde_json::from_str(
        r#"{"directives":[
            {"op":"dropIndex","table":"docs","name":"search_body"},
            {"op":"dropIndex","table":"docs","name":"by_embedding"}
        ]}"#,
    )
    .expect("parse migrate request");
    let derived = plan_migration(&db.schema, &request.directives).expect("plan migration");
    let mut tx = db.state.pool.begin().await.expect("begin tx");
    apply_migration(
        &mut tx,
        &db.name,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await
    .expect("apply migration");
    let derived_json = serde_json::to_value(&derived).expect("serialize derived schema");
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(derived_json)
    .execute(&mut *tx)
    .await
    .expect("persist derived schema");
    tx.commit().await.expect("commit migration tx");
    db.schema = derived;

    // THE FIX: both generated columns are gone (previously leaked).
    assert!(
        !column_exists(&db, "docs", "s_search_body").await,
        "search tsvector column dropped with its index"
    );
    assert!(
        !column_exists(&db, "docs", "v_by_embedding").await,
        "vector column dropped with its index"
    );

    // Re-pushing the same schema must not collide on the (now-dropped) columns.
    db.schema = serde_json::from_str(schema_json).expect("parse re-push schema");
    push_schema(&db.state.pool, &db.name, db.schema.clone())
        .await
        .expect("re-push schema after dropIndex must not collide");
    drop_db(&db).await;
}

// ---- SEC-107: evalExpr root-admin gate -------------------------------------
//
// `evalExpr` interpolates client SQL text into an UPDATE; containment is now
// structural (root admin_key only), not a substring denylist. These exercise
// the HTTP gate in `admin_migrate` — the one path the denylist used to guard.

// POST helper carrying an arbitrary bearer (the OAuth-allowlist-admin session
// token), mirroring dashboard_test's `bearer_get`.
async fn bearer_post_migrate(
    addr: std::net::SocketAddr,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("send bearer migrate request")
}

// Seeds a user + session + rtdb_auth.admins row and returns the session bearer.
// Distinct uuid-suffixed email/user_id per call so the shared dev Postgres's
// UNIQUE/PK constraints never collide across test runs.
async fn seed_oauth_allowlist_admin(state: &Arc<AppState>) -> String {
    let suffix = uuid::Uuid::now_v7().simple();
    let email = format!("migrate-admin-{suffix}@example.com");
    let user_id = format!("u{suffix}");
    let token = common::mint_user_session(&state.pool, &user_id, &email).await;
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind(&email)
        .bind(rtdb_server::db::now_ms())
        .execute(&state.pool)
        .await
        .expect("insert rtdb_auth.admins row");
    token
}

// SEC-107: an evalExpr directive submitted by a delegated (OAuth-allowlist)
// dashboard admin is rejected 403 at the admin gate — it must not reach the
// raw-SQL applier. This is the verified exploit path closed by the gate: a
// delegated admin could otherwise read rtdb_auth.machine_tokens/sessions/admins
// or any tenant's documents via `expr = (SELECT ... \nFROM rtdb_auth...)` and
// read them back through /api/query. The newline-before-`FROM` and bare
// `SELECT current_setting(...)` shapes bypassed the old denylist; the control
// is now the root-admin gate, which this test exercises directly.
#[tokio::test]
async fn sec107_evalexpr_rejected_for_oauth_allowlist_admin() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    let addr = spawn_app(db.state.clone()).await;
    let token = seed_oauth_allowlist_admin(&db.state).await;

    let resp = bearer_post_migrate(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        &token,
        serde_json::json!({"directives":[
            {"op":"evalExpr","table":"u","set":"upper","expr":"upper(doc->>'name')"}
        ]}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "evalExpr from a delegated admin must be 403"
    );
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["code"], "FORBIDDEN", "body: {body:?}");
    drop_db(&db).await;
}

// SEC-107: the SAME evalExpr directive is admitted under the root admin_key
// (the dashboard CLI / `rtdb` CLI automation path). The root admin_key holder
// already has full server/DB access, so evalExpr under it does not expand
// their reach — the gate rejects only the delegated-admin tier.
#[tokio::test]
async fn sec107_evalexpr_allowed_for_root_admin_key() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    let addr = spawn_app(db.state.clone()).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        serde_json::json!({"directives":[
            {"op":"evalExpr","table":"u","set":"upper","expr":"upper(doc->>'name')"}
        ]}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "root admin admits evalExpr"
    );
    // The rewrite actually ran.
    assert_eq!(get_doc(&db, "u", &id).await["upper"], "ADA");
    drop_db(&db).await;
}

// SEC-107: a NON-evalExpr directive (renameField here) remains available to a
// delegated admin — the gate is scoped to evalExpr only, not the whole migrate
// route. Confirms the fix does not over-broadly lock down admin operations.
#[tokio::test]
async fn sec107_non_evalexpr_directive_allowed_for_oauth_allowlist_admin() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    let addr = spawn_app(db.state.clone()).await;
    let token = seed_oauth_allowlist_admin(&db.state).await;

    let resp = bearer_post_migrate(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        &token,
        serde_json::json!({"directives":[
            {"op":"renameField","table":"u","from":"name","to":"displayName"}
        ]}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "non-evalExpr directives stay available to delegated admins"
    );
    drop_db(&db).await;
}

// ENH-020 / SEC-107: a TYPED evalExpr (closed ValueExpr grammar, every literal
// bound, every field schema-validated) has no SQL-injection surface, so it is
// admitted for a delegated (OAuth-allowlist) dashboard admin. This is the
// capability the typed grammar unlocks — safe backfills without the root key.
// The legacy raw-SQL form remains root-only (the preceding test).
#[tokio::test]
async fn enh020_typed_evalexpr_allowed_for_oauth_allowlist_admin() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"},"upper":{"type":"optional","inner":{"type":"string"}}},"indexes":[]}}}"#,
    )
    .await;
    let id = insert_doc(&db, "u", r#"{"name":"ada"}"#).await;
    let addr = spawn_app(db.state.clone()).await;
    let token = seed_oauth_allowlist_admin(&db.state).await;

    let resp = bearer_post_migrate(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        &token,
        serde_json::json!({"directives":[
            {"op":"evalExpr","table":"u","set":"upper",
             "expr":{"op":"upper","value":{"op":"field","field":"name"}}}
        ]}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::OK,
        "typed evalExpr should be admitted for a delegated admin (no SQL-injection surface)"
    );
    assert_eq!(get_doc(&db, "u", &id).await["upper"], "ADA");
    drop_db(&db).await;
}

// ENH-020 / SEC-107: a delegated admin who mixes a typed `expr` with a legacy
// raw-SQL `where` is still rejected 403 — the legacy `where` carries the
// injection surface, so the root-admin gate fires on it.
#[tokio::test]
async fn enh020_typed_expr_legacy_where_rejected_for_delegated_admin() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let addr = spawn_app(db.state.clone()).await;
    let token = seed_oauth_allowlist_admin(&db.state).await;

    let resp = bearer_post_migrate(
        addr,
        &format!("/admin/db/{}/migrate", db.name),
        &token,
        serde_json::json!({"directives":[
            {"op":"evalExpr","table":"u","set":"x",
             "expr":{"op":"literal","value":1},"where":"true"}
        ]}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "legacy raw-SQL 'where' must still require the root admin key"
    );
    drop_db(&db).await;
}

// ---- SEC-124: real identifier checks (release-mode) ------------------------
//
// The prior `debug_assert!(is_valid_identifier(...))` backstops compiled away
// under `--release` (the Dockerfile builds release). These tests reach the NEW
// real checks in the apply layer via `migrate_err`, which calls plan_migration
// + apply_migration but NOT the upstream `derived.validate()` — so the apply
// check is the only guard. `to` is a fresh identifier not existence-checked by
// `validate_one`, so an invalid value reaches the apply function and the real
// check fires.

// SEC-124: renameField with a quote-bearing `to` is rejected BAD_REQUEST at the
// apply layer (the real check that replaced `debug_assert!`). A stray quote
// would otherwise break the `doc - '{from}'` / `jsonb_set` path literal SQL.
#[tokio::test]
async fn sec124_rename_field_rejects_invalid_target_identifier() {
    let db = setup_db_with_schema(
        r#"{"tables":{"u":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let err = migrate_err(
        &db,
        r#"{"directives":[{"op":"renameField","table":"u","from":"name","to":"bad'id"}]}"#,
    )
    .await;
    assert_eq!(err.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains("invalid field identifier"),
        "should name the field-identifier rejection: {}",
        err.message
    );
    drop_db(&db).await;
}

// SEC-124: renameTable with a quote-bearing `to` is rejected BAD_REQUEST at the
// apply layer — a NEW check at a site that previously had NO backstop at all
// (the sibling-interpolation gap SEC-124 called out).
#[tokio::test]
async fn sec124_rename_table_rejects_invalid_target_identifier() {
    let db = setup_db_with_schema(
        r#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#,
    )
    .await;
    let err = migrate_err(
        &db,
        r#"{"directives":[{"op":"renameTable","from":"users","to":"bad'table"}]}"#,
    )
    .await;
    assert_eq!(err.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        err.message.contains("invalid table identifier"),
        "should name the table-identifier rejection: {}",
        err.message
    );
    drop_db(&db).await;
}
