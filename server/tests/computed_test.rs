//! ENH-028 — declarative computed fields, server write-path half. A schema
//! field declared `computed: <ValueExpr>` is re-derived by `stamp_computed` on
//! every write (insert / patch / replace / upsert both branches / patchByQuery)
//! and stored in the doc body AND its typed column — which is what makes the
//! field indexable. A null result leaves the key absent (the unset-optional
//! shape convention); an evaluation error fails the whole write. A
//! client-supplied computed value never survives.
//!
//! The later tests (9–14) cover Task 4: the migrate interplay (renameField /
//! dropField / changeType / evalExpr) and the push backfill.

use crate::common::{test_state, wrap_test_db};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::migrate::{Directive, MigrationEffects, apply_migration, plan_migration};
use rtdb_server::query::{FilterExpr, Order, Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use sqlx::PgPool;

fn computed_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
        "users":{
            "fields":{
                "first":{"type":"string"},
                "last":{"type":"string"},
                "fullName":{"type":"string"},
                "nickname":{"type":"optional","inner":{"type":"string"}},
                "nick":{"type":"optional","inner":{"type":"string"}}},
            "indexes":[{"name":"by_fullName","fields":["fullName"]},
                       {"name":"by_first","fields":["first"]}],
            "computed":{
                "fullName":{"op":"concat","parts":[
                    {"op":"field","field":"first"},
                    {"op":"literal","value":" "},
                    {"op":"field","field":"last"}]},
                "nick":{"op":"coalesce","parts":[{"op":"field","field":"nickname"}]}}
        },
        "metrics":{
            "fields":{
                "denom":{"type":"optional","inner":{"type":"number"}},
                "ratio":{"type":"optional","inner":{"type":"number"}}},
            "indexes":[{"name":"by_denom","fields":["denom"]}],
            "computed":{
                "ratio":{"op":"div",
                    "left":{"op":"field","field":"denom"},
                    "right":{"op":"literal","value":0.0}}}
        }
    }})
}

async fn computed_db(state: &rtdb_server::AppState) -> (crate::common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(computed_schema_json()).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");
    (db, schema)
}

async fn run(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> Result<TxnOutcome, rtdb_server::error::RtDbError> {
    execute_txn(
        pool,
        db,
        schema,
        &Transaction { steps },
        &PrincipalCtx::bypass(),
    )
    .await
}

async fn insert_doc(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    doc: serde_json::Value,
) -> anyhow::Result<String> {
    let outcome = run(
        pool,
        db,
        schema,
        vec![Step::Insert {
            table: table.to_string(),
            doc: doc.as_object().expect("json object").clone(),
        }],
    )
    .await
    .expect("insert txn");
    Ok(outcome.results[0]["id"].as_str().expect("id").to_string())
}

async fn patch_doc(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
    fields: serde_json::Value,
) -> anyhow::Result<()> {
    run(
        pool,
        db,
        schema,
        vec![Step::Patch {
            table: table.to_string(),
            id: id.to_string(),
            fields: fields.as_object().expect("json object").clone(),
        }],
    )
    .await
    .expect("patch txn");
    Ok(())
}

async fn fetch_doc(
    pool: &PgPool,
    db: &str,
    table: &str,
    id: &str,
) -> anyhow::Result<serde_json::Value> {
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_{table}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(doc)
}

fn base_query(table: &str) -> Query {
    Query {
        table: table.to_string(),
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
        distinct: false,
        aggregate: None,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
    }
}

// (1) a client-supplied computed value is overwritten by the stamp, in both
// the doc body and the typed column.
#[tokio::test]
async fn insert_overwrites_client_supplied_computed_value() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace", "fullName": "WRONG"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Lovelace");

    // the typed column carries the stamped value — that is what makes the
    // computed field indexable
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("Ada Lovelace"));
    Ok(())
}

// (2) patch recomputes from the merged doc; a patch key naming the computed
// field directly is dropped, not merged.
#[tokio::test]
async fn patch_recomputes_computed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    patch_doc(
        &pool,
        &db,
        &schema,
        "users",
        &id,
        serde_json::json!({"first": "Grace", "last": "Hopper"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Grace Hopper");

    // a direct write to the computed key is ignored
    patch_doc(
        &pool,
        &db,
        &schema,
        "users",
        &id,
        serde_json::json!({"fullName": "HAX"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Grace Hopper");

    // a WRONG-TYPED client value on the computed key is dropped before
    // validation (not merged-then-overwritten): the write still succeeds
    // where merging first would fail per-field validate_value
    patch_doc(
        &pool,
        &db,
        &schema,
        "users",
        &id,
        serde_json::json!({"fullName": 42}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Grace Hopper");
    Ok(())
}

// (3) replace validates as a complete document, then stamps: the recomputed
// value lands, a client-supplied one never does.
#[tokio::test]
async fn replace_recomputes_computed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "users".to_string(),
            id: id.clone(),
            doc: serde_json::json!({"first": "Alan", "last": "Turing", "fullName": "WRONG"})
                .as_object()
                .expect("json object")
                .clone(),
        }],
    )
    .await
    .expect("replace txn");
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Alan Turing");

    // a WRONG-TYPED client value on the computed key cannot fail the write:
    // it is dropped before validate_doc and the stamp re-derives the field
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "users".to_string(),
            id: id.clone(),
            doc: serde_json::json!({"first": "Ada", "last": "Byron", "fullName": 42})
                .as_object()
                .expect("json object")
                .clone(),
        }],
    )
    .await
    .expect("replace txn");
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Byron");
    Ok(())
}

// (4) upsert recomputes on both branches.
#[tokio::test]
async fn upsert_recomputes_on_both_branches() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    // insert branch: no by_first match yet; the insert doc's "WRONG" loses
    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "users".to_string(),
            index: "by_first".to_string(),
            eq: vec![serde_json::json!("Ada")],
            insert: serde_json::json!({"first": "Ada", "last": "Lovelace", "fullName": "WRONG"})
                .as_object()
                .expect("json object")
                .clone(),
            patch: serde_json::json!({"last": "Unset"})
                .as_object()
                .expect("json object")
                .clone(),
        }],
    )
    .await
    .expect("upsert insert txn");
    assert_eq!(outcome.results[0]["inserted"], true);
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Lovelace");

    // update branch: by_first matches; the patch's inputs recompute
    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "users".to_string(),
            index: "by_first".to_string(),
            eq: vec![serde_json::json!("Ada")],
            insert: serde_json::json!({"first": "Ada", "last": "Ignored"})
                .as_object()
                .expect("json object")
                .clone(),
            patch: serde_json::json!({"last": "Byron"})
                .as_object()
                .expect("json object")
                .clone(),
        }],
    )
    .await
    .expect("upsert update txn");
    assert_eq!(outcome.results[0]["inserted"], false);
    assert_eq!(outcome.results[0]["id"].as_str(), Some(id.as_str()));
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Byron");
    Ok(())
}

// (5) order and count work over the indexed computed field — the typed
// column serves both the scan order and the filter.
#[tokio::test]
async fn order_and_count_work_on_indexed_computed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    for (first, last) in [("Ada", "Lovelace"), ("Grace", "Hopper"), ("Alan", "Turing")] {
        insert_doc(
            &pool,
            &db,
            &schema,
            "users",
            serde_json::json!({"first": first, "last": last}),
        )
        .await?;
    }

    let mut q = base_query("users");
    q.index = Some("by_fullName".to_string());
    q.order = Some(Order::Desc);
    match execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false).await? {
        QueryResult::Docs(docs) => {
            let names: Vec<&str> = docs
                .iter()
                .map(|d| d["fullName"].as_str().expect("fullName string"))
                .collect();
            assert_eq!(names, vec!["Grace Hopper", "Alan Turing", "Ada Lovelace"]);
        }
        other => panic!("expected Docs, got {other:?}"),
    }

    let mut q = base_query("users");
    q.index = Some("by_fullName".to_string());
    q.filter = Some(FilterExpr::Eq {
        field: "fullName".to_string(),
        value: serde_json::json!("Ada Lovelace"),
    });
    q.count = true;
    match execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass(), false).await? {
        QueryResult::Count(n) => assert_eq!(n, 1),
        other => panic!("expected Count, got {other:?}"),
    }
    Ok(())
}

// (6) patchByQuery recomputes per matched row.
#[tokio::test]
async fn patch_by_query_recomputes_computed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Grace", "last": "Hopper"}),
    )
    .await?;
    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::PatchByQuery {
            table: "users".to_string(),
            filter: FilterExpr::Eq {
                field: "first".to_string(),
                value: serde_json::json!("Ada"),
            },
            patch: serde_json::json!({"last": "King"})
                .as_object()
                .expect("json object")
                .clone(),
            limit: None,
        }],
    )
    .await
    .expect("patchByQuery txn");
    assert_eq!(outcome.results[0]["patched"], 1);
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada King");
    Ok(())
}

// (7) a null result leaves the key ABSENT — an unset optional computed field
// is an absent key, never a stored null.
#[tokio::test]
async fn null_result_leaves_computed_key_absent() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert!(
        doc.get("nick").is_none(),
        "null coalesce must not store a key"
    );

    // the input arriving makes the expr non-null → key present
    patch_doc(
        &pool,
        &db,
        &schema,
        "users",
        &id,
        serde_json::json!({"nickname": "Ace"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["nick"], "Ace");

    // clearing the input re-nulls the expr → key removed again
    patch_doc(
        &pool,
        &db,
        &schema,
        "users",
        &id,
        serde_json::json!({"nickname": null}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert!(doc.get("nick").is_none());
    assert!(doc.get("nickname").is_none());
    Ok(())
}

// (8) a runtime evaluation error fails the whole write (BAD_REQUEST, naming
// the field) and the stored doc is unchanged.
#[tokio::test]
async fn div_by_zero_fails_write_and_leaves_doc_unchanged() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db(&state).await;

    // denom absent: null/0 propagates to null, so the insert succeeds with
    // no ratio key
    let id = insert_doc(&pool, &db, &schema, "metrics", serde_json::json!({})).await?;
    let doc = fetch_doc(&pool, &db, "metrics", &id).await?;
    assert!(doc.get("ratio").is_none());

    // a denom makes the divisor check live → the patch fails atomically
    let err = run(
        &pool,
        &db,
        &schema,
        vec![Step::Patch {
            table: "metrics".to_string(),
            id: id.clone(),
            fields: serde_json::json!({"denom": 5.0})
                .as_object()
                .expect("json object")
                .clone(),
        }],
    )
    .await
    .expect_err("div by zero must fail the write");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("computed field 'ratio'"),
        "error must name the field: {}",
        err.message
    );
    assert!(
        err.message.contains("division by zero"),
        "error must carry the cause: {}",
        err.message
    );

    let (doc, version): (serde_json::Value, i64) = sqlx::query_as(&format!(
        "SELECT \"doc\", \"version\" FROM \"db_{db}\".\"t_metrics\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert!(
        doc.get("denom").is_none(),
        "rolled-back patch left no denom"
    );
    assert!(doc.get("ratio").is_none());
    assert_eq!(version, 1, "no version bump from a failed write");
    Ok(())
}

// ---------------------------------------------------------------------------
// Task 4 — migrate interplay + push backfill + Now() alignment
// ---------------------------------------------------------------------------

/// One-table schema for the migrate-interplay tests: `fullName` is computed
/// over `first`/`last`, both computed field and input are indexed, and `age`
/// is a plain optional field no expression references (the "unrelated" drop
/// target). `fullName` stays REQUIRED so the backfill test's v1 (computed map
/// removed, client supplies the value) pushes to v2 additively — only the
/// computed entry differs — and the v2 backfill overwrites the client value.
fn migrate_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{
        "users":{
            "fields":{
                "first":{"type":"string"},
                "last":{"type":"string"},
                "fullName":{"type":"string"},
                "age":{"type":"optional","inner":{"type":"number"}}},
            "indexes":[{"name":"by_first","fields":["first"]},
                       {"name":"by_fullName","fields":["fullName"]}],
            "computed":{
                "fullName":{"op":"concat","parts":[
                    {"op":"field","field":"first"},
                    {"op":"literal","value":" "},
                    {"op":"field","field":"last"}]}}
        }
    }})
}

async fn computed_db_with(
    state: &rtdb_server::AppState,
    schema_json: serde_json::Value,
) -> (crate::common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(schema_json).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");
    (db, schema)
}

fn directive(json: serde_json::Value) -> Directive {
    serde_json::from_value(json).expect("parse directive")
}

/// Plans + applies `directives` in one committed tx (the `migration_test`
/// harness shape) and returns the derived schema plus the migration effects.
async fn apply_directives(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    directives: Vec<Directive>,
) -> anyhow::Result<(SchemaDef, MigrationEffects)> {
    let derived = plan_migration(schema, &directives).expect("plan migration");
    let mut tx = pool.begin().await?;
    let fx = apply_migration(&mut tx, db, &directives, &derived, false)
        .await
        .expect("apply migration");
    tx.commit().await?;
    Ok((derived, fx))
}

async fn fetch_doc_and_version(
    pool: &PgPool,
    db: &str,
    table: &str,
    id: &str,
) -> anyhow::Result<(serde_json::Value, i64)> {
    let (doc, version): (serde_json::Value, i64) = sqlx::query_as(&format!(
        "SELECT \"doc\", \"version\" FROM \"db_{db}\".\"t_{table}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok((doc, version))
}

// (9) push rejects an expression referencing an undeclared field, and a
// Concat (string-producing) entry on a number field — through the real push
// path's validate_computed.
#[tokio::test]
async fn push_rejects_undeclared_ref_and_static_kind_mismatch() -> anyhow::Result<()> {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);

    let mut undeclared = computed_schema_json();
    undeclared["tables"]["users"]["computed"]["fullName"] = serde_json::json!({
        "op":"concat","parts":[
            {"op":"field","field":"first"},
            {"op":"field","field":"middle"}]});
    let schema: SchemaDef = serde_json::from_value(undeclared).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema)
        .await
        .expect_err("undeclared reference must fail the push");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("references undeclared field 'middle'"),
        "must name the undeclared field: {}",
        err.message
    );

    let mut mismatched = computed_schema_json();
    mismatched["tables"]["metrics"]["computed"]["ratio"] = serde_json::json!({
        "op":"concat","parts":[
            {"op":"field","field":"denom"},
            {"op":"literal","value":"x"}]});
    let schema: SchemaDef = serde_json::from_value(mismatched).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema)
        .await
        .expect_err("string-producing expr on a number field must fail the push");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("produces a string"),
        "must name the mismatch: {}",
        err.message
    );
    Ok(())
}

// (10) renameField carries the computed map: the derived expression reads the
// renamed field, and a subsequent patch (against the derived schema)
// recomputes from it.
#[tokio::test]
async fn rename_field_rewrites_expr_and_patch_recomputes() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db_with(&state, migrate_schema_json()).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Lovelace");

    let (derived, _fx) = apply_directives(
        &pool,
        &db,
        &schema,
        vec![directive(serde_json::json!({
            "op":"renameField","table":"users","from":"first","to":"givenName"
        }))],
    )
    .await?;
    // the derived schema's expression reads `givenName` now
    let expr = serde_json::to_value(
        derived.tables["users"]
            .computed
            .get("fullName")
            .expect("computed entry preserved"),
    )?;
    assert_eq!(expr["parts"][0]["field"], "givenName");

    // a patch on the renamed input recomputes the computed field
    patch_doc(
        &pool,
        &db,
        &derived,
        "users",
        &id,
        serde_json::json!({"givenName": "Grace"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Grace Lovelace");
    assert_eq!(doc["givenName"], "Grace");
    assert!(doc.get("first").is_none(), "doc key was renamed");

    // the indexed computed column followed the re-stamp
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("Grace Lovelace"));
    Ok(())
}

// (11) dropField on a field a computed expression reads is a BAD_REQUEST
// naming the computed field; dropping an unreferenced field still works.
#[tokio::test]
async fn drop_field_rejects_computed_reference_allows_unrelated() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db_with(&state, migrate_schema_json()).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace", "age": 36}),
    )
    .await?;

    let err = plan_migration(
        &schema,
        &[directive(serde_json::json!({
            "op":"dropField","table":"users","field":"first"
        }))],
    )
    .expect_err("dropping a referenced field must fail the plan");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("computed field 'users.fullName'"),
        "must name the computed field: {}",
        err.message
    );
    assert!(
        err.message.contains("first"),
        "must name the dropped field: {}",
        err.message
    );

    // an unrelated drop plans, applies, and removes the stored key
    let (derived, _fx) = apply_directives(
        &pool,
        &db,
        &schema,
        vec![directive(serde_json::json!({
            "op":"dropField","table":"users","field":"age"
        }))],
    )
    .await?;
    assert!(!derived.tables["users"].fields.contains_key("age"));
    let doc = fetch_doc(&pool, &db, "users", &id).await?;
    assert!(doc.get("age").is_none());
    assert_eq!(doc["fullName"], "Ada Lovelace");
    Ok(())
}

// (12) changeType of a computed field to a type its expression cannot produce
// is rejected on the derived schema (plan_migration's validate_computed) —
// pure planning, no db needed.
#[tokio::test]
async fn change_type_to_unproducible_kind_is_rejected() -> anyhow::Result<()> {
    let schema: SchemaDef = serde_json::from_value(migrate_schema_json()).expect("parse schema");

    // Concat produces a string; fullName becomes boolean → rejected.
    let err = plan_migration(
        &schema,
        &[directive(serde_json::json!({
            "op":"changeType","table":"users","field":"fullName",
            "to":{"type":"boolean"},"cast":"toBoolean"
        }))],
    )
    .expect_err("string-producing expr cannot feed a boolean field");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("produces a string"),
        "must name the mismatch: {}",
        err.message
    );

    // A change the expression CAN feed still plans (string → optional string).
    let derived = plan_migration(
        &schema,
        &[directive(serde_json::json!({
            "op":"changeType","table":"users","field":"last",
            "to":{"type":"optional","inner":{"type":"string"}},"cast":"toString"
        }))],
    )
    .expect("compatible changeType plans");
    assert!(derived.tables["users"].fields.contains_key("last"));
    Ok(())
}

// (13) evalExpr rewriting an input field re-stamps the computed value in the
// SAME migrate — affected rows carry the new derivation, untouched rows keep
// theirs.
#[tokio::test]
async fn eval_expr_restamps_computed_same_migrate() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db_with(&state, migrate_schema_json()).await;

    let ada = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace"}),
    )
    .await?;
    let grace = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Grace", "last": "Hopper"}),
    )
    .await?;

    let (_derived, fx) = apply_directives(
        &pool,
        &db,
        &schema,
        vec![directive(serde_json::json!({
            "op":"evalExpr","table":"users","set":"first",
            "expr":{"op":"literal","value":"New"},
            "where":{"op":"eq","field":"first","value":"Ada"}
        }))],
    )
    .await?;
    assert_eq!(fx.reports[0].affected_rows, 1);

    let doc = fetch_doc(&pool, &db, "users", &ada).await?;
    assert_eq!(doc["first"], "New");
    assert_eq!(doc["fullName"], "New Lovelace", "re-stamped in-migrate");

    let doc = fetch_doc(&pool, &db, "users", &grace).await?;
    assert_eq!(doc["first"], "Grace");
    assert_eq!(doc["fullName"], "Grace Hopper", "unaffected row untouched");

    // the indexed computed column followed the re-stamp
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&ada)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("New Lovelace"));
    Ok(())
}

// (14) push ADDING a computed entry backfills every existing row (doc + typed
// column, no version bump); a CHANGED expression re-backfills; a pure re-push
// and an unrelated push rewrite nothing (docs and versions untouched).
#[tokio::test]
async fn push_backfills_computed_and_pure_push_rewinds_nothing() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();

    // v1: no computed map — fullName is a plain required field the client owns
    let mut v1 = migrate_schema_json();
    v1["tables"]["users"]
        .as_object_mut()
        .expect("users object")
        .remove("computed");
    let (db, schema_v1) = computed_db_with(&state, v1).await;

    let id = insert_doc(
        &pool,
        &db,
        &schema_v1,
        "users",
        serde_json::json!({"first": "Ada", "last": "Lovelace", "fullName": "WRONG"}),
    )
    .await?;
    let (doc, version) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "WRONG", "v1 stores the client value");
    assert_eq!(version, 1);

    // v2: the real push path adds the computed entry → existing rows backfill
    let schema_v2: SchemaDef =
        serde_json::from_value(migrate_schema_json()).expect("parse v2 schema");
    push_schema(&pool, &db, schema_v2).await.expect("push v2");
    let (doc, version) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Lovelace", "backfilled on push");
    assert_eq!(version, 1, "a push backfill is not a write");
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("Ada Lovelace"));

    // v3: a CHANGED expression re-backfills
    let mut v3 = migrate_schema_json();
    v3["tables"]["users"]["computed"]["fullName"] = serde_json::json!({
        "op":"concat","parts":[
            {"op":"field","field":"first"},
            {"op":"literal","value":"·"},
            {"op":"field","field":"last"}]});
    let schema_v3: SchemaDef = serde_json::from_value(v3.clone()).expect("parse v3");
    push_schema(&pool, &db, schema_v3.clone())
        .await
        .expect("push v3");
    let (doc, version) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada·Lovelace", "changed expr re-backfills");
    assert_eq!(version, 1);

    // v4: a pure re-push of v3 — computed map unchanged, nothing rewrites
    push_schema(&pool, &db, schema_v3.clone())
        .await
        .expect("re-push v3");
    let (after_repush, version_after) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(after_repush, doc, "pure re-push rewrites no doc");
    assert_eq!(version_after, 1, "pure re-push bumps no version");

    // v5: an unrelated additive push (new plain field) — still nothing rewrites
    let mut v5 = v3;
    v5["tables"]["users"]["fields"]["tier"] =
        serde_json::json!({"type":"optional","inner":{"type":"string"}});
    let schema_v5: SchemaDef = serde_json::from_value(v5).expect("parse v5");
    push_schema(&pool, &db, schema_v5).await.expect("push v5");
    let (after_v5, version_v5) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(after_v5, doc, "unrelated push rewrites no doc");
    assert_eq!(version_v5, 1, "unrelated push bumps no version");
    Ok(())
}

// (15) the schema RESTORE path backfills too: restoring a snapshot that adds a
// computed map re-derives it for existing rows. The deferred-review finding —
// restore reshapes via reconcile_schema_destructive + apply_schema_additive,
// so the backfill wired into the additive apply must fire there as well. Also
// pins the removal semantics: with the computed entry gone (restored to v1),
// the stored value is an ordinary client-writable field again.
#[tokio::test]
async fn restore_backfills_computed_and_removal_unlocks_client_writes() -> anyhow::Result<()> {
    use crate::common::{admin_post, spawn_app};

    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);

    let push_http = |schema_json: serde_json::Value| {
        let db = db.to_string();
        async move {
            let resp = admin_post(
                addr,
                "/admin/push-schema",
                serde_json::json!({ "db": db, "schema": schema_json }),
            )
            .await;
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "push failed: {:?}",
                resp.text().await
            );
        }
    };
    let mutate_http = |steps: serde_json::Value| {
        let db = db.to_string();
        async move {
            let resp = admin_post(
                addr,
                &format!("/admin/db/{db}/mutate"),
                serde_json::json!({ "txn": { "steps": steps } }),
            )
            .await;
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "mutate failed: {:?}",
                resp.text().await
            );
            let body: serde_json::Value = resp.json().await.expect("parse mutate response");
            body["results"][0].clone()
        }
    };
    let restore_http = |version: i64| {
        let db = db.to_string();
        async move {
            let resp = admin_post(
                addr,
                &format!("/admin/db/{db}/schema/restore"),
                serde_json::json!({ "version": version, "confirm": db }),
            )
            .await;
            assert_eq!(
                resp.status(),
                reqwest::StatusCode::OK,
                "restore failed: {:?}",
                resp.text().await
            );
        }
    };

    // v1: no computed map — fullName is client-owned
    let mut v1 = migrate_schema_json();
    v1["tables"]["users"]
        .as_object_mut()
        .expect("users object")
        .remove("computed");
    push_http(v1).await;
    let inserted = mutate_http(serde_json::json!([
        {"op":"insert","table":"users",
         "doc":{"first":"Ada","last":"Lovelace","fullName":"WRONG"}}
    ]))
    .await;
    let id = inserted["id"].as_str().expect("id").to_string();
    let (doc, _) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "WRONG");

    // v2: adds the computed map — the push backfills over the client value.
    push_http(migrate_schema_json()).await;
    let (doc, _) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(doc["fullName"], "Ada Lovelace");

    // Capture v2's history version (newest entry right after the push).
    let resp = crate::common::admin_get(addr, &format!("/admin/db/{db}/schema/history")).await;
    let body: serde_json::Value = resp.json().await?;
    let entries = body["entries"].as_array().expect("entries array");
    let v2 = entries[0]["version"].as_i64().expect("version");
    assert_eq!(entries[0]["source"], "push");

    // Restore to v1: the computed entry is gone; the stored value stays and
    // becomes an ordinary client-writable field.
    let v1_version = entries[entries.len() - 1]["version"]
        .as_i64()
        .expect("version");
    restore_http(v1_version).await;
    mutate_http(serde_json::json!([
        {"op":"patch","table":"users","id":id,"fields":{"fullName":"WRONG2"}}
    ]))
    .await;
    let (doc, _) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(
        doc["fullName"], "WRONG2",
        "without the computed entry the field is client-writable"
    );

    // Restore forward to v2: the computed map is re-added and the restore
    // backfill re-stamps — no stale client value survives.
    restore_http(v2).await;
    let (doc, _) = fetch_doc_and_version(&pool, &db, "users", &id).await?;
    assert_eq!(
        doc["fullName"], "Ada Lovelace",
        "restore that adds a computed map backfills existing rows"
    );
    Ok(())
}

// (16) setDefault feeding a computed INPUT: rows that receive the default get
// their computed value re-derived in the same migrate; rows that already had
// the field (excluded from the affected ids) keep their stamp untouched.
#[tokio::test]
async fn set_default_restamps_dependent_computed_values() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db_with(
        &state,
        serde_json::json!({"tables":{
            "users":{
                "fields":{
                    "first":{"type":"string"},
                    "last":{"type":"optional","inner":{"type":"string"}},
                    "fullName":{"type":"string"}},
                "indexes":[{"name":"by_fullName","fields":["fullName"]}],
                "computed":{
                    "fullName":{"op":"concat","parts":[
                        {"op":"field","field":"first"},
                        {"op":"literal","value":" "},
                        {"op":"field","field":"last"}]}}
            }
        }}),
    )
    .await;

    // lacking `last` → concat skips the null part
    let ada = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada"}),
    )
    .await?;
    let grace = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Grace", "last": "Hopper"}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &ada).await?;
    assert_eq!(doc["fullName"], "Ada ");

    let (derived, fx) = apply_directives(
        &pool,
        &db,
        &schema,
        vec![directive(serde_json::json!({
            "op":"setDefault","table":"users","field":"last","value":"Lovelace"
        }))],
    )
    .await?;
    assert!(derived.tables["users"].computed.contains_key("fullName"));
    assert_eq!(fx.reports[0].affected_rows, 1, "only the row lacking last");

    let doc = fetch_doc(&pool, &db, "users", &ada).await?;
    assert_eq!(doc["last"], "Lovelace");
    assert_eq!(
        doc["fullName"], "Ada Lovelace",
        "setDefault feeding a computed input re-stamps in the same migrate"
    );
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_fullname\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&ada)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("Ada Lovelace"));

    // the row that already had `last` was not in the affected ids
    let doc = fetch_doc(&pool, &db, "users", &grace).await?;
    assert_eq!(doc["last"], "Hopper");
    assert_eq!(doc["fullName"], "Grace Hopper");
    Ok(())
}

// (17) changeType of a computed INPUT: the cast rewrites stored values (here
// boolean→number turns `true` into `1.0`, whose text form differs), and the
// dependent computed values re-derive in the same migrate for every carrier
// row. (`flag` is a bare boolean — the cast matrix accepts no optional source.)
#[tokio::test]
async fn change_type_of_input_restamps_dependent_computed_values() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = computed_db_with(
        &state,
        serde_json::json!({"tables":{
            "users":{
                "fields":{
                    "first":{"type":"string"},
                    "flag":{"type":"boolean"},
                    "label":{"type":"string"}},
                "indexes":[{"name":"by_label","fields":["label"]}],
                "computed":{
                    "label":{"op":"concat","parts":[
                        {"op":"literal","value":"v="},
                        {"op":"field","field":"flag"}]}}
            }
        }}),
    )
    .await;

    let flagged = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Ada", "flag": true}),
    )
    .await?;
    let unflagged = insert_doc(
        &pool,
        &db,
        &schema,
        "users",
        serde_json::json!({"first": "Grace", "flag": false}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, "users", &flagged).await?;
    assert_eq!(doc["label"], "v=true");
    let doc = fetch_doc(&pool, &db, "users", &unflagged).await?;
    assert_eq!(doc["label"], "v=false");

    let (_derived, fx) = apply_directives(
        &pool,
        &db,
        &schema,
        vec![directive(serde_json::json!({
            "op":"changeType","table":"users","field":"flag",
            "to":{"type":"number"},"cast":"toNumber"
        }))],
    )
    .await?;
    assert_eq!(fx.reports[0].affected_rows, 2, "both rows carry flag");

    // true → 1.0; text extraction of jsonb 1.0 is "1.0", so the concat result
    // genuinely differs — without the re-stamp this would still read "v=true"
    let doc = fetch_doc(&pool, &db, "users", &flagged).await?;
    assert_eq!(doc["flag"].as_f64(), Some(1.0));
    assert_eq!(doc["label"], "v=1.0");
    let (col,): (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"f_label\" FROM \"db_{db}\".\"t_users\" WHERE \"id\" = $1"
    ))
    .bind(&flagged)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col.as_deref(), Some("v=1.0"));

    // false → 0.0 re-derives too — every carrier row refreshes
    let doc = fetch_doc(&pool, &db, "users", &unflagged).await?;
    assert_eq!(doc["flag"].as_f64(), Some(0.0));
    assert_eq!(doc["label"], "v=0.0");
    Ok(())
}
