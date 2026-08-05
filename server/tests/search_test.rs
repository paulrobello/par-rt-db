mod common;

use std::sync::Arc;

use common::test_state;
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;

/// A `notes` table with a btree index (`by_title`) and a full-text search index
/// (`search_content`) over both text fields.
fn search_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"},"body":{"type":"string"}},
        "indexes":[
            {"name":"by_title","fields":["title"]},
            {"name":"search_content","fields":["title","body"],"search":true}
        ]
    }}})
}

fn search_schema() -> SchemaDef {
    serde_json::from_value(search_schema_json()).expect("parse search schema")
}

async fn fresh_search_db(state: &Arc<AppState>) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let schema = search_schema();
    ddl::push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    (common::wrap_test_db(name), schema)
}

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

async fn insert_note(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    title: &str,
    body: &str,
) -> String {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "notes".to_string(),
                doc: doc(serde_json::json!({"title": title, "body": body})),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert note");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

fn search_query(index: &str, query: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": index, "query": query}
    }))
    .expect("search query")
}

fn titles(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Docs(docs) => docs
            .iter()
            .map(|d| d["title"].as_str().unwrap_or("").to_string())
            .collect(),
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// A search returns matching documents ranked by relevance: a body with the term
// repeated outranks a title with a single occurrence; non-matching docs are
// excluded.
#[tokio::test]
async fn search_returns_ranked_results() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database notes", "").await; // 1 occurrence (title)
    insert_note(
        pool,
        &db,
        &schema,
        "frequent hits",
        "database database database database",
    )
    .await; // 4 occurrences (body)
    insert_note(pool, &db, &schema, "cooking", "recipes for dinner").await; // none

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    let titles = titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"database notes".to_string()));
    assert!(titles.contains(&"frequent hits".to_string()));
    assert!(!titles.contains(&"cooking".to_string()));
    assert_eq!(titles[0], "frequent hits");
    assert_eq!(titles[1], "database notes");
}

// `take` caps the number of ranked results.
#[tokio::test]
async fn search_with_take_limits_results() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database notes", "").await;
    insert_note(
        pool,
        &db,
        &schema,
        "frequent hits",
        "database database database database",
    )
    .await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database"},
        "take": 1
    }))
    .expect("query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("search");
    let titles = titles(&res);
    assert_eq!(titles.len(), 1);
    assert_eq!(titles[0], "frequent hits");
}

// A query that matches nothing returns an empty result, not an error.
#[tokio::test]
async fn search_with_no_matches_returns_empty() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database notes", "").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "supercalifragilistic"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    assert!(matches!(res, QueryResult::Docs(ref d) if d.is_empty()));
}

// An unknown search index is a clear BadRequest, never a 500.
#[tokio::test]
async fn search_unknown_index_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &search_query("nope", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("unknown index");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("search index 'nope' not found"));
}

// Naming a btree index (which exists but is not a search index) is also a
// BadRequest — the search terminal only resolves search indexes.
#[tokio::test]
async fn search_btree_index_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &search_query("by_title", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("btree used as search");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

// Empty / whitespace-only query text is a BadRequest.
#[tokio::test]
async fn search_empty_query_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &search_query("search_content", "   "),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect_err("empty query");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("must not be empty"));
}

// Combining search with an index-based terminal is a BadRequest.
#[tokio::test]
async fn search_combined_with_index_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database"},
        "index": "by_title"
    }))
    .expect("query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("search + index");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

// Adding a search index to a table that already has rows backfills the
// generated tsvector column, so existing docs become searchable — an additive
// schema change, no rebuild.
#[tokio::test]
async fn adding_search_index_backfills_existing_rows() {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db)
        .await
        .expect("create db");
    let db = common::wrap_test_db(db);

    let v1: SchemaDef = serde_json::from_value(serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"},"body":{"type":"string"}},
        "indexes":[{"name":"by_title","fields":["title"]}]
    }}}))
    .expect("schema v1");
    ddl::push_schema(&state.pool, &db, v1.clone())
        .await
        .expect("push v1");
    insert_note(
        &state.pool,
        &db,
        &v1,
        "database notes",
        "a note about a database",
    )
    .await;

    // v2 adds the search index additively.
    ddl::push_schema(&state.pool, &db, search_schema())
        .await
        .expect("push v2");

    let res = execute_query(
        &state.pool,
        &db,
        &search_schema(),
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    assert_eq!(titles(&res), vec!["database notes".to_string()]);
}

// ENH-006: a search index may declare a `language` (a Postgres regconfig) that
// drives its tsvector column and query tsquery. These cover the end-to-end path,
// the behavioral effect (stemming), and the validation surface.

fn lang_search_schema(language: Option<&str>) -> SchemaDef {
    let mut idx =
        serde_json::json!({"name":"search_content","fields":["title","body"],"search":true});
    if let Some(lang) = language {
        idx["language"] = serde_json::json!(lang);
    }
    serde_json::from_value(serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"},"body":{"type":"string"}},
        "indexes":[idx]
    }}}))
    .expect("parse language search schema")
}

async fn fresh_db_with(state: &Arc<AppState>, schema: SchemaDef) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    ddl::push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    (common::wrap_test_db(name), schema)
}

// A `simple`-language index (no stemming, no stop-words) matches an exact word,
// proving the declared regconfig flows through both the generated column and the
// query tsquery end to end.
#[tokio::test]
async fn search_index_language_simple_matches_exact_word() {
    let state = test_state().await;
    let (db, schema) = fresh_db_with(&state, lang_search_schema(Some("simple"))).await;
    insert_note(&state.pool, &db, &schema, "quick fox", "lazy dog").await;
    let res = execute_query(
        &state.pool,
        &db,
        &schema,
        &search_query("search_content", "fox"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    assert_eq!(titles(&res), vec!["quick fox".to_string()]);
}

// The `simple` regconfig does no stemming, so a singular query does not match a
// plural document — while the default `english` config does (Porter stemming).
// This is the load-bearing behavioral proof that `language` actually changes
// tokenization rather than being a no-op.
#[tokio::test]
async fn search_index_language_simple_does_not_stem() {
    let state = test_state().await;
    let (db_simple, schema_simple) =
        fresh_db_with(&state, lang_search_schema(Some("simple"))).await;
    insert_note(&state.pool, &db_simple, &schema_simple, "databases", "").await;
    let res = execute_query(
        &state.pool,
        &db_simple,
        &schema_simple,
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    assert!(
        matches!(res, QueryResult::Docs(ref d) if d.is_empty()),
        "simple config must not stem the plural"
    );

    let (db_en, schema_en) = fresh_db_with(&state, lang_search_schema(None)).await;
    insert_note(&state.pool, &db_en, &schema_en, "databases", "").await;
    let res = execute_query(
        &state.pool,
        &db_en,
        &schema_en,
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("search");
    assert_eq!(
        titles(&res),
        vec!["databases".to_string()],
        "english config should stem the plural"
    );
}

// A language that is not a real Postgres text-search config is rejected at push
// (existence-checked against pg_ts_config), surfacing as a clear BadRequest
// rather than a DDL-time 500.
#[tokio::test]
async fn search_index_unknown_language_rejected_at_push() {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let name = common::wrap_test_db(name);
    let err = ddl::push_schema(&state.pool, &name, lang_search_schema(Some("nonsense")))
        .await
        .expect_err("unknown language");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("nonsense"));
}

// `language` is only meaningful on a search index; declaring it on a btree index
// is rejected at schema-validation time.
#[tokio::test]
async fn search_index_language_on_non_search_rejected() {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let name = common::wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"}},
        "indexes":[{"name":"by_title","fields":["title"],"language":"english"}]
    }}}))
    .expect("parse schema");
    let err = ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect_err("language on btree index");
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("not a search index"));
}

// Changing an existing search index's language is a breaking change (the regconfig
// is baked into a STORED generated column Postgres cannot alter in place) and is
// rejected, while re-pushing the same language is accepted.
#[tokio::test]
async fn search_index_language_change_is_breaking() {
    let state = test_state().await;
    let (db, _schema) = fresh_db_with(&state, lang_search_schema(Some("simple"))).await;
    // Same language re-pushed: accepted.
    ddl::push_schema(&state.pool, &db, lang_search_schema(Some("simple")))
        .await
        .expect("re-push same language");
    // Different language: rejected.
    let err = ddl::push_schema(&state.pool, &db, lang_search_schema(Some("english")))
        .await
        .expect_err("language change");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("changed language"));
}
