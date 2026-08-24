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
        false,
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
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
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
        false,
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
        false,
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
        false,
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
        false,
    )
    .await
    .expect_err("empty query");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("must not be empty"));
}

// SEC-007: query text past the project-owned byte cap is rejected before
// compilation.
#[tokio::test]
async fn search_over_long_query_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    // One byte past `search::MAX_SEARCH_QUERY_BYTES` (the module is private, so
    // the cap is spelled out here; the server-side const is the source of truth).
    let long = "a".repeat(4097);
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &search_query("search_content", &long),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("over-long query");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at most"), "got {}", err.message);
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
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx::bypass(),
        false,
    )
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
        false,
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
        false,
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
        false,
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
        false,
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

// =========================================================================
// search + filter (the `filter()` DSL narrowed into the search WHERE)
// =========================================================================

/// A `posts` table: text `title` (search-indexed), numeric `category` (btree
/// `by_category`, so eq/range bind a typed `f_category` column), and a `tag`
/// string with NO index (so filtering on it exercises jsonb extraction). Used
/// to exercise the search+filter combination end to end.
fn filter_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({"tables":{"posts":{
        "fields":{
            "title":{"type":"string"},
            "category":{"type":"number"},
            "tag":{"type":"string"}
        },
        "indexes":[
            {"name":"by_category","fields":["category"]},
            {"name":"search_title","fields":["title"],"search":true}
        ]
    }}}))
    .expect("parse filter schema")
}

async fn fresh_filter_db(state: &Arc<AppState>) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let schema = filter_schema();
    ddl::push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    (common::wrap_test_db(name), schema)
}

async fn insert_post(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    title: &str,
    category: i64,
    tag: &str,
) -> String {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "posts".to_string(),
                doc: doc(serde_json::json!({"title": title, "category": category, "tag": tag})),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert post");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

fn search_filter_query(query: &str, filter: serde_json::Value) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "posts",
        "search": {"index": "search_title", "query": query, "filter": filter}
    }))
    .expect("search+filter query")
}

fn post_titles(result: &QueryResult) -> Vec<String> {
    match result {
        QueryResult::Docs(docs) => docs
            .iter()
            .map(|d| d["title"].as_str().unwrap_or("").to_string())
            .collect(),
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// Omitting `filter` behaves exactly like plain search (proves the field is
// additive: an existing-shape request still works and returns every match).
#[tokio::test]
async fn search_filter_omitted_behaves_like_plain_search() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "posts",
        "search": {"index": "search_title", "query": "database"}
    }))
    .expect("plain search query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("search");
    assert_eq!(post_titles(&res).len(), 2);
}

// An eq filter on an indexed numeric field narrows ranked results via its typed
// `f_category` column.
#[tokio::test]
async fn search_filter_eq_on_indexed_field_narrows() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;
    insert_post(pool, &db, &schema, "database deep dive", 1, "c").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"eq","field":"category","value":1}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    let titles = post_titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"database intro".to_string()));
    assert!(titles.contains(&"database deep dive".to_string()));
    assert!(!titles.contains(&"database advanced".to_string()));
}

// A range filter (gt) on the indexed numeric field narrows the same way.
#[tokio::test]
async fn search_filter_range_on_indexed_field_narrows() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;
    insert_post(pool, &db, &schema, "database deep dive", 3, "c").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"gt","field":"category","value":1}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    let titles = post_titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"database advanced".to_string()));
    assert!(titles.contains(&"database deep dive".to_string()));
}

// An eq filter on a NON-indexed string field narrows via jsonb extraction.
#[tokio::test]
async fn search_filter_eq_on_unindexed_field_narrows() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "alpha").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "beta").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"eq","field":"tag","value":"beta"}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    let titles = post_titles(&res);
    assert_eq!(titles, vec!["database advanced".to_string()]);
}

// `and` combines two predicates; `or` matches either. Both narrow correctly.
#[tokio::test]
async fn search_filter_and_or_combine() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "alpha").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "beta").await;
    insert_post(pool, &db, &schema, "database deep dive", 3, "alpha").await;

    // category == 1 AND tag == alpha -> only "database intro".
    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"and","exprs":[
                {"op":"eq","field":"category","value":1},
                {"op":"eq","field":"tag","value":"alpha"}
            ]}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    assert_eq!(post_titles(&res), vec!["database intro".to_string()]);

    // category == 1 OR category == 3 -> intro + deep dive.
    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"or","exprs":[
                {"op":"eq","field":"category","value":1},
                {"op":"eq","field":"category","value":3}
            ]}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    let titles = post_titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"database intro".to_string()));
    assert!(titles.contains(&"database deep dive".to_string()));
}

// `not` excludes the matching subset.
#[tokio::test]
async fn search_filter_not_excludes() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "alpha").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "beta").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"not","expr":{"op":"eq","field":"tag","value":"beta"}}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    assert_eq!(post_titles(&res), vec!["database intro".to_string()]);
}

// A filter that matches no documents returns an empty result, not an error.
#[tokio::test]
async fn search_filter_no_match_returns_empty() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "alpha").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"eq","field":"category","value":99}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("search+filter");
    assert!(matches!(res, QueryResult::Docs(ref d) if d.is_empty()));
}

// An unknown field in the filter is a clear BadRequest, never a 500.
#[tokio::test]
async fn search_filter_unknown_field_is_bad_request() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "alpha").await;

    let err = execute_query(
        pool,
        &db,
        &schema,
        &search_filter_query(
            "database",
            serde_json::json!({"op":"eq","field":"nope","value":1}),
        ),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("unknown filter field");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

// --- trgm mode (FM-30) ---

fn trgm_query(index: &str, query: &str) -> Query {
    serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": index, "query": query, "mode": "trgm"}
    }))
    .expect("trgm search query")
}

// trgm matches substrings tsquery never can ("conv" is no lexeme anywhere) and
// ranks by similarity — the exact-title doc scores 1.0 and comes first.
#[tokio::test]
async fn trgm_mode_matches_substrings_and_ranks_by_similarity() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "convex", "").await;
    insert_note(pool, &db, &schema, "convexity in practice", "").await;
    insert_note(pool, &db, &schema, "cooking", "").await;

    // Default (tsquery) mode: "conv" is not a word in any doc — zero matches.
    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "conv"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("tsquery search");
    assert!(matches!(res, QueryResult::Docs(ref d) if d.is_empty()));

    // trgm mode: prefix substring of both "convex" and "convexity".
    let res = execute_query(
        pool,
        &db,
        &schema,
        &trgm_query("search_content", "conv"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("trgm search");
    let titles = titles(&res);
    assert_eq!(titles.len(), 2);
    assert_eq!(titles[0], "convex");
    assert!(titles.contains(&"convexity in practice".to_string()));
}

// ILIKE is case-insensitive — autocomplete over mixed-case text.
#[tokio::test]
async fn trgm_mode_is_case_insensitive() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "PostgreSQL Guide", "").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &trgm_query("search_content", "POSTGRE"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("trgm search");
    let titles = titles(&res);
    assert_eq!(titles, vec!["PostgreSQL Guide".to_string()]);
}

// trgm composes with filter: an infix ("atab" inside "database") narrowed by an
// eq predicate on an indexed field.
#[tokio::test]
async fn trgm_mode_composes_with_filter() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;
    insert_post(pool, &db, &schema, "cooking", 1, "c").await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "posts",
        "search": {
            "index": "search_title",
            "query": "atab",
            "mode": "trgm",
            "filter": {"op":"eq","field":"category","value":1}
        }
    }))
    .expect("trgm+filter query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("trgm+filter");
    let titles = post_titles(&res);
    assert_eq!(titles, vec!["database intro".to_string()]);
}

// trgm composes with take, capping the ranked result list.
#[tokio::test]
async fn trgm_mode_composes_with_take() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "posts",
        "search": {"index": "search_title", "query": "atab", "mode": "trgm"},
        "take": 1
    }))
    .expect("trgm+take query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("trgm+take");
    assert_eq!(post_titles(&res).len(), 1);
}

// Explicit mode "tsquery" is accepted and behaves exactly like the default.
#[tokio::test]
async fn trgm_mode_explicit_tsquery_matches_default() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database notes", "database database").await;

    let explicit: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database", "mode": "tsquery"}
    }))
    .expect("explicit tsquery");
    let res = execute_query(
        pool,
        &db,
        &schema,
        &explicit,
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("explicit tsquery search");
    assert_eq!(titles(&res).len(), 1);

    let res_default = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("default search");
    assert_eq!(titles(&res_default), titles(&res));
}

// An unknown mode value fails Query deserialization (deny_unknown_fields +
// enum) — a BadRequest at the transport boundary, never a silent fallback.
#[test]
fn trgm_mode_invalid_value_is_rejected() {
    let parsed: Result<Query, _> = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "x", "mode": "fuzzy"}
    }));
    assert!(parsed.is_err());
}

async fn index_def(pool: &PgPool, schema_name: &str, index_name: &str) -> Option<String> {
    sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND indexname = $2")
        .bind(schema_name)
        .bind(index_name)
        .fetch_optional(pool)
        .await
        .expect("read pg_indexes")
}

// Every search index carries a trigram GIN (`gin_trgm_ops`) beside its
// tsvector GIN — and re-pushing the same schema is idempotent, and recreates
// the trigram index if it is missing (the backfill for deployments whose
// search indexes predate trgm mode).
#[tokio::test]
async fn trgm_gin_index_created_backfilled_and_idempotent() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    let schema_name = ddl::pg_schema(&db);

    let def = index_def(pool, &schema_name, "tg_notes_search_content")
        .await
        .expect("trigram GIN present after push");
    assert!(def.contains("gin_trgm_ops"), "not a trigram GIN: {def}");
    assert!(def.contains("f_title"), "missing title column: {def}");
    assert!(def.contains("f_body"), "missing body column: {def}");

    // Simulate a pre-FM-30 deployment: trigram index gone, schema unchanged.
    sqlx::query(&format!(
        "DROP INDEX \"{schema_name}\".\"tg_notes_search_content\""
    ))
    .execute(pool)
    .await
    .expect("drop trigram GIN");
    ddl::push_schema(pool, &db, schema.clone())
        .await
        .expect("re-push schema");
    assert!(
        index_def(pool, &schema_name, "tg_notes_search_content")
            .await
            .is_some(),
        "re-push did not backfill the trigram GIN"
    );

    // A third push over an existing index is a no-op, not an error.
    ddl::push_schema(pool, &db, schema)
        .await
        .expect("idempotent push");
}

// Removing a search index via the destructive reconcile drops BOTH GIN
// indexes (tsvector `i_` and trigram `tg_`).
#[tokio::test]
async fn trgm_gin_index_dropped_by_reconcile() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    let schema_name = ddl::pg_schema(&db);

    let mut target = schema.clone();
    target
        .tables
        .get_mut("notes")
        .expect("notes table")
        .indexes
        .retain(|i| !i.search);

    let mut tx = pool.begin().await.expect("begin tx");
    ddl::reconcile_schema_destructive(&mut tx, &db, &schema, &target)
        .await
        .expect("reconcile");
    tx.commit().await.expect("commit");

    assert!(
        index_def(pool, &schema_name, "tg_notes_search_content")
            .await
            .is_none(),
        "trigram GIN survived reconcile"
    );
    assert!(
        index_def(pool, &schema_name, "i_notes_search_content")
            .await
            .is_none(),
        "tsvector GIN survived reconcile"
    );
}

// --- phrase/operator search (FM-31) ---

// A quoted phrase requires the words ADJACENT: only the doc where "database
// notes" appears as a contiguous phrase matches; the doc carrying the same
// words apart does not. The same words unquoted still match both — AND
// semantics — pinning plain-query equivalence with the former
// plainto_tsquery behavior through the websearch upgrade.
#[tokio::test]
async fn phrase_query_requires_adjacent_words() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(
        pool,
        &db,
        &schema,
        "adjacent",
        "the database notes are great",
    )
    .await;
    insert_note(pool, &db, &schema, "apart", "notes about the database").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "\"database notes\""),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("phrase search");
    assert_eq!(titles(&res), vec!["adjacent".to_string()]);

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "database notes"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("plain AND search");
    let titles = titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"adjacent".to_string()));
    assert!(titles.contains(&"apart".to_string()));
}

// The bare word `or` unions alternatives: a doc with either term matches.
#[tokio::test]
async fn or_operator_unions_alternatives() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "alpha only", "").await;
    insert_note(pool, &db, &schema, "beta only", "").await;
    insert_note(pool, &db, &schema, "gamma", "").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "alpha or beta"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("or search");
    let titles = titles(&res);
    assert_eq!(titles.len(), 2);
    assert!(titles.contains(&"alpha only".to_string()));
    assert!(titles.contains(&"beta only".to_string()));
    assert!(!titles.contains(&"gamma".to_string()));
}

// `-term` excludes docs carrying the negated word while keeping the positive
// one.
#[tokio::test]
async fn minus_operator_excludes_term() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database intro", "database basics").await;
    insert_note(pool, &db, &schema, "database cooking", "database recipes").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "database -cooking"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("minus search");
    assert_eq!(titles(&res), vec!["database intro".to_string()]);
}

// Phrases work through a declared `language` regconfig too (the same
// config string flows into websearch_to_tsquery and the generated column).
#[tokio::test]
async fn phrase_query_works_with_language_index() {
    let state = test_state().await;
    let (db, schema) = fresh_db_with(&state, lang_search_schema(Some("simple"))).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "adjacent", "quick fox lazy dog").await;
    insert_note(pool, &db, &schema, "apart", "fox quick").await;

    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "\"quick fox\""),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("language phrase search");
    assert_eq!(titles(&res), vec!["adjacent".to_string()]);
}

// --- snippets (FM-31) ---

// snippet: true attaches a `_searchSnippet` to every hit — a ts_headline
// render wrapping the matched term in <mark>, honoring the server word
// bound. Omitted, no snippet field appears.
#[tokio::test]
async fn snippet_returns_highlighted_fragment() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(
        pool,
        &db,
        &schema,
        "database notes",
        "a database is a database",
    )
    .await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database", "snippet": true}
    }))
    .expect("snippet query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("snippet search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            let snippet = docs[0]["_searchSnippet"]
                .as_str()
                .expect("_searchSnippet string");
            assert!(
                snippet.contains("<mark>database</mark>"),
                "no highlighted term in {snippet}"
            );
            assert!(
                snippet.split_whitespace().count() <= 37,
                "snippet exceeds the server word bound: {snippet}"
            );
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }

    // Omitted snippet: docs carry no _searchSnippet field.
    let res = execute_query(
        pool,
        &db,
        &schema,
        &search_query("search_content", "database"),
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect("plain search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            assert!(
                docs[0].get("_searchSnippet").is_none(),
                "snippet field present without snippet: true"
            );
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// An explicit `snippet: false` behaves exactly like omission (the wire
// defaults to off; nothing is double-rendered).
#[tokio::test]
async fn snippet_false_behaves_like_omitted() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(pool, &db, &schema, "database notes", "").await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database", "snippet": false}
    }))
    .expect("snippet-false query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("snippet-false search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            assert!(docs[0].get("_searchSnippet").is_none());
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// snippet highlights PHRASE queries — the headline renders from the same
// websearch_to_tsquery the WHERE matched. ts_headline marks each matched
// word, so a phrase hit shows as adjacent per-word marks.
#[tokio::test]
async fn snippet_highlights_phrase_queries() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(
        pool,
        &db,
        &schema,
        "adjacent",
        "the database notes are great today",
    )
    .await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {
            "index": "search_content",
            "query": "\"database notes\"",
            "snippet": true
        }
    }))
    .expect("phrase snippet query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("phrase snippet search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            let snippet = docs[0]["_searchSnippet"]
                .as_str()
                .expect("_searchSnippet string");
            assert!(
                snippet.contains("<mark>database</mark> <mark>notes</mark>"),
                "phrase words not contiguously highlighted in {snippet}"
            );
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// Snippets compose with `filter`: narrowed hits are snippeted, filtered-out
// rows are neither returned nor snippeted.
#[tokio::test]
async fn snippet_composes_with_filter() {
    let state = test_state().await;
    let (db, schema) = fresh_filter_db(&state).await;
    let pool = &state.pool;
    insert_post(pool, &db, &schema, "database intro", 1, "a").await;
    insert_post(pool, &db, &schema, "database advanced", 2, "b").await;
    insert_post(pool, &db, &schema, "cooking", 1, "c").await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "posts",
        "search": {
            "index": "search_title",
            "query": "database",
            "snippet": true,
            "filter": {"op":"eq","field":"category","value":1}
        }
    }))
    .expect("snippet+filter query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("snippet+filter search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            let snippet = docs[0]["_searchSnippet"]
                .as_str()
                .expect("_searchSnippet string");
            assert!(
                snippet.contains("<mark>database</mark>"),
                "no highlighted term in {snippet}"
            );
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
}

// snippet + trgm mode is rejected up front — trgm matches substrings, so
// there is no tsquery tree to highlight.
#[tokio::test]
async fn snippet_rejected_with_trgm_mode() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {
            "index": "search_content",
            "query": "conv",
            "mode": "trgm",
            "snippet": true
        }
    }))
    .expect("snippet+trgm query");
    let err = execute_query(
        &state.pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .expect_err("snippet+trgm must fail");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("tsquery mode"));
}

// --- projection + search ---

// fields composes with the search terminal: hits keep the system fields, the
// listed user fields, AND the synthetic `_searchSnippet` (every `_`-prefixed
// key is a system/synthetic field the projection never strips — snippet:true
// stays useful alongside a projection).
#[tokio::test]
async fn projection_composes_with_search_and_keeps_snippet() {
    let state = test_state().await;
    let (db, schema) = fresh_search_db(&state).await;
    let pool = &state.pool;
    insert_note(
        pool,
        &db,
        &schema,
        "database notes",
        "a database is a database",
    )
    .await;

    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "notes",
        "search": {"index": "search_content", "query": "database", "snippet": true},
        "fields": ["title"]
    }))
    .expect("projection search query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .expect("projection search");
    match res {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1);
            let doc = &docs[0];
            let mut keys: Vec<&str> = doc
                .as_object()
                .expect("doc object")
                .keys()
                .map(|k| k.as_str())
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                vec![
                    "_creationTime",
                    "_id",
                    "_searchSnippet",
                    "_version",
                    "title"
                ]
            );
            assert_eq!(doc["title"], serde_json::json!("database notes"));
            assert!(
                doc["_searchSnippet"]
                    .as_str()
                    .is_some_and(|s| s.contains("<mark>"))
            );
        }
        other => panic!("expected Docs variant, got {other:?}"),
    }
}
