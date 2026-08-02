//! Integration tests for document TTL insert-time default stamping (Task 2 of
//! the document-TTL feature). Mirrors the harness conventions of `txn_test.rs`
//! and `scheduled_test.rs`: `common::test_state()` bootstraps `rtdb_auth` and
//! hands out a pool; each test creates a uniquely-named `t<uuid>` database,
//! pushes a schema, and drives the document path through `execute_txn` /
//! `execute_query` directly (no HTTP).

mod common;

use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

/// Default duration used by the fixture schema: 1 day in ms.
const DEFAULT_DURATION_MS: i64 = 86_400_000;
/// Slack for the stamp window: the TTL stamp and the row's `created_at` are
/// each taken from `now_ms()` a few milliseconds apart, so the stamped
/// `expiresAt` lands within `±SLACK_MS` of `_creationTime + DEFAULT_DURATION_MS`.
const SLACK_MS: i64 = 5_000;

/// Fixture schema: a `sessions` table with a numeric `expiresAt` field, a
/// single-field non-unique btree index on it, and
/// `ttl: { field, defaultDurationMs }` (camelCase wire casing per spec). Field
/// types use the object form (`{"type":"number"}`) that `FieldType`'s
/// internally-tagged enum deserializes from, matching `kanban_schema_json`.
fn sessions_schema_json() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "sessions": {
                "fields": {
                    "expiresAt": { "type": "number" },
                    "userId": { "type": "string" }
                },
                "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
                "ttl": { "field": "expiresAt", "defaultDurationMs": DEFAULT_DURATION_MS }
            }
        }
    })
}

fn sessions_schema() -> SchemaDef {
    serde_json::from_value(sessions_schema_json()).expect("parse sessions schema")
}

/// Creates a uniquely-named database and pushes the TTL fixture schema.
/// Returns the db name.
async fn setup_ttl_db(pool: &sqlx::PgPool) -> String {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    ddl::push_schema(pool, &name, sessions_schema())
        .await
        .expect("push sessions schema");
    name
}

/// Point-read by id. Mirrors the `Query { get: Some(..), .. }` shape used
/// across the test suite.
async fn get_doc(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
) -> serde_json::Value {
    let query = Query {
        table: table.to_string(),
        get: Some(id.to_string()),
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
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    };
    match execute_query(pool, db, schema, &query, None).await {
        Ok(QueryResult::Doc(Some(doc))) => doc,
        other => panic!("expected Doc(Some(..)) for id {id}, got {other:?}"),
    }
}

// (a) Insert omitting the TTL field → server stamps `expiresAt` to
// `now + defaultDurationMs`; the stamped value must be present on read-back and
// land within the insert window anchored on the row's own `_creationTime`.
#[tokio::test]
async fn insert_stamps_ttl_default_when_field_absent() {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let db = setup_ttl_db(&pool).await;
    let schema = sessions_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "sessions".to_string(),
                doc: serde_json::json!({ "userId": "u1" })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        None,
    )
    .await
    .expect("insert txn");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let doc = get_doc(&pool, &db, &schema, "sessions", &id).await;
    let expires_at = doc["expiresAt"]
        .as_i64()
        .expect("expiresAt stamped as a number");
    let creation_time = doc["_creationTime"]
        .as_i64()
        .expect("_creationTime present");

    let lower = creation_time + DEFAULT_DURATION_MS - SLACK_MS;
    let upper = creation_time + DEFAULT_DURATION_MS + SLACK_MS;
    assert!(
        expires_at >= lower && expires_at <= upper,
        "expiresAt={expires_at} not within [{lower}, {upper}] \
         (_creationTime={creation_time} + defaultDurationMs={DEFAULT_DURATION_MS} ± {SLACK_MS})"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// (b) Insert that DOES supply the TTL field → server must NOT overwrite it: the
// stamp guard skips when the field is already present, so the stored value is
// the caller's, not `now + defaultDurationMs`.
#[tokio::test]
async fn insert_keeps_caller_expires_at_when_field_present() {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let db = setup_ttl_db(&pool).await;
    let schema = sessions_schema();

    // Caller-supplied value far from anything `now + defaultDurationMs` would
    // produce, so a wrong stamp is unambiguous.
    const CALLER_EXPIRES_AT: i64 = 7_000_000_000_000;

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "sessions".to_string(),
                doc: serde_json::json!({ "userId": "u2", "expiresAt": CALLER_EXPIRES_AT })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        None,
    )
    .await
    .expect("insert txn");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let doc = get_doc(&pool, &db, &schema, "sessions", &id).await;
    assert_eq!(
        doc["expiresAt"].as_i64(),
        Some(CALLER_EXPIRES_AT),
        "caller-supplied expiresAt must be preserved, not overwritten by the default stamp"
    );

    let _ = db::drop_database(&pool, &db).await;
}
