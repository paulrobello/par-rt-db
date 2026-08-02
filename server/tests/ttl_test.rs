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

/// Minimal `sessions` schema (no `expiresAt` field, no index, no ttl) used by
/// the backfill test to seed rows before `expiresAt` + `ttl` are added on a
/// re-push — the realistic migration path the backfill UPDATE targets.
fn sessions_minimal_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({
        "tables": {
            "sessions": { "fields": { "userId": { "type": "string" } } }
        }
    }))
    .expect("parse minimal sessions schema")
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

// (c) Adding `ttl` with a `defaultDurationMs` to a table that already has rows
// backfills the existing rows' `expiresAt` to `created_at + default` — but only
// rows where the field is still NULL; a pre-existing app-set value is preserved
// by the `WHERE col IS NULL` guard. The backfill runs inside `push_schema`'s
// migration transaction when the ttl block is added on a re-push.
//
// Push sequence (the realistic migration path the backfill targets):
//   v1: minimal `sessions` (userId only) — seed row A, which has no `expiresAt`
//       field, so the typed column is NULL once the field is added.
//   v2: add `expiresAt` + its index (still no ttl) — seed row B WITH a caller-
//       supplied `expiresAt`; row A's column stays NULL.
//   v3: add `ttl` + `defaultDurationMs` — backfill stamps row A (NULL→stamped),
//       leaves row B untouched (non-NULL, `IS NULL` guard).
#[tokio::test]
async fn adding_ttl_backfills_existing_rows() {
    let state = common::test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db)
        .await
        .expect("create fresh database");

    // v1: minimal sessions table. Insert row A — it has no `expiresAt` field.
    let schema_v1 = sessions_minimal_schema();
    ddl::push_schema(&pool, &db, schema_v1.clone())
        .await
        .expect("push v1 minimal schema");
    let outcome = execute_txn(
        &pool,
        &db,
        &schema_v1,
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
    .expect("insert row A under v1");
    let id_backfill = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // v2: add the `expiresAt` field + its index, but no ttl yet. Insert row B
    // WITH a caller-supplied `expiresAt` far from anything the backfill would
    // produce — it must survive the v3 backfill. Row A's `expiresAt` column is
    // NULL after this push (added, absent from its doc).
    const CALLER_EXPIRES_AT: i64 = 7_000_000_000_000;
    let schema_v2: SchemaDef = serde_json::from_value(serde_json::json!({
        "tables": {
            "sessions": {
                "fields": {
                    "expiresAt": { "type": "number" },
                    "userId": { "type": "string" }
                },
                "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }]
            }
        }
    }))
    .expect("parse v2 schema");
    ddl::push_schema(&pool, &db, schema_v2.clone())
        .await
        .expect("push v2 field+index schema");
    let outcome = execute_txn(
        &pool,
        &db,
        &schema_v2,
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
    .expect("insert row B under v2");
    let id_keep = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Capture row A's state before the ttl re-push. `expiresAt` is NULL; the
    // backfill anchors on the physical `created_at`, which `_creationTime`
    // mirrors in epoch-ms.
    let doc_a_before = get_doc(&pool, &db, &schema_v2, "sessions", &id_backfill).await;
    assert!(
        doc_a_before["expiresAt"].is_null(),
        "row A expiresAt must be NULL before the ttl backfill"
    );
    let creation_time = doc_a_before["_creationTime"]
        .as_i64()
        .expect("_creationTime present");

    // v3: add `ttl` + `defaultDurationMs` (index and field already exist). The
    // backfill UPDATE in `push_schema` stamps row A and skips row B.
    let schema_v3 = sessions_schema();
    ddl::push_schema(&pool, &db, schema_v3.clone())
        .await
        .expect("push v3 ttl schema");

    // Row A: `expiresAt` is now `_creationTime + default` within slack.
    let doc_a = get_doc(&pool, &db, &schema_v3, "sessions", &id_backfill).await;
    let expires_at = doc_a["expiresAt"]
        .as_i64()
        .expect("expiresAt backfilled as a number");
    let lower = creation_time + DEFAULT_DURATION_MS - SLACK_MS;
    let upper = creation_time + DEFAULT_DURATION_MS + SLACK_MS;
    assert!(
        expires_at >= lower && expires_at <= upper,
        "backfilled expiresAt={expires_at} not within [{lower}, {upper}] \
         (_creationTime={creation_time} + defaultDurationMs={DEFAULT_DURATION_MS} ± {SLACK_MS})"
    );

    // Row B: the caller's value survives the backfill untouched.
    let doc_b = get_doc(&pool, &db, &schema_v3, "sessions", &id_keep).await;
    assert_eq!(
        doc_b["expiresAt"].as_i64(),
        Some(CALLER_EXPIRES_AT),
        "pre-existing expiresAt must be preserved by the WHERE-col-IS-NULL guard"
    );

    let _ = db::drop_database(&pool, &db).await;
}
