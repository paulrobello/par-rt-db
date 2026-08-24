//! Integration tests for document TTL insert-time default stamping (Task 2 of
//! the document-TTL feature). Mirrors the harness conventions of `txn_test.rs`
//! and `scheduled_test.rs`: `crate::common::test_state()` bootstraps `rtdb_auth` and
//! hands out a pool; each test creates a uniquely-named `t<uuid>` database,
//! pushes a schema, and drives the document path through `execute_txn` /
//! `execute_query` directly (no HTTP).

use rtdb_server::auth::PrincipalCtx;
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
async fn setup_ttl_db(pool: &sqlx::PgPool) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    ddl::push_schema(pool, &name, sessions_schema())
        .await
        .expect("push sessions schema");
    crate::common::wrap_test_db(name)
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
        fields: None,
        aggregate: None,
    };
    match execute_query(pool, db, schema, &query, &PrincipalCtx::bypass(), false).await {
        Ok(QueryResult::Doc(Some(doc))) => doc,
        other => panic!("expected Doc(Some(..)) for id {id}, got {other:?}"),
    }
}

// (a) Insert omitting the TTL field → server stamps `expiresAt` to
// `now + defaultDurationMs`; the stamped value must be present on read-back and
// land within the insert window anchored on the row's own `_creationTime`.
#[tokio::test]
async fn insert_stamps_ttl_default_when_field_absent() {
    let state = crate::common::test_state().await;
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
        &PrincipalCtx::bypass(),
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
    let state = crate::common::test_state().await;
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
        &PrincipalCtx::bypass(),
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
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db)
        .await
        .expect("create fresh database");
    let db = crate::common::wrap_test_db(db);

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
        &PrincipalCtx::bypass(),
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
        &PrincipalCtx::bypass(),
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

// ===========================================================================
// Task 4 — the reaper. Drives writes through `state.realtime.committers`
// (NOT `execute_txn` directly) so the per-db committer task is lazily spawned
// via `channel_for`, which also spawns the reaper task. Then polls until the
// expired row is reaped.
// ===========================================================================

use crate::common::{test_state_with_ttl_sweep, wait_until};

/// Point-read by id, returning `true` while the doc is still present. Used by
/// the reaper poll loop (the existing `get_doc` helper panics on `Doc(None)`).
async fn doc_present(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
) -> bool {
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
        fields: None,
        aggregate: None,
    };
    matches!(
        execute_query(pool, db, schema, &query, &PrincipalCtx::bypass(), false).await,
        Ok(QueryResult::Doc(Some(_)))
    )
}

/// Inserts one `sessions` doc with an explicit `expiresAt` (no default
/// stamping) through the committer — this both spawns the committer + reaper
/// tasks for `db` and writes the row. Returns the new doc id.
async fn insert_session_via_committer(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    expires_at: i64,
) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("userId".into(), serde_json::json!("u1"));
    doc.insert("expiresAt".into(), serde_json::json!(expires_at));
    let outcome = state
        .realtime
        .committers
        .mutate(
            db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "sessions".into(),
                    doc,
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("insert via committer");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

// The reaper deletes a row whose `expiresAt` is in the past, and leaves a
// not-yet-due row untouched. Sweep cadence is 1s (test_state_with_ttl_sweep);
// the poll loop bounds the wait to ~10s.
#[tokio::test]
async fn reaper_deletes_expired_document() {
    let state = test_state_with_ttl_sweep(1).await;
    let pool = state.pool.clone();
    let db = setup_ttl_db(&pool).await;
    let schema = sessions_schema();

    // Insert one expired and one live doc through the committer. The first
    // mutate spawns the per-db committer task, which lazily spawns the reaper
    // alongside the scheduler and mutation-log cleanup.
    let past = db::now_ms() - 1_000_000;
    let future = db::now_ms() + 1_000_000;
    let expired_id = insert_session_via_committer(&state, &db, past).await;
    let live_id = insert_session_via_committer(&state, &db, future).await;

    // Poll until the reaper sweeps (interval=1s in this test state). Bound to
    // ~10s so a missing reap fails loudly rather than hanging.
    assert!(
        wait_until(std::time::Duration::from_secs(10), || async {
            !doc_present(&pool, &db, &schema, "sessions", &expired_id).await
        })
        .await,
        "expired doc was not reaped within the poll window"
    );

    // The not-yet-due doc is untouched.
    assert!(
        doc_present(&pool, &db, &schema, "sessions", &live_id).await,
        "live doc must not be reaped"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// ===========================================================================
// Coverage: audit + webhook publish with source="ttl", per-row-auth bypass,
// and non-ttl tables left untouched. Mirrors audit_test.rs / webhook_test.rs
// / per_row_auth_test.rs helper shapes.
// ===========================================================================

use crate::common::{admin_post, spawn_app, test_state_with_ttl_audit_webhooks};
use rtdb_server::ddl::pg_schema;

/// Polls until `expired_id` is gone from `db`'s `sessions` table, bounded to
/// ~10s. Shared by the coverage tests so each can wait for the reaper without
/// re-deriving the poll loop. `table` is the LOGICAL table name (translated to
/// its physical `pg_table` name internally).
async fn poll_until_reaped(pool: &sqlx::PgPool, db: &str, table: &str, expired_id: &str) {
    let schema_name = pg_schema(db);
    let table_ident = ddl::pg_table(table);
    let reaped = wait_until(std::time::Duration::from_secs(10), || async {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM \"{schema_name}\".\"{table_ident}\" WHERE id = $1"
        ))
        .bind(expired_id)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("count {table_ident} row in {schema_name}: {e}"));
        count == 0
    })
    .await;
    assert!(
        reaped,
        "expired doc {expired_id} was not reaped within the poll window"
    );
}

// A reaped row publishes to BOTH the durable audit log (op="delete",
// source="ttl", principal=null) and the webhook outbox (payload source="ttl",
// kind="delete"). Drives the insert over the admin mutate route so the
// committer (and the reaper task it lazily spawns) are exercised through the
// real handler stack — mirroring audit_test.rs / webhook_test.rs.
#[tokio::test]
async fn reaper_delete_publishes_to_audit_and_webhooks() -> anyhow::Result<()> {
    let state = test_state_with_ttl_audit_webhooks(1).await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db).await?;
    let db = crate::common::wrap_test_db(db);

    // Push the sessions ttl schema via the admin route (same wire shape the
    // dashboard uses), then register a webhook matching sessions/delete.
    let push = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db, "schema": sessions_schema_json() }),
    )
    .await;
    assert_eq!(push.status(), reqwest::StatusCode::OK, "push-schema failed");
    let webhook_id: i64 = admin_post(
        addr,
        &format!("/admin/db/{db}/webhooks"),
        serde_json::json!({"url": "http://example.com/hook", "table": "sessions", "events": ["delete"]}),
    )
    .await
    .json::<serde_json::Value>()
    .await?["id"]
        .as_i64()
        .expect("webhook id");

    // Insert an expired doc via admin mutate — this spawns the committer +
    // reaper for this db. The committer's audit/webhook taps fire for the
    // INSERT with source="mutate"; the reaper's later DELETE fires them with
    // source="ttl", which is what we assert below.
    let past = db::now_ms() - 1_000_000;
    let results = admin_post(
        addr,
        &format!("/admin/db/{db}/mutate"),
        serde_json::json!({"txn": {"steps": [
            {"op": "insert", "table": "sessions", "doc": {"userId": "u1", "expiresAt": past}}
        ]}}),
    )
    .await
    .json::<serde_json::Value>()
    .await?["results"]
        .clone();
    let expired_id = results[0]["id"].as_str().expect("id").to_string();

    poll_until_reaped(&pool, &db, "sessions", &expired_id).await;

    // The DELETE auto-commits inside `handle_reaper` and is visible to this poll
    // BEFORE the tap-site writes (fan_out / op_feed / audit / webhook) finish —
    // they run in the same committer turn AFTER the DELETE, each on its own
    // await. So poll for the audit row to land before asserting its content.
    let audit_landed = wait_until(std::time::Duration::from_secs(10), || async {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM rtdb.audit_log WHERE db = $1 AND source = 'ttl'",
        )
        .bind(db.as_str())
        .fetch_one(&pool)
        .await
        .expect("count ttl audit rows");
        count > 0
    })
    .await;
    assert!(audit_landed, "ttl audit row never landed after reap");

    // Audit: exactly one row for this db with source="ttl" — the reaped delete.
    // (The insert row carries source="mutate", filtered out by source="ttl".)
    type AuditProbe = (String, Option<String>, String);
    let rows: Vec<AuditProbe> = sqlx::query_as(
        "SELECT tbl, principal, source FROM rtdb.audit_log \
         WHERE db = $1 AND source = 'ttl' ORDER BY id ASC",
    )
    .bind(db.as_str())
    .fetch_all(&pool)
    .await?;
    assert_eq!(rows.len(), 1, "one ttl audit row: {rows:?}");
    assert_eq!(rows[0].0, "sessions", "audit tbl");
    assert!(rows[0].1.is_none(), "audit principal null (system)");
    assert_eq!(rows[0].2, "ttl", "audit source");

    // Also confirm the reaped op was recorded as a delete (separate query so
    // the assertion is unambiguous about op + source together).
    let deleted: Option<(String,)> = sqlx::query_as(
        "SELECT op FROM rtdb.audit_log WHERE db = $1 AND source = 'ttl' AND op = 'delete'",
    )
    .bind(db.as_str())
    .fetch_optional(&pool)
    .await?;
    assert!(deleted.is_some(), "audit row op=delete source=ttl");

    // Webhook: one delivery for the registered webhook, payload carries
    // source="ttl" and kind="delete". The enqueue runs in the same committer
    // turn as the audit write, so poll for it to land too.
    let payload: std::cell::RefCell<Option<serde_json::Value>> = std::cell::RefCell::new(None);
    wait_until(std::time::Duration::from_secs(10), || async {
        if let Some((p,)) = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT payload FROM rtdb.webhook_deliveries WHERE webhook_id = $1 \
             ORDER BY id DESC LIMIT 1",
        )
        .bind(webhook_id)
        .fetch_optional(&pool)
        .await
        .expect("query webhook delivery")
        {
            *payload.borrow_mut() = Some(p);
            true
        } else {
            false
        }
    })
    .await;
    let payload = payload
        .borrow_mut()
        .take()
        .expect("webhook delivery never landed after reap");
    assert_eq!(
        payload["source"],
        serde_json::json!("ttl"),
        "webhook source"
    );
    assert_eq!(payload["kind"], serde_json::json!("delete"), "webhook kind");
    assert_eq!(
        payload["table"],
        serde_json::json!("sessions"),
        "webhook table"
    );
    assert_eq!(
        payload["docId"],
        serde_json::json!(expired_id),
        "webhook docId"
    );
    assert!(payload["owner"].is_null(), "webhook owner null (system)");

    let _ = db::drop_database(&pool, &db).await;
    Ok(())
}

// The reaper is a system principal (owner=None) and bypasses per-row owner
// enforcement: it deletes an expired row on an owner-gated table even though
// no interactive caller initiated the delete. Mirrors per_row_auth_test.rs's
// owner-scoped insert + bypass-query shape.
#[tokio::test]
async fn reaper_bypasses_per_row_owner_auth() {
    let state = test_state_with_ttl_sweep(1).await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db).await.expect("create db");
    let db = crate::common::wrap_test_db(db);
    // notes table with ownerField=userId AND ttl on expiresAt. The ttl field
    // must carry its own single-field btree index (schema-validator requirement).
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({
        "tables": {
            "notes": {
                "fields": {
                    "title": { "type": "string" },
                    "userId": { "type": "string" },
                    "expiresAt": { "type": "number" }
                },
                "indexes": [
                    { "name": "by_user", "fields": ["userId"] },
                    { "name": "by_expiresAt", "fields": ["expiresAt"] }
                ],
                "ownerField": "userId",
                "ttl": { "field": "expiresAt" }
            }
        }
    }))
    .expect("parse notes+ttl schema");
    ddl::push_schema(&pool, &db, schema.clone())
        .await
        .expect("push schema");

    // Insert as alice a note with a past expiresAt. The server stamps
    // userId=alice (ownerField), so the row is owner-gated to alice. A user
    // other than alice could neither read nor delete it — but the reaper can.
    let past = db::now_ms() - 1_000_000;
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), serde_json::json!("expires soon"));
    doc.insert("expiresAt".into(), serde_json::json!(past));
    let outcome = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "notes".into(),
                    doc,
                }],
            },
            PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
                ..Default::default()
            },
        )
        .await
        .expect("insert via committer");
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();

    // Poll until reaped (bypass query, owner=None, sees the row until deleted).
    let schema_name = pg_schema(&db);
    let notes_table = ddl::pg_table("notes");
    wait_until(std::time::Duration::from_secs(10), || async {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM \"{schema_name}\".\"{notes_table}\" WHERE id = $1"
        ))
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("count notes");
        count == 0
    })
    .await;
    // Confirm the owner-gated row is gone — the reaper deleted it despite
    // ownerField enforcement that would block any non-owner interactive caller.
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema_name}\".\"{notes_table}\" WHERE id = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await
    .expect("final count");
    assert_eq!(count, 0, "owner-gated expired row must be reaped");

    let _ = db::drop_database(&pool, &db).await;
}

// A table WITHOUT a ttl block is untouched by the reaper even if it has a
// field named `expiresAt` with a past value — only tables that declare `ttl`
// are swept. Two tables in one db; the reaper reaps the ttl one and leaves the
// plain one alone.
#[tokio::test]
async fn reaper_ignores_tables_without_ttl() {
    let state = test_state_with_ttl_sweep(1).await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &db).await.expect("create db");
    let db = crate::common::wrap_test_db(db);
    // `with_ttl` declares ttl; `plain` has the same expiresAt field shape but
    // NO ttl block — the reaper must never touch it.
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({
        "tables": {
            "with_ttl": {
                "fields": { "expiresAt": { "type": "number" } },
                "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
                "ttl": { "field": "expiresAt" }
            },
            "plain": {
                "fields": { "expiresAt": { "type": "number" } }
            }
        }
    }))
    .expect("parse two-table schema");
    ddl::push_schema(&pool, &db, schema.clone())
        .await
        .expect("push schema");

    let past = db::now_ms() - 1_000_000;
    // Insert an expired doc into each table through the committer (spawns the
    // reaper). Both carry a past expiresAt; only `with_ttl` should be reaped.
    let outcome_ttl = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "with_ttl".into(),
                    doc: serde_json::json!({ "expiresAt": past })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("insert with_ttl");
    let id_ttl = outcome_ttl.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();
    let outcome_plain = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "plain".into(),
                    doc: serde_json::json!({ "expiresAt": past })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("insert plain");
    let id_plain = outcome_plain.results[0]["id"]
        .as_str()
        .expect("id")
        .to_string();

    // Wait for the ttl table's row to be reaped.
    poll_until_reaped(&pool, &db, "with_ttl", &id_ttl).await;

    // The non-ttl table's row is still present — same expired shape, but no
    // `ttl` block means the reaper never considered it.
    let schema_name = pg_schema(&db);
    let plain_table = ddl::pg_table("plain");
    let plain_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema_name}\".\"{plain_table}\" WHERE id = $1"
    ))
    .bind(&id_plain)
    .fetch_one(&pool)
    .await
    .expect("count plain");
    assert_eq!(
        plain_count, 1,
        "non-ttl table row must NOT be reaped despite a past expiresAt"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// (g) Upsert taking the INSERT branch is an insert — the ttl default stamps
// there exactly like the `Insert` step. The server historically skipped it
// (only FM-32 defaults ran in that branch), silently producing non-expiring
// rows and diverging from all four client engines, whose shared insert paths
// stamp it. Pinned to the same `_creationTime + defaultDurationMs ± SLACK_MS`
// window as test (a). The upsert eq lookup needs an index on `userId`, so
// these two tests carry their own fixture variant.
fn sessions_upsert_schema() -> SchemaDef {
    let mut json = sessions_schema_json();
    json["tables"]["sessions"]["indexes"]
        .as_array_mut()
        .expect("indexes array")
        .push(serde_json::json!({ "name": "by_userId", "fields": ["userId"] }));
    serde_json::from_value(json).expect("parse upsert sessions schema")
}

async fn setup_upsert_ttl_db(pool: &sqlx::PgPool) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    ddl::push_schema(pool, &name, sessions_upsert_schema())
        .await
        .expect("push upsert sessions schema");
    crate::common::wrap_test_db(name)
}

#[tokio::test]
async fn upsert_insert_stamps_ttl_default_when_field_absent() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let db = setup_upsert_ttl_db(&pool).await;
    let schema = sessions_upsert_schema();

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "sessions".to_string(),
                index: "by_userId".to_string(),
                eq: vec![serde_json::json!("u1")],
                insert: serde_json::json!({ "userId": "u1" })
                    .as_object()
                    .expect("object")
                    .clone(),
                patch: serde_json::json!({}).as_object().expect("object").clone(),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("upsert txn");
    assert_eq!(outcome.results[0]["inserted"], serde_json::json!(true));
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let doc = get_doc(&pool, &db, &schema, "sessions", &id).await;
    let expires_at = doc["expiresAt"]
        .as_i64()
        .expect("expiresAt stamped as a number on the upsert insert branch");
    let creation_time = doc["_creationTime"]
        .as_i64()
        .expect("_creationTime present");

    let lower = creation_time + DEFAULT_DURATION_MS - SLACK_MS;
    let upper = creation_time + DEFAULT_DURATION_MS + SLACK_MS;
    assert!(
        expires_at >= lower && expires_at <= upper,
        "expiresAt={expires_at} not within [{lower}, {upper}] — a doc born via \
         upsert-insert must expire like any other insert"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// (h) Upsert taking the UPDATE branch never re-stamps: after insert the TTL
// field is ordinary, so a patch that omits it leaves the stored expiry exactly
// as it was (spec: "defaultDurationMs is never re-stamped after insert").
#[tokio::test]
async fn upsert_update_does_not_restamp_ttl_field() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let db = setup_upsert_ttl_db(&pool).await;
    let schema = sessions_upsert_schema();

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
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert txn");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();
    let first = get_doc(&pool, &db, &schema, "sessions", &id).await;
    let first_expires = first["expiresAt"].as_i64().expect("insert stamped expiry");

    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "sessions".to_string(),
                index: "by_userId".to_string(),
                eq: vec![serde_json::json!("u1")],
                insert: serde_json::json!({ "userId": "u1" })
                    .as_object()
                    .expect("object")
                    .clone(),
                patch: serde_json::json!({ "userId": "u1" })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("upsert txn");

    let doc = get_doc(&pool, &db, &schema, "sessions", &id).await;
    assert_eq!(
        doc["expiresAt"].as_i64(),
        Some(first_expires),
        "upsert-update must leave the stored expiry untouched"
    );

    let _ = db::drop_database(&pool, &db).await;
}
