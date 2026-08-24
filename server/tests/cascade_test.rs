//! Integration tests for FM-33 — app-level cascade delete (`onDelete`) and
//! soft delete (`softDelete`). Mirrors the harness conventions of
//! `defaults_test.rs` / `ttl_test.rs`: `crate::common::test_state()` hands out a
//! pool; each test creates a uniquely-named `t<uuid>` database, pushes a
//! schema, and drives the document path through `execute_txn` /
//! `execute_query` directly (no HTTP). The reaper test drives writes through
//! `state.realtime.committers` so the per-db reaper task spawns, per
//! `ttl_test.rs`.
//!
//! The `MAX_CASCADE_ROWS` (10_000) budget conflict is covered by
//! `cascade_over_budget_conflicts_and_rolls_back`: >10k children are seeded
//! with one raw INSERT … generate_series (document txns would need 11+ given
//! MAX_STEPS 1024), then the initiating Delete runs the full document path.

use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::RtDbError;
use rtdb_server::query::{FilterExpr, Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{OpKind, Step, Transaction, TxnOutcome, execute_txn};

// ===========================================================================
// Fixtures
// ===========================================================================

/// An id field referencing `table` with `onDelete: <action>`, wrapped in one
/// `optional` when the action is `setNull` (the only action legal on a
/// non-optional id is cascade/restrict — setNull must be able to hold null).
fn id_field(table: &str, on_delete: &str) -> serde_json::Value {
    let id = serde_json::json!({ "type": "id", "table": table, "onDelete": on_delete });
    if on_delete == "setNull" {
        serde_json::json!({ "type": "optional", "inner": id })
    } else {
        id
    }
}

/// parents + children with `children.parentId` declaring `onDelete: <action>`.
/// The referencing field carries the single-field non-unique btree index the
/// validator requires. `setNull` uses the optional-id shape.
fn fk_schema(on_delete: &str) -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": id_field("parents", on_delete)
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }]
            }
        }
    })
}

/// Same as `fk_schema` but the `parentId` field declares NO onDelete — the v1
/// shape the additive-push test upgrades from.
fn fk_schema_without_on_delete() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": { "type": "id", "table": "parents" }
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }]
            }
        }
    })
}

/// Self-referencing `nodes` (parentId → nodes, onDelete cascade) for the
/// cycle-termination test. Optional id: a root node has no parent.
fn self_ref_schema() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "nodes": {
                "fields": {
                    "name": { "type": "string" },
                    "parentId": {
                        "type": "optional",
                        "inner": { "type": "id", "table": "nodes", "onDelete": "cascade" }
                    }
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }]
            }
        }
    })
}

/// parents + children(onDelete: <action>) where `children` declares
/// `softDelete: true`, plus grandchildren referencing children (cascade).
/// Used for the soft-delete/cascade interaction tests; the grandchildren
/// table proves the cascade stops at a stamped row.
fn soft_child_cascade_schema(on_delete: &str) -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": id_field("parents", on_delete)
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }],
                "softDelete": true
            },
            "grandchildren": {
                "fields": {
                    "note": { "type": "string" },
                    "childId": { "type": "id", "table": "children", "onDelete": "cascade" }
                },
                "indexes": [{ "name": "by_child", "fields": ["childId"] }]
            }
        }
    })
}

/// tasks (softDelete) with a UNIQUE index on `name` — the unique-exclusion
/// contract — plus a plain by_done index for collect/count terminals. `plain`
/// is a hard table used by the undelete-on-non-soft-table rejection.
fn soft_tasks_schema() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "tasks": {
                "fields": {
                    "name": { "type": "string" },
                    "done": { "type": "boolean" }
                },
                "indexes": [
                    { "name": "by_name", "fields": ["name"], "unique": true },
                    { "name": "by_done", "fields": ["done"] }
                ],
                "softDelete": true
            },
            "plain": { "fields": { "note": { "type": "string" } } }
        }
    })
}

/// parents + owner-gated children (cascade) for the cascade-bypasses-ownerField
/// test. Children are stamped `userId` on insert (ownerField).
fn owner_cascade_schema() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "userId": { "type": "string" },
                    "parentId": { "type": "id", "table": "parents", "onDelete": "cascade" }
                },
                "indexes": [
                    { "name": "by_parent", "fields": ["parentId"] },
                    { "name": "by_user", "fields": ["userId"] }
                ],
                "ownerField": "userId"
            }
        }
    })
}

/// sessions (softDelete + ttl on expiresAt) + children referencing sessions
/// (cascade) for the reaper test: TTL expiry hard-deletes even a softDelete
/// table and cascades its children.
fn reaper_cascade_schema() -> serde_json::Value {
    serde_json::json!({
        "tables": {
            "sessions": {
                "fields": { "expiresAt": { "type": "number" } },
                "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
                "ttl": { "field": "expiresAt" },
                "softDelete": true
            },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "sessionId": { "type": "id", "table": "sessions", "onDelete": "cascade" }
                },
                "indexes": [{ "name": "by_session", "fields": ["sessionId"] }]
            }
        }
    })
}

fn parse_schema(value: serde_json::Value) -> SchemaDef {
    serde_json::from_value(value).expect("parse fixture schema")
}

// ===========================================================================
// Harness helpers
// ===========================================================================

async fn setup_db(pool: &sqlx::PgPool, schema: SchemaDef) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(pool, &name)
        .await
        .expect("create fresh database");
    ddl::push_schema(pool, &name, schema)
        .await
        .expect("push fixture schema");
    crate::common::wrap_test_db(name)
}

/// Runs `steps` as one txn under the bypass principal, returning the raw
/// outcome so tests can assert on both `results` and `write_set`.
async fn run_steps(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> Result<TxnOutcome, RtDbError> {
    execute_txn(
        pool,
        db,
        schema,
        &Transaction { steps },
        &PrincipalCtx::bypass(),
    )
    .await
}

async fn insert(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    doc: serde_json::Value,
) -> String {
    let outcome = run_steps(
        pool,
        db,
        schema,
        vec![Step::Insert {
            table: table.to_string(),
            doc: doc.as_object().expect("object").clone(),
        }],
    )
    .await
    .expect("insert txn");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

async fn delete(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
) -> Result<TxnOutcome, RtDbError> {
    run_steps(
        pool,
        db,
        schema,
        vec![Step::Delete {
            table: table.to_string(),
            id: id.to_string(),
        }],
    )
    .await
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
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        fields: None,
        aggregate: None,
    }
}

/// Point-read by id; `None` when absent OR soft-deleted and
/// `include_deleted` is false (the read path's soft-delete filter).
async fn get_doc(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
    include_deleted: bool,
) -> Option<serde_json::Value> {
    let mut query = base_query(table);
    query.get = Some(id.to_string());
    match execute_query(
        pool,
        db,
        schema,
        &query,
        &PrincipalCtx::bypass(),
        include_deleted,
    )
    .await
    {
        Ok(QueryResult::Doc(doc)) => doc,
        other => panic!("expected Doc for id {id}, got {other:?}"),
    }
}

/// collect (take) terminal over `index` with one eq bind.
async fn collect_eq(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    index: &str,
    eq: serde_json::Value,
) -> Vec<serde_json::Value> {
    let mut query = base_query(table);
    query.index = Some(index.to_string());
    query.eq = vec![eq];
    query.take = Some(100);
    match execute_query(pool, db, schema, &query, &PrincipalCtx::bypass(), false).await {
        Ok(QueryResult::Docs(docs)) => docs,
        other => panic!("expected Docs, got {other:?}"),
    }
}

/// count terminal over `index` with one eq bind.
async fn count_eq(
    pool: &sqlx::PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    index: &str,
    eq: serde_json::Value,
) -> i64 {
    let mut query = base_query(table);
    query.index = Some(index.to_string());
    query.eq = vec![eq];
    query.count = true;
    match execute_query(pool, db, schema, &query, &PrincipalCtx::bypass(), false).await {
        Ok(QueryResult::Count(n)) => n,
        other => panic!("expected Count, got {other:?}"),
    }
}

/// Raw physical presence check (bypasses the read path entirely — sees
/// soft-deleted rows too).
async fn row_exists(pool: &sqlx::PgPool, db: &str, table: &str, id: &str) -> bool {
    let schema_name = ddl::pg_schema(db);
    let table_ident = ddl::pg_table(table);
    let n: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema_name}\".\"{table_ident}\" WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("row count");
    n > 0
}

/// Raw state of a row on a soft-delete table: `(doc, version, live)` where
/// `live` is `deleted_at IS NULL`. `None` when the row is physically gone.
async fn soft_row_state(
    pool: &sqlx::PgPool,
    db: &str,
    table: &str,
    id: &str,
) -> Option<(serde_json::Value, i64, bool)> {
    let schema_name = ddl::pg_schema(db);
    let table_ident = ddl::pg_table(table);
    sqlx::query_as(&format!(
        "SELECT \"doc\", \"version\", (\"deleted_at\" IS NULL) AS live \
         FROM \"{schema_name}\".\"{table_ident}\" WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await
    .expect("soft row state")
}

/// Raw physical count of a whole table (sees soft-deleted rows).
async fn table_count(pool: &sqlx::PgPool, db: &str, table: &str) -> i64 {
    let schema_name = ddl::pg_schema(db);
    let table_ident = ddl::pg_table(table);
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM \"{schema_name}\".\"{table_ident}\""
    ))
    .fetch_one(pool)
    .await
    .expect("table count")
}

/// Whether `field`'s typed column is NULL on the row (setNull check).
async fn typed_col_is_null(
    pool: &sqlx::PgPool,
    db: &str,
    table: &str,
    field: &str,
    id: &str,
) -> bool {
    let schema_name = ddl::pg_schema(db);
    let table_ident = ddl::pg_table(table);
    let col = ddl::pg_col(field);
    let (is_null,): (bool,) = sqlx::query_as(&format!(
        "SELECT (\"{col}\" IS NULL) FROM \"{schema_name}\".\"{table_ident}\" WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("typed col null check");
    is_null
}

// ===========================================================================
// Cascade / restrict / setNull
// ===========================================================================

// Deleting a parent with onDelete: cascade removes every child in the same
// txn, and the WriteSet records a Delete op for the parent AND each child —
// the op-feed/audit/webhook/fan-out "every durable write publishes" contract.
#[tokio::test]
async fn cascade_delete_removes_children_and_records_every_op() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(fk_schema("cascade"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p1" }),
    )
    .await;
    let mut child_ids = Vec::new();
    for i in 0..3 {
        child_ids.push(
            insert(
                &pool,
                &db,
                &schema,
                "children",
                serde_json::json!({ "note": format!("c{i}"), "parentId": parent_id }),
            )
            .await,
        );
    }

    let outcome = delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect("cascade delete succeeds");
    assert_eq!(
        outcome.results[0],
        serde_json::Value::Null,
        "delete result is null"
    );

    // Parent and every child are physically gone.
    assert!(!row_exists(&pool, &db, "parents", &parent_id).await);
    for id in &child_ids {
        assert!(
            !row_exists(&pool, &db, "children", id).await,
            "child {id} cascaded"
        );
    }

    // WriteSet: one Delete op per child + one for the parent, all present in
    // `docs` too (point-read subscriptions see every removal).
    let child_deletes = outcome
        .write_set
        .ops
        .iter()
        .filter(|op| op.table == "children" && op.kind == OpKind::Delete)
        .count();
    let parent_deletes = outcome
        .write_set
        .ops
        .iter()
        .filter(|op| op.table == "parents" && op.kind == OpKind::Delete)
        .count();
    assert_eq!(child_deletes, 3, "one Delete op per cascaded child");
    assert_eq!(parent_deletes, 1, "one Delete op for the parent");
    for id in &child_ids {
        assert!(
            outcome
                .write_set
                .docs
                .contains(&("children".to_string(), id.clone())),
            "docs set carries child {id}"
        );
    }
    assert_eq!(table_count(&pool, &db, "children").await, 0);

    let _ = db::drop_database(&pool, &db).await;
}

// onDelete: restrict blocks the parent delete with a Conflict naming the
// referencing table.field, and the whole txn rolls back — an earlier
// childless delete in the same txn does not survive the failure.
#[tokio::test]
async fn restrict_delete_conflicts_naming_field_and_rolls_back_whole_txn() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(fk_schema("restrict"));
    let db = setup_db(&pool, schema.clone()).await;

    let p_childless = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "free" }),
    )
    .await;
    let p_ref = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "held" }),
    )
    .await;
    let child_id = insert(
        &pool,
        &db,
        &schema,
        "children",
        serde_json::json!({ "note": "c", "parentId": p_ref }),
    )
    .await;

    let err = run_steps(
        &pool,
        &db,
        &schema,
        vec![
            Step::Delete {
                table: "parents".into(),
                id: p_childless.clone(),
            },
            Step::Delete {
                table: "parents".into(),
                id: p_ref.clone(),
            },
        ],
    )
    .await
    .expect_err("restrict must block the delete");
    assert_eq!(
        err.code,
        rtdb_server::error::ErrorCode::Conflict,
        "restrict surfaces as Conflict: {err}"
    );
    assert!(
        err.message.contains("children.parentId"),
        "error names the referencing table.field: {err}"
    );
    assert!(
        err.message.contains("is referenced by"),
        "error names the blocking child: {err}"
    );

    // Atomic: the earlier childless delete rolled back with the failed step.
    assert!(
        row_exists(&pool, &db, "parents", &p_childless).await,
        "txn rolled back"
    );
    assert!(row_exists(&pool, &db, "parents", &p_ref).await);
    assert!(row_exists(&pool, &db, "children", &child_id).await);

    // Deleting the child first unblocks the parent delete.
    delete(&pool, &db, &schema, "children", &child_id)
        .await
        .expect("child delete");
    delete(&pool, &db, &schema, "parents", &p_ref)
        .await
        .expect("parent delete unblocked once the child is gone");

    let _ = db::drop_database(&pool, &db).await;
}

// onDelete: setNull leaves the child row in place with the typed column
// cleared, the `parentId` key REMOVED from the doc body (not set to null),
// and version bumped — recorded as a patch-shaped op.
#[tokio::test]
async fn set_null_clears_column_doc_key_and_bumps_version() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(fk_schema("setNull"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p" }),
    )
    .await;
    let child_id = insert(
        &pool,
        &db,
        &schema,
        "children",
        serde_json::json!({ "note": "c", "parentId": parent_id }),
    )
    .await;
    let before_version = get_doc(&pool, &db, &schema, "children", &child_id, false)
        .await
        .expect("child live before")["_version"]
        .as_i64()
        .expect("version number");

    let outcome = delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect("setNull delete succeeds");

    // Parent gone; child row physically present, still live.
    assert!(!row_exists(&pool, &db, "parents", &parent_id).await);
    assert!(row_exists(&pool, &db, "children", &child_id).await);

    // Typed column cleared + doc key REMOVED (absent, not null) + version+1.
    assert!(
        typed_col_is_null(&pool, &db, "children", "parentId", &child_id).await,
        "typed f_ column must be NULL after setNull"
    );
    let doc = get_doc(&pool, &db, &schema, "children", &child_id, false)
        .await
        .expect("child still visible");
    assert!(
        doc.get("parentId").is_none(),
        "doc key removed, not nulled: {doc}"
    );
    assert_eq!(
        doc["_version"].as_i64(),
        Some(before_version + 1),
        "setNull bumps version like a patch"
    );

    // Patch-shaped op on the child.
    let child_patches = outcome
        .write_set
        .ops
        .iter()
        .filter(|op| op.table == "children" && op.kind == OpKind::Patch)
        .count();
    assert_eq!(child_patches, 1, "setNull records a patch-shaped op");

    let _ = db::drop_database(&pool, &db).await;
}

// A self-referencing cascade cycle terminates: A references itself, B and C
// hang off the chain; deleting A removes all three without looping.
#[tokio::test]
async fn cascade_self_reference_cycle_terminates() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(self_ref_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let a = insert(
        &pool,
        &db,
        &schema,
        "nodes",
        serde_json::json!({ "name": "a" }),
    )
    .await;
    // Self-cycle: a.parentId = a.
    let b = insert(
        &pool,
        &db,
        &schema,
        "nodes",
        serde_json::json!({ "name": "b", "parentId": a }),
    )
    .await;
    let c = insert(
        &pool,
        &db,
        &schema,
        "nodes",
        serde_json::json!({ "name": "c", "parentId": b }),
    )
    .await;
    run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Patch {
            table: "nodes".into(),
            id: a.clone(),
            fields: serde_json::json!({ "parentId": a })
                .as_object()
                .expect("object")
                .clone(),
        }],
    )
    .await
    .expect("close the self cycle");

    delete(&pool, &db, &schema, "nodes", &a)
        .await
        .expect("self-referencing cascade terminates");

    assert!(!row_exists(&pool, &db, "nodes", &a).await, "a gone");
    assert!(!row_exists(&pool, &db, "nodes", &b).await, "b cascaded");
    assert!(
        !row_exists(&pool, &db, "nodes", &c).await,
        "c cascaded through b"
    );
    assert_eq!(table_count(&pool, &db, "nodes").await, 0);

    let _ = db::drop_database(&pool, &db).await;
}

// A softDelete child under a cascading parent gets STAMPED, not hard-deleted,
// and the cascade stops there — the grandchild is untouched.
#[tokio::test]
async fn soft_delete_child_is_stamped_and_not_recursed_past() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_child_cascade_schema("cascade"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p" }),
    )
    .await;
    let child_id = insert(
        &pool,
        &db,
        &schema,
        "children",
        serde_json::json!({ "note": "c", "parentId": parent_id }),
    )
    .await;
    let grandchild_id = insert(
        &pool,
        &db,
        &schema,
        "grandchildren",
        serde_json::json!({ "note": "g", "childId": child_id }),
    )
    .await;

    delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect("delete succeeds");

    // Parent hard-deleted; child STAMPED (present, not live); grandchild
    // untouched (present and live) — no recursion past a stamped row.
    assert!(
        !row_exists(&pool, &db, "parents", &parent_id).await,
        "parent gone"
    );
    let (child_doc, _, child_live) = soft_row_state(&pool, &db, "children", &child_id)
        .await
        .expect("child row present");
    assert!(!child_live, "soft child is stamped, not hard-deleted");
    assert!(
        child_doc.get("note").is_some(),
        "stamped child keeps its body: {child_doc}"
    );
    // grandchildren is a hard table — present IS live there.
    assert!(
        row_exists(&pool, &db, "grandchildren", &grandchild_id).await,
        "cascade stops at the stamped child — grandchild untouched"
    );

    // The stamped child is invisible to the read path.
    assert!(
        get_doc(&pool, &db, &schema, "children", &child_id, false)
            .await
            .is_none()
    );

    let _ = db::drop_database(&pool, &db).await;
}

// A soft-deleted (stamped) child is INVISIBLE to the parent's cascade: the
// parent delete succeeds and the stamped row is left exactly as it was.
#[tokio::test]
async fn soft_deleted_child_is_invisible_to_cascade() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_child_cascade_schema("cascade"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p" }),
    )
    .await;
    let child_id = insert(
        &pool,
        &db,
        &schema,
        "children",
        serde_json::json!({ "note": "c", "parentId": parent_id }),
    )
    .await;

    // Soft-delete the child FIRST (a stamp — not a cascade trigger).
    delete(&pool, &db, &schema, "children", &child_id)
        .await
        .expect("soft delete child");

    // Now the parent delete: no VISIBLE child references it.
    delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect("stamped child does not block or receive the cascade");

    assert!(
        !row_exists(&pool, &db, "parents", &parent_id).await,
        "parent gone"
    );
    let (_, _, live) = soft_row_state(&pool, &db, "children", &child_id)
        .await
        .expect("stamped child still present");
    assert!(!live, "child stays stamped");

    let _ = db::drop_database(&pool, &db).await;
}

// A soft-deleted child is INVISIBLE to restrict as well — the parent delete
// succeeds once the only referencing child is stamped.
#[tokio::test]
async fn soft_deleted_child_is_invisible_to_restrict() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_child_cascade_schema("restrict"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p" }),
    )
    .await;
    let child_id = insert(
        &pool,
        &db,
        &schema,
        "children",
        serde_json::json!({ "note": "c", "parentId": parent_id }),
    )
    .await;

    // With a live child, restrict blocks.
    let err = delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect_err("live child blocks the delete");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Conflict);

    // Soft-delete the child, then the same delete succeeds.
    delete(&pool, &db, &schema, "children", &child_id)
        .await
        .expect("soft delete child");
    delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect("stamped child is invisible to restrict");

    let _ = db::drop_database(&pool, &db).await;
}

// The cascade child lookup bypasses per-row ownerField auth — the FK
// consequence is deterministic from the schema, not from who deleted the
// parent. A child owned by alice is cascaded when bob deletes the parent,
// even though bob could not touch the child directly.
#[tokio::test]
async fn cascade_child_lookup_bypasses_per_row_owner_auth() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(owner_cascade_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "p" }),
    )
    .await;
    // Insert the child as alice — ownerField stamps userId=alice.
    let alice = PrincipalCtx {
        user_id: Some("alice".to_string()),
        email: None,
        ..Default::default()
    };
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "children".into(),
                doc: serde_json::json!({ "note": "c", "parentId": parent_id })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        &alice,
    )
    .await
    .expect("insert child as alice");
    let child_id = outcome.results[0]["id"].as_str().expect("id").to_string();

    // Control: bob cannot patch alice's child (ownerField is genuinely on).
    let bob = PrincipalCtx {
        user_id: Some("bob".to_string()),
        email: None,
        ..Default::default()
    };
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "children".into(),
                id: child_id.clone(),
                fields: serde_json::json!({ "note": "nope" })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        &bob,
    )
    .await
    .expect_err("bob cannot patch alice's child");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Forbidden);

    // But bob's parent delete cascades the child anyway.
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "parents".into(),
                id: parent_id.clone(),
            }],
        },
        &bob,
    )
    .await
    .expect("parent delete as bob cascades");
    assert!(
        outcome
            .write_set
            .ops
            .iter()
            .any(|op| op.table == "children"),
        "cascaded child op recorded"
    );
    assert!(
        !row_exists(&pool, &db, "children", &child_id).await,
        "child gone"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// ===========================================================================
// DeleteByQuery
// ===========================================================================

// Each row a DeleteByQuery matches deletes through the same onDelete-aware
// path as a per-id Delete — children of every matched parent cascade.
#[tokio::test]
async fn delete_by_query_cascades_each_matched_row() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(fk_schema("cascade"));
    let db = setup_db(&pool, schema.clone()).await;

    for title in ["p1", "p2"] {
        let parent_id = insert(
            &pool,
            &db,
            &schema,
            "parents",
            serde_json::json!({ "title": title }),
        )
        .await;
        insert(
            &pool,
            &db,
            &schema,
            "children",
            serde_json::json!({ "note": "c", "parentId": parent_id }),
        )
        .await;
    }
    // A parent the filter must NOT match.
    let keeper = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "keep" }),
    )
    .await;

    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::DeleteByQuery {
            table: "parents".into(),
            filter: FilterExpr::In {
                field: "title".into(),
                values: vec![serde_json::json!("p1"), serde_json::json!("p2")],
            },
            limit: None,
        }],
    )
    .await
    .expect("delete by query");
    assert_eq!(outcome.results[0]["deleted"], serde_json::json!(2));
    assert_eq!(outcome.results[0]["truncated"], serde_json::json!(false));

    assert_eq!(
        table_count(&pool, &db, "parents").await,
        1,
        "only the keeper remains"
    );
    assert!(row_exists(&pool, &db, "parents", &keeper).await);
    assert_eq!(
        table_count(&pool, &db, "children").await,
        0,
        "both parents' children cascaded"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// The visited set is shared across a DeleteByQuery step: a row already
// cascaded by an earlier matched row's cascade is skipped, not a NotFound
// abort. Self-referencing nodes make one row's cascade delete another matched
// row.
#[tokio::test]
async fn delete_by_query_skips_rows_already_cascaded_this_step() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(self_ref_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let a = insert(
        &pool,
        &db,
        &schema,
        "nodes",
        serde_json::json!({ "name": "a1" }),
    )
    .await;
    insert(
        &pool,
        &db,
        &schema,
        "nodes",
        serde_json::json!({ "name": "a2", "parentId": a }),
    )
    .await;

    // Matches BOTH rows; a's cascade deletes b first, then the loop reaches
    // b and must skip it (already visited), not error.
    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::DeleteByQuery {
            table: "nodes".into(),
            filter: FilterExpr::In {
                field: "name".into(),
                values: vec![serde_json::json!("a1"), serde_json::json!("a2")],
            },
            limit: None,
        }],
    )
    .await
    .expect("visited-skip keeps the step alive");
    assert_eq!(outcome.results[0]["deleted"], serde_json::json!(2));
    assert_eq!(table_count(&pool, &db, "nodes").await, 0);

    let _ = db::drop_database(&pool, &db).await;
}

// DeleteByQuery on a softDelete table stamps every matched row — the rows
// stay physically present, invisible to reads.
#[tokio::test]
async fn delete_by_query_on_soft_table_stamps_rows() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id1 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "one", "done": false }),
    )
    .await;
    let id2 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "two", "done": false }),
    )
    .await;

    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::DeleteByQuery {
            table: "tasks".into(),
            filter: FilterExpr::Eq {
                field: "done".into(),
                value: serde_json::json!(false),
            },
            limit: None,
        }],
    )
    .await
    .expect("delete by query on soft table");
    assert_eq!(outcome.results[0]["deleted"], serde_json::json!(2));

    // Both rows stamped, not removed.
    assert_eq!(
        table_count(&pool, &db, "tasks").await,
        2,
        "rows are stamped, not removed"
    );
    for id in [&id1, &id2] {
        let (_, _, live) = soft_row_state(&pool, &db, "tasks", id)
            .await
            .expect("row present");
        assert!(!live, "{id} stamped");
    }
    assert_eq!(
        count_eq(
            &pool,
            &db,
            &schema,
            "tasks",
            "by_done",
            serde_json::json!(false)
        )
        .await,
        0
    );

    let _ = db::drop_database(&pool, &db).await;
}

// ===========================================================================
// Soft delete semantics
// ===========================================================================

// A soft delete stamps `deleted_at`, bumps version, and hides the row from
// get / collect / count — the row itself stays physically present.
#[tokio::test]
async fn soft_delete_stamps_version_and_hides_from_every_read_terminal() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id1 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "one", "done": false }),
    )
    .await;
    let id2 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "two", "done": false }),
    )
    .await;
    let version_before = get_doc(&pool, &db, &schema, "tasks", &id1, false)
        .await
        .unwrap()["_version"]
        .as_i64()
        .expect("version");

    delete(&pool, &db, &schema, "tasks", &id1)
        .await
        .expect("soft delete");

    // Stamped: present, not live, version bumped.
    let (_, version_after, live) = soft_row_state(&pool, &db, "tasks", &id1)
        .await
        .expect("row present");
    assert!(!live, "deleted_at stamped");
    assert_eq!(
        version_after,
        version_before + 1,
        "soft delete bumps version"
    );

    // Hidden from every read terminal.
    assert!(
        get_doc(&pool, &db, &schema, "tasks", &id1, false)
            .await
            .is_none(),
        "get misses"
    );
    let docs = collect_eq(
        &pool,
        &db,
        &schema,
        "tasks",
        "by_done",
        serde_json::json!(false),
    )
    .await;
    assert_eq!(docs.len(), 1, "collect sees only the live row: {docs:?}");
    assert_eq!(docs[0]["_id"], serde_json::json!(id2));
    assert_eq!(
        count_eq(
            &pool,
            &db,
            &schema,
            "tasks",
            "by_done",
            serde_json::json!(false)
        )
        .await,
        1,
        "count sees only the live row"
    );

    // Deleting an already-soft-deleted row is NotFound (like a hard miss).
    let err = delete(&pool, &db, &schema, "tasks", &id1)
        .await
        .expect_err("double soft delete is NotFound");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::NotFound);

    let _ = db::drop_database(&pool, &db).await;
}

// Soft-deleted rows are excluded from unique indexes: the same key can be
// re-inserted while the original is stamped. Undeleting the original while a
// live duplicate holds the key surfaces the unique violation as Conflict.
#[tokio::test]
async fn soft_deleted_row_is_excluded_from_unique_index_and_undelete_conflicts_on_duplicate() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id1 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "dup", "done": false }),
    )
    .await;
    delete(&pool, &db, &schema, "tasks", &id1)
        .await
        .expect("soft delete the original");

    // Same unique key, fresh row — succeeds because the stamped row is
    // outside the partial unique index's predicate.
    let id2 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "dup", "done": false }),
    )
    .await;
    assert_ne!(id1, id2);
    assert_eq!(
        count_eq(
            &pool,
            &db,
            &schema,
            "tasks",
            "by_name",
            serde_json::json!("dup")
        )
        .await,
        1,
        "only the fresh row is live"
    );

    // Restoring the original while id2 holds the key violates the unique
    // index — surfaced as Conflict, not a raw sqlx error.
    let err = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Undelete {
            table: "tasks".into(),
            id: id1,
        }],
    )
    .await
    .expect_err("undelete onto a held unique key conflicts");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Conflict);

    let _ = db::drop_database(&pool, &db).await;
}

// Undelete restores a stamped row (version+1, visible again), is an
// idempotent no-op on a live row (no version bump), NotFound on an absent
// id, and BadRequest on a table that does not declare softDelete.
#[tokio::test]
async fn undelete_restores_is_idempotent_and_rejects_absent_or_non_soft_tables() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "back", "done": true }),
    )
    .await;
    delete(&pool, &db, &schema, "tasks", &id)
        .await
        .expect("soft delete");
    let (_, stamped_version, _) = soft_row_state(&pool, &db, "tasks", &id).await.unwrap();

    // Restore: null result, version+1, visible again.
    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Undelete {
            table: "tasks".into(),
            id: id.clone(),
        }],
    )
    .await
    .expect("undelete");
    assert_eq!(outcome.results[0], serde_json::Value::Null);
    let doc = get_doc(&pool, &db, &schema, "tasks", &id, false)
        .await
        .expect("restored to visibility");
    assert_eq!(
        doc["_version"].as_i64(),
        Some(stamped_version + 1),
        "undelete bumps version"
    );
    // The restore is a patch-shaped op (content-bearing subs re-run).
    assert!(
        outcome
            .write_set
            .ops
            .iter()
            .any(|op| op.table == "tasks" && op.kind == OpKind::Patch),
        "undelete records a patch-shaped op"
    );

    // Idempotent on a live row: Ok, no version bump.
    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Undelete {
            table: "tasks".into(),
            id: id.clone(),
        }],
    )
    .await
    .expect("undelete on live row is idempotent");
    assert_eq!(outcome.results[0], serde_json::Value::Null);
    let doc = get_doc(&pool, &db, &schema, "tasks", &id, false)
        .await
        .unwrap();
    assert_eq!(
        doc["_version"].as_i64(),
        Some(stamped_version + 1),
        "idempotent undelete does not bump version again"
    );

    // Absent id: NotFound.
    let err = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Undelete {
            table: "tasks".into(),
            id: "no-such-id".into(),
        }],
    )
    .await
    .expect_err("undelete absent id");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::NotFound);

    // Non-soft table: BadRequest naming the table.
    let plain_id = insert(
        &pool,
        &db,
        &schema,
        "plain",
        serde_json::json!({ "note": "x" }),
    )
    .await;
    let err = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Undelete {
            table: "plain".into(),
            id: plain_id,
        }],
    )
    .await
    .expect_err("undelete on non-soft table");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::BadRequest);
    assert!(
        err.message.contains("does not declare softDelete"),
        "error names the constraint: {err}"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// Upsert over a soft-deleted key INSERTS a fresh row: the eq lookup that
// decides insert-vs-update only sees live rows.
#[tokio::test]
async fn upsert_over_soft_deleted_key_inserts_fresh_row() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id1 = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "k", "done": false }),
    )
    .await;
    delete(&pool, &db, &schema, "tasks", &id1)
        .await
        .expect("soft delete");

    let outcome = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("k")],
            insert: serde_json::json!({ "name": "k", "done": true })
                .as_object()
                .expect("object")
                .clone(),
            patch: serde_json::json!({ "done": true })
                .as_object()
                .expect("object")
                .clone(),
        }],
    )
    .await
    .expect("upsert over soft-deleted key");
    assert_eq!(
        outcome.results[0]["inserted"],
        serde_json::json!(true),
        "inserts fresh"
    );
    let id2 = outcome.results[0]["id"].as_str().expect("id").to_string();
    assert_ne!(id1, id2);

    // The stamped original is untouched; both rows are physically present.
    assert_eq!(table_count(&pool, &db, "tasks").await, 2);
    let (_, _, live1) = soft_row_state(&pool, &db, "tasks", &id1).await.unwrap();
    assert!(!live1, "original stays stamped");
    let doc = get_doc(&pool, &db, &schema, "tasks", &id2, false)
        .await
        .expect("fresh row live");
    assert_eq!(doc["done"], serde_json::json!(true));

    let _ = db::drop_database(&pool, &db).await;
}

// ExpectAbsent treats a soft-deleted row as absent (it passes), and fails
// with PreconditionFailed while a live row holds the key.
#[tokio::test]
async fn expect_absent_treats_soft_deleted_row_as_absent() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "e", "done": false }),
    )
    .await;

    // Live row blocks.
    let err = run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::ExpectAbsent {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("e")],
        }],
    )
    .await
    .expect_err("live row fails ExpectAbsent");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::PreconditionFailed);

    // Soft-deleted row is absent.
    delete(&pool, &db, &schema, "tasks", &id)
        .await
        .expect("soft delete");
    run_steps(
        &pool,
        &db,
        &schema,
        vec![Step::ExpectAbsent {
            table: "tasks".into(),
            index: "by_name".into(),
            eq: vec![serde_json::json!("e")],
        }],
    )
    .await
    .expect("soft-deleted key is absent");

    let _ = db::drop_database(&pool, &db).await;
}

// The remaining per-id write paths treat a soft-deleted row as absent:
// ExpectVersion, Patch, and Replace all surface NotFound on a stamped row.
#[tokio::test]
async fn expect_version_and_patch_replace_treat_soft_deleted_row_as_absent() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "s", "done": false }),
    )
    .await;
    let version = get_doc(&pool, &db, &schema, "tasks", &id, false)
        .await
        .unwrap()["_version"]
        .as_i64()
        .expect("version");
    delete(&pool, &db, &schema, "tasks", &id)
        .await
        .expect("soft delete");

    for (label, step) in [
        (
            "expectVersion",
            Step::ExpectVersion {
                table: "tasks".into(),
                id: id.clone(),
                version,
            },
        ),
        (
            "patch",
            Step::Patch {
                table: "tasks".into(),
                id: id.clone(),
                fields: serde_json::json!({ "done": true })
                    .as_object()
                    .expect("object")
                    .clone(),
            },
        ),
        (
            "replace",
            Step::Replace {
                table: "tasks".into(),
                id,
                doc: serde_json::json!({ "name": "s2", "done": true })
                    .as_object()
                    .expect("object")
                    .clone(),
            },
        ),
    ] {
        let Err(err) = run_steps(&pool, &db, &schema, vec![step]).await else {
            panic!("{label} on soft-deleted row must be NotFound");
        };
        assert_eq!(
            err.code,
            rtdb_server::error::ErrorCode::NotFound,
            "{label} on soft-deleted row: {err}"
        );
    }

    let _ = db::drop_database(&pool, &db).await;
}

// The admin-only include_deleted pass-through (the `includeDeleted` body
// field on POST /admin/db/{db}/query) surfaces soft-deleted rows; the default
// (false) keeps them hidden.
#[tokio::test]
async fn include_deleted_pass_through_surfaces_soft_deleted_rows() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(soft_tasks_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        "tasks",
        serde_json::json!({ "name": "ghost", "done": false }),
    )
    .await;
    delete(&pool, &db, &schema, "tasks", &id)
        .await
        .expect("soft delete");

    assert!(
        get_doc(&pool, &db, &schema, "tasks", &id, false)
            .await
            .is_none(),
        "default hides the stamped row"
    );
    let doc = get_doc(&pool, &db, &schema, "tasks", &id, true)
        .await
        .expect("include_deleted=true surfaces the stamped row");
    assert_eq!(doc["_id"], serde_json::json!(id));
    assert_eq!(doc["name"], serde_json::json!("ghost"));

    let _ = db::drop_database(&pool, &db).await;
}

// ===========================================================================
// Push validation
// ===========================================================================

fn validate_schema(value: serde_json::Value) -> Result<(), RtDbError> {
    parse_schema(value).validate()
}

// onDelete buried inside a compositor (array-of-id here — any non-top-level
// shape) is rejected at push time.
#[tokio::test]
async fn push_validation_rejects_nested_on_delete() {
    let err = validate_schema(serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": {
                        "type": "array",
                        "element": { "type": "id", "table": "parents", "onDelete": "cascade" }
                    }
                }
            }
        }
    }))
    .expect_err("nested onDelete rejected");
    assert!(
        err.message
            .contains("onDelete is legal only on a top-level"),
        "error names the constraint: {err}"
    );
}

// onDelete requires a single-field, non-unique, non-partial btree index on
// the referencing field — a unique index (and equally a partial or
// multi-field one) is rejected.
#[tokio::test]
async fn push_validation_rejects_on_delete_without_single_field_index() {
    // No index on the field at all.
    let err = validate_schema(serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": { "type": "id", "table": "parents", "onDelete": "cascade" }
                },
                "indexes": [{ "name": "by_note", "fields": ["note"] }]
            }
        }
    }))
    .expect_err("missing index rejected");
    assert!(
        err.message
            .contains("requires a single-field, non-unique, non-partial btree index"),
        "error names the constraint: {err}"
    );

    // A UNIQUE index on the field is equally rejected.
    let err = validate_schema(serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": { "type": "id", "table": "parents", "onDelete": "cascade" }
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"], "unique": true }]
            }
        }
    }))
    .expect_err("unique index rejected");
    assert!(
        err.message
            .contains("requires a single-field, non-unique, non-partial btree index"),
        "error names the constraint: {err}"
    );
}

// setNull requires the id field to be optional — a plain (required) id with
// onDelete setNull would have nowhere to put the null.
#[tokio::test]
async fn push_validation_rejects_set_null_without_optional() {
    let err = validate_schema(serde_json::json!({
        "tables": {
            "parents": { "fields": { "title": { "type": "string" } } },
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": { "type": "id", "table": "parents", "onDelete": "setNull" }
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }]
            }
        }
    }))
    .expect_err("setNull on a required id rejected");
    assert!(
        err.message
            .contains("onDelete 'setNull' requires the id field to be optional"),
        "error names the constraint: {err}"
    );
}

// The referenced table must exist in the same schema.
#[tokio::test]
async fn push_validation_rejects_unknown_referenced_table() {
    let err = validate_schema(serde_json::json!({
        "tables": {
            "children": {
                "fields": {
                    "note": { "type": "string" },
                    "parentId": { "type": "id", "table": "ghost", "onDelete": "cascade" }
                },
                "indexes": [{ "name": "by_parent", "fields": ["parentId"] }]
            }
        }
    }))
    .expect_err("unknown referenced table rejected");
    assert!(
        err.message.contains("references unknown table 'ghost'"),
        "error names the table: {err}"
    );
}

// Adding onDelete to an existing schema — or changing the action — is an
// ADDITIVE push (never destructive): the comparison strips the action before
// deciding.
#[tokio::test]
async fn adding_or_changing_on_delete_is_an_additive_push() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema_v1 = parse_schema(fk_schema_without_on_delete());
    let db = setup_db(&pool, schema_v1).await;

    // v2: add onDelete: cascade.
    ddl::push_schema(&pool, &db, parse_schema(fk_schema("cascade")))
        .await
        .expect("adding onDelete is additive");

    // v3: change the action — still additive.
    ddl::push_schema(&pool, &db, parse_schema(fk_schema("restrict")))
        .await
        .expect("changing the onDelete action is additive");

    let _ = db::drop_database(&pool, &db).await;
}

// Turning ON softDelete on a table that already has a unique index re-pushes
// cleanly (the index is rebuilt with the `AND deleted_at IS NULL` predicate),
// and afterwards a stamped row no longer conflicts with a fresh row on the
// same unique key.
#[tokio::test]
async fn soft_delete_flag_add_rebuilds_unique_index() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();

    // v1: tasks WITHOUT softDelete.
    let mut v1 = soft_tasks_schema();
    v1["tables"]["tasks"]
        .as_object_mut()
        .expect("table")
        .remove("softDelete");
    let schema_v1 = parse_schema(v1);
    let db = setup_db(&pool, schema_v1.clone()).await;

    let id1 = insert(
        &pool,
        &db,
        &schema_v1,
        "tasks",
        serde_json::json!({ "name": "z", "done": false }),
    )
    .await;

    // A duplicate insert under the PLAIN unique index still conflicts.
    let err = run_steps(
        &pool,
        &db,
        &schema_v1,
        vec![Step::Insert {
            table: "tasks".into(),
            doc: serde_json::json!({ "name": "z", "done": false })
                .as_object()
                .expect("object")
                .clone(),
        }],
    )
    .await
    .expect_err("plain unique index still conflicts");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Conflict);

    // v2: same shape + softDelete: true — the unique index is rebuilt partial.
    let schema_v2 = parse_schema(soft_tasks_schema());
    ddl::push_schema(&pool, &db, schema_v2.clone())
        .await
        .expect("softDelete flag-add re-pushes cleanly");

    // Stamp the original, then the same key inserts fresh — proof the
    // rebuilt index excludes stamped rows.
    delete(&pool, &db, &schema_v2, "tasks", &id1)
        .await
        .expect("soft delete under the rebuilt index");
    insert(
        &pool,
        &db,
        &schema_v2,
        "tasks",
        serde_json::json!({ "name": "z", "done": false }),
    )
    .await;
    assert_eq!(
        table_count(&pool, &db, "tasks").await,
        2,
        "both rows present, one stamped"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// ===========================================================================
// Reaper — TTL expiry hard-deletes even a softDelete table when it has
// onDelete children, cascading them (force_hard).
// ===========================================================================

use crate::common::test_state_with_ttl_sweep;

// The expired session row on a softDelete table is HARD-deleted by the reaper
// (not stamped), and its cascade children go with it.
#[tokio::test]
async fn reaper_hard_deletes_soft_delete_table_and_cascades_children() {
    let state = test_state_with_ttl_sweep(1).await;
    let pool = state.pool.clone();
    let schema = parse_schema(reaper_cascade_schema());
    let db = setup_db(&pool, schema.clone()).await;

    let past = db::now_ms() - 1_000_000;
    // Insert through the committer so the per-db tasks (incl. the reaper)
    // spawn for this db.
    let session_id = {
        let outcome = state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                Transaction {
                    steps: vec![Step::Insert {
                        table: "sessions".into(),
                        doc: serde_json::json!({ "expiresAt": past })
                            .as_object()
                            .expect("object")
                            .clone(),
                    }],
                },
                PrincipalCtx::bypass(),
            )
            .await
            .expect("insert session via committer");
        outcome.results[0]["id"].as_str().expect("id").to_string()
    };
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "children".into(),
                    doc: serde_json::json!({ "note": "c", "sessionId": session_id })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("insert child via committer");

    // Poll until BOTH rows are physically gone (reaper hard-deletes the soft
    // session row and cascades the child), bounded to ~10s.
    let mut gone = false;
    for _ in 0..100 {
        let session_gone = !row_exists(&pool, &db, "sessions", &session_id).await;
        let child_count = table_count(&pool, &db, "children").await;
        if session_gone && child_count == 0 {
            gone = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        gone,
        "reaper must hard-delete the expired softDelete session row and cascade its children"
    );

    let _ = db::drop_database(&pool, &db).await;
}

// MAX_CASCADE_ROWS budget guard: a cascade exceeding the 10_000-row budget
// conflicts and rolls back atomically. Seeding >10k children via document
// txns would need 11+ txns (MAX_STEPS caps a txn at 1024 steps), so the
// children are seeded with ONE raw INSERT … generate_series against the
// compiled table shape — the initiating Delete still runs the full document
// path (`execute_txn` → `delete_row_cascade`), which is what the guard
// protects. The parent (1) + 10_001 children cross the budget; the txn must
// leave every row in place.
#[tokio::test]
async fn cascade_over_budget_conflicts_and_rolls_back() {
    let state = crate::common::test_state().await;
    let pool = state.pool.clone();
    let schema = parse_schema(fk_schema("cascade"));
    let db = setup_db(&pool, schema.clone()).await;

    let parent_id = insert(
        &pool,
        &db,
        &schema,
        "parents",
        serde_json::json!({ "title": "hub" }),
    )
    .await;
    let seeded: u64 = 10_001;
    let schema_name = ddl::pg_schema(&db);
    let table_ident = ddl::pg_table("children");
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".\"{table_ident}\" (\"id\", \"doc\", \"created_at\", \"f_parentid\") \
         SELECT 'bulk-' || g, jsonb_build_object('note', 'bulk', 'parentId', $1), 1, $1 \
         FROM generate_series(1, {seeded}) g"
    ))
    .bind(&parent_id)
    .execute(&pool)
    .await
    .expect("bulk-seed children");

    let err = delete(&pool, &db, &schema, "parents", &parent_id)
        .await
        .expect_err("over-budget cascade must conflict");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Conflict);

    // Atomic rollback: nothing was deleted.
    assert!(
        row_exists(&pool, &db, "parents", &parent_id).await,
        "parent must survive the rolled-back cascade"
    );
    assert_eq!(
        table_count(&pool, &db, "children").await,
        seeded as i64,
        "all children must survive the rolled-back cascade"
    );

    let _ = db::drop_database(&pool, &db).await;
}
