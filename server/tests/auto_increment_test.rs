//! Server-assigned `autoIncrementField` — per-table Postgres sequences.
//! Push-time validation (undeclared / non-int64 / ttl and updatedAt
//! collisions), insert authority (sequential assignment overwriting
//! client-supplied values, distinct under concurrency), post-insert
//! immutability (patch / replace / upsert-update / patchByQuery rejections
//! with round-trip-friendly equal values), legality under a unique index
//! (CONFLICT on duplicate imported values), and snapshot import continuing
//! the sequence past the imported max.

mod common;

use common::{test_state, wrap_test_db};
use rtdb_server::ddl::push_schema;
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use sqlx::PgPool;

fn counter_schema(extra: serde_json::Value) -> serde_json::Value {
    let mut table = serde_json::json!({
        "fields": {
            "title": {"type": "string"},
            "num": {"type": "int64"}
        },
        "indexes": [{"name": "by_title", "fields": ["title"]}],
        "autoIncrementField": "num"
    });
    let table = table.as_object_mut().expect("table object");
    match extra {
        serde_json::Value::Object(patch) => {
            for (k, v) in patch {
                if k == "indexes" {
                    let mut idxs = table["indexes"].as_array().expect("indexes").clone();
                    idxs.extend(v.as_array().expect("index list").iter().cloned());
                    table["indexes"] = serde_json::Value::Array(idxs);
                } else {
                    table.insert(k, v);
                }
            }
        }
        _ => panic!("extra must be an object"),
    }
    serde_json::json!({"tables": {"tickets": table}})
}

/// The same table WITHOUT the counter declaration — for testing a
/// declaration added to an already-populated table.
fn plain_schema() -> serde_json::Value {
    let mut json = counter_schema(serde_json::json!({}));
    json["tables"]["tickets"]
        .as_object_mut()
        .expect("table")
        .remove("autoIncrementField");
    json
}

async fn fresh_db_with(
    state: &rtdb_server::AppState,
    schema_json: serde_json::Value,
) -> (common::TestDb, SchemaDef) {
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

async fn run(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> anyhow::Result<TxnOutcome> {
    Ok(execute_txn(
        pool,
        db,
        schema,
        &Transaction { steps },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?)
}

async fn insert(pool: &PgPool, db: &str, schema: &SchemaDef, doc: serde_json::Value) -> String {
    let outcome = run(
        pool,
        db,
        schema,
        vec![Step::Insert {
            table: "tickets".to_string(),
            doc: doc.as_object().expect("json object").clone(),
        }],
    )
    .await
    .expect("insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

async fn fetch_doc(pool: &PgPool, db: &str, id: &str) -> serde_json::Value {
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_tickets\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("fetch doc");
    doc
}

async fn fetch_counter(pool: &PgPool, db: &str, id: &str) -> String {
    let doc = fetch_doc(pool, db, id).await;
    doc["num"]
        .as_str()
        .expect("int64 decimal-string counter")
        .to_string()
}

async fn validation_error_db(state: &rtdb_server::AppState) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    wrap_test_db(name)
}

/// `run` wraps in anyhow; step-path assertions need the `RtDbError` back out.
fn rtdb_err(err: anyhow::Error) -> rtdb_server::error::RtDbError {
    err.downcast::<rtdb_server::error::RtDbError>()
        .unwrap_or_else(|e| panic!("expected RtDbError, got: {e}"))
}

// ---- push-time validation ----

#[tokio::test]
async fn push_rejects_undeclared_auto_increment_field() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;
    let mut json = counter_schema(serde_json::json!({}));
    json["tables"]["tickets"]["autoIncrementField"] = serde_json::json!("nope");
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message
            .contains("autoIncrementField 'nope' is not a declared field"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_non_int64_auto_increment_field() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;
    for (field, ty) in [
        ("num", serde_json::json!({"type": "number"})),
        ("num", serde_json::json!({"type": "string"})),
        (
            "num",
            serde_json::json!({"type": "optional", "inner": {"type": "int64"}}),
        ),
    ] {
        let mut json = counter_schema(serde_json::json!({}));
        json["tables"]["tickets"]["fields"][field] = ty;
        let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
        let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
        assert!(
            err.message
                .contains("autoIncrementField 'num' must be an int64 field"),
            "unexpected error for {field}: {}",
            err.message
        );
    }
}

#[tokio::test]
async fn push_rejects_counter_colliding_with_ttl_or_updated_at() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;

    // The counter doubles as the ttl field (ttl validation requires a
    // single-field btree index on it first).
    let ttl_json = counter_schema(serde_json::json!({
        "indexes": [{"name": "by_num", "fields": ["num"]}],
        "ttl": {"field": "num"}
    }));
    let schema: SchemaDef = serde_json::from_value(ttl_json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message
            .contains("autoIncrementField 'num' must differ from ttl.field"),
        "unexpected error: {}",
        err.message
    );

    let mut at_json = counter_schema(serde_json::json!({"updatedAtField": "num"}));
    at_json["tables"]["tickets"]["fields"]["num"] = serde_json::json!({"type": "int64"});
    let schema: SchemaDef = serde_json::from_value(at_json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message
            .contains("autoIncrementField 'num' must differ from updatedAtField"),
        "unexpected error: {}",
        err.message
    );
}

// ---- insert authority ----

#[tokio::test]
async fn insert_assigns_sequential_values_and_overwrites_client_value() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;

    // A client-supplied value (even a plausible one) is overwritten: the
    // first insert is 1 regardless.
    let id = insert(
        &pool,
        &db,
        &schema,
        serde_json::json!({"title": "A", "num": "999"}),
    )
    .await;
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");

    let id2 = insert(&pool, &db, &schema, serde_json::json!({"title": "B"})).await;
    assert_eq!(fetch_counter(&pool, &db, &id2).await, "2");

    let id3 = insert(&pool, &db, &schema, serde_json::json!({"title": "C"})).await;
    assert_eq!(fetch_counter(&pool, &db, &id3).await, "3");
    Ok(())
}

/// Distinctness under concurrency is the sequence's whole reason to exist:
/// `nextval` is atomic, so parallel `execute_txn` calls (each its own sqlx
/// transaction, no committer serialization in this test) must still hand out
/// a distinct, gap-free-from-1 run of values.
#[tokio::test]
async fn concurrent_inserts_receive_distinct_sequential_values() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(
        &state,
        counter_schema(serde_json::json!({
            "indexes": [{"name": "by_num", "fields": ["num"], "unique": true}]
        })),
    )
    .await;

    let n = 16;
    let mut handles = Vec::new();
    for i in 0..n {
        let pool = pool.clone();
        let db = db.as_str().to_string();
        let schema = schema.clone();
        handles.push(tokio::spawn(async move {
            insert(
                &pool,
                &db,
                &schema,
                serde_json::json!({"title": format!("t{i}")}),
            )
            .await
        }));
    }
    let mut ids = Vec::new();
    for handle in handles {
        ids.push(handle.await.expect("task join"));
    }

    let mut values: Vec<i64> = Vec::new();
    for id in &ids {
        values.push(fetch_counter(&pool, &db, id).await.parse::<i64>()?);
    }
    values.sort_unstable();
    let expected: Vec<i64> = (1..=n as i64).collect();
    assert_eq!(values, expected, "distinct sequential run from 1");
    Ok(())
}

#[tokio::test]
async fn stamp_wins_over_defaults_entry() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(
        &state,
        counter_schema(serde_json::json!({"defaults": {"num": "42"}})),
    )
    .await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    assert_eq!(
        fetch_counter(&pool, &db, &id).await,
        "1",
        "the sequence stamp wins over a defaults entry on the same field"
    );
    Ok(())
}

// ---- post-insert immutability ----

#[tokio::test]
async fn patch_cannot_change_the_counter() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;
    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;

    // Changing the value is rejected.
    let err = rtdb_err(
        run(
            &pool,
            &db,
            &schema,
            vec![Step::Patch {
                table: "tickets".into(),
                id: id.clone(),
                fields: serde_json::Map::from_iter([("num".to_string(), serde_json::json!("99"))]),
            }],
        )
        .await
        .unwrap_err(),
    );
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );
    assert_eq!(err.status().as_u16(), 400);

    // Round-tripping the same value is allowed.
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Patch {
            table: "tickets".into(),
            id: id.clone(),
            fields: serde_json::Map::from_iter([("num".to_string(), serde_json::json!("1"))]),
        }],
    )
    .await?;
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");
    Ok(())
}

#[tokio::test]
async fn replace_preserves_or_rejects_the_counter() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;
    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;

    // A replace that omits the field keeps the stored value (it validates as
    // a complete document only because the server fills it back in).
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "tickets".into(),
            id: id.clone(),
            doc: serde_json::Map::from_iter([("title".to_string(), serde_json::json!("A2"))]),
        }],
    )
    .await?;
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");
    let doc = fetch_doc(&pool, &db, &id).await;
    assert_eq!(doc["title"], "A2");

    // A replace that changes the value is rejected.
    let err = rtdb_err(
        run(
            &pool,
            &db,
            &schema,
            vec![Step::Replace {
                table: "tickets".into(),
                id: id.clone(),
                doc: serde_json::Map::from_iter([
                    ("title".to_string(), serde_json::json!("A3")),
                    ("num".to_string(), serde_json::json!("5")),
                ]),
            }],
        )
        .await
        .unwrap_err(),
    );
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );

    // Round-tripping the stored value works.
    run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "tickets".into(),
            id: id.clone(),
            doc: serde_json::Map::from_iter([
                ("title".to_string(), serde_json::json!("A4")),
                ("num".to_string(), serde_json::json!("1")),
            ]),
        }],
    )
    .await?;
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");
    Ok(())
}

#[tokio::test]
async fn upsert_insert_assigns_and_update_preserves() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;

    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tickets".into(),
            index: "by_title".into(),
            eq: vec![serde_json::json!("A")],
            insert: serde_json::Map::from_iter([("title".to_string(), serde_json::json!("A"))]),
            patch: serde_json::Map::from_iter([("title".to_string(), serde_json::json!("A"))]),
        }],
    )
    .await?;
    assert_eq!(outcome.results[0]["inserted"], true);
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");

    // Update branch: a patch without the counter preserves it.
    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tickets".into(),
            index: "by_title".into(),
            eq: vec![serde_json::json!("A")],
            insert: serde_json::Map::from_iter([("title".to_string(), serde_json::json!("A"))]),
            patch: serde_json::Map::from_iter([("title".to_string(), serde_json::json!("A2"))]),
        }],
    )
    .await?;
    assert_eq!(outcome.results[0]["inserted"], false);
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");

    // Update branch: changing the counter is rejected.
    let err = rtdb_err(
        run(
            &pool,
            &db,
            &schema,
            vec![Step::Upsert {
                table: "tickets".into(),
                index: "by_title".into(),
                eq: vec![serde_json::json!("A2")],
                insert: serde_json::Map::from_iter([(
                    "title".to_string(),
                    serde_json::json!("A2"),
                )]),
                patch: serde_json::Map::from_iter([("num".to_string(), serde_json::json!("7"))]),
            }],
        )
        .await
        .unwrap_err(),
    );
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );
    Ok(())
}

#[tokio::test]
async fn patch_by_query_cannot_change_the_counter() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;
    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;

    let err = rtdb_err(
        run(
            &pool,
            &db,
            &schema,
            vec![Step::PatchByQuery {
                table: "tickets".into(),
                filter: rtdb_server::dsl::FilterExpr::Eq {
                    field: "title".into(),
                    value: serde_json::json!("A"),
                },
                patch: serde_json::Map::from_iter([("num".to_string(), serde_json::json!("50"))]),
                limit: None,
            }],
        )
        .await
        .unwrap_err(),
    );
    assert!(
        err.message
            .contains("autoIncrementField 'num' cannot be changed"),
        "unexpected error: {}",
        err.message
    );
    assert_eq!(fetch_counter(&pool, &db, &id).await, "1");
    Ok(())
}

// ---- unique index ----

#[tokio::test]
async fn unique_index_rejects_duplicate_imported_values_with_conflict() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let schema_json = counter_schema(serde_json::json!({
        "indexes": [{"name": "by_num", "fields": ["num"], "unique": true}]
    }));
    let schema: SchemaDef = serde_json::from_value(schema_json).expect("parse schema");
    let schema_value = serde_json::to_value(&schema)?;

    // The sequence can never hand out duplicates, so the CONFLICT path is
    // duplicate values arriving by snapshot replay.
    let jsonl = format!(
        "{}\n{}\n{}\n",
        serde_json::json!({"kind": "schema", "schema": schema_value}),
        serde_json::json!({
            "kind": "doc", "table": "tickets", "id": "dup1",
            "doc": {"title": "A", "num": "7"}, "createdAt": 1, "version": 1
        }),
        serde_json::json!({
            "kind": "doc", "table": "tickets", "id": "dup2",
            "doc": {"title": "B", "num": "7"}, "createdAt": 2, "version": 1
        }),
    );

    let target = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &target)
        .await
        .expect("create target database");
    let _target_db = wrap_test_db(target.clone());
    let err = rtdb_server::snapshot::import_database(&pool, &target, &jsonl)
        .await
        .unwrap_err();
    assert_eq!(
        err.status().as_u16(),
        409,
        "duplicate counter values under a unique index are CONFLICT: {}",
        err.message
    );
    Ok(())
}

// ---- sequence repositioning ----

#[tokio::test]
async fn snapshot_import_continues_past_imported_max() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;

    for title in ["A", "B", "C"] {
        insert(&pool, &db, &schema, serde_json::json!({"title": title})).await;
    }

    let jsonl = rtdb_server::snapshot::export_database(&pool, &db, &schema).await?;
    let target = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &target)
        .await
        .expect("create target database");
    let _target_db = wrap_test_db(target.clone());
    let imported = rtdb_server::snapshot::import_database(&pool, &target, &jsonl).await?;
    assert_eq!(
        imported.tables["tickets"].auto_increment_field,
        Some("num".to_string()),
        "schema line carries the declaration"
    );

    // The imported docs replay 1..3 verbatim, and the next insert continues
    // at 4 instead of restarting at 1 (which would collide under a unique
    // index and duplicate ticket numbers either way).
    let id = insert(&pool, &target, &imported, serde_json::json!({"title": "D"})).await;
    assert_eq!(
        fetch_counter(&pool, &target, &id).await,
        "4",
        "numbering continues past the imported max"
    );
    Ok(())
}

#[tokio::test]
async fn declaration_added_to_populated_table_repositions_past_max() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();

    // v1: plain int64 field, client-supplied values 1..=5 (no counter yet).
    let (db, schema_v1) = fresh_db_with(&state, plain_schema()).await;
    for i in 1..=5 {
        insert(
            &pool,
            &db,
            &schema_v1,
            serde_json::json!({"title": format!("t{i}"), "num": i.to_string()}),
        )
        .await;
    }

    // v2: same schema plus the declaration — additive push.
    let schema_v2: SchemaDef =
        serde_json::from_value(counter_schema(serde_json::json!({}))).expect("parse schema");
    push_schema(&pool, &db, schema_v2.clone()).await?;

    let id = insert(&pool, &db, &schema_v2, serde_json::json!({"title": "new"})).await;
    assert_eq!(
        fetch_counter(&pool, &db, &id).await,
        "6",
        "the sequence is repositioned past the stored max, not restarted at 1"
    );
    Ok(())
}

#[tokio::test]
async fn re_push_does_not_disturb_the_sequence() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, counter_schema(serde_json::json!({}))).await;

    insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    insert(&pool, &db, &schema, serde_json::json!({"title": "B"})).await;

    // An unrelated additive push (new field) must not reposition anything.
    let mut evolved = counter_schema(serde_json::json!({}));
    evolved["tables"]["tickets"]["fields"]["owner"] =
        serde_json::json!({"type": "optional", "inner": {"type": "string"}});
    let schema_v2: SchemaDef = serde_json::from_value(evolved).expect("parse schema");
    push_schema(&pool, &db, schema_v2.clone()).await?;

    let id = insert(&pool, &db, &schema_v2, serde_json::json!({"title": "C"})).await;
    assert_eq!(fetch_counter(&pool, &db, &id).await, "3");
    Ok(())
}
