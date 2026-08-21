//! FM-36 — server-stamped `updatedAtField`. Push-time validation (undeclared /
//! non-numeric / ttl-collision) and stamp semantics on every version-bumping
//! write path: insert, patch, replace, upsert (both branches), patchByQuery,
//! and cascade setNull — each overwriting a client-supplied value. The stamp
//! wins over a `defaults` entry on the same field (same authority family as
//! the ttl default), and snapshot export/import preserves the stored value
//! verbatim (import is replay, never a re-stamp).

mod common;

use common::{test_state, wrap_test_db};
use rtdb_server::ddl::push_schema;
use rtdb_server::dsl::FilterExpr;
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use sqlx::PgPool;

/// Epoch-ms floor: any real stamp is far past this, so a stamped value is
/// distinguishable from every client-supplied literal used below.
const ANCIENT: i64 = 1_000_000_000_000;

fn schema_json(updated_at: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"tables":{"tasks":{
        "fields":{
            "title":{"type":"string"},
            "updatedAt": updated_at},
        "indexes":[{"name":"by_title","fields":["title"]}],
        "updatedAtField":"updatedAt"
    }}})
}

fn number_schema() -> serde_json::Value {
    schema_json(serde_json::json!({"type":"number"}))
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
            table: "tasks".to_string(),
            doc: doc.as_object().expect("json object").clone(),
        }],
    )
    .await
    .expect("insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

async fn fetch_doc(pool: &PgPool, db: &str, id: &str) -> serde_json::Value {
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_tasks\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("fetch doc");
    doc
}

fn stamped_number(doc: &serde_json::Value) -> i64 {
    doc["updatedAt"].as_i64().expect("numeric updatedAt stamp")
}

/// Distinct wall-clock stamps need distinct milliseconds; every "strictly
/// greater" assertion below waits past the resolution of `now_ms()`.
async fn tick() {
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
}

// ---- push-time validation ----

/// Validation errors surface from `push_schema` itself (structure validation
/// runs before any DDL), but the target database must still exist — mirror
/// defaults_test's create-then-fail pattern.
async fn validation_error_db(state: &rtdb_server::AppState) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    wrap_test_db(name)
}

#[tokio::test]
async fn push_rejects_undeclared_updated_at_field() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;
    let mut json = number_schema();
    json["tables"]["tasks"]["updatedAtField"] = serde_json::json!("nope");
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message
            .contains("updatedAtField 'nope' is not a declared field"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_non_numeric_updated_at_field() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;
    // the field is declared but string-typed
    let json = schema_json(serde_json::json!({"type":"string"}));
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message
            .contains("updatedAtField 'updatedAt' must be a number or bigint field"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_updated_at_field_matching_ttl_field() {
    let state = test_state().await;
    let db = validation_error_db(&state).await;
    let json = serde_json::json!({"tables":{"sessions":{
        "fields":{"token":{"type":"string"},"expiresAt":{"type":"number"}},
        "indexes":[
            {"name":"by_token","fields":["token"]},
            {"name":"by_expiresAt","fields":["expiresAt"]}],
        "ttl":{"field":"expiresAt"},
        "updatedAtField":"expiresAt"
    }}});
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    let err = push_schema(&state.pool, &db, schema).await.unwrap_err();
    assert!(
        err.message.contains("must differ from ttl.field"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_accepts_and_round_trips_updated_at_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema) = fresh_db_with(&state, number_schema()).await;
    assert_eq!(
        schema.tables["tasks"].updated_at_field,
        Some("updatedAt".to_string())
    );
    // omitted when unset: an ordinary table serializes without the key
    let plain: SchemaDef = serde_json::from_value(serde_json::json!({"tables":{"tasks":{
        "fields":{"title":{"type":"string"}},
        "indexes":[{"name":"by_title","fields":["title"]}]
    }}}))
    .expect("parse plain schema");
    let wire = serde_json::to_value(&plain.tables["tasks"])?;
    assert!(wire.get("updatedAtField").is_none());
    drop(db);
    Ok(())
}

// ---- stamp semantics: number field ----

#[tokio::test]
async fn insert_stamps_and_overwrites_client_value() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        serde_json::json!({
            "title": "A", "updatedAt": 123
        }),
    )
    .await;
    let doc = fetch_doc(&pool, &db, &id).await;
    let stamped = stamped_number(&doc);
    assert!(
        stamped > ANCIENT,
        "server stamp, not the client's 123: {stamped}"
    );
    Ok(())
}

#[tokio::test]
async fn insert_stamps_int64_field_as_decimal_string() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    // index updatedAt so a typed bigint column exists (typed columns are
    // per-indexed-field) — this also pins that the field is indexable
    let mut json = schema_json(serde_json::json!({"type":"int64"}));
    json["tables"]["tasks"]["indexes"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"name":"by_updatedAt","fields":["updatedAt"]}));
    let (db, schema) = fresh_db_with(&state, json).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let doc = fetch_doc(&pool, &db, &id).await;
    // int64 fields hold decimal strings end to end (wire convention)
    let stamped = doc["updatedAt"]
        .as_str()
        .expect("int64 stamp is a decimal string")
        .parse::<i64>()
        .expect("parses as i64");
    assert!(stamped > ANCIENT, "stamped with epoch-ms: {stamped}");

    // the typed bigint column agrees with the doc body
    let (col,): (i64,) = sqlx::query_as(&format!(
        "SELECT \"f_updatedat\" FROM \"db_{db}\".\"t_tasks\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(col, stamped);
    Ok(())
}

#[tokio::test]
async fn patch_restamps_and_overwrites_client_value() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let first = stamped_number(&fetch_doc(&pool, &db, &id).await);
    tick().await;

    run(
        &pool,
        &db,
        &schema,
        vec![Step::Patch {
            table: "tasks".to_string(),
            id: id.clone(),
            fields: serde_json::json!({"title": "B", "updatedAt": 1})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await;
    let second = stamped_number(&doc);
    assert!(second > first, "patch restamps: {second} !> {first}");
    assert_eq!(doc["title"], "B");
    Ok(())
}

#[tokio::test]
async fn replace_restamps() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let first = stamped_number(&fetch_doc(&pool, &db, &id).await);
    tick().await;

    run(
        &pool,
        &db,
        &schema,
        vec![Step::Replace {
            table: "tasks".to_string(),
            id: id.clone(),
            doc: serde_json::json!({"title": "A2", "updatedAt": 7})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await?;
    let second = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(second > first, "replace restamps: {second} !> {first}");
    Ok(())
}

#[tokio::test]
async fn upsert_insert_stamps_and_update_restamps() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let outcome = run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".to_string(),
            index: "by_title".to_string(),
            eq: vec![serde_json::json!("A")],
            insert: serde_json::json!({"title": "A", "updatedAt": 9})
                .as_object()
                .unwrap()
                .clone(),
            patch: serde_json::json!({}).as_object().unwrap().clone(),
        }],
    )
    .await?;
    let id = outcome.results[0]["id"].as_str().unwrap().to_string();
    let first = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(first > ANCIENT, "upsert-insert stamps: {first}");
    tick().await;

    run(
        &pool,
        &db,
        &schema,
        vec![Step::Upsert {
            table: "tasks".to_string(),
            index: "by_title".to_string(),
            eq: vec![serde_json::json!("A")],
            insert: serde_json::json!({"title": "A"})
                .as_object()
                .unwrap()
                .clone(),
            patch: serde_json::json!({"title": "A3", "updatedAt": 5})
                .as_object()
                .unwrap()
                .clone(),
        }],
    )
    .await?;
    let second = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(
        second > first,
        "upsert-update restamps: {second} !> {first}"
    );
    Ok(())
}

#[tokio::test]
async fn patch_by_query_restamps() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let first = stamped_number(&fetch_doc(&pool, &db, &id).await);
    tick().await;

    run(
        &pool,
        &db,
        &schema,
        vec![Step::PatchByQuery {
            table: "tasks".to_string(),
            filter: FilterExpr::Eq {
                field: "title".to_string(),
                value: serde_json::json!("A"),
            },
            patch: serde_json::json!({"updatedAt": 3})
                .as_object()
                .unwrap()
                .clone(),
            limit: None,
        }],
    )
    .await?;
    let second = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(second > first, "patchByQuery restamps: {second} !> {first}");
    Ok(())
}

#[tokio::test]
async fn cascade_set_null_restamps_child() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let json = serde_json::json!({"tables":{
        "parents":{
            "fields":{"name":{"type":"string"}},
            "indexes":[{"name":"by_name","fields":["name"]}]},
        "children":{
            "fields":{
                "parentId":{"type":"optional","inner":{"type":"id","table":"parents","onDelete":"setNull"}},
                "title":{"type":"string"},
                "updatedAt":{"type":"number"}},
            "indexes":[{"name":"by_parentId","fields":["parentId"]}],
            "updatedAtField":"updatedAt"}
    }});
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(json).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");

    let parent = {
        let outcome = run(
            &pool,
            &db,
            &schema,
            vec![Step::Insert {
                table: "parents".to_string(),
                doc: serde_json::json!({"name": "P"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        )
        .await?;
        outcome.results[0]["id"].as_str().unwrap().to_string()
    };
    let child = {
        let outcome = run(
            &pool,
            &db,
            &schema,
            vec![Step::Insert {
                table: "children".to_string(),
                doc: serde_json::json!({"parentId": parent, "title": "C"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        )
        .await?;
        outcome.results[0]["id"].as_str().unwrap().to_string()
    };
    let (first,): (i64,) = sqlx::query_as(&format!(
        "SELECT (\"doc\"->>'updatedAt')::bigint FROM \"db_{db}\".\"t_children\" WHERE \"id\" = $1"
    ))
    .bind(&child)
    .fetch_one(&pool)
    .await?;
    tick().await;

    run(
        &pool,
        &db,
        &schema,
        vec![Step::Delete {
            table: "parents".to_string(),
            id: parent,
        }],
    )
    .await?;
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_children\" WHERE \"id\" = $1"
    ))
    .bind(&child)
    .fetch_one(&pool)
    .await?;
    assert!(doc.get("parentId").is_none(), "setNull removed the ref");
    let second = stamped_number(&doc);
    assert!(
        second > first,
        "cascade setNull restamps: {second} !> {first}"
    );
    Ok(())
}

#[tokio::test]
async fn stamp_wins_over_defaults_entry() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let mut json = number_schema();
    json["tables"]["tasks"]["defaults"] = serde_json::json!({"updatedAt": 12345.0});
    let (db, schema) = fresh_db_with(&state, json).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let stamped = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(
        stamped > ANCIENT && stamped != 12345,
        "server stamp wins over the defaults entry: {stamped}"
    );
    Ok(())
}

// ---- snapshot replay ----

#[tokio::test]
async fn snapshot_export_import_preserves_stamped_value() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_db_with(&state, number_schema()).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await;
    let stamped = stamped_number(&fetch_doc(&pool, &db, &id).await);
    assert!(stamped > ANCIENT);

    let jsonl = rtdb_server::snapshot::export_database(&pool, &db, &schema).await?;
    let target = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &target)
        .await
        .expect("create target database");
    let _target_db = wrap_test_db(target.clone());
    let imported = rtdb_server::snapshot::import_database(&pool, &target, &jsonl).await?;
    assert_eq!(
        imported.tables["tasks"].updated_at_field,
        Some("updatedAt".to_string()),
        "schema line carries the declaration"
    );

    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{target}\".\"t_tasks\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        stamped_number(&doc),
        stamped,
        "import replays the stored stamp verbatim (no re-stamp)"
    );
    Ok(())
}
