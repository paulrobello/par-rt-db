//! FM-32 — field-level default values. Push-time validation (unknown key /
//! type mismatch / null) and apply semantics on the write path: defaults stamp
//! a NEW document that omits the key (insert, replace, upsert-insert), `patch`
//! never re-applies (clearing an optional field stays cleared), and server
//! stamps (ttl default, ownerField, authorize `$user`) win over a defaults
//! entry on the same field. Design: docs/superpowers/specs/2026-08-16-field-defaults-design.md.

mod common;

use common::{test_state, wrap_test_db};
use rtdb_server::ddl::push_schema;
use rtdb_server::migrate::{Directive, plan_migration};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;

fn defaults_schema_json(defaults: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"tables":{"tasks":{
        "fields":{
            "title":{"type":"string"},
            "status":{"type":"union","variants":[
                {"type":"literal","value":"backlog"},{"type":"literal","value":"done"}]},
            "priority":{"type":"number"},
            "note":{"type":"optional","inner":{"type":"string"}}},
        "indexes":[{"name":"by_title","fields":["title"]}],
        "defaults": defaults
    }}})
}

const USUAL_DEFAULTS: &str = r#"{"status":"backlog","priority":0.0}"#;

async fn fresh_defaults_db(
    state: &rtdb_server::AppState,
    defaults_json: serde_json::Value,
) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef =
        serde_json::from_value(defaults_schema_json(defaults_json)).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");
    (db, schema)
}

async fn insert(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    doc: serde_json::Value,
) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "tasks".to_string(),
                doc: doc.as_object().expect("json object").clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    Ok(outcome.results[0]["id"].as_str().expect("id").to_string())
}

async fn fetch_doc(pool: &PgPool, db: &str, id: &str) -> anyhow::Result<serde_json::Value> {
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_tasks\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?;
    Ok(doc)
}

// ---- push-time validation ----

#[tokio::test]
async fn push_rejects_unknown_defaults_key() {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let _db = wrap_test_db(name.clone());
    let schema: SchemaDef =
        serde_json::from_value(defaults_schema_json(serde_json::json!({"nope": 1})))
            .expect("parse schema");
    let err = push_schema(&state.pool, &name, schema).await.unwrap_err();
    assert!(
        err.message.contains("not a declared field"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_type_mismatched_default() {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let _db = wrap_test_db(name.clone());
    // priority is a number; "high" is not
    let schema: SchemaDef = serde_json::from_value(defaults_schema_json(
        serde_json::json!({"priority": "high"}),
    ))
    .expect("parse schema");
    let err = push_schema(&state.pool, &name, schema).await.unwrap_err();
    assert!(
        err.message.contains("does not match the field type"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_rejects_null_default() {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let _db = wrap_test_db(name.clone());
    let schema: SchemaDef =
        serde_json::from_value(defaults_schema_json(serde_json::json!({"note": null})))
            .expect("parse schema");
    let err = push_schema(&state.pool, &name, schema).await.unwrap_err();
    assert!(
        err.message.contains("must not be null"),
        "unexpected error: {}",
        err.message
    );
}

#[tokio::test]
async fn push_accepts_wellformed_defaults() {
    let state = test_state().await;
    let (db, schema) =
        fresh_defaults_db(&state, serde_json::from_str(USUAL_DEFAULTS).unwrap()).await;
    // the round-tripped schema from push_schema keeps the map
    assert_eq!(
        schema.tables["tasks"].defaults.get("status"),
        Some(&serde_json::json!("backlog"))
    );
    drop(db);
}

// ---- apply semantics ----

#[tokio::test]
async fn insert_applies_defaults_for_omitted_keys() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) =
        fresh_defaults_db(&state, serde_json::from_str(USUAL_DEFAULTS).unwrap()).await;

    let id = insert(&pool, &db, &schema, serde_json::json!({"title": "A"})).await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert_eq!(doc["status"], "backlog");
    assert_eq!(doc["priority"], 0.0);
    assert!(doc.get("note").is_none());
    Ok(())
}

#[tokio::test]
async fn insert_client_value_wins() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) =
        fresh_defaults_db(&state, serde_json::from_str(USUAL_DEFAULTS).unwrap()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        serde_json::json!({"title": "A", "status": "done", "priority": 5.0}),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert_eq!(doc["status"], "done");
    assert_eq!(doc["priority"], 5.0);
    Ok(())
}

#[tokio::test]
async fn patch_does_not_reapply_after_clear() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) = fresh_defaults_db(
        &state,
        serde_json::json!({"status":"backlog","priority":0.0,"note":"n/a"}),
    )
    .await;

    let id = insert(
        &pool,
        &db,
        &schema,
        serde_json::json!({"title": "A", "note": "hello"}),
    )
    .await?;
    // patch note -> null removes the optional field (apply_patch null rule)
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "tasks".to_string(),
                id: id.clone(),
                fields: serde_json::json!({"note": null})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert!(doc.get("note").is_none(), "cleared note must stay absent");

    // a later patch touching a different field still does not resurrect it
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "tasks".to_string(),
                id: id.clone(),
                fields: serde_json::json!({"title": "B"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert!(
        doc.get("note").is_none(),
        "defaults must not re-apply on patch"
    );
    Ok(())
}

#[tokio::test]
async fn replace_reapplies_defaults() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) =
        fresh_defaults_db(&state, serde_json::from_str(USUAL_DEFAULTS).unwrap()).await;

    let id = insert(
        &pool,
        &db,
        &schema,
        serde_json::json!({"title": "A", "status": "done"}),
    )
    .await?;
    // replace omits status/priority entirely: a replace is a NEW document, so
    // defaults stamp again
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "tasks".to_string(),
                id: id.clone(),
                doc: serde_json::json!({"title": "A2"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert_eq!(doc["status"], "backlog");
    assert_eq!(doc["priority"], 0.0);
    Ok(())
}

#[tokio::test]
async fn upsert_insert_applies_and_update_does_not() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema) =
        fresh_defaults_db(&state, serde_json::from_str(USUAL_DEFAULTS).unwrap()).await;

    // insert branch: doc omits priority -> default stamped
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "tasks".to_string(),
                index: "by_title".to_string(),
                eq: vec![serde_json::json!("A")],
                insert: serde_json::json!({"title": "A"})
                    .as_object()
                    .unwrap()
                    .clone(),
                patch: serde_json::json!({}).as_object().unwrap().clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let id = outcome.results[0]["id"].as_str().unwrap().to_string();
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert_eq!(doc["priority"], 0.0);

    // update branch: patch sets priority explicitly; defaults never touch it
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "tasks".to_string(),
                index: "by_title".to_string(),
                eq: vec![serde_json::json!("A")],
                insert: serde_json::json!({"title": "A"})
                    .as_object()
                    .unwrap()
                    .clone(),
                patch: serde_json::json!({"priority": 9.0})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let doc = fetch_doc(&pool, &db, &id).await?;
    assert_eq!(
        doc["priority"], 9.0,
        "update branch must not stamp defaults"
    );
    Ok(())
}

#[tokio::test]
async fn ttl_default_wins_over_defaults_entry() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    // expiresAt carries BOTH a ttl defaultDurationMs and a defaults entry;
    // the ttl stamp runs first, so the reaper contract wins.
    let schema_json = serde_json::json!({"tables":{"sessions":{
        "fields":{"token":{"type":"string"},"expiresAt":{"type":"number"}},
        "indexes":[{"name":"by_expiresAt","fields":["expiresAt"]}],
        "ttl":{"field":"expiresAt","defaultDurationMs":60000},
        "defaults":{"expiresAt":12345.0}
    }}});
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(schema_json).expect("parse schema");
    push_schema(&state.pool, &db, schema.clone())
        .await
        .expect("push schema");

    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "sessions".to_string(),
                doc: serde_json::json!({"token": "t"})
                    .as_object()
                    .unwrap()
                    .clone(),
            }],
        },
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await?;
    let id = outcome.results[0]["id"].as_str().unwrap().to_string();
    let (doc,): (serde_json::Value,) = sqlx::query_as(&format!(
        "SELECT \"doc\" FROM \"db_{db}\".\"t_sessions\" WHERE \"id\" = $1"
    ))
    .bind(&id)
    .fetch_one(&pool)
    .await?;
    let stamped = doc["expiresAt"].as_i64().expect("numeric expiresAt");
    assert!(stamped > 1_000_000_000_000, "ttl-stamped, not the literal");
    assert_ne!(stamped, 12345);
    Ok(())
}

// ---- migrate ----

#[test]
fn rename_field_rekeys_defaults() {
    let schema: SchemaDef = serde_json::from_value(defaults_schema_json(
        serde_json::json!({"status":"backlog"}),
    ))
    .expect("parse schema");
    let derived = plan_migration(
        &schema,
        &[Directive::RenameField {
            table: "tasks".to_string(),
            from: "status".to_string(),
            to: "state".to_string(),
        }],
    )
    .expect("plan migration");
    // re-keyed and still valid (a stale key would fail validate)
    derived.validate().expect("derived schema valid");
    assert!(derived.tables["tasks"].defaults.contains_key("state"));
    assert!(!derived.tables["tasks"].defaults.contains_key("status"));
}

#[test]
fn drop_field_removes_defaults_entry() {
    let schema: SchemaDef =
        serde_json::from_value(defaults_schema_json(serde_json::json!({"priority":0.0})))
            .expect("parse schema");
    let derived = plan_migration(
        &schema,
        &[Directive::DropField {
            table: "tasks".to_string(),
            field: "priority".to_string(),
        }],
    )
    .expect("plan migration");
    derived.validate().expect("derived schema valid");
    assert!(derived.tables["tasks"].defaults.is_empty());
}
