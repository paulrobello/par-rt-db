//! `insertOnly` tables: only `insert`/`insertMany` pass the txn gate; every
//! step that modifies or removes an existing row fails with `SCHEMA_VIOLATION`.
//! Schema-driven removal (TTL reaping, `onDelete` cascade) and the admin
//! owner-bypass context stay sanctioned.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::error::{ErrorCode, RtDbError};
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Transaction, TxnOutcome, execute_txn};
use serde_json::{Value, json};

use crate::common::{TestDb, test_state, test_state_with_ttl_sweep, wait_until};

fn schema() -> SchemaDef {
    serde_json::from_value(json!({"tables": {
        "events": {
            "fields": {"kind": {"type": "string"}, "n": {"type": "number"}},
            "indexes": [
                {"name": "by_kind", "fields": ["kind"], "unique": true},
                {"name": "by_n", "fields": ["n"]}
            ],
            "insertOnly": true
        },
        "parents": {
            "fields": {"name": {"type": "string"}},
            "indexes": [{"name": "by_name", "fields": ["name"]}]
        },
        "kids": {
            "fields": {
                "parentId": {"type": "id", "table": "parents", "onDelete": "cascade"},
                "note": {"type": "string"}
            },
            "indexes": [{"name": "by_parent", "fields": ["parentId"]}],
            "insertOnly": true
        }
    }}))
    .expect("parse schema")
}

async fn fresh(state: &Arc<AppState>, schema: &SchemaDef) -> TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    ddl::push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    crate::common::wrap_test_db(name)
}

async fn run(
    state: &Arc<AppState>,
    db: &str,
    schema: &SchemaDef,
    ctx: &PrincipalCtx,
    steps: Value,
) -> Result<TxnOutcome, RtDbError> {
    let txn: Transaction = serde_json::from_value(json!({ "steps": steps })).expect("txn");
    execute_txn(&state.pool, db, schema, &txn, ctx, None).await
}

fn id_of(outcome: &TxnOutcome, i: usize) -> String {
    outcome.results[i]["id"].as_str().expect("id").to_string()
}

#[tokio::test]
async fn push_accepts_flag_and_round_trips() {
    let state = test_state().await;
    let s = schema();
    let _db = fresh(&state, &s).await;
    let wire = serde_json::to_value(&s).expect("serialize");
    assert_eq!(wire["tables"]["events"]["insertOnly"], json!(true));
    assert!(wire["tables"]["parents"].get("insertOnly").is_none());
}

#[tokio::test]
async fn mutating_steps_fail_with_schema_violation_and_inserts_pass() {
    let state = test_state().await;
    let s = schema();
    let db = fresh(&state, &s).await;
    let ctx = PrincipalCtx::bypass();
    let seeded = run(
        &state,
        &db,
        &s,
        &ctx,
        json!([
            {"op": "insert", "table": "events", "doc": {"kind": "a", "n": 1}},
            {"op": "insertMany", "table": "events", "docs": [{"kind": "b", "n": 2}]}
        ]),
    )
    .await
    .expect("insert + insertMany allowed");
    let id = id_of(&seeded, 0);

    let rejected = [
        json!({"op": "patch", "table": "events", "id": id, "fields": {"n": 9}}),
        json!({"op": "adjustCounter", "table": "events", "id": id, "field": "n", "delta": 1}),
        json!({"op": "replace", "table": "events", "id": id, "doc": {"kind": "a", "n": 3}}),
        json!({"op": "delete", "table": "events", "id": id}),
        json!({"op": "undelete", "table": "events", "id": id}),
        json!({"op": "upsert", "table": "events", "index": "by_kind", "eq": ["a"],
               "insert": {"kind": "a", "n": 1}, "patch": {"n": 2}}),
        json!({"op": "upsertMany", "table": "events", "index": "by_kind",
               "docs": [{"kind": "a", "n": 5}]}),
        json!({"op": "patchByQuery", "table": "events",
               "filter": {"op": "eq", "field": "kind", "value": "a"}, "patch": {"n": 7}}),
        json!({"op": "deleteByQuery", "table": "events",
               "filter": {"op": "eq", "field": "kind", "value": "a"}}),
    ];
    for step in rejected {
        let err = run(&state, &db, &s, &ctx, json!([step.clone()]))
            .await
            .expect_err(&format!("{step} must be rejected"));
        assert_eq!(err.code, ErrorCode::SchemaViolation, "{step}: {err:?}");
    }
}

#[tokio::test]
async fn admin_bypass_context_may_modify_insert_only_rows() {
    let state = test_state().await;
    let s = schema();
    let db = fresh(&state, &s).await;
    let seeded = run(
        &state,
        &db,
        &s,
        &PrincipalCtx::bypass(),
        json!([{"op": "insert", "table": "events", "doc": {"kind": "a", "n": 1}}]),
    )
    .await
    .expect("insert");
    let id = id_of(&seeded, 0);
    let admin = PrincipalCtx::admin_bypass();
    run(
        &state,
        &db,
        &s,
        &admin,
        json!([
            {"op": "patch", "table": "events", "id": id, "fields": {"n": 5}},
            {"op": "delete", "table": "events", "id": id}
        ]),
    )
    .await
    .expect("admin owner-bypass is exempt");
}

#[tokio::test]
async fn cascade_delete_reaches_insert_only_child() {
    let state = test_state().await;
    let s = schema();
    let db = fresh(&state, &s).await;
    let ctx = PrincipalCtx::bypass();
    let seeded = run(
        &state,
        &db,
        &s,
        &ctx,
        json!([{"op": "insert", "table": "parents", "doc": {"name": "p"}}]),
    )
    .await
    .expect("parent");
    let parent = id_of(&seeded, 0);
    run(
        &state,
        &db,
        &s,
        &ctx,
        json!([{"op": "insert", "table": "kids", "doc": {"parentId": parent, "note": "x"}}]),
    )
    .await
    .expect("child");
    run(
        &state,
        &db,
        &s,
        &ctx,
        json!([{"op": "delete", "table": "parents", "id": parent}]),
    )
    .await
    .expect("cascade is schema-driven and exempt");
    let q: Query = serde_json::from_value(json!({"table": "kids"})).expect("query");
    let res = execute_query(&state.pool, &db, &s, &q, &ctx, false)
        .await
        .expect("read kids");
    assert!(matches!(res, QueryResult::Docs(ref d) if d.is_empty()));
}

#[tokio::test]
async fn ttl_reaper_still_reaps_insert_only_table() {
    let state = test_state_with_ttl_sweep(1).await;
    let s: SchemaDef = serde_json::from_value(json!({"tables": {"logs": {
        "fields": {"msg": {"type": "string"}, "expiresAt": {"type": "number"}},
        "indexes": [{"name": "by_expires_at", "fields": ["expiresAt"]}],
        "ttl": {"field": "expiresAt"},
        "insertOnly": true
    }}}))
    .expect("parse");
    let db = fresh(&state, &s).await;
    let past = db::now_ms() - 1_000_000;
    let outcome = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            serde_json::from_value(json!({"steps": [
                {"op": "insert", "table": "logs", "doc": {"msg": "m", "expiresAt": past}}
            ]}))
            .expect("txn"),
            PrincipalCtx::bypass(),
        )
        .await
        .expect("insert via committer");
    let id = id_of(&outcome, 0);
    let q: Query = serde_json::from_value(json!({"table": "logs", "get": id})).expect("query");
    assert!(
        wait_until(std::time::Duration::from_secs(10), || async {
            !matches!(
                execute_query(&state.pool, &db, &s, &q, &PrincipalCtx::bypass(), false).await,
                Ok(QueryResult::Doc(Some(_)))
            )
        })
        .await,
        "expired row of an insertOnly table was not reaped"
    );
}
