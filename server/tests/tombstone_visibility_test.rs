//! FM-33 tombstone-visibility invariant (card 01a1f177): a soft-deleted doc
//! is invisible to EVERY read surface. The secondary-index scan and the PK
//! point-get filter identically, so the index/get divergence a client
//! reported after a deploy flap has no server-side path — this test pins
//! that invariant: if a tombstone ever enumerates via `by_state` while
//! `get` returns null, it fails.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::create_database;
use rtdb_server::ddl::push_schema;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use serde_json::{Value, json};

use crate::common::{TestDb, test_state, wrap_test_db};

async fn fixture() -> (Arc<AppState>, TestDb, SchemaDef) {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    create_database(&state.pool, &name).await.unwrap();
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(json!({"tables": {
        "tasks": {
            "fields": {
                "state": {"type": "string"},
                "title": {"type": "string"}
            },
            "indexes": [{"name": "by_state", "fields": ["state"]}],
            "softDelete": true
        }
    }}))
    .unwrap();
    push_schema(&state.pool, &db, schema.clone()).await.unwrap();
    (state, db, schema)
}

async fn run(
    state: &AppState,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> Result<TxnOutcome, rtdb_server::error::RtDbError> {
    execute_txn(
        &state.pool,
        db,
        schema,
        &Transaction { steps },
        &PrincipalCtx::bypass(),
        None,
    )
    .await
}

async fn scan(state: &AppState, db: &str, schema: &SchemaDef, q: Query) -> Vec<Value> {
    match execute_query(&state.pool, db, schema, &q, &PrincipalCtx::bypass(), false)
        .await
        .unwrap()
    {
        QueryResult::Docs(docs) => docs,
        other => panic!("expected Docs, got {other:?}"),
    }
}

fn ids(docs: &[Value]) -> Vec<&str> {
    docs.iter()
        .map(|d| d["_id"].as_str().expect("_id string"))
        .collect()
}

#[tokio::test]
async fn tombstoned_doc_is_invisible_to_index_scan_point_get_and_table_scan() {
    let (state, db, schema) = fixture().await;
    let ctx = PrincipalCtx::bypass();

    let outcome = run(
        &state,
        &db,
        &schema,
        vec![
            Step::Insert {
                table: "tasks".into(),
                doc: serde_json::Map::from_iter([
                    ("state".to_string(), json!("open")),
                    ("title".to_string(), json!("first")),
                ]),
            },
            Step::Insert {
                table: "tasks".into(),
                doc: serde_json::Map::from_iter([
                    ("state".to_string(), json!("open")),
                    ("title".to_string(), json!("second")),
                ]),
            },
        ],
    )
    .await
    .unwrap();
    let victim = outcome.results[0]["id"].as_str().unwrap().to_string();
    let survivor = outcome.results[1]["id"].as_str().unwrap().to_string();

    let index_query = || Query {
        table: "tasks".into(),
        index: Some("by_state".into()),
        eq: vec![json!("open")],
        ..Default::default()
    };
    let table_query = || Query {
        table: "tasks".into(),
        ..Default::default()
    };
    let point_get = |id: &str| Query {
        table: "tasks".into(),
        get: Some(id.to_string()),
        ..Default::default()
    };

    // Baseline: the index enumerates both live docs.
    let before = scan(&state, &db, &schema, index_query()).await;
    assert_eq!(ids(&before), vec![victim.as_str(), survivor.as_str()]);

    // Soft-delete one (FM-33: the Delete step stamps `deleted_at` on a
    // softDelete table instead of removing the row — the tombstone stays in
    // the table AND in its secondary index's typed column).
    run(
        &state,
        &db,
        &schema,
        vec![Step::Delete {
            table: "tasks".into(),
            id: victim.clone(),
        }],
    )
    .await
    .unwrap();

    // The claimed divergence: index scan shows the tombstone, point-get
    // returns null. Both surfaces must agree — the tombstone is invisible
    // to the index scan and to the point-get alike.
    let via_index = scan(&state, &db, &schema, index_query()).await;
    assert_eq!(
        ids(&via_index),
        vec![survivor.as_str()],
        "index scan must filter the tombstoned doc exactly like point-get"
    );
    let via_table = scan(&state, &db, &schema, table_query()).await;
    assert_eq!(ids(&via_table), vec![survivor.as_str()]);

    for q in [point_get(&victim), point_get(&survivor)] {
        let result = execute_query(&state.pool, &db, &schema, &q, &ctx, false)
            .await
            .unwrap();
        let got = match result {
            QueryResult::Doc(doc) => doc,
            other => panic!("expected Doc, got {other:?}"),
        };
        let expected = if got.as_ref().and_then(|d| d["_id"].as_str()) == Some(survivor.as_str()) {
            true
        } else {
            got.is_none()
        };
        assert!(
            expected && got.as_ref().and_then(|d| d["_id"].as_str()) != Some(victim.as_str()),
            "point-get must filter the tombstoned doc; got {got:?}"
        );
    }
    let survivor_doc = execute_query(
        &state.pool,
        &db,
        &schema,
        &point_get(&survivor),
        &ctx,
        false,
    )
    .await
    .unwrap();
    assert!(
        matches!(survivor_doc, QueryResult::Doc(Some(_))),
        "the live doc stays visible via point-get: {survivor_doc:?}"
    );
}
