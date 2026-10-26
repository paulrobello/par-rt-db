//! `Step::ExpectNoMatch` (the server half of the `insertIf` conditional insert):
//! precondition semantics, visibility scoping, the by-query step cap, filter
//! validation, and single-writer exclusivity under concurrent HTTP mutates.

use std::sync::Arc;

use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::create_database;
use rtdb_server::ddl::{pg_schema, pg_table, push_schema};
use rtdb_server::error::{ErrorCode, RtDbError};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use serde_json::{Value, json};

use crate::common::{TestDb, admin_post, spawn_app, test_state, wrap_test_db};

struct Fixture {
    state: Arc<AppState>,
    db: TestDb,
    schema: SchemaDef,
}

async fn fixture() -> Fixture {
    let state = test_state().await;
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    create_database(&state.pool, &name).await.unwrap();
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(json!({"tables": {
        "bookings": {
            "fields": {
                "room": {"type": "string"},
                "startsAt": {"type": "number"},
                "endsAt": {"type": "number"}
            },
            "indexes": [{"name": "by_room", "fields": ["room"]}]
        },
        "slots": {
            "fields": {"name": {"type": "string"}},
            "indexes": [],
            "softDelete": true
        },
        "notes": {
            "fields": {"owner": {"type": "string"}, "name": {"type": "string"}},
            "indexes": [],
            "ownerField": "owner"
        }
    }}))
    .unwrap();
    push_schema(&state.pool, &db, schema.clone()).await.unwrap();
    Fixture { state, db, schema }
}

async fn run(f: &Fixture, steps: Vec<Step>, ctx: &PrincipalCtx) -> Result<TxnOutcome, RtDbError> {
    execute_txn(
        &f.state.pool,
        &f.db,
        &f.schema,
        &Transaction { steps },
        ctx,
        None,
    )
    .await
}

fn user(id: &str) -> PrincipalCtx {
    PrincipalCtx {
        user_id: Some(id.into()),
        email: None,
        tables: None,
        host_capability: None,
        enqueuer: None,
    }
}

fn filter_from(value: Value) -> par_rt_db_core::wire::FilterExpr {
    serde_json::from_value(value).unwrap()
}

fn no_match(table: &str, filter: Value) -> Step {
    Step::ExpectNoMatch {
        table: table.into(),
        filter: filter_from(filter),
    }
}

fn insert(table: &str, doc: Value) -> Step {
    Step::Insert {
        table: table.into(),
        doc: doc.as_object().unwrap().clone(),
    }
}

fn room_eq(room: &str) -> Value {
    json!({"op": "eq", "field": "room", "value": room})
}

async fn live_count(f: &Fixture, table: &str) -> i64 {
    let schema = pg_schema(&f.db);
    let ident = pg_table(table);
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"{ident}\""
    )))
    .fetch_one(&f.state.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn no_match_inserts_and_returns_null_then_id() {
    let f = fixture().await;
    let out = run(
        &f,
        vec![
            no_match("bookings", room_eq("r1")),
            insert(
                "bookings",
                json!({"room": "r1", "startsAt": 1, "endsAt": 2}),
            ),
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(out.results.len(), 2);
    assert_eq!(out.results[0], Value::Null);
    assert!(out.results[1]["id"].as_str().is_some());
    assert_eq!(live_count(&f, "bookings").await, 1);
}

#[tokio::test]
async fn match_aborts_with_precondition_failed_and_rolls_back() {
    let f = fixture().await;
    run(
        &f,
        vec![insert(
            "bookings",
            json!({"room": "r1", "startsAt": 1, "endsAt": 2}),
        )],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    let err = run(
        &f,
        vec![
            no_match("bookings", room_eq("r1")),
            insert(
                "bookings",
                json!({"room": "r1", "startsAt": 5, "endsAt": 6}),
            ),
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert_eq!(err.message, "filter matched an existing document");
    assert_eq!(live_count(&f, "bookings").await, 1);
}

#[tokio::test]
async fn sees_insert_from_earlier_step_in_same_txn() {
    let f = fixture().await;
    let err = run(
        &f,
        vec![
            insert(
                "bookings",
                json!({"room": "r1", "startsAt": 1, "endsAt": 2}),
            ),
            no_match("bookings", room_eq("r1")),
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert_eq!(live_count(&f, "bookings").await, 0);
}

#[tokio::test]
async fn soft_deleted_match_does_not_count() {
    let f = fixture().await;
    let out = run(
        &f,
        vec![insert("slots", json!({"name": "a"}))],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    let id = out.results[0]["id"].as_str().unwrap().to_string();
    run(
        &f,
        vec![Step::Delete {
            table: "slots".into(),
            id,
        }],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    let out = run(
        &f,
        vec![
            no_match("slots", json!({"op": "eq", "field": "name", "value": "a"})),
            insert("slots", json!({"name": "a"})),
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(out.results[0], Value::Null);
}

#[tokio::test]
async fn row_the_caller_cannot_see_does_not_count() {
    let f = fixture().await;
    run(
        &f,
        vec![insert("notes", json!({"owner": "alice", "name": "x"}))],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    let probe = || {
        vec![no_match(
            "notes",
            json!({"op": "eq", "field": "name", "value": "x"}),
        )]
    };
    // bob cannot see alice's row, so it does not count for him.
    let out = run(&f, probe(), &user("bob")).await.unwrap();
    assert_eq!(out.results, vec![Value::Null]);
    // alice sees her own row.
    let err = run(&f, probe(), &user("alice")).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    // a bypass principal sees every row.
    let err = run(&f, probe(), &PrincipalCtx::bypass()).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
}

#[tokio::test]
async fn older_than_filter_is_accepted() {
    let f = fixture().await;
    let out = run(
        &f,
        vec![no_match(
            "bookings",
            json!({"op": "olderThan", "field": "startsAt", "ms": 1000}),
        )],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(out.results, vec![Value::Null]);
}

#[tokio::test]
async fn by_query_step_cap_counts_expect_no_match() {
    let f = fixture().await;
    let steps = |n: usize| -> Vec<Step> {
        (0..n)
            .map(|i| no_match("bookings", room_eq(&format!("r{i}"))))
            .collect()
    };
    let out = run(&f, steps(16), &PrincipalCtx::bypass()).await.unwrap();
    assert_eq!(out.results.len(), 16);
    let err = run(&f, steps(17), &PrincipalCtx::bypass())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn undeclared_filter_field_is_bad_request() {
    let f = fixture().await;
    let err = run(
        &f,
        vec![no_match(
            "bookings",
            json!({"op": "eq", "field": "bogus", "value": 1}),
        )],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn concurrent_overlapping_bookings_admit_exactly_one() {
    const N: usize = 8;
    let f = fixture().await;
    let addr = spawn_app(f.state.clone()).await;
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        json!({"db": f.db.as_str(), "name": "expect-no-match"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let token = body["token"].as_str().unwrap().to_string();

    let txn = json!({"steps": [
        {"op": "expectNoMatch", "table": "bookings", "filter": {"op": "and", "exprs": [
            {"op": "eq", "field": "room", "value": "r1"},
            {"op": "lt", "field": "startsAt", "value": 200},
            {"op": "gt", "field": "endsAt", "value": 100}
        ]}},
        {"op": "insert", "table": "bookings",
         "doc": {"room": "r1", "startsAt": 100, "endsAt": 200}}
    ]});

    let mut handles = Vec::new();
    for _ in 0..N {
        let token = token.clone();
        let db = f.db.as_str().to_string();
        let txn = txn.clone();
        handles.push(tokio::spawn(async move {
            let resp = reqwest::Client::new()
                .post(format!("http://{addr}/api/mutate"))
                .header("Authorization", format!("Bearer {token}"))
                .json(&json!({"db": db, "txn": txn}))
                .send()
                .await
                .unwrap();
            let status = resp.status();
            let body: Value = resp.json().await.unwrap();
            (status, body)
        }));
    }
    let mut ok = 0;
    let mut failed = 0;
    for h in handles {
        let (status, body) = h.await.unwrap();
        if status == reqwest::StatusCode::OK {
            ok += 1;
        } else {
            assert_eq!(body["code"], "PRECONDITION_FAILED", "body: {body}");
            failed += 1;
        }
    }
    assert_eq!(ok, 1);
    assert_eq!(failed, N - 1);
    assert_eq!(live_count(&f, "bookings").await, 1);
}
