use std::sync::Arc;
use std::time::{Duration, Instant};

use par_rt_db_core::mutation::FieldExpectation;
use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::{bootstrap, create_database, now_ms};
use rtdb_server::ddl::{pg_schema, pg_table, push_schema};
use rtdb_server::error::{ErrorCode, RtDbError};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, TxnOutcome, execute_txn};
use serde_json::{Value, json};
use sqlx::{PgPool, Postgres};

use crate::common::{
    TestDb, background_guard, test_config, test_hot, test_shared_pool, test_state, wrap_test_db,
};

struct Fixture {
    state: Arc<AppState>,
    db: TestDb,
    schema: SchemaDef,
}

async fn fixture() -> Fixture {
    fixture_for_state(test_state().await).await
}

async fn concurrent_fixture() -> Fixture {
    let config = test_config();
    // The lock holder, blocked writer and lock observer need independent connections.
    let pool = test_shared_pool(&config.database_url, 4).await.unwrap();
    bootstrap(&pool).await.unwrap();
    fixture_for_state(AppState::new(pool, config, test_hot())).await
}

async fn fixture_for_state(state: Arc<AppState>) -> Fixture {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    create_database(&state.pool, &name).await.unwrap();
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(json!({"tables": {
        "rooms": {
            "fields": {
                "owner": {"type": "string"},
                "collaborators": {"type": "array", "element": {"type": "string"}},
                "allowed": {"type": "boolean"},
                "updatedAt": {"type": "number"},
                "ownerGeneration": {"type": "number"},
                "leaseOwner": {"type": "string"},
                "epoch": {"type": "number"},
                "lifecycle": {"type": "string"},
                "journal": {"type": "any"}
            },
            "indexes": [], "ownerField": "owner", "collaboratorsField": "collaborators",
            "authorize": {"op": "eq", "field": "allowed", "value": true},
            "updatedAtField": "updatedAt", "softDelete": true
        },
        "chunks": {"fields": {"name": {"type": "string"}}, "indexes": []}
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

fn journal(tail: i64) -> Value {
    json!({"baseSequence": 1, "tailSequence": tail, "scope": {"backend": "wasm", "version": 1}, "nil": null})
}

async fn seed(f: &Fixture) -> String {
    let result = run(
        f,
        vec![Step::Insert {
            table: "rooms".into(),
            doc: json!({"owner": "alice", "collaborators": ["bob"], "allowed": true,
                "updatedAt": 0, "ownerGeneration": 1, "leaseOwner": "worker-a",
                "epoch": 1, "lifecycle": "ACTIVE", "journal": journal(1)})
            .as_object()
            .unwrap()
            .clone(),
        }],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    result.results[0]["id"].as_str().unwrap().to_string()
}

fn expected(path: &[&str], value: Value) -> FieldExpectation {
    FieldExpectation {
        path: path.iter().map(|key| (*key).to_string()).collect(),
        value,
    }
}

fn guard(id: &str, generation: i64) -> Step {
    Step::ExpectFields {
        table: "rooms".into(),
        id: id.into(),
        expected: vec![
            expected(&["owner"], json!("alice")),
            expected(&["ownerGeneration"], json!(generation)),
            expected(&["leaseOwner"], json!("worker-a")),
            expected(&["epoch"], json!(1)),
            expected(&["lifecycle"], json!("ACTIVE")),
            expected(&["journal", "baseSequence"], json!(1)),
            expected(
                &["journal", "scope"],
                json!({"backend": "wasm", "version": 1}),
            ),
        ],
    }
}

fn patch(id: &str, fields: Value) -> Step {
    Step::Patch {
        table: "rooms".into(),
        id: id.into(),
        fields: fields.as_object().unwrap().clone(),
    }
}

fn chunk(name: &str) -> Step {
    Step::Insert {
        table: "chunks".into(),
        doc: json!({"name": name}).as_object().unwrap().clone(),
    }
}

fn user(id: &str) -> PrincipalCtx {
    PrincipalCtx {
        user_id: Some(id.into()),
        email: None,
        tables: None,
        host_capability: None,
        enqueuer: None,
        admin: false,
        session_hash: None,
    }
}

async fn row(f: &Fixture, id: &str) -> (Value, i64, i64, bool) {
    let schema = pg_schema(&f.db);
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT doc, version, created_at, deleted_at IS NOT NULL FROM \"{schema}\".t_rooms WHERE id = $1"
    )))
    .bind(id)
    .fetch_one(&f.state.pool)
    .await
    .unwrap()
}

async fn chunks(f: &Fixture) -> i64 {
    let schema = pg_schema(&f.db);
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".t_chunks"
    )))
    .fetch_one(&f.state.pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn field_guard_is_read_only_and_allows_unasserted_tail_growth() {
    let f = fixture().await;
    let id = seed(&f).await;
    let before = row(&f, &id).await;
    let result = run(&f, vec![guard(&id, 1)], &user("bob")).await.unwrap();
    assert_eq!(result.results, vec![Value::Null]);
    assert!(result.write_set.tables.is_empty());
    assert!(result.write_set.docs.is_empty());
    assert!(result.write_set.ops.is_empty());
    assert!(result.write_set.doc_values.is_empty());
    assert_eq!(row(&f, &id).await, before);

    run(
        &f,
        vec![patch(&id, json!({"journal": journal(2)}))],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(
        run(
            &f,
            vec![Step::ExpectVersion {
                table: "rooms".into(),
                id: id.clone(),
                version: before.1
            }],
            &user("alice")
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::PreconditionFailed
    );
    run(&f, vec![guard(&id, 1), chunk("accepted")], &user("bob"))
        .await
        .unwrap();
    assert_eq!(chunks(&f).await, 1);
    run(
        &f,
        vec![patch(&id, json!({"ownerGeneration": 2}))],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(
        run(&f, vec![guard(&id, 1), chunk("fenced")], &user("alice"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    assert_eq!(chunks(&f).await, 1);
}

#[tokio::test]
async fn field_guard_mismatch_rolls_back_and_selected_objects_are_exact() {
    let f = fixture().await;
    let id = seed(&f).await;
    let bad = Step::ExpectFields {
        table: "rooms".into(),
        id: id.clone(),
        expected: vec![expected(&["journal", "scope"], json!({"backend": "wasm"}))],
    };
    assert_eq!(
        run(
            &f,
            vec![chunk("before"), bad, chunk("after")],
            &PrincipalCtx::bypass()
        )
        .await
        .unwrap_err()
        .code,
        ErrorCode::PreconditionFailed
    );
    assert_eq!(chunks(&f).await, 0);
    for (key, succeeds) in [("nil", true), ("missing", false)] {
        let result = run(
            &f,
            vec![Step::ExpectFields {
                table: "rooms".into(),
                id: id.clone(),
                expected: vec![expected(&["journal", key], Value::Null)],
            }],
            &user("alice"),
        )
        .await;
        if succeeds {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code, ErrorCode::PreconditionFailed);
        }
    }
}

#[tokio::test]
async fn field_guard_visibility_and_scopes_match_existing_preconditions() {
    let f = fixture().await;
    let id = seed(&f).await;
    for who in ["alice", "bob"] {
        run(&f, vec![guard(&id, 1)], &user(who)).await.unwrap();
    }
    for steps in [
        vec![guard(&id, 99)],
        vec![Step::ExpectVersion {
            table: "rooms".into(),
            id: id.clone(),
            version: 99,
        }],
        vec![Step::ExpectFields {
            table: "rooms".into(),
            id: id.clone(),
            expected: vec![],
        }],
    ] {
        assert_eq!(
            run(&f, steps, &user("charlie")).await.unwrap_err().code,
            ErrorCode::NotFound
        );
    }
    let scoped = PrincipalCtx {
        tables: Some(vec!["chunks".into()]),
        ..PrincipalCtx::bypass()
    };
    assert_eq!(
        run(&f, vec![chunk("rolled-back"), guard(&id, 1)], &scoped)
            .await
            .unwrap_err()
            .code,
        ErrorCode::Forbidden
    );
    assert_eq!(chunks(&f).await, 0);
    assert_eq!(
        run(&f, vec![guard("missing-row", 1)], &user("alice"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    run(
        &f,
        vec![patch(&id, json!({"allowed": false}))],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(
        run(&f, vec![guard(&id, 1)], &user("alice"))
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
    run(
        &f,
        vec![Step::Delete {
            table: "rooms".into(),
            id: id.clone(),
        }],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert!(row(&f, &id).await.3);
    assert_eq!(
        run(&f, vec![guard(&id, 1)], &PrincipalCtx::bypass())
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn field_guard_observes_step_order_and_preserves_deadline_checks() {
    let f = fixture().await;
    let id = seed(&f).await;
    run(
        &f,
        vec![
            patch(&id, json!({"ownerGeneration": 2})),
            guard(&id, 2),
            patch(&id, json!({"ownerGeneration": 3})),
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
    assert_eq!(row(&f, &id).await.0["ownerGeneration"], json!(3));
    let err = run(
        &f,
        vec![
            chunk("expired"),
            guard(&id, 3),
            Step::ExpectBefore { deadline_ms: 1 },
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert_eq!(chunks(&f).await, 0);
    run(
        &f,
        vec![
            guard(&id, 3),
            Step::ExpectBefore {
                deadline_ms: now_ms() + 60_000,
            },
        ],
        &PrincipalCtx::bypass(),
    )
    .await
    .unwrap();
}

async fn block_writes(
    f: &Fixture,
    table_name: &str,
    key: i64,
) -> sqlx::Transaction<'static, Postgres> {
    assert!(matches!(table_name, "rooms" | "chunks"));
    let event = if table_name == "rooms" {
        "UPDATE"
    } else {
        "INSERT"
    };
    let schema = pg_schema(&f.db);
    let table = pg_table(table_name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION \"{schema}\".field_guard_gate() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END; $$"
    )))
    .execute(&f.state.pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TRIGGER field_guard_gate BEFORE {event} ON \"{schema}\".\"{table}\" \
         FOR EACH ROW EXECUTE FUNCTION \"{schema}\".field_guard_gate()"
    )))
    .execute(&f.state.pool)
    .await
    .unwrap();
    let mut connection = f.state.pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *connection)
        .await
        .unwrap();
    connection
}

async fn wait_for_guarded_write(pool: &PgPool, key: i64) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
             AND classid::bigint = $1 AND objid::bigint = $2)",
        )
        .bind(key >> 32)
        .bind(key & 0xffff_ffff)
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "write never reached the controlled SQL gate"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn warm_committer(f: &Fixture) {
    f.state
        .realtime
        .committers
        .mutate(
            &f.db,
            None,
            Transaction { steps: vec![] },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn field_guard_serializes_concurrent_tail_and_authority_changes() {
    let mut base = journal(2);
    base["baseSequence"] = json!(2);
    let mut backend = journal(2);
    backend["scope"]["backend"] = json!("native");
    for (name, fields, accepts) in [
        ("tail", json!({"journal": journal(2)}), true),
        ("base", json!({"journal": base}), false),
        ("backend", json!({"journal": backend}), false),
        ("lease-generation", json!({"ownerGeneration": 2}), false),
        ("lease-owner", json!({"leaseOwner": "worker-b"}), false),
        ("epoch", json!({"epoch": 2}), false),
        ("lifecycle", json!({"lifecycle": "PAUSED"}), false),
    ] {
        let f = concurrent_fixture().await;
        let _background = background_guard(&f.state);
        let id = seed(&f).await;
        warm_committer(&f).await;
        let key = (uuid::Uuid::now_v7().as_u128() % i64::MAX as u128) as i64;
        let lock = block_writes(&f, "rooms", key).await;
        let state = f.state.clone();
        let db = f.db.to_string();
        let changed_id = id.clone();
        let change = tokio::spawn(async move {
            state
                .realtime
                .committers
                .mutate(
                    &db,
                    None,
                    Transaction {
                        steps: vec![patch(&changed_id, fields)],
                    },
                    PrincipalCtx::bypass(),
                )
                .await
        });
        wait_for_guarded_write(&f.state.pool, key).await;
        let request = f.state.realtime.committers.mutate(
            &f.db,
            None,
            Transaction {
                steps: vec![guard(&id, 1), chunk(name)],
            },
            PrincipalCtx::bypass(),
        );
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), request.as_mut())
                .await
                .is_err()
        );
        assert!(!change.is_finished());
        lock.rollback().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), change)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(10), request)
            .await
            .unwrap();
        assert_eq!(row(&f, &id).await.1, 2);
        if accepts {
            result.unwrap();
            assert_eq!(chunks(&f).await, 1, "{name}");
        } else {
            assert_eq!(
                result.unwrap_err().code,
                ErrorCode::PreconditionFailed,
                "{name}"
            );
            assert_eq!(chunks(&f).await, 0, "{name}");
        }
    }
}

#[tokio::test]
async fn field_guard_deadline_loss_during_staged_write_rolls_back() {
    let f = concurrent_fixture().await;
    let _background = background_guard(&f.state);
    let id = seed(&f).await;
    warm_committer(&f).await;
    let before = row(&f, &id).await;
    let key = (uuid::Uuid::now_v7().as_u128() % i64::MAX as u128) as i64;
    let lock = block_writes(&f, "chunks", key).await;
    let deadline = now_ms() + 2000;
    let state = f.state.clone();
    let db = f.db.to_string();
    let staged_id = id.clone();
    let stage = tokio::spawn(async move {
        state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                Transaction {
                    steps: vec![
                        guard(&staged_id, 1),
                        Step::ExpectBefore {
                            deadline_ms: deadline,
                        },
                        chunk("expired-stage"),
                    ],
                },
                PrincipalCtx::bypass(),
            )
            .await
    });
    wait_for_guarded_write(&f.state.pool, key).await;
    assert!(
        now_ms() < deadline,
        "guarded write did not start before its deadline"
    );
    tokio::time::sleep(Duration::from_millis(
        (deadline - now_ms()).max(0) as u64 + 25,
    ))
    .await;
    lock.rollback().await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), stage)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(chunks(&f).await, 0);
    assert_eq!(row(&f, &id).await, before);
}
