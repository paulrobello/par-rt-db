use std::sync::Arc;
use std::time::{Duration, Instant};

use rtdb_server::AppState;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db::{bootstrap, create_database, now_ms};
use rtdb_server::ddl::{pg_schema, pg_table, push_schema};
use rtdb_server::error::ErrorCode;
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction};
use sqlx::{PgPool, Postgres};

use crate::common::{
    TestDb, background_guard, test_config, test_hot, test_shared_pool, wrap_test_db,
};

async fn fixture() -> (Arc<AppState>, TestDb) {
    let config = test_config();
    let pool = test_shared_pool(&config.database_url, 4).await.unwrap();
    bootstrap(&pool).await.unwrap();
    let state = AppState::new(pool, config, test_hot());
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    create_database(&state.pool, &name).await.unwrap();
    let db = wrap_test_db(name);
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({"tables": {
        "tasks": {"fields": {"name": {"type": "string"},
            "ownerGeneration": {"type": "optional", "inner": {"type": "number"}}}, "indexes": []}
    }}))
    .unwrap();
    push_schema(&state.pool, &db, schema).await.unwrap();
    // Warm schema/committer setup before the deadline-bearing request.
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction { steps: vec![] },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    (state, db)
}

fn insert(name: &str) -> Step {
    Step::Insert {
        table: "tasks".into(),
        doc: serde_json::json!({"name": name})
            .as_object()
            .unwrap()
            .clone(),
    }
}

async fn count(pool: &PgPool, db: &str) -> i64 {
    let schema = pg_schema(db);
    let table = pg_table("tasks");
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{schema}\".\"{table}\""
    )))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn block_inserts(pool: &PgPool, db: &str, key: i64) -> sqlx::Transaction<'static, Postgres> {
    let schema = pg_schema(db);
    let table = pg_table("tasks");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION \"{schema}\".deadline_gate() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN PERFORM pg_advisory_xact_lock({key}); RETURN NEW; END; $$"
    )))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TRIGGER deadline_gate BEFORE INSERT ON \"{schema}\".\"{table}\" \
         FOR EACH ROW EXECUTE FUNCTION \"{schema}\".deadline_gate()"
    )))
    .execute(pool)
    .await
    .unwrap();
    let mut connection = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(key)
        .execute(&mut *connection)
        .await
        .unwrap();
    connection
}

async fn wait_for_blocked_insert(pool: &PgPool, key: i64) {
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
            "insert never reached the guarded SQL trigger"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn release_after_deadline(connection: sqlx::Transaction<'static, Postgres>, deadline: i64) {
    let remaining = deadline - now_ms() + 1;
    if remaining > 0 {
        tokio::time::sleep(Duration::from_millis(remaining as u64)).await;
    }
    connection.rollback().await.unwrap();
}

#[tokio::test]
async fn expired_deadline_aborts_the_entire_real_committer_transaction() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let error = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![insert("forbidden"), Step::ExpectBefore { deadline_ms: 0 }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 0);
}

#[tokio::test]
async fn expiry_during_async_sql_work_rolls_back_at_the_final_commit_decision() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let key = (uuid::Uuid::now_v7().as_u128() % i64::MAX as u128) as i64;
    let lock = block_inserts(&state.pool, &db, key).await;
    let deadline = now_ms() + 2000;
    let pending_state = state.clone();
    let pending_db = db.to_string();
    let pending = tokio::spawn(async move {
        pending_state
            .realtime
            .committers
            .mutate(
                &pending_db,
                Some("deadline-late-write".into()),
                Transaction {
                    steps: vec![
                        Step::ExpectBefore {
                            deadline_ms: deadline,
                        },
                        insert("late"),
                    ],
                },
                PrincipalCtx::bypass(),
            )
            .await
    });
    wait_for_blocked_insert(&state.pool, key).await;
    assert!(
        now_ms() < deadline,
        "request expired before reaching SQL; final-check seam was not exercised"
    );
    release_after_deadline(lock, deadline).await;
    let error = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 0);
    let retried = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("deadline-late-write".into()),
            Transaction {
                steps: vec![
                    Step::ExpectBefore {
                        deadline_ms: deadline,
                    },
                    insert("retry"),
                ],
            },
            PrincipalCtx::bypass(),
        )
        .await;
    assert_eq!(retried.unwrap_err().code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 0);
}

#[tokio::test]
async fn time_waiting_behind_another_committer_request_counts_against_the_deadline() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let key = (uuid::Uuid::now_v7().as_u128() % i64::MAX as u128) as i64;
    let lock = block_inserts(&state.pool, &db, key).await;
    let first_state = state.clone();
    let first_db = db.to_string();
    let first = tokio::spawn(async move {
        first_state
            .realtime
            .committers
            .mutate(
                &first_db,
                None,
                Transaction {
                    steps: vec![insert("blocker")],
                },
                PrincipalCtx::bypass(),
            )
            .await
    });
    wait_for_blocked_insert(&state.pool, key).await;
    let deadline = now_ms() + 500;
    let second_state = state.clone();
    let second_db = db.to_string();
    let second = tokio::spawn(async move {
        second_state
            .realtime
            .committers
            .mutate(
                &second_db,
                None,
                Transaction {
                    steps: vec![
                        Step::ExpectBefore {
                            deadline_ms: deadline,
                        },
                        insert("expired-in-queue"),
                    ],
                },
                PrincipalCtx::bypass(),
            )
            .await
    });
    release_after_deadline(lock, deadline).await;
    tokio::time::timeout(Duration::from_secs(10), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 1);
}

#[tokio::test]
async fn committed_idempotent_replay_after_expiry_is_history_and_never_writes_again() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let deadline = now_ms() + 500;
    let txn = Transaction {
        steps: vec![
            Step::ExpectBefore {
                deadline_ms: deadline,
            },
            insert("once"),
        ],
    };
    let first = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("same-request".into()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    let remaining = deadline - now_ms() + 1;
    if remaining > 0 {
        tokio::time::sleep(Duration::from_millis(remaining as u64)).await;
    }
    let replay = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("same-request".into()),
            txn,
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    assert_eq!(first.results, replay.results);
    assert!(replay.write_set.tables.is_empty());
    assert_eq!(count(&state.pool, &db).await, 1);
}

#[tokio::test]
async fn a_guarded_read_only_transaction_still_checks_the_final_deadline() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let seeded = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![insert("unchanged")],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    let id = seeded.results[0]["id"].as_str().unwrap().to_owned();
    let schema = pg_schema(&db);
    let table = pg_table("tasks");
    let relation: i64 = sqlx::query_scalar("SELECT to_regclass($1)::oid::bigint")
        .bind(format!("\"{schema}\".\"{table}\""))
        .fetch_one(&state.pool)
        .await
        .unwrap();
    let mut lock = state.pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE \"{schema}\".\"{table}\" IN ACCESS EXCLUSIVE MODE"
    )))
    .execute(&mut *lock)
    .await
    .unwrap();
    let deadline = now_ms() + 2000;
    let pending_state = state.clone();
    let pending_db = db.to_string();
    let pending = tokio::spawn(async move {
        pending_state
            .realtime
            .committers
            .mutate(
                &pending_db,
                None,
                Transaction {
                    steps: vec![
                        Step::ExpectBefore {
                            deadline_ms: deadline,
                        },
                        Step::ExpectVersion {
                            table: "tasks".into(),
                            id,
                            version: 1,
                        },
                    ],
                },
                PrincipalCtx::bypass(),
            )
            .await
    });
    let wait_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_locks \
            WHERE locktype = 'relation' AND relation::bigint = $1 AND mode = 'AccessShareLock' AND NOT granted)")
            .bind(relation).fetch_one(&state.pool).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            Instant::now() < wait_deadline,
            "guarded read never reached the locked table"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        now_ms() < deadline,
        "precheck was not exercised before expiry"
    );
    release_after_deadline(lock, deadline).await;
    let error = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 1);
}

#[tokio::test]
async fn a_future_deadline_never_bypasses_a_replaced_owner_version() {
    let (state, db) = fixture().await;
    let _background = background_guard(&state);
    let seeded = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "tasks".into(),
                    doc: serde_json::json!({"name": "owned", "ownerGeneration": 1})
                        .as_object()
                        .unwrap()
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    let id = seeded.results[0]["id"].as_str().unwrap().to_owned();
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "tasks".into(),
                    id: id.clone(),
                    fields: serde_json::json!({"ownerGeneration": 2})
                        .as_object()
                        .unwrap()
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap();
    let error = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![
                    Step::ExpectBefore {
                        deadline_ms: 9_007_199_254_740_991,
                    },
                    Step::ExpectVersion {
                        table: "tasks".into(),
                        id,
                        version: 1,
                    },
                    insert("stale-writer"),
                ],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::PreconditionFailed);
    assert_eq!(count(&state.pool, &db).await, 1);
}
