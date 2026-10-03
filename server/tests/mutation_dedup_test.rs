use crate::common::{fresh_db, test_state};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::mutation_log;
use rtdb_server::txn::{Step, Transaction};

fn valid_project_doc() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "name": "Alpha",
        "description": null,
        "status": "active",
        "tags": ["a", "b"],
        "updatedAt": 1.0
    })
    .as_object()
    .expect("json object")
    .clone()
}

#[tokio::test]
async fn check_returns_none_when_absent() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let result = mutation_log::check(&state.pool, &db, "mut-1").await?;
    assert!(result.is_none());

    Ok(())
}

#[tokio::test]
async fn store_then_check_returns_cached_results() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let results = vec![serde_json::json!({"id": "abc123"})];
    mutation_log::store(
        &state.pool,
        &db,
        "mut-2",
        &results,
        mutation_log::DEFAULT_DEDUP_TTL_MS,
    )
    .await?;

    let cached = mutation_log::check(&state.pool, &db, "mut-2").await?;
    assert_eq!(cached, Some(results));

    Ok(())
}

#[tokio::test]
async fn expired_entry_returns_none() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let results = vec![serde_json::json!({"id": "xyz789"})];
    mutation_log::store(&state.pool, &db, "mut-3", &results, 1).await?;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let cached = mutation_log::check(&state.pool, &db, "mut-3").await?;
    assert!(cached.is_none());

    Ok(())
}

#[tokio::test]
async fn same_mut_id_dedups_and_replays_cached_result() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    let first = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("retry-key-1".to_string()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;
    let second = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("retry-key-1".to_string()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;

    assert_eq!(first.results, second.results);

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 1);

    Ok(())
}

#[tokio::test]
async fn no_mut_id_does_not_dedup() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    state
        .realtime
        .committers
        .mutate(&db, None, txn.clone(), PrincipalCtx::bypass())
        .await?;
    state
        .realtime
        .committers
        .mutate(&db, None, txn.clone(), PrincipalCtx::bypass())
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 2);

    Ok(())
}

#[tokio::test]
async fn empty_string_idempotency_key_is_treated_as_absent() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    state
        .realtime
        .committers
        .mutate(
            &db,
            Some(String::new()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;
    state
        .realtime
        .committers
        .mutate(
            &db,
            Some(String::new()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 2);

    Ok(())
}

#[tokio::test]
async fn expired_mut_id_re_executes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    mutation_log::store(&state.pool, &db, "retry-key-2", &[], 0).await?;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    state
        .realtime
        .committers
        .mutate(
            &db,
            Some("retry-key-2".to_string()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 1);

    Ok(())
}

// ARC-003: the dedup row commits in the SAME transaction as the write, so
// the `mutations` row is visible the instant the write is — no post-commit
// cache-fill window a crash or takeover resubmit could fall into. A retry
// under the same key replays the cached results and applies nothing.
#[tokio::test]
async fn dedup_row_commits_atomically_with_the_write() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    let first = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("atomic-key".to_string()),
            txn.clone(),
            PrincipalCtx::bypass(),
        )
        .await?;

    // No delay, no cleanup tick: the row must already be committed alongside
    // the document write.
    let pg_schema = format!("db_{db}");
    let row: (serde_json::Value, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT result, expires_at FROM \"{pg_schema}\".mutations WHERE mut_id = 'atomic-key'"
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(row.0, serde_json::to_value(&first.results)?);
    assert!(row.1 > rtdb_server::db::now_ms());

    // Retry with no second write; version unchanged.
    let second = state
        .realtime
        .committers
        .mutate(
            &db,
            Some("atomic-key".to_string()),
            txn,
            PrincipalCtx::bypass(),
        )
        .await?;
    assert_eq!(first.results, second.results);
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 1);

    Ok(())
}

// ARC-003: a pre-existing dedup row makes a concurrent execution replay
// instead of double-applying. `execute_txn` is called directly with the
// `idem` tuple (bypassing the arm's `check` fast path) to prove the in-
// transaction conflict detection rolls the second execution back.
#[tokio::test]
async fn pre_existing_key_row_replays_instead_of_double_applying() -> anyhow::Result<()> {
    use rtdb_server::schema::SchemaDef;

    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(crate::common::kanban_schema_json()).expect("parse kanban schema");

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    // Seed a still-live sentinel row for the key — as if another execution
    // already committed under it.
    let sentinel = vec![serde_json::json!({"replayed": true})];
    mutation_log::store(
        &state.pool,
        &db,
        "race-key",
        &sentinel,
        mutation_log::DEFAULT_DEDUP_TTL_MS,
    )
    .await?;

    let err = rtdb_server::txn::execute_txn(
        &state.pool,
        &db,
        &schema,
        &txn,
        &PrincipalCtx::bypass(),
        Some(("race-key", mutation_log::DEFAULT_DEDUP_TTL_MS)),
    )
    .await
    .expect_err("must be rejected as a replay");
    assert!(
        rtdb_server::txn::is_idempotency_replay(&err),
        "expected the idempotency-replay marker, got {err:?}"
    );

    // The second execution's write rolled back: still zero documents, and the
    // sentinel row is untouched.
    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    )))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 0);
    let cached = mutation_log::check(&state.pool, &db, "race-key").await?;
    assert_eq!(cached, Some(sentinel));

    Ok(())
}
