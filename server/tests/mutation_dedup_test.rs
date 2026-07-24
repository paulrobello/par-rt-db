mod common;

use common::{fresh_db, test_state};
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
        mutation_log::DEDUP_TTL_MS,
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
        .committers
        .mutate(&db, Some("retry-key-1".to_string()), txn.clone(), None)
        .await?;
    let second = state
        .committers
        .mutate(&db, Some("retry-key-1".to_string()), txn.clone(), None)
        .await?;

    assert_eq!(first.results, second.results);

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    ))
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
        .committers
        .mutate(&db, None, txn.clone(), None)
        .await?;
    state
        .committers
        .mutate(&db, None, txn.clone(), None)
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    ))
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
        .committers
        .mutate(&db, Some(String::new()), txn.clone(), None)
        .await?;
    state
        .committers
        .mutate(&db, Some(String::new()), txn.clone(), None)
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    ))
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
        .committers
        .mutate(&db, Some("retry-key-2".to_string()), txn.clone(), None)
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) = sqlx::query_as(&format!(
        "SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""
    ))
    .fetch_one(&state.pool)
    .await?;
    assert_eq!(count.0, 1);

    Ok(())
}
