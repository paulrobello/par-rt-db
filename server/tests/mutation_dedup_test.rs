mod common;

use common::{fresh_db, test_state};
use rtdb_server::mutation_log;

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
