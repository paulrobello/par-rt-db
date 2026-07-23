//! Integration tests for file storage (FEATURE_MATRIX #16).
//!
//! These cover only the table lifecycle: every new database gets a
//! `db_<name>.storage` table from `db::create_database`, the global
//! `rtdb.storage_index` exists after `db::bootstrap`, and `storage::ensure_table`
//! is idempotent and revives a dropped table for databases that predate the
//! feature. Accessors and HTTP routes land in later tasks.

mod common;

use common::{fresh_db, test_state};

#[tokio::test]
async fn storage_table_created_with_database() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // create_database ran inside fresh_db; the per-db storage table must exist.
    let schema = format!("db_{db}");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage'
        )",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "storage table should exist for a fresh database");

    // And the global index exists after bootstrap (test_state bootstraps).
    let idx: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'rtdb' AND table_name = 'storage_index'
        )",
    )
    .fetch_one(&state.pool)
    .await?;
    assert!(idx, "rtdb.storage_index should exist after bootstrap");
    Ok(())
}

#[tokio::test]
async fn ensure_table_is_idempotent_and_revives_dropped_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema = format!("db_{db}");

    // Simulate a database that predates the feature: drop storage, then ensure.
    sqlx::query(&format!("DROP TABLE \"{schema}\".storage"))
        .execute(&state.pool)
        .await?;
    rtdb_server::storage::ensure_table(&state.pool, &db).await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage')",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "ensure_table should recreate the storage table");
    Ok(())
}
