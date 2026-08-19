//! Verifies the `TestDb` RAII teardown mechanism end-to-end.
//!
//! The novel part this de-risks: `Drop::drop` is sync but `db::drop_database`
//! is async, and tests run on current-thread `#[tokio::test]` runtimes that
//! shut down when the test returns. The cleanup worker solves this by running
//! on its OWN OS thread with its OWN `tokio::runtime::Runtime` + OWN `PgPool`,
//! independent of the test runtime — so cleanup proceeds after the test
//! returns. This test PROVES that by dropping a `TestDb` and polling
//! `db::database_exists` until it returns false within ~5s.

mod common;

use std::time::Duration;

use common::{TestDb, ensure_cleanup_worker, kanban_schema_json, test_state};
use rtdb_server::db;

/// Regression: `CREATE EXTENSION IF NOT EXISTS` is check-then-insert — two
/// concurrent `create_database` calls can both see the extension absent and
/// the loser dies on `pg_extension_name_index` (observed as a recurring CI
/// failure in `admin_test`'s fresh-db setup). `EXTENSION_LOCK_KEY`
/// serializes them; this test hammers the concurrent path so a fresh
/// Postgres (where the extensions start absent) exercises the window.
#[tokio::test]
async fn concurrent_database_creation_succeeds() {
    let state = test_state().await;
    ensure_cleanup_worker(&state.config.database_url);

    let mut handles = Vec::new();
    for _ in 0..16 {
        let pool = state.pool.clone();
        handles.push(tokio::spawn(async move {
            let name = format!("t{}", uuid::Uuid::now_v7().simple());
            db::create_database(&pool, &name).await?;
            Ok::<_, rtdb_server::error::RtDbError>(TestDb(name))
        }));
    }
    for handle in handles {
        let _db = handle.await.expect("join").expect("create db");
    }
}

/// Same race, `push_schema` side: its extension backfill shares
/// `EXTENSION_LOCK_KEY`, so concurrent first pushes (each to its own fresh
/// database) must all succeed.
#[tokio::test]
async fn concurrent_schema_push_into_fresh_databases_succeeds() {
    let state = test_state().await;
    ensure_cleanup_worker(&state.config.database_url);

    let mut names = Vec::new();
    for _ in 0..8 {
        let name = format!("t{}", uuid::Uuid::now_v7().simple());
        db::create_database(&state.pool, &name)
            .await
            .expect("create db");
        names.push(name);
    }

    let mut handles = Vec::new();
    for name in names {
        let pool = state.pool.clone();
        handles.push(tokio::spawn(async move {
            let schema: rtdb_server::schema::SchemaDef =
                serde_json::from_value(kanban_schema_json()).expect("parse kanban schema fixture");
            rtdb_server::ddl::push_schema(&pool, &name, schema).await?;
            Ok::<_, rtdb_server::error::RtDbError>(TestDb(name))
        }));
    }
    for handle in handles {
        let _db = handle.await.expect("join").expect("push schema");
    }
}

/// Dropping a `TestDb` must clean up its database via the dedicated worker
/// thread (own runtime + pool), even though the test runs on a current-thread
/// runtime that would cancel a plain `tokio::spawn`.
#[tokio::test]
async fn testdb_drop_cleans_up_via_worker() {
    let state = test_state().await;
    ensure_cleanup_worker(&state.config.database_url);

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    assert!(
        db::database_exists(&state.pool, &name)
            .await
            .expect("exists check")
    );

    // Drop enqueues the name on the worker (separate thread/runtime).
    drop(TestDb(name.clone()));

    // Poll until the worker has dropped it (it's async + on another thread).
    let mut gone = false;
    for _ in 0..100 {
        if !db::database_exists(&state.pool, &name)
            .await
            .expect("exists check")
        {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        gone,
        "TestDb::drop did not clean up the database via the worker within ~5s"
    );

    // Keep `state` alive for the duration of the polling above.
    drop(state);
}
