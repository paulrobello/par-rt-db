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

use common::{TestDb, ensure_cleanup_worker, test_state};
use rtdb_server::db;

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
