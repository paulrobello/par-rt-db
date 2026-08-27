//! ENH-032 test-harness regression: the connection ceiling of every
//! `AppState` test pool.
//!
//! The consolidated `main` binary runs 50+ test modules in ONE process, and
//! each test leaves its `AppState` (and its `PgPool`) alive in a detached
//! server task for the rest of the binary's life. With SQLx's default
//! `max_connections = 10` per pool, a full run accumulates enough detached
//! pools to exhaust the Postgres connection ceiling, surfacing as late-run
//! `PoolTimedOut` flakes. The fix — pinned here — is that every test pool is
//! built through `common::test_pool`/`common::test_shared_pool`, which bound
//! the ceiling explicitly instead of trusting the default. The bound must be
//! asserted, not assumed: a helper that silently reverts to
//! `PgPool::connect` (default ceiling 10) is exactly the regression this
//! file exists to catch.

use crate::common::test_state;

#[tokio::test]
async fn test_state_pool_ceiling_is_one_connection() {
    let state = test_state().await;
    assert_eq!(
        state.pool.options().get_max_connections(),
        1,
        "test_state's pool must be constructed with max_connections = 1"
    );
}

#[tokio::test]
async fn concurrent_queries_on_test_state_pool_share_one_backend() {
    // Behavioral proof of the ceiling: 8 concurrent queries on one
    // test_state pool can only ever land on a single Postgres backend.
    let state = test_state().await;
    let pids: Vec<i32> =
        futures_util::future::join_all((0..8).map(|_| {
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()").fetch_one(&state.pool)
        }))
        .await
        .into_iter()
        .map(|pid| pid.expect("pg_backend_pid query"))
        .collect();
    let distinct: std::collections::HashSet<_> = pids.iter().collect();
    assert_eq!(
        distinct.len(),
        1,
        "8 concurrent queries must share the single pooled backend, saw {distinct:?}"
    );
}
