//! ENH-022 Stage 4 two-process integration tests: cross-process rate limits
//! and the committer ownership lease (docs/superpowers/specs/
//! 2026-08-22-multi-instance-stage4-design.md, option A1 + B1). "Two
//! processes" are two full `AppState`s with distinct instance ids sharing
//! one Postgres — the same shape `notify_test.rs` uses for Stage 2/3.

mod common;

use common::{test_config, test_hot};
use rtdb_server::AppState;
use rtdb_server::auth::{Principal, PrincipalCtx};
use rtdb_server::error::ErrorCode;
use rtdb_server::txn::{Step, Transaction};
use sqlx::PgPool;

/// A multi-instance `AppState` with distinct instance id and optional rate
/// limits, sharing `pool` — one stand-in per replica process.
async fn replica(
    pool: &PgPool,
    instance_id: &str,
    per_token_rpm: u32,
    per_db_rpm: u32,
) -> std::sync::Arc<AppState> {
    let mut cfg = test_config();
    cfg.multi_instance = true;
    cfg.instance_id = Some(instance_id.to_string());
    cfg.rate_limit_per_token_rpm = per_token_rpm;
    cfg.rate_limit_per_db_rpm = per_db_rpm;
    AppState::new(pool.clone(), cfg, test_hot())
}

async fn shared_pool() -> PgPool {
    let state = common::test_state().await;
    state.pool.clone()
}

fn insert_item(title: &str) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "title": title })
                .as_object()
                .unwrap()
                .clone(),
        }],
    }
}

/// (T2) A per-db rate budget configured on two replicas is ONE budget: the
/// counters live in `rtdb_auth.rate_counters`, so the Nth+1 request is denied
/// regardless of which replica handled the first N.
#[tokio::test]
async fn rate_budget_is_shared_across_replicas() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "stage4-rate-a", 0, 4).await;
    let b = replica(&pool, "stage4-rate-b", 0, 4).await;

    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let db = common::wrap_test_db(db);

    let principal = Principal::Machine {
        db: db.as_str().to_string(),
        token_id: "stage4-rate-token".to_string(),
        read_only: false,
        tables: None,
    };
    // Two through A, two through B — all allowed (budget 4 shared).
    for state in [&a, &a, &b, &b] {
        rtdb_server::rate_limit::check_http_rate_limits(state, &principal, &db).await?;
    }
    // The fifth — on either replica — is denied with an in-range hint.
    let err = rtdb_server::rate_limit::check_http_rate_limits(&a, &principal, &db)
        .await
        .expect_err("shared budget exhausted");
    assert_eq!(err.code, ErrorCode::RateLimited);
    let retry = err.retry_after_secs.expect("retry hint present");
    assert!((1..=60).contains(&retry), "retry hint {retry} in [1,60]");
    let err = rtdb_server::rate_limit::check_http_rate_limits(&b, &principal, &db)
        .await
        .expect_err("shared budget exhausted on the peer too");
    assert_eq!(err.code, ErrorCode::RateLimited);
    Ok(())
}

/// (T1) Single-writer per database under the ownership lease, and failover
/// when the leaseholder dies: killing the owner's lease (its dedicated
/// connection — what process death looks like to Postgres) hands the next
/// replica the lease on its next write, and writes continue exactly-once.
#[tokio::test]
async fn ownership_lease_single_writer_and_failover_on_death() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "stage4-own-a", 0, 0).await;
    let b = replica(&pool, "stage4-own-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = common::wrap_test_db(name);

    let schema: rtdb_server::schema::SchemaDef = serde_json::from_value(serde_json::json!({
        "tables": { "items": {
            "fields": { "title": { "type": "string" } },
            "indexes": [{ "name": "by_title", "fields": ["title"] }]
        }}
    }))?;
    // A's push takes the lease (A becomes the owner).
    a.realtime
        .committers
        .push_schema(&db, schema.clone())
        .await?;

    // Single-writer: B's write is rejected with CONFLICT while A owns.
    let err = b
        .realtime
        .committers
        .mutate(&db, None, insert_item("from-b"), PrincipalCtx::bypass())
        .await
        .expect_err("non-owner write must be rejected");
    assert_eq!(err.code, ErrorCode::Conflict, "got: {err}");

    // The owner writes fine.
    a.realtime
        .committers
        .mutate(&db, None, insert_item("from-a"), PrincipalCtx::bypass())
        .await?;

    // Owner death: dropping A's channel entry drops the lease pool — its
    // connection closes, and Postgres releases the advisory lock exactly as
    // it does when the process dies.
    a.realtime.committers.drop_db(&db).await;

    // Failover: B's next write attempt acquires the lease and lands.
    b.realtime
        .committers
        .mutate(
            &db,
            None,
            insert_item("from-b-after-failover"),
            PrincipalCtx::bypass(),
        )
        .await?;

    // Exactly-once: the rejected write never landed; both committed ones did.
    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 2, "one rejected write absent, two owner writes present");
    Ok(())
}
