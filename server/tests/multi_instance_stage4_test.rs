//! ENH-022 Stage 4 two-process integration tests: cross-process rate limits
//! and the committer ownership lease (docs/superpowers/specs/
//! 2026-08-22-multi-instance-stage4-design.md, option A1 + B1), plus Stage 4c
//! forwarding of non-owner writes to the lease owner. "Two processes" are two
//! full `AppState`s with distinct instance ids sharing one Postgres — the
//! same shape `notify_test.rs` uses for Stage 2/3.

mod common;

use common::{test_config, test_hot};
use rtdb_server::AppState;
use rtdb_server::auth::{Principal, PrincipalCtx};
use rtdb_server::error::ErrorCode;
use rtdb_server::txn::{Step, Transaction};
use sqlx::PgPool;

/// A multi-instance `AppState` with distinct instance id and optional rate
/// limits, sharing `pool` — one stand-in per replica process. The forward
/// timeout is short (300ms) so the timeout→takeover failover path is
/// exercisable within a test without waiting the 5s production default.
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
    cfg.forward_timeout_ms = 300;
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

/// `mutate` with a bounded retry on CONFLICT. A forwarded write can surface
/// CONFLICT transiently — the peer's forward listener may still be
/// connecting, or a just-dropped owner's lease may not be released yet — and
/// the production contract is that the client retries into convergence.
/// Every failed attempt executed nothing (the shadow CONFLICT backstop), so
/// retrying cannot double-write.
async fn mutate_until_landed(
    state: &std::sync::Arc<AppState>,
    db: &str,
    txn: Transaction,
    principal: PrincipalCtx,
) -> anyhow::Result<rtdb_server::txn::TxnOutcome> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let result = state
            .realtime
            .committers
            .mutate(db, None, txn.clone(), principal.clone())
            .await;
        match result {
            Ok(outcome) => return Ok(outcome),
            Err(err) if err.code == ErrorCode::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "write kept conflicting past the deadline: {err}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err.into()),
        }
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

/// The items schema the ownership tests push. `owner_field` is set (and the
/// `owner` field declared — ownerField must name a declared field) so the
/// forwarded-principal test can assert identity stamping on the owner.
fn items_schema(with_owner_field: bool) -> rtdb_server::schema::SchemaDef {
    let (fields, owner_field) = if with_owner_field {
        (
            serde_json::json!({ "title": { "type": "string" }, "owner": { "type": "string" } }),
            serde_json::json!("owner"),
        )
    } else {
        (
            serde_json::json!({ "title": { "type": "string" } }),
            serde_json::Value::Null,
        )
    };
    serde_json::from_value(serde_json::json!({
        "tables": { "items": {
            "fields": fields,
            "indexes": [{ "name": "by_title", "fields": ["title"] }],
            "ownerField": owner_field
        }}
    }))
    .expect("valid schema")
}

/// (T1) Single-writer per database under the ownership lease, writes COMMIT
/// from BOTH replicas (Stage 4c forwarding: the non-owner's write is executed
/// by the owner), and failover when the leaseholder dies: after the owner's
/// lease connection drops — what process death looks like to Postgres — the
/// survivor's next write forwards to nobody, times out, takes the lease, and
/// lands. Writes land exactly once throughout.
#[tokio::test]
async fn ownership_lease_forwarding_and_failover_on_death() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "stage4-own-a", 0, 0).await;
    let b = replica(&pool, "stage4-own-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = common::wrap_test_db(name);

    // A's push takes the lease (A becomes the owner).
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // Stage 4c: B's write is FORWARDED to A and commits — no CONFLICT. The
    // retry wrapper only absorbs the listener-connecting startup window.
    mutate_until_landed(&b, &db, insert_item("from-b"), PrincipalCtx::bypass()).await?;

    // The owner writes fine.
    a.realtime
        .committers
        .mutate(&db, None, insert_item("from-a"), PrincipalCtx::bypass())
        .await?;

    // Owner death: dropping A's channel entry drops the lease pool — its
    // connection closes, and Postgres releases the advisory lock exactly as
    // it does when the process dies.
    a.realtime.committers.drop_db(&db).await;

    // Failover: B's next write forwards (no owner answers — A's entry is
    // gone), times out, takes the lease itself, and lands.
    mutate_until_landed(
        &b,
        &db,
        insert_item("from-b-after-failover"),
        PrincipalCtx::bypass(),
    )
    .await?;

    // Exactly-once: three writes, three rows, no duplicates from retries.
    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 3, "two forwarded/owner writes + one post-failover write");
    Ok(())
}

/// (T3) A write whose forward finds no owner AND whose takeover fails (the
/// lease is held by an unreachable session — a live-but-unresponsive owner)
/// surfaces CONFLICT; once that session releases, the same path takes the
/// lease and the write lands. The "unreachable owner" is a raw advisory lock
/// held on a held-live pooled connection: lease held, no listener behind it.
#[tokio::test]
async fn forward_timeout_conflicts_then_takes_over_when_lease_frees() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let b = replica(&pool, "stage4c-ghost-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = common::wrap_test_db(name);
    b.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;
    // ^ B pushed to a cold db — B is now the OWNER. Simulate an unreachable
    // owner instead by retiring B's entry (releasing its lease) and holding
    // the advisory lock on a raw connection no listener answers for.
    b.realtime.committers.drop_db(&db).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let hex = rtdb_server::db::sha256_hex(&db);
    let key = u64::from_str_radix(&hex[..16], 16).unwrap() as i64;
    let mut ghost = pool.acquire().await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *ghost)
        .await?;
    assert!(locked, "ghost session must take the advisory lock");

    // Forward finds no owner (nobody owns the db), times out (300ms), the
    // takeover hits the ghost's lock, and the write surfaces CONFLICT.
    let err = b
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_item("while-ghosted"),
            PrincipalCtx::bypass(),
        )
        .await
        .expect_err("no owner answers and the lease is held");
    assert_eq!(err.code, ErrorCode::Conflict, "got: {err}");
    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 0, "the conflicted write must not land");

    // The ghost session releases — the next write's takeover acquires.
    let unlocked: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut *ghost)
        .await?;
    assert!(unlocked, "ghost session must release the lock");
    drop(ghost);

    mutate_until_landed(
        &b,
        &db,
        insert_item("after-release"),
        PrincipalCtx::bypass(),
    )
    .await?;
    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    assert_eq!(n, 1, "the post-release write lands exactly once");
    Ok(())
}

/// (T4) The forwarded write carries the ORIGIN's principal: the owner stamps
/// `ownerField` with the user id that authorized the write at the edge, not
/// with a bypass identity. A table with `ownerField: "owner"`, an insert
/// submitted on B with `user_id: Some("user-fwd")`, executed by owner A —
/// the stored row's `owner` must be `user-fwd`.
#[tokio::test]
async fn forwarded_write_preserves_principal_on_owner() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "stage4c-prin-a", 0, 0).await;
    let b = replica(&pool, "stage4c-prin-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = common::wrap_test_db(name);
    a.realtime
        .committers
        .push_schema(&db, items_schema(true))
        .await?;

    let principal = PrincipalCtx {
        user_id: Some("user-fwd".to_string()),
        ..Default::default()
    };
    let outcome = mutate_until_landed(&b, &db, insert_item("owned-doc"), principal).await?;
    assert_eq!(outcome.results.len(), 1, "one step, one result");

    let (owner,): (String,) = sqlx::query_as(&format!(
        "SELECT doc->>'owner' FROM \"db_{db}\".\"t_items\" WHERE doc->>'title' = 'owned-doc'"
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        owner, "user-fwd",
        "ownerField stamped with the origin's principal"
    );
    Ok(())
}
