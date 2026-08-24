//! ENH-022 Stage 4 two-process integration tests: cross-process rate limits
//! and the committer ownership lease (docs/superpowers/specs/
//! 2026-08-22-multi-instance-stage4-design.md, option A1 + B1), plus Stage 4c
//! forwarding of non-owner writes to the lease owner. "Two processes" are two
//! full `AppState`s with distinct instance ids sharing one Postgres — the
//! same shape `notify_test.rs` uses for Stage 2/3.

use crate::common::{test_config, test_hot};
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
    // ARC-007: this helper's one rate-limiting test
    // (`rate_budget_is_shared_across_replicas`) asserts the budget is shared
    // *synchronously* across replicas with no flush delay — that's the exact
    // path's guarantee, not the (now-default) approximate path's. The other
    // callers all pass 0/0 (rate limiting disabled), so this is a no-op for
    // them.
    cfg.rate_limit_exact = true;
    cfg.forward_timeout_ms = 300;
    AppState::new(pool.clone(), cfg, test_hot())
}

async fn shared_pool() -> PgPool {
    let state = crate::common::test_state().await;
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
    let db = crate::common::wrap_test_db(db);

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
    let db = crate::common::wrap_test_db(name);

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
    let db = crate::common::wrap_test_db(name);
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
    let db = crate::common::wrap_test_db(name);
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

/// (ARC-002) A forwarded write whose serialized payload exceeds Postgres's
/// 8000-byte `pg_notify` cap still round-trips: the body travels through the
/// `rtdb_auth.forward_queue` spool and the NOTIFY carries only the row id.
/// Before the spool, Postgres rejected the `pg_notify` outright and the write
/// fell into the takeover path instead of reaching the owner.
#[tokio::test]
async fn forwarded_mutate_larger_than_notify_cap_round_trips() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "arc002-big-a", 0, 0).await;
    let b = replica(&pool, "arc002-big-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // 20 KB of title — two and a half times the NOTIFY cap on its own.
    let big_title = "x".repeat(20 * 1024);
    let txn = insert_item(&big_title);
    assert!(
        serde_json::to_string(&txn)?.len() > 8000,
        "the fixture must exceed the pg_notify cap to be a regression test"
    );

    mutate_until_landed(&b, &db, txn, PrincipalCtx::bypass()).await?;

    let (stored,): (String,) = sqlx::query_as(&format!(
        "SELECT doc->>'title' FROM \"db_{db}\".\"t_items\""
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(stored.len(), big_title.len(), "the whole body forwarded");
    Ok(())
}

/// (ARC-002) The REPLY direction is capped too: a forwarded `RunPushSchema`
/// answers with the whole `SchemaDef`, which for an 80-table schema is well
/// past 8000 bytes. Both legs go through the spool.
#[tokio::test]
async fn forwarded_push_schema_reply_larger_than_notify_cap() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "arc002-schema-a", 0, 0).await;
    let b = replica(&pool, "arc002-schema-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    // A takes the lease with the baseline schema; B's push is forwarded.
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // Additive-only: keep `items` and add 80 more tables.
    let mut tables = serde_json::Map::new();
    tables.insert(
        "items".to_string(),
        serde_json::json!({
            "fields": { "title": { "type": "string" } },
            "indexes": [{ "name": "by_title", "fields": ["title"] }]
        }),
    );
    for i in 0..80 {
        tables.insert(
            format!("wide_table_number_{i}"),
            serde_json::json!({
                "fields": {
                    "alpha": { "type": "string" },
                    "bravo": { "type": "number" },
                    "charlie": { "type": "boolean" },
                    "delta": { "type": "string" }
                },
                "indexes": [
                    { "name": "by_alpha", "fields": ["alpha"] },
                    { "name": "by_bravo_delta", "fields": ["bravo", "delta"] }
                ]
            }),
        );
    }
    let big_schema: rtdb_server::schema::SchemaDef =
        serde_json::from_value(serde_json::json!({ "tables": tables }))?;
    assert!(
        serde_json::to_string(&big_schema)?.len() > 8000,
        "the fixture must exceed the pg_notify cap to be a regression test"
    );

    // Retry on CONFLICT the same way `mutate_until_landed` does — the peer's
    // forward listener may still be connecting.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let pushed = loop {
        match b
            .realtime
            .committers
            .push_schema(&db, big_schema.clone())
            .await
        {
            Ok(schema) => break schema,
            Err(err) if err.code == ErrorCode::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "forwarded push kept conflicting: {err}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err.into()),
        }
    };
    assert_eq!(
        pushed.tables.len(),
        81,
        "the owner's full schema came back through the reply spool"
    );
    Ok(())
}

/// Drain `rx` until a `QueryUpdate` for `query_id` arrives or `within` elapses.
async fn await_query_update(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<rtdb_server::protocol::ServerMessage>,
    query_id: &str,
    within: std::time::Duration,
) -> Option<serde_json::Value> {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        match rx.try_recv() {
            Ok(rtdb_server::protocol::ServerMessage::QueryUpdate {
                query_id: id,
                result,
            }) if id == query_id => {
                return Some(result);
            }
            Ok(_) => continue,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
        }
    }
    None
}

/// (ARC-001) A write executed by the OWNER invalidates subscriptions on every
/// replica, not just its own. Before this, a client subscribed through replica
/// B saw nothing when replica A — the lease owner — committed a write: the
/// op-feed NOTIFY only fed the admin activity ring, and the origin-side
/// fan-out only covered writes B itself had forwarded. B's client stayed stale
/// until B happened to write.
#[tokio::test]
async fn owner_side_write_invalidates_peer_subscriptions() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "arc001-owner-a", 0, 0).await;
    let b = replica(&pool, "arc001-peer-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    // A's push takes the lease — A is the owner for the rest of the test.
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // The subscription lives on B, which owns nothing.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let query: rtdb_server::query::Query =
        serde_json::from_value(serde_json::json!({ "table": "items" }))?;
    b.realtime
        .committers
        .subscribe(
            &db,
            rtdb_server::subs::next_conn_id(),
            "q-peer".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    let initial = await_query_update(&mut rx, "q-peer", std::time::Duration::from_secs(5))
        .await
        .expect("initial query update");
    assert_eq!(
        initial.as_array().expect("docs array").len(),
        0,
        "the table starts empty"
    );

    // The write goes straight to the OWNER — nothing is forwarded, so the
    // only path that can reach B's subscriber is the write-set NOTIFY.
    a.realtime
        .committers
        .mutate(&db, None, insert_item("owner-side"), PrincipalCtx::bypass())
        .await?;

    let pushed = await_query_update(&mut rx, "q-peer", std::time::Duration::from_secs(10))
        .await
        .expect("the peer's subscription re-ran for the owner's write");
    assert_eq!(
        pushed.as_array().expect("docs array").len(),
        1,
        "the peer sees the owner's insert"
    );
    Ok(())
}

/// (ARC-001) The invalidation survives a write set too large to travel inline
/// in a `pg_notify` payload: a bulk insert's `WriteSet` goes through the spool
/// (`kind='writeset'`) and the NOTIFY carries only the row id.
#[tokio::test]
async fn oversized_write_set_invalidates_peer_subscriptions() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "arc001-bulk-a", 0, 0).await;
    let b = replica(&pool, "arc001-bulk-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let query: rtdb_server::query::Query =
        serde_json::from_value(serde_json::json!({ "table": "items" }))?;
    b.realtime
        .committers
        .subscribe(
            &db,
            rtdb_server::subs::next_conn_id(),
            "q-bulk".to_string(),
            query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await?;
    await_query_update(&mut rx, "q-bulk", std::time::Duration::from_secs(5))
        .await
        .expect("initial query update");

    // 200 inserts: the resulting WriteSet carries 200 doc ids plus 200 ops,
    // comfortably past the 7500-byte inline threshold.
    let bulk = Transaction {
        steps: (0..200)
            .map(|i| Step::Insert {
                table: "items".to_string(),
                doc: serde_json::json!({ "title": format!("bulk-{i}") })
                    .as_object()
                    .expect("json object")
                    .clone(),
            })
            .collect(),
    };
    a.realtime
        .committers
        .mutate(&db, None, bulk, PrincipalCtx::bypass())
        .await?;

    let pushed = await_query_update(&mut rx, "q-bulk", std::time::Duration::from_secs(10))
        .await
        .expect("the peer's subscription re-ran for the spooled write set");
    assert_eq!(
        pushed.as_array().expect("docs array").len(),
        200,
        "the peer sees every bulk-inserted doc"
    );
    Ok(())
}

/// (ARC-003) An unkeyed mutate that is FORWARDED gets a server-minted
/// idempotency key, so the owner records it in the shared `mutations` dedup
/// table. That row is what makes the timeout→takeover resubmission a replay
/// rather than a second write: without it, a reply racing the forward timeout
/// left the origin resubmitting a write that had already committed.
#[tokio::test]
async fn forwarded_mutate_is_deduped_by_a_server_minted_key() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let a = replica(&pool, "arc003-key-a", 0, 0).await;
    let b = replica(&pool, "arc003-key-b", 0, 0).await;

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // A local (owner-side) unkeyed mutate is NOT keyed — it has no forward to
    // be ambiguous about, so it must not pay for a dedup row.
    a.realtime
        .committers
        .mutate(
            &db,
            None,
            insert_item("owner-local"),
            PrincipalCtx::bypass(),
        )
        .await?;
    let (local_rows,): (i64,) =
        sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".mutations"))
            .fetch_one(&pool)
            .await?;
    assert_eq!(local_rows, 0, "an owner-side write mints no key");

    // The forwarded one is keyed by the server.
    mutate_until_landed(&b, &db, insert_item("forwarded"), PrincipalCtx::bypass()).await?;
    let (keyed_rows,): (i64,) =
        sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".mutations"))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        keyed_rows, 1,
        "the forwarded mutate recorded a dedup row under the minted key"
    );

    // Replaying that exact key returns the recorded outcome instead of
    // writing again — the property the takeover path relies on.
    let (mut_id,): (String,) = sqlx::query_as(&format!("SELECT mut_id FROM \"db_{db}\".mutations"))
        .fetch_one(&pool)
        .await?;
    a.realtime
        .committers
        .mutate(
            &db,
            Some(mut_id),
            insert_item("forwarded"),
            PrincipalCtx::bypass(),
        )
        .await?;
    let (rows,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 2, "the replay wrote nothing new");
    Ok(())
}

/// (ARC-008) `run_forward_listener` bounds concurrent forwarded-write
/// executions with `RTDB_FORWARD_CONCURRENCY`. With the owner's cap set to 1,
/// firing several forwarded writes at once must not spawn an unbounded pile
/// of committer submits: everything past the single in-flight slot gets an
/// immediate RATE_LIMITED reply — bounded and retryable — instead of hanging
/// until the forward timeout drives the origin into a lease takeover.
#[tokio::test]
async fn forward_concurrency_cap_rate_limits_excess_requests() -> anyhow::Result<()> {
    let pool = shared_pool().await;
    let mut cfg_a = test_config();
    cfg_a.multi_instance = true;
    cfg_a.instance_id = Some("arc008-cap-a".to_string());
    cfg_a.forward_timeout_ms = 2000;
    cfg_a.forward_concurrency = 1;
    let a = AppState::new(pool.clone(), cfg_a, test_hot());

    let mut cfg_b = test_config();
    cfg_b.multi_instance = true;
    cfg_b.instance_id = Some("arc008-cap-b".to_string());
    cfg_b.forward_timeout_ms = 2000;
    let b = AppState::new(pool.clone(), cfg_b, test_hot());

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &name).await?;
    let db = crate::common::wrap_test_db(name);
    // A's push takes the lease — A is the owner for the rest of the test.
    a.realtime
        .committers
        .push_schema(&db, items_schema(false))
        .await?;

    // Let both replicas' forward listeners finish LISTENing before the burst
    // so the saturation this test targets isn't masked by the unrelated
    // listener-connecting startup window `mutate_until_landed` retries past
    // elsewhere in this file.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Fire more forwarded writes at once than A's single permit — B is a
    // non-owner throughout, so every one of these forwards.
    let n = 8;
    let mut handles = Vec::with_capacity(n);
    for i in 0..n {
        let b = b.clone();
        // NOT `db.clone()`: `TestDb::Drop` schedules a real `DROP SCHEMA` for
        // every clone, so 8 of them would race the writes with real cleanup.
        // A plain owned `String` carries the name with no cleanup attached —
        // the original `db` still owns the one teardown, at end of test.
        let db_name = db.as_str().to_string();
        handles.push(tokio::spawn(async move {
            b.realtime
                .committers
                .mutate(
                    &db_name,
                    None,
                    insert_item(&format!("burst-{i}")),
                    PrincipalCtx::bypass(),
                )
                .await
        }));
    }

    // Bounded wait: a hang here (rather than a bounded RATE_LIMITED reply)
    // is exactly the regression ARC-008 fixes.
    let results = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        futures::future::join_all(handles),
    )
    .await
    .expect("the whole burst must resolve well inside the forward timeout, not hang");

    let mut rate_limited = 0;
    let mut landed = 0;
    for r in results {
        match r.expect("task panicked") {
            Ok(_) => landed += 1,
            Err(err) if err.code == ErrorCode::RateLimited => rate_limited += 1,
            Err(err) => panic!("unexpected error: {err}"),
        }
    }
    assert!(
        rate_limited > 0,
        "at least one of {n} concurrent forwards past the concurrency=1 cap must be \
         RATE_LIMITED instead of executing or hanging"
    );
    assert!(landed >= 1, "at least one write should still land");

    let (n_rows,): (i64,) =
        sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        n_rows, landed as i64,
        "row count matches the writes that actually landed — no phantom commit behind a \
         RATE_LIMITED reply"
    );
    Ok(())
}
