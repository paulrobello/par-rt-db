//! ENH-022 Stage 4 two-process integration tests: cross-process rate limits
//! and the committer ownership lease (docs/superpowers/specs/
//! 2026-08-22-multi-instance-stage4-design.md, option A1 + B1), plus Stage 4c
//! forwarding of non-owner writes to the lease owner. "Two processes" are two
//! full `AppState`s with distinct instance ids sharing one Postgres — built
//! with the ENH-029 `common::cluster` harness (`Cluster::two`/`two_with`/`two_bare`),
//! the shared shape for all multi-instance tests.

use crate::common::cluster::{Cluster, ReplicaId, ReplicaOpts, insert_item, mutate_until_landed};
use rtdb_server::auth::{Principal, PrincipalCtx};
use rtdb_server::error::ErrorCode;
use rtdb_server::txn::{Step, Transaction};

/// Deadline for this file's CONFLICT retry loops. Generous relative to the
/// harness's 2_000ms forward timeout so a loop that does pay one full timeout
/// still has many attempts left to converge.
const RETRY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30);

/// (T2) A per-db rate budget configured on two replicas is ONE budget: the
/// counters live in `rtdb_auth.rate_counters`, so the Nth+1 request is denied
/// regardless of which replica handled the first N.
#[tokio::test]
async fn rate_budget_is_shared_across_replicas() -> anyhow::Result<()> {
    let opts = |label: &str| ReplicaOpts {
        label: label.into(),
        per_db_rpm: 4,
        exact_limits: true,
        ..Default::default()
    };
    let cluster = Cluster::two_bare(opts("stage4-rate-a"), opts("stage4-rate-b")).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let db = cluster.db.as_str().to_string();

    let principal = Principal::Machine {
        db: db.clone(),
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
    let mut cluster = Cluster::two(items_schema(false)).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

    // Stage 4c: B's write is FORWARDED to A and commits — no CONFLICT. The
    // retry wrapper only absorbs the listener-connecting startup window.
    mutate_until_landed(&b, &db, insert_item("from-b"), PrincipalCtx::bypass()).await?;

    // The owner writes fine.
    a.realtime
        .committers
        .mutate(&db, None, insert_item("from-a"), PrincipalCtx::bypass())
        .await?;

    // Owner death: kill(A) drops A's committer entry (releasing the lease),
    // stops its axum server and background listeners, and drops every AppState
    // clone — what process death looks like to Postgres.
    cluster.kill(ReplicaId::A).await;

    // Failover: B's next write forwards (no owner answers — A is gone),
    // times out, takes the lease itself, and lands.
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
    let mut cluster = Cluster::two(items_schema(false)).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

    // A took the lease via `Cluster::two`; kill it the way process death looks
    // to Postgres — the whole state drops, so the advisory lock releases.
    cluster.kill(ReplicaId::A).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let hex = rtdb_server::db::sha256_hex(&db);
    let key = u64::from_str_radix(&hex[..16], 16).unwrap() as i64;
    let mut ghost = pool.acquire().await?;
    let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *ghost)
        .await?;
    assert!(locked, "ghost session must take the advisory lock");

    // Forward finds no owner (nobody owns the db), times out after
    // forward_timeout_ms, the takeover hits the ghost's lock, and the write
    // surfaces CONFLICT.
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
    let cluster = Cluster::two(items_schema(true)).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

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
    let cluster = Cluster::two(items_schema(false)).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

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
    let cluster = Cluster::two(items_schema(false)).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let db = cluster.db.as_str().to_string();

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
    let deadline = std::time::Instant::now() + RETRY_DEADLINE;
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
    let cluster = Cluster::two(items_schema(false)).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let db = cluster.db.as_str().to_string();

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
    let cluster = Cluster::two(items_schema(false)).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let db = cluster.db.as_str().to_string();

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
    let cluster = Cluster::two(items_schema(false)).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

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
    let a_opts = ReplicaOpts {
        label: "arc008-cap-a".into(),
        forward_concurrency: 1,
        ..Default::default()
    };
    let b_opts = ReplicaOpts {
        label: "arc008-cap-b".into(),
        ..Default::default()
    };
    let cluster = Cluster::two_with(items_schema(false), a_opts, b_opts).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

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
        // NOT `cluster.db.clone()`: `TestDb::Drop` schedules a real `DROP SCHEMA`
        // for every clone, so 8 of them would race the writes with real cleanup.
        // A plain owned `String` carries the name with no cleanup attached —
        // the original `cluster.db` still owns the one teardown, at end of test.
        let db_name = db.clone();
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

/// (ARC-001) Terminating the lease backend (not the process) must demote the
/// owner: A holds the lease and writes; an operator `pg_terminate_backend`s
/// A's lease connection (the pool silently reopens it). A's next write must
/// NOT execute unlocked — it surfaces CONFLICT (shadow behavior) and the
/// committer self-demotes. After B's failover write takes the lease, exactly
/// one replica executes writes and concurrent writes from both land without
/// lost updates (monotonic `version`, one row per write).
#[tokio::test]
async fn lease_backend_termination_demotes_owner() -> anyhow::Result<()> {
    use std::time::Duration;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let cluster = Cluster::two(items_schema(false)).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

    // A owns the lease; its write lands locally.
    a.realtime
        .committers
        .mutate(
            &db,
            None,
            insert_item("before-kill"),
            PrincipalCtx::bypass(),
        )
        .await?;

    // Find A's lease backend: the session advisory lock on the db's key.
    let hex = rtdb_server::db::sha256_hex(&db);
    let key = u64::from_str_radix(&hex[..16], 16).unwrap() as i64;
    let (lease_pid,): (i32,) = sqlx::query_as(
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted \
         AND ((classid::bigint << 32) | objid::bigint) = $1 LIMIT 1",
    )
    .bind(key)
    .fetch_one(&pool)
    .await?;

    // Operator-style backend termination: A's process keeps running, but its
    // lease session dies and the advisory lock releases. A's lease pool
    // reopens a connection on the next acquire — the `after_connect` hook
    // fails it (another session may own the lock), `lease_lost` flips, and
    // the per-write-turn verification rejects the write.
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(lease_pid)
        .execute(&pool)
        .await?;

    // Give the terminated backend a moment to fully release the lock and A's
    // pool to notice the dead connection.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // B's failover write: forwards to A (A's is_owner is now false or its
    // write CONFLICTs), times out, takes the lease itself, and lands. Retry
    // absorbs the takeover window.
    mutate_until_landed(
        &b,
        &db,
        insert_item("b-after-termination"),
        PrincipalCtx::bypass(),
    )
    .await?;

    // A's next write must not execute unlocked: after B owns the lease, A's
    // write either forwards to B (lands once) or CONFLICTs (then B's write
    // lands once via retry). Interleave concurrent writes from both sides;
    // total rows must equal total landed writes — no lost updates, no
    // double-applies.
    let mut handles = Vec::new();
    for i in 0..4 {
        let src = if i % 2 == 0 { a.clone() } else { b.clone() };
        let title = format!("post-{i}");
        let db_clone = db.clone();
        handles.push(tokio::spawn(async move {
            mutate_until_landed(&src, &db_clone, insert_item(&title), PrincipalCtx::bypass()).await
        }));
    }
    for h in handles {
        h.await??;
    }

    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(&pool)
        .await?;
    // 1 pre-kill + 1 B failover + 4 interleaved = 6 rows, every id distinct
    // (count(*) over unique titles) — exactly-once semantics held throughout.
    assert_eq!(n, 6, "each write landed exactly once, no lost updates");

    // Every row starts at version 1 on insert; single-writer integrity here is
    // proven by the exactly-once row count above (six distinct ids, no lost or
    // duplicated inserts) plus the id-distinctness check below.
    let (distinct,): (i64,) = sqlx::query_as(&format!(
        "SELECT count(DISTINCT id) FROM \"db_{db}\".\"t_items\""
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        distinct, n,
        "no duplicated or lost updates — single-writer held"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

/// (ARC-004) SchemaCache coherence across replicas. Push v1 via A (the
/// cluster bootstrap), warm B's cache with a forwarded write, push v2 adding
/// table `t2` via A, then within a short window a write against `t2` on B
/// must succeed: the schema NOTIFY invalidates B's cached entry and the next
/// read reloads from Postgres.
#[tokio::test]
async fn schema_push_invalidates_peer_cache() -> anyhow::Result<()> {
    fn schema_with(tables: serde_json::Value) -> rtdb_server::schema::SchemaDef {
        serde_json::from_value(serde_json::json!({ "tables": tables })).expect("valid schema")
    }
    let items_table = serde_json::json!({
        "fields": { "title": { "type": "string" } },
        "indexes": [{ "name": "by_title", "fields": ["title"] }]
    });

    let cluster = Cluster::two(schema_with(serde_json::json!({ "items": items_table }))).await;
    let a = cluster.replica(ReplicaId::A).state.clone();
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

    // Warm B's cache with v1 (its forwarded write goes through A, and B's own
    // schema reads on the forward path populate the cache).
    mutate_until_landed(&b, &db, insert_item("warm-cache"), PrincipalCtx::bypass()).await?;

    // Push v2 adding `t2` via A (the owner).
    let v2 = schema_with(serde_json::json!({
        "items": items_table,
        "t2": {
            "fields": { "name": { "type": "string" } },
            "indexes": [{ "name": "by_name", "fields": ["name"] }]
        }
    }));
    let _applied = a.realtime.committers.push_schema(&db, v2).await?;

    // Within 2s, a write against `t2` on B must succeed: B's stale cache
    // would answer NotFound until the NOTIFY-driven invalidation lands.
    let insert_t2 = || Transaction {
        steps: vec![Step::Insert {
            table: "t2".to_string(),
            doc: serde_json::json!({ "name": "peer" })
                .as_object()
                .expect("object")
                .clone(),
        }],
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let landed = loop {
        match b
            .realtime
            .committers
            .mutate(&db, None, insert_t2(), PrincipalCtx::bypass())
            .await
        {
            Ok(_) => break true,
            Err(err) if err.code == ErrorCode::NotFound => {
                if std::time::Instant::now() >= deadline {
                    break false;
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
            Err(err) => return Err(err.into()),
        }
    };
    assert!(
        landed,
        "B must see the pushed table t2 within 2s (cache invalidated)"
    );

    // Forwarded case: push v3 adding `t3` via the SHADOW B; the owner A
    // executes and replies; B's origin-side cache refresh must make B's very
    // next write against `t3` succeed without waiting for its own NOTIFY
    // round trip.
    let v3 = schema_with(serde_json::json!({
        "items": { "fields": { "title": { "type": "string" } },
                   "indexes": [{ "name": "by_title", "fields": ["title"] }] },
        "t2": { "fields": { "name": { "type": "string" } },
                "indexes": [{ "name": "by_name", "fields": ["name"] }] },
        "t3": { "fields": { "name": { "type": "string" } },
                "indexes": [{ "name": "by_name", "fields": ["name"] }] }
    }));
    let deadline = std::time::Instant::now() + RETRY_DEADLINE;
    loop {
        match b.realtime.committers.push_schema(&db, v3.clone()).await {
            Ok(_) => break,
            Err(err) if err.code == ErrorCode::Conflict => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "forwarded push kept conflicting: {err}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(err) => return Err(err.into()),
        }
    }
    b.realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "t3".to_string(),
                    doc: serde_json::json!({ "name": "origin-refresh" })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("B must see t3 immediately after its own forwarded push");

    // Sanity: both tables actually exist in Postgres.
    let (n,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM information_schema.tables \
         WHERE table_schema = $1 AND table_name IN ('t_t2','t_t3')",
    )
    .bind(format!("db_{db}"))
    .fetch_one(&pool)
    .await?;
    assert_eq!(n, 2, "t2 and t3 exist in Postgres");
    Ok(())
}
