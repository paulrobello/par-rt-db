//! ENH-029 step 4: the five multi-instance subscription scenarios from
//! `docs/fable/ENH-029-multi-instance-test-harness.md`, all on the shared
//! `common::cluster` harness (`Cluster::two`) rather than inline replica
//! setup. Each replica binds a real HTTP/WS listener, so subscriptions ride
//! the actual `/sync` websocket and writes go through the actual HTTP edge —
//! exactly the paths a load balancer would exercise:
//!
//! 1. subscribe on B, mutate on A via HTTP — B's `queryUpdate` (ARC-001);
//! 2. subscribe on B, schedule a job on A that writes — B's push;
//! 3. a 20 KB forwarded mutate plus a spool-sized forwarded reply (ARC-002);
//! 4. `drop_replies(A)` then mutate on B — exactly one row after
//!    takeover/dedup (ARC-003's server-minted idempotency key);
//! 5. `Cluster::kill(owner)` mid-stream — B's open subscription keeps
//!    receiving after takeover.

use std::time::{Duration, Instant};

use crate::common::cluster::{Cluster, ReplicaId, WsClient, insert_item};
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::{ClientMessage, ScheduleWhen, ServerMessage};
use rtdb_server::query::Query;
use rtdb_server::schema::SchemaDef;
use sqlx::PgPool;

/// Bounded wait for the first subscription snapshot. Generous relative to the
/// harness's listener startup but far below any test deadline.
const SNAPSHOT_WAIT: Duration = Duration::from_secs(10);
/// Bounded wait for a push triggered by a remote write (owner commit →
/// write-set NOTIFY → peer invalidation → subscription re-run).
const PUSH_WAIT: Duration = Duration::from_secs(10);
/// Bounded wait covering the forward timeout (2 s) plus a takeover.
const FORWARD_WAIT: Duration = Duration::from_secs(15);
/// Bounded wait for the ≤2 s scheduler poll to claim and fire a due job.
const SCHEDULER_WAIT: Duration = Duration::from_secs(15);

/// The items schema the harness pushes on cluster startup (mirrors
/// `multi_instance_stage4_test::items_schema(false)`).
fn items_schema() -> SchemaDef {
    serde_json::from_value(serde_json::json!({
        "tables": { "items": {
            "fields": { "title": { "type": "string" } },
            "indexes": [{ "name": "by_title", "fields": ["title"] }]
        }}
    }))
    .expect("valid schema")
}

fn items_query() -> Query {
    serde_json::from_value(serde_json::json!({ "table": "items" })).expect("parse query")
}

fn docs_len(value: &serde_json::Value) -> usize {
    value.as_array().expect("docs array").len()
}

/// Read the next server frame matching `pred` within `within`, skipping
/// unrelated frames (acks, pongs, pushes for other queries). `None` on
/// timeout or socket close — callers assert on it with context.
async fn ws_next<F>(ws: &mut WsClient, within: Duration, mut pred: F) -> Option<ServerMessage>
where
    F: FnMut(&ServerMessage) -> bool,
{
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match ws.recv_timeout(remaining).await {
            Some(msg) if pred(&msg) => return Some(msg),
            Some(_) => continue,
            None => return None,
        }
    }
    None
}

/// The next `QueryUpdate` for `query_id`, or `None` if it never arrives.
async fn ws_query_update(
    ws: &mut WsClient,
    query_id: &str,
    within: Duration,
) -> Option<serde_json::Value> {
    match ws_next(
        ws,
        within,
        |m| matches!(m, ServerMessage::QueryUpdate { query_id: id, .. } if id == query_id),
    )
    .await
    {
        Some(ServerMessage::QueryUpdate { result, .. }) => Some(result),
        _ => None,
    }
}

/// The next `MutateOk`/`MutateErr` for `mut_id`, panicking with context if
/// neither arrives within `within`.
async fn ws_mutate_reply(ws: &mut WsClient, mut_id: &str, within: Duration) -> ServerMessage {
    ws_next(ws, within, |m| {
        matches!(
            m,
            ServerMessage::MutateOk { mut_id: id, .. } | ServerMessage::MutateErr { mut_id: id, .. }
                if id == mut_id
        )
    })
    .await
    .unwrap_or_else(|| panic!("no MutateOk/MutateErr for {mut_id} within {within:?}"))
}

async fn items_count(pool: &PgPool, db: &str) -> anyhow::Result<i64> {
    let (n,): (i64,) = sqlx::query_as(&format!("SELECT count(*) FROM \"db_{db}\".\"t_items\""))
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Subscribe `ws` to the items table and consume the initial snapshot,
/// asserting it currently holds `expected` rows. Returns the snapshot.
async fn subscribe_and_snapshot(
    ws: &mut WsClient,
    query_id: &str,
    expected: usize,
) -> serde_json::Value {
    ws.send(&ClientMessage::Subscribe {
        query_id: query_id.to_string(),
        query: Box::new(items_query()),
    })
    .await;
    let snapshot = ws_query_update(ws, query_id, SNAPSHOT_WAIT)
        .await
        .unwrap_or_else(|| panic!("initial QueryUpdate for {query_id}"));
    assert_eq!(docs_len(&snapshot), expected, "unexpected initial snapshot");
    snapshot
}

/// (1, ARC-001) A write submitted through the OWNER's HTTP edge reaches a
/// client subscribed through the non-owner's websocket: the owner's commit
/// fans out via the write-set NOTIFY, B invalidates the subscription and
/// re-runs it, and B's client gets its `queryUpdate`.
#[tokio::test]
async fn owner_http_mutate_pushes_query_update_to_b_subscriber() -> anyhow::Result<()> {
    let cluster = Cluster::two(items_schema()).await;
    assert_eq!(cluster.owner().await, ReplicaId::A, "A owns after push");

    // The subscription lives on B, which owns nothing.
    let mut ws_b = cluster.ws(ReplicaId::B).await;
    subscribe_and_snapshot(&mut ws_b, "q-http", 0).await;

    // The write goes through A's HTTP edge — straight to the owner, so the
    // only path that can reach B's subscriber is the cross-replica fan-out.
    let outcome = cluster
        .mutate_http(ReplicaId::A, insert_item("owner-http-write"))
        .await;
    assert!(!outcome.results.is_empty(), "HTTP mutate returned results");

    let pushed = ws_query_update(&mut ws_b, "q-http", PUSH_WAIT)
        .await
        .expect("B's subscription re-ran for the owner's HTTP write");
    assert_eq!(docs_len(&pushed), 1, "B sees the owner's insert");
    assert_eq!(
        pushed[0].get("title").and_then(|t| t.as_str()),
        Some("owner-http-write")
    );
    Ok(())
}

/// (2) A scheduled job submitted through A fires on the owner's scheduler,
/// and the resulting write pushes B's subscriber — the scheduled-write path
/// inherits the same cross-replica invalidation as a direct mutate.
#[tokio::test]
async fn owner_scheduled_write_pushes_b_subscriber() -> anyhow::Result<()> {
    const SCHED_ID: &str = "s-subs-1";

    let cluster = Cluster::two(items_schema()).await;
    let mut ws_b = cluster.ws(ReplicaId::B).await;
    subscribe_and_snapshot(&mut ws_b, "q-sched", 0).await;

    // Schedule through A's websocket: due immediately, the owner's ≤2 s
    // scheduler poll claims and fires it.
    let mut ws_a = cluster.ws(ReplicaId::A).await;
    ws_a.send(&ClientMessage::Schedule {
        schedule_id: SCHED_ID.to_string(),
        when: ScheduleWhen::AfterMs { ms: 1 },
        txn: insert_item("from-scheduler"),
    })
    .await;
    let ack = ws_next(&mut ws_a, SCHEDULER_WAIT, |m| {
        matches!(
            m,
            ServerMessage::ScheduleOk { schedule_id: s, .. }
                | ServerMessage::ScheduleErr { schedule_id: s, .. }
                if s == SCHED_ID
        )
    })
    .await
    .expect("schedule ack");
    assert!(
        matches!(ack, ServerMessage::ScheduleOk { .. }),
        "schedule rejected: {ack:?}"
    );

    let pushed = ws_query_update(&mut ws_b, "q-sched", SCHEDULER_WAIT)
        .await
        .expect("B's subscription re-ran for the scheduled write");
    assert_eq!(docs_len(&pushed), 1, "B sees the scheduled insert");
    assert_eq!(
        pushed[0].get("title").and_then(|t| t.as_str()),
        Some("from-scheduler")
    );
    Ok(())
}

/// (3, ARC-002) Both spool directions round-trip through a real cluster:
/// a 20 KB document mutated through B's HTTP edge is forwarded to owner A
/// past the 8000-byte `pg_notify` cap, and a forwarded `RunPushSchema`
/// returns an 81-table schema — a reply far past the cap — intact.
#[tokio::test]
async fn large_forwarded_mutate_and_reply_round_trip() -> anyhow::Result<()> {
    let cluster = Cluster::two(items_schema()).await;
    let b = cluster.replica(ReplicaId::B).state.clone();
    let pool = b.pool.clone();
    let db = cluster.db.as_str().to_string();

    // Leg 1 — large forwarded MUTATE: 20 KB of title through B's HTTP edge
    // (B owns nothing, so the write is forwarded to A and spooled).
    let big_title = "x".repeat(20 * 1024);
    let txn = insert_item(&big_title);
    assert!(
        serde_json::to_string(&txn)?.len() > 8000,
        "the fixture must exceed the pg_notify cap to be a regression test"
    );
    let outcome = cluster.mutate_http(ReplicaId::B, txn).await;
    assert!(
        !outcome.results.is_empty(),
        "forwarded 20 KB mutate succeeded"
    );

    let (stored,): (String,) = sqlx::query_as(&format!(
        "SELECT doc->>'title' FROM \"db_{db}\".\"t_items\""
    ))
    .fetch_one(&pool)
    .await?;
    assert_eq!(stored.len(), big_title.len(), "the whole body forwarded");

    // Leg 2 — large forwarded REPLY: keep `items` and add 80 more tables;
    // B's push_schema forwards to A, and the owner's full-schema reply
    // (well past the cap) comes back through the reply spool. Retry on
    // CONFLICT the same way `mutate_until_landed` does — the owner's
    // forward listener may still be connecting.
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
    let big_schema: SchemaDef = serde_json::from_value(serde_json::json!({ "tables": tables }))?;
    assert!(
        serde_json::to_string(&big_schema)?.len() > 8000,
        "the fixture must exceed the pg_notify cap to be a regression test"
    );

    let deadline = Instant::now() + Duration::from_secs(30);
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
                    Instant::now() < deadline,
                    "forwarded push kept conflicting: {err}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
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

/// (4, ARC-003) With the owner swallowing forward replies, an unkeyed mutate
/// on B is committed by A but its reply never returns. Killing A while that
/// original request is still pending forces B's takeover path to replay the
/// server-minted dedup key, without a second client submission.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn dropped_replies_commit_once_and_replay_after_takeover() -> anyhow::Result<()> {
    let mut cluster = Cluster::two(items_schema()).await;
    assert_eq!(cluster.owner().await, ReplicaId::A, "A owns after push");
    let pool = cluster.replica(ReplicaId::B).state.pool.clone();
    let db = cluster.db.as_str().to_string();
    let mut ws_b = cluster.ws(ReplicaId::B).await;

    cluster.drop_replies(ReplicaId::A, true).await;
    ws_b.send(&ClientMessage::Mutate {
        mut_id: "m-drop".to_string(),
        idempotency_key: None,
        txn: insert_item("dropped-reply"),
    })
    .await;

    let landed = crate::common::wait_until(PUSH_WAIT, || async {
        matches!(items_count(&pool, &db).await, Ok(1))
    })
    .await;
    assert!(
        landed,
        "owner committed the forwarded write despite the dropped reply"
    );

    // Kill A before B's original forward request reaches its timeout. The
    // request then takes over on B and replays A's server-minted dedup key.
    cluster.kill(ReplicaId::A).await;
    cluster.wait_takeover(ReplicaId::B).await;
    let reply = ws_mutate_reply(&mut ws_b, "m-drop", FORWARD_WAIT).await;
    assert!(
        matches!(reply, ServerMessage::MutateOk { .. }),
        "the in-flight mutate replay succeeded: {reply:?}"
    );

    assert_eq!(
        items_count(&pool, &db).await?,
        1,
        "reply loss + takeover + server-minted replay must net exactly one row"
    );
    Ok(())
}

/// (5) Killing the lease owner mid-stream does not break a subscriber held
/// through the OTHER replica: pushes keep flowing across the kill, the next
/// write drives B's takeover, and B's still-open subscription receives the
/// post-takeover write. Exactly-once throughout.
#[tokio::test]
async fn owner_kill_midstream_subscription_survives_takeover() -> anyhow::Result<()> {
    let mut cluster = Cluster::two(items_schema()).await;
    assert_eq!(cluster.owner().await, ReplicaId::A, "A owns after push");
    let pool = cluster.replica(ReplicaId::B).state.pool.clone();
    let db = cluster.db.as_str().to_string();

    // The subscription lives on B for the whole test — across the kill.
    let mut ws_b = cluster.ws(ReplicaId::B).await;
    subscribe_and_snapshot(&mut ws_b, "q-live", 0).await;

    // Establish the stream: two owner-side writes through A's HTTP edge,
    // each pushed to B's subscriber.
    for (i, title) in ["pre-kill-1", "pre-kill-2"].iter().enumerate() {
        cluster.mutate_http(ReplicaId::A, insert_item(title)).await;
        let pushed = ws_query_update(&mut ws_b, "q-live", PUSH_WAIT)
            .await
            .unwrap_or_else(|| panic!("push {i} before the kill"));
        assert_eq!(docs_len(&pushed), i + 1, "stream is flowing pre-kill");
    }

    // Kill the owner mid-stream: lease released, HTTP server stopped,
    // background listeners shut down — process death to Postgres.
    cluster.kill(ReplicaId::A).await;

    // The next write lands on B: no owner answers the forward, B times out,
    // takes the lease itself, and commits locally.
    ws_b.send(&ClientMessage::Mutate {
        mut_id: "m-takeover".to_string(),
        idempotency_key: None,
        txn: insert_item("post-kill"),
    })
    .await;
    let deadline = Instant::now() + FORWARD_WAIT;
    let mut reply = None;
    let mut pushed = None;
    while Instant::now() < deadline && (reply.is_none() || pushed.is_none()) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match ws_b.recv_timeout(remaining).await {
            Some(ServerMessage::MutateOk {
                mut_id, results, ..
            }) if mut_id == "m-takeover" => {
                reply = Some(ServerMessage::MutateOk { mut_id, results });
            }
            Some(ServerMessage::QueryUpdate {
                query_id, result, ..
            }) if query_id == "q-live" => {
                pushed = Some(result);
            }
            Some(_) => {}
            None => break,
        }
    }
    let reply = reply.expect("B committed after takeover");
    assert!(
        matches!(reply, ServerMessage::MutateOk { .. }),
        "B committed after takeover: {reply:?}"
    );
    cluster.wait_takeover(ReplicaId::B).await;

    // The SAME open subscription receives the post-takeover write.
    let pushed = pushed.expect("the pre-kill subscription survives the takeover");
    assert_eq!(docs_len(&pushed), 3, "B's subscriber saw every write");
    assert_eq!(
        pushed[2].get("title").and_then(|t| t.as_str()),
        Some("post-kill")
    );

    assert_eq!(
        items_count(&pool, &db).await?,
        3,
        "two owner writes + one post-takeover write, no duplicates"
    );
    Ok(())
}
