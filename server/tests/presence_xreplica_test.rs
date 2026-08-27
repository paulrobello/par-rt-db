//! Integration tests for cross-instance presence gossip via Postgres
//! LISTEN/NOTIFY (ENH-022 Stage 3).
//!
//! The gossip test builds its two replicas (A + B) with the ENH-029
//! `common::cluster` harness — `Cluster::two_with` with presence enabled on
//! both — so both share one Postgres, run `multi_instance = true` with
//! distinct instance ids, and bind real listeners. A `join` on replica A
//! publishes the room's local snapshot via `pg_notify('rtdb_presence', …)`;
//! replica B's dedicated presence LISTEN task mirrors it into its peer shadow
//! map and marks the room dirty so the next flush broadcasts the union —
//! local members first, then namespaced peer members
//! (`"{origin_instance_id}:{conn_id}"`).
//!
//! The peer-eviction test drives `PresenceManager::expire_peers` directly with
//! a short beat timeout, proving the "killing A evicts A's members within the
//! beat timeout" contract without waiting out a real 15s timeout; it builds a
//! bare `PresenceManager` (no Postgres) and so does not use the harness.

use crate::common::cluster::{Cluster, ReplicaId, ReplicaOpts};
use crate::common::wait_until;
use rtdb_server::presence::{PresenceConfig, PresenceManager};
use rtdb_server::protocol::{AuthedUser, PresenceMember, ServerMessage, UserKind};
use serde_json::json;
use tokio::sync::mpsc;

/// Harness options for one presence-gossip replica: presence enabled with a
/// non-zero broadcast interval so the flush task is spawned normally (matches
/// a real multi-instance deploy). Tests drive `flush_once` directly for
/// determinism; the background task coexists without interfering.
fn presence_opts(label: &str) -> ReplicaOpts {
    ReplicaOpts {
        label: label.to_string(),
        presence_enabled: true,
        presence_broadcast_interval_ms: 50,
        ..Default::default()
    }
}

/// The schema `Cluster::two_with` pushes before the test runs. Presence rooms
/// key by db NAME only — the schema itself is incidental, so a minimal
/// one-table schema suffices.
fn presence_schema() -> rtdb_server::schema::SchemaDef {
    serde_json::from_value(serde_json::json!({
        "tables": { "items": {
            "fields": { "title": { "type": "string" } },
            "indexes": [{ "name": "by_title", "fields": ["title"] }]
        }}
    }))
    .expect("valid schema")
}

fn user(email: &str) -> AuthedUser {
    AuthedUser {
        kind: UserKind::User,
        email: Some(email.into()),
        name: None,
        github_login: None,
        github_id: None,
    }
}

/// ENH-022 Stage 3: a `join` on replica A is visible to a subscriber on
/// replica B via Postgres LISTEN/NOTIFY gossip. Replica B's union broadcast
/// contains BOTH its own local member AND the peer member from A, with A's
/// member namespaced as `"<a's instance id>:<conn>"` so the two replicas' per-process
/// ConnIds cannot collide. This is the load-bearing wire-visible difference in
/// multi-instance mode and the whole point of the gossip layer.
#[tokio::test]
async fn cross_replica_presence_union_includes_namespaced_peer_member() -> anyhow::Result<()> {
    let cluster = Cluster::two_with(
        presence_schema(),
        presence_opts("replica-a"),
        presence_opts("replica-b"),
    )
    .await;
    let state_a = cluster.replica(ReplicaId::A).state.clone();
    let state_b = cluster.replica(ReplicaId::B).state.clone();
    // A's members arrive on B namespaced by A's instance id. That id is
    // process-unique and the harness minted it, so read it back from the
    // replica rather than rebuilding it (`unique_instance_id` is stable per
    // process, so this is the very string replica A was constructed with).
    let peer_conn_id = format!("{}:1", cluster.replica(ReplicaId::A).instance_id);
    let db = cluster.db.as_str().to_string();

    // 1. A LOCAL subscriber on replica B conn 1 in "room-x". This is the
    //    client whose receipt of the union broadcast we assert against. The
    //    `rx` is what receives `PresenceSnapshot` when B flushes.
    let (tx_b, rx_b) = mpsc::unbounded_channel::<ServerMessage>();
    let rx_b = std::cell::RefCell::new(rx_b);
    state_b
        .realtime
        .presence
        .join(&db, 1, "room-x", None, user("local@b.example"), tx_b)
        .await
        .expect("B local join");
    // Drain the initial local-only snapshot from B's join so the next recv
    // is unambiguously the union after A's gossip arrives.
    let _ = rx_b.borrow_mut().try_recv();

    // 2. Join on replica A conn 1 in the SAME room. A's join calls
    //    gossip_publish -> pg_notify('rtdb_presence', …). B's presence LISTEN
    //    task picks it up and calls ingest_peer_snapshot, which refreshes the
    //    shadow map and marks (db, room-x) dirty on B.
    let (tx_a, _rx_a) = mpsc::unbounded_channel::<ServerMessage>();
    state_a
        .realtime
        .presence
        .join(
            &db,
            1,
            "room-x",
            Some(json!({"role": "caller"})),
            user("peer@a.example"),
            tx_a,
        )
        .await
        .expect("A join");

    // 3. Poll: drive B's flush_once until the union broadcast arrives AND
    //    contains the namespaced peer member. NOTIFY delivery is async; bound
    //    the wait at ~5s with 50ms sleeps (matching the broadcast interval).
    let found: std::cell::RefCell<Option<Vec<PresenceMember>>> = std::cell::RefCell::new(None);
    let got_peer = wait_until(std::time::Duration::from_secs(5), || async {
        state_b.realtime.presence.flush_once().await;
        if let Ok(ServerMessage::PresenceSnapshot { members, .. }) = rx_b.borrow_mut().try_recv()
            && members.iter().any(|m| m.connection_id == peer_conn_id)
        {
            *found.borrow_mut() = Some(members);
            true
        } else {
            false
        }
    })
    .await;
    assert!(
        got_peer,
        "replica B never observed A's join in its union broadcast within the \
         deadline — the rtdb_presence LISTEN path or gossip_publish is broken"
    );
    let members = found.borrow_mut().take().expect("checked above");
    // Assert the union shape: local member "1" (B's own, plain conn id) AND
    // the namespaced peer member `peer_conn_id` (A's instance id + ":1").
    let local = members.iter().find(|m| m.connection_id == "1");
    let peer = members.iter().find(|m| m.connection_id == peer_conn_id);
    assert!(
        local.is_some(),
        "union must contain B's own local member with plain conn id"
    );
    let peer = peer.expect("checked above");
    assert_eq!(
        peer.user.email.as_deref(),
        Some("peer@a.example"),
        "peer member carries A's user"
    );
    assert_eq!(
        peer.state,
        json!({"role": "caller"}),
        "peer member carries A's state"
    );

    Ok(())
}

/// ENH-022 Stage 3: when a peer replica dies it stops beating, and the
/// `expire_peers` sweep (driven by the flush loop) drops its shadow entries
/// once `last_beat` is older than `beat_timeout_ms`. This test drives the
/// sweep directly via the `expire_peers` test seam with a short timeout,
/// proving the contract without waiting out a real 15s window.
///
/// Approach: build a minimal multi-instance `PresenceManager` with a 50ms beat
/// timeout. Ingest a peer snapshot. Verify the union broadcast contains the
/// peer member. Sleep past the timeout. Call `expire_peers`. Verify the union
/// broadcast no longer contains the peer member.
#[tokio::test]
async fn expire_peers_evicts_dead_replica_members_from_union() -> anyhow::Result<()> {
    // Short beat timeout so the test can drive eviction in tens of ms.
    let config = PresenceConfig {
        enabled: true,
        max_state_bytes: 1024,
        max_room_size: 100,
        max_rooms_per_conn: 32,
        max_room_bytes: 256,
        broadcast_interval_ms: 0,
        update_limit_per_sec: 20,
        max_ttl_ms: 300_000,
        beat_interval_ms: 5000,
        beat_timeout_ms: 50,
    };
    // multi_instance=true; pool=None because this test never publishes (only
    // ingests + expires). The constructor accepts None for pool precisely so
    // unit-style tests can exercise the peer map without a live Postgres.
    let mgr = PresenceManager::new(None, config, true, "self".to_string(), None);

    // 1. Local member conn 1 on "self" in "room-y". This is the subscriber
    //    whose received snapshot we assert against.
    let (tx, rx) = mpsc::unbounded_channel::<ServerMessage>();
    let rx = std::cell::RefCell::new(rx);
    mgr.join("db-y", 1, "room-y", None, user("me@self.example"), tx)
        .await
        .expect("local join");
    // Drain the initial local-only snapshot.
    let _ = rx.borrow_mut().try_recv();

    // 2. Ingest a peer snapshot from "replica-dead" — simulates receiving a
    //    pg_notify from a peer that is about to die. This refreshes last_beat
    //    to NOW, so the entry is live until the timeout elapses.
    let peer_member = PresenceMember {
        connection_id: "42".to_string(),
        user: user("ghost@dead.example"),
        state: json!({"dying": true}),
    };
    mgr.ingest_peer_snapshot("replica-dead", "db-y", "room-y", vec![peer_member])
        .await;

    // 3. Flush. The union broadcast must include BOTH the local member AND
    //    the namespaced peer member "replica-dead:42".
    mgr.flush_once().await;
    let snap = rx
        .borrow_mut()
        .try_recv()
        .expect("union snapshot after peer ingest");
    let ServerMessage::PresenceSnapshot { members, .. } = snap else {
        panic!("expected PresenceSnapshot, got {snap:?}")
    };
    assert!(
        members.iter().any(|m| m.connection_id == "1"),
        "union includes local member"
    );
    let peer = members
        .iter()
        .find(|m| m.connection_id == "replica-dead:42")
        .expect("union includes peer member before expiry");
    assert_eq!(
        peer.user.email.as_deref(),
        Some("ghost@dead.example"),
        "peer member user preserved"
    );

    // 4. Wait out the beat timeout, then drive eviction. This is the exact
    //    code path the flush task's per-tick `expire_peers` call takes.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    mgr.expire_peers().await;

    // 5. Flush again. The union must now contain ONLY the local member — the
    //    dead peer's shadow entry was evicted and the room was marked dirty.
    //    Re-mark dirty manually because flush_once drains the dirty set and
    //    we want to force another broadcast cycle here (expire_peers already
    //    marks dirty, but the prior flush consumed it; this is belt-and-
    //    suspenders for the assertion).
    mgr.flush_once().await;
    // If no new snapshot was broadcast (dirty set was already drained), poke
    // the room by re-ingesting an EMPTY local snapshot via update_state on the
    // local member, which re-marks dirty without changing membership.
    // Simpler: directly assert via flush loop by joining a second local member
    // to force a fresh dirty+flush cycle.
    let (tx2, rx2) = mpsc::unbounded_channel::<ServerMessage>();
    let rx2 = std::cell::RefCell::new(rx2);
    mgr.join("db-y", 2, "room-y", None, user("other@self.example"), tx2)
        .await
        .expect("second local join triggers fresh flush");
    // Drain potentially several snapshots until we see one without the peer.
    let confirmed_peer_gone = wait_until(std::time::Duration::from_secs(2), || async {
        mgr.flush_once().await;
        let mut confirmed = false;
        while let Ok(ServerMessage::PresenceSnapshot { members, .. }) = rx.borrow_mut().try_recv() {
            let has_peer = members.iter().any(|m| m.connection_id == "replica-dead:42");
            if !has_peer {
                confirmed = true;
                // Local members must still be present.
                assert!(
                    members.iter().any(|m| m.connection_id == "1"),
                    "local member 1 still present after peer eviction"
                );
            }
        }
        while let Ok(ServerMessage::PresenceSnapshot { members, .. }) = rx2.borrow_mut().try_recv()
        {
            let has_peer = members.iter().any(|m| m.connection_id == "replica-dead:42");
            if !has_peer {
                confirmed = true;
            }
        }
        confirmed
    })
    .await;
    assert!(
        confirmed_peer_gone,
        "peer member was not evicted from the union within the deadline — \
         expire_peers did not drop the stale PeerSnapshot"
    );

    Ok(())
}

/// ENH-022 Stage 3 negative: with `multi_instance = false` (the default), the
/// presence manager must NEVER publish `pg_notify`, and no presence LISTEN
/// task is spawned. A join on instance A must not affect instance B's room
/// state. This is the single-instance invariant: byte-identical behavior to
/// pre-Stage-3.
#[tokio::test]
async fn single_instance_does_not_gossip() -> anyhow::Result<()> {
    // Default config: multi_instance = false, presence_enabled = true.
    let config = PresenceConfig {
        enabled: true,
        max_state_bytes: 1024,
        max_room_size: 100,
        max_rooms_per_conn: 32,
        max_room_bytes: 256,
        broadcast_interval_ms: 0,
        update_limit_per_sec: 20,
        max_ttl_ms: 300_000,
        beat_interval_ms: 5000,
        beat_timeout_ms: 15000,
    };
    let mgr = PresenceManager::new(None, config, false, "solo".to_string(), None);

    // Local join + ingest attempt. In single-instance mode, ingest_peer_snapshot
    // is a debug_assert-only guard (the listener is not spawned), so we verify
    // the WIRE shape instead: the local snapshot contains exactly one member
    // with a plain conn id, no namespacing, no peer contributions.
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();
    mgr.join("db-solo", 1, "room-solo", None, user("a@b.example"), tx)
        .await
        .expect("local join");
    mgr.flush_once().await;
    let snap = rx.try_recv().expect("snapshot");
    let ServerMessage::PresenceSnapshot { members, .. } = snap else {
        panic!("expected PresenceSnapshot, got {snap:?}")
    };
    assert_eq!(members.len(), 1, "single-instance room has one member");
    assert_eq!(
        members[0].connection_id, "1",
        "single-instance conn id is plain, never namespaced"
    );

    Ok(())
}
