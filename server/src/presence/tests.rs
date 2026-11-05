use super::*;
use crate::protocol::UserKind;
use tokio::sync::mpsc;

fn cfg() -> PresenceConfig {
    PresenceConfig {
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
    }
}
fn user(email: &str) -> AuthedUser {
    AuthedUser {
        kind: UserKind::User,
        email: Some(email.into()),
        name: None,
        github_login: None,
        github_id: None,
        user_id: Some(format!("u-{email}")),
    }
}
fn tx() -> (
    mpsc::UnboundedSender<ServerMessage>,
    mpsc::UnboundedReceiver<ServerMessage>,
) {
    mpsc::unbounded_channel()
}
fn mgr() -> Arc<PresenceManager> {
    PresenceManager::new(None, cfg(), false, "test".to_string(), None)
}

#[tokio::test]
async fn join_then_snapshot_lists_the_member() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    let members = m.snapshot("db", "room").await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].user.email.as_deref(), Some("a@b.com"));
    assert_eq!(members[0].connection_id, "1");
}

#[tokio::test]
async fn update_state_changes_the_blob() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    m.update_state("db", 1, "room", serde_json::json!({"x": 5}), None)
        .await
        .unwrap();
    let members = m.snapshot("db", "room").await;
    assert_eq!(members[0].state, serde_json::json!({"x": 5}));
}

/// The ARC-013 gate: a connection that negotiated a version below
/// `PRESENCE_DELTA_MIN_VERSION` keeps receiving full `PresenceSnapshot`
/// broadcasts on every flush, while a v3 connection in the same room
/// receives the delta. Neither ever receives a frame type it can't
/// parse.
#[tokio::test]
async fn flush_sends_deltas_to_v3_and_snapshots_to_prev3_connections() {
    let m = mgr();
    let (t_v3, mut r_v3) = tx();
    let (t_old, mut r_old) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t_v3, Some(3))
        .await
        .unwrap();
    m.join("db", 2, "room", None, user("b@b.com"), t_old, Some(2))
        .await
        .unwrap();
    // First flush: both are fresh joins (nothing to diff against) →
    // both get the full snapshot regardless of version.
    m.flush_once().await;
    match r_v3.try_recv().expect("v3 first broadcast") {
        ServerMessage::PresenceSnapshot { members, .. } => assert_eq!(members.len(), 2),
        other => panic!("expected PresenceSnapshot, got {other:?}"),
    }
    match r_old.try_recv().expect("legacy first broadcast") {
        ServerMessage::PresenceSnapshot { members, .. } => assert_eq!(members.len(), 2),
        other => panic!("expected PresenceSnapshot, got {other:?}"),
    }
    // A state change on the next flush: the v3 connection gets the
    // delta; the v2 connection gets a full snapshot again — never a
    // `presenceDelta` frame its parser would reject.
    m.update_state("db", 1, "room", serde_json::json!({"n": 1}), None)
        .await
        .unwrap();
    m.flush_once().await;
    match r_v3.try_recv().expect("v3 delta") {
        ServerMessage::PresenceDelta {
            seq, state_changed, ..
        } => {
            assert_eq!(seq, 2);
            assert_eq!(state_changed.len(), 1);
            assert_eq!(state_changed[0].connection_id, "1");
        }
        other => panic!("expected PresenceDelta, got {other:?}"),
    }
    match r_old.try_recv().expect("legacy snapshot") {
        ServerMessage::PresenceSnapshot { members, .. } => {
            let m1 = members.iter().find(|m| m.connection_id == "1").unwrap();
            assert_eq!(m1.state, serde_json::json!({"n": 1}));
        }
        other => panic!("expected PresenceSnapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn inspect_lists_rooms_with_counts_bytes_and_oldest_age() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "lobby", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    let (t2, _r2) = tx();
    m.join(
        "db",
        2,
        "lobby",
        Some(serde_json::json!({"k": "v"})),
        user("c@d.com"),
        t2,
        Some(3),
    )
    .await
    .unwrap();
    let rooms = m.inspect().await;
    assert_eq!(rooms.len(), 1, "one room across shards");
    let r = &rooms[0];
    assert_eq!(r.room, "lobby");
    assert_eq!(r.member_count, 2);
    assert_eq!(r.local_member_count, 2, "single-instance: local == merged");
    // Sum of serialized member state sizes: `null` (4 bytes) + `{"k":"v"}`
    // (9 bytes).
    assert_eq!(r.state_bytes, 13);
    assert!(
        !r.merged_with_peers,
        "single-instance mode never merges peers"
    );
    assert!(
        r.oldest_member_age_ms <= 1_000,
        "oldest join is this test's own age: {}",
        r.oldest_member_age_ms
    );
}

#[tokio::test]
async fn inspect_merges_gossiped_peer_membership_in_multi_instance_mode() {
    // multi_instance=true, no pool (this test never publishes — only
    // ingests, the same seam `expire_peers_evicts_dead_replica_members`
    // in presence_xreplica_test.rs uses for a bare `PresenceManager`).
    let m = PresenceManager::new(None, cfg(), true, "self".to_string(), None);
    let (t, _r) = tx();
    m.join("db", 1, "lobby", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    // Before any peer snapshot arrives: merged == local, not merged.
    let rooms = m.inspect().await;
    assert_eq!(rooms.len(), 1);
    assert_eq!(rooms[0].member_count, 1);
    assert_eq!(rooms[0].local_member_count, 1);
    assert!(!rooms[0].merged_with_peers, "no peer contribution yet");

    // Ingest a peer snapshot for the SAME room from a different instance.
    let peer_member = PresenceMember {
        connection_id: "9".to_string(),
        user: user("peer@remote.example"),
        state: serde_json::json!({"k": "v"}), // 9 bytes serialized
    };
    m.ingest_peer_snapshot("replica-b", "db", "lobby", vec![peer_member])
        .await;

    let rooms = m.inspect().await;
    assert_eq!(rooms.len(), 1);
    let r = &rooms[0];
    assert_eq!(
        r.member_count, 2,
        "merged count includes the gossiped peer member"
    );
    assert_eq!(
        r.local_member_count, 1,
        "local count is unaffected by the merge"
    );
    // local `null` (4 bytes) + peer `{"k":"v"}` (9 bytes).
    assert_eq!(r.state_bytes, 13);
    assert!(r.merged_with_peers);

    // A DIFFERENT room this replica never joined LOCALLY, with only a
    // peer contribution, must still surface — this is the card's core
    // bug: a room with zero local members previously had no shard entry
    // to report from at all, so it vanished from the inspector entirely
    // rather than merely under-reporting its count.
    m.ingest_peer_snapshot(
        "replica-b",
        "db",
        "peer-only-room",
        vec![PresenceMember {
            connection_id: "1".to_string(),
            user: user("solo@remote.example"),
            state: serde_json::Value::Null,
        }],
    )
    .await;
    let rooms = m.inspect().await;
    assert_eq!(rooms.len(), 2, "the peer-only room now surfaces too");
    let peer_only = rooms
        .iter()
        .find(|r| r.room == "peer-only-room")
        .expect("peer-only-room present");
    assert_eq!(peer_only.member_count, 1);
    assert_eq!(
        peer_only.local_member_count, 0,
        "this replica never joined it"
    );
    assert!(peer_only.merged_with_peers);
}

#[tokio::test]
async fn update_state_when_not_joined_errors() {
    let m = mgr();
    let err = m
        .update_state("db", 1, "room", serde_json::json!({}), None)
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
}

#[tokio::test]
async fn leave_removes_the_member() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    m.leave("db", 1, "room").await;
    assert!(m.snapshot("db", "room").await.is_empty());
}

#[tokio::test]
async fn remove_conn_evicts_every_room_the_conn_was_in() {
    let m = mgr();
    let (t1, _r1) = tx();
    let (t2, _r2) = tx();
    m.join("db", 1, "a", None, user("a@b.com"), t1, Some(3))
        .await
        .unwrap();
    m.join(
        "db",
        1,
        "b",
        None,
        user("a@b.com"),
        mpsc::unbounded_channel().0,
        Some(3),
    )
    .await
    .unwrap();
    m.join("db", 2, "a", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap();
    m.remove_conn("db", 1).await;
    // `("db","a")` is ONE room shared by conns 1 and 2 — conn 2 remains.
    assert_eq!(m.snapshot("db", "a").await.len(), 1);
    assert!(m.snapshot("db", "b").await.is_empty());
}

#[tokio::test]
async fn repeated_rejoin_does_not_inflate_rooms_per_conn_cap() {
    // Regression: re-joining the same room used to append a duplicate
    // entry to `conn_rooms[conn]`, inflating the `max_rooms_per_conn`
    // accounting and locking the conn out of new rooms after enough
    // re-joins despite genuinely being in one room.
    let mut c = cfg();
    c.max_rooms_per_conn = 4;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t, _r) = tx();
    // Join "a" once, then re-join "a" 5 more times.
    m.join("db", 1, "a", None, user("a@b.com"), t.clone(), Some(3))
        .await
        .unwrap();
    for _ in 0..5 {
        m.join("db", 1, "a", None, user("a@b.com"), t.clone(), Some(3))
            .await
            .unwrap();
    }
    // The conn is still in exactly one room — a new room must be accepted.
    m.join("db", 1, "b", None, user("a@b.com"), t.clone(), Some(3))
        .await
        .unwrap();
    m.join("db", 1, "c", None, user("a@b.com"), t.clone(), Some(3))
        .await
        .unwrap();
    m.join("db", 1, "d", None, user("a@b.com"), t.clone(), Some(3))
        .await
        .unwrap();
    // Now at the cap (4 rooms: a, b, c, d). The next NEW room must reject.
    let err = m
        .join("db", 1, "e", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
    // And the room that rejected is genuinely empty.
    assert!(m.snapshot("db", "e").await.is_empty());
}

#[tokio::test]
async fn flush_once_sends_a_snapshot_to_each_member() {
    let m = mgr();
    let (t1, mut r1) = tx();
    let (t2, mut r2) = tx();
    m.join(
        "db",
        1,
        "room",
        Some(serde_json::json!({"x":1})),
        user("a@b.com"),
        t1,
        Some(3),
    )
    .await
    .unwrap();
    m.join("db", 2, "room", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap();
    m.flush_once().await;
    // each member receives a snapshot listing both members
    for r in [&mut r1, &mut r2] {
        let msg = r.try_recv().expect("snapshot");
        match msg {
            ServerMessage::PresenceSnapshot { room, members } => {
                assert_eq!(room, "room");
                assert_eq!(members.len(), 2);
            }
            _ => panic!("expected PresenceSnapshot, got {msg:?}"),
        }
    }
}

#[tokio::test]
async fn flush_once_coalesces_multiple_updates_into_one_snapshot() {
    let m = mgr();
    let (t1, mut r1) = tx();
    let (t2, _r2) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t1, Some(3))
        .await
        .unwrap();
    m.join("db", 2, "room", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap();
    // three rapid state updates by conn 1 -> conn 2 should see ONE snapshot after flush
    m.update_state("db", 1, "room", serde_json::json!({"n":1}), None)
        .await
        .unwrap();
    m.update_state("db", 1, "room", serde_json::json!({"n":2}), None)
        .await
        .unwrap();
    m.update_state("db", 1, "room", serde_json::json!({"n":3}), None)
        .await
        .unwrap();
    m.flush_once().await;
    let msg = r1.try_recv().expect("snapshot");
    let ServerMessage::PresenceSnapshot { members, .. } = msg else {
        panic!()
    };
    assert_eq!(
        members
            .iter()
            .find(|memb| memb.connection_id == "1")
            .unwrap()
            .state,
        serde_json::json!({"n":3})
    );
    // only one snapshot was enqueued despite 3 updates + 2 joins:
    assert!(r1.try_recv().is_err(), "expected exactly one snapshot");
}

#[tokio::test]
async fn flush_once_clears_the_dirty_set() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    m.flush_once().await;
    // a second flush with no new changes sends nothing
    // (assert by joining a 2nd conn that drains its own join snapshot separately)
    let (t2, mut r2) = tx();
    m.join("db", 2, "room", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap();
    m.flush_once().await;
    let _ = r2.try_recv().expect("2nd conn got its snapshot");
    m.flush_once().await;
    assert!(
        r2.try_recv().is_err(),
        "dirty set was cleared; no further snapshot"
    );
}

#[tokio::test]
async fn oversize_state_is_rejected_bad_request() {
    let mut c = cfg();
    c.max_state_bytes = 8;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t, _r) = tx();
    let big = serde_json::json!({"big":"0123456789"});
    let err = m
        .join("db", 1, "room", Some(big), user("a@b.com"), t, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
}

#[tokio::test]
async fn empty_room_name_is_rejected() {
    let m = mgr();
    let (t, _r) = tx();
    let err = m
        .join("db", 1, "", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
}

#[tokio::test]
async fn room_full_is_rejected_forbidden() {
    let mut c = cfg();
    c.max_room_size = 1;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t1, _r1) = tx();
    let (t2, _r2) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t1, Some(3))
        .await
        .unwrap();
    let err = m
        .join("db", 2, "room", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
}

#[tokio::test]
async fn too_many_rooms_per_conn_is_rejected_forbidden() {
    let mut c = cfg();
    c.max_rooms_per_conn = 1;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t, _r) = tx();
    m.join(
        "db",
        1,
        "a",
        None,
        user("a@b.com"),
        mpsc::unbounded_channel().0,
        Some(3),
    )
    .await
    .unwrap();
    let err = m
        .join("db", 1, "b", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
}

#[tokio::test]
async fn presence_state_rate_limit_denies_after_cap() {
    let mut c = cfg();
    c.update_limit_per_sec = 2;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    assert!(
        m.update_state("db", 1, "room", serde_json::json!({"n":1}), None)
            .await
            .is_ok()
    );
    assert!(
        m.update_state("db", 1, "room", serde_json::json!({"n":2}), None)
            .await
            .is_ok()
    );
    let err = m
        .update_state("db", 1, "room", serde_json::json!({"n":3}), None)
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::RateLimited);
}

#[tokio::test]
async fn feature_disabled_rejects_join_forbidden() {
    let mut c = cfg();
    c.enabled = false;
    let m = PresenceManager::new(None, c, false, "test".to_string(), None);
    let (t, _r) = tx();
    let err = m
        .join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
}

#[tokio::test]
async fn update_state_arms_ttl_that_expires_to_null_not_remove() {
    let m = mgr();
    let (t1, _r1) = tx();
    let (t2, mut r2) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t1, Some(3))
        .await
        .unwrap();
    m.join("db", 2, "room", None, user("b@b.com"), t2, Some(3))
        .await
        .unwrap();
    // conn 1 arms a 60ms ttl on its typing state.
    m.update_state(
        "db",
        1,
        "room",
        serde_json::json!({"typing": true}),
        Some(60),
    )
    .await
    .unwrap();
    // member is still present, state is the blob.
    let members = m.snapshot("db", "room").await;
    assert_eq!(
        members
            .iter()
            .find(|x| x.connection_id == "1")
            .unwrap()
            .state,
        serde_json::json!({"typing": true})
    );
    assert_eq!(members.len(), 2);
    // wait out the ttl, then expire.
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    let expired = m.expire_once().await;
    assert!(expired);
    // state cleared to null, but the MEMBER is still present.
    let members = m.snapshot("db", "room").await;
    let conn1 = members.iter().find(|x| x.connection_id == "1").unwrap();
    assert_eq!(conn1.state, serde_json::Value::Null);
    assert_eq!(members.len(), 2, "expiry clears state, not membership");
    // and the dirty room broadcast a snapshot with null state.
    m.flush_once().await;
    let msg = r2.try_recv().expect("snapshot after expiry");
    let ServerMessage::PresenceSnapshot { members, .. } = msg else {
        panic!("snapshot")
    };
    let conn1 = members.iter().find(|x| x.connection_id == "1").unwrap();
    assert_eq!(conn1.state, serde_json::Value::Null);
}

#[tokio::test]
async fn omitted_ttl_is_permanent_and_does_not_expire() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    // update with NO ttl -> permanent.
    m.update_state("db", 1, "room", serde_json::json!({"x": 1}), None)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let expired = m.expire_once().await;
    assert!(!expired, "no ttl armed -> nothing expires");
    let members = m.snapshot("db", "room").await;
    assert_eq!(members[0].state, serde_json::json!({"x": 1}));
}

#[tokio::test]
async fn ttl_refresh_re_arms_and_a_non_ttl_update_clears_it() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    m.update_state("db", 1, "room", serde_json::json!({"t": true}), Some(200))
        .await
        .unwrap();
    // a later update with NO ttl clears the expiry -> permanent.
    m.update_state("db", 1, "room", serde_json::json!({"t": false}), None)
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(220)).await;
    let expired = m.expire_once().await;
    assert!(!expired, "ttl was cleared by the non-ttl update");
    assert_eq!(
        m.snapshot("db", "room").await[0].state,
        serde_json::json!({"t": false})
    );
}

#[tokio::test]
async fn ttl_validation_rejects_zero_and_over_cap() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    let err = m
        .update_state("db", 1, "room", serde_json::json!({}), Some(0))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    let err = m
        .update_state("db", 1, "room", serde_json::json!({}), Some(300_001))
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
}

#[tokio::test]
async fn expire_once_with_nothing_expired_is_idle() {
    let m = mgr();
    let (t, _r) = tx();
    m.join("db", 1, "room", None, user("a@b.com"), t, Some(3))
        .await
        .unwrap();
    assert!(!m.expire_once().await);
}
