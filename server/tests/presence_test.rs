mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use common::{admin_post, fresh_db, mint_user_session, spawn_app, test_state_with_presence};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use rtdb_server::AppState;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_connect(addr: SocketAddr) -> WsStream {
    let (ws, _) = connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn send_json(ws: &mut WsStream, msg: Value) {
    ws.send(Message::Text(msg.to_string().into()))
        .await
        .expect("send frame");
}

async fn recv_json(ws: &mut WsStream) -> Value {
    match ws.next().await.expect("stream ended").expect("frame ok") {
        Message::Text(text) => serde_json::from_str(&text).expect("parse json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn auth(ws: &mut WsStream, token: &str, db: &str) -> Value {
    send_json(ws, json!({"type": "auth", "token": token, "db": db})).await;
    recv_json(ws).await
}

/// Allowlist every `email` for `db` via the admin route (mirrors
/// `per_row_auth_test.rs::wire_setup_two_users` exactly — the only established
/// way tests seed the db allowlist before a user-session handshake).
async fn allowlist(addr: SocketAddr, db: &str, emails: &[&str]) {
    for email in emails {
        let r = admin_post(
            addr,
            "/admin/allowlist",
            json!({"db": db, "action": "add", "email": email}),
        )
        .await;
        assert_eq!(
            r.status(),
            reqwest::StatusCode::OK,
            "allowlist add failed for {email}"
        );
    }
}

/// Read frames from `ws` until a `presenceSnapshot` for `room` whose member
/// count satisfies `pred`, discarding stray frames (e.g. `pong`). Bounded by a
/// 2s timeout so a missing/wrong snapshot fails the test fast instead of
/// hanging the binary. Returns the matching snapshot for member-level
/// assertions.
async fn drain_until_snapshot<F>(ws: &mut WsStream, room: &str, pred: F) -> Value
where
    F: Fn(usize) -> bool,
{
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let msg = recv_json(ws).await;
            if msg["type"] == "presenceSnapshot" && msg["room"] == room {
                let n = msg["members"].as_array().map(|a| a.len()).unwrap_or(0);
                if pred(n) {
                    return msg;
                }
            }
        }
    })
    .await
    .expect("timed out waiting for presenceSnapshot")
}

/// Mint a user session with an identity derived from `db` so concurrent tests
/// in the same binary never collide on `rtdb_auth.users.id` / `.email` /
/// `.github_id` (all UNIQUE). Mirrors `per_row_auth_test.rs::alice_uid`'s
/// rationale. Returns `(token, email)`.
async fn mint_user_for_db(state: &Arc<AppState>, db: &str, suffix: &str) -> (String, String) {
    let email = format!("{suffix}-{db}@example.com");
    let user_id = format!("{suffix}-{db}");
    let token = mint_user_session(&state.pool, &user_id, &email).await;
    (token, email)
}

/// Two connections join the same room; each receives a `presenceSnapshot`
/// containing the other. Drives determinism by explicitly flushing the
/// presence manager after each client action (the interval=0 background flush
/// task also spins, but `flush_once` guarantees the snapshot is enqueued before
/// we drain).
#[tokio::test]
async fn two_conns_see_each_other_in_a_room() {
    let state = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let (token_a, email_a) = mint_user_for_db(&state, &db, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db, "b").await;
    allowlist(addr, &db, &[&email_a, &email_b]).await;

    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    let ok_a = auth(&mut wa, &token_a, &db).await;
    let ok_b = auth(&mut wb, &token_b, &db).await;
    assert_eq!(ok_a["type"], json!("authOk"));
    assert_eq!(ok_b["type"], json!("authOk"));

    // wa joins first → 1-member snapshot to wa.
    send_json(&mut wa, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    let snap = drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;
    assert_eq!(snap["members"].as_array().map(|a| a.len()), Some(1));

    // wb joins → 2-member snapshot to BOTH members (join marks the room dirty;
    // flush broadcasts to every member, including the just-joined wb and the
    // already-present wa).
    send_json(&mut wb, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    let snap_a = drain_until_snapshot(&mut wa, "doc:1", |n| n == 2).await;
    let snap_b = drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;
    assert_eq!(snap_a["members"].as_array().map(|a| a.len()), Some(2));
    assert_eq!(snap_b["members"].as_array().map(|a| a.len()), Some(2));
}

/// A `presenceState` update from one member is observed, via the next
/// snapshot, by its peer. Asserts both the member count is unchanged AND the
/// peer's snapshot carries the new state blob on the updating member.
#[tokio::test]
async fn state_update_is_observed_by_peer() {
    let state = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let (token_a, email_a) = mint_user_for_db(&state, &db, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db, "b").await;
    allowlist(addr, &db, &[&email_a, &email_b]).await;

    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    auth(&mut wa, &token_a, &db).await;
    auth(&mut wb, &token_b, &db).await;

    send_json(&mut wa, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;

    send_json(&mut wb, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 2).await;
    drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;

    // wa publishes a state update; wb's next snapshot carries it on wa's entry.
    send_json(
        &mut wa,
        json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}}),
    )
    .await;
    state.realtime.presence.flush_once().await;
    let snap = drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;
    let members = snap["members"].as_array().expect("members array");
    assert_eq!(members.len(), 2, "room still has both members");
    let has_typing = members
        .iter()
        .any(|m| m["state"] == json!({"typing": true}));
    assert!(
        has_typing,
        "peer's snapshot should carry wa's updated state; got {snap}"
    );
}

/// `leavePresence` removes the member; the survivor's next snapshot shrinks
/// to 1. (The leaver is NOT broadcast to — `leave` drops them from the room
/// before the flush sends the snapshot — so only the survivor observes it.)
#[tokio::test]
async fn leave_shrinks_the_room() {
    let state = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let (token_a, email_a) = mint_user_for_db(&state, &db, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db, "b").await;
    allowlist(addr, &db, &[&email_a, &email_b]).await;

    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    auth(&mut wa, &token_a, &db).await;
    auth(&mut wb, &token_b, &db).await;

    send_json(&mut wa, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;

    send_json(&mut wb, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 2).await;
    drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;

    // wb leaves; wa's snapshot shrinks to 1.
    send_json(&mut wb, json!({"type": "leavePresence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    let snap = drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;
    assert_eq!(snap["members"].as_array().map(|a| a.len()), Some(1));
}

/// An abrupt TCP disconnect fires the `remove_conn` hook (ws.rs cleanup),
/// which evicts the member from every room; the survivor sees the room shrink.
/// Bound by a 3s timeout — the TCP close is detected quickly by tokio, but the
/// server's ws loop return + `remove_conn` + background flush task takes a few
/// ticks, so the outer wait must accommodate that polling cadence.
#[tokio::test]
async fn disconnect_evicts_the_member() {
    let state = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let (token_a, email_a) = mint_user_for_db(&state, &db, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db, "b").await;
    allowlist(addr, &db, &[&email_a, &email_b]).await;

    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    auth(&mut wa, &token_a, &db).await;
    auth(&mut wb, &token_b, &db).await;

    send_json(&mut wa, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;

    send_json(&mut wb, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    drain_until_snapshot(&mut wa, "doc:1", |n| n == 2).await;
    drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;

    // Drop wb's socket → server reads EOF → remove_conn → room dirty → the
    // interval=0 background flush task broadcasts the 1-member snapshot to wa.
    // An explicit `flush_once` here would race the background task (both drain
    // the same dirty set under one mutex), so we rely on the background task.
    drop(wb);
    // `drain_until_snapshot`'s own 2s timeout is the single bound on the
    // eviction wait — the prior outer 3s wrapper was unreachable dead code (the
    // inner helper times out first). The interval=0 flush task enqueues the
    // eviction snapshot within milliseconds of the TCP close, so 2s is ample.
    let snap = drain_until_snapshot(&mut wa, "doc:1", |n| n == 1).await;
    assert_eq!(snap["members"].as_array().map(|a| a.len()), Some(1));
}

/// A connection authed to db1 joining room "X" and a connection authed to db2
/// joining room "X" are in different room instances (presence is db-sharded):
/// neither ever sees the other. Asserted at the wire (each client's snapshot
/// for "X" has exactly 1 member) AND at the manager's `snapshot()` API (db1
/// room "X" and db2 room "X" each have 1 member, never 2), plus a bounded
/// negative check that wa receives no frame at all after wb's db2 join.
#[tokio::test]
async fn rooms_are_db_scoped() {
    let state: Arc<AppState> = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db1 = fresh_db(&state).await;
    let db2 = fresh_db(&state).await;
    // Identities are derived per-db so both dbs can mint without colliding
    // on the shared `rtdb_auth.users` UNIQUE constraints.
    let (token_a, email_a) = mint_user_for_db(&state, &db1, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db2, "b").await;
    allowlist(addr, &db1, &[&email_a, &email_b]).await;
    allowlist(addr, &db2, &[&email_a, &email_b]).await;

    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    auth(&mut wa, &token_a, &db1).await;
    auth(&mut wb, &token_b, &db2).await;

    // wa joins "X" in db1 → 1-member snapshot.
    send_json(&mut wa, json!({"type": "presence", "room": "X"})).await;
    state.realtime.presence.flush_once().await;
    let snap_a = drain_until_snapshot(&mut wa, "X", |n| n == 1).await;
    assert_eq!(snap_a["members"].as_array().map(|a| a.len()), Some(1));

    // wb joins "X" in db2 → 1-member snapshot (a DIFFERENT room instance).
    send_json(&mut wb, json!({"type": "presence", "room": "X"})).await;
    state.realtime.presence.flush_once().await;
    let snap_b = drain_until_snapshot(&mut wb, "X", |n| n == 1).await;
    assert_eq!(snap_b["members"].as_array().map(|a| a.len()), Some(1));

    // wb's join must not have leaked into db1's room "X": wa should not see a
    // 2-member snapshot. Verify at the db-sharded manager state...
    let db1_members = state.realtime.presence.snapshot(&db1, "X").await;
    let db2_members = state.realtime.presence.snapshot(&db2, "X").await;
    assert_eq!(db1_members.len(), 1, "db1 room X has only wa");
    assert_eq!(db2_members.len(), 1, "db2 room X has only wb");
    // ...and at the wire: wa receives NO frame within 150ms after wb's db2
    // join, because db1's room "X" was never dirtied by wb's action. 150ms is
    // a safe negative window: the flush task runs at interval=0 here (a genuine
    // cross-db leak would land within ~ms, not 150), the only periodic wire
    // frame is the 30s PING (well outside this window), and the dirty set is
    // keyed by (db, room) so a db2 join can never mark db1's room dirty.
    let leaked = tokio::time::timeout(Duration::from_millis(150), wa.next()).await;
    assert!(
        leaked.is_err(),
        "wa should not receive any frame from wb's db2 join, got {leaked:?}"
    );
}

// --- Smoke test (Task 7) — retained --------------------------------------

#[tokio::test]
async fn presence_manager_is_wired_and_disabled_by_default() {
    let state = common::test_state().await;
    let cfg = state.realtime.presence.config();
    assert!(!cfg.enabled, "test_config defaults presence off");
    // joining is rejected when disabled.
    let (t, _r) = tokio::sync::mpsc::unbounded_channel();
    let err = state
        .realtime
        .presence
        .join(
            "any",
            1,
            "room",
            None,
            rtdb_server::protocol::AuthedUser {
                kind: rtdb_server::protocol::UserKind::Machine,
                email: None,
                name: None,
                github_login: None,
                github_id: None,
            },
            t,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Forbidden);
}
