//! Transient, in-memory, connection-bound presence (ENH-015). Per-db "rooms"
//! of ephemeral per-session state. NOT committer-bound, NOT durable, NOT
//! persisted. Driven by `/sync` connection lifecycle; broadcasts over each
//! connection's existing `out_tx`.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc::UnboundedSender};

use crate::error::RtDbError;
use crate::metrics::Metrics;
use crate::protocol::{AuthedUser, PresenceMember, ServerMessage};
use crate::subs::ConnId;

/// Boot-only presence tuning (mirrors the relevant `Config` fields).
#[derive(Clone, Copy, Debug)]
pub struct PresenceConfig {
    pub enabled: bool,
    pub max_state_bytes: usize,
    pub max_room_size: usize,
    pub max_rooms_per_conn: usize,
    pub max_room_bytes: usize,
    pub broadcast_interval_ms: u64,
    pub update_limit_per_sec: u32,
    pub max_ttl_ms: u64,
}

impl PresenceConfig {
    /// Copy the presence fields off a `Config` (defined in Task 2).
    pub fn from_config(c: &crate::config::Config) -> Self {
        Self {
            enabled: c.presence_enabled,
            max_state_bytes: c.presence_max_state_bytes,
            max_room_size: c.presence_max_room_size,
            max_rooms_per_conn: c.presence_max_rooms_per_conn,
            max_room_bytes: c.presence_max_room_bytes,
            broadcast_interval_ms: c.presence_broadcast_interval_ms,
            update_limit_per_sec: c.presence_update_limit_per_sec,
            max_ttl_ms: c.presence_max_ttl_ms,
        }
    }
}

struct Session {
    user: AuthedUser,
    state: serde_json::Value,
    tx: UnboundedSender<ServerMessage>,
    #[allow(dead_code)] // read in Task 4's broadcast path
    joined_at: i64,
    updated_at: i64,
}

/// Per-db shard.
struct DbPresence {
    /// room -> (conn -> session)
    rooms: HashMap<String, HashMap<ConnId, Session>>,
    /// conn -> rooms it belongs to (O(1) eviction)
    conn_rooms: HashMap<ConnId, Vec<String>>,
    /// Per-conn `presenceState` rate window: (window_start_ms, count).
    update_window: HashMap<ConnId, (i64, u32)>,
}

impl DbPresence {
    fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            conn_rooms: HashMap::new(),
            update_window: HashMap::new(),
        }
    }
}

pub struct PresenceManager {
    dbs: Mutex<HashMap<String, Arc<Mutex<DbPresence>>>>,
    /// Rooms that need a broadcast on the next flush.
    dirty: Mutex<HashSet<(String, String)>>, // (db, room)
    metrics: Option<Arc<Metrics>>,
    config: PresenceConfig,
}

impl PresenceManager {
    pub fn new(metrics: Option<Arc<Metrics>>, config: PresenceConfig) -> Arc<Self> {
        Arc::new(Self {
            dbs: Mutex::new(HashMap::new()),
            dirty: Mutex::new(HashSet::new()),
            metrics,
            config,
        })
    }

    pub fn config(&self) -> PresenceConfig {
        self.config
    }

    async fn shard(&self, db: &str) -> Arc<Mutex<DbPresence>> {
        // Fast path: read existing shard. The outer lock is held only to clone
        // the shard `Arc` — never across a shard lock acquisition (mirrors
        // `subs.rs`'s `shard_insert`/`shard_get` discipline).
        {
            let dbs = self.dbs.lock().await;
            if let Some(s) = dbs.get(db) {
                return s.clone();
            }
        }
        let mut dbs = self.dbs.lock().await;
        dbs.entry(db.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(DbPresence::new())))
            .clone()
    }

    fn validate_room(room: &str, cfg: &PresenceConfig) -> Result<(), RtDbError> {
        if room.is_empty() || room.len() > cfg.max_room_bytes {
            return Err(RtDbError::bad_request("invalid room name"));
        }
        Ok(())
    }

    fn validate_state(state: &serde_json::Value, cfg: &PresenceConfig) -> Result<(), RtDbError> {
        // Cheap byte cap on the serialized blob.
        if serde_json::to_vec(state)
            .map(|v| v.len())
            .unwrap_or(usize::MAX)
            > cfg.max_state_bytes
        {
            return Err(RtDbError::bad_request("presence state too large"));
        }
        Ok(())
    }

    /// Join `room` as `conn`: register present + subscribe. Idempotent
    /// (re-join updates `state`). `tx` is the connection's outbound channel.
    pub async fn join(
        &self,
        db: &str,
        conn: ConnId,
        room: &str,
        state: Option<serde_json::Value>,
        user: AuthedUser,
        tx: UnboundedSender<ServerMessage>,
    ) -> Result<(), RtDbError> {
        if !self.config.enabled {
            return Err(RtDbError::forbidden("presence not enabled"));
        }
        Self::validate_room(room, &self.config)?;
        let st = state.unwrap_or(serde_json::Value::Null);
        Self::validate_state(&st, &self.config)?;
        let shard = self.shard(db).await;
        let mut p = shard.lock().await;
        // rooms-per-conn cap (only when newly joining this room)
        let already = p
            .conn_rooms
            .get(&conn)
            .map(|v| v.contains(&room.to_string()))
            .unwrap_or(false);
        if !already
            && p.conn_rooms.get(&conn).map(|v| v.len()).unwrap_or(0)
                >= self.config.max_rooms_per_conn
        {
            return Err(RtDbError::forbidden("too many rooms for this connection"));
        }
        let room_map = p.rooms.entry(room.to_string()).or_insert_with(HashMap::new);
        if room_map.len() >= self.config.max_room_size && !room_map.contains_key(&conn) {
            return Err(RtDbError::forbidden("room is full"));
        }
        let now = crate::db::now_ms();
        room_map.insert(
            conn,
            Session {
                user,
                state: st,
                tx,
                joined_at: now,
                updated_at: now,
            },
        );
        // index (only on a genuine new join — re-join must not append a
        // duplicate, or `max_rooms_per_conn` accounting drifts on each re-join)
        if !already {
            p.conn_rooms.entry(conn).or_default().push(room.to_string());
        }
        drop(p);
        self.mark_dirty(db, room).await;
        Ok(())
    }

    pub async fn update_state(
        &self,
        db: &str,
        conn: ConnId,
        room: &str,
        state: serde_json::Value,
    ) -> Result<(), RtDbError> {
        if !self.config.enabled {
            return Err(RtDbError::forbidden("presence not enabled"));
        }
        Self::validate_state(&state, &self.config)?;
        let shard = self.shard(db).await;
        let mut p = shard.lock().await;
        // per-conn update rate limit (membership required)
        if !p.rooms.get(room).is_some_and(|m| m.contains_key(&conn)) {
            return Err(RtDbError::bad_request("not present in room"));
        }
        // Per-conn 1-second tumbling window on `presenceState` updates. A
        // not-present conn already got `BadRequest` above — rate-limiting is
        // only for members, never a denial to a connection that can't clear
        // membership.
        let limit = self.config.update_limit_per_sec;
        let now = crate::db::now_ms();
        let win = p.update_window.entry(conn).or_insert((now, 0));
        if now - win.0 >= 1000 {
            *win = (now, 0);
        }
        win.1 += 1;
        if win.1 > limit {
            // Floor retry-after to the remainder of the 1s window, min 1.
            let retry = (((win.0 + 1000) - now) / 1000).max(0) as u32;
            return Err(RtDbError::rate_limited(retry.max(1)));
        }
        let Some(room_map) = p.rooms.get_mut(room) else {
            return Err(RtDbError::bad_request("not present in room"));
        };
        let Some(sess) = room_map.get_mut(&conn) else {
            return Err(RtDbError::bad_request("not present in room"));
        };
        sess.state = state;
        sess.updated_at = now;
        drop(p);
        self.mark_dirty(db, room).await;
        Ok(())
    }

    /// Leave a room; no-op if not joined. Never errors.
    pub async fn leave(&self, db: &str, conn: ConnId, room: &str) {
        let shard = self.shard(db).await;
        let mut p = shard.lock().await;
        let mut changed = false;
        if let Some(room_map) = p.rooms.get_mut(room) {
            if room_map.remove(&conn).is_some() {
                changed = true;
            }
            if room_map.is_empty() {
                p.rooms.remove(room); // prune empty room
            }
        }
        if let Some(rooms) = p.conn_rooms.get_mut(&conn) {
            rooms.retain(|r| r != room);
            if rooms.is_empty() {
                p.conn_rooms.remove(&conn);
            }
        }
        drop(p);
        if changed {
            self.mark_dirty(db, room).await;
        }
    }

    /// Evict every room a connection was in (disconnect). Idempotent.
    pub async fn remove_conn(&self, db: &str, conn: ConnId) {
        let shard = self.shard(db).await;
        let mut p = shard.lock().await;
        let rooms = p.conn_rooms.remove(&conn).unwrap_or_default();
        p.update_window.remove(&conn);
        let mut dirty: Vec<String> = Vec::new();
        for room in &rooms {
            if let Some(room_map) = p.rooms.get_mut(room) {
                room_map.remove(&conn);
                if room_map.is_empty() {
                    p.rooms.remove(room);
                }
            }
            dirty.push(room.clone());
        }
        drop(p);
        for room in dirty {
            self.mark_dirty(db, &room).await;
        }
    }

    /// Drop an entire db's presence (delete-db). Idempotent.
    pub async fn drop_db(&self, db: &str) {
        let mut dbs = self.dbs.lock().await;
        dbs.remove(db);
        let mut dirty = self.dirty.lock().await;
        dirty.retain(|(d, _)| d != db);
    }

    async fn mark_dirty(&self, db: &str, room: &str) {
        self.dirty
            .lock()
            .await
            .insert((db.to_string(), room.to_string()));
    }

    /// Read-only member list for a room (test helper + Task 4 broadcast input).
    pub async fn snapshot(&self, db: &str, room: &str) -> Vec<PresenceMember> {
        let shard = self.shard(db).await;
        let p = shard.lock().await;
        p.rooms
            .get(room)
            .map(|m| {
                m.iter()
                    .map(|(conn, s)| PresenceMember {
                        connection_id: conn.to_string(),
                        user: s.user.clone(),
                        state: s.state.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Live counts across all shards: `(distinct_rooms, total_sessions)`.
    /// Computed at snapshot time for the `rtdb_presence_rooms` /
    /// `rtdb_presence_sessions` gauges — not atomic counters on `Metrics`, since
    /// membership is per-shard `HashMap` state that can't be tallied at increment
    /// time. Same lock discipline as `shard()`: clone each shard `Arc` under the
    /// brief outer lock, release it, then lock each shard individually (never
    /// hold the outer lock across a shard lock acquisition).
    pub async fn counts(&self) -> (usize, usize) {
        let shards: Vec<Arc<Mutex<DbPresence>>> = {
            let dbs = self.dbs.lock().await;
            dbs.values().cloned().collect()
        };
        let mut rooms = 0usize;
        let mut sessions = 0usize;
        for shard in shards {
            let p = shard.lock().await;
            rooms += p.rooms.len();
            sessions += p.rooms.values().map(|m| m.len()).sum::<usize>();
        }
        (rooms, sessions)
    }

    /// Broadcast one `presenceSnapshot` per dirty room to every member of that
    /// room, then clear the dirty set. Called by the periodic flush task and by
    /// tests. Cheap when there are no dirty rooms.
    ///
    /// Returns `true` if at least one dirty room had members and was broadcast
    /// to (i.e. real work went out), `false` otherwise (no dirty rooms, or all
    /// dirty rooms were empty-between-mark-and-flush). The flush task uses this
    /// to distinguish busy vs idle so the `interval_ms == 0` path can sleep
    /// instead of busy-yielding when there is nothing to do.
    pub async fn flush_once(&self) -> bool {
        let dirty: Vec<(String, String)> = {
            let mut g = self.dirty.lock().await;
            g.drain().collect()
        };
        if dirty.is_empty() {
            return false;
        }
        let mut did_work = false;
        for (db, room) in dirty {
            let shard = self.shard(&db).await;
            let members: Vec<PresenceMember> = {
                let p = shard.lock().await;
                p.rooms
                    .get(&room)
                    .map(|m| {
                        m.iter()
                            .map(|(conn, s)| PresenceMember {
                                connection_id: conn.to_string(),
                                user: s.user.clone(),
                                state: s.state.clone(),
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            if members.is_empty() {
                continue; // room emptied between mark_dirty and flush
            }
            did_work = true;
            // Capture the recipient tx list under the lock (the room may still
            // be the source of truth), then send outside the lock.
            let recipients: Vec<UnboundedSender<ServerMessage>> = {
                let p = shard.lock().await;
                p.rooms
                    .get(&room)
                    .map(|m| m.values().map(|s| s.tx.clone()).collect())
                    .unwrap_or_default()
            };
            if let Some(metrics) = &self.metrics {
                metrics.record_presence_broadcast();
            }
            for tx in recipients {
                let _ = tx.send(ServerMessage::PresenceSnapshot {
                    room: room.clone(),
                    members: members.clone(),
                });
            }
        }
        did_work
    }

    /// Periodic driver. `interval_ms == 0` means flush as fast as possible
    /// (no coalescing sleep) — used by indicator-only deployments. Runs for
    /// the process lifetime. When `interval_ms == 0`, the loop cooperatively
    /// yields while there is work to flush and sleeps 1ms when idle so an idle
    /// room does not pin a worker core.
    pub fn run_flush_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        let interval_ms = this.config.broadcast_interval_ms;
        tokio::spawn(async move {
            let mut ticker = if interval_ms == 0 {
                None
            } else {
                Some(tokio::time::interval(tokio::time::Duration::from_millis(
                    interval_ms,
                )))
            };
            loop {
                if let Some(t) = ticker.as_mut() {
                    t.tick().await;
                    let _ = this.flush_once().await;
                } else {
                    // interval == 0: cooperative when busy, 1ms sleep when idle
                    if this.flush_once().await {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
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
        }
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
    fn tx() -> (
        mpsc::UnboundedSender<ServerMessage>,
        mpsc::UnboundedReceiver<ServerMessage>,
    ) {
        mpsc::unbounded_channel()
    }
    fn mgr() -> Arc<PresenceManager> {
        PresenceManager::new(None, cfg())
    }

    #[tokio::test]
    async fn join_then_snapshot_lists_the_member() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap();
        m.update_state("db", 1, "room", serde_json::json!({"x": 5}))
            .await
            .unwrap();
        let members = m.snapshot("db", "room").await;
        assert_eq!(members[0].state, serde_json::json!({"x": 5}));
    }

    #[tokio::test]
    async fn update_state_when_not_joined_errors() {
        let m = mgr();
        let err = m
            .update_state("db", 1, "room", serde_json::json!({}))
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn leave_removes_the_member() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
        m.join("db", 1, "a", None, user("a@b.com"), t1)
            .await
            .unwrap();
        m.join(
            "db",
            1,
            "b",
            None,
            user("a@b.com"),
            mpsc::unbounded_channel().0,
        )
        .await
        .unwrap();
        m.join("db", 2, "a", None, user("b@b.com"), t2)
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
        let m = PresenceManager::new(None, c);
        let (t, _r) = tx();
        // Join "a" once, then re-join "a" 5 more times.
        m.join("db", 1, "a", None, user("a@b.com"), t.clone())
            .await
            .unwrap();
        for _ in 0..5 {
            m.join("db", 1, "a", None, user("a@b.com"), t.clone())
                .await
                .unwrap();
        }
        // The conn is still in exactly one room — a new room must be accepted.
        m.join("db", 1, "b", None, user("a@b.com"), t.clone())
            .await
            .unwrap();
        m.join("db", 1, "c", None, user("a@b.com"), t.clone())
            .await
            .unwrap();
        m.join("db", 1, "d", None, user("a@b.com"), t.clone())
            .await
            .unwrap();
        // Now at the cap (4 rooms: a, b, c, d). The next NEW room must reject.
        let err = m
            .join("db", 1, "e", None, user("a@b.com"), t)
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
        )
        .await
        .unwrap();
        m.join("db", 2, "room", None, user("b@b.com"), t2)
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
        m.join("db", 1, "room", None, user("a@b.com"), t1)
            .await
            .unwrap();
        m.join("db", 2, "room", None, user("b@b.com"), t2)
            .await
            .unwrap();
        // three rapid state updates by conn 1 -> conn 2 should see ONE snapshot after flush
        m.update_state("db", 1, "room", serde_json::json!({"n":1}))
            .await
            .unwrap();
        m.update_state("db", 1, "room", serde_json::json!({"n":2}))
            .await
            .unwrap();
        m.update_state("db", 1, "room", serde_json::json!({"n":3}))
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap();
        m.flush_once().await;
        // a second flush with no new changes sends nothing
        // (assert by joining a 2nd conn that drains its own join snapshot separately)
        let (t2, mut r2) = tx();
        m.join("db", 2, "room", None, user("b@b.com"), t2)
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
        let m = PresenceManager::new(None, c);
        let (t, _r) = tx();
        let big = serde_json::json!({"big":"0123456789"});
        let err = m
            .join("db", 1, "room", Some(big), user("a@b.com"), t)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn empty_room_name_is_rejected() {
        let m = mgr();
        let (t, _r) = tx();
        let err = m
            .join("db", 1, "", None, user("a@b.com"), t)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn room_full_is_rejected_forbidden() {
        let mut c = cfg();
        c.max_room_size = 1;
        let m = PresenceManager::new(None, c);
        let (t1, _r1) = tx();
        let (t2, _r2) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t1)
            .await
            .unwrap();
        let err = m
            .join("db", 2, "room", None, user("b@b.com"), t2)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn too_many_rooms_per_conn_is_rejected_forbidden() {
        let mut c = cfg();
        c.max_rooms_per_conn = 1;
        let m = PresenceManager::new(None, c);
        let (t, _r) = tx();
        m.join(
            "db",
            1,
            "a",
            None,
            user("a@b.com"),
            mpsc::unbounded_channel().0,
        )
        .await
        .unwrap();
        let err = m
            .join("db", 1, "b", None, user("a@b.com"), t)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn presence_state_rate_limit_denies_after_cap() {
        let mut c = cfg();
        c.update_limit_per_sec = 2;
        let m = PresenceManager::new(None, c);
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap();
        assert!(
            m.update_state("db", 1, "room", serde_json::json!({"n":1}))
                .await
                .is_ok()
        );
        assert!(
            m.update_state("db", 1, "room", serde_json::json!({"n":2}))
                .await
                .is_ok()
        );
        let err = m
            .update_state("db", 1, "room", serde_json::json!({"n":3}))
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::RateLimited);
    }

    #[tokio::test]
    async fn feature_disabled_rejects_join_forbidden() {
        let mut c = cfg();
        c.enabled = false;
        let m = PresenceManager::new(None, c);
        let (t, _r) = tx();
        let err = m
            .join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
    }
}
