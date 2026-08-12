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

/// ENH-022 Stage 3: one NOTIFY payload per `(db, room)` snapshot published on
/// the `rtdb_presence` channel. `instance_id` is the origin replica's id
/// (self-dedupe); `members` is the origin's FULL local snapshot for that room
/// (a delta would need reconciliation; a full snapshot is idempotent and
/// small — rooms are capped at `max_room_size`). `camelCase` on the wire for
/// consistency with the Stage 2 op-feed payload. Defined here (not in
/// `notify.rs`) so the presence listener can import it without a circular
/// dependency on presence's types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PresenceNotifyPayload {
    pub instance_id: String,
    pub db: String,
    pub room: String,
    pub members: Vec<PresenceMember>,
}

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
    /// ENH-022 Stage 3: cadence (ms) at which each instance republishes its
    /// full per-room membership to peers via `pg_notify('rtdb_presence', …)`.
    /// Only consulted when `multi_instance` is on.
    pub beat_interval_ms: u64,
    /// ENH-022 Stage 3: a peer whose last beat is older than this (ms) is
    /// evicted from the local peer shadow map. Default 3 × beat interval.
    pub beat_timeout_ms: u64,
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
            beat_interval_ms: c.presence_beat_interval_ms,
            beat_timeout_ms: c.presence_beat_timeout_ms,
        }
    }
}

struct Session {
    user: AuthedUser,
    state: serde_json::Value,
    tx: UnboundedSender<ServerMessage>,
    updated_at: i64,
    /// Absolute epoch-ms at which `state` should be cleared to null by
    /// `expire_once`. `None` = permanent (no ttl armed). Joins never arm it
    /// (ttl rides on `presenceState` only); only `update_state` sets/clears it.
    expires_at: Option<i64>,
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

/// ENH-022 Stage 3: a snapshot of one room's membership as reported by a peer
/// instance via `pg_notify('rtdb_presence', …)`. Stored in the local peer
/// shadow map keyed by origin `instance_id`; never sent to local clients
/// verbatim (the union built by `flush_once` is what clients receive).
/// `last_beat` is refreshed on every notification (beat or snapshot) and is
/// the basis for the eviction sweep — a peer that stops beating is assumed
/// dead and its entries are dropped after `beat_timeout_ms`.
#[derive(Clone, Debug)]
struct PeerSnapshot {
    members: Vec<PresenceMember>,
    last_beat: i64,
}

pub struct PresenceManager {
    dbs: Mutex<HashMap<String, Arc<Mutex<DbPresence>>>>,
    /// Rooms that need a broadcast on the next flush.
    dirty: Mutex<HashSet<(String, String)>>, // (db, room)
    metrics: Option<Arc<Metrics>>,
    config: PresenceConfig,
    /// ENH-022 Stage 3: cross-instance presence gossip. When `multi_instance`
    /// is on, every dirty room publishes its local snapshot to peers over
    /// `pg_notify('rtdb_presence', …)` and a per-process LISTEN task feeds
    /// peer notifications back into this map. Keyed `(db, room) → instance_id
    /// → PeerSnapshot`. Empty and unused in single-instance mode.
    peers: Mutex<HashMap<(String, String), HashMap<String, PeerSnapshot>>>,
    /// True when `RTDB_MULTI_INSTANCE=true`. Gates every gossip publish,
    /// ingest, union, and sweep so a single-instance deploy is byte-identical
    /// to pre-Stage-3.
    multi_instance: bool,
    /// This process's replica id. Tags presence NOTIFY payloads so a receiver
    /// can skip its own self-echoed notifications (Postgres delivers a
    /// session's own `pg_notify` back to it).
    instance_id: String,
    /// Cloned `PgPool` used for best-effort `pg_notify` publishes from the
    /// mutation ops and the beat task. Held on the manager so every op
    /// site can publish without threading the pool through each call.
    pool: Option<sqlx::PgPool>,
}

impl PresenceManager {
    /// Construct a presence manager. The last three args wire ENH-022 Stage 3
    /// cross-instance gossip: `multi_instance` gates the whole layer (false ⇒
    /// single-instance byte-identical behavior, no NOTIFY, no peer map);
    /// `instance_id` tags NOTIFY payloads for self-dedupe; `pool` is used for
    /// the best-effort `pg_notify` publishes (None when multi_instance is
    /// false — the caller passes a real pool only when gossip is active).
    pub fn new(
        metrics: Option<Arc<Metrics>>,
        config: PresenceConfig,
        multi_instance: bool,
        instance_id: String,
        pool: Option<sqlx::PgPool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            dbs: Mutex::new(HashMap::new()),
            dirty: Mutex::new(HashSet::new()),
            metrics,
            config,
            peers: Mutex::new(HashMap::new()),
            multi_instance,
            instance_id,
            pool,
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
                updated_at: now,
                expires_at: None,
            },
        );
        // index (only on a genuine new join — re-join must not append a
        // duplicate, or `max_rooms_per_conn` accounting drifts on each re-join)
        if !already {
            p.conn_rooms.entry(conn).or_default().push(room.to_string());
        }
        drop(p);
        self.mark_dirty(db, room).await;
        self.gossip_publish(db, room).await;
        Ok(())
    }

    pub async fn update_state(
        &self,
        db: &str,
        conn: ConnId,
        room: &str,
        state: serde_json::Value,
        ttl_ms: Option<u64>,
    ) -> Result<(), RtDbError> {
        if !self.config.enabled {
            return Err(RtDbError::forbidden("presence not enabled"));
        }
        Self::validate_state(&state, &self.config)?;
        // ttl validation (early reject, alongside the state-size check, before
        // the rate-limit window is touched): 0 is nonsensical, over-cap is
        // rejected — never clamped (presence's "no silent clamping" rule).
        if let Some(t) = ttl_ms {
            if t == 0 {
                return Err(RtDbError::bad_request("ttl must be positive"));
            }
            if t > self.config.max_ttl_ms {
                return Err(RtDbError::bad_request("ttl exceeds maximum"));
            }
        }
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
        sess.expires_at = ttl_ms.map(|t| now + t as i64); // arm or clear
        drop(p);
        self.mark_dirty(db, room).await;
        self.gossip_publish(db, room).await;
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
            self.gossip_publish(db, room).await;
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
            self.gossip_publish(db, &room).await;
        }
    }

    /// Drop an entire db's presence (delete-db). Idempotent. Also drops any
    /// peer-shadow entries for this db (ENH-022 Stage 3) so a recreated db of
    /// the same name doesn't inherit stale peer members.
    pub async fn drop_db(&self, db: &str) {
        let mut dbs = self.dbs.lock().await;
        dbs.remove(db);
        let mut dirty = self.dirty.lock().await;
        dirty.retain(|(d, _)| d != db);
        let mut peers = self.peers.lock().await;
        peers.retain(|(d, _), _| d != db);
    }

    async fn mark_dirty(&self, db: &str, room: &str) {
        self.dirty
            .lock()
            .await
            .insert((db.to_string(), room.to_string()));
    }

    /// ENH-022 Stage 3: snapshot the LOCAL members of `(db, room)` for a
    /// gossip publish. This is the same projection `flush_once` uses for the
    /// local half of the union — factored out so the publish and the broadcast
    /// never drift apart. Returns `None` when the room does not exist or has
    /// no local members (a peer would store an empty snapshot and there would
    /// be nothing to merge, so the caller skips the NOTIFY).
    async fn local_snapshot(&self, db: &str, room: &str) -> Option<Vec<PresenceMember>> {
        let shard = self.shard(db).await;
        let p = shard.lock().await;
        p.rooms.get(room).map(|m| {
            m.iter()
                .map(|(conn, s)| PresenceMember {
                    connection_id: conn.to_string(),
                    user: s.user.clone(),
                    state: s.state.clone(),
                })
                .collect()
        })
    }

    /// ENH-022 Stage 3: publish this instance's local snapshot for `(db, room)`
    /// to peer replicas via `pg_notify('rtdb_presence', …)`. Called from every
    /// mutation op (join/update_state/leave/remove_conn) and from the beat task
    /// — wherever a room goes dirty in multi-instance mode. Best-effort like
    /// the Stage 2 op-feed NOTIFY: a publish failure logs `warn!` and
    /// continues, because the next beat resyncs. Self-publish is fine (the
    /// receiver self-dedupes by instance_id). Skipped entirely when
    /// `multi_instance` is off (single-instance byte-identical).
    ///
    /// Gossip invariant: this method performs NO document write, NO committer
    /// interaction. Presence was never committer-bound; gossip adds only a
    /// NOTIFY publish and (on the receive side) an in-memory shadow write.
    async fn gossip_publish(&self, db: &str, room: &str) {
        if !self.multi_instance {
            return;
        }
        // local_snapshot returns None when the room does not exist locally
        // (e.g. after `leave` pruned an empty room). Publish an empty snapshot
        // in that case so peers see the room emptied — otherwise a stale peer
        // entry would linger until beat_timeout.
        let members = self.local_snapshot(db, room).await.unwrap_or_default();
        let Some(pool) = self.pool.as_ref() else {
            // multi_instance on without a pool is a wiring bug — surface it.
            tracing::error!(
                db = %db,
                room = %room,
                "presence gossip: multi_instance on but no pool wired; skipping publish"
            );
            return;
        };
        let payload = PresenceNotifyPayload {
            instance_id: self.instance_id.clone(),
            db: db.to_string(),
            room: room.to_string(),
            members,
        };
        let payload_json = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    db = %db, room = %room, error = %e,
                    "presence gossip: failed to serialize payload; skipping"
                );
                return;
            }
        };
        if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)")
            .bind(crate::notify::PRESENCE_CHANNEL)
            .bind(&payload_json)
            .execute(pool)
            .await
        {
            tracing::warn!(
                db = %db, room = %room, error = %e,
                "presence gossip: pg_notify failed (best-effort; next beat resyncs)"
            );
        }
    }

    /// ENH-022 Stage 3: ingest a peer-reported snapshot into the local shadow
    /// map, refresh the peer's `last_beat`, and mark the room dirty so the
    /// next flush broadcasts the updated union to local members. Called by the
    /// dedicated `rtdb_presence` LISTEN task; self-notifications are skipped
    /// by the caller (the listener) before this is invoked. Performs NO write
    /// and NO committer interaction — peer state lives only in the shadow map.
    pub async fn ingest_peer_snapshot(
        &self,
        instance_id: &str,
        db: &str,
        room: &str,
        members: Vec<PresenceMember>,
    ) {
        debug_assert!(
            self.multi_instance,
            "ingest_peer_snapshot called in single-instance mode (listener should not be spawned)"
        );
        let now = crate::db::now_ms();
        let mut peers = self.peers.lock().await;
        peers
            .entry((db.to_string(), room.to_string()))
            .or_default()
            .insert(
                instance_id.to_string(),
                PeerSnapshot {
                    members,
                    last_beat: now,
                },
            );
        drop(peers);
        // Mark dirty so the next flush broadcasts the union including this
        // peer's members to LOCAL subscribers. mark_dirty takes only the
        // dirty lock; safe to call after releasing the peer lock.
        self.mark_dirty(db, room).await;
    }

    /// ENH-022 Stage 3: evict every `PeerSnapshot` whose `last_beat` is older
    /// than `beat_timeout_ms`, marking each affected room dirty so the next
    /// flush broadcasts the union WITHOUT the dead peer's members. This is the
    /// "killing A evicts A's members within beat_timeout" contract: a dead
    /// instance stops beating, and within the timeout its shadow entries
    /// expire and disappear from peers' unions. Called on the flush task's
    /// tick in multi-instance mode. Also exposed as a `pub` test seam so the
    /// cross-replica eviction test can drive eviction deterministically
    /// without waiting out a real beat timeout.
    pub async fn expire_peers(&self) {
        if !self.multi_instance {
            return;
        }
        let now = crate::db::now_ms();
        let cutoff = now.saturating_sub(self.config.beat_timeout_ms as i64);
        let mut dirty_rooms: Vec<(String, String)> = Vec::new();
        {
            let mut peers = self.peers.lock().await;
            // Retain per-room maps that still have at least one live peer;
            // drop rooms whose every peer has expired.
            peers.retain(|(db, room), by_instance| {
                let before = by_instance.len();
                by_instance.retain(|_id, snap| snap.last_beat >= cutoff);
                if by_instance.len() != before {
                    dirty_rooms.push((db.clone(), room.clone()));
                }
                !by_instance.is_empty()
            });
        }
        for (db, room) in &dirty_rooms {
            self.mark_dirty(db, room).await;
        }
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

    /// Clear every session whose armed TTL has elapsed: set its `state` to
    /// `null`, clear its expiry, mark each affected room dirty, and count one
    /// expiry per session. Called at the top of every flush-tick (before
    /// `flush_once`) so the coalesced broadcast picks up the null states.
    /// Returns `true` if any session expired (so the immediate-mode flush loop
    /// can yield instead of sleeping). Membership is never removed here — only
    /// `leavePresence`/disconnect do that.
    pub async fn expire_once(&self) -> bool {
        let now = crate::db::now_ms();
        let shards: Vec<(String, Arc<Mutex<DbPresence>>)> = {
            let dbs = self.dbs.lock().await;
            dbs.iter().map(|(db, s)| (db.clone(), s.clone())).collect()
        };
        let mut expired_any = false;
        for (db, shard) in shards {
            let mut dirty_rooms: Vec<String> = Vec::new();
            {
                let mut p = shard.lock().await;
                for (room, members) in p.rooms.iter_mut() {
                    let mut room_expired = false;
                    for sess in members.values_mut() {
                        if let Some(exp) = sess.expires_at
                            && exp <= now
                        {
                            sess.state = serde_json::Value::Null;
                            sess.expires_at = None;
                            room_expired = true;
                            expired_any = true;
                            if let Some(m) = &self.metrics {
                                m.record_presence_ttl_expiry();
                            }
                        }
                    }
                    if room_expired {
                        dirty_rooms.push(room.clone());
                    }
                }
            }
            // mark_dirty takes only the dirty lock — never hold the shard lock across it.
            for room in &dirty_rooms {
                self.mark_dirty(&db, room).await;
                self.gossip_publish(&db, room).await;
            }
        }
        expired_any
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
            let local_members: Vec<PresenceMember> = {
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
            // ENH-022 Stage 3: in multi-instance mode, the broadcast is the
            // UNION of local members ++ every peer replica's reported members
            // for this room. Single-instance mode (multi_instance == false)
            // skips this entirely — `members == local_members` and the wire
            // output is byte-identical to pre-Stage-3.
            //
            // Connection-id namespacing: peer-originated members get
            // `connection_id = "{origin_instance_id}:{conn_id}"` so two
            // replicas that both have a local conn "1" cannot collide (a
            // client that identifies "me" by connection_id would otherwise
            // mistake a peer's member for its own). Local members keep their
            // plain per-process conn-id string. Only active in multi-instance
            // mode — opaque to clients (they treat connection_id as a string).
            let members: Vec<PresenceMember> = if self.multi_instance {
                let peers_for_room: Vec<(String, Vec<PresenceMember>)> = {
                    let peers = self.peers.lock().await;
                    peers
                        .get(&(db.clone(), room.clone()))
                        .map(|by_instance| {
                            by_instance
                                .iter()
                                .map(|(id, snap)| (id.clone(), snap.members.clone()))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                };
                self.build_union(local_members, peers_for_room)
            } else {
                // Single-instance: no peers, no namespacing — byte-identical.
                local_members
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

    /// ENH-022 Stage 3: build the union of local members ++ peer members for
    /// the multi-instance broadcast. Local members always win the cap (a local
    /// member is never dropped in favor of a remote one — that would make a
    /// client's own presence invisible to itself). Peer members are
    /// namespaced (`"{instance_id}:{conn_id}"`) and deduped by that
    /// namespaced id. The union is capped at `max_room_size` total; peer
    /// members beyond the cap are dropped (over-approximate freely,
    /// never under-approximate — a dropped remote member reappears on
    /// the next beat, which is the resync contract).
    ///
    /// Determinism: locals first (in their iteration order), then peers
    /// sorted by (instance_id, connection_id) so two flushes of the
    /// same state produce identical wire bytes.
    fn build_union(
        &self,
        local_members: Vec<PresenceMember>,
        peers_for_room: Vec<(String, Vec<PresenceMember>)>,
    ) -> Vec<PresenceMember> {
        let cap = self.config.max_room_size;
        let mut seen: std::collections::HashSet<String> =
            std::collections::HashSet::with_capacity(local_members.len());
        let mut union: Vec<PresenceMember> = Vec::with_capacity(local_members.len());
        for m in local_members.into_iter() {
            if seen.insert(m.connection_id.clone()) {
                union.push(m);
            }
            if union.len() >= cap {
                return union;
            }
        }
        // Collect + namespace peer members, then sort by namespaced id so the
        // order is stable across flushes for the same peer state.
        let mut peer_members: Vec<PresenceMember> = Vec::new();
        for (instance_id, members) in peers_for_room {
            for mut m in members {
                m.connection_id = format!("{instance_id}:{}", m.connection_id);
                peer_members.push(m);
            }
        }
        peer_members.sort_by(|a, b| a.connection_id.cmp(&b.connection_id));
        for m in peer_members {
            if seen.insert(m.connection_id.clone()) {
                union.push(m);
            }
            if union.len() >= cap {
                break;
            }
        }
        union
    }

    /// Periodic driver. `interval_ms == 0` means flush as fast as possible
    /// (no coalescing sleep) — used by indicator-only deployments. Runs for
    /// the process lifetime. When `interval_ms == 0`, the loop cooperatively
    /// yields while there is work to flush and sleeps 1ms when idle so an idle
    /// room does not pin a worker core.
    ///
    /// ENH-022 Stage 3: in multi-instance mode, the same task also (a) runs
    /// `expire_peers` on every tick so a dead replica's shadow entries are
    /// evicted within `beat_timeout_ms`, and (b) fires a liveness beat every
    /// `beat_interval_ms` that republishes every populated room's local
    /// snapshot to peers (the resync path for a peer that missed an
    /// incremental NOTIFY). Both are no-ops in single-instance mode.
    pub fn run_flush_task(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        let interval_ms = this.config.broadcast_interval_ms;
        let multi_instance = this.multi_instance;
        let beat_interval_ms = this.config.beat_interval_ms;
        tokio::spawn(async move {
            let mut ticker = if interval_ms == 0 {
                None
            } else {
                Some(tokio::time::interval(tokio::time::Duration::from_millis(
                    interval_ms,
                )))
            };
            // Beat ticker: fires every beat_interval_ms in multi-instance mode.
            // Missed ticks are coalesced (`MissedTickBehavior::Delay` is the
            // default and the safe choice — a beat is a resync, not a deadline).
            let mut beat_ticker = if multi_instance {
                Some(tokio::time::interval(tokio::time::Duration::from_millis(
                    beat_interval_ms,
                )))
            } else {
                None
            };
            loop {
                if let Some(t) = ticker.as_mut() {
                    t.tick().await;
                    if multi_instance {
                        this.expire_peers().await;
                    }
                    let _ = this.expire_once().await;
                    let _ = this.flush_once().await;
                } else {
                    // interval == 0: cooperative when busy, 1ms sleep when idle
                    let expired = this.expire_once().await;
                    if this.flush_once().await || expired {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                }
                // ENH-022 Stage 3 liveness beat. Republish every populated
                // room's local snapshot so a peer that missed incremental
                // NOTIFYs resyncs on the next beat. Bounded: snapshot each
                // shard under its lock, release, then publish (never hold a
                // shard lock across the pg_notify round-trip).
                if let Some(bt) = beat_ticker.as_mut() {
                    if bt.tick().await.elapsed().is_zero() {
                        // First tick fires immediately on construction; skip
                        // it so we don't beat before any local state exists.
                        continue;
                    }
                    this.beat_all_rooms().await;
                }
            }
        })
    }

    /// ENH-022 Stage 3: republish every populated room's local snapshot to
    /// peers. Called by the beat ticker in `run_flush_task`. Gated on
    /// `multi_instance` at the call site (the beat ticker only exists then),
    /// but re-checked here for safety. Best-effort: a publish failure is
    /// logged in `gossip_publish` and the next beat retries.
    async fn beat_all_rooms(&self) {
        if !self.multi_instance {
            return;
        }
        // Collect (db, room) for every populated local room under the brief
        // outer lock, then publish each outside the lock. A room that becomes
        // empty between collect and publish publishes an empty snapshot,
        // which is correct (peers see the room emptied).
        let rooms: Vec<(String, String)> = {
            let dbs = self.dbs.lock().await;
            let mut out = Vec::new();
            for (db, shard) in dbs.iter() {
                let p = shard.lock().await;
                for room in p.rooms.keys() {
                    out.push((db.clone(), room.clone()));
                }
            }
            out
        };
        for (db, room) in rooms {
            self.gossip_publish(&db, &room).await;
        }
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
        m.update_state("db", 1, "room", serde_json::json!({"x": 5}), None)
            .await
            .unwrap();
        let members = m.snapshot("db", "room").await;
        assert_eq!(members[0].state, serde_json::json!({"x": 5}));
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
        let m = PresenceManager::new(None, c, false, "test".to_string(), None);
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
        let m = PresenceManager::new(None, c, false, "test".to_string(), None);
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
        let m = PresenceManager::new(None, c, false, "test".to_string(), None);
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
        let m = PresenceManager::new(None, c, false, "test".to_string(), None);
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
        let m = PresenceManager::new(None, c, false, "test".to_string(), None);
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
            .join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::Forbidden);
    }

    #[tokio::test]
    async fn update_state_arms_ttl_that_expires_to_null_not_remove() {
        let m = mgr();
        let (t1, _r1) = tx();
        let (t2, mut r2) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t1)
            .await
            .unwrap();
        m.join("db", 2, "room", None, user("b@b.com"), t2)
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
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
        m.join("db", 1, "room", None, user("a@b.com"), t)
            .await
            .unwrap();
        assert!(!m.expire_once().await);
    }
}
