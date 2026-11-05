//! Presence model types (QA-003 split): the wire payload, per-deploy
//! config, per-session and per-db room state, the cross-instance peer
//! snapshot, the inspection view, and the member-diff helper the manager's
//! delta broadcasts are built from. The manager itself lives in
//! `super::mod`'s `PresenceManager`.

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::{AuthedUser, PresenceMember, ServerMessage};
use crate::subs::ConnId;

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

pub(super) struct Session {
    pub(super) user: AuthedUser,
    pub(super) state: serde_json::Value,
    pub(super) tx: UnboundedSender<ServerMessage>,
    pub(super) updated_at: i64,
    /// Epoch-ms when this connection joined the room — the basis for the
    /// `oldestMemberAgeMs` column of `GET /admin/presence`. Never updated
    /// after join (a re-join DOES refresh it, matching the idempotent-join
    /// semantics that also refresh `state`).
    pub(super) joined_at: i64,
    /// Absolute epoch-ms at which `state` should be cleared to null by
    /// `expire_once`. `None` = permanent (no ttl armed). Joins never arm it
    /// (ttl rides on `presenceState` only); only `update_state` sets/clears it.
    pub(super) expires_at: Option<i64>,
    /// False until this connection has received at least one broadcast
    /// (`PresenceSnapshot` or `PresenceDelta`) for the room it's in. A
    /// `false` connection always receives a full `PresenceSnapshot` on the
    /// next flush (never a delta — there is nothing to diff against yet); it
    /// flips to `true` the moment that snapshot is sent, so every following
    /// flush for this connection is a delta against the room's shared
    /// baseline — for a v3 connection. A pre-v3 connection keeps receiving
    /// snapshots regardless of `caught_up` (see `protocol_version`).
    /// Reset to `false` on join/re-join, matching the semantics
    /// `flush_once` already relies on (a fresh join always sees the full
    /// list, never a partial delta).
    pub(super) caught_up: bool,
    /// The `protocolVersion` this connection declared on its `Auth` frame
    /// (`None` = omitted, v1 semantics). `flush_once` emits `PresenceDelta`
    /// only to connections that negotiated at least
    /// `protocol::PRESENCE_DELTA_MIN_VERSION`; every other connection keeps
    /// receiving full `PresenceSnapshot` frames, so the delta frame's
    /// rollout never breaks an older SDK (ARC-013 gate).
    pub(super) protocol_version: Option<u32>,
}

/// Per-db shard.
pub(super) struct DbPresence {
    /// room -> (conn -> session)
    pub(super) rooms: HashMap<String, HashMap<ConnId, Session>>,
    /// conn -> rooms it belongs to (O(1) eviction)
    pub(super) conn_rooms: HashMap<ConnId, Vec<String>>,
    /// Per-conn `presenceState` rate window: (window_start_ms, count).
    pub(super) update_window: HashMap<ConnId, (i64, u32)>,
    /// room -> the last (`seq`, `members`) broadcast to every caught-up
    /// connection in that room. One shared baseline per room, not one per
    /// connection — an O(members) cost, not O(members²) — since every
    /// caught-up connection in a room is diffed against the same prior
    /// state by construction (a delta always fully replaces what every
    /// caught-up recipient already has). Absent for a room that has never
    /// completed a flush (the first flush for a room always seeds this
    /// entry and every recipient that tick gets the full snapshot instead
    /// of a diff, since a fresh `dirty` room has no members with
    /// `caught_up == true` yet).
    pub(super) room_baselines: HashMap<String, (u64, Vec<PresenceMember>)>,
}

impl DbPresence {
    pub(super) fn new() -> Self {
        Self {
            rooms: HashMap::new(),
            conn_rooms: HashMap::new(),
            room_baselines: HashMap::new(),
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
pub(super) struct PeerSnapshot {
    pub(super) members: Vec<PresenceMember>,
    pub(super) last_beat: i64,
}

/// One room's live footprint as surfaced by `GET /admin/presence` and the
/// `presenceDetail` field on `/admin/metrics`. In multi-instance mode
/// `member_count`/`state_bytes` are merged with gossiped peer membership
/// (ENH-022 Stage 3's `peers` shadow map, the same union `flush_once` already
/// broadcasts to clients) so an operator sees the room's whole footprint, not
/// just this replica's slice; `local_member_count` is always this replica's
/// own count so the split stays visible. In single-instance mode
/// `local_member_count == member_count` and the merge is a no-op (there are
/// no peers to merge).
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RoomInspect {
    pub room: String,
    /// Merged member count: local members plus deduped gossiped peer members
    /// in multi-instance mode, else identical to `local_member_count`.
    pub member_count: u64,
    /// This replica's own local member count, always (never merged).
    pub local_member_count: u64,
    /// Sum of serialized `presenceState` blob sizes across the room's
    /// members, merged the same way as `member_count`.
    pub state_bytes: u64,
    /// Age of the oldest member's join, in ms (a re-join refreshes it).
    /// Local only — `PresenceMember` carries no join timestamp for peer
    /// members, so this can never reflect a remote member's join age.
    pub oldest_member_age_ms: u64,
    /// True when `member_count`/`state_bytes` include gossiped peer
    /// membership — an eventually-consistent, best-effort merge (gossip
    /// publish is fire-and-forget; see `gossip_publish`), never an
    /// authoritative total. False in single-instance mode or when this room
    /// currently has no live peer contributions.
    pub merged_with_peers: bool,
}

/// True when a connection's negotiated `protocolVersion` allows the server
/// to send it `PresenceDelta` frames: a declared version at or above
/// `protocol::PRESENCE_DELTA_MIN_VERSION`. `None` (the client omitted
/// `protocolVersion`) is legacy v1 semantics and never receives deltas —
/// nor does any declared version below the gate — so an SDK that predates
/// the v3 frame keeps working off full `PresenceSnapshot` broadcasts
/// (ARC-013).
pub(super) fn supports_presence_delta(negotiated: Option<u32>) -> bool {
    negotiated.is_some_and(|v| v >= crate::protocol::PRESENCE_DELTA_MIN_VERSION)
}

/// The three buckets `diff_members` sorts a member-list transition into.
pub(super) struct MemberDiff {
    pub(super) joined: Vec<PresenceMember>,
    pub(super) left: Vec<String>,
    pub(super) state_changed: Vec<PresenceMember>,
}

/// Compute the `PresenceDelta` buckets between two full member lists for the
/// same room: members present in `after` but not `before` are `joined`,
/// members present in `before` but not `after` are `left` (by
/// `connection_id`), and members present in both whose `state` differs are
/// `state_changed` (this also covers a TTL expiry — `expire_once` nulls
/// `state` without touching membership, so that transition lands here, never
/// in `left`). `user` is not compared: a member's `user` never changes after
/// join (there is no rename op), so a same-id member is fully identified by
/// its state alone.
pub(super) fn diff_members(before: &[PresenceMember], after: &[PresenceMember]) -> MemberDiff {
    let before_by_id: HashMap<&str, &PresenceMember> = before
        .iter()
        .map(|m| (m.connection_id.as_str(), m))
        .collect();
    let after_by_id: HashSet<&str> = after.iter().map(|m| m.connection_id.as_str()).collect();

    let mut joined = Vec::new();
    let mut state_changed = Vec::new();
    for m in after {
        match before_by_id.get(m.connection_id.as_str()) {
            None => joined.push(m.clone()),
            Some(prev) if prev.state != m.state => state_changed.push(m.clone()),
            Some(_) => {}
        }
    }
    let left: Vec<String> = before
        .iter()
        .filter(|m| !after_by_id.contains(m.connection_id.as_str()))
        .map(|m| m.connection_id.clone())
        .collect();

    MemberDiff {
        joined,
        left,
        state_changed,
    }
}
