//! Presence rooms + handles for the in-memory harness.
//!
//! Extracted from `in_memory.rs` (QA-108) so the core module holds only the
//! store/executor. Pure file movement - behavior and public visibility unchanged.

use super::*;

/// Shared in-memory presence backing: a `room → connectionId → member` map with
/// a per-room subscriber list. Two [`InMemoryRtDbClient`]s that share a
/// `PresenceRooms` instance see each other's joins/updates/leaves fan out,
/// approximating the server's per-db presence registry for tests (one client,
/// one connection — exactly like the live server's per-ConnId keying). A client
/// with no `presence_rooms` option gets a private instance and only ever sees
/// itself in its rooms.
///
/// Ports `PresenceRooms` from `ts-client/src/in_memory.ts`. Mirrors the existing
/// harness pattern of `notify_subs(write_set)` fanning a recomputed snapshot to
/// every local subscriber after a write, including the `Arc<AtomicBool>` alive
/// flag that lets a dropped handle lazily unregister its callback.
#[derive(Default)]
pub struct PresenceRooms {
    /// `room → connectionId → member`. Inner vec preserves insertion order so
    /// `snapshot` returns members in join order (matching the TS `Map` iteration
    /// semantics).
    members: HashMap<String, Vec<(String, PresenceMember)>>,
    /// `room → list of (alive, callback)`. The alive flag is cleared by
    /// [`PresenceHandle`]'s Drop; fan_out skips and lazily compacts dead entries
    /// — the same pattern as [`InMemoryRtDbClient::notify_subs`].
    subs: HashMap<String, Vec<PresenceListener>>,
    /// `room → connectionId → expiresAt(ms)` for ENH-015 presence-ttl. A member
    /// with an entry here has its `state` cleared to null at the recorded
    /// instant by [`expire`](Self::expire) (the member stays listed). Mirrors
    /// the ts harness's parallel `expiry` map — kept separate from
    /// [`PresenceMember`] because that struct is the server→client snapshot
    /// shape and must stay byte-identical with the server.
    expiry: HashMap<String, HashMap<String, i64>>,
}

/// One registered presence callback + its alive flag.
struct PresenceListener {
    alive: Arc<AtomicBool>,
    callback: Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync>,
}

/// Handle returned by [`PresenceRooms::subscribe`]. Dropping it (or calling
/// [`unsubscribe`](Self::unsubscribe)) clears the callback so no further fan-outs
/// fire — matching the TS `() => unsub()` contract. Mirrors [`SubscriptionHandle`].
#[derive(Clone)]
pub struct PresenceHandle {
    alive: Arc<AtomicBool>,
}

impl PresenceHandle {
    /// Detach the listener; equivalent to dropping the handle.
    pub fn unsubscribe(self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

impl Drop for PresenceHandle {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::SeqCst);
    }
}

impl PresenceRooms {
    /// Returns a stable-order snapshot of `room`'s current members (join order).
    pub fn snapshot(&self, room: &str) -> Vec<PresenceMember> {
        self.members
            .get(room)
            .map(|entries| entries.iter().map(|(_, m)| m.clone()).collect())
            .unwrap_or_default()
    }

    /// Adds or replaces `member` in `room` and fans out a fresh snapshot.
    pub fn join(&mut self, room: &str, member: PresenceMember) {
        let entries = self.members.entry(room.to_string()).or_default();
        if let Some((_, existing)) = entries
            .iter_mut()
            .find(|(id, _)| *id == member.connection_id)
        {
            *existing = member;
        } else {
            entries.push((member.connection_id.clone(), member));
        }
        self.fan_out(room);
    }

    /// Updates `connection_id`'s state in `room` and fans out. No-op if the
    /// connection is not in the room (matches the live server, which would not
    /// relay an update for a non-member). When `ttl_ms` is `Some(n)` with `n > 0`,
    /// schedules an expiry sweep that nulls this member's `state` at `now + n`
    /// (the member stays listed); `None` clears any pending expiry, mirroring the
    /// live server's "ttlMs after the last refresh" semantics. `Some(<=0)` is
    /// treated as `None` (no expiry) — a permissive offline approximation; the
    /// LIVE SERVER rejects ttl_ms <= 0 with BAD_REQUEST. Mirrors TS
    /// `PresenceRooms.update` (ENH-015 presence-ttl).
    pub fn update(
        &mut self,
        room: &str,
        connection_id: &str,
        state: Value,
        ttl_ms: Option<u64>,
        now: i64,
    ) {
        let Some(entries) = self.members.get_mut(room) else {
            return;
        };
        let Some((_, member)) = entries.iter_mut().find(|(id, _)| id == connection_id) else {
            return;
        };
        member.state = state;
        let exp = self.expiry.entry(room.to_string()).or_default();
        match ttl_ms {
            Some(n) if n > 0 => {
                exp.insert(connection_id.to_string(), now + n as i64);
            }
            _ => {
                exp.remove(connection_id);
            }
        }
        if exp.is_empty() {
            self.expiry.remove(room);
        }
        self.fan_out(room);
    }

    /// Removes `connection_id` from `room` and fans out. No-op if absent. Also
    /// clears any pending expiry entry so a re-join doesn't inherit a stale ttl.
    pub fn leave(&mut self, room: &str, connection_id: &str) {
        let Some(entries) = self.members.get_mut(room) else {
            return;
        };
        let before = entries.len();
        entries.retain(|(id, _)| id != connection_id);
        if entries.len() == before {
            return; // was not a member — no fan-out
        }
        if entries.is_empty() {
            self.members.remove(room);
        }
        if let Some(exp) = self.expiry.get_mut(room) {
            exp.remove(connection_id);
            if exp.is_empty() {
                self.expiry.remove(room);
            }
        }
        self.fan_out(room);
    }

    /// Clears expired members' `state` to `Value::Null` (the member stays
    /// listed) and fans out each touched room once. Returns `true` if anything
    /// expired. Mirrors the live server's per-connection ttl clearing
    /// (`server::presence::expire_once`) and the TS harness's `expire`. Idempotent:
    /// a second sweep with the same `now` is a no-op (the expiry entries were
    /// already drained).
    pub fn expire(&mut self, now: i64) -> bool {
        let mut any = false;
        let mut touched: Vec<String> = Vec::new();
        // Drain the rooms that currently have an expiry map. We can't mutate
        // `self.members` while iterating `self.expiry`, so collect the work
        // first, then apply.
        let rooms_with_expiry: Vec<String> = self.expiry.keys().cloned().collect();
        for room in rooms_with_expiry {
            let Some(exp) = self.expiry.get_mut(&room) else {
                continue;
            };
            let due: Vec<String> = exp
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(id, _)| id.clone())
                .collect();
            if due.is_empty() {
                continue;
            }
            // Drop the expired entries from the expiry map.
            for id in &due {
                exp.remove(id);
            }
            if exp.is_empty() {
                self.expiry.remove(&room);
            }
            // Null the state of each due member in the room's member list.
            if let Some(entries) = self.members.get_mut(&room) {
                let mut room_touched = false;
                for id in &due {
                    if let Some((_, member)) = entries.iter_mut().find(|(cid, _)| cid == id) {
                        member.state = Value::Null;
                        any = true;
                        room_touched = true;
                    }
                }
                if room_touched {
                    touched.push(room);
                }
            }
        }
        for room in &touched {
            self.fan_out(room);
        }
        any
    }

    /// Registers `cb` for `room` snapshots and immediately fires it with the
    /// current snapshot (mirroring the server's first `presenceSnapshot` on
    /// join). Returns a [`PresenceHandle`] whose Drop clears the callback.
    pub fn subscribe<F>(&mut self, room: &str, cb: F) -> PresenceHandle
    where
        F: Fn(Vec<PresenceMember>) + Send + Sync + 'static,
    {
        let alive = Arc::new(AtomicBool::new(true));
        let callback: Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync> = Arc::new(cb);
        let initial = self.snapshot(room);
        self.subs
            .entry(room.to_string())
            .or_default()
            .push(PresenceListener {
                alive: alive.clone(),
                callback: callback.clone(),
            });
        callback(initial);
        PresenceHandle { alive }
    }

    /// Re-snapshots `room` and fires every live callback. Lazily compacts dead
    /// listeners (handle dropped → alive=false), mirroring `notify_subs`.
    fn fan_out(&mut self, room: &str) {
        let snap = self.snapshot(room);
        let mut fires: Vec<Arc<dyn Fn(Vec<PresenceMember>) + Send + Sync>> = Vec::new();
        if let Some(listeners) = self.subs.get_mut(room) {
            // Collect live callbacks, then compact.
            for l in listeners.iter() {
                if l.alive.load(Ordering::SeqCst) {
                    fires.push(l.callback.clone());
                }
            }
            listeners.retain(|l| l.alive.load(Ordering::SeqCst));
            if listeners.is_empty() {
                self.subs.remove(room);
            }
        }
        for cb in fires {
            cb(snap.clone());
        }
    }
}
