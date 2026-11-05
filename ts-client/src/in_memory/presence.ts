/**
 * In-memory presence for the par-rt-db test harness (split out of
 * `./store.ts`, QA-003b): the shared `PresenceRooms` backing (a
 * `room -> connectionId -> member` registry with fan-out, approximating the
 * server's per-db presence registry) and `PresenceMembership`, the per-client
 * join/update/leave mechanics `InMemoryRtDbClient` delegates to.
 */

import type { AuthedUser, PresenceMember } from "../protocol.js";

/**
 * Shared in-memory presence backing: a `room -> connectionId -> member` map with
 * a per-room subscriber set. Two `InMemoryRtDbClient`s that share a
 * `PresenceRooms` instance see each other's joins/updates/leaves fan out,
 * approximating the server's per-db presence registry for tests (one client,
 * one connection — exactly like the live server's per-ConnId keying). A client
 * with no `presenceRooms` option gets a private instance and only ever sees
 * itself in its rooms.
 *
 * Mirrors the existing harness pattern of `notifySubs(writeSet)` fanning a
 * recomputed snapshot to every local subscriber after a write.
 */
export class PresenceRooms {
  private readonly members = new Map<string, Map<string, PresenceMember>>();
  private readonly subs = new Map<string, Set<(members: PresenceMember[]) => void>>();
  private readonly expiry = new Map<string, Map<string, number>>();

  /** Returns a stable-order snapshot of `room`'s current members. */
  snapshot(room: string): PresenceMember[] {
    const map = this.members.get(room);
    return map ? [...map.values()] : [];
  }

  /** Adds or replaces `member` in `room` and fans out a fresh snapshot. */
  join(room: string, member: PresenceMember): void {
    let map = this.members.get(room);
    if (!map) {
      map = new Map();
      this.members.set(room, map);
    }
    map.set(member.connectionId, member);
    this.fanOut(room);
  }

  /** Updates `connectionId`'s state in `room` and fans out. No-op if the
   * connection is not in the room (matches the live server, which would not
   * relay an update for a non-member). When `ttlMs` > 0, schedules an expiry
   * sweep that nulls this member's `state` at `now + ttlMs` (the member stays
   * listed); a refresh with `ttlMs` absent/0/falsy clears any pending expiry (a
   * permissive offline approximation — the LIVE SERVER rejects ttlMs <= 0 with
   * BAD_REQUEST), mirroring the live server's "ttlMs after the last refresh"
   * semantics for the > 0 case. */
  update(
    room: string,
    connectionId: string,
    state: unknown,
    ttlMs?: number,
    now: number = Date.now(),
  ): void {
    const map = this.members.get(room);
    const member = map?.get(connectionId);
    if (!member) {
      return;
    }
    member.state = state ?? null;
    let exp = this.expiry.get(room);
    if (!exp) {
      exp = new Map();
      this.expiry.set(room, exp);
    }
    if (ttlMs && ttlMs > 0) {
      exp.set(connectionId, now + ttlMs);
    } else {
      exp.delete(connectionId);
    }
    this.fanOut(room);
  }

  /** Removes `connectionId` from `room` and fans out. No-op if absent. Also
   * clears any pending expiry so a re-join doesn't inherit a stale ttl. */
  leave(room: string, connectionId: string): void {
    const map = this.members.get(room);
    if (!map) {
      return;
    }
    map.delete(connectionId);
    this.expiry.get(room)?.delete(connectionId);
    if (map.size === 0) {
      this.members.delete(room);
    }
    this.fanOut(room);
  }

  /** Clears expired members' `state` to `null` (the member stays listed) and
   * fans out each touched room once. Returns true if anything expired. Mirrors
   * the live server's per-connection ttl clearing. */
  expire(now: number = Date.now()): boolean {
    let any = false;
    const touched: string[] = [];
    for (const [room, exp] of this.expiry) {
      const roomMap = this.members.get(room);
      if (!roomMap) {
        this.expiry.delete(room);
        continue;
      }
      let roomTouched = false;
      for (const [connId, at] of exp) {
        if (at <= now) {
          const m = roomMap.get(connId);
          if (m) {
            m.state = null;
            any = true;
            roomTouched = true;
          }
          exp.delete(connId);
        }
      }
      if (roomTouched) {
        touched.push(room);
      }
    }
    for (const room of touched) {
      this.fanOut(room);
    }
    return any;
  }

  /** Registers `fn` for `room` snapshots and immediately fires it with the
   * current snapshot (mirroring the server's first `presenceSnapshot` on join).
   * Returns an unsubscribe. */
  subscribe(room: string, fn: (members: PresenceMember[]) => void): () => void {
    let set = this.subs.get(room);
    if (!set) {
      set = new Set();
      this.subs.set(room, set);
    }
    set.add(fn);
    fn(this.snapshot(room));
    return () => {
      const current = this.subs.get(room);
      if (!current) {
        return;
      }
      current.delete(fn);
      if (current.size === 0) {
        this.subs.delete(room);
      }
    };
  }

  private fanOut(room: string): void {
    const set = this.subs.get(room);
    if (!set) {
      return;
    }
    const snap = this.snapshot(room);
    for (const fn of set) {
      fn(snap);
    }
  }
}

/**
 * One client's presence membership: the rooms this connection has joined and
 * the unsubscribe handles for its per-room callbacks, driving the shared
 * `PresenceRooms` backing. `InMemoryRtDbClient`'s
 * `presence`/`updatePresence`/`leavePresence` are thin delegates onto this
 * class (the mechanics moved out of the client in QA-003b).
 */
export class PresenceMembership {
  private readonly joinedRooms = new Set<string>();
  /** Unsubscribe handles for the per-room callbacks registered on `rooms`.
   * Tracked so `leave(room)` can drop every local subscriber for that room,
   * mirroring the reactive client and the live server (which stops delivering
   * snapshots once a connection has left). */
  private readonly unsubs = new Map<string, Array<() => void>>();

  constructor(
    private readonly rooms: PresenceRooms,
    private readonly connectionId: string,
    private readonly user: AuthedUser,
    private readonly now: () => number,
  ) {}

  /**
   * Joins presence room `room` with optional initial `state`, mirroring
   * `RtDbClient.presence`. When `onUpdate` is supplied, it fires with the
   * current member list on join and again on every local mutation (a peer's
   * join/update/leave on a shared `PresenceRooms`).
   *
   * Returns an unsubscribe that stops listening but does NOT leave the room —
   * call `leave(room)` for that, mirroring the reactive client.
   */
  join(room: string, state: unknown, onUpdate?: (members: PresenceMember[]) => void): () => void {
    this.joinedRooms.add(room);
    this.rooms.join(room, {
      connectionId: this.connectionId,
      user: this.user,
      state: state ?? null,
    });
    let off: (() => void) | undefined;
    if (onUpdate) {
      off = this.rooms.subscribe(room, onUpdate);
      let arr = this.unsubs.get(room);
      if (!arr) {
        arr = [];
        this.unsubs.set(room, arr);
      }
      arr.push(off);
    }
    return () => {
      off?.();
      if (off) {
        const arr = this.unsubs.get(room);
        if (arr) {
          const i = arr.indexOf(off);
          if (i >= 0) arr.splice(i, 1);
          if (arr.length === 0) this.unsubs.delete(room);
        }
      }
    };
  }

  /** Broadcasts updated `state` for this connection in `room`. No-op if this
   * client has not joined `room` (mirrors the live server, which would not
   * relay an update from a non-member). When `ttlMs` is set, the harness
   * schedules an expiry that nulls this member's `state` at `now + ttlMs`
   * (the member stays listed) — mirroring the live server. */
  update(room: string, state: unknown, ttlMs?: number): void {
    if (!this.joinedRooms.has(room)) {
      return;
    }
    this.rooms.update(room, this.connectionId, state, ttlMs, this.now());
  }

  /** Leaves `room`: removes this connection from the member list, drops every
   * local subscriber this client registered for that room, and fans out a
   * fresh snapshot to remaining subscribers. */
  leave(room: string): void {
    if (!this.joinedRooms.delete(room)) {
      return;
    }
    const arr = this.unsubs.get(room);
    if (arr) {
      for (const off of arr) off();
      this.unsubs.delete(room);
    }
    this.rooms.leave(room, this.connectionId);
  }
}
