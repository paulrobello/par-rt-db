# Per-State TTL / Lease for Ephemeral Presence State

**Date:** 2026-08-06
**Status:** Implemented (2026-08-10)
**Scope:** Adds an optional per-state time-to-live to the realtime presence
layer (ENH-015, `docs/superpowers/specs/2026-08-06-presence-design.md`) so a
connection's `state` in a room auto-clears a bounded duration after its last
refresh, and the change is broadcast. Backward compatible; connection-bound
eviction is unchanged. Backlog item **ENH-015 follow-up**. A small, surgical
delta over the shipped primitive — no new task, no new transport, no new
persistence.

## Background & motivation

Presence-state eviction today is **connection-bound only**: a connection's
`state` in a room persists until the client sends a new `presenceState` to
clear it, or the `/sync` socket drops (which reuses the existing Ping/Pong
liveness, ~75 s). The ENH-015 spec's "Out of scope (v1)" lists the delta/diff
protocol and durability, but **not** per-state expiry — so a state cannot
auto-expire today.

This breaks the flagship use cases the primitive was built for. The robust,
common typing-indicator pattern is refresh-based: the client re-sends
`{ typing: true }` on each keystroke and relies on the state auto-clearing
~3 s after the *last* keystroke. With no per-state TTL the client must send an
explicit `{ typing: false }` clear, and if it forgets, lags, or the tab
backgrounds (throttling JS timers), "Alice is typing…" sticks until she
disconnects. The same failure hits raise-hand, transient "away", and idle-cursor
freeze. Every chat/collab app on par-rt-db hits this.

### Hard constraints (inherited, unchanged)

- **Not a document write.** TTL expiry broadcasts a `presenceSnapshot`; it
  still touches neither the committer nor Postgres. No new tap site, no
  `CommitterCtx` change.
- **The flush task is the existing coalescing fan-out.** Expiry reuses it; no
  second process-wide task is added.
- **Wire contract is four-implementations-identical.** The new optional field
  lands in `server/src/protocol.rs`, `ts-client/src/protocol.ts`,
  `rust-client/src/wire.rs`, and `python-client/src/par_rt_db/wire.py` with
  byte-identical tags and field names.

## Decision summary

- **Clear-state-to-null, not remove-member.** On expiry the server clears the
  session's `state` blob to `null`; the member **stays** in the room (still
  "online"), so an online indicator survives a transient typing flag expiring.
  Membership is still removed only by `leavePresence` or disconnect. This is
  what the acceptance criteria specify ("clears that connection's state"; the
  disconnect path "clears all of a connection's presence state" is unchanged)
  and what makes typing indicators work. The card title's word "lease" is loose
  phrasing; the criteria are precise.
- **Piggyback expiry on the existing flush task.** Each `Session` gains
  `ttl_expires_at: Option<i64>`. The process-wide flush task
  (`run_flush_task`, `presence.rs`) already wakes every `RTDB_PRESENCE_BROADCAST_INTERVAL_MS`
  (default 50 ms); a new `expire_once()` step at the top of each iteration
  clears expired sessions' state to null and marks their rooms dirty, then the
  existing `flush_once()` broadcasts the updated snapshot. No per-session timer,
  no new task. Expiry latency ≤ broadcast interval (~50 ms; ≤1 ms in immediate
  mode) — fine for 3 s typing indicators.
- **Per-update ttl, re-armed on every refresh; omitted ttl removes the expiry.**
  Each `presenceState` carrying `ttlMs` sets `ttl_expires_at =
  now + ttlMs`. An update with no `ttlMs` clears the expiry (state becomes
  permanent again). ttl omitted entirely = today's behavior (criterion 3).
  ttl rides on `presenceState` **only** — not the join `presence` frame (see
  Wire protocol).

## Wire protocol

Only the `presenceState` client→server frame gains an optional `ttlMs` — ttl is
a *refresh* concept, so it belongs on the update frame, not the join. (`presence`
join is unchanged.) The field is `Option<u64>` (milliseconds),
`rename_all = "camelCase"` → serialized as `ttlMs`,
`#[serde(default, skip_serializing_if = "Option::is_none")]` so an omitted field
round-trips identically across all four implementations.

| `type`          | fields (delta bolded) | meaning |
|-----------------|-----------------------|---------|
| `presence`      | `room: String`, `state?: Value` | Join/re-join (**unchanged**). |
| `presenceState` | `room: String`, `state: Value`, **`ttlMs?: u64`** | State update. If `ttlMs` is present, arm an expiry for this session's state `ttlMs` after `now`; if absent, clear any existing expiry (state permanent). |

**Why not the join frame too:** the reactive clients cache join state for
reconnect replay (rust `PresenceRoomState`, python `_PresenceRoom.join_state`, ts
`joinedRooms`). Putting ttl on `presenceState` (which is **not** replayed) means
zero replay-cache changes across all three clients, and avoids churn in the
join methods' signatures. The join→update sequence covers every use case (join
to appear online, then refresh the transient typing/cursor state with a ttl).

`leavePresence` is unchanged. No new server→client frame: the observable
effect of an expiry is the next coalesced `presenceSnapshot` showing the
member's `state` as `null`, exactly like a client-driven state update.

### Validation

- `ttlMs == 0` → `presenceErr` `BAD_REQUEST` ("ttl must be positive").
- `ttlMs > RTDB_PRESENCE_MAX_TTL_MS` → `presenceErr` `BAD_REQUEST` ("ttl
  exceeds maximum"). **Never clamped** — consistent with presence's "no silent
  clamping" rule for every other cap (`max_state_bytes`, room size, …).
- Validated alongside the existing `validate_state` size check, before the
  session is mutated (same early-reject placement). An out-of-range `ttlMs` is
  rejected without arming an expiry; the accepted-update path (and thus the
  `update_limit_per_sec` window) is untouched.

## Behavior rules

1. **Arm on refresh.** A `presenceState` with a valid `ttlMs` sets
   `ttl_expires_at = now + ttlMs` for that `(conn, room)` session and marks the
   room dirty (the state itself also changed). Repeated refreshes push the
   expiry forward.
2. **Clear on permanent update.** A `presenceState` with no `ttlMs`
   sets `ttl_expires_at = None` (state permanent again). Omitted ttl = current
   behavior (criterion 3).
3. **Expiry clears state, not membership.** When `ttl_expires_at <= now`,
   `expire_once()` sets the session's `state` to `null`, clears
   `ttl_expires_at`, marks the room dirty, and increments
   `rtdb_presence_ttl_expiries_total`. The member remains present; the next
   `flush_once()` broadcasts the snapshot with `state: null`.
4. **Connection-bound eviction unchanged.** `remove_conn` (disconnect) still
   clears every room the conn was in; `leave` still removes a membership. TTL
   never removes a membership (criterion 2).
5. **Re-arm after expiry.** After expiry clears state to null, a subsequent
   `presenceState` with new `state` (+ optional `ttlMs`) works normally — the
   member was never removed.

## Expiry mechanism (Approach A)

`run_presence_flush`'s loop gains one step per iteration, run **before**
`flush_once()` in both the coalesced (ticker) and immediate (`interval_ms == 0`)
paths:

```
loop {
    expire_once().await;   // NEW: clear expired state, mark rooms dirty
    flush_once().await;    // existing: broadcast one snapshot per dirty room
    // (ticker.tick() or the existing 1ms idle-sleep cadence)
}
```

`expire_once()` iterates each db shard's rooms, and for each session with
`ttl_expires_at` set and `<= now`: sets `state = null`, `ttl_expires_at = None`,
collects the room into the dirty set. It returns whether it expired anything
(so the immediate-mode path can yield instead of sleeping when there is work —
mirroring `flush_once()`'s `did_work` contract).

**Why a sweep, not a heap.** A full O(sessions) sweep per tick is bounded
(rooms ≤ `MAX_ROOM_SIZE` 100, conns ≤ `MAX_ROOMS_PER_CONN` 32) and negligible
for v1 at a 50 ms cadence. A per-shard expiry heap (`BinaryHeap<(expires_at,
conn, room)>`) is the documented future optimization if large deployments make
the sweep hot; it is deliberately not built now (YAGNI).

**Lock discipline unchanged.** `expire_once()` takes each shard lock, mutates
sessions in place, releases, then marks dirty rooms (the `mark_dirty` path
already takes only the `dirty` lock). It never holds a shard lock across
`mark_dirty` or across another shard — same discipline as `join`/`update_state`.

## Configuration

One new boot-only knob, parsed in `Config::from_env` alongside the seven
existing `RTDB_PRESENCE_*` fields, threaded through `PresenceConfig`:

| env var | default | meaning |
|---------|---------|---------|
| `RTDB_PRESENCE_MAX_TTL_MS` | `300000` (5 min) | Upper bound on a client-supplied `ttlMs`. Over-cap → `BAD_REQUEST` (no clamping). |

Boot-only (not hot-reloadable), matching the rest of the presence runtime.

## Integration points

- **`protocol.rs`** — add `ttl_ms: Option<u64>` (`rename_all = "camelCase"`) to
  the `PresenceState` `ClientMessage` variant only; wire-tag/field tests next to
  the existing presence ones.
- **`presence.rs`** — add `ttl_expires_at: Option<i64>` to `Session`; arm/clear
  it in `update_state` from the new `ttl_ms` arg (`join` is unchanged — joins are
  permanent until an update arms a ttl); add `expire_once()`; call it at the top
  of `run_flush_task`'s loop; add `RTDB_PRESENCE_MAX_TTL_MS` to
  `PresenceConfig`/`Config`; validate `ttl_ms`.
- **`ws.rs`** — thread the parsed `ttl_ms` from the `PresenceState` frame into
  `update_state`.
- **`config.rs`** — `RTDB_PRESENCE_MAX_TTL_MS`.
- **`metrics`** — `rtdb_presence_ttl_expiries_total` (counter), surfaced on the
  dashboard metrics page.
- **Tests** — `presence.rs` unit (arm/expire/re-arm; omitted-ttl = permanent;
  clear-to-null not remove; ttl validation; `expire_once` marks dirty and
  flush broadcasts null state) and `presence_test.rs` integration (two conns in
  a room; one arms a short ttl and stops refreshing; survivor observes
  `state → null` after the flush cadence while the member remains present).

TTL does **not** touch: `CommitterCtx`, any `handle_*` tap, the op-feed, audit
log, webhooks, `SubscriptionManager`, document tables, or storage.

## Client mirror (all four clients + harness + matrix)

- **ts-client** — `ttlMs?` on the `presenceState` wire frame;
  `updatePresence(room, state, ttlMs?)` accepts it (the join method is
  unchanged); `usePresence`'s returned `updatePresence` forwards it.
- **rust-client** — `ttlMs: Option<u64>` on `PresenceState` in `wire.rs`;
  `update_presence` takes an optional ttl (join method unchanged).
- **python-client** — `ttlMs` on `_ClientPresenceState` in `wire.py`;
  `update_presence` takes an optional `ttl_ms` (join method unchanged).
- **In-memory harness** (ts `InMemoryRtDbClient` + rust `in_memory` feature) —
  model ttl expiry against the harness's own clock so a two-"client" typing flow
  is testable with no network.
- **Live-server opt-in tests** — ts `tests/integration/**` and
  `rust-client/tests/http_integration.rs` convention (`#[ignore]`,
  `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`).
- **`FEATURE_MATRIX.md`** — annotate the presence row with per-state TTL.
  READMEs and the parent ENH-015 spec stay in sync (flip "per-state expiry" from
  out-of-scope to shipped).

## Acceptance criteria (from the card)

1. `presenceState` accepts an optional `ttl_ms`; the server clears that
   connection's state in the room `ttl_ms` after the last refresh and broadcasts
   the change.
2. Connection-bound eviction is unchanged (disconnect still clears all of a
   connection's presence state).
3. ttl omitted preserves current behavior (state lives until explicit clear or
   disconnect).
4. Wire field + in-memory harness + integration test mirrored across all four
   clients (parity).

## Out of scope

- Per-**field** ttl (the state blob is opaque to the server; ttl clears the
  whole blob). Clients keep transient-only state in a dedicated room/blob.
- Membership-expiring "leases" (ttl removes the member). Rejected — see Decision
  summary; disconnect/leave own membership removal.
- An expiry index/heap optimization (full sweep per tick is fine for v1).
- Any change to `leavePresence`, disconnect eviction, the op-feed, or
  persistence.

## References

- Parent primitive: `docs/superpowers/specs/2026-08-06-presence-design.md`;
  implementation `server/src/presence.rs`.
- Flush task + coalesced broadcast + per-conn rate limit: `presence.rs`
  (`run_flush_task`, `flush_once`, `update_state`).
- Boot-config env-knob patterns: `server/src/config.rs`.
