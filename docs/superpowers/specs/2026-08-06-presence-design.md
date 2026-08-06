# Realtime Presence Primitive

**Date:** 2026-08-06
**Status:** Design (pre-implementation)
**Scope:** Adds a transient, in-memory presence layer to the realtime surface —
per-database "rooms" of ephemeral per-session state (online indicators, cursors,
typing) that clients join over the existing `/sync` WebSocket and receive as live
member lists. **Not** committer-bound, **not** durable, **not** persisted. Convex
presence-component parity (FEATURE_MATRIX, new row). Backlog item **ENH-015**.

## Background & motivation

Every realtime query par-rt-db serves today is over *durable* document data:
`subscribe` runs a `Query` against document tables, the committer re-runs it on
write, and the diff is pushed. "Who is online right now", "where is each cursor",
and "who is typing" do not fit that model — they are ephemeral, high-frequency,
and meaningless to persist. Apps fake it today by writing presence docs and
reaping them with TTL, a clumsy workaround that burns document writes, committer
turns, and row invalidation for data that should never touch Postgres.

Convex ships this as a *component* that is, in fact, table-backed (heartbeats
write rows; a batch worker reaps timeouts). ENH-015 deliberately does better:
par-rt-db already has a **live `/sync` WebSocket per client with its own liveness
timeout**, so an open connection is itself a presence session and a liveness
signal for free — collapsing most of Convex's heartbeat/reaper machinery. The
result is a pure in-memory layer that is connection-driven, broadcasts over the
existing outbound channel, and touches neither the committer nor Postgres.

### Hard constraints (from the architecture)

- **Single-writer invariant.** Every document write goes through that database's
  committer task, serialized; the op-feed/audit/webhook taps fire inside that
  turn. Presence is **not** a document write, so it must **not** route through
  the committer, extend `CommitterCtx`, or add a tap site to
  `handle_mutate`/`handle_scheduled`/`handle_migrate`/`handle_reaper`. It is a
  sibling reactive surface, not a write path.
- **Postgres is the source of truth; no persistence for ephemeral data.** Per
  the vendor-lock-to-Postgres steer, document tables hold durable data only.
  Presence is regenerable (a reconnect re-establishes it) and is held purely in
  process memory — it does **not** persist to Postgres (that would bloat the DB
  with throwaway rows and add a storage write path, exactly the workaround this
  replaces).
- **No embedded JS runtime, no per-app server code.** Presence is a fixed,
  declarative server capability driven by a small set of WS frames — there is no
  plan to make presence logic scriptable.
- **The `/sync` connection is the authenticated, liveness-bearing unit.**
  Presence piggybacks on the connection that already authenticates the principal
  to a database and is already kept alive by the server-Ping / client-Pong cycle
  (`PING_INTERVAL` 30 s, `LIVENESS_TIMEOUT` 75 s). There is no second transport
  and no app-level heartbeat.
- **Identity is reduced past the WS boundary.** The full `Principal` (with
  `email`/`name`/`github_*`) lives only in the WS handler; downstream code sees
  `PrincipalCtx` (which carries no display identity). Presence must capture
  display identity at the WS layer at join time.
- **Wire contract is four-implementations-identical.** New frames land in
  `server/src/protocol.rs`, `ts-client/src/protocol.ts`,
  `rust-client/src/wire.rs`, and `python-client/src/par_rt_db/wire.py` with
  byte-identical tags and field names (non-uniform casing is load-bearing).

### Decision summary

- **Session model: connection-bound.** Each `/sync` connection joined to a room
  is one presence session. The connection is the session and the liveness
  signal; disconnect evicts. One user with two tabs is two presence entries
  (correct for cursors; online-indicator dedup is a client concern or a v2).
  Rejected the principal/user-bound (Convex-style refcount) model: it needs a
  per-entry refcount and an expiry sweep even for connected users, and gains
  little over a connection-bound layer that gets liveness for free.
- **Symmetric join.** Joining a room makes a connection *present* in it *and*
  subscribes it to the room's member list in one act — the dominant case for
  cursors and online indicators. Observe-only (subscribe without being present)
  is deferred.
- **Full-list snapshots, not deltas, in v1.** Rooms are size-capped, so a full
  `PresenceMember[]` per broadcast is bounded; a delta/diff protocol is a v2
  optimization.
- **Coalesced broadcast.** A single process-wide flush task bounds the per-room
  broadcast rate regardless of update frequency — the difference between cursors
  that scale and cursors that fall over.
- **In-memory `PresenceManager`** as a sibling to `SubscriptionManager` on
  `state.realtime`, driven by connection lifecycle, broadcasting over each
  connection's existing `out_tx`. No committer, no DB, no tap sites.

## Data model & lifecycle

A new `PresenceManager` mirrors `SubscriptionManager`'s shard shape (outer
`Mutex<HashMap<db, Arc<Mutex<DbPresence>>>>`, the outer lock held only to clone
the shard `Arc`):

```
PresenceManager {
    dbs:    Mutex<HashMap<String, Arc<Mutex<DbPresence>>>>,
    metrics: Arc<Metrics>,
    config: PresenceConfig,
}
DbPresence {
    rooms:      HashMap<String /* room */, HashMap<ConnId, PresenceSession>>,
    conn_index: HashMap<ConnId, Vec<String> /* rooms this conn is present in */>,
}
PresenceSession {
    user:       AuthedUser,   // captured at the WS layer at join
    state:      serde_json::Value,
    joined_at:  i64,
    updated_at: i64,
}
```

- **One membership per `(conn, room)`**, keyed by `ConnId` (process-unique `u64`).
  The `conn_index` reverse map gives O(1) eviction on disconnect.
- **Lifecycle = connection lifecycle.** A connection's death is the leave event.
  On disconnect, `PresenceManager::remove_conn(db, conn_id)` walks `conn_index`,
  removes the session from each room, marks each affected room dirty, and lets
  the next flush broadcast the updated list to survivors. Half-open sockets are
  bounded by the existing 75 s liveness timeout — no presence-specific expiry
  sweeper is needed.
- **Identity capture at the WS layer.** `authed_user(&principal)` is computed in
  the WS handler at join and stored on `PresenceSession`. (The full `Principal`
  is not available past `handle_text_frame`.)
- **Not on `CommitterCtx`; not in any `handle_*` tap block.** This is the
  property that makes the layer ephemeral and non-durable.

`PresenceManager` is **not** per-db-task-scoped like the committer/scheduler/
reaper (which self-terminate on db deletion). It is a single process-wide
service on `AppState`; `delete-db` calls `presence.drop_db(db)` to clear that
db's shard (mirroring `subs.drop_db`).

## API — wire protocol

Three new client→server frames and two new server→client frames on `/sync`, all
tagged `type`, `rename_all = "camelCase"`, and added to the existing
`ClientMessage` / `ServerMessage` enums. `ClientMessage` keeps
`deny_unknown_fields`; the new variants are always present in the enum (a
feature-disabled server still parses them and returns `presenceErr`, never a
"malformed → close socket").

### Client → server

| `type`           | fields                          | meaning |
|------------------|---------------------------------|---------|
| `presence`       | `room: String`, `state?: Value` | Join `room`: register this connection as present (optional initial `state`) and subscribe to the room's member list. Idempotent — a re-`presence` to a joined room updates `state` and re-sends the snapshot. |
| `presenceState`  | `room: String`, `state: Value`  | Update this connection's `state` in a room it has joined (cursor moved, typing toggled). `presenceErr` if not joined. |
| `leavePresence`  | `room: String`                  | Leave the room: stop being present and stop receiving its member list. No-op if not joined. |

### Server → client

| `type`              | fields                                  | meaning |
|---------------------|-----------------------------------------|---------|
| `presenceSnapshot`  | `room: String`, `members: Vec<PresenceMember>` | The room's full current member list. Sent on join and on any membership/state change, coalesced (§Scaling). |
| `presenceErr`       | `room: String`, `error: RtDbError`      | Join/state failed: feature disabled, bad room name, `state` too large, room full, too many rooms, rate-limited. |

```
PresenceMember {
    connectionId: String,   // opaque, unique per presence session (the ConnId, stringified)
    user:         AuthedUser, // { kind, email?, name?, githubLogin?, githubId? } — display identity
    state:        Value,    // opaque client state blob (cursor pos, typing flag, …)
}
```

`connectionId` is the opaque key other clients key cursors/avatars on. `user`
carries display identity; machine-token sessions surface as `kind: "machine"`
with no email (a client may label or hide them). `state` is opaque to the
server — the server validates only its size, never its shape.

### Behavior rules

1. **Join.** `presence { room, state? }` with feature enabled and a valid room
   name registers the membership, captures `authed_user(principal)`, and marks
   the room dirty. The joining connection — now a subscriber — receives the
   `presenceSnapshot` (alongside the room's other members) on the next flush
   (latency ≤ `BROADCAST_INTERVAL_MS`; `0` = immediate).
2. **Idempotent re-join.** `presence` to an already-joined room updates `state`
   (equivalent to `presenceState`) and re-confirms subscription. Never errors
   "already present".
3. **State update.** `presenceState { room, state }` updates `state` and marks
   the room dirty; not-joined → `presenceErr` `BAD_REQUEST`.
4. **Leave.** `leavePresence { room }` removes the membership, marks the room
   dirty; not-joined is a silent no-op (no error). `leavePresence` and
   `presenceState` produce **no success ack** of their own — the observable
   effect is the next `presenceSnapshot` (mirroring `Unsubscribe`, which is also
   ack-less); only errors surface, as `presenceErr`.
5. **Disconnect.** Connection close evicts every room the conn was in and marks
   each dirty — survivors see the member disappear on the next flush.
6. **Empty rooms are pruned.** A room whose last member leaves/disconnects is
   dropped from memory (no unbounded room-key growth, no persistence).
7. **Feature disabled** (`RTDB_PRESENCE_ENABLED=false`): any presence frame →
   `presenceErr` `FORBIDDEN` ("presence not enabled"); the connection stays open.
8. **Room name validation.** Non-empty, ≤ `RTDB_PRESENCE_MAX_ROOM_BYTES`
   (default 256). Invalid → `presenceErr` `BAD_REQUEST`. No charset restriction
   — rooms are arbitrary app-defined string keys.
9. **No silent clamping.** Oversized `state`, oversized `room`, a full room, or
   too many rooms per connection all reject explicitly (the standard
   `{code, message}` envelope) — never truncate.

## Scaling & safety (what makes cursors viable)

Presence is an in-memory fan-out surface that an authenticated peer can drive at
high frequency (a cursor at 30 fps). These guards bound the cost; they are
load-bearing, not optional.

- **Coalesced broadcast.** A single process-wide flush task
  (`run_presence_flush`, spawned once in `AppState::new`) wakes every
  `RTDB_PRESENCE_BROADCAST_INTERVAL_MS` (default 50 ms), snapshots every dirty
  room once, and clears the dirty set. This caps the broadcast rate at
  1 snapshot / room / tick regardless of how many `presenceState` frames arrive.
  A room is marked dirty on join, leave, state update, and disconnect.
  `BROADCAST_INTERVAL_MS = 0` disables coalescing (immediate broadcast) for
  indicator-only deployments that prefer minimum latency. The flush task runs for
  the process lifetime (it is not per-db; `drop_db` only clears a shard's data).
- **Per-connection update rate limit.** `RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC`
  (default 20) bounds `presenceState` frequency per connection; excess →
  `presenceErr` `RATE_LIMITED` with `retryAfter`, connection stays open. This is
  independent of the coarse 200-msg/10s per-connection flood valve (which closes
  the socket) and of the machine-token/db RPM limiter.
- **Hard caps (all reject explicitly, never clamp).**
  - `RTDB_PRESENCE_MAX_STATE_BYTES` (default 1024) — per-`state` payload.
  - `RTDB_PRESENCE_MAX_ROOM_SIZE` (default 100) — members per room; join beyond
    → `presenceErr` `FORBIDDEN`.
  - `RTDB_PRESENCE_MAX_ROOMS_PER_CONN` (default 32) — rooms one connection may
    join; join beyond → `presenceErr` `FORBIDDEN`.

Worst-case bounded broadcast at defaults: a full 100-member room dirty every
50 ms with ~200-byte members ≈ 20 snapshots/s × ~20 KiB ≈ ~400 KiB/s/room —
acceptable for v1, and `MAX_ROOM_SIZE`/`BROADCAST_INTERVAL_MS`/`UPDATE_LIMIT`
are the levers to lower it.

## Auth, security & privacy

- **Per-db and authenticated-only.** A presence session exists only on a
  `/sync` connection that authenticated to the target database. `presence`
  (join) re-runs `authorize` + `is_admin` per op (SEC-004 parity: an allowlist
  removal or admin revocation takes effect on open connections); failure →
  `presenceErr` `FORBIDDEN`, connection stays open.
- **Membership implies prior auth.** `presenceState` and `leavePresence`
  require an existing membership; they do **not** re-query the database. This
  keeps high-frequency cursor updates off Postgres while preserving the gate at
  the one place a new presence is established.
- **Not a document mutation.** Presence writes no rows, so read-only machine
  tokens *may* be present, and per-row `ownerField` / `authorize`-predicate rules
  do not apply (no documents are involved). The db-level `authorize` gate still
  runs first, as everywhere.
- **Rooms are soft partitions within a database.** Any peer authenticated to the
  db may join any room key. Private rooms are an application concern
  (unguessable room keys) or a v2 ACL; the db-level auth boundary is the trust
  boundary, as it is for documents.
- **Member visibility.** A room's member list is visible only to other members
  of that room, all of whom authenticated to the same db. The list surfaces
  display identity (`AuthedUser`: email/name/github) — appropriate for
  collaborative surfaces that show avatars/names, and the same identity the
  dashboard already surfaces. `connectionId` is an opaque, process-local value
  (not a secret; it resets on server restart).

## Configuration

Boot-only `Config` fields (mirroring the `RTDB_IMAGE_*` / `RTDB_TTL_*` patterns:
truthy default-off on/off, and numeric `.unwrap_or(default)` + clamp), parsed in
`Config::from_env`, threaded through `AppState::new`, consumed via
`state.config.*`:

| env var | default | meaning |
|---------|---------|---------|
| `RTDB_PRESENCE_ENABLED` | `true` | Master switch (default-on, image-transforms-style: anything but `false`/`0`/`no` enables). |
| `RTDB_PRESENCE_MAX_STATE_BYTES` | `1024` | Per-`state` payload cap. |
| `RTDB_PRESENCE_MAX_ROOM_SIZE` | `100` | Members per room. |
| `RTDB_PRESENCE_MAX_ROOMS_PER_CONN` | `32` | Rooms one connection may join. |
| `RTDB_PRESENCE_MAX_ROOM_BYTES` | `256` | Room-name length cap. |
| `RTDB_PRESENCE_BROADCAST_INTERVAL_MS` | `50` | Coalesce flush interval; `0` = immediate. |
| `RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC` | `20` | Per-conn `presenceState` rate cap. |

These are boot-only (not hot-reloadable) to match the rest of the presence
runtime; hot-config is reserved for the four existing `HotConfig` fields.

## Integration points

- **`protocol.rs`** — add `Presence` / `PresenceState` / `LeavePresence` to
  `ClientMessage` and `PresenceSnapshot` / `PresenceErr` to `ServerMessage`;
  add `PresenceMember`. Wire-tag and field-name tests next to the existing ones.
- **`presence.rs`** (new) — `PresenceManager`, `DbPresence`, `PresenceSession`,
  `PresenceConfig`, and `run_presence_flush`.
- **`lib.rs`** — add `presence: Arc<PresenceManager>` to `Realtime`; construct it
  in `AppState::new`; spawn the flush task. (No new route — presence is
  `/sync`-frame-native. An optional admin inspector, if added, mounts via
  `admin_routes()`.)
- **`ws.rs`** — handle the three new `ClientMessage` arms in `handle_text_frame`
  (capturing `authed_user(principal)` at join); add
  `state.realtime.presence.remove_conn(&db, conn_id)` to `handle_socket`'s
  cleanup, immediately after `subs.remove_conn`.
- **`config.rs`** — the seven env knobs.
- **`metrics`** — `rtdb_presence_rooms`, `rtdb_presence_sessions`,
  `rtdb_presence_updates_total`, `rtdb_presence_broadcasts_total` (and the flush
  task's dirty-room count), surfaced on the dashboard metrics page.
- **Optional: `GET /admin/db/{db}/presence`** — operator-dashboard inspector
  (rooms + member counts + a sampled member list). Small; not required for the
  primitive to function.

Presence does **not** touch: `CommitterCtx`, `handle_mutate` /
`handle_scheduled` / `handle_migrate` / `handle_reaper`, the op-feed, audit log,
webhooks, `SubscriptionManager`, document tables, or the storage layer.

## Client mirror (all four clients + harness + matrix)

Per the project rule, any server protocol change is mirrored across all clients.

- **ts-client** (`@par-rt-db/client`) — wire types in `protocol.ts`; low-level
  `presence(room, state?)` / `updatePresence(room, state)` / `leavePresence(room)`
  on the reactive client; a `usePresence(room)` hook (React) returning the live
  `PresenceMember[]` plus `updatePresence`/`leavePresence`, mirroring Convex
  ergonomics. Exported from `./react`.
- **rust-client** (`par-rt-db-client`) — the three wire methods on the reactive
  `ws` client; `PresenceMember` / frame types in `wire.rs`.
- **python-client** (`par-rt-db`) — the three wire methods on `RtDbClient`
  (`pip install par-rt-db[ws]`); frame types in `wire.py`.
- **In-memory harness** (ts `InMemoryRtDbClient` + rust `in_memory` feature) — a
  fake presence layer so app-level collaborative flows are testable with no
  network (sessions tracked by a local conn id; `presence`/`updatePresence`/
  `leavePresence` mutate local room state and fan out to local subscribers).
- **`FEATURE_MATRIX.md`** — new row (Convex presence-component parity, all four
  clients). **`ENHANCEMENTS.md`** — check the ENH-015 box. READMEs and this spec
  stay in sync.

## Testing

- **Server unit (`presence.rs`)** — join/leave/state correctness; idempotent
  re-join; `remove_conn` evicts every room and marks each dirty; caps reject
  explicitly (state size, room size, rooms-per-conn, room-name length);
  coalescing dedups multiple updates into one snapshot per flush tick; update
  rate limit denies without dropping state; feature-disabled returns
  `FORBIDDEN`.
- **Server integration (new `presence_test.rs`)** — two connections authed to
  the same db join one room; each receives a `presenceSnapshot` containing the
  other; a `presenceState` from one produces a snapshot the other observes (after
  a flush); `leavePresence` and abrupt disconnect both remove the member and
  notify the survivor; a third connection authed to a *different* db does not
  see the room.
- **Clients** — wire round-trip (tags/fields byte-identical to the server);
  hook/observer reactivity (ts); the live-server opt-in path mirrors the existing
  `ts-client/tests/integration/**` and `rust-client/tests/http_integration.rs`
  convention (`#[ignore]`, `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`).
- **In-memory harness** — a two-"client" cursor flow with no network.

## Out of scope (v1, explicit)

- Observe-only subscriptions (subscribe to a room without being present).
- Principal/user-bound dedup (one user across N tabs = one entry) — decided
  against in favor of the connection-bound model.
- Delta/diff protocol (full-list snapshots only; deltas are a v2 optimization).
- Persistence / history / "last seen at" (presence is strictly live).
- Cross-database presence (it is per-db).
- Room-level ACL / private rooms (db-level auth is the trust boundary).
- An HTTP presence API (the admin inspector excepted) — presence is live and
  belongs on `/sync`.
- Interaction with per-row `ownerField` / `authorize` predicates (none — no
  documents are involved).

## References

- Architecture invariants and the committer single-writer / tap-site contract:
  `CLAUDE.md` ("Committer is the correctness core", "Op-feed tap").
- Realtime runtime (`Realtime { subs, committers, op_feed }`), `SubscriptionManager`
  shard shape, `ConnId`, `Principal`/`PrincipalCtx`, and the boot-config env-knob
  patterns: `server/src/lib.rs`, `server/src/subs.rs`, `server/src/auth/mod.rs`,
  `server/src/config.rs`, `server/src/ws.rs`.
- Convex parity target: the Convex Presence component (online indicators,
  cursors, typing) — matched in *capability*, improved on in *implementation*
  (no tables, no reaper, liveness for free from the live `/sync` connection).
- Sibling design docs: `2026-08-05-image-transforms-design.md` (a committer-
  bypassing read-time capability — spec structure and safety-section precedent)
  and `2026-08-01-document-ttl-design.md` (the reaper-task pattern the flush task
  adapts, minus the document writes).
