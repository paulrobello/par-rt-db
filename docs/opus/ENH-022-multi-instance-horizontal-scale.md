# ENH-022 — Multi-instance horizontal scale via Postgres LISTEN/NOTIFY

> **Source**: kanban card `[ENH-022]`, project `par-rt-db`. Derived from the 2026-08-09 Opus audit.
> **Impact**: high · **Effort**: large · **Breaking**: no (single-instance behavior preserved)
> **Resolves**: audit finding `ARC-126` (undocumented single-instance constraint)

## Goal

Let par-rt-db run as more than one process behind a load balancer — for availability during a deploy,
and to stop the WebSocket fan-out layer from being pinned to one machine's memory. Today a second
replica **silently breaks OAuth login**, which is the worst kind of scaling limit: undocumented and
failing quietly.

## Current state

Four pieces of `AppState` are per-process in-memory maps, verified against `server/src/lib.rs:85` and
the audit's `ARC-126`:

| State | Where | What breaks with 2 replicas |
|---|---|---|
| `oauth_states` | `lib.rs:85` | **Silent login failure.** `/auth/{provider}/begin` mints a state on replica A; the provider redirects to `/auth/callback`, load-balanced to replica B, which has no such state and rejects. Intermittent, ~50% at two replicas. |
| `op_feed` | `AppState` ring buffer | `/admin/stream` and the dashboard op feed show only the writes that happened to hit *this* replica. |
| `presence` | `presence.rs` | ENH-015's whole premise — "who is online" — fragments per replica. Users on different replicas cannot see each other. |
| `rate_limit` | `rate_limit.rs:18-22` | Effective limits multiply by replica count. |

Explicitly **not** a problem: the committer's single-writer invariant. Each database has one committer
task, and correctness depends on that. **This enhancement must not create a second writer** — see Risks.

Verified: grepping `server/src/` for `LISTEN`, `NOTIFY`, `pg_notify` returns **nothing**, and there is
no read-replica or secondary-pool support.

## Implementation

> Postgres `LISTEN`/`NOTIFY` is the right transport here: the deployment already depends on one
> Postgres, it needs no new infrastructure (unlike Redis), and every payload below is small
> (NOTIFY's 8 KB limit is not a constraint for ids and channel names).

### Stage 0 — Document the constraint first *(ships immediately, closes ARC-126)*

Before any code: add to `deploy/README.md` and `README.md` that par-rt-db is **currently
single-instance**, name the four reasons, and note that the OAuth failure is silent. Add a boot `WARN`
if a replica count is detectable (e.g. an `RTDB_INSTANCE_COUNT` hint, or simply always log
"single-instance mode — do not run multiple replicas"). This is a half-day of work that removes the
sharpest edge and is worth shipping independently of the rest.

### Stage 1 — `oauth_states` to Postgres *(the correctness fix)*

Replace the in-memory map with a table in the existing `rtdb_auth` schema:

```sql
CREATE TABLE rtdb_auth.oauth_states (
  state       TEXT PRIMARY KEY,
  provider    TEXT NOT NULL,
  origin      TEXT,
  created_at  BIGINT NOT NULL,
  session_token TEXT,          -- set by the callback, read by the /auth/state poll
  consumed_at BIGINT
);
```

- `/begin` inserts; `/callback` updates with the minted session token; `/auth/state` selects-and-consumes
  in one statement (`UPDATE … SET consumed_at = … WHERE state = $1 AND consumed_at IS NULL RETURNING …`)
  so single-use is enforced by the database rather than by a map mutation.
- A periodic sweep deletes rows older than the 10-minute TTL. **Gate the sweep the way ENH-011's quota
  warmer is gated** — `ARC-102` is the audit finding about ungated pollers; do not add another.
- This also fixes the audit's `SEC-132` note that the map is unbounded and pruned only opportunistically,
  and gives `SEC-121`'s hardening a place to hang a cookie binding.

**This stage alone makes two replicas viable for login**, which is most of the practical value.

### Stage 2 — Cross-instance op-feed via NOTIFY

- After a committer turn publishes taps, `pg_notify('rtdb_ops', <compact json>)` with `{db, table, op,
  doc_id, ts_ms, source}` — the same shape the op feed already carries.
- Each instance holds one dedicated `LISTEN` connection (a `sqlx::PgListener`), and feeds received
  notifications into its local `op_feed` ring buffer and `/admin/stream` subscribers.
- **Filter self-notifications** by tagging each payload with an instance id, or accept the duplicate and
  dedupe on `(ts_ms, db, doc_id, op)`. Prefer the instance-id tag — cheaper and exact.
- Publish from `publish_taps` (`committer.rs:545`), the single enforcement point `ARC-001` created.
  **Do not add a fifth tap site**; extend the existing helper so the "every durable write publishes
  here" invariant continues to hold by construction.

### Stage 3 — Cross-instance presence

ENH-015's presence is connection-bound: liveness *is* the WebSocket ping/pong, with no heartbeat table.
Preserve that locally and add a gossip layer:

- Each instance NOTIFYs its per-room membership delta on join/leave/update.
- Each instance keeps a per-room map of `{instance_id → members}` and broadcasts the union.
- **Instance death**: a `LISTEN`er cannot observe a peer crashing. Have each instance NOTIFY a
  liveness beat on the existing flush interval (`RTDB_PRESENCE_BROADCAST_INTERVAL_MS`, default 50 ms —
  beat far less often, e.g. every 5 s) and evict a peer's contributions after a few missed beats.
- Keep every existing per-connection safeguard (state bytes, room size, room count, total room bytes,
  update rate). The union must be bounded too — cap total members per room across instances, not just
  per instance.

#### Stage 3 design (settled before implementation)

The gossip layer lives entirely in `presence.rs`, gated on `RTDB_MULTI_INSTANCE` (the same flag Stage 2
added). All four presence ops (`join`/`update_state`/`leave`/`remove_conn`) and the flush loop already
mark rooms dirty; the gossip hook rides the **dirty set**, not the WS handler.

- **Peer shadow map.** `PresenceManager` gains `peers: Mutex<HashMap<(String,String), HashMap<String, PeerSnapshot>>>`
  keyed `(db, room) → instance_id → PeerSnapshot{ members: Vec<PresenceMember>, last_beat: i64 }`.
  This is what other instances have reported for each room; it is read by `flush_once` to build the union
  and never sent to local clients as-is.
- **Publish on dirty.** When a room goes dirty (existing `mark_dirty`), if `multi_instance` is on,
  publish `pg_notify('rtdb_presence', {instanceId, db, room, members})` where `members` is the room's
  **full local member snapshot** (a delta would need reconciliation; a full snapshot per dirty room is
  simpler, idempotent, and small — presence rooms are capped at `max_room_size`). This is best-effort
  like the Stage 2 op-feed NOTIFY. Reuse `notify.rs`'s `pg_notify` plumbing pattern (separate channel).
- **Receive in the listener.** The Stage 2 `run_listener` already holds one `PgListener` per instance.
  Extend it (or add a sibling listener) to `LISTEN rtdb_presence` too: on a notification, store the
  snapshot under `peers[(db,room)][instanceId]` (refreshing `last_beat`), then `mark_dirty(db, room)`
  so the next flush broadcasts the updated union to local members. Skip self-notifications by `instanceId`.
- **Union broadcast.** `flush_once` builds the union = local members ++ all `peers[(db,room)]` members,
  deduped by `(origin_instance_id, connection_id)` (see namespacing below), capped at `max_room_size`
  total. Local members always win the cap (a local member is never dropped in favor of a remote one).
  The union is what local clients receive in `presenceSnapshot`.
- **`connection_id` namespacing.** Local `ConnId` is a per-process integer, so instance A and B can both
  have `conn 1`. Clients identify "me" by `connection_id` (to skip rendering their own cursor/state), so a
  collision would make B's client mistake A's member for itself. **Decision:** in multi-instance mode,
  prefix each member's `connection_id` with its origin instance id (`"{instanceId}:{connId}"`) when
  building the union for the wire. Local-only members stay unprefixed in single-instance mode (unchanged
  behavior). This is wire-compatible — clients treat `connection_id` as opaque — and only active in the
  opt-in multi-instance path.
- **Liveness beats + eviction.** Each instance NOTIFYs a beat on `rtdb_presence` every
  `RTDB_PRESENCE_BEAT_INTERVAL_MS` (default 5000), carrying its full per-room snapshots so a peer that
  missed an incremental update resyncs. The listener refreshes `last_beat` for every notification (beat
  or snapshot). A sweep (riding the existing flush loop) evicts every `PeerSnapshot` whose `last_beat` is
  older than `RTDB_PRESENCE_BEAT_TIMEOUT_MS` (default `3 × beat_interval` = 15000) — drop the entry,
  mark affected rooms dirty so the union broadcast reflects the eviction. This is the "killing A evicts
  its members within the beat timeout" contract: a dead instance stops beating, and within `beat_timeout`
  its shadow entries expire and disappear from peers' unions.
- **Bounded union.** `max_room_size` (existing config) caps the **union** in multi-instance mode (not just
  the local room). Each instance's own local room is still capped at `max_room_size` on join (existing
  guard unchanged), so the raw union is ≤ `N_instances × max_room_size`; the union cap drops the
  highest-`connection_id` remote members beyond the limit. Presence is ephemeral, so dropping a remote
  member means a stale view until the next beat — acceptable, and consistent with "over-approximate
  freely, never under-approximate" (a dropped member reappears on the next snapshot/beat).
- **Single-writer invariant.** Intact — presence is NOT committer-bound (it never was; it's a sibling).
  The gossip layer adds only `pg_notify` publishes + in-memory shadow writes; no `execute_txn`, no
  document write, no committer interaction. A second instance's presence members are mirrored into a
  read-only shadow map, never written to document tables.

### Stage 4 — Shared rate limiting

Simplest correct option: keep the in-memory fixed window per instance but divide configured limits by
an `RTDB_INSTANCE_COUNT` hint. Crude but honest and zero-latency.

Better: a Postgres-backed counter per `(key, window)` with an atomic `INSERT … ON CONFLICT DO UPDATE
… RETURNING count`. That is one round-trip per rate-limited request, which is acceptable for the
storage/admin routes but **not** for the per-frame WS check. Use the divided-limit approach for the WS
path and the shared counter for HTTP.

Fix `SEC-112` (spoofable `X-Forwarded-For` key) **before** this stage — a shared counter keyed on a
spoofable value is worse than a local one, because now an attacker can evict other instances' entries.

### Stage 5 — Deployment surface

- `docker-compose.yml` — document (do not default) a scaled configuration.
- `deploy/README.md` — replace Stage 0's "single instance only" with the real multi-instance runbook,
  including the sticky-session question for `/sync` (not required once presence gossips, but simpler if
  the LB supports it).
- Replace the Stage 0 boot warning with an informational log naming the instance id.

## Writer-funnelling decision *(criterion #7 — settled before Stage 2)*

The single-writer invariant is load-bearing and non-negotiable: `subs.rs` skip-invalidation
assumes writes to a database are serialized, and the committer is what serializes them. The
question multi-instance forces is: **with two processes, which one owns the committer for a
given database?**

**Decision (Stages 1–2):** multi-instance means multiple *readers/connection-holders* behind a
load balancer, **not** multiple writers for the same database. The writer for a database stays
funnelled to **one committer owner**. Specifically:

- **Stage 2 adds notification fan-out only — never a second writer.** `publish_taps` emits a
  `pg_notify('rtdb_ops', …)` after the local `op_feed.publish`, and a per-instance `PgListener`
  feeds received notifications into its own local op-feed ring. No `execute_txn` call site is
  added; the NOTIFY path runs *after* commit, inside the existing committer's serialized turn,
  exactly where the local op-feed/audit/webhook taps already run. A second instance receiving
  the notification re-publishes it into an in-memory ring — it does not re-execute the write.
- **The hard problem — guaranteeing one committer owner per db across processes — is explicitly
  deferred.** `channel_for` spawns a committer per-db-per-process on first request. With two
  instances both receiving writes to the same db, both would spawn a committer and `execute_txn`
  would interleave across processes (READ COMMITTED, no cross-process lock) — a correctness
  catastrophe. A real fix needs a Postgres advisory lock or a lease so the db's committer owner
  is elected and failover is handled; that is a materially larger design and is **out of scope**
  for this enhancement (future stage / separate card).
- **Consequence for safe multi-instance operation:** until write-funnelling lands, a scaled
  deploy must funnel writes — either sticky-session the `/sync` + `/api/*` write paths to one
  owner per db, or run the db on exactly one writer instance with the others read-only. The
  op-feed/presence/rate-limit work in Stages 2–4 is correct and valuable *independent of* the
  funnelling mechanism: a notification that crosses to an instance that happens not to own that
  db's writer is harmlessly published into that instance's local ring (and would, once presence
  ships, reach its own subscribers). Self-notification is deduped by instance id; a cross-talk
  duplicate into a non-owning instance's ring is a cosmetic extra entry, never a write.

This satisfies criterion #7: the op-feed NOTIFY work in Stage 2 is built on the documented
decision that writes stay funnelled to one owner, and Stage 2 itself adds no write path.

## Files to touch

- `server/src/lib.rs` — `AppState`: drop `oauth_states`, add the listener handle + instance id
- `server/src/auth/provider.rs` — `/begin`, `/callback`, `/auth/state` against the new table
- new `server/src/notify.rs` — `PgListener` task, publish helpers, instance id
- `server/src/committer.rs` — `publish_taps` emits `pg_notify`
- `server/src/presence.rs` — gossip layer + peer eviction
- `server/src/rate_limit.rs` — instance-count division and/or shared counter
- `server/src/db.rs` — `rtdb_auth.oauth_states` DDL at boot
- `server/src/config.rs`, `.env.example`, `docker-compose.yml` — `RTDB_INSTANCE_COUNT`,
  `RTDB_INSTANCE_ID`, presence beat interval
- `deploy/README.md`, `README.md`, `DESIGN.md`, `FEATURE_MATRIX.md`, `CHANGELOG.md`

**No client mirror required** — no wire, DSL, or protocol change. State that explicitly in the PR.

## Verify

```bash
make -C /Users/probello/Repos/par-rt-db dev-db-up
make -C /Users/probello/Repos/par-rt-db checkall > /tmp/enh022.log 2>&1; echo "EXIT=$?" >> /tmp/enh022.log
grep '^EXIT=' /tmp/enh022.log
make -C /Users/probello/Repos/par-rt-db env-drift-check
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml oauth
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml presence
cargo test --manifest-path /Users/probello/Repos/par-rt-db/server/Cargo.toml notify
```

Two-instance integration check (the whole point — a single-process test cannot prove any of this):

```bash
# Two servers, one Postgres, distinct ports
RTDB_PORT=8300 RTDB_INSTANCE_ID=a cargo run &   # from server/
RTDB_PORT=8301 RTDB_INSTANCE_ID=b cargo run &
# 1. begin OAuth on :8300, complete the callback against :8301 -> login succeeds
# 2. mutate against :8300 -> the op appears on :8301's /admin/stream
# 3. presence join on :8300 -> visible to a subscriber on :8301
```

**Acceptance criteria** (mirror these onto the card):
1. `make checkall` green; single-instance behavior unchanged (all existing tests pass untouched).
2. An OAuth flow begun on instance A and completed on instance B **succeeds** — the headline fix.
3. `rtdb_auth.oauth_states` enforces single-use consumption at the database level (a second
   `/auth/state` poll for the same token returns nothing), and expired rows are swept.
4. A durable write on instance A appears in instance B's `/admin/stream` within one second, exactly
   once (no self-notification duplicate).
5. Presence members join on A and are visible to a subscriber on B; killing A evicts its members from
   B's view within the configured beat timeout.
6. **The single-writer invariant is intact** — still exactly one committer task per database per
   process, and no code path executes `execute_txn` outside it. Assert by review *and* by the existing
   committer/subs test suites passing unchanged.
7. `make env-drift-check` passes for every new `RTDB_*` key.
8. Stage 0's documentation exists and is superseded correctly by the Stage 5 runbook.

## Rollback

Staged by design, and each stage is independently revertible:

- **Stage 0** is documentation — no rollback needed, and worth shipping alone.
- **Stage 1** replaces an in-memory map with a table. Reverting restores the map; in-flight OAuth
  states (10-minute TTL) are lost, which surfaces as "please sign in again". Acceptable.
- **Stages 2–4** are additive listeners. Add `RTDB_MULTI_INSTANCE=false` (default) so the NOTIFY
  publish and LISTEN task are inert unless opted in — then rollback is a config flip, not a deploy.

**Ship Stage 0 and Stage 1 first and independently.** They deliver the correctness fix and most of the
practical value; Stages 2–4 are the optimization.

## Risks

- **The single-writer invariant is the thing that must not break.** `CLAUDE.md` is explicit: never call
  `execute_txn` outside the committer, never add a second writer. This enhancement adds *notification*
  fan-out, not write fan-out. Two instances each running a committer for the *same* database would be a
  correctness catastrophe (the skip-invalidation logic in `subs.rs` assumes serialized writes). If
  multiple instances can each own a committer for a given database, you need a lease — that is a
  materially larger design and is **out of scope here**. Until then, multi-instance means multiple
  *readers/connection-holders*, with writes for a given database still funnelled to one owner.
  **Decide and document how that funnelling works before Stage 2**, or the op-feed work is built on
  sand.
- **NOTIFY delivery is at-most-once and not durable.** A missed notification means a stale op feed or
  presence view, never lost data. Acceptable for these three surfaces; would not be for anything durable.
- **Listener connection churn.** `PgListener` holds a dedicated connection per instance — account for it
  against `pool_max_connections` (75), which `ARC-102` notes is already under pressure from idle pollers.
- **`SEC-112` must land first** for Stage 4, per above.
