# Architecture — par-rt-db server internals

How the server is actually built: the committer, the per-database background
tasks, the data pipeline, transports, auth, storage, quotas, and the admin
surface. Written for contributors and agents changing server internals;
[`../CLAUDE.md`](../CLAUDE.md) holds the agent guidance and the invariant list,
and the root [`../README.md`](../README.md) holds the HTTP/WS surface.

The protocol and semantics are defined by the code and by three documents that
track it: [`../README.md`](../README.md) (HTTP/WS surface, DSL, configuration),
[`../FEATURE_MATRIX.md`](../FEATURE_MATRIX.md) (the Convex-parity contract), and
[`../wire-corpus/README.md`](../wire-corpus/README.md) (the executable semantics
corpus every implementation must pass). The 2026-07-21 design spec
([`superpowers/specs/2026-07-21-par-rt-db-design.md`](superpowers/specs/2026-07-21-par-rt-db-design.md))
is a historical record of the original design, not a current reference.

Unless a path says otherwise, bare `.rs` file references in this document are
relative to `server/src/`.

## Table of contents

- [The single-writer committer](#the-single-writer-committer)
- [Per-database background tasks](#per-database-background-tasks)
- [Data pipeline](#data-pipeline)
- [Image transforms](#image-transforms)
- [Transports and rate limiting](#transports-and-rate-limiting)
- [Multi-instance coordination](#multi-instance-coordination)
- [Auth](#auth)
- [File storage](#file-storage)
- [Per-database resource quotas](#per-database-resource-quotas)
- [Realtime presence](#realtime-presence)
- [Wire contract and clients](#wire-contract-and-clients)
- [Admin surface and the op-feed tap](#admin-surface-and-the-op-feed-tap)
- [Backups and restore](#backups-and-restore)
- [Hot config and dynamic CORS](#hot-config-and-dynamic-cors)
- [Static SPA hosting](#static-spa-hosting)
- [Performance characteristics](#performance-characteristics)

## The single-writer committer

The correctness core lives in `committer/` + `subs.rs`. Each database has one
task that serializes all writes, then — before dequeuing the next message —
re-runs affected subscriptions, diffs against the last pushed value, and pushes
only on change. Subscription registration rides the same queue.

**This serialization is load-bearing**: `execute_txn`/`execute_query` run READ
COMMITTED with no row locking. Never call `execute_txn` outside the committer;
never add a second writer.

One write, end to end:

```mermaid
sequenceDiagram
    participant T as Transport (ws.rs / http_api.rs)
    participant C as Committer task (per-db)
    participant PG as Postgres
    participant S as Subscribers

    T->>C: CommitterRequest::Mutate (via Committers::mutate)
    Note over C: the queue is the serialization —<br/>one message at a time, no locking needed
    C->>PG: execute_txn (READ COMMITTED)
    PG-->>C: write set
    C->>C: publish_taps — op feed, audit log,<br/>webhook outbox, cross-replica NOTIFY
    C->>PG: fan_out: re-run affected subscriptions
    C->>S: push only the queries whose value changed
    C-->>T: TxnOutcome
    Note over C: only now does the next message dequeue
```

Taps and fan-out both happen *before* the next message is dequeued. That
ordering is why a subscriber can never observe a write out of order with
respect to a later one.

```mermaid
graph TD
    subgraph transports["Two transports, one vocabulary"]
        WS["/sync WebSocket — ws.rs"]
        HTTP["One-shot HTTP — http_api.rs"]
    end

    CO["Committer task (per-db, single writer)<br />committer/ + subs.rs"]

    WS -->|"CommitterRequest::Mutate"| CO
    HTTP -->|"Committers::mutate (same arm)"| CO

    subgraph arms["Eight handle_* arms — every durable-write path"]
        A1["handle_mutate"]
        A2["handle_scheduled"]
        A3["handle_migrate"]
        A4["handle_reaper"]
        A5["handle_push_schema"]
        A6["handle_restore_schema"]
        A7["handle_merge_users"]
        A8["handle_workflow_advance"]
    end

    CO --> arms
    PT["publish_taps — the single enforcement point"]
    arms --> PT

    PT --> OP["Op-feed ring (op_feed.rs)<br />→ /admin/stream + dashboard"]
    PT --> AU["Audit log (rtdb.audit_log)"]
    PT --> WH["Webhook outbox (webhook_deliveries)"]
    PT -->|"pg_notify rtdb_ops<br />(RTDB_MULTI_INSTANCE)"| NT["Peer replicas (notify.rs)"]

    CO -->|"fan_out: re-run affected subs,<br />push only on change"| SUBS["Subscribers"]

    subgraph tasks["Per-db background tasks — never write document tables"]
        SC["Scheduler — scheduler.rs"]
        RE["TTL reaper — reaper.rs"]
        WF["Workflows poller — workflows.rs"]
    end

    SC -->|"claim/reset side table,<br />enqueue RunScheduled"| CO
    RE -->|"enqueue RunReaper"| CO
    WF -->|"enqueue RunWorkflowAdvance"| CO

    classDef core fill:#1E1E1E,stroke:#4CAF50,stroke-width:3px,color:#E6E6E6
    classDef arm fill:#1E1E1E,stroke:#2196F3,stroke-width:2px,color:#E6E6E6
    classDef tap fill:#1E1E1E,stroke:#FFC107,stroke-width:2px,color:#E6E6E6
    classDef task fill:#1E1E1E,stroke:#F44336,stroke-width:2px,color:#E6E6E6
    classDef plain fill:#1E1E1E,stroke:#78909c,stroke-width:1px,color:#E6E6E6
    class CO,SUBS core
    class A1,A2,A3,A4,A5,A6,A7 arm
    class PT,OP,AU,WH,NT tap
    class SC,RE,WF task
    class WS,HTTP plain
```

### Skip-invalidation classes

Invalidation skips soundly for three read-set classes (any doubt
over-approximates to re-run, so it never under-approximates):

- **`get(id)` point reads** (`subs::ReadSet::Point`) skip when the txn's
  `WriteSet.docs` doesn't contain their `(table, id)`.
- **`count`/`collect`/`unique` on a btree index's eq-prefix** (+ optional range
  bound) (`subs::ReadSet::Indexed`) skip when every written doc is provably
  outside their window — `WriteSet.doc_values` carries each written doc's
  before/after state so `fan_out` evaluates `Window::contains` per written doc
  (deleted ⇒ re-run; `count` is membership-only, `collect`/`unique` are
  content-bearing).
- **`take(N)`/`first`/`paginate`** (`subs::ReadSet::Ordered`) pair that window
  with the sort key of the last computed result's final row — the boundary —
  and skip a written doc that is outside the window or ranks beyond the
  boundary in both its before- and after-state (it can neither be in the top N
  nor displace a member, since displacement requires a member's own write). The
  boundary is seeded at registration and refreshed on every re-run; an unfull
  result (fewer than N docs, or a page with no next cursor) leaves it unset ⇒
  plain membership. Ranking needs `created_at`, which `doc_values` carries (a
  `Delete` captures none ⇒ deletes always re-run).

`distinct`/`aggregate`/`search`/`vector`/`hybrid` stay table-level by design —
their results depend on member values or a ranking function (over-approximate
freely, never under-approximate).

### Safety nets

A wrong skip is otherwise silent (no error — the client just never hears), so
two nets guard the logic:

- `cmp_binds` is structured so its outer match is exhaustive over `EqBind`,
  making a new variant a compile error instead of a `_ => None` that
  under-approximates (do not collapse it back into `match (a, b)`).
- `RTDB_SUBS_VERIFY_SKIP_EVERY` (boot, **ships ON at a 1000 sampling interval
  by default**; 0 = off) shadow-verifies 1 skip in every N by re-running the
  query anyway and diffing against the last pushed result — a divergence logs
  at ERROR, increments `rtdb_subs_missed_pushes_total`, and pushes the
  corrected result.

`fan_out` also records skip/re-run counts per read-set class
(`rtdb_subs_skips_total{class=point|indexed|ordered}`, `rtdb_subs_reruns_total`),
surfaced on the dashboard metrics page. Any non-zero `subsMissedPushesTotal` is
a correctness defect, not a tuning signal.

### Schema migrate rides the same queue

Schema migrate is its own committer request arm (`CommitterRequest::RunMigrate`,
handled by `handle_migrate`), one of the eight that publish taps:
`POST /admin/db/{db}/migrate` enqueues an ordered `Directive` list and the
committer executes the DDL + DML in its serialized turn, then `fan_out`s and
publishes at the same tap sites — so the op-feed/audit/webhook "every durable
write publishes here" guarantee extends to migrate (see `migrate.rs` and the
2026-07-31 spec).

## Per-database background tasks

`Committers::channel_for` spawns the per-db task set: committer, scheduler,
TTL reaper, mutation-log cleanup, and the storage-quota cache warmer. All the
non-committer tasks follow the same rule: **they never write document
tables** — they only claim/enqueue work
back through the committer, preserving the single-writer invariant.

### Scheduler and durable workflows (`scheduler.rs`, `workflows.rs`)

The scheduler is a second per-db task, not a second writer: a timer
(`run_scheduler`) drains the `scheduled_txns` side table of
`(due_at, txn)` rows. It writes ONLY that side table (claim/reset) and enqueues
each due job as a `CommitterRequest::RunScheduled`; the committer's `RunScheduled`
arm (`handle_scheduled`) executes it via the normal `execute_txn` +
`subs.fan_out` path and finalizes the row. Delivery is at-least-once; one-shot
catches up if past due, cron skips missed windows.

**Durable declarative workflows (FM-29, `workflows.rs`)**: the same timer task
also polls the per-db `workflows` side table and enqueues each due run as a
`CommitterRequest::RunWorkflowAdvance`; the committer's arm
(`handle_workflow_advance`) executes the current step's txn via the normal
`execute_txn` + `subs.fan_out` path — steps fire as the system (bypass)
principal like scheduled jobs — records the step outcome, and applies
`StepRetry` backoff and `sleepBeforeMs` before re-arming. Still at-least-once
per step; a crash mid-advance leaves the row `running`, and scheduler startup
`reset_running` re-arms it.

A step is either a txn or an `awaitSignal {name, timeoutMs?}` wait (exactly
one per step). An `awaitSignal` step parks the run in a non-terminal
`waiting` state; inside the same advance arm, `waited_since` is the
discriminator — NULL means first arrival (park: set `wait_name`,
`waited_since`, and the `sleep_until` timeout gate), set-and-expired means
timeout (a failed attempt through the step's `retry`, re-parked with a fresh
full `timeoutMs`), and a present `signal_payload` means the signal won
(consumed atomically with the step boundary; the payload is recorded on the
step outcome, latest-wins). `claim_due`/`next_due` admit `waiting` rows.
Signal delivery (`deliver_signal`, from the HTTP/WS/admin handlers) is a
single conditional side-table UPDATE flipping the `waiting` row to `pending`
with the payload — the same side-table-only precedent as `cancel`. There is
**no new committer arm and no new tap site**: `awaitSignal` steps write no
documents, so the op-feed/audit/webhook tap enumeration is unchanged.

**Cold-db liveness**: every workflows surface ensures the side table inline
(admin handlers + the client list/cancel arms — the table is otherwise ensured
only at scheduler startup), and all three standalone start surfaces (WS
`startWorkflow` frame, `POST /api/workflows`, admin create) call
`Committers::ensure_spawned` + `workflows::ensure_table` BEFORE insert, so a
workflow started on a cold db (no per-db tasks yet) both starts and advances.

**Task lifecycle**: the poller tasks (scheduler, mutation-log cleanup, TTL
reaper, quota warmer) self-terminate when they detect their database has been
deleted (a `db::database_exists` check on a poll error, after `DROP SCHEMA`
removes their tables), so `delete-db` (`POST /admin/delete-db`, typed
`{name, confirm}` guard) retires the per-db tasks cleanly instead of leaving
perpetual error-log spam — and dropping their channel senders lets the
reactive committer task exit too.

**Idle-database reclamation** (`RTDB_DB_IDLE_RECLAIM_SECS`, default 0 = off,
ARC-102 step 4): when non-zero, a server-wide background sweep retires a
database's five per-db tasks once it has had no client activity for that long
AND has no live subscriptions AND no pending scheduled jobs or workflows
(`scheduler::next_due` and `workflows::next_due` are both `None`) — reusing the
same evict-channel + `Shutdown` cascade as `delete-db`, so the single-writer
invariant and the pollers' `committer_tx.closed()` exit all hold. Steps 1–3
already gate each poller's per-tick work (scheduler skips `claim_due` when
nothing is due; reaper skips when no table declares a ttl; mutation-log cleanup
backs off after a zero-row sweep), so reclamation is opt-in task-slot release,
not a correctness fix; the next request respawns the tasks via `channel_for`.
A db with a live subscription, a pending cron/one-shot, or a pending workflow
is never reclaimed.

### TTL reaper (`reaper.rs`)

A third per-db task, not a third writer: a periodic reaper (`run_reaper`)
enqueues a fire-and-forget `CommitterRequest::RunReaper` every
`RTDB_TTL_SWEEP_INTERVAL_SECS` (boot, default 60). It writes NOTHING — it only
does a `database_exists` keepalive + `send(RunReaper)`. The committer's
`RunReaper` arm (`handle_reaper`) batch-deletes rows whose declared `ttl.field`
value is past (`DELETE … WHERE f_<field> < now() LIMIT RTDB_TTL_BATCH`, default
5000) inside its serialized turn, then publishes through the same tap sites as
`handle_mutate`/`handle_scheduled`/`handle_migrate` with `source = "ttl"`,
`owner = None` (system-initiated — bypasses per-row `ownerField` like scheduled
jobs). Same self-termination lifecycle (db-deletion `database_exists` check +
channel-close). Never write document tables from the reaper task — the delete
runs only in `handle_reaper` inside the committer turn.

## Data pipeline

`schema.rs` → `ddl.rs` → `txn.rs`/`query/`. A pushed schema compiles to
Postgres DDL — one typed column per indexed field, documents stored as `doc`
jsonb with system fields merged in at read time, schema changes additive-only.

```mermaid
graph LR
    subgraph perdb["Per-database schema (created by db::create_database)"]
        direction TB
        DOC["Document tables — one typed column per indexed field<br />+ doc jsonb (system fields merged at read time)"]
        META["meta — live derived schema (key/value jsonb)"]
        SCHED["scheduled_txns — (due_at, txn) rows the scheduler claims"]
        WFS["workflows — durable runs (snapshotted spec + step outcomes)"]
        STOR["storage — blob rows (sha256/size/content_type,<br />inline bytes for legacy blobs)"]
        CHUNK["storage_chunks — 1 MiB bytea chunks (streamed uploads, ENH-021)"]
        MUT["mutations — idempotency-key replay log"]
        HIST["schema_history — captured schema versions (push/migrate/restore)"]
    end

    subgraph global["Server-wide schemas"]
        direction TB
        AUTH["rtdb_auth.* — databases, users, sessions, admin_sessions,<br />allowlist, machine_tokens, admins, oauth_states"]
        RATE["rtdb_auth.rate_counters — fleet-wide rate budgets<br />(one ceiling per route/token/db/ip)"]
        FWD["rtdb_auth.forward_queue — spooled forward requests + replies<br />(the NOTIFY carries only a row id)"]
        SIDX["rtdb.storage_index — storage id → owning db"]
        CFG["rtdb_config — single-row hot config"]
        AUD["rtdb.audit_log — best-effort per-DocOp rows"]
        WHK["rtdb.webhooks + webhook_deliveries — registrations + outbox"]
    end

    STOR -.->|"public GET /storage/{id} resolves the owning db"| SIDX

    classDef doc fill:#1E1E1E,stroke:#4CAF50,stroke-width:3px,color:#E6E6E6
    classDef side fill:#1E1E1E,stroke:#2196F3,stroke-width:2px,color:#E6E6E6
    classDef glob fill:#1E1E1E,stroke:#FFC107,stroke-width:2px,color:#E6E6E6
    class DOC doc
    class SCHED,WFS,STOR,CHUNK,MUT,HIST,META side
    class AUTH,RATE,FWD,SIDX,CFG,AUD,WHK glob
```

- **Indexes**: a btree index may declare `unique` + a partial `where` predicate
  (compiles to `CREATE [UNIQUE] INDEX … [WHERE …]`); a uniqueness violation
  surfaces as the `CONFLICT` (409) error code.
- **Defaults** (FM-32): a table may declare `defaults`, a map of field name →
  literal JSON value (additive wire key, omitted when empty): push-validated
  (key is a declared field, value non-null and type-correct via the same
  `validate_value` the write path uses) and stamped onto a new document that
  omits the key — insert/replace/upsert-insert only, never
  `patch`/upsert-update/patchByQuery/snapshot replay — with the ttl
  `defaultDurationMs` stamp and the principal stamps (`ownerField`, authorize
  `$user`) winning over a defaults entry on the same field; migrate
  `renameField` re-keys the map and `dropField`/`changeType` drop entries.
- **Server-stamped `updatedAtField`** (FM-36): a table may name a declared
  `number`/`int64` field (additive wire key, omitted when unset;
  push-validated — declared, numeric, and distinct from `ttl.field`) that the
  server stamps with the current epoch-ms on every version-bumping write:
  insert, patch, replace, upsert (both branches), patchByQuery, and cascade
  `setNull`, overwriting any client-supplied value (the `ownerField` authority
  model). The stamp's value form follows the field's wire convention — a JSON
  number on `number`, a decimal string on `int64` — and it wins over a
  `defaults` entry on the same field (same authority family as the ttl
  default). It sits in the doc body and, when the field is indexed, the typed
  column — so it is orderable with a declared index. Deliberately NOT stamped:
  snapshot replay (`insert_snapshot_row` preserves the stored value verbatim)
  and the anon→real merge restamp (an administrative ownership rewrite, not a
  content write).
- **Server-assigned `autoIncrementField`** (FM-37): a table may name a declared
  `int64` field (additive wire key, omitted when unset; push-validated —
  declared, exactly `int64`, and distinct from `ttl.field` and
  `updatedAtField`) that the server stamps from a per-table standalone
  Postgres sequence (`seq_<table>`, created by `ddl::apply_sequence` when the
  declaration is new) on the two insert paths — `Insert` and upsert's insert
  branch — overwriting any client-supplied value. `nextval` is
  non-transactional, so rolled-back txns leave gaps (sequences are monotonic,
  not gap-free). Immutable after insert: `apply_patch` (the one seam behind
  patch, upsert-update, and patchByQuery) and `do_replace` reject a changed
  value with `BAD_REQUEST`; an omitted replace field is filled from the stored
  row, and round-tripping the stored value is allowed. A replace of a doc that
  predates the declaration may set the value once (first-set). Snapshot import
  replays values verbatim then repositions each sequence past the imported max
  (`ddl::reposition_sequence`, forward-only — it also runs when a declaration
  is added to a populated table); the destructive reconcile drops sequences
  for dropped tables / removed declarations, and migrate's `renameTable` /
  `dropTable` rename / drop the sequence physically while `renameField` /
  `dropField` follow / clear the declaration.
- **Cascade delete + soft delete** (FM-33): an `id` field may declare
  `onDelete: cascade|restrict|setNull` (legal top-level or one `optional`
  deep) and a table may declare `softDelete` — both enforced **in the app layer
  inside `execute_txn`** (`delete_row_cascade`), deliberately NOT SQL FKs, so
  every cascaded/stamped/nulled row records its own `DocOp`/`WriteSet` entry
  (subscriptions/op-feed/audit/webhooks fire per row): the walk is
  children-first in BTreeMap order with a visited-set cycle guard, `restrict`
  conflicts naming the referencing `table.field`, `setNull` patches the child
  field to null (key removed from the doc body, version bumps), and one shared
  per-initiating-step budget (`MAX_CASCADE_ROWS`, 10_000) turns an over-budget
  cascade into a rolled-back `CONFLICT`. `Delete` on a `softDelete` table
  stamps a physical `deleted_at` column instead of removing the row (a stamped
  row never triggers further cascade — recursion stops there); every read
  terminal (incl. `get`/`search`/`vector`) composes `deleted_at IS NULL`,
  unique indexes widen their partial predicate to live rows only (and rebuild
  when the flag is added), the TTL reaper always hard-deletes (through the same
  cascade walk, `force_hard`), and an `Undelete` step (wire `undelete`,
  softDelete tables only) clears the stamp — idempotent-`Ok` on an already-live
  row, patch-shaped `DocOp`.

A `Transaction` is an ordered list of `Step`s:

- **Per-id steps**: `Insert`/`Patch`/`Replace`/`Delete`/`Undelete`/
  `ExpectVersion`/`ExpectAbsent`/`Upsert`.
- **Predicate-driven bulk steps**: `PatchByQuery{table,filter,patch,limit?}`
  and `DeleteByQuery{table,filter,limit?}` find rows matching a `FilterExpr`
  (the same predicate the read `.filter()` accepts) and act on them in one
  serialized committer turn. Row-visibility matches the read path exactly
  (`compile_scan_where` composes the client filter with the
  `ownerField`/`collaboratorsField`/`authorize` predicates), so an interactive
  caller touches only rows it could read; each affected row records a
  `DocOp`/`WriteSet` entry so subscriptions, op-feed, audit, and webhooks fire
  per row.
- **Scheduling steps**: `Schedule{when,txn}` inserts the `scheduled_txns` row
  on the open sqlx tx (atomic with the txn's writes) after a recursive
  table-allowlist check (`authorize_txn_tables`, also run at all three
  standalone enqueue surfaces), and `CancelSchedule{id}` removes a pending job
  in-txn; fire-time semantics unchanged.
- **Workflow steps** (FM-29): `StartWorkflow{spec}` inserts the per-db
  `workflows` side-table row on the open sqlx tx (atomic with the txn's writes)
  after the same recursive table-allowlist check over every step's txn
  (`authorize_spec_tables`, also run at all three standalone start surfaces: WS
  frame, `POST /api/workflows`, admin create — steps fire later as the system
  principal, so submit time is the only scoped check), and `CancelWorkflow{id}`
  flips a non-terminal run to `cancelled` in-txn.

Limits: `MAX_STEPS` (1024) bounds step count; `MAX_BY_QUERY_ROWS` (1000)
bounds rows per by-query step; `MAX_TAKE` (4096) bounds result/group counts
(scale via `paginate`, not larger collects). The `aggregate` terminal supports
`sum`/`avg`/`min`/`max`/`count` (the last counts rows, consumes no aggregate
field; grouped `count` is the count-per-group the dashboards need).

The read and write paths share index-value typing; keep them aligned.

## Image transforms

Read-time, HTTP-only (`image_transform.rs`): pure-Rust decode → resize →
re-encode over the `image` crate, served from a byte-weighted `TransformCache`
(`moka`) with a decode `Semaphore` and pixel cap. Wired into `serve_bytes` on
both `GET /storage/{id}` and `GET /api/storage/{db}/{id}`; no committer,
protocol, or WS involvement. See also [File storage](#file-storage) for the
query parameters and gating env vars.

## Transports and rate limiting

`protocol.rs`, `ws.rs`, `http_api.rs`. `ws.rs` is the reactive WebSocket
handler, `http_api.rs` is one-shot query/mutate. **Both route mutations
through `Committers::mutate`** so subscriptions fire regardless of which
transport wrote.

HTTP requests are optionally rate-limited per machine-token and per-db
(`RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`, 0 =
off/default; over-limit → 429 `RATE_LIMITED` + `Retry-After`; in-memory
fixed-window `RateLimiter` on `AppState`, checked after `authorize`;
rate-limit keys are namespaced by route as well as principal, so a burst
against one endpoint cannot exhaust another endpoint's budget; the same
limiter also covers inbound WS `Mutate`/`Subscribe` frames (after the per-op
`authorize` re-run): a denial replies with a `RATE_LIMITED` `MutateErr`/
`SubscribeErr` carrying `retryAfter` and the connection stays open; the
per-connection WS frame cap (200 msgs/10s, closes the socket) is a separate
coarse flood valve).

## Multi-instance coordination

Under `RTDB_MULTI_INSTANCE` (default false) several server processes share one
Postgres and coordinate entirely through it — no consensus layer, no extra
service, and no second writer. Everything below is inert in a single-instance
deploy: the publish taps skip the NOTIFY calls and the listener tasks are never
spawned.

### The ownership lease

`committer/lease.rs`. `db_ownership_key(db)` derives a stable `i64` from the
first eight bytes of the database name's SHA-256. `acquire_ownership_lease`
opens a dedicated pool with `max_connections(1)`, `min_connections(1)`, and no
idle or lifetime timeout, then runs `pg_try_advisory_lock(key)` on it. The
caller runs that database's committer **and all of its pollers** on this pool,
so the lease and every write share one Postgres backend.

That single-backend coupling is what makes the guarantee structural rather than
procedural:

- No other replica can acquire the lock mid-flight, so split-brain is
  impossible by construction — there is no fencing token to get wrong.
- An owner's death (`kill -9`, container stop, a partition that drops the
  session) ends the backend's session, which releases the advisory lock.
  Failover is the next replica's ordinary acquire path; there is no separate
  recovery routine.
- A failed acquire holds nothing: the connection returns to its pool and is
  closed with it.

A replica that does not hold the lease runs a **shadow** committer for that
database. A shadow serves reads, live subscriptions, and presence normally. It
never calls `execute_txn`.

### Forwarding a write to the owner

`committer/forwarding.rs` (origin side) and `forward.rs` (transport). A write
submitted to a shadow committer is forwarded:

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Shadow replica
    participant Q as rtdb_auth.forward_queue
    participant O as Owner replica

    C->>S: mutate
    S->>Q: INSERT request row (payload jsonb)
    S->>O: NOTIFY rtdb_write_fwd (row id only)
    Note over O: every replica reads the row;<br/>non-owners drop it silently
    O->>O: execute inside the committer turn
    O->>Q: INSERT reply row (TxnOutcome)
    O->>S: NOTIFY rtdb_write_replies (row id only)
    S->>Q: DELETE … RETURNING (claim = target filter)
    S->>C: owner's outcome
    S->>S: re-run local subscriptions against the write set
```

Exactly one execution happens per forwarded write regardless of fleet size:
every replica reads the request row, and every replica that does not hold the
lease on `request.db` drops it.

**Why the spool.** A `pg_notify` payload is capped at 8000 bytes and Postgres
rejects anything larger. A full `ForwardRequest` can carry up to 1024
transaction steps, an entire `SchemaDef`, or a `MigrateRequest`, and a reply
carries a whole `TxnOutcome` — all of which routinely exceed the cap. Sending
only a 36-byte row id and keeping the body in `rtdb_auth.forward_queue`
removes the size ceiling entirely, and it makes forwarded work durable across a
listener reconnect. `run_forward_sweeper` reclaims rows a crashed consumer left
behind.

**Trust.** The `PrincipalCtx` that authorized the write at the edge travels in
the payload and the owner reuses it verbatim, so per-row `ownerField` /
`authorize` rules evaluate against the identity that actually made the call.
NOTIFY is writable only by sessions holding Postgres credentials — the same
trust domain as the tables themselves — so a peer that could forge a forwarded
write could already write directly. Rate limits stay enforced at the origin
(the client-facing edge); quotas stay enforced inside the owner's turn.

### Takeover and idempotency

If no owner answers within `RTDB_FORWARD_TIMEOUT_MS` (default 5000, clamped to
a 100 ms floor), the origin attempts the lease takeover itself and re-submits
the request. That is the same acquire path as any other failover.

The re-submit is what makes idempotency necessary: an owner that executed and
then died, or replied late, would otherwise have its write applied twice. So a
forwarded `Mutate` carrying no client idempotency key is given a server-minted
UUIDv7 key, threaded through both the forward payload and the takeover
re-submit. The dedup table is shared by every replica, so the takeover replays
the owner's first outcome instead of writing again. The non-`Mutate` arms are
idempotent by construction.

What the *client* observes is still ambiguous in the timeout window — the write
may have committed even though the caller saw `CONFLICT`. A caller that must
distinguish the two sends its own idempotency key and retries with it.

### Cross-replica subscription invalidation

`notify.rs`, `publish_write_set` / `run_write_set_listener`. Without this, a
subscriber connected to replica B would never learn about a write the owner
executed on replica A — a silently stale live query, which is the one failure
mode this project treats as a correctness defect rather than a tuning signal.

Every durable write publishes its write set on `rtdb_write_sets`, and each
replica re-runs the subscriptions that write touched. Scheduled jobs, TTL
expiry, and migrations invalidate the same way, because they all publish at the
same tap sites.

Two details shape what the receiving replica can do with the payload:

- **Size.** Write sets serializing above `WRITE_SET_INLINE_LIMIT` (7500 bytes,
  leaving headroom under the 8000-byte cap for multi-byte escaping) travel
  through the forward spool instead of inline.
- **Values.** `WriteSet::doc_values` is `#[serde(skip)]`, so document values
  never cross the wire. `Indexed` and `Ordered` subscriptions on the receiving
  replica therefore fall back to their conservative "unrankable ⇒ re-run" path.
  The cost is extra re-runs; a missed push is not possible.

### Everything else that coordinates

| Channel / table | Carries | Module |
| --- | --- | --- |
| `rtdb_ops` | Op-feed events, so `/admin/stream` on any replica shows every replica's writes | `notify.rs` (`publish_ops` / `run_listener`) |
| `rtdb_presence` | Per-room member snapshots (gated on presence also being enabled) | `notify.rs` (`run_presence_listener`) |
| `rtdb_write_sets` | Subscription invalidation | `notify.rs` |
| `rtdb_write_fwd` / `rtdb_write_replies` | Forward request and reply row ids | `forward.rs` |
| `rtdb_auth.rate_counters` | One rate-limit ceiling per route/token/db/ip across the fleet, rather than one per process | `rate_limit.rs` |
| `rtdb_auth.forward_queue` | Forward request and reply bodies | `forward.rs`, `db.rs` |

Sessions and OAuth state need no channel — they already live in `rtdb_auth`.

**Self-dedupe.** Postgres delivers a session's own `pg_notify` back to that
session, so every payload carries its origin `instance_id` and every listener
skips its own echo. Without it a local write would land in the op-feed ring
twice and double `/admin/stream`'s event count.

## Auth

`auth/`: per-database machine tokens and OAuth sessions (GitHub, Google,
GitLab, Microsoft, Apple, and generic OIDC, each optional via the
`OAuthProvider` trait). The WS handler **re-runs `authorize` on every Subscribe
and Mutate** — not just at connect — so revocation, allowlist changes, and
session expiry take effect on open connections.

**One identity resolver, six providers.** Every completed login goes through
`auth::resolve_user`, which keys on the provider's own stable subject column
(`github_id`, `apple_sub`, `microsoft_sub`, `google_sub`, `gitlab_id`,
`oidc_sub`) — never on the email, which providers let users change. Resolution
is three ordered steps: reuse the row carrying this subject (refreshing
`login`/`email`, so a provider-side email change follows the account instead of
forking a second one); else link an existing row with the same verified email
whose subject column is still NULL (this is what links one person across two
providers, and what adopts a row created before a provider had a subject
column); else insert. The email-link step is skipped for Microsoft unless the
tenant asserts `xms_edov` — SEC-102's nOAuth defense. The subject columns are
nullable and never backfilled: a pre-existing user's first login after a column
is added takes the link step, and every later login matches at step one.

### Per-row authorization

On top of the db-level gate:

- **`ownerField`** (`schema.rs`, enforced in `query/`/`txn.rs`/`subs.rs`): an
  authenticated user reads/mutates only rows they own, or rows that list them
  in a declared `collaboratorsField` (an optional array-of-strings field) —
  owner OR collaborator (inserts are server-stamped; `patch`/`replace`/
  `delete`/`upsert`-update pre-check ownership inside the serialized txn →
  `Forbidden`/403; subscriptions re-filter to the subscriber's owner on every
  `fan_out`).
- **`authorize`** (a `FilterExpr` predicate over doc fields plus
  `$user`/`$email` principal markers; design at
  [`superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md`](superpowers/specs/2026-08-02-per-row-auth-predicate-dsl-design.md))
  generalizes this to any declarable rule, enforced on reads (silent filter),
  writes (pre-check + auto-stamp of `$user` fields + post-write verify →
  `Forbidden`/403, atomic — on all five write paths for `ownerField` parity),
  and subscription re-runs; the `FilterExpr` `Not`/`Contains`/`Exists` variants
  it adds are also available in client `.filter()`, while principal markers
  are valid only in `authorize`.

Machine tokens and scheduled jobs (no interactive principal) bypass per-row
rules; the db-level allowlist/token/session gate still runs first.

### OAuth login flow

The OAuth login popup opens with `noopener,noreferrer` (reverse-tabnabbing
defense, SEC-012); completion is relayed by the parent polling
`GET /auth/state?state=` keyed on the single-use state token minted by
`GET /auth/{provider}/begin` (not `window.opener.postMessage`, which
`noopener` severs) — the callback sets the HttpOnly session cookie and closes
the popup without interpolating the parent origin. The poll is keyed on the
state token plus the `SameSite=None` `rtdb-oauth-state` cookie set at `/begin`
(SEC-121 — constant-time-compared to the `state` param, so a leaked state URL
alone cannot poll), which is what lets the flow work cross-origin where the
`SameSite=Lax` session cookie would not be sent. Login-CSRF is defended by a
double-submit nonce cookie (`rtdb-oauth-csrf`, value = the `state` token;
`SameSite=None;HttpOnly`, 10-min) set at `/begin` and constant-time-verified at
`/callback` (disable via `RTDB_OAUTH_LOGIN_CSRF=false`); the CORS layer sets
`Access-Control-Allow-Credentials` so cross-origin SDK consumers can store it,
and the ts-client sends `credentials:"include"` on `/begin`.

### Anonymous auth and the anon→real merge

**Anonymous auth** (`POST /auth/anonymous`, gated
`RTDB_AUTH_ANONYMOUS_ENABLED` default off, plus a per-db toggle —
`GET|PATCH /admin/db/{db}/anonymous-access`, SEC-103 — that must also allow
it): mints an ephemeral `rtdb_auth.users` row (`anonymous = TRUE`, no email) +
session, returning the session token in the body and setting the same HttpOnly
cookie as OAuth. An anonymous user is a `Principal::User` with
`anonymous = true` and `email = None` — `authorize` bypasses the per-db
allowlist for it (the boot gate is its authorization), and per-row
`ownerField` stamps its `user_id` so it owns its own drafts/cursors.

The anon→real merge on a later OAuth sign-in is shipped (FM-27): `/begin`
resolves the caller's anon session server-side (never caller-supplied) into
`oauth_states.anon_user_id`, and the callback merges synchronously via
`merge::merge_users` — per-database principal-bearing doc restamps each inside
that db's committer turn (`CommitterRequest::RunMergeUsers`, op-feed/audit/
webhook taps with `source = "merge"`), storage blob owner swap, session
re-point (an open WS or stored SDK token promotes to the real principal on its
next op), then a guarded anon-row delete; every step is idempotent, and any
interruption is recovered by signing in again. `POST /admin/merge-users` (typed
confirm, 404 on a missing anon row) runs the merge synchronously as the
operator escape hatch; `rtdb_merge_docs_total` counts restamped docs. The
per-db iteration skips registry rows whose backing schema is gone (one bulk
`information_schema.schemata` probe) — leaked rows from aborted runs would
otherwise cost a committer spawn each and surface their missing `meta`
relation as an INTERNAL merge failure. The same torn shape is answered
`NOT_FOUND` everywhere else too: `db::load_schema` maps a missing backing
schema (3F000/42P01 on `meta`) to "no schema", so every schema-loading arm
reports the db as absent rather than a generic 500. Spec:
[`superpowers/specs/2026-08-14-anon-merge-design.md`](superpowers/specs/2026-08-14-anon-merge-design.md).
ts-client exposes it as `useRtDbAuth().signInAnonymous()`; rust/python clients
are machine-side and out of scope. See FEATURE_MATRIX #35.

### Active session management

`admin/sessions.rs`, part of the FEATURE_MATRIX Admin control plane row:
`GET /admin/sessions` lists every live interactive session
(OAuth/anonymous/admin-key); `DELETE /admin/sessions` (by `user`, or
`?expired=true` to sweep every expired row from both the `sessions` and
`admin_sessions` tables — untouched expired rows are never reaped otherwise,
since lazy deletion fires only when that session is used again) and
`DELETE /admin/sessions/{token_hash}` (by single session) revoke — and because
`authorize` re-runs on every Subscribe/Mutate, revocation takes effect on the
**next op** over an already-open connection (no force-disconnect needed, no
stale-auth window). The admin dashboard mirrors this at `/sessions`;
`list_sessions`/`revoke_session`/`revoke_user_sessions`/`revoke_expired_sessions`
mirror into all client SDKs and the `rtdb sessions list|revoke` CLI.

## File storage

HTTP-only and bypasses the committer (`storage.rs`, `http_api.rs`): per-db
`bytea` blobs in a `storage` side table — streamed uploads (ENH-021) land as
1 MiB chunks in a companion `storage_chunks` table with the `storage` row's
`bytes` left NULL; legacy blobs keep inline bytes, and reads probe chunks
first — plus a global `rtdb.storage_index(id →
db)` for opaque public-serve resolution. Upload (`POST /api/storage/{db}`) and
the authed routes carry `{db}` in the path (raw bodies can't carry it; session
principals aren't db-scoped) and reuse the `bearer → resolve_bearer →
authorize` triple. **`GET /storage/{id}` is the one unauthenticated route**
(public bearer URL, Convex parity). Storage writes via `storage::put`
directly — never the committer — because blobs don't touch document tables or
subscriptions.

- **On-the-fly image transforms** (ENH-014, `image_transform.rs`) are a
  READ-TIME capability on both serve routes
  (`?w=&h=&fit=cover|contain|scale-down&q=&format=jpeg|png|auto`), gated by
  `RTDB_IMAGE_*` boot config (`RTDB_IMAGE_TRANSFORMS_ENABLED` kill switch,
  `RTDB_IMAGE_MAX_DIM`/`RTDB_IMAGE_MAX_PIXELS` decode caps,
  `RTDB_IMAGE_CACHE_BYTES`, `RTDB_IMAGE_CONCURRENCY`,
  `RTDB_IMAGE_DEFAULT_QUALITY`); a request with no transform params is a
  zero-overhead passthrough, a transformed response carries
  `Cache-Control: public, max-age=31536000, immutable`.
- **Signed, time-limited URLs** (ENH-017) are an additive capability on that
  public route: when `?exp=&sig=` are present the route HMAC-verifies (key
  derived from `admin_key`) and enforces expiry (403 on failure); absent,
  behavior is unchanged. A bearer-authorized
  `GET /api/storage/{db}/{id}/signed-url` mints them.
- **HTTP `Range` requests** (ENH-016, `serve_bytes` in `http_api.rs`):
  `Range: bytes=...` → `206 Partial Content` with `Content-Range`/
  `Content-Length` + `Accept-Ranges: bytes`; out-of-bounds → `416 Range Not
  Satisfiable`; no/ignored range → `200` full body. Single-range only
  (multipart/non-`bytes`/malformed ranges are ignored per RFC 7233); Range is
  skipped for on-the-fly image transforms (cache-keyed whole renders).

## Per-database resource quotas

ENH-011, `quota.rs`: three optional **global** caps on `HotConfig` —
`maxTablesPerDb`, `maxStorageBytesPerDb`, `maxSubsPerDb` (all `0` =
unlimited, default off) — enforced **hard** so one db cannot crowd the rest on
a shared instance.

- **Tables**: `SchemaDef::check_table_quota` at the admin push-schema handler
  + `handle_migrate` (**not** in `ddl::push_schema`, which the test harness's
  `fresh_db` calls regardless of caps).
- **Subs**: `SubscriptionManager::count_for_db` checked in
  `handle_subscribe` before `register` (`SubscribeErr`, connection stays
  open).
- **Storage**: a per-db usage cache (`quota::UsageCache`,
  `Arc<RwLock<HashMap>>` on `AppState` + cloned into `CommitterCtx`) measured
  by a `SUM(pg_total_relation_size)` query (heap + indexes + TOAST + the
  `storage` blob table; framework side-tables `meta`/`mutations`/
  `scheduled_txns`/`schema_history` excluded; `RTDB_QUOTA_CACHE_TTL_SECS`
  default 60 doubles as the warmer interval). Enforced at
  `handle_mutate`/`handle_scheduled`/`handle_migrate` entry before
  `execute_txn` (so an over-cap write commits nothing partial) as a **cheap
  stale-read** — `enforce` uses any cached reading (fresh *or* stale) and runs
  **no** `pg_total_relation_size` scan on the serialized committer turn; the
  only inline measure is a one-time cold start before the cache is seeded
  (ARC-004). A per-db background warmer task (`run_quota_warmer`, spawned
  alongside the committer/scheduler/reaper/cleanup, self-terminating on db
  removal) plus a best-effort `tokio::spawn` post-commit/post-put refresh
  keep the reading current — both are fire-and-forget, never block the client
  or the committer turn, and only re-measure + write the cache (never a
  document write); the warmer skips its tick entirely when no storage cap is
  configured (default off). The `upload_handler` path uses `current_usage`
  (TTL-bounded, measure-on-miss) for the exact `used + blob > cap` figure
  since it is per-HTTP-request, not the serialized committer. `delete-db`
  evicts the entry.

**No admin bypass** — `PrincipalCtx` cannot distinguish admin from a machine
token at the committer (both arrive as `PrincipalCtx::bypass()`,
`user_id == None`), so quotas apply uniformly to every principal; raise the
cap via `PATCH /admin/config` (instant, no restart) for a large op. A
scheduled job over the storage cap is `mark_error`'d (terminal — a config
constraint won't clear on retry). Over-cap is `QUOTA_EXCEEDED` (HTTP 507);
metric `rtdb_quota_rejections_total{kind=tables|storage|subs}` with a per-db
breakdown on the `/admin/metrics` JSON only (never the Prometheus scrape —
per-db labels would blow up cardinality). `db_stats` exposes quota+usage;
mirrored across all four clients (`HotConfig`/`HotConfigPatch` +
`QUOTA_EXCEEDED`).

## Realtime presence

`presence.rs`. Presence is deliberately the one live subsystem that does **not**
go through the committer: it is transient, in-memory, and connection-bound.
Nothing is persisted, so nothing needs serializing against document writes.

A room is a `(db, room)` pair holding one entry per `/sync` connection. State
arrives on the `presenceState` frame, is broadcast over each connection's
existing `out_tx`, and disappears when the connection closes — the WS lifecycle
is the only membership authority. Entries may also carry a `ttlMs` so a
connection that stops refreshing ages out without disconnecting.

Every dimension is bounded by boot-only `PresenceConfig` fields
(`RTDB_PRESENCE_*`): state bytes per member, members per room, rooms per
connection, bytes per room, broadcast interval, updates per second, and maximum
TTL. Presence is off by default; when disabled the frames answer
`PRESENCE_DISABLED`.

Under `RTDB_MULTI_INSTANCE` each replica republishes its **full** local
membership for a room on `rtdb_presence` at a configured cadence. A full
snapshot rather than a delta, because a snapshot is idempotent and needs no
reconciliation, and rooms are capped small enough for it to stay cheap.

## Wire contract and clients

`server/src/protocol.rs`, `ts-client/src/protocol.ts`,
`rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, and
`swift-client/Sources/ParRtDbClient/Wire.swift` (query/txn wire structs in
`Query.swift`/`Mutation.swift` alongside it) are five
implementations of the same protocol and must stay byte-identical (serde tags
and field names). The casing is deliberately non-uniform and load-bearing —
match the protocol files exactly (see the spec). The SDKs are no-codegen: a
schema object is both pushed to the server and the source of inferred types.
The Rust client ports the TS SDK (design at
[`superpowers/specs/2026-07-22-rust-client-design.md`](superpowers/specs/2026-07-22-rust-client-design.md));
its `http`, reactive `ws`, and `admin` features all ship, plus
index/`mutate_with_retry` helpers and `.filter()`/`.search()`/`.fields()`
builders. The
Python client ships the wire contract and schema/mutation/query DSL today
(design at
[`superpowers/specs/2026-07-25-python-client-design.md`](superpowers/specs/2026-07-25-python-client-design.md));
its HTTP/admin/storage surfaces ship (a sync `httpx` client —
`pip install par-rt-db[http]`), as does the reactive `ws` surface
(`RtDbClient` async over `/sync` — `pip install par-rt-db[ws]`; live
`subscribe` + at-most-once `mutate` + schedule ops), optimistic updates, and
an in-memory test harness — the four clients are now at feature parity.
`FEATURE_MATRIX.md` tracks parity vs. Convex.

### The semantics corpus

"Byte-identical" is a claim, and `wire-corpus/` is what tests it. Each case in
`wire-corpus/semantics/` is a JSON description of a schema, a sequence of
operations, and the expected outcome, and **five runners execute every case**:
the real server against Postgres, and the four clients' in-memory engines
(TypeScript, Rust, Python, Swift). A behavior that differs between any two of
them fails the corpus rather than surfacing later as a client bug.

The corpus is what makes the "mirror every server change in all four clients"
rule enforceable instead of aspirational, so every behavior-changing change
ships with a case. `wire-corpus/README.md` states that authoring rule and the
case format. `wire-corpus/golden-vector.json` separately pins the exact wire
encoding of representative frames, catching a serde-tag or casing drift that a
semantics case would not.

## Admin surface and the op-feed tap

### The publish_taps enforcement point

Durable document mutations publish through the committer's single enforcement
point — the `publish_taps` helper (`committer/taps.rs`) — called from
**eight** `handle_*` arms:

| Arm | `source` | Emits DocOps | Notes |
| --- | --- | --- | --- |
| `handle_mutate` | per-request | yes | The ordinary write path, WS and HTTP alike |
| `handle_scheduled` | `"scheduled"` | yes | Scheduler-claimed jobs |
| `handle_workflow_advance` | `"workflow"` | yes | Durable workflow step commits |
| `handle_migrate` | `"migrate"` | yes | Schema migrate DDL + DML |
| `handle_reaper` | `"ttl"` | yes | TTL deletes are durable writes, `owner = None` |
| `handle_merge_users` | `"merge"` | yes | The anon→real merge's committed doc restamps |
| `handle_push_schema` | `"push"` | no | Schema push runs through the committer; it publishes for audit/webhooks but emits no DocOps |
| `handle_restore_schema` | `"restore"` | no | Same shape as push, for schema-history restore |

Any future code path that commits a document txn must call `publish_taps` too,
or the op-feed (and `/admin/stream`) will silently miss those writes. Add new
tap sites to this table.

Since ARC-005 the committer is a module rather than one file, and the rule is
enforced structurally: each arm lives in `committer/arms/<name>.rs`,
`publish_taps` is `pub(in crate::committer)` in `committer/taps.rs`, and `execute_txn` is
called only from inside `arms/`. The rest of the module is `mod.rs` (the
`Committers` handle, `CommitterRequest`, `CommitterCtx`, `run_committer`),
`lease.rs` (the ownership lease), `forwarding.rs` (Stage 4c origin-side glue),
and `supervisor.rs` (idle reclamation + quota warmer). The split was a pure
move — no behavior changed.

- **`/admin/stream` auth**: the WS upgrade gate accepts the admin bearer from
  the `Authorization` header (CLI/automation) **or** the
  `Sec-WebSocket-Protocol: rtdb-admin.<token>` subprotocol (the browser
  dashboard — browsers can't set headers on a WS handshake); both funnel
  through `authenticate_admin` (`admin/mod.rs`), preserving the constant-time
  admin-key compare and the OAuth-admin-allowlist check.
- **Durable audit log**: when `RTDB_AUDIT_LOG_ENABLED` (boot, default off) is
  set, `publish_taps` best-effort writes one `rtdb.audit_log` row per `DocOp`
  (`ts_ms, db, table, op, doc_id, principal=owner, source`) — so the audit
  trail inherits the op-feed's "every durable write publishes here" guarantee;
  the table is ensured at boot only when enabled, and
  `GET /admin/audit?db=&limit=&offset=` reads it (empty when disabled).
- **Webhook/event delivery**: when `RTDB_WEBHOOKS_ENABLED` (boot, default off)
  is set, `publish_taps` enqueues one `rtdb.webhook_deliveries` (outbox) row
  per matching `DocOp` (per `rtdb.webhooks` row filtered by db/table/events),
  and a boot worker drains the outbox via reqwest POSTs with exponential
  backoff (at-least-once); admin CRUD at `/admin/db/{db}/webhooks`.

### Admin CSRF

Dashboard admin requests authenticate with an HttpOnly session cookie, so
mutating admin routes need a CSRF defense: a readable `rtdb-admin-csrf` cookie
whose value the dashboard echoes back in the `X-Rtdb-Csrf` header
(double-submit). `admin_csrf_guard` requires the match on mutating methods.
Bearer-token callers (CLI, automation, machine clients) send no cookie at all,
so the defense does not apply to them and they are not asked for a nonce.

`require_admin_mw` also **self-heals** a session that predates the CSRF
deployment: a browser holding a valid session cookie but no `rtdb-admin-csrf`
cookie is issued one on its next request rather than being logged out.
`/admin/login`, `/admin/logout`, and `/admin/stream` are exempt — the first two
mint or clear credentials, and the stream's WS upgrade authenticates inline.

### Admin document access

`admin/docs.rs`, `ws.rs`: a dashboard admin reads/writes documents across
every database via `POST /admin/db/{db}/query|mutate` (`owner=None`,
bypassing per-row `ownerField`) and over `/sync`, where an admin (`is_admin`
at the handshake) bypasses the per-db `authorize` and subscribes/mutates with
`owner=None`. Machine tokens and non-admins are unchanged;
`auth::authorize`/`owner_of` are untouched.

The admin-only `RTDB_MAX_AFFECTED_DOCS` (boot `Config`, default 100)
step-count cap rejects an over-cap mutation before the committer — an
over-cap write never becomes durable. The step-count cap bounds the per-id
steps (`Insert`/`Patch`/`Replace`/`Delete`/`Undelete`/`Upsert`, each ≤1 doc,
so `steps.len() ≤ cap` ⟹ affected ≤ cap); the predicate-driven
`PatchByQuery`/`DeleteByQuery` steps are ONE step each but touch many rows, so
they bypass the step-count cap and are bounded instead by a per-step row cap
(`MAX_BY_QUERY_ROWS`, const default 1000) enforced inside `execute_txn` — a
step whose match set exceeds its `limit` touches exactly `limit` and reports
`truncated: true`. The admin query route also accepts `includeDeleted: true`
(an internal `execute_query` param, NOT a wire `Query` field) so an operator
can see soft-deleted rows.

## Backups and restore

`backup.rs`, `admin/backups.rs`. The manual `POST /admin/backup` trigger
spawns one `pg_dump` **outside the committer** (a read — same as the cron
task) and is gated by an `AppState` `backup_running` flag (a second call while
running → 409). `POST /admin/restore` restores a dump into a **fresh
`rtdb_restored_<stamp>` Postgres DB** via `createdb` + `pg_restore
--no-owner --no-privileges` — the live `rtdb` DB is never touched, so the
single-writer invariant is preserved and there are no locks/races. An existing
target of the same name is pre-dropped (the name is `rtdb_restored_`-scoped,
never the live DB), so retrying the same dump is an idempotent
drop-and-recreate; when the drop cannot run (a connection still open on the
target), the route 409s naming the manual recovery step.
`GET|DELETE /admin/backups/{name}` download/delete a dump; all name-bearing
routes pass `backup::validate_dump_name` (path-traversal guard). Restore
requires a typed `confirm == name`. **`CREATEDB` privilege** is required on
the DB role (new — the server previously only ran `CREATE SCHEMA`/
`CREATE EXTENSION`). Credentials travel via `PG*` env, never argv.

## Hot config and dynamic CORS

`config.rs`, `lib.rs`. Seven settings — `allowed_origins`, `session_ttl_days`,
`max_file_size`, `idempotency_ttl_ms`, plus the three ENH-011 quota caps
(`maxTablesPerDb`/`maxStorageBytesPerDb`/`maxSubsPerDb`) — are
runtime-mutable, held on `AppState` as `Arc<ArcSwap<HotConfig>>` and
persisted in a single-row
`rtdb_config` table (boot seeds from env when no row exists; a malformed row
warns and falls back to env rather than blocking startup). Every consumer
reads `state.hot.load()` (the committer reads `idempotency_ttl_ms` live at
mutate-dedup time via the same shared `Arc`); `PATCH /admin/config` validates
+ persists + swaps live (no restart). The `CorsLayer` is built once but its
origin check is an `AllowOrigin::predicate` that re-reads live
`allowed_origins` per request. `GET /admin/config` is structurally redacted —
`admin_key`, OAuth secrets, and `database_url` are configured-bools, never
values.

## Static SPA hosting

`lib.rs`. When `RTDB_STATIC_DIR` (boot `Config`, `Option<String>`) is set and
the dir exists, a `tower-http` `ServeDir` (with a `ServeFile` `index.html`
SPA fallback) is mounted as the router's **last** `fallback_service` — it only
handles paths no registered route matches, so it can never shadow `/healthz`,
`/privacy`, `/metrics`, `/api/*`, `/admin/*`, `/sync`, or `/auth/*`
(`/metrics` is content-negotiated: a browser `Accept: text/html` is served the
SPA `index.html`, any other client gets Prometheus text exposition — see
`prometheus_metrics_handler`). Unset/empty/non-existent dir ⇒ API-only.

A `from_fn` middleware sets `Cache-Control` from the response `Content-Type`
(text/html → no-cache so a new deploy's index.html is always fetched;
everything else → immutable) and wraps only the `ServeDir`, never the API
routes. Same-origin ⇒ the dashboard needs no `allowed_origins` entry. In the
docker deploy the SPA is **baked into the image** (the `dashboard` build stage
in `Dockerfile` runs the bun/vite build and copies `dist/` to
`/app/dashboard-dist`, which `RTDB_STATIC_DIR` points at) — so a frontend
change ships via the standard `docker compose up -d --build` (image rebuild +
server container recreate); it is not a live-mounted volume.

## Performance characteristics

ENH-033 added a two-layer benchmark harness: criterion micro-benchmarks over
the pure hot paths (`server/benches/`, `rust-client/benches/`, run via
`make bench-micro`) and a black-box load script (`scripts/bench/load.ts`, run
via `make bench`) that drives real running servers over HTTP + WS. See
[CONTRIBUTING.md's Benchmarks section](../CONTRIBUTING.md#benchmarks) for how
to run either.

The numbers below are quoted from `bench/baseline.json`, the median of 3
real runs captured 2026-08-25 against the live deployed instance
(`RTDB_BUILD_COMMIT=3ab8b13`), run directly on `lenny2` against
`127.0.0.1:8300` (no Cloudflare tunnel) with the `bench` database reset to
empty before the series. Scenario (c) is not represented — the production
deploy is a single instance, so multi-instance forward/takeover has no real
number here; a two-instance `make bench-baseline` run on a dev machine would
be needed to capture it. The three runs were not independent trials: the
`bench` database was reused and grew across them (writes from run 1 were
still present for run 2, and so on), so scenario (d)'s turn-hold-time and the
subscription rerun counts trend upward run over run — the median partly
absorbs that, but a future baseline recapture should reset the database
before each run it wants to be comparable, not just once before the series:

| Scenario | Metric | Baseline value (median of 3) |
| --- | --- | --- |
| (a) sustained writes, 8 writers / 30s | commits/s | 149.1 |
| (a) sustained writes | commit latency p50 / p99 | 42.9 ms / 169.1 ms |
| (b) subscription fan-out, 8 subscribers | write-to-update latency p50 / p99 | 14.1 ms / 84.2 ms |
| (d) bulk `deleteByQuery`, 5k rows, 100 subscribers | turn hold time | 3418.6 ms |
| (d) subscription rerun ratio | reruns / (reruns + skips) | 0.84 |

The scenario (d) numbers are the most actionable finding so far: a 3.4s
committer-turn hold on one bulk delete blocks every other write to that
database for the same window (single-writer committer, see above), and an
0.84 rerun ratio means most of the 100 subscriptions paid the full rerun
path rather than an indexed/ordered skip — consistent with the audit's
ARC-006 (`pg_notify`-per-op inside the committer turn) hotspot. Not yet
investigated further; a follow-up optimization pass should start here.

A regression beyond 15% on any metric (latency up, throughput down) is
checked manually with `scripts/bench/compare.ts` — no CI job runs benchmarks
(GitHub-hosted runners are noisy shared hardware, not comparable run to run).
Runs happen only from the operator's dev machine or `lenny2` (the deploy
host, run there against `127.0.0.1:8300` to skip the tunnel) — see
[CONTRIBUTING.md's Benchmarks section](../CONTRIBUTING.md#benchmarks).
