# Scheduled & cron transactions — design

## Problem

par-rt-db has no way to defer or repeat a mutation: a txn either runs now or not
at all. FEATURE_MATRIX #9 (scheduled transactions, the `runAfter`/`runAt` analog)
and #10 (cron jobs, recurring txns) are the two remaining gaps that "deliver most
of what apps use Convex's scheduler + actions for, without compromising the
no-server-code architecture" (FEATURE_MATRIX §5). The use cases are concrete for
this instance's apps: deferred writes, TTL/expiry cleanup, periodic heartbeat or
roll-up writes, and "fire this update N seconds from now."

Because par-rt-db transactions are *data*, not code, scheduling needs no embedded
JS runtime: a job is a `(due_at, txn)` row drained through the existing
single-writer committer by a timer task. Convex needs a scheduler + actions
service for this; par-rt-db needs one table and one task.

## Scope

This design covers **#9 and #10 together** — one-shot scheduled transactions and
recurring cron jobs — because they share one per-db table and one timer task.
Splitting them would mean a second table and a second spec for the cron half,
which the matrix rates S-effort once #9 exists.

Server: `db.rs` (new `scheduled_txns` table at `create_database` time), a new
`server/src/scheduler.rs` (table access + the timer task), `committer.rs` (a new
`CommitterRequest::RunScheduled` arm that executes due jobs and finalizes their
rows), `protocol.rs` (new WS message variants), `ws.rs` and `http_api.rs` (the
new surfaces on both transports), and `Cargo.toml` (`chrono` + `cron`).

Clients: the TS SDK (`protocol.ts`, `client.ts`, `http.ts`, the in-memory test
harness) and the Rust client (`wire.rs`, `http`, `ws`) gain a `schedule()` API
plus `cancelSchedule` / `pauseSchedule` / `resumeSchedule` / `listSchedules`, all
mirroring the server wire contract.

Out of scope: per-row authorization of which principals may schedule, automatic
client-side retry of a scheduled job whose execution failed, time-zone support
beyond UTC, and a React `useSchedules()` hook (the client API ships first; the
hook is a follow-up if wanted).

## Design

### Architectural fork: where the timer lives

Three placements were considered.

**A. Dedicated per-db scheduler task that enqueues into the committer
(recommended, adopted).** A tokio task per database, spawned in
`Committers::channel_for` alongside the committer with a clone of its
`mpsc::Sender`. It sleeps until the next `due_at` (capped wake interval), claims
due rows, and for each sends a `CommitterRequest::RunScheduled { id }` into the
committer channel. The committer executes the txn via `execute_txn` + fan-out and
finalizes the row. `execute_txn` stays strictly inside the committer, so the
load-bearing single-writer invariant is untouched — the scheduler only ever
writes the `scheduled_txns` side table, never app data. Crash recovery is
trivial (see below).

**B. Postgres `pg_cron` / `LISTEN/NOTIFY`.** Rejected. `pg_cron` runs SQL, not
our txn DSL, so it cannot drive `execute_txn`; `LISTEN/NOTIFY` only signals "a
job was just inserted," not "its due time arrived," so it cannot replace the
timer. The design spec also records that par-rt-db is deliberately Postgres's
only client and avoids `LISTEN/NOTIFY`. Neither removes the need for an
app-level timer; both add moving parts.

**C. Scheduler folded into the committer loop.** Rejected. The committer loop
processes exactly one request to completion before pulling the next, and its
correctness argument depends on that simplicity. Mixing timer-driven work in via
`tokio::select!` couples two concerns and lets a long-running txn delay
scheduled fires, muddying the ordering guarantee.

### Data model — per-db `scheduled_txns` table

A sibling of the existing `mutations` dedup table, created in the same
transaction as `meta` and `mutations` inside `db::create_database` (new dbs), and
via `CREATE TABLE IF NOT EXISTS` at committer startup for existing dbs — exactly
the `mutation_log::ensure_table` lifecycle.

```sql
CREATE TABLE "<schema>".scheduled_txns (
    id          text PRIMARY KEY,          -- uuid v7 (db::new_id), server-generated, returned to client
    kind        text NOT NULL,             -- 'oneshot' | 'cron'
    due_at      bigint NOT NULL,           -- epoch ms UTC; next fire time
    txn         jsonb NOT NULL,            -- the declarative Transaction (data, not code)
    cron        text,                      -- 5-field cron expr when kind='cron', else NULL
    status      text NOT NULL,             -- 'pending' | 'running' | 'paused' | 'error'
    last_error  text,                      -- error message when status='error', else NULL
    created_at  bigint NOT NULL,
    fired_count bigint NOT NULL DEFAULT 0  -- incremented each successful fire
);
CREATE INDEX ON "<schema>".scheduled_txns (status, due_at);
```

The `(status, due_at)` index backs the due-scan. `id`, `kind`, `due_at`, `txn`,
`cron`, `status`, `created_at`, and `fired_count` are returned by `listSchedules`
(more on that below). `last_error` is surfaced there too so operators can see
failed jobs.

### Claim → execute → finalize flow

The scheduler task runs this loop:

1. **Sleep** until the next due time. It queries `SELECT MIN(due_at) FROM
   scheduled_txns WHERE status = 'pending'`; if that is in the future it sleeps
   for the difference, otherwise it proceeds immediately. A capped maximum sleep
   (a few seconds) guards against a newly inserted nearer job arriving while
   asleep — the next wake re-reads `MIN(due_at)`, so the cap only bounds latency
   for a job inserted with a *sooner* `due_at` than the current sleep target, at
   the cost of an occasional early wake.
2. **Claim** due rows atomically:

   ```sql
   UPDATE "<schema>".scheduled_txns SET status = 'running'
   WHERE id IN (
       SELECT id FROM "<schema>".scheduled_txns
       WHERE status = 'pending' AND due_at <= $1
       ORDER BY due_at LIMIT $2
       FOR UPDATE SKIP LOCKED
   )
   RETURNING id, kind, txn, cron;
   ```

   `$1` is `now_ms()`, `$2` a small batch bound. `FOR UPDATE SKIP LOCKED` is safe
   here because only one scheduler task exists per db — but it future-proofs the
   claim against any second claimer and never blocks.
3. **Dispatch**: for each claimed row, send `CommitterRequest::RunScheduled { id }`
   into the committer channel. The scheduler does not execute the txn itself.

The committer gains a `RunScheduled` arm that loads the claimed row, runs
`execute_txn` + `subs.fan_out` (identical to `handle_mutate`'s write path), then
finalizes:

- **one-shot, success** → `DELETE FROM scheduled_txns WHERE id = $1`. (Fired jobs
  self-remove so the table does not accumulate history.)
- **cron, success** → compute the next fire time and
  `UPDATE … SET status='pending', due_at=$next, fired_count=fired_count+1,
  last_error=NULL WHERE id=$1`. The cron expression is re-parsed each fire and
  `next_fire = Schedule::after(now_utc).next()`.
- **failure** (the txn returned a step error / precondition failure) → one-shot:
  `UPDATE … SET status='error', last_error=$msg`; cron: log the error via
  `tracing` and still reschedule the next fire (`status='pending', due_at=$next`)
  so one bad payload cannot permanently stall a recurring job.

A `RunScheduled` whose row no longer exists (it was cancelled between claim and
execution) is a no-op.

### Crash recovery and delivery semantics

On scheduler startup, one statement resets any orphaned claims:

```sql
UPDATE "<schema>".scheduled_txns SET status = 'pending' WHERE status = 'running';
```

**Delivery is at-least-once** — the same semantics as Convex's scheduler. A
crash in the window between `execute_txn` committing and the row-finalize
(`DELETE` for one-shot, `UPDATE due_at` for cron) leaves a `running` row that
recovery resets to `pending`, so the job re-fires once. Apps should write
scheduled txns to be idempotent (a `Step::Patch` with `expectVersion`, or an
insert that tolerates already-present rows). This is stated honestly rather than
claiming exactly-once, which the no-additional-coordination design cannot
guarantee.

### Catch-up and timezone

- **One-shot past due** (the server was down at `due_at`) fires ASAP on catch-up
  — recovery resets `running`→`pending` and the due-scan's `due_at <= now`
  reclaims it.
- **Cron missed windows** are skipped entirely: on catch-up the next fire is
  `Schedule::after(now).next()`, i.e. the next scheduled time *after now*, never
  a backfill of the windows the server was down for.
- **All cron expressions are evaluated in UTC.** `due_at` is UTC epoch ms (the
  existing `now_ms()`), and next-fire is computed against a `DateTime<Utc>`. No
  timezone database is needed in the scheduler.

### Wire surface

Both transports and all three clients must stay byte-identical (serde tags and
field names, non-uniform casing matching the rest of the protocol).

**WS** — new `ClientMessage` variants:

```
Schedule          { scheduleId, when: When, txn }
CancelSchedule    { scheduleId, id }
PauseSchedule     { scheduleId, id }
ResumeSchedule    { scheduleId, id }
ListSchedules     { scheduleId }
```

where `When` is a tagged enum: `{ "type": "afterMs", "ms": <i64> }` |
`{ "type": "runAt", "ms": <i64> }` | `{ "type": "cron", "expr": "<5-field>" }`.
`scheduleId` is reply-correlation, mirroring `mutId` (client-generated, never
persisted); `id` is the server job id.

New `ServerMessage` variants:

```
ScheduleOk        { scheduleId, id }
ScheduleErr       { scheduleId, error: RtDbError }
ScheduleAck       { scheduleId, ok: bool, error: Option<RtDbError> }   // cancel/pause/resume
ListSchedulesOk   { scheduleId, schedules: Vec<ScheduleInfo> }
```

`ScheduleInfo` carries `{ id, kind, dueAt, cron, status, lastError, createdAt,
firedCount }` (`cron` and `lastError` omitted on the wire when null, like
`github_login`). `pause`/`resume` apply to cron jobs (and a not-yet-fired
one-shot); `cancel` deletes any job.

**HTTP** — parallel one-shot routes, authorized like `/api/query` and
`/api/mutate`:

- `POST /api/schedule` — body `{ when, txn }` → `{ id }`.
- `POST /api/schedule/<id>/cancel` · `/pause` · `/resume` → `{ ok }`.
- `GET /api/schedules` → `{ schedules: [...] }`.

### Authorization

`schedule` and the manage ops (cancel/pause/resume) authorize as **mutate**
(write-level); `listSchedules` authorizes as **query** (read-level). All reuse
the existing `authorize` path — no new auth mechanism. As with mutate/subscribe,
the WS handler re-runs `authorize` on each of these on an open connection, so
token revocation, allowlist changes, and session expiry take effect live.

### Failure handling

Scheduled-txn execution failure does **not** retry (covered above): a one-shot
records `status='error'` and stops; a cron logs and reschedules. Failures are
visible in `listSchedules` via `last_error`/`status`. A malformed scheduling
request — an unparseable cron expression or a negative `afterMs` — is rejected
with `BadRequest` before any row is written.

Note on `runAt` in the past: a one-shot with `runAt <= now` is accepted and fires
immediately on the next due-scan (this is the catch-up path and the natural
meaning of "run at this time"). It is not an error.

### Clients

**TS SDK.** `client.schedule(txn, when: { afterMs } | { runAt } | { cron })`
→ `Promise<{ id: string }>` on the reactive WS client; `http.schedule(...)` on
the HTTP client. Plus `cancelSchedule(id)`, `pauseSchedule(id)`,
`resumeSchedule(id)`, and `listSchedules()` on both. `protocol.ts` gains the new
wire types. The in-memory test harness (`InMemoryRtDbClient`) gets a
timer-less stub: it stores jobs and exposes them via `listSchedules`, and fires
due jobs only when `mutate`/a manual `tick()` advances a virtual clock — enough
to unit-test the client surface without a real timer.

**Rust client.** `wire.rs` gains the same wire types; the `http`, `ws`, and
`admin` feature modules gain `schedule`, `cancel_schedule`, `pause_schedule`,
`resume_schedule`, and `list_schedules`.

## Testing

- New `server/tests/scheduled_test.rs` (mirrors the module→binary convention):
  - One-shot fires after `afterMs`: insert, advance, assert the txn's write is
    visible and the row is gone.
  - `runAt` absolute (future and past) behaves as one-shot.
  - Cron fires on schedule and `fired_count` increments; `due_at` advances to the
    next expression-matched time.
  - Catch-up: insert a one-shot with `due_at` in the past, assert it fires on the
    next scan.
  - Cron skips missed windows: set `due_at` well in the past, assert exactly one
    fire (next-after-now), not a backfill.
  - Pause → no fire; resume → fires on schedule again.
  - Cancel deletes the row (one-shot pending and cron alike).
  - Failing txn (a precondition that does not hold) → one-shot `status='error'`
    with `last_error`; cron logs and reschedules.
  - Crash recovery: manually mark a row `running`, restart the scheduler
    (`ensure_table`/reset path), assert it re-fires.
  - Auth: a revoked principal cannot schedule/manage; list requires read-level.
- `ts-client` tests: `schedule.test.ts` (builder + wire shape, in-memory fire via
  `tick()`), and `client.test.ts`/`http.test.ts` passthrough coverage.
- `rust-client` tests: `wire.rs` round-trips and the `http`/`ws` builder coverage.
- Opt-in live-server E2E (`ts-client/tests/integration/schedule.test.ts`,
  `rust-client/tests/http_integration.rs` style, `#[ignore]` + `RTDB_TEST_SERVER_URL`)
  for real-timer round trips.

## Verification

`make checkall` from the repo root (fmt-check + clippy `-D warnings` + typecheck
+ tests for server, ts-client, and rust-client). Integration tests require
`make dev-db-up`. This is the definition of done.

## Out of scope

- Automatic retry of a scheduled job whose execution failed (at-least-once
  already covers the crash window; app-level idempotency is the documented
  contract).
- Timezones other than UTC.
- A React `useSchedules()` reactive hook (ship the client API first).
- Per-row authorization of scheduling (covered by the existing allowlist +
  machine/session auth, same as mutate today).
- Backfill of missed cron windows.
