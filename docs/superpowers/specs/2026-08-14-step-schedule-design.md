# Step::Schedule / Step::CancelSchedule — atomic scheduling from inside a transaction (FM-28)

Date: 2026-08-14
Status: approved design (user-approved 2026-08-14)
Board card: FM-28 (`[FM-28] Step::Schedule — schedule a txn from inside a txn`)

## Problem

Scheduling is today a standalone op (WS `Schedule` frame, `POST /api/schedule`,
admin create): the client sends a transaction to run later. A transaction
cannot atomically "write doc + enqueue follow-up job" — the enqueue and the
write are two round trips with no atomicity: a crash between them loses the
job; a retry duplicates it. Convex parity is `ctx.scheduler.runAfter`/`runAt`
callable from any mutation.

## Goals

1. A `Transaction` can carry a `Schedule` step; the `scheduled_txns` row is
   inserted **in the same Postgres transaction** as the txn's document writes —
   a crash between write and enqueue is impossible.
2. The step result returns the scheduled job id; the job is cancellable via the
   existing cancel surface (and by a new in-txn `CancelSchedule` step).
3. Mirrored in all four clients (wire + builder + in-memory harness).
4. The enqueue-time table allowlist gap on the existing Schedule op is closed
   in the same change (user-approved tightening; auth-touching, flagged for
   manual review).

## Non-goals

- Durable workflows (FM-29, separate card, depends on this one).
- Per-row authorization pre-checks on the nested txn — unsound (rows change
  between enqueue and fire). Scheduled jobs continue to fire as the system
  (bypass) principal; that stays documented behavior.
- Changes to the scheduler loop, `RunScheduled` committer arm, pause/resume,
  or the `scheduled_txns` schema — all reused unchanged.

## Wire protocol

Two new variants of the existing `Step` enum (`#[serde(tag = "op",
rename_all = "camelCase", deny_unknown_fields)]`, `server/src/txn.rs`),
mirrored byte-identically in `ts-client/src/protocol.ts`,
`rust-client/src/wire.rs`, and `python-client/src/par_rt_db/wire.py`:

```json
{"op": "schedule", "when": {"type": "afterMs", "ms": 60000}, "txn": {"steps": [...]}}
{"op": "cancelSchedule", "id": "<job id>"}
```

- `when` reuses the existing `ScheduleWhen` union
  (`{"type":"afterMs","ms":i64}` | `{"type":"runAt","ms":i64}` |
  `{"type":"cron","expr":"5-field cron"}`) — cron-in-a-txn comes free via the
  shared `scheduler::resolve_when`.
- `txn` is a full `Transaction` (`{"steps":[...]}`), the same type the
  standalone Schedule op accepts.
- `id` is a job id previously returned by a schedule step/op result.

## Server execution

### Step::Schedule (`txn.rs` step loop)

Runs inside `execute_txn` on the open sqlx transaction:

1. `scheduler::resolve_when(when, now_ms())` — validates timing (negative
   `afterMs` / invalid or never-firing cron ⇒ `BadRequest`), which fails the
   whole txn before commit (document writes roll back too).
2. Enqueue-time table authorization (see below) over the nested txn's steps.
3. Generate the job id (`db::new_id()`), INSERT into `<schema>.scheduled_txns`
   via a new `scheduler::insert_on(&mut PgConnection, …)` — a connection-bound
   variant of the existing `scheduler::insert` (identical SQL; `insert`
   refactors to call it). Status `pending`, `created_at = now`.
4. Push step result `{"scheduleId": "<id>"}`.

The row becomes visible at `tx.commit()`; the scheduler's existing ≤2s poll
picks it up and fires it through the unchanged `RunScheduled` committer arm.
A chained schedule (a scheduled txn that itself carries a Schedule step) works
— the fire executes `execute_txn`, whose Schedule step inserts the next row.

### Step::CancelSchedule

`DELETE FROM "<schema>".scheduled_txns WHERE id = $1` on the open sqlx
transaction. Pushes `{"cancelled": <bool>}` (`true` iff a row was deleted).
A missing / already-fired / already-cancelled id yields `false`, not an error —
matching the standalone cancel op. A job whose fire is currently in flight
completes that fire; the delete removes the row so the cron finalize update
touches 0 rows and the job never fires again.

### Step plumbing

- `Step::table()` returns `Option<&str>`: every document step returns
  `Some(table)`; `Schedule` and `CancelSchedule` return `None`, and the
  per-step `authorize_table` gate in `execute_txn` skips `None`.
- `worst_case_affected`: both new variants count **0** (they touch no
  documents).
- `MAX_STEPS` counts steps **recursively**: a `Schedule` step contributes
  `1 + count(nested steps recursively)`. The total tree must be ≤ 1024. This
  bounds one request body's serialized size and blocks the
  1024-steps-scheduling-1024-steps nesting bomb. By-query caps
  (`MAX_BY_QUERY_STEPS_PER_TXN`, `MAX_AFFECTED_ROWS_PER_TXN`) are NOT applied
  to nested txns at enqueue time — the nested txn executes in a future
  committer turn and `execute_txn` re-validates it fully at fire time.

### Enqueue-time table authorization (tighten both surfaces)

`authorize_table(ctx, table)` (`auth/mod.rs`, a pure in-memory check of the
machine-token table allowlist) runs over **every step of the nested txn,
recursively** (steps inside nested Schedule payloads included), at enqueue
time, in all four enqueue paths:

1. `Step::Schedule` inside `execute_txn` (against the current caller's
   `PrincipalCtx`).
2. WS `Schedule` frame handler (`ws.rs handle_schedule`).
3. HTTP `POST /api/schedule` (`http_api.rs`).
4. Admin create-schedule (`admin/schedules.rs`) — a no-op for bypass/admin
   principals but applied uniformly so the four paths cannot drift.

Effect: a scoped machine token can no longer smuggle a future write into a
table outside its allowlist via a scheduled job (previously possible on the
standalone op — this closes that gap). Interactive users and full-access
principals are unaffected (`tables = None` bypasses). Per-row rules are not
pre-checked (non-goal); the firing job remains a system principal.

## Fire-time semantics (unchanged, restated)

The job fires via `RunScheduled` → `execute_txn` as a bypass principal;
one-shot catch-up fires immediately if past due; delivery is at-least-once
across crashes (`reset_running`); a fire-time validation failure (e.g. a
table dropped between enqueue and fire) follows existing error semantics
(`mark_error` for one-shot, `reschedule_cron_error` for cron). All observable
through the existing list/pause/resume/cancel surfaces and `ScheduleInfo`.

## Client mirrors

- **ts-client**: `Step` union + `TxnBuilder.schedule(when, txn)` and
  `.cancelSchedule(id)`; step-result type extended with `scheduleId` /
  `cancelled`. Verified with `bun run typecheck` + vitest (green vitest is not
  typecheck).
- **rust-client**: `wire.rs` step enum + `Transaction` builder methods.
- **python-client**: `wire.py` step union + `Mutation` DSL methods; the
  in-memory test harness honors Schedule steps in `tick()` (enqueued jobs due
  at/ before tick time execute against the harness state; CancelSchedule
  removes them).
- **cli**: no new command needed (jobs are visible/cancellable via existing
  `rtdb schedules` surfaces); no change.
- Wire-corpus tests: entries for both new variants in all four client protocol
  tests (server fixture ↔ client round-trip).

## Testing

Server integration tests (new `server/tests/schedule_step_test.rs`, dev
Postgres via the standard harness):

1. **Atomicity**: txn `Insert(doc) + Schedule(afterMs 0)` commits — the doc is
   readable AND the job exists in `scheduler::list` before it fires; after the
   fire (poll `db_stats`/list or await the fired side effect) the nested txn's
   writes are durable. A txn whose later step fails (e.g. `ExpectVersion`
   mismatch) rolls back the schedule row too — no orphan job.
2. **Bad `when`**: negative `afterMs` / garbage cron ⇒ `BadRequest`, doc writes
   in the same txn rolled back.
3. **Step results**: `scheduleId` present and job cancellable via the existing
   `CancelSchedule` op; `{"cancelled":true}` then `{"cancelled":false}` on
   repeat.
4. **Cancel atomicity**: txn `CancelSchedule(id) + Insert` — both commit or
   both roll back.
5. **Scoped token**: a machine token scoped to table A cannot enqueue a
   nested txn writing table B — via the step AND via the standalone Schedule
   op (both surfaces tightened).
6. **Recursive budget**: a txn whose recursive step count exceeds 1024 is
   rejected before any step executes.
7. **Chained scheduling**: a scheduled job whose txn carries a Schedule step
   enqueues the follow-up when it fires.
8. **Cron-in-a-txn**: a cron Schedule step creates a repeating job; cleanup
   cancels it.

Client tests per mirror (ts vitest+typecheck, rust cargo test, python pytest
incl. harness `tick()`).

## Documentation

- `FEATURE_MATRIX.md` #28 → ✅ with client-mirror status.
- `CLAUDE.md` data-pipeline step list gains `Schedule`/`CancelSchedule`.
- Server + client READMEs (DSL section) and the `rtdb` CLI docs if they list
  step kinds.

## Invariants preserved

- Single-writer: no new writer; the insert rides `execute_txn`'s existing
  serialized turn (and is covered by its `SET LOCAL statement_timeout`).
- SQL construction: the INSERT/DELETE reuse `scheduler`'s validated-identifier
  helpers, values bound via `$n`.
- Errors: `RtDbError` envelope; timing validation is `BadRequest`, allowlist
  violation is `Forbidden` (403).
- Clients mirror the core: any server wire change lands in all three clients +
  harness in the same change.
