# Workflow `awaitSignal` — external/human approval gates

Date: 2026-08-21
Status: approved design (user-approved 2026-08-21)
Board card: `Workflow awaitSignal step — external/human approval gates`
Depends on: FM-29 durable declarative workflows (shipped 2026-08-15,
`2026-08-15-workflows-design.md`) — the side table, scheduler claim loop,
committer advance arm, and mirror pattern are all reused.

## Problem

Every FM-29 step is automatic: a run advances through txn steps, sleeps, and
retries with no external pause point. An approval gate (human sign-off, agent
dispatch confirm, "wait for the payment webhook") must today be hand-rolled as
a side table the app polls and writes. Convex's Workflow component has no
first-class external wait either — a `awaitSignal` step is a par-rt-db-native
lead.

The feature: a workflow step that **parks the run in a `waiting` state until
an out-of-band signal is delivered** (or an optional timeout fires). The
signal carries a payload that is recorded on the step result.

## Goals

1. New declarative step shape `awaitSignal {name, timeoutMs?}` — not a
   document txn; parks the run. A timeout counts as a failed attempt and
   routes into the step's existing retry/error policy.
2. Delivery on three surfaces — HTTP one-shot, WS frame, admin route —
   authorized like the other workflow ops; unknown id and name mismatch are
   typed errors.
3. Visibility: `waitingFor` (signal name) and `waitedSince` on
   `WorkflowInfo`; the dashboard Workflows page shows waiting runs and can
   send the signal.
4. Wire mirrored in all four clients + wire-corpus case; `make checkall`
   green across all packages.

## Non-goals

- **Signal fan-in / multiple waiters.** One run waits on exactly one named
  signal per `awaitSignal` step. Waiting on "any of N names" is a follow-up
  (a `names: []` variant) if a use case demands it.
- **FIFO signal queue.** Latest-wins single payload slot (decision below).
  Multiple deliveries while waiting are all ACKed; the step sees the last
  payload. A queue is a follow-up if multi-approver payloads matter.
- **Signals as a txn step / from inside a workflow.** `sendSignal` as a
  `Step` fires as the system principal and invites loops; signals are
  external by nature. Not in v1.
- **Dataflow into later txns.** The payload lands on the step outcome for
  observability, same rule as FM-29's no-placeholder-substitution stance.
- **Wake push (LISTEN/NOTIFY) on delivery.** Delivery flips the row to
  `pending` with a due gate; the scheduler's existing ≤ 2 s wake claims it.

## Decisions (user-approved 2026-08-21)

1. **Timeout retry waits the full `timeoutMs` again** — not backoff. Each
   timed-out attempt counts against `retry.maxAttempts`; a re-parked step
   re-waits the whole timeout (wait 24 h, timeout, retry → wait another 24 h).
2. **Latest-wins payload slot.** One `signal_payload` column on the
   `workflows` row, written atomically with the wake-flip in a single
   conditional UPDATE. No queue table, no consume race, no cleanup.

## Wire protocol

All types mirror byte-identically in `server/src/protocol.rs`,
`ts-client/src/protocol.ts`, `rust-client/src/wire.rs`,
`python-client/src/par_rt_db/wire.py`, and
`swift-client/Sources/ParRtDbClient/Wire.swift` (serde tags and field casing
load-bearing, as ever).

### `WorkflowStepSpec` — becomes a union

```json
{ "txn": { "steps": [ ... ] },
  "retry": { "maxAttempts": 3, "initialRetryMs": 1000, "maxRetryMs": 60000 },
  "sleepBeforeMs": 0 }
```

gains the alternative shape

```json
{ "awaitSignal": { "name": "approve", "timeoutMs": 86400000 },
  "retry": { "maxAttempts": 2 } }
```

- `txn` becomes `Option<Transaction>`; new `await_signal:
  Option<AwaitSignalSpec>` (`{"name": string, "timeoutMs"?: u64}`,
  camelCase, `deny_unknown_fields`).
- **Exactly one of `txn` / `awaitSignal` per step** — both or neither is
  `BadRequest` (`steps[i]` in the message).
- `awaitSignal.name`: non-empty, ≤ 256 chars. `timeoutMs`: > 0 when present.
  Omitted `timeoutMs` parks forever (cancel is the escape).
- `retry` and `sleepBeforeMs` apply to `awaitSignal` steps identically:
  `sleepBeforeMs` gates the *start* of the wait (composes via the existing
  next-gate computation, zero special-casing); `retry.maxAttempts` bounds
  timeout attempts. Defaults when `retry` omitted are unchanged
  (`maxAttempts 3`) — a default-retried 24 h gate therefore spans 3 timeouts
  before failing; callers who want fail-fast set `maxAttempts: 1`.
- Recursive step budget: an `awaitSignal` step carries no nested txn and
  contributes 0 to the FM-28 recursive counter; the 64-step top-level cap
  already bounds it.

### `WorkflowStatus` — gains `waiting`

`pending | running | success | failed | cancelled | waiting`. Additive wire
value; every client status union grows (TS literal, Rust/Python/Swift enums)
and the CLI's status parser accepts six values. A `waiting` row is
non-terminal: cancel and signal delivery both act on it.

### `WorkflowInfo` — gains wait visibility

```
waitingFor?: string   // signal name the current step waits on (waiting rows only)
waitedSince?: i64     // ms epoch the wait started; "how long" = now - waitedSince
```

Both omitted unless `status == "waiting"`.

### `StepOutcome` — gains the delivered payload

```
signal?: Value   // verbatim payload of the signal that satisfied the step
```

Omitted for txn steps and for outcomes that ended in failure. A delivery that
omits `payload` leaves `signal` omitted; a delivery carrying an explicit JSON
`null` records `signal: null` (the jsonb column distinguishes them).

### WS frame

```
SignalWorkflow { workflowId: <correlation>, id, name, payload? }
  → WorkflowAck { workflowId, ok, error? }   // existing reply shape
```

`error` is the full `RtDbError {code, message}` envelope when `ok: false` —
typed errors on the WS surface come free by reusing `WorkflowAck`.

### HTTP one-shot

```
POST /api/workflows/{id}/signal   body = { db, name, payload? }
  → 200 { "delivered": true }
  → 404 NOT_FOUND      unknown workflow id
  → 409 CONFLICT       run not in `waiting` status
  → 409 CONFLICT       name mismatch (message names both signals)
```

Bearer-token authorized and per-frame `authorize`d, exactly like
`/api/workflows/{id}/cancel`. `payload` capped at 64 KiB serialized →
`BadRequest` over limit (checked on all surfaces; WS frames don't pass
through axum's body limit, so the cap is enforced in `deliver_signal`
itself).

### Admin route

```
POST /admin/db/{db}/workflows/{id}/signal   body = { name, payload? }  → { ok }
```

Admin-key authorized; same typed errors. The dashboard and `rtdb` CLI ride
this route.

## Server design

### DDL

`workflows` gains three NULLable columns:

```sql
ALTER TABLE "<schema>".workflows ADD COLUMN IF NOT EXISTS wait_name      text;
ALTER TABLE "<schema>".workflows ADD COLUMN IF NOT EXISTS waited_since   bigint;
ALTER TABLE "<schema>".workflows ADD COLUMN IF NOT EXISTS signal_payload jsonb;
```

`CREATE TABLE IF NOT EXISTS` does not upgrade existing databases, so
`ensure_table` appends these `ALTER`s after it (runs once per scheduler
startup, the established lazy-upgrade path). `signal_payload` holds the
latest-wins slot; `wait_name`/`waited_since` are cleared whenever the row
leaves `waiting`.

### Claim and wake widen

`claim_due` and `next_due` change `WHERE status = 'pending'` to
`WHERE status IN ('pending', 'waiting')`. A `waiting` row's `sleep_until` is
its timeout gate (`i64::MAX` when `timeoutMs` omitted — never due, never
claimed). Signal delivery flips the row to `pending` with a due gate, so
post-delivery latency is the scheduler's normal ≤ 2 s wake. `WorkflowRow`
gains the three columns in its `RETURNING` set so the committer never
re-reads.

### Advance arm (`handle_workflow_advance`) — new branch, no new arm

When `spec.steps[current_step]` is an `awaitSignal` step:

1. **`signal_payload` present → signal received (success path).** One
   UPDATE consumes the slot and writes the step boundary atomically:
   append `StepOutcome {status: success, signal: payload, attempts:
   attempts + 1}`, clear `wait_name`/`waited_since`/`signal_payload`,
   `current_step += 1`, `attempts = 0`. From there the existing boundary
   logic is unchanged: last step → `finalize_success`; else compute the next
   gate from the next step's `sleepBeforeMs` and loop-or-release. Metric
   `outcome="success"`.
2. **`signal_payload` absent and `waited_since` NULL → first arrival:
   park.** One UPDATE: `status = 'waiting'`, `wait_name`,
   `waited_since = now`,
   `sleep_until = now + timeoutMs` (clamped `u64→i64`, the same wrap
   hazard the sleep gate already guards; omitted timeout → `i64::MAX`),
   `signal_payload = NULL`. The arm returns; the scheduler re-claims on
   timeout or post-delivery.
3. **`signal_payload` absent and `waited_since` set → the row parked and
   its gate expired: timeout (failure path).** `waited_since` is the
   discriminator — only `park` ever sets it, and it is cleared on consume
   and on every leave-`waiting` transition, so NULL means the arm just
   reached the step (crash recovery included: a reset-`running` row parked
   at the step boundary re-parks, a re-parked timeout claims again).
   `attempts += 1`; if `attempts < retry.maxAttempts` → **re-park with a
   fresh `now + timeoutMs` gate** (decision 1 — never backoff), metric
   `outcome="retry"`; else → `mark_failed` (clearing `wait_name`/
   `waited_since`) with
   `last_error = "awaitSignal 'name' timed out"`, metric `outcome="fail"`.

Signal-vs-timeout race resolves here: if a delivery landed between claim and
the branch check, the payload is present and the signal wins. A timeout
verdict is only reached with the slot genuinely empty.

### Delivery (`workflows::deliver_signal`)

One conditional UPDATE, side-table only — the exact precedent of
`workflows::cancel` being called from admin/WS/HTTP handlers today:

```sql
UPDATE "<schema>".workflows
   SET status = 'pending', sleep_until = $now, signal_payload = $payload,
       updated_at = $now
 WHERE id = $id AND status = 'waiting' AND wait_name = $name
RETURNING id
```

Row(s) returned → `{delivered: true}` (the scheduler wake is the flip
itself). Zero rows → re-read the row and classify:
missing → `NotFound` ("unknown workflow"); `status != waiting` →
`Conflict` ("workflow is not waiting for a signal"); name mismatch →
`Conflict` ("workflow waiting on 'X', got 'Y'"). If the row was momentarily
`running` (claimed for a timeout check), delivery gets the not-waiting 409 —
the sub-second window is documented behavior; the caller retries and either
the re-park or the terminal state answers.

Cancel and delete act on `waiting` rows (both already flip/remove
non-terminal rows); `cancel` additionally clears the three wait columns per
the leave-`waiting` rule. A late signal then classifies to 404/409 and is
never silently dropped.

### Semantics

- **At-least-once, crash-safe.** Park, delivery, and consume-plus-boundary
  are each a single atomic UPDATE on one row; there is no multi-statement
  window. A crash after a delivery commit re-claims the `pending` row and
  consumes the slot; a crash after the consume commit has the boundary
  already written. A crash mid-advance at an `awaitSignal` step is recovered
  by `reset_running` → `pending` → re-advance → re-park (the wait clock
  restarts — same at-least-once posture as every other step).
- **Single writer preserved.** `awaitSignal` steps write no documents; no
  new committer arm; no new `publish_taps` site (the tap-site list is
  unchanged — signal delivery touches only the side table, like cancel).
- **Principal.** Delivery is authenticated as the caller (machine token /
  admin key / WS session, per surface) but writes no document rows, so no
  table-allowlist applies — the same stance as cancel. Submit-time
  validation of the spec (including the new exactly-one-of rule) applies at
  all four start surfaces as today.
- **Timeout accounting.** Only timeouts increment `attempts` on an
  `awaitSignal` step; a delivered signal is a success and resets them.

### Metrics

No new series. `rtdb_workflow_steps_total{outcome=success|retry|fail}`
classifies signal-received / timeout-retry / timeout-fail respectively.

## Client mirrors

All four clients, same change (parallel subagents after the server lands):

- **Wire types**: `awaitSignal` field on the step spec (exactly-one-of
  validation ported completely — partial validation ports are invisible
  debt), `waiting` status value, `waitingFor`/`waitedSince`, `signal` on
  `StepOutcome`, `SignalWorkflow` frame + `WorkflowAck` reuse.
- **DSL builders**: `awaitSignal(name, timeoutMs?)` step builder;
  `signalWorkflow(id, name, payload?)` on WS/reactive/HTTP/admin surfaces.
- **In-memory harness engines** (ts, python, swift): model the parked state
  in `tick()` — an `awaitSignal` step parks against the injected clock,
  `signal()` delivers (latest-wins), a tick past `waitedSince + timeoutMs`
  fires the timeout attempt. The rust harness documents its existing
  Internal-error asymmetry rather than porting the engine, per the FM-29
  mirror note.
- **CLI**: `rtdb workflows signal <id> --db <db> --name <n> [--payload-json
  '<json>']`; `workflows list` parses six statuses; list output shows
  `waitingFor`.
- **Dashboard**: Workflows page — `waiting` status chip, "waiting on
  `<name>` for `<duration>`" line, and a Send Signal dialog (name prefilled
  from `waitingFor`, payload JSON editor) posting the admin route.
- **Wire-corpus**: new case — a spec fixture with an `awaitSignal` step, the
  `SignalWorkflow` frame, a `waiting` `WorkflowInfo` with the visibility
  fields, and a `StepOutcome` carrying `signal`. All five runners execute it.

## Testing

Server integration tests (extend `server/tests/workflows_test.rs`; dev
Postgres via the standard harness). Card criteria in brackets.

1. **Park → deliver → advance** [criterion 1]: run parks at `awaitSignal`
   (`status = waiting`, `waitingFor` visible), signal delivered via HTTP,
   run advances; `stepOutcomes` records the payload verbatim; final status
   `success`. Same flow via WS frame and admin route.
2. **Timeout → retry → signal** [criterion 1]: short `timeoutMs`, no
   signal → re-parked with a **fresh** full timeout (pinned: new gate −
   re-park time ≈ timeoutMs, not backoff); second attempt satisfied by a
   signal → success with `attempts` accounting correct.
3. **Timeout → exhaustion** [criterion 1]: `maxAttempts` exhausted →
   `failed`, `last_error = "awaitSignal 'name' timed out"`, later steps
   never execute.
4. **Typed errors** [criterion 2]: unknown id → 404 `NOT_FOUND`; name
   mismatch → 409 `CONFLICT` naming both signals; delivery to a
   non-waiting (pending/success/failed/cancelled) run → 409. On WS the ack
   carries the same `{code, message}`.
5. **Authorization** [criterion 2]: missing/invalid token rejected on HTTP
   and WS; non-admin on the admin route; the routes re-run `authorize` per
   request.
6. **Latest-wins**: two deliveries while waiting → both `{delivered:
   true}`, step outcome carries the second payload.
7. **No timeout**: omitted `timeoutMs` → row stays `waiting` past any
   elapsed test time (never claimed); signal still advances it.
8. **Composition**: `sleepBeforeMs` before an `awaitSignal` step gates the
   wait start; `retry` defaults documented (3 attempts).
9. **Cancel while waiting** → `cancelled`; subsequent signal → 409.
10. **Crash recovery**: orphaned `running` row at an `awaitSignal` step →
    `reset_running` → re-advance re-parks; delivered-then-crashed row
    consumes on re-claim (at-least-once).
11. **Submit validation**: both/neither `txn`+`awaitSignal`, empty/oversize
    name, `timeoutMs: 0`, payload > 64 KiB → `BadRequest`.
12. **Visibility** [criterion 3]: `listWorkflows` filters by `waiting`;
    `waitingFor`/`waitedSince` present only while waiting.
13. **Regression**: txn-only specs behave byte-identically (existing suite
    untouched and green).

Client tests per mirror (ts vitest **and** `bun run typecheck`, rust cargo,
python pytest incl. harness `tick()`, swift `make swift-client-checkall`).

## Documentation

- `FEATURE_MATRIX.md` — FM-29 row extended with `awaitSignal` + mirror
  status.
- Server/client READMEs (workflow DSL section), `rtdb` CLI help, dashboard
  README.
- `CLAUDE.md` — one line in the workflows paragraph (`waiting` state +
  signal delivery surfaces; explicitly no new committer arm or tap site).
- No new env vars — nothing for `.env.example`/`docker-compose.yml`.

## Invariants preserved

- **Single-writer**: no document writes anywhere in this feature; the
  committer remains the only document writer and the only advancer.
- **SQL construction**: validated/quoted identifiers (schema via
  `validate_db_name`), every value bound via `$n`.
- **Errors**: `RtDbError` envelope throughout; client-facing messages carry
  no sqlx/serde internals.
- **Clients mirror the core**: server wire change lands in all four clients
  + harnesses + corpus in the same change; a client that can't carry a
  surface files the gap explicitly.
- **Op-feed tap**: unchanged — `awaitSignal` writes no documents, so the
  tap-site enumeration stays as ARCHITECTURE.md lists it.
