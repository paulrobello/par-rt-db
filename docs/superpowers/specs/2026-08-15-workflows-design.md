# Durable declarative workflows (FM-29)

Date: 2026-08-15
Status: approved design (user-approved 2026-08-15)
Board card: FM-29 (`[FM-29] Durable declarative workflows`)
Depends on: FM-28 `Step::Schedule` (shipped 2026-08-14) — recursive step
validation and the table-allowlist tightening are reused, and workflow step
txns may themselves carry `Schedule`/`CancelSchedule` steps.

## Problem

Long-running multi-step jobs — agent chains, ingest pipelines, drip
sequences, retry-with-backoff — must today be hand-rolled as chained
client-side schedule calls: no retry policy, no durability across client
restarts, no way to see "which step is this run on". Convex ships this as the
Workflow component. par-rt-db needs the declarative analog: a workflow is
**data** (an ordered list of ordinary `Transaction`s plus retry/sleep
policy), advanced by the server inside the committer's serialized turn.

## Goals

1. A per-db `workflows` side table is the state machine: each row is one run
   (spec snapshot, current step, attempts, status, sleep gate, per-step
   outcome trail).
2. Steps advance inside the committer turn with per-step retry policy
   (max attempts + exponential backoff) and declarable sleeps; a crash
   mid-workflow resumes (at-least-once per step, the documented
   scheduled-txn contract).
3. Start/cancel from all four client surfaces (txn step, WS frame, HTTP
   route, admin) — the FM-28 mirror pattern — plus list/get observability
   (admin API, CLI, dashboard page).
4. `make checkall` green across all six packages.

## Non-goals

- **Dataflow between steps.** Convex threads step results because steps are
  code; declarative steps are static `Transaction`s. No placeholder
  substitution in v1 (follow-up card if a use case demands it). Step
  *outcomes* are recorded for observability, not for later steps to consume.
- **Parallel/branching steps.** Sequential ordered steps only. `steps.parallel`/
  `steps.branch` analogs are a follow-up card.
- **Named reusable definitions.** A run snapshots its spec inline; there is no
  server-side definitions library with versioning. A follow-up card can add
  named templates if hand-editing specs at every start site hurts.
- **Per-row authorization pre-checks on step txns** — unsound (rows change
  between submit and advance). Steps fire as the system (bypass) principal;
  same documented behavior as scheduled jobs.
- **Terminal-row retention policy.** Finished runs stay as the audit trail
  until an operator deletes them (`DELETE /admin/db/{db}/workflows/{id}`).
  No retention env var in v1.
- **Pause/resume of runs.** Cancel only. Pause/resume can follow the
  schedules pattern later if operators need it.

## Wire protocol

All types below mirror byte-identically in `server/src/protocol.rs`,
`ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, and
`python-client/src/par_rt_db/wire.py` (serde tags and field casing are
load-bearing, as ever).

### WorkflowSpec

```json
{
  "name": "onboarding-drip",
  "steps": [
    { "txn": { "steps": [ ... ] },
      "retry": { "maxAttempts": 3, "initialRetryMs": 1000, "maxRetryMs": 60000 },
      "sleepBeforeMs": 0 },
    { "txn": { "steps": [ ... ] }, "sleepBeforeMs": 86400000 }
  ]
}
```

- `name` — required human label for list/dashboard; not unique (runs are
  identified by id).
- `steps[k].txn` — a full `Transaction`, same type the standalone Mutate op
  accepts. May carry `Schedule`/`CancelSchedule` steps (FM-28 recursive
  validation applies).
- `steps[k].retry` — optional `StepRetry { maxAttempts: u32 ≥ 1,
  initialRetryMs: u64 > 0, maxRetryMs: u64 ≥ initialRetryMs }`. Defaults
  when omitted: `maxAttempts 3, initialRetryMs 1000, maxRetryMs 60000`.
- `steps[k].sleepBeforeMs` — optional `u64`, default 0. Gates the step's
  *first* attempt: the step does not execute until `sleep_until` passes.
  Retries use backoff, not `sleepBeforeMs`.
- Validation at submit: 1 ≤ steps ≤ `MAX_WORKFLOW_STEPS` (64); recursive
  step count summed across every step's txn (FM-28 counter) ≤ `MAX_STEPS`
  (1024); `retry` fields within bounds. Violations are `BadRequest` before
  anything is written.

### WorkflowInfo (list/get projection)

```
{ id, name, status, currentStep, stepCount, attempts,
  sleepUntil?, lastError?, createdAt, updatedAt, startedAt?, finishedAt? }
```

`GET .../{id}` additionally returns `stepOutcomes`:
`[ { stepIndex, status: "success"|"failed", attempts, at, error? } ]` — one
entry per completed (or terminally failed) step attempt-batch, in order.

### Txn steps (`Step` enum, tag `"op"`, camelCase)

```json
{ "op": "startWorkflow", "spec": { ...WorkflowSpec... } }   → { "workflowId": "<id>" }
{ "op": "cancelWorkflow", "id": "<workflow id>" }           → { "cancelled": <bool> }
```

- `startWorkflow` inserts the run row **on the open sqlx transaction**
  (FM-28's `insert_on` pattern): "write sign-up doc + start drip" is atomic;
  a rolled-back txn leaves no orphan run.
- `cancelWorkflow` flips the row to `cancelled` (see Semantics) via a
  same-connection UPDATE, pushing `{"cancelled": <bool>}` (`true` iff the
  row was in a non-terminal state).
- Both return `None` from `Step::table()`; `worst_case_affected` counts 0;
  they contribute 1 to `MAX_STEPS` (the spec's nested steps are counted
  recursively like FM-28's nested txn).

### WS frames (`ClientMessage`, tag `"type"`, camelCase)

```
StartWorkflow  { workflowId: <correlation>, spec }      → ServerMsg echoing workflowId + WorkflowInfo
CancelWorkflow { workflowId: <correlation>, id }        → echoes + { cancelled: bool }
ListWorkflows  { workflowId: <correlation>, status? }   → echoes + [ WorkflowInfo ]
```

Mirrors the `Schedule`/`ListSchedules` correlation-id style. Authorized per
frame (authorize re-runs every op); the submitter's table allowlist is
enforced at submit (see Semantics).

### HTTP one-shot routes

```
POST /api/workflows            body = WorkflowSpec            → { id }        (start)
POST /api/workflows/{id}/cancel                               → { cancelled } (cancel)
POST /api/workflows/list       body = { status? }             → [ WorkflowInfo ]
```

Bearer-token authorized, same as `/api/schedule*`.

### Admin routes

```
GET    /admin/db/{db}/workflows?status=&limit=   → [ WorkflowInfo ]
POST   /admin/db/{db}/workflows                  (body = WorkflowSpec) → { id }
GET    /admin/db/{db}/workflows/{id}             → WorkflowInfo + stepOutcomes
POST   /admin/db/{db}/workflows/{id}/cancel      → { cancelled }
DELETE /admin/db/{db}/workflows/{id}             → hard-delete one run row
```

## Server design

### Data model

```sql
CREATE TABLE IF NOT EXISTS "<schema>".workflows (
    id            text PRIMARY KEY,
    name          text NOT NULL,
    status        text NOT NULL,        -- pending|running|success|failed|cancelled
    spec          jsonb NOT NULL,       -- WorkflowSpec snapshot
    current_step  int  NOT NULL,        -- index of the NEXT step to execute
    attempts      int  NOT NULL,        -- attempts spent on current_step
    sleep_until   bigint NOT NULL,      -- advance gate; pending rows claim when <= now
    step_outcomes jsonb NOT NULL DEFAULT '[]',
    last_error    text,
    created_at    bigint NOT NULL,
    updated_at    bigint NOT NULL,
    started_at    bigint,               -- first advance claim time
    finished_at   bigint
);
CREATE INDEX ... ON workflows (status, sleep_until);
```

`ensure_table` runs once at scheduler startup (the `mutation_log`/
`scheduled_txns` pattern), so databases that predate this feature gain the
table lazily.

### New module `server/src/workflows.rs`

Owns the table DDL + all row operations, mirroring `scheduler.rs`:
`ensure_table`, `insert_on` (open sqlx tx, for the txn step) + `insert`
(pool, for WS/HTTP/admin start), `claim_due` (`pending` + `sleep_until <=
now` → `running`, `FOR UPDATE SKIP LOCKED`, batch-capped), `reset_running`
(orphaned `running` → `pending`, crash recovery), `advance_success` /
`schedule_retry` / `finalize_*` / `cancel` / `mark_failed` bookkeeping
UPDATEs, `list` / `get`. All identifiers validated-and-quoted, values bound
via `$n`.

### Scheduler loop extension (`scheduler.rs`)

`run_scheduler` keeps owning the per-db timer — **no new task**. Changes:

- Startup additionally calls `workflows::ensure_table` and
  `workflows::reset_running`.
- The wake computation takes `min(scheduled next_due, workflows min
  sleep_until among pending)`; when either is due, both `claim_due` sweeps
  run (ARC-102 skip logic preserved per table — claim only what is due).
- Each claimed run enqueues `CommitterRequest::RunWorkflowAdvance { row }`
  on the existing committer channel, exactly like `RunScheduled`.

### Committer arm (`committer.rs`)

New `CommitterRequest::RunWorkflowAdvance { row }` (fire-and-forget like
`RunReaper`), handled by `handle_workflow_advance`:

1. **Quota**: storage-cap `enforce` at entry (best-effort stale read,
   uniform, no admin bypass). A rejection counts as a step *failure subject
   to the step's retry policy* — unlike `handle_scheduled`'s terminal
   `mark_error`, because a cap raised mid-retry lets the run recover.
2. **Execute**: `execute_txn(pool, db, schema, spec.steps[current_step].txn,
   PrincipalCtx::bypass())` — the same fire path as scheduled jobs.
3. **On success**: append the step outcome; `current_step += 1`;
   `attempts = 0`; if that was the last step → `status = success`,
   `finished_at`; else compute the next gate
   (`now + next.sleepBeforeMs`) and **loop in the same committer turn**
   while the gate is already due — a no-sleep N-step chain completes in one
   turn (bounded by 64 steps). **Claim discipline during the loop:** the
   scheduler only ever claims `pending` rows, so the arm keeps the row
   `running` while looping — per-step bookkeeping (`current_step`,
   `attempts`, `step_outcomes`) is written at each step boundary, but the
   row returns to `pending` (with its future gate) only when the arm stops
   advancing (sleep gate, retry backoff, or terminal state). A crash
   mid-loop therefore re-advances from the last written `current_step` —
   at-least-once per step, never double-claim. Each step's `execute_txn`
   result publishes through `publish_taps(source = "workflow")` — the
   tap-site list grows from six arms to seven, preserving the "every
   durable write publishes here" guarantee.
4. **On failure**: `attempts += 1`; if `attempts < maxAttempts` →
   `sleep_until = now + backoff`, back to `pending` (metric
   `rtdb_workflow_steps_total{outcome="retry"}`); else → `status = failed`,
   `last_error` (metric `{outcome="fail"}`), `finished_at`.

`backoff(attempts) = min(initialRetryMs * 2^(attempts-1), maxRetryMs)`.

### Semantics

- **Delivery: at-least-once per step.** A crash after `execute_txn` commits
  but before the bookkeeping UPDATE re-executes that step on resume (the
  scheduled-txn contract). Steps should be idempotent-by-construction —
  deterministic ids, `upsert`, `ExpectAbsent`.
- **Crash resume**: any `running` row was orphaned mid-advance;
  `reset_running` at scheduler startup returns it to `pending` (its
  `sleep_until` is already past), so the run re-advances.
- **Principal**: steps fire as the system (bypass) principal, `owner = None`
  in tap payloads — same as `handle_scheduled`/`handle_reaper`. The
  *submitter's* machine-token table allowlist is enforced **at submit time**
  over the whole spec recursively (`authorize_txn_tables`, all four start
  surfaces, FM-28's tightened pattern); per-row rules are not pre-checked
  (non-goal, same rationale as FM-28).
- **Cancel semantics**: a `pending` row is deleted-never — it flips to
  `cancelled` (the row is the audit trail). Cancel of a `running` row is
  honored at the next step boundary: the arm checks the row's status between
  steps and stops advancing. Cancel of a terminal row returns
  `{"cancelled": false}`.
- **First advance latency**: a fresh run with `sleepBeforeMs 0` on step 0 is
  claimed on the scheduler's next wake (≤ 2 s), like a scheduled job.

### Metrics

- Prometheus: `rtdb_workflow_steps_total{outcome=success|retry|fail}` (no
  per-db label — cardinality rule).
- `/admin/metrics` JSON only: per-db counts by status.

## Client mirrors

Per the established four-clients-one-contract rule, all in the same change
(implementation runs as parallel subagents after the server lands):

- **ts-client**: wire types; `TxnBuilder.startWorkflow(spec)` /
  `.cancelWorkflow(id)`; reactive-client `startWorkflow`/`cancelWorkflow`/
  `listWorkflows` methods; admin client workflows list/get/start/cancel;
  in-memory harness advances runs in `tick()` (due steps execute against
  harness state; retry gates and `sleepBeforeMs` honored against the tick
  clock). Verified with `bun run typecheck` + vitest (green vitest is not
  typecheck).
- **rust-client**: `wire.rs` step/enum mirrors + `Transaction` builder
  methods + http/ws/admin surfaces + tests.
- **python-client**: `wire.py` union + `Mutation` DSL methods + async `ws`
  client + sync http/admin + in-memory harness `tick()` + pytest.
- **cli**: `rtdb workflows list|get|start|cancel --db …` (admin-key
  surfaces, mirroring the existing admin subcommands).
- **dashboard**: `/workflows` page — per-db run table (name, status chip,
  step n/m, attempts, sleep-until, last error), status/stuck filter,
  expandable per-step timeline from `stepOutcomes`, cancel button, and a
  start-from-JSON editor (the schema-push pattern). Polls like the other
  operator pages; no new subscription surface.
- Wire-corpus tests: `startWorkflow`/`cancelWorkflow` step entries + spec
  fixtures in all four client protocol tests.

## Testing

Server integration tests (new `server/tests/workflows_test.rs`, dev
Postgres via the standard harness):

1. **Happy path**: 3-step workflow, no sleeps — all steps' writes durable,
   final status `success`, `stepOutcomes` records 3 successes, run
   observable in admin list.
2. **Retry then success**: step 1 fails twice (e.g. `ExpectVersion`
   mismatch seeded to clear), succeeds on attempt 3 — attempts counter and
   outcome trail correct; backoff observed via `sleep_until`.
3. **Exhaustion**: step fails past `maxAttempts` → `failed`, `lastError`
   set, no further advancement; later steps never execute.
4. **Sleeps**: `sleepBeforeMs` on step 1 — step 0 advances immediately,
   step 1 not until the gate passes.
5. **Crash resume**: simulate by manually flipping a row to `running`
   (orphan), restart path (`reset_running`) → run re-advances (at-least-once
   asserted).
6. **Cancel mid-flight**: cancel between steps stops advancement; cancel of
   terminal returns false.
7. **Txn-step atomicity**: `Insert(doc) + startWorkflow` commits both;
   a failing later step rolls back the run row too. `cancelWorkflow`
   result shapes.
8. **Allowlist**: scoped token cannot start a workflow whose steps write an
   out-of-allowlist table — via all four start surfaces.
9. **Quota**: storage-cap rejection counts as retryable failure; run
   proceeds after the cap is raised (or fails after exhaustion).
10. **Bounds**: > 64 steps, > 1024 recursive steps, empty steps, bad retry
    fields → `BadRequest`.
11. **Tap coverage**: step writes appear in the op feed with
    `source = "workflow"`.

Client tests per mirror (ts vitest+typecheck, rust cargo test, python
pytest incl. harness `tick()`).

## Documentation

- `FEATURE_MATRIX.md` #29 → ✅ with client-mirror status.
- `CLAUDE.md`: committer arm list + tap-site list grow to include
  `handle_workflow_advance`; data-pipeline step list gains
  `StartWorkflow`/`CancelWorkflow`; a workflows paragraph in the
  architecture section.
- Server + client READMEs (DSL section), `rtdb` CLI help, dashboard
  README.
- No new env vars — nothing to add to `.env.example`/`docker-compose.yml`.

## Invariants preserved

- **Single-writer**: advancement runs only in the committer turn; the
  scheduler task writes nothing (claim/reset are the same side-table writes
  `scheduler.rs` already makes for `scheduled_txns`).
- **SQL construction**: validated/quoted identifiers (schema from
  `validate_db_name`), every value bound via `$n`.
- **Errors**: `RtDbError` envelope; submit-time validation is `BadRequest`,
  allowlist violation `Forbidden` (403); client-facing 500s stay generic.
- **Clients mirror the core**: server wire change lands in all four clients
  + harness in the same change.
- **Op-feed tap**: every step write publishes through `publish_taps` —
  `handle_workflow_advance` joins the tap-site list.
