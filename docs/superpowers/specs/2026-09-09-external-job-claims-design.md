# External job claims with fencing tokens

**Date:** 2026-09-09 · **Card:** `01a08981926c7c3288668ff321935cda` (par-rt-db backlog, filed by par-automate) · **FM row:** #9 extension

## Problem

par-rt-db's durable job store (`scheduled_txns`) is executed end-to-end by the
internal scheduler: `claim_due` picks up due rows with `FOR UPDATE SKIP LOCKED`
and the committer executes them as the bypass principal. Application workers
(specifically Par Automate, executing external actions the server cannot run
itself) cannot use these jobs: there is no way for an app worker to claim a job,
and the claim paths expose no lease generation, so a worker that resumes after
lease expiry or server recovery can race with its replacement.

## Contract (the card's acceptance criteria)

1. A claim atomically assigns a monotonic per-job lease generation (fencing
   token). Every external claim runs `claim_generation = claim_generation + 1`
   inside the claim UPDATE — the row lock from `FOR UPDATE SKIP LOCKED` makes
   the assignment atomic; generation only ever increases per job.
2. Completion, retry, and failure transitions reject a stale token. Each
   finalize transition is a conditional UPDATE guarded by
   `status='running' AND external AND claim_generation = $lease`; a stale token
   (or a non-running / non-external row) is a `409 CONFLICT`, not a silent no-op.
3. Lease expiry or restart recovery permits a new claim and preserves rejection
   of the former owner. `reset_running` (crash recovery) does not touch external
   rows — a live external lease survives a server restart; an expired lease is
   re-claimable through the claim predicate. Recovery never resets the
   generation, so the former owner's token stays stale after any re-claim.

## Design

### Data model (additive, `scheduled_txns`)

| column | type | meaning |
|---|---|---|
| `external` | `boolean NOT NULL DEFAULT false` | never internally executed; served to app workers |
| `claim_generation` | `bigint NOT NULL DEFAULT 0` | monotonic per-job fencing token |
| `lease_deadline_ms` | `bigint` (NULL when not held) | lease expiry instant (epoch ms) |

Both CREATE paths (create-database DDL and `scheduler::ensure_table`) gain the
columns; `ensure_table` runs `ADD COLUMN IF NOT EXISTS` ALTERs for pre-existing
databases, same additive discipline as the `every_ms` precedent.

### Create path — `external` flag

`Step::Schedule`, the WS `Schedule` frame, and HTTP `POST /api/schedule` gain an
optional `external: bool` (default false). External jobs are **never executed by
the internal scheduler** — `claim_due`/`next_due`/`reset_running` all exclude
`external` rows — so an external job sits `pending` until an app worker claims
it. Ordinary jobs are unaffected; the flag is absent by default everywhere.

### Claim surface (HTTP-only)

Workers poll over HTTP; no WS frames (the WS Schedule frame only gains the
create flag). Same auth as every schedule op: db token, non-read-only,
rate-limited, `ensure_table` inline (cold-db guard).

- `POST /api/schedule/claim {db, limit?, leaseMs?}`
  → `{jobs: [{id, kind, dueAt, txn, cron?, everyMs?, leaseGeneration, leaseDeadlineMs}]}`

  Atomic claim UPDATE over `FOR UPDATE SKIP LOCKED`:
  due `pending` external rows, or `running` external rows with an **expired**
  lease. Sets `status='running'`, bumps `claim_generation`, stamps
  `lease_deadline_ms = now + leaseMs`. Caps: `limit` default 8 / max 64
  (CLAIM_BATCH); `leaseMs` default 300_000, min 1_000, max 86_400_000 (24h).

- `POST /api/schedule/{id}/complete {db, lease}`
  → `{ok: true}`; one-shot → row deleted (same terminal as internal one-shot
  success); cron/interval → advances to its next due instant (fired_count+1,
  back to `pending`). Stale lease → `409 CONFLICT`.
- `POST /api/schedule/{id}/retry {db, lease, delayMs?, error?}`
  → `{ok: true}`; re-arms at `now + delayMs` (default 60_000, max MAX_EVERY_MS),
  records `last_error`, keeps the generation history. Stale lease → 409.
- `POST /api/schedule/{id}/fail {db, lease, error}` → `{ok: true}`; terminal
  `error` status + `last_error` (required). Stale lease → 409. Reviving an
  `error` external job is future scope (the existing `resume` op only lifts
  `paused`).

### What recovery does and does not reset

- `reset_running` resets internal `running` rows only (`external = false`).
  A live external lease survives a restart: the row stays `running` and the
  holder's token stays current.
- An external `running` row whose lease expired while the server was down is
  immediately re-claimable through the claim predicate — no recovery needed.

## Scope decisions

- **HTTP-only finalize surface.** Workers poll; adding WS claim/finalize frames
  would double the client mirror for no worker need.
- **No new error code.** Stale lease uses the existing `CONFLICT` (409) — the
  error-codes corpus is untouched.
- **No semantics-corpus case.** No query result, step result, visibility, or
  error-code change; claim semantics are not expressible in the semantics
  corpus's op vocabulary. The new wire fields (`external` on Schedule
  steps/frames, `external` on ScheduleInfo) are pinned by `wire-corpus.json`
  encode/decode fixtures instead.
- **Client mirrors (all four).** Wire types gain the optional flag and
  `ScheduleInfo.external`; each client's HTTP API gains
  `claimSchedules`/`completeSchedule`/`retrySchedule`/`failSchedule`. The TS
  (and any modeling) in-memory harnesses' internal `tick()`/claim simulation
  must skip external jobs, mirroring server semantics.
- **No admin surface.** The existing schedule manage ops (cancel/pause/resume)
  already work on external jobs through the API surface; an admin view of lease
  state is future scope.

## Mirrors

| surface | ts | rust | python | swift |
|---|---|---|:--|---|
| `external` on schedule create (WS frame + step + HTTP) | ✓ | ✓ | ✓ | ✓ |
| `ScheduleInfo.external` | ✓ | ✓ | ✓ | ✓ |
| `claimSchedules` / `completeSchedule` / `retrySchedule` / `failSchedule` (HTTP) | ✓ | ✓ | sync+async | ✓ |
| wire-corpus schedule fixtures gain the flag | ✓ | ✓ | ✓ | ✓ |
