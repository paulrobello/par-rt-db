# Durable Declarative Workflows Implementation Plan (FM-29)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship durable multi-step workflows — a per-db `workflows` state-machine table whose steps are ordinary `Transaction`s with per-step retry/backoff and sleeps, advanced inside the committer turn, startable/cancellable from all four client surfaces, observable via admin API, CLI, and dashboard.

**Architecture:** A new `server/src/workflows.rs` module owns the side table (DDL, claim, bookkeeping). The existing per-db scheduler loop polls both `scheduled_txns` and `workflows` and enqueues `RunWorkflowAdvance` onto the same committer channel; the committer arm executes the current step's txn via `execute_txn` as the bypass principal, publishes through `publish_taps(source="workflow")`, and loops while gates are due (row stays `running` until it stops advancing). Clients mirror the wire types per the four-clients-one-contract rule.

**Tech Stack:** Rust (axum/tokio/sqlx, server), TypeScript (ts-client, dashboard), Rust (rust-client, cli), Python (python-client). Postgres 17.

**Spec:** `docs/superpowers/specs/2026-08-15-workflows-design.md` (approved, commit ec19213). The plan argues from the spec — executors read both.

## Global Constraints

- `make checkall` (run as `make -C /Users/probello/Repos/par-rt-db checkall`) is the definition of done; server integration tests need the dev Postgres: `make -C /Users/probello/Repos/par-rt-db dev-db-up` first.
- Wire casing is load-bearing and deliberately non-uniform: `Step` is `#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]`; `ClientMessage`/`ServerMessage` are `tag = "type", rename_all = "camelCase"`; `WorkflowStatus` serializes snake_case (`"pending"` etc.). Every server wire change lands in ts-client + rust-client + python-client + in-memory harness in the same feature.
- SQL: validate + double-quote every identifier (schema via `validate_db_name`/`pg_schema`), bind every value via `$n`. Never interpolate a value.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`.
- Errors: `RtDbError` envelope; spec validation is `BadRequest`, allowlist violation `Forbidden` (403); client-facing 500s stay generic.
- Single-writer invariant: step execution happens ONLY in the committer turn (`handle_workflow_advance`); the scheduler task never executes a step.
- No new env vars.
- Run cargo from `server/` (or `--manifest-path server/Cargo.toml`); `make` always as `make -C <repo-root>` (a bare `make checkall` after a `cd` runs a partial gate — known trap).
- Commits: conventional style (`feat(workflows): …`), one per task, directly on `main` (trunk-based repo). Pushing requires explicit user confirmation — do not push.

## File Structure

- Create: `server/src/workflows.rs` — side-table DDL + row ops + spec validation + backoff math.
- Create: `server/src/admin/workflows.rs` — admin routes (thin, mirrors `admin/schedules.rs`).
- Create: `server/tests/workflows_test.rs` — integration tests.
- Modify: `server/src/protocol.rs` — workflow wire types; WS frame variants (Task 5).
- Modify: `server/src/txn.rs` — `Step::StartWorkflow`/`CancelWorkflow` + step handlers + recursive validation.
- Modify: `server/src/committer.rs` — `RunWorkflowAdvance` arm.
- Modify: `server/src/scheduler.rs` — poll both tables.
- Modify: `server/src/ws.rs`, `server/src/http_api.rs`, `server/src/admin/mod.rs`, `server/src/metrics.rs`, `server/src/lib.rs` (module registration).
- Modify (mirrors): `ts-client/src/{protocol.ts,client.ts or split,builder file,admin file,in-memory harness}`; `rust-client/src/{wire.rs,builder,http,ws,admin}`; `python-client/src/par_rt_db/{wire.py,mutation dsl,http,ws,in_memory}`; `cli/src/main.rs`; `dashboard/src/*` (new Workflows page).
- Modify (docs): `FEATURE_MATRIX.md`, `CLAUDE.md`, server/ts/rust/python READMEs, dashboard README.

Task order: 1→2→3→4→5→6 sequential (server); 7, 8, 9 parallel (client mirrors, after 6); 10 after 8; 11 after 7; 12 last.

---

### Task 1: Workflow wire types (server protocol)

**Files:**
- Modify: `server/src/protocol.rs` (add types after `ScheduleInfo`; serde tests in the file's `#[cfg(test)]` tests)
- Test: `server/src/protocol.rs` (inline tests)

**Interfaces:**
- Consumes: `Transaction` (already in protocol.rs).
- Produces (used by Tasks 2–9): `WorkflowSpec { name: String, steps: Vec<WorkflowStepSpec> }`, `WorkflowStepSpec { txn: Transaction, retry: Option<StepRetry>, sleep_before_ms: Option<u64> }`, `StepRetry { max_attempts: u32, initial_retry_ms: u64, max_retry_ms: u64 }` (Default = `{3, 1000, 60000}`), `WorkflowStatus` (Pending|Running|Success|Failed|Cancelled, snake_case wire, `FromStr`, `as_wire_str`), `StepOutcome { step_index: u32, status: OutcomeStatus, attempts: u32, at: i64, error: Option<String> }`, `OutcomeStatus` (Success|Failed, lowercase wire), `WorkflowInfo { id, name, status, current_step, step_count, attempts, sleep_until: Option<i64>, last_error: Option<String>, created_at, updated_at, started_at: Option<i64>, finished_at: Option<i64> }`, `WorkflowInfoFull { info: WorkflowInfo, step_outcomes: Vec<StepOutcome> }`.

- [ ] **Step 1: Write failing serde tests**

Add to `protocol.rs`'s `#[cfg(test)] mod tests` (mirror the existing `ClientMessage` serialization test at ~line 494):

```rust
#[test]
fn workflow_spec_wire_shape() {
    let spec = serde_json::from_value::<WorkflowSpec>(serde_json::json!({
        "name": "drip",
        "steps": [
            { "txn": { "steps": [ { "op": "insert", "table": "t", "doc": {} } ] } },
            { "txn": { "steps": [] },
              "retry": { "maxAttempts": 5, "initialRetryMs": 500, "maxRetryMs": 2000 },
              "sleepBeforeMs": 86400000 }
        ]
    }))
    .unwrap();
    assert_eq!(spec.steps.len(), 2);
    assert_eq!(spec.steps[1].sleep_before_ms, Some(86_400_000));
    let retry = spec.steps[1].retry.unwrap();
    assert_eq!((retry.max_attempts, retry.initial_retry_ms, retry.max_retry_ms), (5, 500, 2000));
    // Omitted retry defaults on deserialize:
    assert!(serde_json::from_value::<StepRetry>(serde_json::json!({"maxAttempts": 2}))
        .unwrap()
        .initial_retry_ms == 1_000);
    // Round-trip: absent optionals are SKIPPED on serialize (corpus parity).
    let v = serde_json::to_value(&spec).unwrap();
    assert!(v["steps"][0].get("retry").is_none());
    assert!(v["steps"][0].get("sleepBeforeMs").is_none());
}

#[test]
fn workflow_status_wire_is_snake_case() {
    assert_eq!(serde_json::to_value(WorkflowStatus::Pending).unwrap(), json!("pending"));
    assert_eq!("failed".parse::<WorkflowStatus>().unwrap(), WorkflowStatus::Failed);
    assert!("bogus".parse::<WorkflowStatus>().is_err());
}

#[test]
fn workflow_info_wire_shape() {
    let info = serde_json::from_value::<WorkflowInfo>(serde_json::json!({
        "id": "wf1", "name": "drip", "status": "pending",
        "currentStep": 0, "stepCount": 3, "attempts": 0,
        "sleepUntil": 123, "createdAt": 1, "updatedAt": 2
    }))
    .unwrap();
    assert_eq!(info.step_count, 3);
    assert!(info.last_error.is_none());
    let v = serde_json::to_value(&info).unwrap();
    assert!(v.get("lastError").is_none() && v.get("finishedAt").is_none());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --lib protocol::tests::workflow -- --nocapture`
Expected: compile error (`WorkflowSpec` etc. not found).

- [ ] **Step 3: Implement the types**

Add to `protocol.rs` (after `ScheduleInfo`, same derive/style as neighbors):

```rust
/// Per-step retry policy (FM-29). `maxAttempts` counts TOTAL attempts — the
/// first try included. Defaults when a step omits `retry`: 3 attempts, 1s
/// initial backoff doubling to a 60s cap.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepRetry {
    pub max_attempts: u32,
    #[serde(default = "default_initial_retry_ms")]
    pub initial_retry_ms: u64,
    #[serde(default = "default_max_retry_ms")]
    pub max_retry_ms: u64,
}

fn default_initial_retry_ms() -> u64 { 1_000 }
fn default_max_retry_ms() -> u64 { 60_000 }

impl Default for StepRetry {
    fn default() -> Self {
        Self { max_attempts: 3, initial_retry_ms: 1_000, max_retry_ms: 60_000 }
    }
}

/// One workflow step: an ordinary `Transaction` plus policy. The txn may
/// itself carry `Schedule`/`CancelSchedule` steps (FM-28 rules apply).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowStepSpec {
    pub txn: Transaction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<StepRetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_before_ms: Option<u64>,
}

/// A submitted workflow definition. Stored verbatim per run — a run
/// snapshots its spec, so template edits never drift a live run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowSpec {
    pub name: String,
    pub steps: Vec<WorkflowStepSpec>,
}

/// Run lifecycle. Closed domain (ARC-004/QA-008 pattern — was never a free
/// string). Snake-case wire: pending|running|success|failed|cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus { Pending, Running, Success, Failed, Cancelled }

impl WorkflowStatus {
    pub fn as_wire_str(&self) -> &'static str {
        match self { WorkflowStatus::Pending => "pending", WorkflowStatus::Running => "running",
            WorkflowStatus::Success => "success", WorkflowStatus::Failed => "failed",
            WorkflowStatus::Cancelled => "cancelled" }
    }
}

impl std::str::FromStr for WorkflowStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s { "pending" => Ok(Self::Pending), "running" => Ok(Self::Running),
            "success" => Ok(Self::Success), "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled), other => Err(format!("unknown WorkflowStatus: {other}")) }
    }
}

/// Terminal record for one step: completed successfully, or exhausted its
/// retries (`status: failed`). Individual retried attempts are NOT recorded —
/// the `attempts` count on the entry (and on the row) carries them.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepOutcome {
    pub step_index: u32,
    pub status: OutcomeStatus,
    pub attempts: u32,
    pub at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutcomeStatus { Success, Failed }

/// List/get projection of one run (FM-29).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfo {
    pub id: String,
    pub name: String,
    pub status: WorkflowStatus,
    pub current_step: u32,
    pub step_count: u32,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_until: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<i64>,
}

/// `GET .../{id}` shape: the info row plus the per-step outcome trail.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfoFull {
    #[serde(flatten)]
    pub info: WorkflowInfo,
    pub step_outcomes: Vec<StepOutcome>,
}
```

- [ ] **Step 4: Run tests**

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --lib protocol::tests::workflow`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git -C /Users/probello/Repos/par-rt-db add server/src/protocol.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 wire types — WorkflowSpec/StepRetry/WorkflowInfo (server)"
```

---

### Task 2: `workflows.rs` storage module

**Files:**
- Create: `server/src/workflows.rs`
- Modify: `server/src/lib.rs` (register `mod workflows;` next to `mod scheduler;`)
- Modify: `server/src/txn.rs` only if `count_steps`/`MAX_STEPS` are not visible — make them `pub(crate)`
- Create: `server/tests/workflows_test.rs` (table-op tests; the file grows in Tasks 3–5)

**Interfaces:**
- Consumes: `WorkflowSpec`, `StepRetry`, `WorkflowInfo(Full)`, `StepOutcome`, `WorkflowStatus` (Task 1); `crate::db::{new_id, now_ms, validate_db_name}`; `crate::ddl::pg_schema`; `crate::txn::{Transaction, count_steps, MAX_STEPS}`.
- Produces (Tasks 3–6): `ensure_table(pool, &str) -> Result<()>`; `validate_spec(&WorkflowSpec) -> Result<(), RtDbError>`; `backoff_ms(&StepRetry, attempts: u32) -> u64`; `insert_on(&mut PgConnection, db, spec: &WorkflowSpec, sleep_until: i64) -> Result<String>`; `insert(pool, db, &WorkflowSpec) -> Result<String>` (computes `sleep_until = now_ms() + steps[0].sleep_before_ms.unwrap_or(0)`); `WorkflowRow { id, name, spec: WorkflowSpec, current_step: u32, attempts: u32, sleep_until: i64, step_outcomes: Vec<StepOutcome>, status: WorkflowStatus }`; `claim_due(pool, db, now, batch: i64) -> Result<Vec<WorkflowRow>>`; `reset_running(pool, db) -> Result<u64>`; `next_due(pool, db) -> Result<Option<i64>>`; `record_step_success(pool, db, id, current_step: u32, outcome: &StepOutcome) -> Result<()>`; `set_pending(pool, db, id, sleep_until) -> Result<()>`; `schedule_retry(pool, db, id, attempts: u32, sleep_until) -> Result<()>`; `finalize_success(pool, db, id, outcome: &StepOutcome) -> Result<()>`; `mark_failed(pool, db, id, outcome: &StepOutcome, error: &str) -> Result<()>`; `cancel(pool, db, id) -> Result<bool>`; `cancel_on(&mut PgConnection, db, id) -> Result<bool>`; `status_of(pool, db, id) -> Result<Option<WorkflowStatus>>`; `list(pool, db, status: Option<&WorkflowStatus>, limit: u32) -> Result<Vec<WorkflowInfo>>`; `get(pool, db, id) -> Result<Option<WorkflowInfoFull>>`; `delete(pool, db, id) -> Result<bool>`; `count_by_status(pool, db) -> Result<Vec<(WorkflowStatus, i64)>>`; `const MAX_WORKFLOW_STEPS: usize = 64`.

- [ ] **Step 1: Write failing unit tests (validation + backoff)**

In `workflows.rs` `#[cfg(test)] mod tests` (pure fns, no DB):

```rust
use super::*;
use crate::protocol::{StepRetry, WorkflowSpec, WorkflowStepSpec};
use crate::txn::{Step, Transaction};

fn spec_with(steps: Vec<WorkflowStepSpec>) -> WorkflowSpec {
    WorkflowSpec { name: "t".into(), steps }
}
fn step_txn() -> WorkflowStepSpec {
    WorkflowStepSpec { txn: Transaction { steps: vec![Step::Insert {
        table: "t".into(), doc: serde_json::Map::new() }] }, retry: None, sleep_before_ms: None }
}

#[test]
fn validate_rejects_empty_and_oversized() {
    assert!(validate_spec(&spec_with(vec![])).is_err());
    assert!(validate_spec(&spec_with((0..65).map(|_| step_txn()).collect())).is_err());
    assert!(validate_spec(&spec_with(vec![step_txn()])).is_ok());
}

#[test]
fn validate_rejects_bad_retry_fields() {
    let mut s = step_txn();
    s.retry = Some(StepRetry { max_attempts: 0, initial_retry_ms: 100, max_retry_ms: 200 });
    assert!(validate_spec(&spec_with(vec![s])).is_err());
    let mut s = step_txn();
    s.retry = Some(StepRetry { max_attempts: 2, initial_retry_ms: 0, max_retry_ms: 200 });
    assert!(validate_spec(&spec_with(vec![s])).is_err());
    let mut s = step_txn();
    s.retry = Some(StepRetry { max_attempts: 2, initial_retry_ms: 500, max_retry_ms: 100 });
    assert!(validate_spec(&spec_with(vec![s])).is_err()); // max < initial
}

#[test]
fn validate_enforces_recursive_step_budget() {
    let big = Transaction { steps: (0..600).map(|_| Step::Insert {
        table: "t".into(), doc: serde_json::Map::new() }).collect() };
    let s1 = WorkflowStepSpec { txn: big.clone(), retry: None, sleep_before_ms: None };
    let s2 = WorkflowStepSpec { txn: big, retry: None, sleep_before_ms: None };
    assert!(validate_spec(&spec_with(vec![s1, s2])).is_err()); // 1200 > MAX_STEPS(1024)
}

#[test]
fn backoff_doubles_and_caps() {
    let r = StepRetry { max_attempts: 9, initial_retry_ms: 1_000, max_retry_ms: 6_000 };
    assert_eq!(backoff_ms(&r, 1), 1_000);
    assert_eq!(backoff_ms(&r, 2), 2_000);
    assert_eq!(backoff_ms(&r, 3), 4_000);
    assert_eq!(backoff_ms(&r, 4), 6_000); // capped
    assert_eq!(backoff_ms(&r, 8), 6_000); // saturating shift, no overflow
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --lib workflows::`
Expected: compile error (module missing).

- [ ] **Step 3: Implement the module**

`server/src/workflows.rs` header + constants + pure fns:

```rust
//! Per-database durable workflow store (FM-29). A run is a row: a
//! `WorkflowSpec` snapshot plus the state machine counters (`current_step`,
//! `attempts`, `sleep_until`, `step_outcomes`). The scheduler claims due rows
//! and the COMMITTER advances them (`handle_workflow_advance`) — this module
//! never executes steps. Mirrors `scheduler.rs` structure.
//! Spec: docs/superpowers/specs/2026-08-15-workflows-design.md

use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;
use crate::protocol::{
    OutcomeStatus, StepOutcome, StepRetry, WorkflowInfo, WorkflowInfoFull, WorkflowSpec,
    WorkflowStatus,
};
use sqlx::{PgConnection, PgPool};

/// Max steps in one workflow spec (spec: bounds).
pub const MAX_WORKFLOW_STEPS: usize = 64;

/// Submit-time validation: 1..=MAX_WORKFLOW_STEPS steps, retry fields in
/// bounds, and the recursive step count summed across every step's txn
/// within `txn::MAX_STEPS` (the FM-28 counter — bounds body size and the
/// nesting bomb).
pub fn validate_spec(spec: &WorkflowSpec) -> Result<(), RtDbError> {
    if spec.steps.is_empty() {
        return Err(RtDbError::bad_request("workflow must have at least one step"));
    }
    if spec.steps.len() > MAX_WORKFLOW_STEPS {
        return Err(RtDbError::bad_request(format!(
            "workflow exceeds {MAX_WORKFLOW_STEPS} steps"
        )));
    }
    for (i, step) in spec.steps.iter().enumerate() {
        if let Some(r) = &step.retry {
            if r.max_attempts == 0 {
                return Err(RtDbError::bad_request(format!("steps[{i}].retry.maxAttempts must be >= 1")));
            }
            if r.initial_retry_ms == 0 || r.max_retry_ms < r.initial_retry_ms {
                return Err(RtDbError::bad_request(format!(
                    "steps[{i}].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs"
                )));
            }
        }
    }
    let total: usize = spec.steps.iter().map(|s| crate::txn::count_steps(&s.txn)).sum();
    if total > crate::txn::MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "workflow recursive step count {total} exceeds MAX_STEPS {}",
            crate::txn::MAX_STEPS
        )));
    }
    Ok(())
}

/// Backoff before attempt `attempts + 1` (i.e. after `attempts` failures):
/// `min(initial * 2^(attempts-1), max)`, saturating.
pub fn backoff_ms(retry: &StepRetry, attempts: u32) -> u64 {
    let shift = (attempts - 1).min(32) as u32;
    retry.initial_retry_ms.saturating_mul(1u64 << shift).min(retry.max_retry_ms)
}
```

DDL + row plumbing (all queries follow `scheduler.rs`: `format!` only for the validated schema identifier, values via `$n`):

```rust
#[derive(Debug, Clone)]
pub struct WorkflowRow {
    pub id: String,
    pub name: String,
    pub status: WorkflowStatus,
    pub spec: WorkflowSpec,
    pub current_step: u32,
    pub attempts: u32,
    pub sleep_until: i64,
    pub step_outcomes: Vec<StepOutcome>,
}

pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".workflows (
            id            text PRIMARY KEY,
            name          text NOT NULL,
            status        text NOT NULL,
            spec          jsonb NOT NULL,
            current_step  int  NOT NULL,
            attempts      int  NOT NULL,
            sleep_until   bigint NOT NULL,
            step_outcomes jsonb NOT NULL DEFAULT '[]',
            last_error    text,
            created_at    bigint NOT NULL,
            updated_at    bigint NOT NULL,
            started_at    bigint,
            finished_at   bigint
        )"
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS \"{schema}_workflows_due_idx\"
         ON \"{schema}\".workflows (status, sleep_until)"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn insert_on(
    conn: &mut PgConnection,
    db: &str,
    spec: &WorkflowSpec,
    sleep_until: i64,
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let id = new_id();
    let now = now_ms();
    let spec_json = serde_json::to_value(spec).map_err(|err| {
        tracing::error!(error = %err, db, "failed to serialize workflow spec");
        RtDbError::internal("failed to start workflow")
    })?;
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".workflows
            (id, name, status, spec, sleep_until, created_at, updated_at)
         VALUES ($1, $2, 'pending', $3, $4, $5, $5)"
    ))
    .bind(&id)
    .bind(&spec.name)
    .bind(&spec_json)
    .bind(sleep_until)
    .bind(now)
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

/// Start a run on the pool (WS/HTTP/admin surfaces). The first step's
/// `sleepBeforeMs` becomes the initial advance gate.
pub async fn insert(pool: &PgPool, db: &str, spec: &WorkflowSpec) -> Result<String, RtDbError> {
    let gate = now_ms() + spec.steps[0].sleep_before_ms.unwrap_or(0) as i64;
    let mut conn = pool.acquire().await?;
    insert_on(&mut conn, db, spec, gate).await
}
```

Claim/reset/next_due (mirror `claim_due`'s `FOR UPDATE SKIP LOCKED` shape; deserialize `spec`/`step_outcomes` with the `tracing::error!` + `RtDbError::internal` pattern from `scheduler::claim_due`):

```rust
pub async fn claim_due(
    pool: &PgPool, db: &str, now: i64, batch: i64,
) -> Result<Vec<WorkflowRow>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, String, serde_json::Value, i32, i32, i64, serde_json::Value)> =
        sqlx::query_as(&format!(
            "UPDATE \"{schema}\".workflows
             SET status = 'running', started_at = COALESCE(started_at, $2), updated_at = $2
             WHERE id IN (
                 SELECT id FROM \"{schema}\".workflows
                 WHERE status = 'pending' AND sleep_until <= $1
                 ORDER BY sleep_until LIMIT $3
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, name, spec, current_step, attempts, sleep_until, step_outcomes"
        ))
        .bind(now)
        .bind(now)
        .bind(batch)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|(id, name, spec, current_step, attempts, sleep_until, outcomes)| {
            Ok(WorkflowRow {
                id, name,
                status: WorkflowStatus::Running,
                spec: serde_json::from_value(spec).map_err(deser_err(db, &id))?,
                current_step: current_step.max(0) as u32,
                attempts: attempts.max(0) as u32,
                sleep_until,
                step_outcomes: serde_json::from_value(outcomes).map_err(deser_err(db, &id))?,
            })
        })
        .collect()
}

fn deser_err(db: &str, id: &str) -> impl Fn(serde_json::Error) -> RtDbError + '_ {
    move |err| {
        tracing::error!(error = %err, db, %id, "failed to deserialize workflow row");
        RtDbError::internal("failed to read workflow")
    }
}

/// Crash recovery: a `running` row was orphaned mid-advance (its `sleep_until`
/// gate is already past) — return it to `pending` so the run re-advances.
pub async fn reset_running(pool: &PgPool, db: &str) -> Result<u64, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows SET status = 'pending', updated_at = $2 WHERE status = 'running'"
    ))
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

pub async fn next_due(pool: &PgPool, db: &str) -> Result<Option<i64>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Option<i64>,)> = sqlx::query_as(&format!(
        "SELECT MIN(sleep_until) FROM \"{schema}\".workflows WHERE status = 'pending'"
    ))
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(m,)| m))
}
```

Bookkeeping (per spec §Claim discipline — `record_step_success` writes counters while the row STAYS `running`; only `set_pending`/`finalize_*`/`mark_failed` release it):

```rust
/// Per-step-boundary write while the row stays `running` (the scheduler only
/// claims `pending`, so the committer's in-turn loop cannot be double-claimed).
pub async fn record_step_success(
    pool: &PgPool, db: &str, id: &str, current_step: u32, outcome: &StepOutcome,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json = serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET current_step = $2, attempts = 0,
             step_outcomes = step_outcomes || $3::jsonb, updated_at = $4
         WHERE id = $1"
    ))
    .bind(id)
    .bind(current_step as i32)
    .bind(&outcome_json)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// Release a `running` row back to claimable with a future gate.
pub async fn set_pending(pool: &PgPool, db: &str, id: &str, sleep_until: i64) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows SET status = 'pending', sleep_until = $2, updated_at = $3 WHERE id = $1"
    ))
    .bind(id).bind(sleep_until).bind(now_ms()).execute(pool).await?;
    Ok(())
}

/// A failed attempt that has retries left: bump attempts, schedule the
/// backoff gate, release to `pending`. Does NOT append an outcome (retries
/// are carried by the attempts counters).
pub async fn schedule_retry(
    pool: &PgPool, db: &str, id: &str, attempts: u32, sleep_until: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'pending', attempts = $2, sleep_until = $3, updated_at = $4
         WHERE id = $1"
    ))
    .bind(id).bind(attempts as i32).bind(sleep_until).bind(now_ms())
    .execute(pool).await?;
    Ok(())
}

pub async fn finalize_success(
    pool: &PgPool, db: &str, id: &str, outcome: &StepOutcome,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json = serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'success', attempts = 0, last_error = NULL,
             step_outcomes = step_outcomes || $2::jsonb, finished_at = $3, updated_at = $3
         WHERE id = $1"
    ))
    .bind(id).bind(&outcome_json).bind(now_ms()).execute(pool).await?;
    Ok(())
}

pub async fn mark_failed(
    pool: &PgPool, db: &str, id: &str, outcome: &StepOutcome, error: &str,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json = serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'failed', last_error = $2,
             step_outcomes = step_outcomes || $3::jsonb, finished_at = $4, updated_at = $4
         WHERE id = $1"
    ))
    .bind(id).bind(error).bind(&outcome_json).bind(now_ms())
    .execute(pool).await?;
    Ok(())
}

/// Cancel: flip a non-terminal row to `cancelled`. Returns false for a
/// missing or already-terminal run. An in-flight `running` arm notices at
/// its next step boundary (`status_of`).
pub async fn cancel(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows SET status = 'cancelled', finished_at = $2, updated_at = $2
         WHERE id = $1 AND status IN ('pending', 'running')"
    ))
    .bind(id).bind(now_ms()).execute(pool).await?;
    Ok(res.rows_affected() > 0)
}

/// Connection-bound cancel for `Step::CancelWorkflow` (rides the open tx).
pub(crate) async fn cancel_on(conn: &mut PgConnection, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows SET status = 'cancelled', finished_at = $2, updated_at = $2
         WHERE id = $1 AND status IN ('pending', 'running')"
    ))
    .bind(id).bind(now_ms()).execute(&mut *conn).await?;
    Ok(res.rows_affected() > 0)
}

pub async fn status_of(pool: &PgPool, db: &str, id: &str) -> Result<Option<WorkflowStatus>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT status FROM \"{schema}\".workflows WHERE id = $1"
    ))
    .bind(id).fetch_optional(pool).await?;
    row.map(|(s,)| s.parse()).transpose().map_err(|e| RtDbError::internal(format!("invalid workflows.status: {e}")))
}
```

`list`/`get`/`delete`/`count_by_status` — SELECT the columns into `WorkflowInfo` (camelCase via the types; parse status with FromStr like `scheduler::list` does for `ScheduleStatus`; `step_count = jsonb_array_length(spec->'steps')` or deserialize the spec — prefer `jsonb_array_length`):

```rust
pub async fn list(
    pool: &PgPool, db: &str, status: Option<&WorkflowStatus>, limit: u32,
) -> Result<Vec<WorkflowInfo>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, String, String, i32, i32, i32, Option<i64>, Option<String>, i64, i64, Option<i64>, Option<i64>)> =
        sqlx::query_as(&format!(
            "SELECT id, name, status, current_step, jsonb_array_length(spec->'steps'), attempts,
                    sleep_until, last_error, created_at, updated_at, started_at, finished_at
             FROM \"{schema}\".workflows
             WHERE ($1::text IS NULL OR status = $1)
             ORDER BY created_at DESC LIMIT $2"
        ))
        .bind(status.map(WorkflowStatus::as_wire_str))
        .bind(limit as i64)
        .fetch_all(pool)
        .await?;
    rows.into_iter()
        .map(|(id, name, status, current_step, step_count, attempts, sleep_until, last_error,
              created_at, updated_at, started_at, finished_at)| {
            Ok(WorkflowInfo {
                id, name,
                status: status.parse().map_err(|e| RtDbError::internal(format!("invalid workflows.status: {e}")))?,
                current_step: current_step.max(0) as u32,
                step_count: step_count.max(0) as u32,
                attempts: attempts.max(0) as u32,
                sleep_until, last_error, created_at, updated_at, started_at, finished_at,
            })
        })
        .collect()
}

/// Full row for `GET .../{id}` — info plus the outcome trail.
pub async fn get(pool: &PgPool, db: &str, id: &str) -> Result<Option<WorkflowInfoFull>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT to_jsonb(w) - 'spec' AS info FROM \"{schema}\".workflows w WHERE w.id = $1"
    ))
    .bind(id).fetch_optional(pool).await?;
    // to_jsonb yields snake_case column names; deserialize the pieces we need
    // and rebuild WorkflowInfo + stepOutcomes from the stored wire shapes:
    // step_outcomes is already the camelCase wire JSON (serde wrote it), the
    // scalar columns are remapped below. (Implementation may instead select
    // explicit columns like `list` + step_outcomes and reuse the same mapper.)
    todo_in_implementer_preferred_style!()
}
```

**Implementer note (binding, not a placeholder):** implement `get` by selecting the same 12 columns as `list` PLUS `step_outcomes jsonb`, reusing `list`'s row-mapping closure for the `WorkflowInfo` half and `serde_json::from_value::<Vec<StepOutcome>>` for the trail; `WHERE id = $1`, `fetch_optional`. `delete` and `count_by_status`:

```rust
pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".workflows WHERE id = $1"))
        .bind(id).execute(pool).await?;
    Ok(res.rows_affected() > 0)
}

/// Per-status counts for the admin metrics JSON (spec: per-db breakdown,
/// admin JSON only — never the Prometheus scrape).
pub async fn count_by_status(pool: &PgPool, db: &str) -> Result<Vec<(WorkflowStatus, i64)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM \"{schema}\".workflows GROUP BY status"
    ))
    .fetch_all(pool).await?;
    rows.into_iter()
        .map(|(s, n)| Ok((s.parse().map_err(|e| RtDbError::internal(format!("invalid workflows.status: {e}")))?, n)))
        .collect()
}
```

Register `pub mod workflows;` in `lib.rs` beside `pub mod scheduler;`. If `txn::count_steps`/`MAX_STEPS` are private, make them `pub(crate)`.

- [ ] **Step 4: Add table-op integration tests**

`server/tests/workflows_test.rs` (harness mirrors `schedule_step_test.rs`; `common::{fresh_db, spawn_app, test_state}` — check `tests/common/mod.rs` for exact names):

```rust
//! Integration tests for durable workflows (FM-29).

mod common;

use common::{fresh_db, test_state};
use rtdb_server::protocol::{OutcomeStatus, WorkflowSpec, WorkflowStatus};
use rtdb_server::workflows;

fn one_step_spec(name: &str) -> WorkflowSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "steps": [ { "txn": { "steps": [ { "op": "insert", "table": "projects",
            "doc": { "name": "W", "description": null, "status": "active",
                     "tags": [], "updatedAt": 1.0 } } ] } } ]
    })).unwrap()
}

#[tokio::test]
async fn insert_claim_reset_roundtrip() {
    let db = fresh_db().await;
    let state = test_state(&db).await;
    workflows::ensure_table(&state.pool, db).await.unwrap();
    let id = workflows::insert(&state.pool, db, &one_step_spec("rt")).await.unwrap();
    let due = workflows::next_due(&state.pool, db).await.unwrap();
    assert!(due.is_some());
    let claimed = workflows::claim_due(&state.pool, db, rtdb_server::db::now_ms(), 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, id);
    assert_eq!(claimed[0].status, WorkflowStatus::Running);
    // Nothing further claims while running:
    assert!(workflows::claim_due(&state.pool, db, rtdb_server::db::now_ms() + 10_000, 10).await.unwrap().is_empty());
    // Crash recovery path:
    assert_eq!(workflows::reset_running(&state.pool, db).await.unwrap(), 1);
    // list/get/cancel/delete shape:
    let listed = workflows::list(&state.pool, db, None, 10).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].status, WorkflowStatus::Pending);
    assert!(workflows::cancel(&state.pool, db, &id).await.unwrap());
    let full = workflows::get(&state.pool, db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Cancelled);
    assert!(full.step_outcomes.is_empty());
    assert!(workflows::delete(&state.pool, db, &id).await.unwrap());
}
```

(If `test_state` is named differently in `common/mod.rs` — e.g. returns `(pool, …)` — follow the exact helper `tests/schedule_step_test.rs` uses; do not invent harness surface.)

- [ ] **Step 5: Run tests**

Run: `make -C /Users/probello/Repos/par-rt-db dev-db-up && cd /Users/probello/Repos/par-rt-db/server && cargo test --lib workflows:: && cargo test --test workflows_test`
Expected: unit tests 4 passed; integration test passed.

- [ ] **Step 6: Commit**

```bash
git -C /Users/probello/Repos/par-rt-db add server/src/workflows.rs server/src/lib.rs server/src/txn.rs server/tests/workflows_test.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 side table, claim/reset, spec validation + backoff"
```

---

### Task 3: Committer arm + scheduler loop (advancement engine)

**Files:**
- Modify: `server/src/committer.rs` (new `CommitterRequest::RunWorkflowAdvance` arm + `handle_workflow_advance`; enum arm dispatch near the `RunReaper` arm ~line 863)
- Modify: `server/src/scheduler.rs` (`run_scheduler`: startup + wake + claim extension)
- Test: `server/tests/workflows_test.rs` (engine tests)

**Interfaces:**
- Consumes: Task 2's `workflows::*`; `execute_txn`, `publish_taps`, `CommitterCtx`, `ctx.metrics`, `ctx.quotas` (all in committer.rs already).
- Produces: `CommitterRequest::RunWorkflowAdvance { row: Box<workflows::WorkflowRow> }`; `workflows` polled by `run_scheduler`. Metrics fns used here are added in Task 6 — until then, call sites are omitted (Task 6 wires `record_workflow_step`); keep the arm free of metrics in this task to stay compiling.

- [ ] **Step 1: Write failing engine tests**

Append to `tests/workflows_test.rs` (uses a real `Committers` like `schedule_step_test.rs::chained_schedule_fires_and_enqueues_follow_up` — copy its setup: `test_state`, `Committers::channel_for`, and the poll-for-completion helper pattern):

```rust
/// Poll until `pred(info)` holds or ~10s elapse (scheduler wake is ≤2s/step).
async fn await_status(db: &str, state: &rtdb_server::AppState, id: &str,
                      pred: impl Fn(&rtdb_server::protocol::WorkflowInfo) -> bool) -> rtdb_server::protocol::WorkflowInfo {
    for _ in 0..200 {
        if let Some(info) = workflows::list(&state.pool, db, None, 1).await.unwrap().into_iter()
            .find(|i| i.id == id) {
            if pred(&info) { return info; }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("workflow never reached expected status");
}
```

Test 1 — happy path + taps:

```rust
#[tokio::test]
async fn three_step_workflow_advances_to_success() {
    let db = fresh_db().await;
    let state = test_state(&db).await;            // real Committers, mirrors schedule_step_test
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "chain",
        "steps": [0, 1, 2].map(|n| { "steps": [ { "op": "insert", "table": "projects",
            "doc": { "name": format!("S{n}"), "description": null, "status": "active",
                     "tags": [], "updatedAt": 1.0 } } ] })
    })).unwrap();
    workflows::ensure_table(&state.pool, db).await.unwrap();
    let id = workflows::insert(&state.pool, db, &spec).await.unwrap();
    let info = await_status(db, &state, &id, |i| i.status == WorkflowStatus::Success).await;
    assert_eq!(info.step_count, 3);
    let full = workflows::get(&state.pool, db, &id).await.unwrap().unwrap();
    assert_eq!(full.step_outcomes.len(), 3);
    assert!(full.step_outcomes.iter().all(|o| o.status == OutcomeStatus::Success));
    // All three docs durable:
    let count = sqlx::query_scalar::<_, i64>(
        &format!("SELECT COUNT(*) FROM \"db_{db}\".projects")).fetch_one(&state.pool).await.unwrap();
    assert_eq!(count, 3);
}
```

Test 2 — retry then success. A step that fails on first attempt: insert with a duplicate id… simplest deterministic failure is `ExpectVersion` on a nonexistent row (`version: 1`) — always fails. For fail-then-pass use `ExpectAbsent` on an index another step populates? Deterministic pattern: step 1 = `ExpectVersion{table:"projects", id:"missing", version:1}` (fails while doc absent), step 0 = schedule a `Schedule` step that inserts doc `missing` after 300ms. Simpler and sufficient: assert retry mechanics with an always-failing step and `maxAttempts: 2, initialRetryMs: 50` → exhausted `failed` with attempts recorded; then a separate test where failure clears (step retries succeed because an earlier scheduled insert landed):

```rust
#[tokio::test]
async fn exhausted_retries_mark_failed_with_trail() {
    let db = fresh_db().await;
    let state = test_state(&db).await;
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "doomed",
        "steps": [
            { "txn": { "steps": [ { "op": "expectVersion", "table": "projects", "id": "nope", "version": 7 } ] },
              "retry": { "maxAttempts": 2, "initialRetryMs": 50, "maxRetryMs": 100 } },
            { "txn": { "steps": [ { "op": "insert", "table": "projects",
                "doc": { "name": "never", "description": null, "status": "active", "tags": [], "updatedAt": 1.0 } } ] } }
        ]
    })).unwrap();
    workflows::ensure_table(&state.pool, db).await.unwrap();
    let id = workflows::insert(&state.pool, db, &spec).await.unwrap();
    let info = await_status(db, &state, &id, |i| i.status == WorkflowStatus::Failed).await;
    assert_eq!(info.current_step, 0);
    let full = workflows::get(&state.pool, db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.attempts, 2);
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(full.step_outcomes[0].status, OutcomeStatus::Failed);
    assert!(full.info.last_error.is_some());
    // Step 1 never ran:
    let count = sqlx::query_scalar::<_, i64>(
        &format!("SELECT COUNT(*) FROM \"db_{db}\".projects")).fetch_one(&state.pool).await.unwrap();
    assert_eq!(count, 0);
}
```

Test 3 — sleep gate: step 1 `sleepBeforeMs: 1500`; assert step 0's doc exists while step 1's does not at t+600ms, and success by ~10s (use distinct doc `name`s and the `await_status` poll; a mid-poll `list` shows `sleepUntil` > now).

Test 4 — crash resume: insert a 1-step spec, manually `claim_due` (orphaning the row as `running`), then call `workflows::reset_running` + let the loop advance (or assert `reset_running` returns it to pending and a subsequent claim advances) — assert final `success` (at-least-once).

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --test workflows_test`
Expected: FAIL — run stays `pending` forever (advance never happens; test times out) or compile error for the missing arm.

- [ ] **Step 3: Implement the committer arm**

In `committer.rs`: add the request variant (doc comment mirroring `RunReaper`'s):

```rust
/// A claimed workflow run is due for advancement (FM-29). Fire-and-forget
/// like `RunReaper`: the scheduler claims the row; the committer executes
/// the current step's txn and writes the bookkeeping inside its serialized
/// turn (single-writer invariant — the scheduler never executes a step).
RunWorkflowAdvance {
    row: Box<crate::workflows::WorkflowRow>,
},
```

Dispatch arm (mirror the `RunReaper` arm):

```rust
CommitterRequest::RunWorkflowAdvance { row } => {
    let span = tracing::info_span!("committer.workflow", db = %ctx.db, id = %row.id);
    let outcome = handle_workflow_advance(&ctx, *row).instrument(span).await;
    if let Err(err) = outcome {
        tracing::error!(db = %ctx.db, error = %err, "workflow advance failed");
    }
}
```

Handler (place after `handle_scheduled`):

```rust
/// Advance a claimed workflow run (FM-29). Executes the current step's txn
/// as the system (bypass) principal — same fire path as `handle_scheduled` —
/// publishes through the tap sites with `source = "workflow"`, and loops
/// while the next gate is already due. Claim discipline: the row is
/// `running` for the whole loop (the scheduler only claims `pending`), so a
/// no-sleep chain completes in one turn, bounded by `MAX_WORKFLOW_STEPS`.
/// At-least-once per step: a crash after `execute_txn` commits but before
/// the bookkeeping write re-executes that step on resume.
async fn handle_workflow_advance(
    ctx: &CommitterCtx,
    mut row: crate::workflows::WorkflowRow,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let outcome = failed_outcome(&row);
            let _ = crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &outcome, "schema load failed").await;
            return Err(err);
        }
    };
    // ENH-011/ARC-004 storage cap — checked once at entry. Unlike
    // handle_scheduled's terminal mark_error, a quota rejection counts as a
    // retryable step failure: raising the cap mid-retry lets the run recover.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    let mut quota_err: Option<RtDbError> = None;
    if storage_cap > 0 {
        if let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await {
            quota_err = Some(e);
        }
    }
    loop {
        // Cancel/terminal check at each step boundary (spec §Semantics).
        match crate::workflows::status_of(&ctx.pool, &ctx.db, &row.id).await {
            Ok(Some(crate::protocol::WorkflowStatus::Running)) => {}
            Ok(Some(_)) => return Ok(()), // cancelled out from under us
            Ok(None) => return Ok(()),    // deleted
            Err(err) => return Err(err),
        }
        let step = &row.spec.steps[row.current_step as usize];
        let retry = step.retry.unwrap_or_default();
        let exec = match quota_err.take() {
            Some(e) => Err(e),
            None => {
                execute_txn(&ctx.pool, &ctx.db, &schema, &step.txn, &PrincipalCtx::bypass()).await
            }
        };
        match exec {
            Ok(outcome) => {
                publish_taps(ctx, &schema, &outcome.write_set, None, "workflow", true, true).await;
                let now = now_ms();
                let finished = row.current_step as usize + 1 >= row.spec.steps.len();
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Success,
                    attempts: row.attempts + 1,
                    at: now,
                    error: None,
                };
                if finished {
                    crate::workflows::finalize_success(&ctx.pool, &ctx.db, &row.id, &record).await?;
                    return Ok(());
                }
                // Write the boundary while staying `running`, then compute the
                // next gate: due now → keep looping; future → release pending.
                crate::workflows::record_step_success(
                    &ctx.pool, &ctx.db, &row.id, row.current_step + 1, &record,
                ).await?;
                row.current_step += 1;
                row.attempts = 0;
                let next = &row.spec.steps[row.current_step as usize];
                let gate = now + next.sleep_before_ms.unwrap_or(0) as i64;
                if gate > now_ms() {
                    crate::workflows::set_pending(&ctx.pool, &ctx.db, &row.id, gate).await?;
                    return Ok(());
                }
            }
            Err(err) => {
                let now = now_ms();
                row.attempts += 1;
                if row.attempts < retry.max_attempts {
                    let backoff = crate::workflows::backoff_ms(&retry, row.attempts) as i64;
                    crate::workflows::schedule_retry(
                        &ctx.pool, &ctx.db, &row.id, row.attempts, now + backoff,
                    ).await?;
                    return Ok(());
                }
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Failed,
                    attempts: row.attempts,
                    at: now,
                    error: Some(err.message.clone()),
                };
                crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &record, &err.message).await?;
                return Ok(());
            }
        }
    }
}

fn failed_outcome(row: &crate::workflows::WorkflowRow) -> crate::protocol::StepOutcome {
    crate::protocol::StepOutcome {
        step_index: row.current_step,
        status: crate::protocol::OutcomeStatus::Failed,
        attempts: row.attempts.max(1),
        at: now_ms(),
        error: Some("schema load failed".to_string()),
    }
}
```

- [ ] **Step 4: Extend the scheduler loop**

In `scheduler.rs` `run_scheduler`: after the existing `ensure_table`/`reset_running` startup calls add:

```rust
if let Err(err) = crate::workflows::ensure_table(&pool, &db).await {
    tracing::error!(db = %db, error = %err, "scheduler: workflows ensure_table failed");
}
if let Err(err) = crate::workflows::reset_running(&pool, &db).await {
    tracing::error!(db = %db, error = %err, "scheduler: workflows reset_running failed");
}
```

Wake computation — replace the single `next_due` read with both sources (keep ARC-102 skip semantics per table):

```rust
let sched_next = next_due(&pool, &db).await;
let wf_next = crate::workflows::next_due(&pool, &db).await;
let nearest = [sched_next.as_ref().ok().copied().flatten(),
               wf_next.as_ref().ok().copied().flatten()]
    .into_iter().flatten().min();
let now = now_ms();
let (sleep, claim_sched, claim_wf) = match nearest {
    Some(due) if due <= now => (Duration::ZERO, true, true),
    Some(due) => (Duration::from_millis((due - now) as u64).min(MAX_SLEEP), false, false),
    None => (MAX_SLEEP, false, false),
};
```

(Equivalent restructure is fine — the requirement is: wake on the min of the two tables; claim each table only when IT has something due, i.e. check `sched_next`/`wf_next` individually against `now` rather than only the min; error handling per table mirrors the existing `database_exists` exit check.) After the existing `RunScheduled` send loop add:

```rust
if claim_wf {
    let now = now_ms();
    let claimed_wf = match crate::workflows::claim_due(&pool, &db, now, CLAIM_BATCH).await {
        Ok(rows) => rows,
        Err(err) => {
            if matches!(crate::db::database_exists(&pool, &db).await, Ok(false)) {
                tracing::info!(db = %db, "scheduler: database removed, exiting");
                return;
            }
            tracing::error!(db = %db, error = %err, "scheduler: workflows claim_due failed");
            Vec::new()
        }
    };
    for row in claimed_wf {
        let req = CommitterRequest::RunWorkflowAdvance { row: Box::new(row) };
        if committer_tx.send(req).await.is_err() {
            tracing::warn!(db = %db, "scheduler: committer channel closed, exiting");
            return;
        }
    }
}
```

- [ ] **Step 5: Run engine tests**

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --test workflows_test`
Expected: all pass (happy path, exhaustion, sleep gate, crash resume).

- [ ] **Step 6: fmt/clippy then commit**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo fmt && cargo clippy --all-targets -- -D warnings
git -C /Users/probello/Repos/par-rt-db add server/src/committer.rs server/src/scheduler.rs server/tests/workflows_test.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 committer RunWorkflowAdvance arm + scheduler dual-table poll"
```

---

### Task 4: `Step::StartWorkflow` / `Step::CancelWorkflow` txn steps

**Files:**
- Modify: `server/src/txn.rs` (Step enum variants, `table()`, `worst_case_affected`, `count_steps`, match arms, `step_start_workflow`, `step_cancel_workflow`, `authorize_txn_tables` recursion)
- Test: `server/tests/workflows_test.rs`

**Interfaces:**
- Consumes: `workflows::{validate_spec, insert_on, cancel_on}` (Task 2).
- Produces: wire steps `{"op":"startWorkflow","spec":{…}}` → result `{"workflowId":"…"}`; `{"op":"cancelWorkflow","id":"…"}` → result `{"cancelled":bool}`. `pub(crate) fn authorize_spec_tables(ctx: &PrincipalCtx, spec: &WorkflowSpec) -> Result<(), RtDbError>` (used by Tasks 5–6 surfaces).

- [ ] **Step 1: Write failing tests**

Append to `tests/workflows_test.rs` (drive `execute_txn` with `PrincipalCtx::bypass()` exactly like `schedule_step_test.rs`):

```rust
use rtdb_server::txn::{Step, Transaction, execute_txn};

#[tokio::test]
async fn start_workflow_step_is_atomic_with_writes() {
    let db = fresh_db().await;
    let state = test_state(&db).await;
    let schema = /* kanban_schema() as in schedule_step_test */;
    let txn = Transaction { steps: vec![
        Step::Insert { table: "projects".into(), doc: valid_project_doc() },
        Step::StartWorkflow { spec: Box::new(one_step_spec("from-step")) },
    ]};
    let out = execute_txn(&state.pool, db, &schema, &txn, &PrincipalCtx::bypass()).await.unwrap();
    assert!(out.results[1]["workflowId"].as_str().is_some());
    let listed = workflows::list(&state.pool, db, None, 10).await.unwrap();
    assert_eq!(listed.len(), 1);

    // Rollback: a failing later step removes the run row too.
    let txn = Transaction { steps: vec![
        Step::StartWorkflow { spec: Box::new(one_step_spec("rolled-back")) },
        Step::ExpectVersion { table: "projects".into(), id: "missing".into(), version: 9 },
    ]};
    assert!(execute_txn(&state.pool, db, &schema, &txn, &PrincipalCtx::bypass()).await.is_err());
    assert_eq!(workflows::list(&state.pool, db, None, 10).await.unwrap().len(), 1); // unchanged
}

#[tokio::test]
async fn cancel_workflow_step_result_shape() {
    let db = fresh_db().await;
    let state = test_state(&db).await;
    let schema = /* kanban_schema() */;
    workflows::ensure_table(&state.pool, db).await.unwrap();
    let id = workflows::insert(&state.pool, db, &one_step_spec("cancelme")).await.unwrap();
    let txn = Transaction { steps: vec![Step::CancelWorkflow { id: id.clone() }] };
    let out = execute_txn(&state.pool, db, &schema, &txn, &PrincipalCtx::bypass()).await.unwrap();
    assert_eq!(out.results[0]["cancelled"], serde_json::json!(true));
    let txn = Transaction { steps: vec![Step::CancelWorkflow { id }] };
    let out = execute_txn(&state.pool, db, &schema, &txn, &PrincipalCtx::bypass()).await.unwrap();
    assert_eq!(out.results[0]["cancelled"], serde_json::json!(false)); // already terminal
}

#[tokio::test]
async fn spec_bounds_and_allowlist_rejected() {
    let db = fresh_db().await;
    let state = test_state(&db).await;
    let schema = /* kanban_schema() */;
    let mut spec = one_step_spec("x");
    spec.steps.clear(); // empty
    let err = execute_txn(&state.pool, db, &schema,
        &Transaction { steps: vec![Step::StartWorkflow { spec: Box::new(spec) }] },
        &PrincipalCtx::bypass()).await;
    assert_eq!(err.unwrap_err().code, ErrorCode::BadRequest);

    // Scoped token: allowlist over the spec's step txns (recursive).
    let scoped = /* a PrincipalCtx with tables = Some(["projects"]) as in
                    schedule_step_test's scoped-token test */;
    let mut spec = one_step_spec("scoped");
    spec.steps[0].txn.steps = vec![Step::Insert { table: "workItems".into(), doc: valid_work_item_doc("x") }];
    let err = execute_txn(&state.pool, db, &schema,
        &Transaction { steps: vec![Step::StartWorkflow { spec: Box::new(spec) }] }, &scoped).await;
    assert_eq!(err.unwrap_err().code, ErrorCode::Forbidden);
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test workflows_test` → compile error (no such variants).

- [ ] **Step 3: Implement**

In `txn.rs` Step enum (after `CancelSchedule`):

```rust
/// Start a durable workflow run (FM-29). The `workflows` row is inserted on
/// the OPEN sqlx transaction — "write doc + start drip" is atomic; a
/// rolled-back txn leaves no orphan run. Step result `{"workflowId": "<id>"}`.
/// The spec is validated and table-scope-checked recursively at submit time;
/// steps fire later as the system (bypass) principal in the committer's
/// `RunWorkflowAdvance` turn.
StartWorkflow {
    spec: Box<crate::protocol::WorkflowSpec>,
},
/// Cancel a workflow run by id, on the open sqlx transaction. Step result
/// `{"cancelled": <bool>}` — `false` when missing or already terminal. A run
/// whose advance is in flight stops at its next step boundary.
CancelWorkflow {
    id: String,
},
```

`Step::table()`: add both to the `None` arm. `worst_case_affected`: add both to the `0` arm (find the match at ~line 1331). `count_steps` (~line 48):

```rust
Step::StartWorkflow { spec } => 1 + spec.steps.iter().map(|s| count_steps(&s.txn)).sum::<usize>(),
Step::CancelWorkflow { .. } => 1,
```

Match arms in `execute_txn` (~line 1457):

```rust
Step::StartWorkflow { spec } => step_start_workflow(&mut sctx, spec).await?,
Step::CancelWorkflow { id } => step_cancel_workflow(&mut sctx, id).await?,
```

Handlers + allowlist recursion (after `step_cancel_schedule`):

```rust
/// Recursive table-scope check over every step txn in a workflow spec,
/// INCLUDING steps nested inside `Schedule` payloads (via
/// `authorize_txn_tables`). Runs at SUBMIT time on every start surface so a
/// scoped machine token cannot smuggle a future write into a forbidden table
/// via a workflow step that fires later as bypass.
pub(crate) fn authorize_spec_tables(
    ctx: &PrincipalCtx,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    for step in &spec.steps {
        authorize_txn_tables(ctx, &step.txn)?;
    }
    Ok(())
}

async fn step_start_workflow(
    sctx: &mut StepCtx<'_>,
    spec: &crate::protocol::WorkflowSpec,
) -> Result<(), RtDbError> {
    crate::workflows::validate_spec(spec)?;
    authorize_spec_tables(sctx.ctx, spec)?;
    let gate = now_ms() + spec.steps[0].sleep_before_ms.unwrap_or(0) as i64;
    let id = crate::workflows::insert_on(sctx.tx, sctx.db, spec, gate).await?;
    sctx.results.push(serde_json::json!({ "workflowId": id }));
    Ok(())
}

async fn step_cancel_workflow(sctx: &mut StepCtx<'_>, id: &str) -> Result<(), RtDbError> {
    let cancelled = crate::workflows::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results.push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}
```

And `authorize_txn_tables` gains workflow recursion (so a `Schedule` step can smuggle no `StartWorkflow` past it either):

```rust
if let Step::StartWorkflow { spec, .. } = step {
    authorize_spec_tables(ctx, spec)?;
}
```

- [ ] **Step 4: Run tests** — `cargo test --test workflows_test` → all pass; also `cargo test --test txn_test` (regression).

- [ ] **Step 5: Commit**

```bash
git -C /Users/probello/Repos/par-rt-db add server/src/txn.rs server/tests/workflows_test.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 startWorkflow/cancelWorkflow txn steps with submit-time scoping"
```

---

### Task 5: WS frames + HTTP routes

**Files:**
- Modify: `server/src/protocol.rs` (ClientMessage/ServerMessage variants + serde tests)
- Modify: `server/src/ws.rs` (three handlers, mirroring `handle_schedule`/`handle_list_schedules`)
- Modify: `server/src/http_api.rs` (three routes, mirroring `schedule_handler`)
- Test: `server/src/protocol.rs` serde tests + `server/tests/workflows_test.rs` HTTP tests

**Interfaces:**
- Consumes: `workflows::{insert, cancel, list, validate_spec}`, `txn::authorize_spec_tables` (Tasks 2/4).
- Produces wire (byte shapes — casing load-bearing):

```text
WS in:  {"type":"startWorkflow","workflowId":"<corr>","spec":{…}}
        {"type":"cancelWorkflow","workflowId":"<corr>","id":"<run id>"}
        {"type":"listWorkflows","workflowId":"<corr>","status":"failed"?}
WS out: {"type":"startWorkflowOk","workflowId":"<corr>","info":{…WorkflowInfo}}
        {"type":"startWorkflowErr","workflowId":"<corr>","error":{…}}
        {"type":"workflowAck","workflowId":"<corr>","ok":true,"error"?}
        {"type":"listWorkflowsOk","workflowId":"<corr>","workflows":[…]}
HTTP:   POST /api/workflows            body {db, spec}          → {id}
        POST /api/workflows/list       body {db, status?}        → {workflows:[…]}
        POST /api/workflows/{id}/cancel body {db}                → {cancelled}
```

- [ ] **Step 1: Failing serde tests** (protocol.rs tests):

```rust
#[test]
fn workflow_frame_wire_shapes() {
    let m = serde_json::to_value(ClientMessage::StartWorkflow {
        workflow_id: "c1".into(), spec: sample_workflow_spec(),
    }).unwrap();
    assert_eq!(m["type"], json!("startWorkflow"));
    assert_eq!(m["workflowId"], json!("c1"));
    let m = serde_json::to_value(ServerMessage::WorkflowAck {
        workflow_id: "c1".into(), ok: true, error: None,
    }).unwrap();
    assert_eq!(m["type"], json!("workflowAck"));
    assert!(m.get("error").is_none());
}
```

(`sample_workflow_spec()` — small helper next to the existing `sample_txn()`.)

- [ ] **Step 2: Verify failure** — `cargo test --lib protocol::tests::workflow_frame` → compile error.

- [ ] **Step 3: Implement protocol variants**

ClientMessage (after `ListSchedules`):

```rust
StartWorkflow {
    workflow_id: String,
    spec: WorkflowSpec,
},
CancelWorkflow {
    workflow_id: String,
    id: String,
},
ListWorkflows {
    workflow_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<WorkflowStatus>,
},
```

ServerMessage (after `ListSchedulesOk`):

```rust
StartWorkflowOk {
    workflow_id: String,
    info: WorkflowInfo,
},
StartWorkflowErr {
    workflow_id: String,
    error: RtDbError,
},
/// Reply to cancelWorkflow. `error` omitted on the wire when `ok`.
WorkflowAck {
    workflow_id: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RtDbError>,
},
ListWorkflowsOk {
    workflow_id: String,
    workflows: Vec<WorkflowInfo>,
},
```

- [ ] **Step 4: WS handlers**

`ws.rs` — dispatch arms mirroring the `ClientMessage::Schedule` arm (~line 388), then handlers cloned from `handle_schedule`/`handle_list_schedules` shape:

```rust
async fn handle_start_workflow(
    fctx: &FrameCtx<'_>, workflow_id: String, spec: WorkflowSpec,
) -> bool {
    let reply = match authorize(&fctx.state.pool, fctx.principal, fctx.db).await {
        Ok(()) if fctx.principal.is_read_only() => ServerMessage::StartWorkflowErr {
            workflow_id, error: RtDbError::forbidden("read-only token cannot mutate") },
        Ok(()) => {
            let prepared = workflows::validate_spec(&spec)
                .and_then(|()| authorize_spec_tables(&fctx.principal.row_ctx(), &spec));
            match prepared {
                Ok(()) => match workflows::insert(&fctx.state.pool, fctx.db, &spec).await {
                    Ok(id) => {
                        let listed = workflows::list(&fctx.state.pool, fctx.db, None, 1).await
                            .ok().and_then(|mut v| v.pop());
                        match listed {
                            Some(info) if info.id == id => ServerMessage::StartWorkflowOk { workflow_id, info },
                            _ => ServerMessage::StartWorkflowErr { workflow_id,
                                error: RtDbError::internal("workflow started but unreadable") },
                        }
                    }
                    Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
                },
                Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
            }
        }
        Err(error) => ServerMessage::StartWorkflowErr { workflow_id, error },
    };
    let _ = fctx.out_tx.send(reply);
    false
}
```

(Simpler than re-reading: have `workflows::insert` return `(id, WorkflowInfo)`? No — keep `insert` as Task 2 defined; the list-re-read above is fine, or construct the info inline. Either way the test asserts `info.id`.) `handle_cancel_workflow` mirrors `handle_schedule`'s authorize + `workflows::cancel` → `WorkflowAck { ok }`; `handle_list_workflows` mirrors `handle_list_schedules` with `workflows::list(pool, db, status.as_ref(), 100)`.

- [ ] **Step 5: HTTP handlers**

`http_api.rs` — routes in `http_api_routes()` after the schedule routes:

```rust
.route("/api/workflows", post(start_workflow_handler))
.route("/api/workflows/list", post(list_workflows_handler))
.route("/api/workflows/{id}/cancel", post(cancel_workflow_handler))
```

Handlers mirror `schedule_handler` (line 353) exactly — `authed(...)`, read-only check, `check_http_rate_limits`, then:

```rust
#[derive(Deserialize)] #[serde(rename_all = "camelCase")]
struct StartWorkflowRequest { db: String, spec: WorkflowSpec }
#[derive(Serialize)]
struct StartWorkflowResponse { id: String }

async fn start_workflow_handler(
    State(state): State<Arc<AppState>>, headers: HeaderMap,
    ApiJson(body): ApiJson<StartWorkflowRequest>,
) -> Result<Json<StartWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
    workflows::validate_spec(&body.spec)?;
    crate::txn::authorize_spec_tables(&principal.row_ctx(), &body.spec)?;
    let id = workflows::insert(&state.pool, &body.db, &body.spec).await?;
    Ok(Json(StartWorkflowResponse { id }))
}
```

`list` body `{db, status?}` → `{workflows: Vec<WorkflowInfo>}` (limit 100); `cancel` path `{id}` + body `{db}` → `{cancelled: bool}` via `workflows::cancel`.

- [ ] **Step 6: Tests** — protocol serde tests pass; add HTTP e2e to `workflows_test.rs` using the harness's `admin_post`/HTTP helper (copy the pattern `schedule_step_test.rs` uses to hit `/api/…` with a machine token; assert `{id}` shape, list round-trip, cancel true/false).

Run: `cd /Users/probello/Repos/par-rt-db/server && cargo test --lib protocol:: && cargo test --test workflows_test && cargo test --test ws_test` (ws regression).

- [ ] **Step 7: Commit**

```bash
git -C /Users/probello/Repos/par-rt-db add server/src/protocol.rs server/src/ws.rs server/src/http_api.rs server/tests/workflows_test.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 WS frames + HTTP one-shot start/cancel/list routes"
```

---

### Task 6: Admin routes + metrics

**Files:**
- Create: `server/src/admin/workflows.rs`
- Modify: `server/src/admin/mod.rs` (`mod workflows; use workflows::*;` + route registrations beside the schedules block ~line 320)
- Modify: `server/src/metrics.rs` (step counters) and `committer.rs` (record calls)
- Test: `server/tests/workflows_test.rs`

**Interfaces:**
- Consumes: `workflows::*` (Task 2), `ApiJson`, `authenticate`-style admin gating as in `admin/schedules.rs`.
- Produces: routes `GET /admin/db/{db}/workflows?status=&limit=`, `POST /admin/db/{db}/workflows` (body = WorkflowSpec → `{id}`), `GET /admin/db/{db}/workflows/{id}` (→ `WorkflowInfoFull`), `POST /admin/db/{db}/workflows/{id}/cancel` (→ `{ok}`), `DELETE /admin/db/{db}/workflows/{id}` (→ `{ok}`). Metrics: `Metrics::record_workflow_step(outcome: WorkflowStepOutcome)` + Prometheus `rtdb_workflow_steps_total{outcome=…}`; `enum WorkflowStepOutcome { Success, Retry, Fail }` (metrics.rs, `pub`).

- [ ] **Step 1: Failing tests** — admin list/get/cancel/delete round-trip via the harness admin helpers (`admin_post`/`admin_get` as used in existing admin tests); metrics unit test asserting the three counters increment via `record_workflow_step`.

- [ ] **Step 2: Verify failure.**

- [ ] **Step 3: Implement**

`admin/workflows.rs` — clone `admin/schedules.rs` structure verbatim (module doc, `database_exists` guards, response structs):

```rust
//! Admin workflow routes: thin admin-gated wrappers over `workflows`
//! accessors, mirroring `admin/schedules.rs`. Create runs the same
//! submit-time validation + recursive table-allowlist check as the other
//! start surfaces (a no-op for the admin bypass principal).

#[derive(Serialize)]
pub(super) struct AdminWorkflowListResponse { pub(crate) workflows: Vec<WorkflowInfo> }
#[derive(Serialize)]
pub(super) struct AdminWorkflowCreateResponse { pub(crate) id: String }
#[derive(Serialize)]
pub(super) struct AdminWorkflowManageResponse { pub(crate) ok: bool }
```

Handlers: `admin_list_workflows(State, HeaderMap, Path(db), Query(params))` → `workflows::list(&state.pool, &db, status, limit.unwrap_or(100).min(500))` where `status: Option<String>` parsed via FromStr (bad value → `BadRequest`); `admin_create_workflow` → `validate_spec` + `authorize_spec_tables(&PrincipalCtx::bypass(), &spec)` (uniform, no-op today) + `insert` → `{id}`; `admin_get_workflow` → `workflows::get` → 404 when `None` (`RtDbError::not_found("unknown workflow")`); `admin_cancel_workflow` → `cancel` → `{ok}`; `admin_delete_workflow` → `delete` → `{ok}`.

`admin/mod.rs` routes (mirror the schedules block):

```rust
"/admin/db/{db}/workflows",
get(admin_list_workflows).post(admin_create_workflow),
"/admin/db/{db}/workflows/{id}/cancel",
post(admin_cancel_workflow),
"/admin/db/{db}/workflows/{id}",
get(admin_get_workflow).delete(admin_delete_workflow),
```

`metrics.rs` — three `AtomicU64` fields beside `quota_rejections_*`:

```rust
workflow_steps_success_total: AtomicU64,
workflow_steps_retry_total: AtomicU64,
workflow_steps_fail_total: AtomicU64,
```

plus `pub enum WorkflowStepOutcome { Success, Retry, Fail }`, `pub fn record_workflow_step(&self, outcome: WorkflowStepOutcome)` (each arm `fetch_add(1, Ordering::Relaxed)` on its field), and three Prometheus render lines in the exposition (`rtdb_workflow_steps_total{outcome="success"} <v>` etc. — follow the exact render style of the neighboring counters). Per-db status counts: extend the `/admin/metrics` JSON's per-db section with `workflows::count_by_status` output, following how the quota per-db breakdown is built there (same file — locate the existing per-db map).

`committer.rs` `handle_workflow_advance` — add the three record calls: `Success` after `publish_taps` in the Ok arm, `Retry` in the `schedule_retry` branch, `Fail` in the `mark_failed` branch.

- [ ] **Step 4: Run** — `cargo test --test workflows_test && cargo test --lib metrics` → pass; `cargo clippy --all-targets -- -D warnings` clean.

- [ ] **Step 5: Commit**

```bash
git -C /Users/probello/Repos/par-rt-db add server/src/admin/workflows.rs server/src/admin/mod.rs server/src/metrics.rs server/src/committer.rs server/tests/workflows_test.rs
git -C /Users/probello/Repos/par-rt-db commit -m "feat(workflows): FM-29 admin routes + rtdb_workflow_steps_total metrics"
```

---

### Task 7: ts-client mirror

**Files (locate exact split by reading the package — protocol/client/builder/admin live in `ts-client/src/`):**
- Modify: `ts-client/src/protocol.ts`, the txn builder module, the client (ws) module, the admin module, the in-memory harness module, wire-corpus/protocol tests.

**Interfaces:**
- Consumes: server wire shapes from Tasks 1/4/5 (byte-identical).
- Produces: TS types `WorkflowSpec`, `WorkflowStepSpec`, `StepRetry`, `WorkflowInfo`, `WorkflowInfoFull`, `StepOutcome`, `WorkflowStatus`; `Step` union members `{op:"startWorkflow",spec}` / `{op:"cancelWorkflow",id}`; `TxnBuilder.startWorkflow(spec)` / `.cancelWorkflow(id)`; client methods `startWorkflow(spec): Promise<WorkflowInfo>`, `cancelWorkflow(id): Promise<boolean>`, `listWorkflows(status?): Promise<WorkflowInfo[]>` (WS + HTTP); admin methods `adminListWorkflows(db, opts?)`, `adminGetWorkflow(db, id)`, `adminStartWorkflow(db, spec)`, `adminCancelWorkflow(db, id)`, `adminDeleteWorkflow(db, id)`; harness `tick()` advances workflows.

- [ ] **Step 1: Read** `ts-client/src/` to map where `Schedule`/`CancelSchedule` landed (protocol, builder, client, harness, tests) — mirror every site.
- [ ] **Step 2: Write failing tests** — protocol round-trip fixtures (start/cancel steps, spec/info shapes), builder tests, harness `tick()` test (a 2-step workflow with `sleepBeforeMs` advances across ticks; retry policy re-fires a failing step on later ticks; exhaustion marks failed), admin client test against the in-memory harness.
- [ ] **Step 3: Run** `cd /Users/probello/Repos/par-rt-db/ts-client && bunx vitest run tests/` → new tests fail.
- [ ] **Step 4: Implement** every mirror site. Harness `tick()` rule: due `pending` runs execute their current step txn against harness state; success appends outcome and applies `sleepBeforeMs` gate (compare against the harness's simulated clock); failure applies retry/backoff identically to the server (`backoffMs` helper mirrors `workflows::backoff_ms`); maxed attempts → `failed`.
- [ ] **Step 5: Verify** `bunx vitest run tests/ && bun run typecheck` (green vitest is NOT typecheck — both must pass).
- [ ] **Step 6: Commit** `git -C /Users/probello/Repos/par-rt-db add ts-client && git -C /Users/probello/Repos/par-rt-db commit -m "feat(clients): mirror FM-29 workflows into ts-client (wire, builders, ws/http, admin, harness)"`

---

### Task 8: rust-client mirror

**Files:** `rust-client/src/wire.rs`, builder module, `http` / `ws` / `admin` feature modules, tests.

**Interfaces:**
- Produces: `WorkflowSpec`/`WorkflowStepSpec`/`StepRetry`/`WorkflowInfo`/`WorkflowInfoFull`/`StepOutcome`/`WorkflowStatus` in `wire.rs` (serde attrs matching server bytes exactly); `Transaction` builder `start_workflow(spec)` / `cancel_workflow(id)`; http+ws client `start_workflow`/`cancel_workflow`/`list_workflows`; admin `list_workflows`/`get_workflow`/`start_workflow`/`cancel_workflow`/`delete_workflow` (follow the `schedules` admin method pattern; wire structs need BOTH `Serialize` and `Deserialize` for CLI pretty-printing — `skip_serializing_if` on optional fields).

- [ ] **Step 1: Read** where `Step::Schedule`/schedule surfaces live in `rust-client/src/` and mirror every site (wire, builder, http, ws, admin, tests).
- [ ] **Step 2: Failing tests** — wire round-trip (byte-compare serde_json against fixture strings taken from the server protocol tests), builder, admin mock (follow the existing schedule admin test pattern).
- [ ] **Step 3: Implement**; **Step 4: Run** `cd /Users/probello/Repos/par-rt-db/rust-client && cargo test && cargo clippy --all-targets -- -D warnings`; **Step 5: Commit** `feat(clients): mirror FM-29 workflows into rust-client (wire, builders, http/ws, admin)`.

---

### Task 9: python-client mirror

**Files:** `python-client/src/par_rt_db/wire.py`, mutation DSL module, sync http/admin module, async ws module, in-memory harness, tests.

**Interfaces:**
- Produces: wire dataclasses/TypedDicts for every Task 1/4/5 type (exact key casing); `Mutation.start_workflow(spec)` / `.cancel_workflow(id)` DSL methods; async `RtDbClient.start_workflow`/`cancel_workflow`/`list_workflows`; sync http/admin equivalents incl. `admin_list_workflows(db, status=None, limit=None)` etc.; harness `tick()` with the same advancement rules as Task 7's.

- [ ] **Step 1: Read** where schedule ops live in the python client and mirror every site.
- [ ] **Step 2: Failing tests** (`uv run pytest -q tests/`): wire round-trip fixtures, DSL builder, harness tick advancement/retry/exhaustion, admin client.
- [ ] **Step 3: Implement**; **Step 4: Run** `cd /Users/probello/Repos/par-rt-db/python-client && uv run pytest -q && uv run pyright`; **Step 5: Commit** `feat(clients): mirror FM-29 workflows into python-client (wire, DSL, ws/http, admin, harness)`.

---

### Task 10: `rtdb` CLI workflows commands

**Files:** `cli/src/main.rs` (Command enum + dispatch + handler fns, mirroring the existing admin subcommands).

**Interfaces:**
- Consumes: rust-client admin methods (Task 8).
- Produces: `rtdb workflows list --db [--status S] [--limit N]`, `rtdb workflows get --db --id ID`, `rtdb workflows start --db --file <spec.json>` (`@file`/path, JSON body = WorkflowSpec), `rtdb workflows cancel --db --id ID`. Output: pretty JSON (serde_json::to_string_pretty); `--status` validated client-side against the five snake_case values.

- [ ] **Step 1: Failing tests** — argument-validation unit tests mirroring the existing CLI tests (bad `--status` errors before network; missing `--db` errors).
- [ ] **Step 2: Implement** — `WorkflowsCommand` subcommand enum + dispatch arms calling the rust-client admin surface.
- [ ] **Step 3: Run** `cd /Users/probello/Repos/par-rt-db/cli && cargo test && cargo clippy --all-targets -- -D warnings`; against a live dev server optionally (`rtdb workflows list --db …`).
- [ ] **Step 4: Commit** `feat(cli): rtdb workflows list|get|start|cancel (FM-29)`.

---

### Task 11: Dashboard `/workflows` page

**Files:** `dashboard/src/` — new `Workflows.tsx` page (+ nav entry + route registration, following how the Sessions/schedules-adjacent pages are wired; read `dashboard/src/App.tsx` / router + `dashboard/README.md` first).

**Interfaces:**
- Consumes: ts-client admin methods (Task 7); the page's data hooks follow the polling pattern of the other operator pages.
- Produces: `/workflows` page — db selector (the shared pattern), status filter (`all|pending|running|success|failed|cancelled` + a `stuck` preset = `failed` or retrying), table (name, status chip, step n/m, attempts, sleep-until countdown, last error), expandable row → per-step timeline from `stepOutcomes` (index, status, attempts, at, error), cancel button (confirm), start-from-JSON editor (the schema-push modal pattern), auto-refresh on the existing poll cadence.

- [ ] **Step 1: Read** the dashboard structure (pages, shared components — status chips, tables, modals — and the api hook pattern); list the reuse set.
- [ ] **Step 2: Implement** the page + route + nav ("Workflows").
- [ ] **Step 3: Verify** — `cd /Users/probello/Repos/par-rt-db/dashboard && bun run build` (or the package's check target) passes; optional live check per the dashboard agentchrome workflow (login, navigate in-SPA).
- [ ] **Step 4: Commit** `feat(dashboard): /workflows operator page — run list, step timeline, cancel/start (FM-29)`.

---

### Task 12: Docs, full gate, board criteria

**Files:** `FEATURE_MATRIX.md`, `CLAUDE.md`, `server/README.md`, `ts-client/README.md`, `rust-client/README.md`, `python-client/README.md`, `dashboard/README.md` (DSL/workflow sections).

- [ ] **Step 1: FEATURE_MATRIX #29** → `✅` with client-mirror status sentence (pattern: row #28's wording).
- [ ] **Step 2: CLAUDE.md** — data-pipeline step list gains `StartWorkflow`/`CancelWorkflow`; committer tap-site list adds `handle_workflow_advance` (six → seven arms); add one workflows sentence to the architecture section (scheduler polls both tables; single-writer preserved).
- [ ] **Step 3: READMEs** — server (workflows table + semantics: at-least-once per step, bypass principal, submit-time scoping, retry defaults), each client (DSL/builder/admin snippets), dashboard (page mention).
- [ ] **Step 4: Full gate** — `make -C /Users/probello/Repos/par-rt-db checkall` (needs dev-db-up; ts-client dist built). Fix everything it reports.
- [ ] **Step 5: Commit** `docs: FM-29 workflows — FEATURE_MATRIX #29 ✅, CLAUDE.md, READMEs`.
- [ ] **Step 6: Board** — the orchestrator (not the task implementer) verifies acceptance criteria 2–4 per criterion and marks the card done: (2) retry/backoff + crash-resume tests green, (3) admin list/get verified, (4) checkall output inspected (exit code from the log, not a piped tail).

---

## Self-Review (completed)

- **Spec coverage:** wire types T1; table/validation/backoff T2; engine + crash-resume + taps + quota T3; txn steps + allowlist T4; WS/HTTP T5; admin + metrics + per-db counts T6; four-client mirrors + harness T7–9; CLI T10; dashboard T11; docs/matrix T12. Sleeps covered by T3 test 3; cancel-mid-flight by T2/T5 tests + engine status check; terminal-row delete by T6. Spec's test list items 7–10 map to T4 (atomicity/allowlist/bounds) and T3 (quota retryable — covered by the quota_err → Err-branch path; if a dedicated quota test is impractical in-harness, the engine test asserting retry-on-error plus code review of the quota_err wiring satisfies it — note this in the task report).
- **Type consistency:** `WorkflowRow` fields match `claim_due` RETURNING; `record_step_success(current_step + 1, …)` then `row.current_step += 1`; `backoff_ms(retry, attempts)` called with post-increment attempts (backoff before attempt N+1 after N failures) — T2's unit test pins this: `backoff_ms(&r,1) == initial`.
- **Placeholder scan:** T2's `get()` shows the selected-columns approach as a binding implementer note, not a `todo!` in shipped code — the shipped implementation must contain the real query.
