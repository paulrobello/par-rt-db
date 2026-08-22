# Workflow `awaitSignal` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `awaitSignal` workflow step that parks a run in a `waiting` state until an out-of-band signal (HTTP/WS/admin) delivers a payload, with timeouts routing into the step's retry policy.

**Architecture:** Extends FM-29's engine in place — no new committer arm, no new tap site. The `workflows` side table gains three nullable columns (`wait_name`, `waited_since`, `signal_payload`); the advance arm branches on the step kind; delivery is one conditional side-table UPDATE (the `workflows::cancel` precedent). `waited_since` NULL-vs-set discriminates first-arrival (park) from expired-wait (timeout attempt).

**Tech Stack:** Rust (axum/tokio/sqlx, Postgres 17), TypeScript (bun), Python (uv), Swift; wire-corpus JSON fixtures.

**Spec:** `docs/superpowers/specs/2026-08-21-workflow-await-signal-design.md` (approved 2026-08-21). Read it first — this plan argues from it.

## Global Constraints

- Five wire implementations stay byte-identical: `server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`, `swift-client/Sources/ParRtDbClient/Wire.swift`. Serde tags/field casing are load-bearing (`deny_unknown_fields` everywhere; `skip_serializing_if` on every new Option).
- New wire names, verbatim: step field `awaitSignal` (Rust `await_signal`), its object `{"name", "timeoutMs"?}`; status `"waiting"`; info fields `waitingFor`/`waitedSince`; outcome field `signal`; WS frame tag `"signalWorkflow"` with fields `{workflowId, id, name, payload?}`; HTTP response `{"delivered": true}`.
- Signals payload cap: 64 KiB serialized (`BadRequest` over). Signal name: 1..=256 chars. `timeoutMs` > 0 when present.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`.
- SQL: validated/quoted identifiers, values bound via `$n`. Errors via the `RtDbError` envelope; 404 `NOT_FOUND` unknown id, 409 `CONFLICT` not-waiting/name-mismatch.
- **No new committer arm, no new `publish_taps` site** — `awaitSignal` steps write no documents.
- Timeout retries wait the **full `timeoutMs` again** (never backoff); delivery is **latest-wins**.
- `make checkall` is the gate (`make dev-db-up` first — integration tests hit real Postgres on 127.0.0.1:55434). First-time: `make ts-client-install`, `make dashboard-install`, `make python-client-install`.
- Docs sync in the same change: `FEATURE_MATRIX.md`, READMEs, CLI help. No new env vars.
- Commit after every task (pre-commit clippy is slow — allow ≥ 5 min).

---

### Task 1: Server wire types + submit validation

**Files:**
- Modify: `server/src/protocol.rs` (WorkflowStepSpec ~398-408, WorkflowStatus ~419-455, WorkflowInfo ~478-498, StepOutcome ~457-469, ClientMessage ~67-79)
- Modify: `server/src/workflows.rs:24-56` (`validate_spec`)
- Test: `server/src/protocol.rs` (unit tests at ~840), `server/src/workflows.rs` (tests at ~639)

**Interfaces:**
- Produces: `AwaitSignalSpec { name: String, timeout_ms: Option<u64> }`; `WorkflowStepSpec { txn: Option<Transaction>, await_signal: Option<AwaitSignalSpec>, retry, sleep_before_ms }`; `WorkflowStatus::Waiting`; `WorkflowInfo { waiting_for: Option<String>, waited_since: Option<i64> }`; `StepOutcome { signal: Option<serde_json::Value> }`; `ClientMessage::SignalWorkflow { workflow_id, id, name, payload: Option<serde_json::Value> }`. All later tasks consume these.

- [ ] **Step 1: Write failing unit tests**

In `protocol.rs` tests (extend the `workflow_spec_wire_shape` block):

```rust
#[test]
fn await_signal_step_wire_shape() {
    let spec = serde_json::from_value::<WorkflowSpec>(serde_json::json!({
        "name": "gate",
        "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 3_600_000 } } ]
    }))
    .expect("parse awaitSignal spec");
    let step = &spec.steps[0];
    assert!(step.txn.is_none());
    let sig = step.await_signal.as_ref().expect("awaitSignal present");
    assert_eq!(sig.name, "approve");
    assert_eq!(sig.timeout_ms, Some(3_600_000));
    // Round-trip omits absent optionals:
    let v = serde_json::to_value(&spec).unwrap();
    assert!(v["steps"][0].get("txn").is_none());
    assert!(v["steps"][0].get("sleepBeforeMs").is_none());
    // deny_unknown_fields:
    assert!(serde_json::from_value::<WorkflowSpec>(serde_json::json!({
        "name": "g", "steps": [ { "awaitSignal": { "name": "a", "bogus": 1 } } ]
    }))
    .is_err());
}

#[test]
fn waiting_status_wire_is_snake_case() {
    assert_eq!(
        serde_json::to_value(WorkflowStatus::Waiting).unwrap(),
        serde_json::json!("waiting")
    );
    assert_eq!("waiting".parse::<WorkflowStatus>().unwrap(), WorkflowStatus::Waiting);
}

#[test]
fn workflow_info_wait_fields_omit_when_absent() {
    let info = serde_json::from_value::<WorkflowInfo>(serde_json::json!({
        "id": "w1", "name": "n", "status": "waiting", "currentStep": 1,
        "stepCount": 2, "attempts": 0, "createdAt": 1, "updatedAt": 2,
        "waitingFor": "approve", "waitedSince": 1234
    }))
    .expect("info");
    assert_eq!(info.waiting_for.as_deref(), Some("approve"));
    assert_eq!(info.waited_since, Some(1234));
    let v = serde_json::to_value(WorkflowInfo { waiting_for: None, waited_since: None, ..info }).unwrap();
    assert!(v.get("waitingFor").is_none() && v.get("waitedSince").is_none());
}

#[test]
fn signal_workflow_frame_wire_shape() {
    let m = serde_json::to_value(ClientMessage::SignalWorkflow {
        workflow_id: "c1".into(), id: "w1".into(), name: "approve".into(),
        payload: Some(serde_json::json!({"ok": true})),
    })
    .unwrap();
    assert_eq!(m["type"], "signalWorkflow");
    assert_eq!(m["name"], "approve");
    assert_eq!(m["payload"]["ok"], true);
}
```

In `workflows.rs` tests:

```rust
#[test]
fn validate_rejects_await_signal_abuse() {
    let mk = |steps: serde_json::Value| serde_json::from_value::<WorkflowSpec>(serde_json::json!({"name": "v", "steps": steps})).unwrap();
    // neither txn nor awaitSignal:
    assert!(validate_spec(&mk(vec![serde_json::json!({ "sleepBeforeMs": 5 })])).is_err());
    // both:
    assert!(validate_spec(&mk(vec![serde_json::json!({
        "txn": { "steps": [] }, "awaitSignal": { "name": "a" } }))]).is_err());
    // empty + oversize name:
    assert!(validate_spec(&mk(vec![serde_json::json!({ "awaitSignal": { "name": "" } }))]).is_err());
    assert!(validate_spec(&mk(vec![serde_json::json!({ "awaitSignal": { "name": "x".repeat(257) } }))]).is_err());
    // timeoutMs 0:
    assert!(validate_spec(&mk(vec![serde_json::json!({ "awaitSignal": { "name": "a", "timeoutMs": 0 } }))]).is_err());
    // valid minimal:
    assert!(validate_spec(&mk(vec![serde_json::json!({ "awaitSignal": { "name": "a" } }))]).is_ok());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --manifest-path server/Cargo.toml --lib workflow`
Expected: compile errors (fields/variants don't exist) — that's the RED.

- [ ] **Step 3: Implement**

`protocol.rs`:

```rust
/// An `awaitSignal` step's wait declaration (spec §Wire): park the run
/// until a signal named `name` is delivered; `timeoutMs` bounds each wait
/// attempt (omitted = wait indefinitely, cancel is the escape).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AwaitSignalSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}
```

`WorkflowStepSpec`: make `txn` optional + add the field (keep doc comment, update it to name both shapes):

```rust
pub struct WorkflowStepSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub txn: Option<Transaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub await_signal: Option<AwaitSignalSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<StepRetry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sleep_before_ms: Option<u64>,
}
```

Add `Waiting` to `WorkflowStatus` (all three impls: enum, `as_wire_str`, `FromStr`). Add to `WorkflowInfo`:

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_for: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waited_since: Option<i64>,
```

Add to `StepOutcome` (after `error`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<serde_json::Value>,
```

Add `ClientMessage::SignalWorkflow { workflow_id: String, id: String, name: String, #[serde(default, skip_serializing_if = "Option::is_none")] payload: Option<serde_json::Value> }` next to `CancelWorkflow` (tag `"signalWorkflow"` via the existing `rename_all`). The reply reuses `ServerMessage::WorkflowAck` — no new server variant.

`workflows.rs validate_spec` — inside the per-step loop, before the retry checks:

```rust
        match (&step.txn, &step.await_signal) {
            (Some(_), None) | (None, Some(sig)) => {
                if let Some(sig) = sig {
                    if sig.name.is_empty() || sig.name.len() > 256 {
                        return Err(RtDbError::bad_request(format!(
                            "steps[{i}].awaitSignal.name must be 1..=256 chars"
                        )));
                    }
                    if sig.timeout_ms == Some(0) {
                        return Err(RtDbError::bad_request(format!(
                            "steps[{i}].awaitSignal.timeoutMs must be > 0"
                        )));
                    }
                }
            }
            _ => {
                return Err(RtDbError::bad_request(format!(
                    "steps[{i}] must carry exactly one of txn or awaitSignal"
                )))
            }
        }
```

and change the recursive counter to tolerate the optional txn:

```rust
    let total: usize = spec
        .steps
        .iter()
        .map(|s| s.txn.as_ref().map_or(0, |t| count_steps(t)))
        .sum();
```

Fix every `step.txn` consumer that now breaks (Task 3 handles the committer; `grep -rn "\.txn" server/src` — most construction sites are serde-driven). Also update `protocol.rs`'s `sample_workflow_spec`/`sample_workflow_info` test helpers with the new fields (`waiting_for: None, waited_since: None`, `signal: None`) and `StepOutcome` literals.

- [ ] **Step 4: Run tests**

Run: `cargo test --manifest-path server/Cargo.toml --lib workflow && cargo test --manifest-path server/Cargo.toml --lib protocol`
Expected: PASS (compile-fix any stragglers; run the full `--lib` once clean).

- [ ] **Step 5: Commit**

```bash
git add server/src/protocol.rs server/src/workflows.rs
git commit -m "feat(server): awaitSignal wire types + submit validation"
```

---

### Task 2: Side-table ops — DDL, claim/wake, park/consume/deliver

**Files:**
- Modify: `server/src/workflows.rs` (ensure_table ~86-115, WorkflowRow ~72-80, claim_due ~169-217, next_due ~243-252, cancel ~387-403, mark_failed ~357-382, finalize_success ~330-352, info_from_row ~461-490; new fns after `schedule_retry`)
- Test: `server/src/workflows.rs` unit tests + `server/tests/workflows_test.rs` (side-table tests only, no committer needed)

**Interfaces:**
- Produces: `WorkflowRow { wait_name: Option<String>, waited_since: Option<i64>, signal_payload: Option<serde_json::Value> }`; `pub const MAX_SIGNAL_PAYLOAD_BYTES: usize = 64 * 1024;`; `pub enum SignalDelivery { Delivered, NotFound, NotWaiting, NameMismatch { waiting_on: String } }`; `pub async fn park_waiting(pool, db, id, attempts: u32, name: &str, gate: i64) -> Result<(), RtDbError>`; `pub async fn record_signal_success(pool, db, id, next_step: u32, outcome: &StepOutcome) -> Result<(), RtDbError>`; `pub async fn deliver_signal(pool, db, id, name: &str, payload: Option<serde_json::Value>) -> Result<SignalDelivery, RtDbError>`.

- [ ] **Step 1: Write failing side-table tests** (extend `server/tests/workflows_test.rs` near `insert_claim_reset_roundtrip`)

```rust
#[tokio::test]
async fn await_signal_side_table_lifecycle() {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = fresh_db(&state).await;
    workflows::ensure_table(&pool, &db).await.unwrap();
    let spec: WorkflowSpec = serde_json::from_value(serde_json::json!({
        "name": "gate", "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 50 } } ]
    }))
    .unwrap();
    let id = workflows::insert(&pool, &db, &spec).await.unwrap();
    // Park: waiting + visibility columns; not claimable before the gate.
    workflows::park_waiting(&pool, &db, &id, 0, "approve", now_ms() + 60_000).await.unwrap();
    let full = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Waiting);
    assert_eq!(full.info.waiting_for.as_deref(), Some("approve"));
    assert!(full.info.waited_since.is_some());
    assert!(workflows::claim_due(&pool, &db, now_ms(), 10).await.unwrap().is_empty());
    // next_due sees the waiting gate:
    assert!(workflows::next_due(&pool, &db).await.unwrap().is_some());

    // Delivery: latest-wins + wake flip.
    let d1 = workflows::deliver_signal(&pool, &db, &id, "wrong", None).await.unwrap();
    assert!(matches!(d1, workflows::SignalDelivery::NameMismatch { .. }));
    let d2 = workflows::deliver_signal(&pool, &db, &id, "approve", Some(serde_json::json!({"v": 1}))).await.unwrap();
    assert!(matches!(d2, workflows::SignalDelivery::Delivered));
    let d3 = workflows::deliver_signal(&pool, &db, &id, "approve", Some(serde_json::json!({"v": 2}))).await.unwrap();
    assert!(matches!(d3, workflows::SignalDelivery::Delivered));
    let claimed = workflows::claim_due(&pool, &db, now_ms(), 10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].signal_payload, Some(serde_json::json!({"v": 2})));

    // Consume + boundary clears the wait columns.
    let outcome = rtdb_server::protocol::StepOutcome {
        step_index: 0, status: OutcomeStatus::Success, attempts: 1, at: now_ms(),
        error: None, signal: Some(serde_json::json!({"v": 2})),
    };
    workflows::record_signal_success(&pool, &db, &id, 1, &outcome).await.unwrap();
    let full = workflows::get(&pool, &db, &id).await.unwrap().unwrap();
    assert_eq!(full.info.status, WorkflowStatus::Running);
    assert!(full.info.waiting_for.is_none() && full.info.waited_since.is_none());
    assert_eq!(full.step_outcomes.len(), 1);
    assert_eq!(full.step_outcomes[0].signal, Some(serde_json::json!({"v": 2})));

    // Typed classification against a fresh parked row + unknown id.
    let id2 = workflows::insert(&pool, &db, &spec).await.unwrap();
    assert!(matches!(
        workflows::deliver_signal(&pool, &db, "nope", "approve", None).await.unwrap(),
        workflows::SignalDelivery::NotFound
    ));
    workflows::cancel(&pool, &db, &id2).await.unwrap();
    assert!(matches!(
        workflows::deliver_signal(&pool, &db, &id2, "approve", None).await.unwrap(),
        workflows::SignalDelivery::NotWaiting
    ));
    let full2 = workflows::get(&pool, &db, &id2).await.unwrap().unwrap();
    assert!(full2.info.waiting_for.is_none(), "cancel clears wait columns");
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --manifest-path server/Cargo.toml --test workflows_test await_signal_side_table_lifecycle`
Expected: compile error (`park_waiting` etc. undefined).

- [ ] **Step 3: Implement in `workflows.rs`**

`ensure_table` — after the two existing statements:

```rust
    // Lazy upgrade for databases that predate awaitSignal (CREATE TABLE IF
    // NOT EXISTS does not touch an existing table).
    sqlx::query(&format!(
        "ALTER TABLE \"{schema}\".workflows
            ADD COLUMN IF NOT EXISTS wait_name text,
            ADD COLUMN IF NOT EXISTS waited_since bigint,
            ADD COLUMN IF NOT EXISTS signal_payload jsonb"
    ))
    .execute(pool)
    .await?;
```

`WorkflowRow` — add `pub wait_name: Option<String>, pub waited_since: Option<i64>, pub signal_payload: Option<serde_json::Value>`.

`claim_due` — widen the WHERE and RETURNING:

```sql
WHERE status IN ('pending', 'waiting') AND sleep_until <= $1
RETURNING id, name, spec, current_step, attempts, sleep_until, step_outcomes,
          wait_name, waited_since, signal_payload
```

(the sqlx tuple grows to 10 fields; the `WorkflowRow` build maps them straight across).

`next_due` — `WHERE status IN ('pending', 'waiting')`.

New constant + enum + functions (place after `schedule_retry`; follow the file's doc-comment density):

```rust
/// Serialized-size cap on a signal payload, enforced in `deliver_signal`
/// so every surface (HTTP body limit, WS frame, admin) gets the same bound.
pub const MAX_SIGNAL_PAYLOAD_BYTES: usize = 64 * 1024;

/// Outcome of a delivery attempt — the classification the three surfaces
/// map onto typed errors (spec §Delivery).
pub enum SignalDelivery {
    Delivered,
    NotFound,
    NotWaiting,
    NameMismatch { waiting_on: String },
}

/// Park the run at an `awaitSignal` step: `waiting` + the signal name +
/// the timeout gate. `gate` is `i64::MAX` when the step omits `timeoutMs`
/// (never due — only a delivery or cancel wakes the run). `attempts` is
/// persisted so a timeout retry that re-parks keeps its count.
pub async fn park_waiting(
    pool: &PgPool, db: &str, id: &str, attempts: u32, name: &str, gate: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
            SET status = 'waiting', attempts = $2, wait_name = $3,
                waited_since = $4, sleep_until = $5, signal_payload = NULL,
                updated_at = $4
         WHERE id = $1"
    ))
    .bind(id).bind(attempts).bind(name).bind(now_ms()).bind(gate)
    .execute(pool).await?;
    Ok(())
}
```

(sqlx binds map positionally to `$1..$n` — parameters are numbered from 1 in bind order.)

```rust
/// Consume a delivered signal and write the step boundary in one UPDATE
/// (atomic consume + bookkeeping — spec §Semantics). Row stays `running`.
pub async fn record_signal_success(
    pool: &PgPool, db: &str, id: &str, next_step: u32, outcome: &StepOutcome,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
            SET current_step = $2, attempts = 0,
                step_outcomes = step_outcomes || $3::jsonb,
                wait_name = NULL, waited_since = NULL, signal_payload = NULL,
                updated_at = $4
         WHERE id = $1"
    ))
    .bind(id).bind(next_step as i32).bind(serde_json::to_value(outcome).map_err(|e| { tracing::error!(%e, db, "step outcome serialize"); RtDbError::internal("failed to record workflow step") })?).bind(now_ms())
    .execute(pool).await?;
    Ok(())
}

/// Deliver a signal: latest-wins slot write + wake flip in one conditional
/// UPDATE. Zero rows ⇒ re-read to classify (spec §Delivery). Side-table
/// only — the `workflows::cancel`-from-handlers precedent.
pub async fn deliver_signal(
    pool: &PgPool, db: &str, id: &str, name: &str,
    payload: Option<serde_json::Value>,
) -> Result<SignalDelivery, RtDbError> {
    validate_db_name(db)?;
    if let Some(p) = &payload {
        let size = serde_json::to_vec(p).map_err(|e| { tracing::error!(%e, db, "signal payload serialize"); RtDbError::internal("failed to deliver signal") })?.len();
        if size > MAX_SIGNAL_PAYLOAD_BYTES {
            return Err(RtDbError::bad_request(format!(
                "signal payload exceeds {MAX_SIGNAL_PAYLOAD_BYTES} bytes"
            )));
        }
    }
    let schema = pg_schema(db);
    let now = now_ms();
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
            SET status = 'pending', sleep_until = $2, signal_payload = $3,
                updated_at = $2
         WHERE id = $1 AND status = 'waiting' AND wait_name = $4"
    ))
    .bind(id).bind(now).bind(payload).bind(name)
    .execute(pool).await?;
    if res.rows_affected() > 0 {
        return Ok(SignalDelivery::Delivered);
    }
    let row: Option<(Option<String>, String)> = sqlx::query_as(&format!(
        "SELECT wait_name, status FROM \"{schema}\".workflows WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    match row {
        None => Ok(SignalDelivery::NotFound),
        Some((waiting_on, status)) if status == "waiting" => Ok(SignalDelivery::NameMismatch {
            waiting_on: waiting_on.unwrap_or_default(),
        }),
        Some(_) => Ok(SignalDelivery::NotWaiting),
    }
}
```

`cancel`, `mark_failed`, `finalize_success` — add `wait_name = NULL, waited_since = NULL, signal_payload = NULL` to each UPDATE's SET clause (the leave-`waiting` rule; harmless on rows never waiting).

`info_from_row`/`list`/`get` — select and populate `waiting_for`/`waited_since` (`Some` only when the row is `waiting`, else `None`, so terminal rows never leak stale values).

- [ ] **Step 4: Run tests**

Run: `make dev-db-up && cargo test --manifest-path server/Cargo.toml --test workflows_test`
Expected: new test PASS, existing tests PASS (tuple/struct changes compile).

- [ ] **Step 5: Commit**

```bash
git add server/src/workflows.rs server/tests/workflows_test.rs
git commit -m "feat(server): workflows side-table awaitSignal ops (park/consume/deliver)"
```

---

### Task 3: Committer advance-arm branch

**Files:**
- Modify: `server/src/committer.rs:1339-1458` (`handle_workflow_advance` loop)
- Test: engine tests land in Task 5 (this task's check is the existing suite stays green + `cargo clippy`)

**Interfaces:**
- Consumes: Task 1 step types, Task 2 `park_waiting`/`record_signal_success`, existing `set_pending`/`mark_failed`/`finalize_success`.

- [ ] **Step 1: Implement the branch**

Inside the loop, after the `retry` binding and before the quota/`execute_txn` path, branch on the step kind. The txn call site becomes `step.txn.as_ref()` (validation guarantees exactly-one; a row that has neither hits the defensive failure below):

```rust
        if let Some(sig) = &step.await_signal {
            // awaitSignal: no document writes, no taps (spec §Server design).
            let now = now_ms();
            if let Some(payload) = row.signal_payload.take() {
                ctx.metrics.record_workflow_step(crate::metrics::WorkflowStepOutcome::Success);
                let finished = row.current_step as usize + 1 >= row.spec.steps.len();
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Success,
                    attempts: row.attempts + 1,
                    at: now,
                    error: None,
                    signal: Some(payload),
                };
                if finished {
                    crate::workflows::finalize_success(&ctx.pool, &ctx.db, &row.id, &record).await?;
                    return Ok(());
                }
                crate::workflows::record_signal_success(
                    &ctx.pool, &ctx.db, &row.id, row.current_step + 1, &record,
                ).await?;
                row.current_step += 1;
                row.attempts = 0;
                row.waited_since = None;
                row.wait_name = None;
                // Same next-gate logic as the txn path:
                let next = &row.spec.steps[row.current_step as usize];
                let sleep_ms = next.sleep_before_ms.unwrap_or(0).min(i64::MAX as u64) as i64;
                if now.saturating_add(sleep_ms) > now_ms() {
                    crate::workflows::set_pending(&ctx.pool, &ctx.db, &row.id, now.saturating_add(sleep_ms)).await?;
                    return Ok(());
                }
                continue;
            }
            // Timeout gate, clamped like the sleep gate (u64→i64 wrap hazard).
            let timeout_gate = match sig.timeout_ms {
                Some(ms) => now.saturating_add(ms.min(i64::MAX as u64) as i64),
                None => i64::MAX,
            };
            if row.waited_since.is_none() {
                // First arrival (or crash-recovered boundary): park.
                crate::workflows::park_waiting(&ctx.pool, &ctx.db, &row.id, row.attempts, &sig.name, timeout_gate).await?;
                return Ok(());
            }
            // The row parked and its gate expired: a timed-out attempt.
            row.attempts += 1;
            if row.attempts < retry.max_attempts {
                crate::workflows::park_waiting(&ctx.pool, &ctx.db, &row.id, row.attempts, &sig.name, timeout_gate).await?;
                ctx.metrics.record_workflow_step(crate::metrics::WorkflowStepOutcome::Retry);
                return Ok(());
            }
            let record = crate::protocol::StepOutcome {
                step_index: row.current_step,
                status: crate::protocol::OutcomeStatus::Failed,
                attempts: row.attempts,
                at: now,
                error: Some(format!("awaitSignal '{}' timed out", sig.name)),
                signal: None,
            };
            crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &record, &record.error.clone().unwrap_or_default()).await?;
            ctx.metrics.record_workflow_step(crate::metrics::WorkflowStepOutcome::Fail);
            return Ok(());
        }
        let txn = match step.txn.as_ref() {
            Some(txn) => txn,
            None => {
                // Unreachable past submit validation — corrupt row defense,
                // same stance as the out-of-range guard above.
                let outcome = failed_outcome(&row, "step has neither txn nor awaitSignal");
                crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &outcome, "workflow step missing txn").await?;
                return Ok(());
            }
        };
```

then `execute_txn(... txn ...)` uses `txn` (the rest of the loop is untouched). Update the arm's doc comment (committer.rs:1309 region) with one line: awaitSignal steps park/consume/timeout without document writes.

- [ ] **Step 2: Run the existing suite**

Run: `cargo test --manifest-path server/Cargo.toml --test workflows_test && cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings`
Expected: PASS / zero warnings.

- [ ] **Step 3: Commit**

```bash
git add server/src/committer.rs
git commit -m "feat(server): committer advances awaitSignal steps (park/consume/timeout)"
```

---

### Task 4: Delivery surfaces — HTTP, WS, admin

**Files:**
- Modify: `server/src/http_api.rs` (~500-606: new handler + struct, route registration)
- Modify: `server/src/ws.rs` (~430 dispatch, ~810 after `handle_cancel_workflow`: new handler)
- Modify: `server/src/admin/workflows.rs` (new handler after `admin_cancel_workflow`), `server/src/admin/mod.rs:346-356` (route)

**Interfaces:**
- Consumes: Task 2 `deliver_signal` + `SignalDelivery`.

- [ ] **Step 1: HTTP handler** (mirror `cancel_workflow_handler` exactly — auth, read-only rejection, rate limits, cold-db ensure):

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignalWorkflowRequest {
    db: String,
    name: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct SignalWorkflowResponse {
    delivered: bool,
}

/// `POST /api/workflows/{id}/signal`: deliver an out-of-band signal to a
/// waiting run (spec §HTTP) — typed 404/409s, latest-wins payload.
async fn signal_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<SignalWorkflowRequest>,
) -> Result<Json<SignalWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
    workflows::ensure_table(&state.pool, &body.db).await?;
    let delivery = workflows::deliver_signal(&state.pool, &body.db, &id, &body.name, body.payload).await?;
    match delivery {
        workflows::SignalDelivery::Delivered => Ok(Json(SignalWorkflowResponse { delivered: true })),
        workflows::SignalDelivery::NotFound => Err(RtDbError::not_found("unknown workflow")),
        workflows::SignalDelivery::NotWaiting => Err(RtDbError::conflict("workflow is not waiting for a signal")),
        workflows::SignalDelivery::NameMismatch { waiting_on } => Err(RtDbError::conflict(format!(
            "workflow waiting on '{waiting_on}', got '{}'", body.name
        ))),
    }
}
```

Register: `.route("/api/workflows/{id}/signal", post(signal_workflow_handler))` next to the cancel route.

- [ ] **Step 2: WS arm** (dispatch arm next to `CancelWorkflow` at ws.rs:433; handler mirrors `handle_cancel_workflow`):

```rust
/// `SignalWorkflow` arm: `authorize`-only gate, reject read-only tokens,
/// deliver (typed errors ride the ack's `error` envelope — spec §WS).
async fn handle_signal_workflow(
    fctx: &FrameCtx<'_>, workflow_id: String, id: String,
    name: String, payload: Option<serde_json::Value>,
) -> bool {
    let state = fctx.state;
    let out_tx = fctx.out_tx;
    let (ok, error) = match authorize(&state.pool, fctx.principal, fctx.db).await {
        Ok(()) if fctx.principal.is_read_only() => (
            false,
            Some(RtDbError::forbidden("read-only token cannot mutate")),
        ),
        Ok(()) => match workflows::ensure_table(&state.pool, fctx.db).await {
            Ok(()) => match workflows::deliver_signal(&state.pool, fctx.db, &id, &name, payload).await {
                Ok(workflows::SignalDelivery::Delivered) => (true, None),
                Ok(workflows::SignalDelivery::NotFound) => (false, Some(RtDbError::not_found("unknown workflow"))),
                Ok(workflows::SignalDelivery::NotWaiting) => (false, Some(RtDbError::conflict("workflow is not waiting for a signal"))),
                Ok(workflows::SignalDelivery::NameMismatch { waiting_on }) => (false, Some(RtDbError::conflict(format!("workflow waiting on '{waiting_on}', got '{name}'")))),
                Err(error) => (false, Some(error)),
            },
            Err(error) => (false, Some(error)),
        },
        Err(error) => (false, Some(error)),
    };
    let _ = out_tx.send(ServerMessage::WorkflowAck { workflow_id, ok, error });
    false
}
```

- [ ] **Step 3: Admin route** — `admin/workflows.rs` (after `admin_cancel_workflow`; params struct + response reuse the file's existing shapes):

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminSignalParams {
    name: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

/// `POST /admin/db/{db}/workflows/{id}/signal` — deliver a signal (typed
/// 404/409s; `ok` only on delivery).
pub(super) async fn admin_signal_workflow(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    ApiJson(params): ApiJson<AdminSignalParams>,
) -> Result<Json<AdminWorkflowManageResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    workflows::ensure_table(&state.pool, &db).await?;
    match workflows::deliver_signal(&state.pool, &db, &id, &params.name, params.payload).await? {
        workflows::SignalDelivery::Delivered => Ok(Json(AdminWorkflowManageResponse { ok: true })),
        workflows::SignalDelivery::NotFound => Err(RtDbError::not_found("unknown workflow")),
        workflows::SignalDelivery::NotWaiting => Err(RtDbError::conflict("workflow is not waiting for a signal")),
        workflows::SignalDelivery::NameMismatch { waiting_on } => Err(RtDbError::conflict(format!(
            "workflow waiting on '{waiting_on}', got '{}'", params.name
        ))),
    }
}
```

Register in `admin/mod.rs` after the cancel route (~line 350):

```rust
        .route(
            "/admin/db/{db}/workflows/{id}/signal",
            post(admin_workflows::admin_signal_workflow),
        )
```

(match the exact `post(...)` path style used by the neighboring routes — check how `admin_cancel_workflow` is imported/wired there and mirror it).

- [ ] **Step 4: Compile + clippy**

Run: `cargo clippy --manifest-path server/Cargo.toml --all-targets -- -D warnings && cargo test --manifest-path server/Cargo.toml --lib`
Expected: clean. (Surface behavior tests are Task 5.)

- [ ] **Step 5: Commit**

```bash
git add server/src/http_api.rs server/src/ws.rs server/src/admin/workflows.rs server/src/admin/mod.rs
git commit -m "feat(server): awaitSignal delivery on HTTP, WS, and admin routes"
```

---

### Task 5: Server integration tests (card criteria)

**Files:**
- Test: `server/tests/workflows_test.rs` (engine tests use the `make_committers` pattern from `schedule_step_test.rs`, already in this file at ~93)

**Interfaces:**
- Consumes: Tasks 1-4. Produces: the evidence for card criteria 1 and 2.

- [ ] **Step 1: Write the engine tests**

Add to `workflows_test.rs` (names are the acceptance evidence — keep them verbatim):

```rust
// --- awaitSignal (approval gates) -------------------------------------------

fn await_gate_spec(name: &str, signal: &str, timeout_ms: u64, max_attempts: u32) -> WorkflowSpec {
    serde_json::from_value(serde_json::json!({
        "name": name,
        "steps": [
            insert_step("pre"),
            { "awaitSignal": { "name": signal, "timeoutMs": timeout_ms },
              "retry": { "maxAttempts": max_attempts, "initialRetryMs": 10, "maxRetryMs": 50 } },
            insert_step("post")
        ]
    }))
    .expect("parse await spec")
}

/// Poll `f` until it returns Some, up to `deadline_ms` (the scheduler's
/// ≤ 2 s claim cadence makes fixed sleeps flaky).
async fn eventually<T>(deadline_ms: u64, mut f: impl FnMut() -> BoxFuture<'_, Option<T>>) -> Option<T> { /* poll loop, 25 ms tick */ }

#[tokio::test]
async fn await_signal_parks_delivers_and_advances_with_payload() {
    // make_committers (line ~93 pattern) → insert await_gate_spec → step 0
    // ("pre") lands, run reaches waiting:
    let full = eventually(10_000, || Box::pin(async {
        workflows::get(&pool, &db, &id).await.ok()?.filter(|f| f.info.status == WorkflowStatus::Waiting)
    })).await.expect("parks");
    assert_eq!(full.info.waiting_for.as_deref(), Some("approve"));
    // Deliver via the HTTP surface (spawn_app pattern from this file):
    // POST /api/workflows/{id}/signal {"name":"approve","payload":{"ok":true}}
    // → 200 {"delivered":true}; then the run completes:
    let done = eventually(10_000, || Box::pin(async {
        workflows::get(&pool, &db, &id).await.ok()?.filter(|f| f.info.status == WorkflowStatus::Success)
    })).await.expect("completes");
    // "post" doc landed; the outcome trail carries the payload verbatim:
    assert_eq!(done.step_outcomes[1].signal, Some(serde_json::json!({"ok": true})));
}

#[tokio::test]
async fn await_signal_timeout_retries_with_fresh_timeout_then_succeeds() {
    // timeoutMs 100, maxAttempts 3. Wait for the FIRST re-park (attempts == 1)
    // and pin FRESH-timeout: new sleep_until - observed re-park time ≈ 100 ms
    // (backoff would be 10 ms). Record re-park updated_at; assert
    // sleep_until - updated_at >= 90. Then deliver → success; attempts on
    // the outcome == 2.
}

#[tokio::test]
async fn await_signal_timeout_exhaustion_fails_typed() {
    // timeoutMs 100, maxAttempts 1, no delivery → failed;
    // last_error == "awaitSignal 'approve' timed out"; "post" never inserted;
    // step_outcomes[1].status == Failed.
}

#[tokio::test]
async fn signal_delivery_typed_errors_on_all_three_surfaces() {
    // parked run: wrong name → 409 CONFLICT naming both signals;
    // unknown id → 404 NOT_FOUND; after cancel → 409 "not waiting".
    // Same classification via WS frame (WorkflowAck error.code) and
    // admin route. Read-only token → 403 on HTTP + WS.
}

#[tokio::test]
async fn await_signal_no_timeout_waits_indefinitely() {
    // no timeoutMs: stays Waiting past several claim sweeps; signal advances it.
}

#[tokio::test]
async fn await_signal_latest_wins_payload() { /* two deliveries; outcome.signal == second */ }

#[tokio::test]
async fn cancel_while_waiting_then_late_signal_conflicts() { /* cancel → cancelled; signal → NotWaiting */ }
```

(Flesh the comment-only bodies out with the real assertions — the `eventually` helper + `make_committers` + `admin_post`/HTTP-call patterns are all in this file or `common/`; follow them. `BoxFuture` needs `use futures::future::BoxFuture;` — check the file's existing imports for the house style; a plain `loop { if let Some(x) = f().await { return Some(x) } tokio::time::sleep(...).await }` generic async fn is fine without the type gymnastics.)

- [ ] **Step 2: Run them**

Run: `make dev-db-up && cargo test --manifest-path server/Cargo.toml --test workflows_test`
Expected: all PASS. On timing flakes, prefer lengthening deadlines over tightening asserts.

- [ ] **Step 3: Commit**

```bash
git add server/tests/workflows_test.rs
git commit -m "test(server): awaitSignal park/deliver/timeout integration coverage"
```

---

### Task 6: ts-client mirror

**Files:**
- Modify: `ts-client/src/protocol.ts`, `ts-client/src/mutation.ts` (step builder), `ts-client/src/client.ts` (WS reactive `signalWorkflow`), `ts-client/src/http.ts`, `ts-client/src/admin.ts`, `ts-client/src/in_memory/store.ts` (engine + `signalWorkflow`)
- Test: `ts-client/tests/workflows.test.ts` (wire + engine), `ts-client/tests/wire-corpus.test.ts` (picks up Task 10 fixtures)

**Interfaces:**
- Consumes: the wire names from Global Constraints (byte-identical to Task 1).

- [ ] **Step 1: Wire + builders** — `protocol.ts`: `WorkflowStepSpec.txn?: Transaction; awaitSignal?: { name: string; timeoutMs?: number }`; `WorkflowStatus` adds `"waiting"`; `WorkflowInfo` adds `waitingFor?: string; waitedSince?: number`; `StepOutcome` adds `signal?: unknown`; `ClientMessage` union adds `{ type: "signalWorkflow"; workflowId: string; id: string; name: string; payload?: unknown }` (reply is the existing `WorkflowAck`). Add a builder `awaitSignal(name: string, timeoutMs?: number)` to the workflow-spec DSL in `mutation.ts` mirroring the existing step constructors, and `signalWorkflow(id, name, payload?)` methods on the reactive client (`client.ts`), http client, and admin client — mirroring each surface's `cancelWorkflow` exactly (route `POST /api/workflows/{id}/signal`, body `{db, name, payload}`, admin `POST /admin/db/{db}/workflows/{id}/signal`).
- [ ] **Step 2: In-memory engine** — `store.ts`: the workflow runner's step executor grows the same three-way branch (payload → success outcome with `signal`; `waitedSince == null` → park with `timeoutMs` gate on the tick clock; gate passed → timeout attempt, fresh gate on retry, terminal fail at exhaustion — spec §Client mirrors). Add `signalWorkflow(id, name, payload)` implementing latest-wins + typed errors (`NOT_FOUND`/`CONFLICT` via the existing error helper) and a wake (next `tick()` claims it).
- [ ] **Step 3: Tests** — extend `tests/workflows.test.ts`: wire round-trip of the new shapes (mirror the Task 1 unit tests); engine tests mirroring Task 5's park/deliver/timeout/fresh-timeout/latest-wins/typed-errors against `tick()` with the injected clock; `signalWorkflow` on the reactive surface.
- [ ] **Step 4: Verify** — `cd ts-client && bun run typecheck && bunx vitest run tests/workflows.test.ts` (green vitest is NOT typecheck — run both).
- [ ] **Step 5: Commit** — `git commit -m "feat(ts-client): awaitSignal mirror (wire, builders, engine)"`

---

### Task 7: rust-client mirror

**Files:**
- Modify: `rust-client/src/wire.rs`, `rust-client/src/mutation.rs` (builder), `rust-client/src/http.rs`, `rust-client/src/ws.rs`, `rust-client/src/wire/admin.rs` + `rust-client/src/admin/` (admin surface)
- Test: the client's existing workflow test module (locate via `grep -rn "cancelWorkflow" rust-client/src` and extend alongside)

**Interfaces:**
- Consumes: wire names from Global Constraints.

- [ ] **Step 1:** `wire.rs`: `WorkflowStepSpec { pub txn: Option<Transaction>, #[serde(default, skip_serializing_if = "Option::is_none")] pub await_signal: Option<AwaitSignalSpec>, ... }` + `AwaitSignalSpec { name, timeout_ms }` (camelCase, deny_unknown_fields); `WorkflowStatus::Waiting`; `WorkflowInfo` + `waiting_for`/`waited_since`; `StepOutcome` + `signal`; `ClientMessage::SignalWorkflow { workflow_id, id, name, #[serde(default, skip_serializing_if)] payload }`. Builder + `signal_workflow` on http/ws/admin mirroring each `cancel_workflow`. Per the FM-29 mirror note the rust harness documents (not ports) engine asymmetry — add a doc line, no engine change.
- [ ] **Step 2: Tests** — wire round-trips mirroring Task 1's unit tests + builder/surface tests per the file's existing style.
- [ ] **Step 3: Verify** — `cargo test --manifest-path rust-client/Cargo.toml && cargo clippy --manifest-path rust-client/Cargo.toml --all-targets -- -D warnings`.
- [ ] **Step 4: Commit** — `git commit -m "feat(rust-client): awaitSignal mirror (wire, builders, surfaces)"`

---

### Task 8: python-client mirror

**Files:**
- Modify: `python-client/src/par_rt_db/wire.py`, `mutation.py`, `http_client.py`, `aio_http_client.py` (if it carries workflow methods — check), `ws_client.py`, `admin.py`, `in_memory/store.py`
- Test: `python-client/tests/test_wire.py`, `tests/test_in_memory.py` (or a new `tests/test_await_signal.py` following file conventions)

**Interfaces:**
- Consumes: wire names from Global Constraints.

- [ ] **Step 1:** `wire.py`: `WorkflowStepSpec` gains optional `await_signal` (`_Camel` dataclass, `awaitSignal` field, omitempty per file style), `txn` optional; `WorkflowStatus` Literal adds `"waiting"`; `WorkflowInfo` + `waiting_for`/`waited_since`; `StepOutcome` + `signal`; `ClientMessage` union + `{"type": "signalWorkflow", ...}`. DSL + `signal_workflow` on http/ws/admin mirrors. `in_memory/store.py`: the ts engine branch from Task 6 ported (injected clock, park/tick-timeout/latest-wins/typed errors). Validation port is complete (exactly-one-of, name bounds, timeout > 0) — partial ports are invisible debt.
- [ ] **Step 2: Tests** — mirror Task 5's engine tests via harness `tick()` + wire round-trips; `uv run pytest -q tests/`.
- [ ] **Step 3: Commit** — `git commit -m "feat(python-client): awaitSignal mirror (wire, DSL, harness)"`

---

### Task 9: swift-client mirror

**Files:**
- Modify: `swift-client/Sources/ParRtDbClient/Wire.swift`, `MutationDsl.swift` (builder), `HttpClient.swift`, `WsClient.swift`, `AdminClient.swift`, `InMemoryEngine.swift`
- Test: `swift-client/Tests/ParRtDbClientTests/WireTests.swift`, `WsClientTests.swift`, `InMemoryTests.swift` (extend alongside existing workflow tests)

**Interfaces:**
- Consumes: wire names from Global Constraints.

- [ ] **Step 1:** `Wire.swift`: `AwaitSignalSpec` (Codable, camelCase keys, Optional `timeoutMs` omitted when nil — the established `encodeIfPresent` pattern); step union fields; `WorkflowStatus.waiting` case + parser; info/outcome fields; `SignalWorkflow` client message + the existing `WorkflowAck` reply. Builders + `signalWorkflow` on all three surfaces mirroring `cancelWorkflow`. `InMemoryEngine.swift`: the Task 6 branch ported (Swift porting rules: injected clock/random, numeric equality tolerating Int/Double).
- [ ] **Step 2: Tests** — wire round-trips + engine park/signal/timeout via `tick` with injected clock, mirroring the existing workflow engine tests.
- [ ] **Step 3: Verify** — `make swift-client-checkall`.
- [ ] **Step 4: Commit** — `git commit -m "feat(swift-client): awaitSignal mirror (wire, DSL, engine)"`

---

### Task 10: wire-corpus fixtures

**Files:**
- Modify: `wire-corpus/wire-corpus.json` (add entries; no semantics/ case — FM-29 precedent: workflow engine behavior is pinned by the five harness test suites, semantics cases cover document visibility)

**Interfaces:**
- Consumes: Tasks 1, 6-9 wire types.

- [ ] **Step 1:** Following the file's existing workflow entries (grep `"startWorkflow"` / `"cancelWorkflow"` in `client_messages`/`server_messages` and mirror their fixture shape): a `client_messages` entry — full `SignalWorkflow` frame carrying a spec-independent payload; a `WorkflowSpec` fixture used by a `startWorkflow` entry whose steps mix one `{"awaitSignal": {"name": "approve", "timeoutMs": 3600000}}` step among txn steps; a `server_messages` `WorkflowAck` reply with `ok: false` + a CONFLICT error envelope; a `WorkflowInfo` with `status: "waiting"`, `waitingFor`, `waitedSince` (extend wherever infos are fixture'd; if none are, add to the section whose shape fits — check `schedule_infos` for the pattern). Include a reject entry asserting unknown fields inside `awaitSignal` are denied if the corpus's reject machinery covers step shapes (mirror `rejects_schedule_when_unknown_field`).
- [ ] **Step 2:** Run all five runners: `cargo test --manifest-path server/Cargo.toml --test golden_vector_test` is NOT it — the corpus runner tests are `server` `wire_corpus` test (locate via `grep -rn "wire-corpus.json" server/tests ts-client/tests rust-client python-client swift-client`), then `cd ts-client && bunx vitest run tests/wire-corpus.test.ts`, `uv run --project python-client pytest -q python-client/tests/test_wire_parity.py` (adjust to the real runner files found), `make swift-client-checkall`.
- [ ] **Step 3: Commit** — `git commit -m "test(corpus): awaitSignal wire fixtures (all five runners)"`

---

### Task 11: CLI + dashboard

**Files:**
- Modify: `cli/src/commands/workflows.rs` (+ its enum in the args file — locate via `grep -rn "WorkflowsCommand" cli/src`)
- Modify: `dashboard/src/pages/WorkflowsPage.tsx` (+ `.module.css` if a new style is needed; extend `WorkflowsPage.test.tsx`)

**Interfaces:**
- Consumes: Task 4 admin route.

- [ ] **Step 1 (CLI):** `WorkflowsCommand` gains `Signal { id: String, name: String, payload_json: Option<String> }`; parse payload with `serde_json::from_str` (BadInput on error); POST the admin route via the existing admin-call helper the cancel subcommand uses; `parse_workflow_status` accepts `"waiting"` (extend its test to six values); the list output prints `waitingFor` when present.
- [ ] **Step 2 (dashboard):** `WorkflowsPage.tsx` — status chip handles `waiting` (existing chip map, warning color); a detail line `waiting on {waitingFor} · {formatDuration(now - waitedSince)}` for waiting rows; a "Send signal" button on waiting rows opening a small dialog (name input prefilled `waitingFor`, payload textarea parsed as JSON, error surfaced on parse/409) POSTing `/admin/db/{db}/workflows/{id}/signal`, then refetching the list. Extend `WorkflowsPage.test.tsx` with a waiting-run render + send-signal interaction test following the page's existing test style.
- [ ] **Step 3: Verify** — `cargo test --manifest-path cli/Cargo.toml`; `cd dashboard && bun run typecheck && bunx vitest run src/pages/WorkflowsPage.test.tsx` (names per the dashboard's real test layout).
- [ ] **Step 4: Commit** — `git commit -m "feat(cli,dashboard): awaitSignal affordances (send signal, waiting state)"`

---

### Task 12: Docs + full gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (FM-29 row/section: `awaitSignal` + mirror status), `README.md` (workflow DSL section), `server/README.md` + client READMEs where workflow surfaces are documented, `docs/ARCHITECTURE.md` (workflows paragraph: `waiting` state, delivery surfaces, explicitly NO new arm/tap), `CLAUDE.md` (one line in the workflows sentence), `cli` help text if separate.

- [ ] **Step 1:** Update each doc — current-tense, matching shipped behavior only.
- [ ] **Step 2: Full gate** — `make dev-db-up && make checkall` from the repo root (pulls ts-client-build first). Fix everything. This is the definition of done.
- [ ] **Step 3: Commit** — `git commit -m "docs: awaitSignal feature (matrix, READMEs, architecture)"`

---

## Execution order & parallelism

Tasks 1→2→3→4→5 are sequential (server). Tasks 6, 7, 8, 9 are independent of each other (one client package each — safe as parallel subagents; they share no files). Task 10 needs all of 1+6-9. Tasks 11 and 12 are final. The orchestrator runs `make checkall` itself after each batch — never trust a subagent's self-reported green.
