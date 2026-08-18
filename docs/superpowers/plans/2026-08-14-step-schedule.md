# Step::Schedule / Step::CancelSchedule Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a transaction atomically schedule a future transaction (`Step::Schedule`) and cancel a scheduled job (`Step::CancelSchedule`), mirrored across all four clients, and close the enqueue-time table-allowlist gap on the standalone Schedule op.

**Architecture:** `execute_txn` already runs all steps inside ONE sqlx transaction, so a Schedule step that INSERTs into `scheduled_txns` on the open connection gets write+enqueue atomicity for free. The scheduler loop, `RunScheduled` committer arm, and fire-time semantics are reused unchanged. A shared recursive `authorize_txn_tables` helper closes the scoped-token gap on all four enqueue surfaces (the step + the three standalone Schedule-op paths).

**Tech Stack:** Rust (axum/tokio/sqlx/Postgres 17), TypeScript client (bun/vitest), Rust client (cargo), Python client (uv/pydantic v2/pytest).

**Spec:** `docs/superpowers/specs/2026-08-14-step-schedule-design.md` (FM-28)

## Global Constraints

- Wire shapes, byte-identical in `server/src/txn.rs`, `ts-client/src/protocol.ts`, `rust-client/src/mutation.rs`, `python-client/src/par_rt_db/mutation.py`:
  - `{"op": "schedule", "when": <ScheduleWhen>, "txn": {"steps": [...]}}` — `when` reuses the existing `ScheduleWhen` union (`{"type":"afterMs","ms":i64}` | `{"type":"runAt","ms":i64}` | `{"type":"cron","expr":"5-field"}`); `txn` is a full `Transaction`.
  - `{"op": "cancelSchedule", "id": "<job id>"}`.
  - The Step enum keeps `#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]`.
- Step results: Schedule pushes `{"scheduleId": "<id>"}`; CancelSchedule pushes `{"cancelled": <bool>}` (`false` on missing/already-fired/already-cancelled — NOT an error).
- `Step::table()` becomes `Option<&str>`: document steps return `Some`, `Schedule`/`CancelSchedule` return `None` (per-step `authorize_table` skips `None`).
- `worst_case_affected`: both new variants count **0**.
- `MAX_STEPS` (1024) counts **recursively**: a Schedule step contributes `1 + count(nested steps recursively)`. By-query caps are NOT applied to nested txns at enqueue (re-validated by `execute_txn` at fire time).
- Single-writer invariant: `scheduled_txns` writes ride `execute_txn`'s existing sqlx transaction via new connection-bound `scheduler::insert_on`/`cancel_on`; never write document tables or `scheduled_txns` from a non-committer path.
- Errors: `RtDbError` envelope; bad `when` (negative `afterMs`, invalid/never-firing cron) ⇒ `BadRequest` and the WHOLE txn rolls back; table-allowlist violation ⇒ `Forbidden` (403).
- **AUTH-TOUCHING (Task 3):** the enqueue-time table-authorization tightening is user-approved ("tighten both in this card"), but its commits must be flagged for manual review in the session report — never slipped in silently.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`; `make checkall` green before the branch merges (Task 7).
- Sub-agents never touch the kanban board; the orchestrating session owns it.
- Verification commands run from the right directories: `cargo` from `server/` / `rust-client/`, `bun` from `ts-client/`, `uv run` from `python-client/`, `make -C ~/Repos/par-rt-db` for repo-wide targets. Dev Postgres must be up (`make -C ~/Repos/par-rt-db dev-db-up`) for server integration tests.

## File Structure

- `server/src/txn.rs` — Step enum + `table()` + recursive budget + `authorize_txn_tables` + `step_schedule`/`step_cancel_schedule` (Task 1)
- `server/src/scheduler.rs` — `insert_on`/`cancel_on` connection-bound variants (Task 1)
- `wire-corpus/wire-corpus.json` — two new mutate-frame entries (Task 1; all four client corpus tests then exercise them)
- `server/tests/schedule_step_test.rs` — new integration test binary (Task 2)
- `server/src/ws.rs`, `server/src/http_api.rs`, `server/src/admin/schedules.rs` — standalone-op table auth (Task 3)
- `ts-client/src/protocol.ts`, `ts-client/src/mutation.ts`, `ts-client/tests/mutation.test.ts` (Task 4)
- `rust-client/src/mutation.rs`, `rust-client/src/unit tests` (Task 5)
- `python-client/src/par_rt_db/mutation.py`, `python-client/src/par_rt_db/in_memory.py`, `python-client/tests/test_mutation.py`, `python-client/tests/test_in_memory.py` (Task 6)
- `FEATURE_MATRIX.md`, `CLAUDE.md`, READMEs (Task 7)

Tasks 4–6 touch disjoint directories and may run as PARALLEL sub-agents after Task 3 lands; the orchestrator runs the full gate after all three finish.

---

### Task 1: Server core — Step variants, connection-bound scheduler writes, recursive budget

**Files:**
- Modify: `server/src/txn.rs` (Step enum :75-138, `table()` :150-162, `worst_case_affected` :1283-1293, `execute_txn` :1295-1417, `StepCtx` :1432-1440, module doc :1-9, tests :1766+)
- Modify: `server/src/scheduler.rs` (`insert` :106-135, `cancel` :181-191)
- Modify: `wire-corpus/wire-corpus.json` (`client_messages` section, after the existing mutate entries ~line 16)
- Test: `server/src/txn.rs` unit tests; `server/tests/wire_corpus.rs` (no edit — it round-trips the corpus file)

**Interfaces:**
- Consumes: `scheduler::resolve_when(when: ScheduleWhen, now: i64) -> Result<(&'static str, i64, Option<String>), RtDbError>` (scheduler.rs:34-51, takes `when` by value); `authorize_table(ctx: &PrincipalCtx, table: &str)` (auth/mod.rs:320).
- Produces (later tasks depend on these exact signatures):
  - `pub(crate) async fn scheduler::insert_on(conn: &mut PgConnection, db: &str, kind: &str, due_at: i64, txn: &Transaction, cron: Option<&str>) -> Result<String, RtDbError>`
  - `pub(crate) async fn scheduler::cancel_on(conn: &mut PgConnection, db: &str, id: &str) -> Result<bool, RtDbError>`
  - `pub(crate) fn txn::authorize_txn_tables(ctx: &PrincipalCtx, txn: &Transaction) -> Result<(), RtDbError>` (recursive; Task 3 calls it from ws.rs/http_api.rs/admin/schedules.rs)
  - `Step::Schedule { when: ScheduleWhen, txn: Box<Transaction> }`, `Step::CancelSchedule { id: String }`, `Step::table(&self) -> Option<&str>`

- [ ] **Step 1: Add `insert_on`/`cancel_on` to scheduler.rs and refactor `insert`/`cancel` onto them**

In `server/src/scheduler.rs`, add `use sqlx::PgConnection;` beside the existing `use sqlx::PgPool;` (line 53), then:

```rust
/// Connection-bound variant of [`insert`] — runs on an OPEN sqlx transaction
/// so a `Step::Schedule` inside `execute_txn` enqueues its row atomically with
/// the txn's document writes (FM-28): the row becomes visible exactly at the
/// caller's `tx.commit()` and rolls back with it. Identical SQL to `insert`.
pub(crate) async fn insert_on(
    conn: &mut PgConnection,
    db: &str,
    kind: &str,
    due_at: i64,
    txn: &Transaction,
    cron: Option<&str>,
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let id = new_id();
    let txn_json = serde_json::to_value(txn).map_err(|err| {
        tracing::error!(error = %err, db, "failed to serialize scheduled txn");
        RtDbError::internal("failed to schedule txn")
    })?;
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".scheduled_txns
            (id, kind, due_at, txn, cron, status, created_at)
         VALUES ($1, $2, $3, $4, $5, 'pending', $6)"
    ))
    .bind(&id)
    .bind(kind)
    .bind(due_at)
    .bind(txn_json)
    .bind(cron)
    .bind(now_ms())
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

/// Connection-bound variant of [`cancel`] for `Step::CancelSchedule` — the
/// DELETE rides the caller's open sqlx transaction (FM-28).
pub(crate) async fn cancel_on(
    conn: &mut PgConnection,
    db: &str,
    id: &str,
) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "DELETE FROM \"{schema}\".scheduled_txns WHERE id = $1"
    ))
    .bind(id)
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() > 0)
}
```

Then rewrite `insert` (scheduler.rs:106-135) as a thin wrapper (its body moves into `insert_on`; keep the `pub` signature — existing call sites in ws.rs/http_api.rs/admin/schedules.rs/scheduled_test.rs are unchanged):

```rust
pub async fn insert(
    pool: &PgPool,
    db: &str,
    kind: &str,
    due_at: i64,
    txn: &Transaction,
    cron: Option<&str>,
) -> Result<String, RtDbError> {
    let mut conn = pool.acquire().await?;
    insert_on(&mut conn, db, kind, due_at, txn, cron).await
}
```

Leave `cancel` as-is (its two-statement body is small; do not refactor it — surgical change only).

- [ ] **Step 2: Add the Step variants and switch `table()` to `Option<&str>`**

In `server/src/txn.rs`, extend the `Step` enum (after `DeleteByQuery`, keeping the enum's existing serde attrs untouched) and update `table()`:

```rust
    /// Schedule `txn` to run later (FM-28). The `scheduled_txns` row is
    /// inserted on the OPEN sqlx transaction, so the enqueue commits (or
    /// rolls back) atomically with this txn's document writes. Step result is
    /// `{"scheduleId": "<id>"}`; the job fires through the unchanged
    /// scheduler → `RunScheduled` committer path as the system (bypass)
    /// principal. Nested steps are table-scope-checked recursively at enqueue
    /// (`authorize_txn_tables`) and fully re-validated by `execute_txn` at
    /// fire time.
    Schedule {
        when: crate::protocol::ScheduleWhen,
        txn: Box<Transaction>,
    },
    /// Cancel a previously scheduled job by id, on the open sqlx transaction.
    /// Step result `{"cancelled": <bool>}` — `false` (not an error) when the
    /// id is missing, already fired, or already cancelled. A fire currently
    /// in flight completes; the job never fires again (the cron finalize
    /// update touches 0 rows).
    CancelSchedule {
        id: String,
    },
```

```rust
impl Step {
    /// The document table this step targets, or `None` for the schedule
    /// control-flow steps (they touch no documents; the per-step table-scope
    /// gate in `execute_txn` skips them, and `Step::Schedule` checks its
    /// NESTED steps recursively via `authorize_txn_tables` instead).
    pub fn table(&self) -> Option<&str> {
        match self {
            Step::Insert { table, .. }
            | Step::Patch { table, .. }
            | Step::Replace { table, .. }
            | Step::Delete { table, .. }
            | Step::ExpectVersion { table, .. }
            | Step::ExpectAbsent { table, .. }
            | Step::Upsert { table, .. }
            | Step::PatchByQuery { table, .. }
            | Step::DeleteByQuery { table, .. } => Some(table),
            Step::Schedule { .. } | Step::CancelSchedule { .. } => None,
        }
    }
}
```

Then fix every caller of `.table()` — run `grep -n '\.table()' server/src server/tests` and update each. Known caller: `execute_txn`'s per-step gate (txn.rs:1379) becomes:

```rust
        if let Some(table) = step.table() {
            authorize_table(sctx.ctx, table)?;
        }
```

- [ ] **Step 3: `worst_case_affected` counts the new steps as 0; recursive step budget**

In `worst_case_affected` (txn.rs:1283-1293) add an arm so the `_ => 1` fallback no longer catches them:

```rust
            Step::Schedule { .. } | Step::CancelSchedule { .. } => 0,
```

Add the recursive counter near `MAX_STEPS`:

```rust
/// Recursive step count: every step counts 1, and a `Schedule` step adds its
/// nested txn's count on top. The total tree must stay within `MAX_STEPS` —
/// this bounds one request body's serialized size and blocks the nesting
/// bomb (N steps each scheduling N steps). By-query caps are NOT applied to
/// nested txns here: the nested txn executes in a future committer turn and
/// `execute_txn` re-validates it fully at fire time.
fn count_steps(txn: &Transaction) -> usize {
    txn.steps
        .iter()
        .map(|step| match step {
            Step::Schedule { txn, .. } => 1 + count_steps(txn),
            _ => 1,
        })
        .sum()
}
```

Replace the flat check in `execute_txn` (txn.rs:1315-1319):

```rust
    if count_steps(txn) > MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "transaction exceeds maximum of {MAX_STEPS} steps (counted recursively, including scheduled txns)"
        )));
    }
```

- [ ] **Step 4: `authorize_txn_tables` + step execution + `StepCtx.db`**

Add the recursive auth helper (used by `step_schedule` now and by the three standalone surfaces in Task 3):

```rust
/// Recursive table-scope check over every step in `txn`, including steps
/// nested inside `Schedule` payloads. Runs at ENQUEUE time (the `Schedule`
/// step here, and the standalone Schedule-op surfaces) so a scoped machine
/// token cannot smuggle a future write into a forbidden table via a
/// scheduled job. Bypass principals (`tables = None` — admin/full-access/
/// interactive) are unaffected; per-row rules are deliberately NOT
/// pre-checked (rows change between enqueue and fire; the firing job runs
/// as the system principal — documented behavior, see the FM-28 spec).
pub(crate) fn authorize_txn_tables(ctx: &PrincipalCtx, txn: &Transaction) -> Result<(), RtDbError> {
    for step in &txn.steps {
        if let Some(table) = step.table() {
            authorize_table(ctx, table)?;
        }
        if let Step::Schedule { txn, .. } = step {
            authorize_txn_tables(ctx, txn)?;
        }
    }
    Ok(())
}
```

Add `db: &'a str` to `StepCtx` (txn.rs:1432-1440) and set it at construction in `execute_txn` (`db` is already a parameter):

```rust
    let mut sctx = StepCtx {
        tx: &mut tx,
        db,
        pg_schema_name: pg_schema_name.as_str(),
        schema,
        ctx,
        owner,
        write_set: &mut write_set,
        results: &mut results,
    };
```

Add the two match arms in `execute_txn`'s step loop (after `Step::DeleteByQuery`):

```rust
            Step::Schedule { when, txn } => step_schedule(&mut sctx, when, txn).await?,
            Step::CancelSchedule { id } => step_cancel_schedule(&mut sctx, id).await?,
```

And the handlers (beside the other `step_*` fns):

```rust
/// `Schedule` step: validate timing, recursively table-scope-check the
/// nested txn against the CURRENT caller, and insert the `scheduled_txns`
/// row on the open sqlx transaction — atomic with the enclosing txn's
/// document writes (FM-28). The row becomes visible at `tx.commit()`; the
/// scheduler's existing ≤2s poll picks it up from there.
async fn step_schedule(
    sctx: &mut StepCtx<'_>,
    when: &crate::protocol::ScheduleWhen,
    txn: &Transaction,
) -> Result<(), RtDbError> {
    authorize_txn_tables(sctx.ctx, txn)?;
    let (kind, due_at, cron) = scheduler::resolve_when(when.clone(), now_ms())?;
    let id = scheduler::insert_on(sctx.tx, sctx.db, kind, due_at, txn, cron.as_deref()).await?;
    sctx.results.push(serde_json::json!({ "scheduleId": id }));
    Ok(())
}

/// `CancelSchedule` step: DELETE the job row on the open sqlx transaction.
/// `false` (not an error) when the id is missing/already-fired/already
/// cancelled — matching the standalone cancel op. A fire in flight
/// completes; the cron finalize update then touches 0 rows.
async fn step_cancel_schedule(sctx: &mut StepCtx<'_>, id: &str) -> Result<(), RtDbError> {
    let cancelled = scheduler::cancel_on(sctx.tx, sctx.db, id).await?;
    sctx.results.push(serde_json::json!({ "cancelled": cancelled }));
    Ok(())
}
```

Add `use crate::scheduler;` to txn.rs's imports if not already present. Update the module doc's step list (txn.rs:1-9) to include `Schedule`/`CancelSchedule` with a one-line note that they target the scheduler, not document tables.

- [ ] **Step 5: Unit test for the recursive counter**

In txn.rs's `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn count_steps_is_recursive() {
        let txn = Transaction {
            steps: vec![
                Step::Insert {
                    table: "t".to_string(),
                    doc: serde_json::Map::new(),
                },
                Step::Schedule {
                    when: crate::protocol::ScheduleWhen::AfterMs { ms: 1 },
                    txn: Box::new(Transaction {
                        steps: vec![
                            Step::Delete {
                                table: "t".to_string(),
                                id: "x".to_string(),
                            },
                            Step::Schedule {
                                when: crate::protocol::ScheduleWhen::RunAt { ms: 2 },
                                txn: Box::new(Transaction {
                                    steps: vec![Step::CancelSchedule {
                                        id: "j".to_string(),
                                    }],
                                }),
                            },
                        ],
                    }),
                },
            ],
        };
        assert_eq!(count_steps(&txn), 5);
    }
```

- [ ] **Step 6: Wire-corpus entries**

In `wire-corpus/wire-corpus.json`, inside the `client_messages` array (after the existing mutate entries at ~line 16), add:

```json
    { "type": "mutate", "mutId": "m4", "txn": { "steps": [ { "op": "schedule", "when": { "type": "afterMs", "ms": 60000 }, "txn": { "steps": [ { "op": "insert", "table": "workItems", "doc": { "title": "later" } } ] } } ] } },
    { "type": "mutate", "mutId": "m5", "txn": { "steps": [ { "op": "cancelSchedule", "id": "0199ab_cd" } ] } }
```

(Match the file's existing indentation/comma style exactly; `mutId` values must not collide with existing entries — check the file first.)

NOTE: after this step, the ts/rust/python corpus round-trip tests FAIL until Tasks 4-6 land their wire types. That is expected mid-sequence; verify this task with server tests only, and the full gate runs in Task 7.

- [ ] **Step 7: Verify**

```bash
make -C ~/Repos/par-rt-db dev-db-up
cd ~/Repos/par-rt-db/server && cargo test count_steps wire_corpus 2>&1 | tail -20; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/server && cargo test --test wire_corpus 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/server && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
```
Expected: all pass, clippy clean. Also `grep -n '\.table()' server/src server/tests` returns only `Option`-aware call sites.

- [ ] **Step 8: Commit**

```bash
git add server/src/txn.rs server/src/scheduler.rs wire-corpus/wire-corpus.json
git commit -m "feat(txn): Step::Schedule/CancelSchedule — atomic enqueue on the open sqlx tx (FM-28)"
```

---

### Task 2: Server integration tests — `schedule_step_test.rs`

**Files:**
- Create: `server/tests/schedule_step_test.rs`

**Interfaces:**
- Consumes: `common::{test_state, fresh_db, kanban_schema}` (`server/tests/common/mod.rs`; `fresh_db` pushes the kanban fixture with tables `projects` + `workItems`); `execute_txn(&pool, &db, &schema, &Transaction, &PrincipalCtx)`; `scheduler::{insert, cancel, list}`; `Step`, `Transaction`, `ScheduleWhen`.
- Consumes (fire tests): the `Committers` construction copied VERBATIM from `server/tests/scheduled_test.rs:324-340` (15 positional args — copy it, do not retype from memory) and the `warm_up_committer` pattern from scheduled_test.rs:235-245.

- [ ] **Step 1: Write the test file with all 8 spec scenarios**

Model the file on `scheduled_test.rs` (helpers) and `txn_test.rs` (direct `execute_txn` calls, `PrincipalCtx::bypass()`). `valid_project_doc()` in txn_test builds a kanban-valid `projects` doc (`name`, `status`, `tags`, `updatedAt` — check txn_test.rs for the exact helper and copy it or construct the JSON literal inline). Key shape for the fire tests:

```rust
async fn make_committers(state: &Arc<AppState>) -> Arc<Committers> {
    // COPY the 15-arg Committers construction from scheduled_test.rs:324-340
    // verbatim (pool, SubscriptionManager::new(), SchemaCache::new(),
    // OpFeed::new(64, 32), Arc<ArcSwap<HotConfig>> from common::test_hot(),
    // two bools, 60, 5000, Metrics::new(), Arc<quota::UsageCache::new()>,
    // 60, 0, String::new(), false) — only rename variables.
}

async fn warm_up(committers: &Arc<Committers>, db: &str) {
    committers
        .mutate(db, None, Transaction { steps: vec![] }, PrincipalCtx::bypass())
        .await
        .expect("warm up committer");
}

async fn poll_until(
    pool: &PgPool,
    db: &TestDb,
    schema: &SchemaDef,
    table: &str,
    want: usize,
    timeout: Duration,
) -> bool {
    // deadline loop: execute_query a `.take(100).collect()` on `table`,
    // sleep 50ms between attempts (model on scheduled_test.rs poll_for_n:252-294)
}
```

The 8 tests (spec §Testing, numbered as there):

1. `schedule_step_commits_atomically_with_writes` — txn `[Insert(projects doc), Schedule { when: RunAt { ms: now_ms() + 600_000 }, txn: [Insert(workItems doc)] }]`. Assert: outcome.results[1]["scheduleId"] is a string; `scheduler::list` shows one pending job; the projects doc is queryable.
2. `schedule_step_rolls_back_with_failed_txn` — txn `[Schedule { RunAt future }, ExpectVersion { table "projects", id "missing", version 1 }]`. `execute_txn` errs `NotFound`; `scheduler::list` is EMPTY (no orphan job).
3. `bad_when_rolls_back_writes` — txn `[Insert(projects), Schedule { when: AfterMs { ms: -1 } }]` ⇒ `BadRequest`, no doc, no job. Same for `Cron { expr: "not a cron" }`.
4. `cancel_schedule_step_result_and_idempotence` — Schedule step (future RunAt) → id from results; txn `[CancelSchedule { id }]` ⇒ results `[{"cancelled":true}]`; repeat ⇒ `{"cancelled":false}`; `scheduler::cancel` on the same id also returns false.
5. `cancel_step_commits_atomically_with_writes` — txn `[CancelSchedule { id: "no-such-job" }, Insert(projects)]` succeeds with results `[{"cancelled":false}, {"id":...}]` and the doc is durable.
6. `scoped_token_cannot_enqueue_forbidden_table` — `PrincipalCtx { user_id: None, email: None, tables: Some(vec!["projects".to_string()]) }`: (a) top-level Insert into `workItems` ⇒ `Forbidden` (pre-existing behavior, regression guard); (b) txn `[Insert(projects ok), Schedule { .., txn: [Insert(workItems)] }]` ⇒ `Forbidden` and NO doc, NO job (whole txn rolled back); (c) nested writing `projects` ⇒ Ok.
7. `recursive_step_budget` — top-level 513 Inserts into `projects` + 1 Schedule whose nested txn has 512 Inserts = 1026 recursive steps ⇒ `BadRequest` before any step executes (assert table empty via query). Counter-case: 511 + 1 Schedule(nested 512) = 1024 ⇒ Ok. (Build with `(0..513).map(|_| Step::Insert {...}).collect()`.)
8. `chained_schedule_fires_and_enqueues_follow_up` — with Committers + warm-up: a Schedule step (`RunAt { ms: 1 }`, past ⇒ immediate) whose nested txn is `[Insert(workItems A), Schedule { when: AfterMs { ms: 0 }, txn: [Insert(workItems B)] }]` via `committers.mutate`. Poll until BOTH docs exist (15s deadline) — proves the fire path executes a nested Schedule step that itself fires.

- [ ] **Step 2: Run them**

```bash
make -C ~/Repos/par-rt-db dev-db-up
cd ~/Repos/par-rt-db/server && cargo test --test schedule_step_test 2>&1 | tail -15; echo "EXIT=${PIPESTATUS[0]}"
```
Expected: 8 passed. If a fire test is flaky-slow, re-run once; a persistent failure is a code defect (R11: understand what the test tests before "fixing" it).

- [ ] **Step 3: Commit**

```bash
git add server/tests/schedule_step_test.rs
git commit -m "test(txn): Step::Schedule/CancelSchedule integration scenarios (FM-28)"
```

---

### Task 3: Enqueue-time table authorization on the standalone Schedule-op surfaces

**AUTH-TOUCHING — flag this commit for manual review in the session report.**

**Files:**
- Modify: `server/src/ws.rs` (`handle_schedule` :617-647)
- Modify: `server/src/http_api.rs` (`schedule_handler` :340-375)
- Modify: `server/src/admin/schedules.rs` (`admin_create_schedule` :56-71)
- Test: `server/tests/schedule_step_test.rs` (add HTTP-surface tests)

**Interfaces:**
- Consumes: `txn::authorize_txn_tables(ctx: &PrincipalCtx, txn: &Transaction) -> Result<(), RtDbError>` (Task 1); `Principal::row_ctx() -> PrincipalCtx` (auth/mod.rs:295-308).

- [ ] **Step 1: ws.rs `handle_schedule`**

Insert the recursive check between the read-only rejection and `resolve_when`. Restructure the `Ok(())` arm of the match (ws.rs:629-643) to:

```rust
        Ok(()) => {
            let prepared = authorize_txn_tables(&principal.row_ctx(), &txn)
                .and_then(|()| scheduler::resolve_when(when, now_ms()));
            match prepared {
                Ok((kind, due_at, cron)) => {
                    match scheduler::insert(&state.pool, db, kind, due_at, &txn, cron.as_deref())
                        .await
                    {
                        Ok(id) => ServerMessage::ScheduleOk { schedule_id, id },
                        Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                    }
                }
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            }
        }
```

(`when` is moved into `resolve_when` inside the closure — keep the original move semantics; the reply still goes out over `out_tx` unchanged.)

- [ ] **Step 2: http_api.rs `schedule_handler`**

After `check_http_rate_limits(...)` (http_api.rs:365), before `resolve_when`:

```rust
    // FM-28 tightening: a scoped machine token cannot smuggle a future write
    // into a table outside its allowlist via a scheduled job (matches the
    // per-step gate `execute_txn` applies at fire time — but fire time runs
    // as bypass, so enqueue time is the only scoped check).
    crate::txn::authorize_txn_tables(&principal.row_ctx(), &body.txn)?;
```

- [ ] **Step 3: admin/schedules.rs `admin_create_schedule`**

After the `database_exists` check (schedules.rs:68-69), before `resolve_when`:

```rust
    // Uniform with the other three enqueue paths (FM-28). Admin is a bypass
    // principal (`tables = None`) so this is a no-op today — it exists so
    // the four surfaces cannot drift if admin principals ever carry scopes.
    crate::txn::authorize_txn_tables(&PrincipalCtx::bypass(), &body.txn)?;
```

(Add the `PrincipalCtx` import if missing. Update the file-header comment at schedules.rs:1-4 to note enqueue-time table scoping now applies here too.)

- [ ] **Step 4: HTTP-surface scoped-token tests**

Append to `server/tests/schedule_step_test.rs` (pattern from admin_test.rs:1588-1608 — `spawn_app(state)` + `admin_post("/admin/mint-token", {"db", "name": "scoped", "tables": ["projects"], "readOnly": false})`, then call `POST /api/schedule` with `Authorization: Bearer <token>`; the route body is `{"db", "when": {"type":"runAt","ms": <future>}, "txn": {"steps":[...]}}`):

1. `scoped_token_cannot_schedule_forbidden_table_http` — token scoped to `projects`, txn inserts `workItems` ⇒ 403 `FORBIDDEN`, and `scheduler::list` is empty.
2. `scoped_token_can_schedule_allowed_table_http` — same token, txn inserts `projects` ⇒ 200, job listed; cancel it to clean up.
3. `admin_create_schedule_still_allows_cross_table` — `admin_post("/admin/db/{db}/schedules", ...)` with a cross-table txn ⇒ 200 (bypass no-op).

Use `reqwest` via the existing `admin_get/admin_post` helpers in common/mod.rs and a raw bearer post for `/api/schedule` (copy the pattern any existing http_api test uses for bearer calls; check `server/tests/` for one — e.g. the storage or schedule tests — and mirror it).

- [ ] **Step 5: Verify + commit**

```bash
cd ~/Repos/par-rt-db/server && cargo test --test schedule_step_test 2>&1 | tail -15; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/server && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
git add server/src/ws.rs server/src/http_api.rs server/src/admin/schedules.rs server/tests/schedule_step_test.rs
git commit -m "security(auth): recursive table-allowlist check at Schedule-op enqueue on all surfaces (FM-28)"
```

---

### Task 4: ts-client mirror

**Files:**
- Modify: `ts-client/src/protocol.ts` (`StepJson` union :163-190)
- Modify: `ts-client/src/mutation.ts` (`StepResult` union :17-47, `parseStepResult` :59-97, `TxnBuilder` :104-178)
- Test: `ts-client/tests/mutation.test.ts`

**Interfaces:**
- Consumes: the existing `ScheduleWhen` type (protocol.ts:116-120) and `TransactionJson` (protocol.ts:192-194).
- Produces: `TxnBuilder.prototype.schedule(when: ScheduleWhen, txn: TransactionJson): this` and `.cancelSchedule(id: string): this`; `StepResult` gains `{ scheduleId: string } | { cancelled: boolean }`.

- [ ] **Step 1: protocol.ts — extend `StepJson`**

Add to the union (match the file's exact generic-parameter style used by the neighboring arms):

```ts
  | { op: "schedule"; when: ScheduleWhen; txn: TransactionJson }
  | { op: "cancelSchedule"; id: string }
```

- [ ] **Step 2: mutation.ts — builder + results**

`TxnBuilder` methods (append-return-`this`, same as `insert`):

```ts
  schedule(when: ScheduleWhen, txn: TransactionJson): this {
    this.steps.push({ op: "schedule", when, txn });
    return this;
  }

  cancelSchedule(id: string): this {
    this.steps.push({ op: "cancelSchedule", id });
    return this;
  }
```

`StepResult` union gains (before the `null` member):

```ts
  | { scheduleId: string }
  | { cancelled: boolean }
```

`parseStepResult` gains two arms (follow the existing strict-parser shape at :59-97):

```ts
  if ("scheduleId" in s && typeof s.scheduleId === "string") return { scheduleId: s.scheduleId };
  if ("cancelled" in s && typeof s.cancelled === "boolean") return { cancelled: s.cancelled };
```

Import `ScheduleWhen` in mutation.ts if not already imported.

- [ ] **Step 3: tests**

In `tests/mutation.test.ts` (follow the existing builder-equality test style):

```ts
test("TxnBuilder schedule + cancelSchedule produce wire shapes", () => {
  const inner = new TxnBuilder()
    .insert("workItems", { title: "later", ...validWorkItemFields }) // match the fixture style of neighboring tests
    .build();
  const txn = new TxnBuilder()
    .schedule({ type: "afterMs", ms: 60000 }, inner)
    .cancelSchedule("j1")
    .build();
  expect(txn.steps).toEqual([
    { op: "schedule", when: { type: "afterMs", ms: 60000 }, txn: inner },
    { op: "cancelSchedule", id: "j1" },
  ]);
});

test("parseStepResult handles scheduleId and cancelled", () => {
  expect(parseStepResult({ scheduleId: "s1" })).toEqual({ scheduleId: "s1" });
  expect(parseStepResult({ cancelled: false })).toEqual({ cancelled: false });
});
```

(Use the file's actual import names/constructor; adjust the work-item fixture to whatever neighboring tests use so schema validation in tests passes — these are pure builder tests, no server involved.)

- [ ] **Step 4: Verify (typecheck is mandatory — green vitest is NOT typecheck) + commit**

```bash
cd ~/Repos/par-rt-db/ts-client && bun run typecheck 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/ts-client && bunx vitest run tests/mutation.test.ts tests/wire-corpus.test.ts tests/client.test.ts 2>&1 | tail -8; echo "EXIT=${PIPESTATUS[0]}"
git add ts-client/src/protocol.ts ts-client/src/mutation.ts ts-client/tests/mutation.test.ts
git commit -m "feat(ts-client): mirror Step::Schedule/CancelSchedule wire + builder (FM-28)"
```

The wire-corpus test now round-trips the two Task-1 corpus entries — it must pass here.

---

### Task 5: rust-client mirror

**Files:**
- Modify: `rust-client/src/mutation.rs` (Step enum :8-71, `StepResult` :82-97, `Mutation` builder :115-230)
- Test: `rust-client/src/mutation.rs` unit tests (`builder_serializes_all_step_kinds` :243-278)

**Interfaces:**
- Consumes: `ScheduleWhen` from `rust-client/src/wire.rs` (already exists for the `schedule` op).
- Produces: `Mutation::schedule(when: ScheduleWhen, txn: Transaction) -> Self`, `Mutation::cancel_schedule(id: impl Into<String>) -> Self`; `StepResult` gains the two shapes.

- [ ] **Step 1: Step enum variants (same serde attrs as the server)**

```rust
    Schedule {
        when: crate::wire::ScheduleWhen,
        txn: Box<Transaction>,
    },
    CancelSchedule {
        id: String,
    },
```

(If `ScheduleWhen` is re-exported from `mutation.rs`'s scope differently, import from wherever the file's existing types come from — check the top of mutation.rs.)

- [ ] **Step 2: builder methods (consuming-self, like `insert`)**

```rust
    /// Schedule `txn` to run later — `Step::Schedule` (FM-28).
    pub fn schedule(mut self, when: crate::wire::ScheduleWhen, txn: Transaction) -> Self {
        self.steps.push(Step::Schedule {
            when,
            txn: Box::new(txn),
        });
        self
    }

    /// Cancel a previously scheduled job — `Step::CancelSchedule` (FM-28).
    pub fn cancel_schedule(mut self, id: impl Into<String>) -> Self {
        self.steps.push(Step::CancelSchedule { id: id.into() });
        self
    }
```

- [ ] **Step 3: `StepResult` variants (untagged — order matters where keys overlap; these don't overlap any existing variant's keys)**

```rust
    Schedule {
        #[serde(rename = "scheduleId")]
        schedule_id: String,
    },
    Cancelled {
        cancelled: bool,
    },
```

(Place them BEFORE any catch-all-looking variant; the existing order constraint "Upsert before Insert" is untouched.)

- [ ] **Step 4: extend `builder_serializes_all_step_kinds` (:243-278)**

Append to the built mutation (mirroring the existing assertions):

```rust
        let m = Mutation::new()
            .schedule(
                ScheduleWhen::AfterMs { ms: 60_000 },
                Transaction { steps: vec![Step::Insert { table: "workItems".into(), doc: Self::obj(r#"{"title":"later"}"#) }] },
            )
            .cancel_schedule("j1")
            .build();
        assert_eq!(
            serde_json::to_value(&m.steps[0]).unwrap(),
            serde_json::json!({ "op": "schedule", "when": { "type": "afterMs", "ms": 60000 },
                "txn": { "steps": [ { "op": "insert", "table": "workItems", "doc": { "title": "later" } } ] } })
        );
        assert_eq!(
            serde_json::to_value(&m.steps[1]).unwrap(),
            serde_json::json!({ "op": "cancelSchedule", "id": "j1" })
        );
```

(Adapt to the test's actual builder construction idiom — check how the existing test builds and asserts; keep the wire-shape asserts.)

- [ ] **Step 5: Verify + commit**

```bash
cd ~/Repos/par-rt-db/rust-client && cargo test 2>&1 | tail -8; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/rust-client && cargo clippy --all-targets -- -D warnings 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
git add rust-client/src/mutation.rs
git commit -m "feat(rust-client): mirror Step::Schedule/CancelSchedule wire + builder (FM-28)"
```

`tests/wire_corpus.rs` round-trips the Task-1 corpus entries — it must pass here.

---

### Task 6: python-client mirror + in-memory harness

**Files:**
- Modify: `python-client/src/par_rt_db/mutation.py` (step models :44-153, `StepResult` :171-234, `_MutationBuilder` :237-336, `Mutation` namespace :339-352, `model_rebuild()` :358)
- Modify: `python-client/src/par_rt_db/in_memory.py` (`_execute_step` :1603-1690, `_execute_transaction` :1549-1601, `schedule()` :1979-2005, `_due_at_for` :2101-2108)
- Test: `python-client/tests/test_mutation.py`, `python-client/tests/test_in_memory.py`

**Interfaces:**
- Consumes: `ScheduleWhen` (wire.py:72-94, re-exported at package root); `_ScheduledJob`, `self._scheduled` (:560), `self._docs` (:553), harness `MAX_STEPS = 256` (:130).
- Produces: `Mutation.schedule(when, txn)`, `Mutation.cancel_schedule(id)` builder methods; `_Schedule`/`_CancelSchedule` step models; harness executes both in `tick()` semantics.

- [ ] **Step 1: mutation.py — step models (follow the `_Step(BaseModel)` camelCase `extra="forbid"` discriminator pattern)**

```python
class _Schedule(_Step):
    op: Literal["schedule"]
    when: ScheduleWhen
    txn: "Transaction"


class _CancelSchedule(_Step):
    op: Literal["cancelSchedule"]
    id: str
```

Register both in the discriminated step union wherever the 9 existing variants are registered (mutation.py :44-153 and the `Annotated[...]` union — find where `_Insert`…`_DeleteByQuery` are unioned), add builder methods to `_MutationBuilder` (append-return-self):

```python
    def schedule(self, when: ScheduleWhen, txn: "Transaction") -> "_MutationBuilder":
        self._steps.append(_Schedule(op="schedule", when=when, txn=txn))
        return self

    def cancel_schedule(self, id: str) -> "_MutationBuilder":
        self._steps.append(_CancelSchedule(op="cancelSchedule", id=id))
        return self
```

(Match the actual attribute name — `self._steps` vs `self.steps` — from the neighboring methods.) Expose them on the `Mutation` namespace (:339-352) the same way the existing methods are exposed. Keep `model_rebuild()` (:358) AFTER the new forward-ref-bearing definitions — pydantic needs it to resolve `"Transaction"` self-reference.

- [ ] **Step 2: mutation.py — `StepResult` union (richest-first, before `None`)**

```python
class _StepScheduleResult(BaseModel):
    scheduleId: str


class _StepCancelScheduleResult(BaseModel):
    cancelled: bool
```

Add both to the `StepResult` union (:171-234) ahead of the `None` member.

- [ ] **Step 3: in_memory.py harness — step execution + recursive budget**

In `_execute_step` (:1603-1690), add two arms (mirror how `schedule()` :1979-2005 computes `due_at`/`kind`/`cron` — if that logic is inline, extract a small `_prepare_job(when)` helper both call; the clock comes from the harness's injectable now, NOT `time.time()` directly — see `_new_clock_client` test :1248):

```python
        elif isinstance(step, _Schedule):
            kind, due_at, cron = self._prepare_job(step.when)
            job = _ScheduledJob(
                id=self._new_job_id(),  # however schedule() mints ids (injectable random)
                kind=kind,
                due_at=due_at,
                txn=step.txn,
                cron=cron,
            )
            self._scheduled.append(job)
            return {"scheduleId": job.id}
        elif isinstance(step, _CancelSchedule):
            before = len(self._scheduled)
            self._scheduled = [j for j in self._scheduled if j.id != step.id]
            return {"cancelled": len(self._scheduled) < before}
```

(Match `_ScheduledJob`'s actual constructor fields — read the dataclass at :560 area — and however `schedule()` builds one.) Add the recursive budget to `_execute_transaction` (:1549-1601), mirroring the server's recursive ruling on the harness's own cap:

```python
def _count_steps(txn: "Transaction") -> int:
    total = 0
    for s in txn.steps:
        total += 1
        if isinstance(s, _Schedule):
            total += _count_steps(s.txn)
    return total
```

…and check `_count_steps(txn) > MAX_STEPS` (the harness's 256) where the flat `len(steps)` check is today, raising the same ValueError/bad-request shape the harness already uses. The nested txn does NOT otherwise execute at enqueue — `tick()` (:2054-2093) fires it later via `_execute_transaction`, which re-validates.

- [ ] **Step 4: tests**

`test_mutation.py` (wire shapes + builder, following the existing style):

```python
def test_schedule_and_cancel_schedule_wire_shapes():
    inner = Mutation().insert("workItems", {"title": "later", ...}).build()
    m = Mutation().schedule(ScheduleWhen.afterMs(60_000), inner).cancel_schedule("j1").build()
    assert m.model_dump(by_alias=True, exclude_none=True) == {
        "steps": [
            {"op": "schedule", "when": {"type": "afterMs", "ms": 60000}, "txn": inner.model_dump(...)},
            {"op": "cancelSchedule", "id": "j1"},
        ]
    }
```

(Use the DSL's actual `ScheduleWhen` constructors / builder entry points — check how test_mutation.py and the schedule-op tests build `when` values today; match the dump-serialization idiom the neighboring step tests use.)

`test_in_memory.py` (use the `_new_clock_client` fixture :1248-1252 for deterministic time):

```python
def test_schedule_step_enqueues_job_fired_by_tick():
    # client.mutate([Insert, Schedule(afterMs 1000, txn=[Insert B])]) → job pending
    # advance clock past due, tick() → doc B exists; results[1]["scheduleId"] matches the listed job

def test_cancel_schedule_step_removes_pending_job():
    # mutate schedule → id; mutate cancel_schedule(id) → {"cancelled": True}; tick() → nothing fires; repeat → False

def test_recursive_step_budget_rejects_oversized_tree():
    # 300 inserts + 1 schedule(nested 300) > 256 → rejected before any write (docs unchanged)
```

- [ ] **Step 5: Verify + commit**

```bash
cd ~/Repos/par-rt-db/python-client && uv run pytest -q tests/test_mutation.py tests/test_in_memory.py tests/test_wire_parity.py 2>&1 | tail -5; echo "EXIT=${PIPESTATUS[0]}"
cd ~/Repos/par-rt-db/python-client && uv run pyright 2>&1 | tail -3; echo "EXIT=${PIPESTATUS[0]}"
git add python-client/src/par_rt_db/mutation.py python-client/src/par_rt_db/in_memory.py python-client/tests/test_mutation.py python-client/tests/test_in_memory.py
git commit -m "feat(python-client): mirror Step::Schedule/CancelSchedule + harness tick support (FM-28)"
```

`test_wire_parity.py` round-trips the Task-1 corpus entries — it must pass here.

---

### Task 7: Docs + full gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (row #28)
- Modify: `CLAUDE.md` (data-pipeline step list)
- Modify: server + client READMEs where step kinds are listed

**Interfaces:** None — documentation only, then the branch gate.

- [ ] **Step 1: FEATURE_MATRIX.md** — flip row #28 (Step::Schedule / scheduler-in-txn) to ✅ with client-mirror status (ts/rust/python all ✅ incl. in-memory harness).

- [ ] **Step 2: CLAUDE.md** — in the "Data pipeline" architecture bullet, extend the step list `Insert/Patch/Replace/Delete/ExpectVersion/ExpectAbsent/Upsert` + by-query sentence with `Schedule`/`CancelSchedule` and one line: a Schedule step inserts the `scheduled_txns` row on the open sqlx tx (atomic with the writes) after a recursive table-allowlist check; fire-time semantics unchanged.

- [ ] **Step 3: READMEs** — `grep -rn "PatchByQuery\|DeleteByQuery" --include="README.md"` across the repo; every README listing step kinds gains the two new steps (server DSL section, ts-client, rust-client, python-client). Do not touch READMEs that don't enumerate steps.

- [ ] **Step 4: Full gate (definition of done)**

```bash
make -C ~/Repos/par-rt-db dev-db-up
make -C ~/Repos/par-rt-db checkall 2>/dev/null; echo "EXIT=$?"   # (capture to a log file if long; verify the EXIT line, never a piped tail)
```

Requires `ts-client/dist` built (main checkout has it; if the dashboard typecheck fails on a missing client build, run `make -C ~/Repos/par-rt-db ts-client-build` first).

- [ ] **Step 5: Commit**

```bash
git add FEATURE_MATRIX.md CLAUDE.md <touched READMEs>
git commit -m "docs: Step::Schedule/CancelSchedule across FEATURE_MATRIX, CLAUDE.md, READMEs (FM-28)"
```

---

## Self-Review (completed at plan-writing time)

1. **Spec coverage** — wire shapes (Task 1), server execution + atomicity (Tasks 1-2), enqueue-time auth on all four surfaces incl. the standalone-op tightening (Tasks 1+3), recursive budget (Tasks 1+2 test 7, Task 6 harness), client mirrors + harness `tick()` (Tasks 4-6), corpus entries (Task 1 + per-client round-trips), docs (Task 7), fire-time semantics unchanged (no task — by design). Spec's test scenario 5 (scoped token, both surfaces) = Task 2 test 6 (step) + Task 3 Step 4 (standalone op). Covered.
2. **Placeholders** — the only "copy from file" instructions point at exact file:line ranges for verbatim transcription (Committers construction, `_ScheduledJob` fields) where re-typing from memory would be the placeholder risk; all new code is given in full.
3. **Type consistency** — `insert_on(conn: &mut PgConnection, db: &str, …)` used by `step_schedule` via `sctx.tx` (a `&mut PgConnection`) and `sctx.db` (added in Task 1 Step 4); `authorize_txn_tables(&PrincipalCtx, &Transaction)` signature identical at all four call sites; ts `schedule(when, txn)` / rust `schedule(when, txn)` / python `schedule(when, txn)` all take the client's existing `ScheduleWhen` + transaction types. Consistent.
