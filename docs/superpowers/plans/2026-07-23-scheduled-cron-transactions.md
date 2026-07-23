# Scheduled & Cron Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add one-shot scheduled transactions (`afterMs`/`runAt`) and recurring cron jobs (5-field, UTC) to par-rt-db, drained through the existing single-writer committer by a per-db timer task, with full lifecycle (list/pause/resume/cancel), mirrored across the server + TS + Rust clients.

**Architecture:** A per-db `scheduled_txns` side table (sibling of `mutations`) holds `(due_at, txn, cron, status)`. A dedicated scheduler task per db (spawned alongside the committer, holding a clone of its channel sender) sleeps until the next `due_at`, atomically claims due rows with `FOR UPDATE SKIP LOCKED`, and enqueues each as a fire-and-forget `CommitterRequest::RunScheduled`. The committer executes the job via the existing `execute_txn` + `fan_out` path (so the load-bearing single-writer invariant is untouched — the scheduler only ever writes the side table) and finalizes the row (delete one-shot, reschedule cron, mark error on failure). Clients send `Schedule`/`Cancel`/`Pause`/`Resume`/`ListSchedules` over WS and parallel HTTP routes. Delivery is at-least-once; one-shot catches up, cron skips missed windows.

**Tech Stack:** Rust (axum/tokio/sqlx/Postgres 17), `croner` + `chrono` for cron parsing/UTC math; TypeScript SDK (bun/vitest); Rust client crate.

## Global Constraints

(Copied from the spec and `CLAUDE.md` project invariants — every task inherits these.)

- **Single-writer invariant:** `execute_txn` runs READ COMMITTED with no row locking. It is **only** ever called inside the per-db committer task. Scheduled jobs execute via a `CommitterRequest::RunScheduled` arm — never call `execute_txn` from the scheduler or a transport directly. The scheduler's writes are confined to the `scheduled_txns` side table.
- **SQL construction:** double-quote every identifier; bind every value via `$n`; never interpolate an unvalidated value. Physical names lowercased + length-capped (existing `ddl.rs`/`db.rs` helpers). Reuse `db::{now_ms, new_id, validate_db_name}` and `ddl::pg_schema`.
- **Errors:** every failure is the `RtDbError` envelope `{code, message}`. Client-facing 500s carry a generic message — never stringify a sqlx/serde error into the body (log via `tracing`). Use `fetch_optional` for lookups that can miss.
- **Wire contract is byte-identical across three implementations:** `server/src/protocol.rs`, `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`. Non-uniform casing is load-bearing — match the existing tags/fields exactly. `ClientMessage` uses `#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase", deny_unknown_fields)]`.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Definition of done:** `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests). Server integration tests need `make dev-db-up` (or use `make test`).
- **Cron semantics:** 5-field standard Vixie-cron, **min-first** (`*/5 * * * *` = every 5 *minutes*, not seconds), evaluated in **UTC**. Task 1's guard test enforces this regardless of crate.
- **Auth:** all schedule/manage/list ops call the existing `authorize(&pool, principal, db)` (uniform across read/write today — same call query/mutate use). WS re-runs it on every op.

---

## File Structure

**Server (created/modified):**
- `server/Cargo.toml` — add `chrono`, `croner`.
- `server/src/lib.rs` — `pub mod scheduler;`
- `server/src/scheduler.rs` (NEW) — side-table access + the timer task. One responsibility: everything about scheduled jobs.
- `server/src/db.rs` — `scheduled_txns` table + index in `create_database`.
- `server/src/committer.rs` — `CommitterRequest::RunScheduled` arm + `handle_scheduled`; spawn the scheduler in `channel_for`.
- `server/src/protocol.rs` — `ScheduleWhen`, `ScheduleInfo`, new `ClientMessage`/`ServerMessage` variants.
- `server/src/ws.rs` — handler arms for the 5 new client messages.
- `server/src/http_api.rs` — `/api/schedule`, `/api/schedule/:id/{cancel,pause,resume}`, `/api/schedules`.
- `server/tests/scheduled_test.rs` (NEW) — integration tests.

**TS client:** `ts-client/src/protocol.ts`, `client.ts`, `http.ts`, `in_memory.ts`, `index.ts`; tests `schedule.test.ts`, `client.test.ts`, `http.test.ts`, `in_memory.test.ts`.

**Rust client:** `rust-client/src/wire.rs`, `http.rs`, `ws.rs`, `lib.rs`; tests.

**Docs:** `FEATURE_MATRIX.md`, server/ts-client/rust-client READMEs, `CLAUDE.md`.

---

## Task 1: Dependencies + cron `next_fire` helper

**Files:**
- Modify: `server/Cargo.toml`
- Create: `server/src/scheduler.rs`
- Modify: `server/src/lib.rs`

**Interfaces:**
- Produces: `scheduler::next_fire(expr: &str, now_ms: i64) -> Result<i64, RtDbError>` — returns the next fire time as UTC epoch ms strictly after `now_ms`. Used by the WS/HTTP handlers (to validate + set initial `due_at`) and the committer's cron finalize.

- [ ] **Step 1: Add dependencies**

Append to `server/Cargo.toml` `[dependencies]`:

```toml
chrono = { version = "0.4", default-features = false, features = ["std"] }
croner = "2"
```

> `croner` parses standard 5-field Vixie-cron expressions **min-first** (unlike the `cron` crate, which is seconds-first by default). `chrono` provides `DateTime<Utc>` conversion (no `clock` feature needed — we always pass explicit instants). If the pinned `croner` major exposes a different API than `Cron::new(...)?.find_next_occurrence(&dt)?`, adapt the body of `next_fire` — the guard test below is what locks the semantics.

- [ ] **Step 2: Register the module**

In `server/src/lib.rs`, add alongside the other `pub mod` lines (keep alphabetical/order consistent with neighbors):

```rust
pub mod scheduler;
```

- [ ] **Step 3: Write the failing test**

Create `server/src/scheduler.rs`:

```rust
//! Per-database scheduled/cron transaction store + timer. Jobs are *data*
//! (a declarative `Transaction` plus a `due_at`), not code — the scheduler
//! drains due rows through the single-writer committer, which executes them
//! via the normal `execute_txn` path. See
//! `docs/superpowers/specs/2026-07-23-scheduled-cron-transactions-design.md`.

use crate::db::now_ms;
use crate::error::RtDbError;

/// Computes the next fire time (UTC epoch ms) for a 5-field cron expression,
/// strictly after `now_ms`. Also validates the expression: a parse failure or
/// an expression with no future fire times is `BadRequest`.
pub fn next_fire(expr: &str, now_ms: i64) -> Result<i64, RtDbError> {
    use chrono::{DateTime, Utc};
    let cron = croner::Cron::new(expr)
        .map_err(|_| RtDbError::bad_request("invalid cron expression"))?;
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms)
        .ok_or_else(|| RtDbError::internal("invalid timestamp"))?;
    let next = cron
        .find_next_occurrence(&now, false)
        .map_err(|_| RtDbError::bad_request("cron expression has no future fire times"))?;
    Ok(next.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-07-23T12:00:00Z = 1784481600000 ms. A fixed anchor so the
    /// minute/hour/day math is deterministic.
    const ANCHOR_MS: i64 = 1_784_481_600_000;

    #[test]
    fn every_5_minutes_not_seconds() {
        // `*/5 * * * *` must mean every 5 MINUTES (min-first), not every 5
        // seconds. The next fire after 12:00:00Z is 12:05:00Z = +300000 ms.
        let next = next_fire("*/5 * * * *", ANCHOR_MS).unwrap();
        assert_eq!(next - ANCHOR_MS, 300_000);
    }

    #[test]
    fn weekdays_at_9am_from_thursday() {
        // 2026-07-23 is a Thursday. `0 9 * * 1-5` next fires 2026-07-24 09:00Z.
        let next = next_fire("0 9 * * 1-5", ANCHOR_MS).unwrap();
        assert_eq!(next - ANCHOR_MS, 21 * 3600 * 1000); // +21h → Fri 09:00
    }

    #[test]
    fn rejects_garbage() {
        assert!(next_fire("not a cron", ANCHOR_MS).is_err());
    }

    #[test]
    fn next_is_strictly_after_now() {
        let next = next_fire("* * * * *", ANCHOR_MS).unwrap();
        assert!(next > ANCHOR_MS);
    }

    #[test]
    fn now_ms_is_available() {
        // Sanity: the helper imports compile against the real clock helper.
        let _ = now_ms();
    }
}
```

- [ ] **Step 4: Run the test to verify it passes (and pins the crate semantics)**

Run: `cd server && cargo test --lib scheduler`
Expected: PASS (4 tests). If `every_5_minutes_not_seconds` fails with `next - ANCHOR_MS == 5000`, the crate is seconds-first — switch crate or prepend `"0 "` to make it 6-field min-first and re-run. Do not proceed with a seconds-first parser.

- [ ] **Step 5: Commit**

```bash
git add server/Cargo.toml server/Cargo.lock server/src/lib.rs server/src/scheduler.rs
git commit -m "feat(server): cron next_fire helper + scheduler module skeleton"
```

---

## Task 2: `scheduled_txns` table + side-table access functions

**Files:**
- Modify: `server/src/db.rs` (the `create_database` function, ~lines 118–162)
- Modify: `server/src/scheduler.rs`

**Interfaces:**
- Consumes: `db::{now_ms, new_id, validate_db_name}`, `ddl::pg_schema`, `txn::Transaction`, `error::RtDbError`.
- Produces:
  - `scheduler::ensure_table(pool, db) -> Result<(), RtDbError>`
  - `scheduler::insert(pool, db, kind, due_at, txn: &Transaction, cron: Option<&str>) -> Result<String, RtDbError>` — returns the new job `id`.
  - `scheduler::list(pool, db) -> Result<Vec<ScheduleInfo>, RtDbError>`
  - `scheduler::cancel(pool, db, id) -> Result<bool, RtDbError>` — true if a row was deleted.
  - `scheduler::set_paused(pool, db, id, paused: bool) -> Result<bool, RtDbError>` — pause/resume; true if a row was updated.
  - `scheduler::reset_running(pool, db) -> Result<u64, RtDbError>` — crash recovery: `running`→`pending`.
  - `scheduler::next_due(pool, db) -> Result<Option<i64>, RtDbError>` — min `due_at` among `pending` rows.
  - `scheduler::claim_due(pool, db, now: i64, batch: usize) -> Result<Vec<ClaimedJob>, RtDbError>`
  - `scheduler::finalize_one_shot_done(pool, db, id) -> Result<(), RtDbError>` — DELETE.
  - `scheduler::finalize_cron_next(pool, db, id, next_due: i64) -> Result<(), RtDbError>`
  - `scheduler::mark_error(pool, db, id, msg: &str) -> Result<(), RtDbError>`
  - `scheduler::ClaimedJob { id, kind, txn, cron }`, `scheduler::ScheduleInfo` (re-exported from `protocol` — see Task 4; for now define a local struct and Task 4 swaps it for the wire type).

- [ ] **Step 1: Add the table to `create_database`**

In `server/src/db.rs`, inside `create_database`'s `tx` transaction, immediately after the `mutations` table `CREATE TABLE` block (~line 151) and before the `INSERT INTO rtdb_auth.databases`, add:

```rust
    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".scheduled_txns (
            id          text PRIMARY KEY,
            kind        text NOT NULL,
            due_at      bigint NOT NULL,
            txn         jsonb NOT NULL,
            cron        text,
            status      text NOT NULL,
            last_error  text,
            created_at  bigint NOT NULL,
            fired_count bigint NOT NULL DEFAULT 0
        )"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX \"{schema_name}_scheduled_due_idx\"
         ON \"{schema_name}\".scheduled_txns (status, due_at)"
    ))
    .execute(&mut *tx)
    .await?;
```

- [ ] **Step 2: Implement the side-table functions in `scheduler.rs`**

Append to `server/src/scheduler.rs` (below `next_fire`):

```rust
use sqlx::PgPool;

use crate::ddl::pg_schema;
use crate::txn::Transaction;

/// Cap on how many due jobs one claim sweep takes. Bounded so a flood of
/// past-due one-shots can't pin the committer channel indefinitely.
pub const CLAIM_BATCH: i64 = 64;

/// A row claimed for execution by `claim_due`. The scheduler hands the full
/// payload to the committer so the committer never has to re-read the row to
/// execute or finalize it.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    pub id: String,
    pub kind: String, // "oneshot" | "cron"
    pub txn: Transaction,
    pub cron: Option<String>,
}

/// Local mirror of the wire `ScheduleInfo` (Task 4 promotes this to the public
/// protocol type; until then it is module-private so list has a return type).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInfo {
    pub id: String,
    pub kind: String,
    pub due_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub fired_count: i64,
}

/// `CREATE TABLE IF NOT EXISTS` for databases that predate this feature.
/// Mirrors `mutation_log::ensure_table`; called once at committer startup.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".scheduled_txns (
            id          text PRIMARY KEY,
            kind        text NOT NULL,
            due_at      bigint NOT NULL,
            txn         jsonb NOT NULL,
            cron        text,
            status      text NOT NULL,
            last_error  text,
            created_at  bigint NOT NULL,
            fired_count bigint NOT NULL DEFAULT 0
        )"
    ))
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX IF NOT EXISTS \"{schema}_scheduled_due_idx\"
         ON \"{schema}\".scheduled_txns (status, due_at)"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert(
    pool: &PgPool,
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
    .execute(pool)
    .await?;
    Ok(id)
}

pub async fn list(pool: &PgPool, db: &str) -> Result<Vec<ScheduleInfo>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, String, i64, Option<String>, String, Option<String>, i64, i64)> =
        sqlx::query_as(&format!(
            "SELECT id, kind, due_at, cron, status, last_error, created_at, fired_count
             FROM \"{schema}\".scheduled_txns ORDER BY due_at"
        ))
        .fetch_all(pool)
        .await?;
    Ok(rows
        .into_iter()
        .map(|(id, kind, due_at, cron, status, last_error, created_at, fired_count)| ScheduleInfo {
            id,
            kind,
            due_at,
            cron,
            status,
            last_error,
            created_at,
            fired_count,
        })
        .collect())
}

pub async fn cancel(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "DELETE FROM \"{schema}\".scheduled_txns WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Pause (`paused=true`) or resume (`paused=false`) a job. Resuming a cron job
/// recomputes `due_at` to the next fire after now; resuming a one-shot leaves
/// its `due_at` alone. Returns true if a row was updated.
pub async fn set_paused(
    pool: &PgPool,
    db: &str,
    id: &str,
    paused: bool,
) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = if paused {
        sqlx::query(&format!(
            "UPDATE \"{schema}\".scheduled_txns SET status = 'paused'
             WHERE id = $1 AND status = 'pending'"
        ))
        .bind(id)
        .execute(pool)
        .await?
    } else {
        // Resume: recompute next fire for cron; one-shot keeps its due_at.
        let row: Option<(String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT kind, cron FROM \"{schema}\".scheduled_txns
             WHERE id = $1 AND status = 'paused'"
        ))
        .bind(id)
        .fetch_optional(pool)
        .await?;
        match row {
            Some(("cron", Some(expr))) => {
                let next = next_fire(&expr, now_ms())?;
                sqlx::query(&format!(
                    "UPDATE \"{schema}\".scheduled_txns
                     SET status = 'pending', due_at = $2, last_error = NULL
                     WHERE id = $1"
                ))
                .bind(id)
                .bind(next)
                .execute(pool)
                .await?
            }
            Some(_) => {
                sqlx::query(&format!(
                    "UPDATE \"{schema}\".scheduled_txns SET status = 'pending'
                     WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?
            }
            None => return Ok(false),
        }
    };
    Ok(res.rows_affected() > 0)
}

/// Crash recovery: any `running` row was orphaned by a task that died mid-fire.
/// Reset to `pending` so it re-fires (at-least-once).
pub async fn reset_running(pool: &PgPool, db: &str) -> Result<u64, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns SET status = 'pending'
         WHERE status = 'running'"
    ))
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Min `due_at` among `pending` rows, or `None` if the table has nothing due.
/// `MIN(due_at)` is SQL `NULL` when no rows match, which sqlx deserializes as
/// `Option<i64> = None`, so this naturally returns `None` for an empty table.
pub async fn next_due(pool: &PgPool, db: &str) -> Result<Option<i64>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Option<i64>,)> = sqlx::query_as(&format!(
        "SELECT MIN(due_at) FROM \"{schema}\".scheduled_txns WHERE status = 'pending'"
    ))
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(m,)| m))
}

/// Atomically claims up to `batch` due rows: `pending`+`due_at <= now` →
/// `running`. `FOR UPDATE SKIP LOCKED` makes the claim safe even if a second
/// claimer ever exists (today there is exactly one scheduler per db).
pub async fn claim_due(
    pool: &PgPool,
    db: &str,
    now: i64,
    batch: i64,
) -> Result<Vec<ClaimedJob>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, String, serde_json::Value, Option<String>)> = sqlx::query_as(
        &format!(
            "UPDATE \"{schema}\".scheduled_txns SET status = 'running'
             WHERE id IN (
                 SELECT id FROM \"{schema}\".scheduled_txns
                 WHERE status = 'pending' AND due_at <= $1
                 ORDER BY due_at LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, kind, txn, cron"
        ),
    )
    .bind(now)
    .bind(batch)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(id, kind, txn_json, cron)| {
            let txn: Transaction = serde_json::from_value(txn_json).map_err(|err| {
                tracing::error!(error = %err, db, %id, "failed to deserialize scheduled txn");
                RtDbError::internal("failed to read scheduled txn")
            })?;
            Ok(ClaimedJob { id, kind, txn, cron })
        })
        .collect()
}

pub async fn finalize_one_shot_done(pool: &PgPool, db: &str, id: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!("DELETE FROM \"{schema}\".scheduled_txns WHERE id = $1"))
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn finalize_cron_next(
    pool: &PgPool,
    db: &str,
    id: &str,
    next_due: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns
         SET status = 'pending', due_at = $2, fired_count = fired_count + 1, last_error = NULL
         WHERE id = $1"
    ))
    .bind(id)
    .bind(next_due)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_error(pool: &PgPool, db: &str, id: &str, msg: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns SET status = 'error', last_error = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(msg)
    .execute(pool)
    .await?;
    Ok(())
}
```

- [ ] **Step 3: Write the failing integration test**

Create `server/tests/scheduled_test.rs`. Use the existing test harness pattern from another integration test (e.g. `server/tests/txn_test.rs`) for pool setup + per-test unique db creation. Read that file first to copy the helpers verbatim (`make dev-db-up` must be running).

```rust
use rtdb_server::scheduler;

// Copy the pool/spawn helpers (unique `t<uuid>` db name per test, create via
// the admin/db path used by txn_test.rs) from tests/txn_test.rs.

#[tokio::test]
async fn insert_list_cancel_roundtrip() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();

    let txn = rtdb_server::txn::Transaction { steps: vec![] };
    let id = scheduler::insert(&pool, &db, "oneshot", 123, &txn, None)
        .await
        .unwrap();
    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, id);
    assert_eq!(listed[0].kind, "oneshot");
    assert_eq!(listed[0].status, "pending");
    assert_eq!(listed[0].due_at, 123);

    assert!(scheduler::cancel(&pool, &db, &id).await.unwrap());
    assert!(scheduler::list(&pool, &db).await.unwrap().is_empty());
    assert!(!scheduler::cancel(&pool, &db, &id).await.unwrap()); // already gone
}

#[tokio::test]
async fn pause_resume_cron_recomputes_due() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = rtdb_server::txn::Transaction { steps: vec![] };
    let id = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"))
        .await
        .unwrap();

    assert!(scheduler::set_paused(&pool, &db, &id, true).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, "paused");
    // A paused job must not be claimed even if due_at is in the past.
    let claimed = scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    assert!(claimed.is_empty());

    assert!(scheduler::set_paused(&pool, &db, &id, false).await.unwrap());
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.status, "pending");
    assert!(info.due_at > 1); // recomputed forward from now
}

#[tokio::test]
async fn claim_due_and_finalize() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = rtdb_server::txn::Transaction { steps: vec![] };
    let one = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None).await.unwrap();
    let cron = scheduler::insert(&pool, &db, "cron", 1, &txn, Some("*/5 * * * *"))
        .await
        .unwrap();

    let claimed = scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 2);

    scheduler::finalize_one_shot_done(&pool, &db, &one).await.unwrap();
    let next = scheduler::next_fire("*/5 * * * *", scheduler_now()).unwrap();
    scheduler::finalize_cron_next(&pool, &db, &cron, next).await.unwrap();

    let listed = scheduler::list(&pool, &db).await.unwrap();
    assert_eq!(listed.len(), 1); // one-shot deleted, cron remains
    assert_eq!(listed[0].id, cron);
    assert_eq!(listed[0].fired_count, 1);
    assert_eq!(listed[0].status, "pending");
}

#[tokio::test]
async fn reset_running_recovers_orphans() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    scheduler::ensure_table(&pool, &db).await.unwrap();
    let txn = rtdb_server::txn::Transaction { steps: vec![] };
    let id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None).await.unwrap();
    // Simulate a crash mid-fire: the committer claimed but never finalized.
    scheduler::claim_due(&pool, &db, i64::MAX, scheduler::CLAIM_BATCH).await.unwrap();
    let n = scheduler::reset_running(&pool, &db).await.unwrap();
    assert_eq!(n, 1);
    assert_eq!(scheduler::list(&pool, &db).await.unwrap()[0].status, "pending");
}
```

(`scheduler_now()` is a thin test helper returning `now_ms()`; or call `rtdb_server::db::now_ms()` directly.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `make dev-db-up && cd server && cargo test --test scheduled_test`
Expected: PASS (4 tests). If `next_due`/`MIN` compile flags: the `Option<i64>` tuple deserialization of `MIN(...) AS bigint` — if sqlx needs `Option<i64>` it is already handled; if it returns the value directly when rows exist, adjust to `(Option<i64>,)`.

- [ ] **Step 5: Commit**

```bash
git add server/src/db.rs server/src/scheduler.rs server/tests/scheduled_test.rs
git commit -m "feat(server): scheduled_txns table + side-table accessors"
```

---

## Task 3: Committer `RunScheduled` arm + scheduler timer task

**Files:**
- Modify: `server/src/committer.rs`
- Modify: `server/src/scheduler.rs`

**Interfaces:**
- Consumes: Task 2's `claim_due`, `finalize_*`, `mark_error`, `next_fire`, `reset_running`, `next_due`; Task 1's `next_fire`.
- Produces: `CommitterRequest::RunScheduled { id, kind, txn, cron }` (fire-and-forget — no reply); `scheduler::run_scheduler(pool, db, committer_tx)` — the per-db timer loop, spawned in `Committers::channel_for`.

- [ ] **Step 1: Add the `RunScheduled` request variant**

In `server/src/committer.rs`, extend `CommitterRequest`:

```rust
pub enum CommitterRequest {
    Mutate {
        idempotency_key: Option<String>,
        txn: Transaction,
        reply: oneshot::Sender<Result<TxnOutcome, RtDbError>>,
    },
    Subscribe {
        conn: ConnId,
        query_id: String,
        query: Box<Query>,
        tx: UnboundedSender<ServerMessage>,
        reply: oneshot::Sender<Result<(), RtDbError>>,
    },
    /// A scheduled/cron job whose `due_at` arrived. Fire-and-forget: the
    /// scheduler does not wait for a reply (it would only stall the timer).
    /// The committer executes the txn through the normal `execute_txn` path
    /// and finalizes the row.
    RunScheduled {
        id: String,
        kind: String,
        txn: Box<Transaction>,
        cron: Option<String>,
    },
}
```

- [ ] **Step 2: Handle `RunScheduled` in the committer loop**

In `run_committer`'s `while let Some(req) = rx.recv().await` match (after `CommitterRequest::Subscribe`), add:

```rust
            CommitterRequest::RunScheduled {
                id,
                kind,
                txn,
                cron,
            } => {
                if let Err(err) = handle_scheduled(&ctx, id, kind, *txn, cron).await {
                    tracing::error!(db = %ctx.db, error = %err, "scheduled job handling failed");
                }
            }
```

Add the handler (it mirrors `handle_mutate`'s write path but finalizes the row and is best-effort about finalize failures — a finalize error must never turn a successful write into a problem, same stance as `mutation_log::store`):

```rust
/// Executes one claimed scheduled job through the normal write path and
/// finalizes its row. Best-effort finalize: the txn has already committed +
/// fanned out by the time we touch the row again, so a finalize failure is
/// logged, not propagated. `at-least-once` recovery handles the rare crash
/// window between commit and finalize.
async fn handle_scheduled(
    ctx: &CommitterCtx,
    id: String,
    kind: String,
    txn: Transaction,
    cron: Option<String>,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, "schema load failed").await;
            return Err(err);
        }
    };
    match execute_txn(&ctx.pool, &ctx.db, &schema, &txn).await {
        Ok(outcome) => {
            ctx.subs
                .fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set)
                .await;
            let finalize = match kind.as_str() {
                "oneshot" => scheduler::finalize_one_shot_done(&ctx.pool, &ctx.db, &id).await,
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            scheduler::finalize_cron_next(&ctx.pool, &ctx.db, &id, next).await
                        }
                        Err(err) => {
                            scheduler::mark_error(&ctx.pool, &ctx.db, &id, &err.message).await
                        }
                    },
                    None => scheduler::mark_error(&ctx.pool, &ctx.db, &id, "cron job missing expr").await,
                },
                other => scheduler::mark_error(&ctx.pool, &ctx.db, &id, &format!("unknown kind {other}")).await,
            };
            if let Err(err) = finalize {
                tracing::error!(db = %ctx.db, %id, error = %err, "scheduled job finalize failed");
            }
        }
        Err(err) => {
            // Execution failed (precondition/step error). No retry (see spec):
            // one-shot records error and stops; cron logs and reschedules.
            let msg = err.message.clone();
            match kind.as_str() {
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            let _ = scheduler::finalize_cron_next(&ctx.pool, &ctx.db, &id, next).await;
                            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                        }
                        Err(_) => {
                            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                        }
                    },
                    None => {
                        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                    }
                },
                _ => {
                    let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                }
            }
        }
    }
    Ok(())
}
```

Add `use crate::db::now_ms;` and `use crate::scheduler;` to committer.rs imports (alongside `use crate::mutation_log;`). Note `RtDbError.message` is the public field — confirm the field name in `error.rs` (it is `message`); if it is private, use the existing accessor.

- [ ] **Step 3: Implement the timer task in `scheduler.rs`**

Append:

```rust
use tokio::sync::mpsc::Sender;
use tokio::time::{Duration, timeout};

use crate::committer::CommitterRequest;

/// Maximum sleep between wakes. Bounds the latency of a job inserted with a
/// sooner `due_at` than the current sleep target (the loop re-reads the min
/// `due_at` each wake, so this only costs an occasional early wake).
const MAX_SLEEP: Duration = Duration::from_secs(2);

/// The per-db scheduler loop. Owns recovery on start, then repeatedly: read
/// the nearest due time, sleep until it (capped), claim due rows, and enqueue
/// each as a fire-and-forget `RunScheduled` on the committer channel. Exits
/// when the committer channel closes (its task died) — the next request to
/// this db respawns both.
pub async fn run_scheduler(pool: PgPool, db: String, committer_tx: Sender<CommitterRequest>) {
    if let Err(err) = ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: ensure_table failed");
    }
    if let Err(err) = reset_running(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: reset_running failed");
    }
    loop {
        let sleep_target = match next_due(&pool, &db).await {
            Ok(Some(due_at)) => {
                let now = now_ms();
                if due_at <= now {
                    Duration::ZERO
                } else {
                    Duration::from_millis((due_at - now) as u64)
                }
            }
            Ok(None) => MAX_SLEEP, // nothing pending
            Err(err) => {
                tracing::error!(db = %db, error = %err, "scheduler: next_due failed");
                MAX_SLEEP
            }
        };
        let sleep = sleep_target.min(MAX_SLEEP);
        if !sleep.is_zero() {
            // Bound the sleep so a shutdown/respawn can't hang the task.
            let _ = timeout(MAX_SLEEP, tokio::time::sleep(sleep)).await;
        }
        let now = now_ms();
        let claimed = match claim_due(&pool, &db, now, CLAIM_BATCH).await {
            Ok(jobs) => jobs,
            Err(err) => {
                tracing::error!(db = %db, error = %err, "scheduler: claim_due failed");
                continue;
            }
        };
        for job in claimed {
            let req = CommitterRequest::RunScheduled {
                id: job.id,
                kind: job.kind,
                txn: Box::new(job.txn),
                cron: job.cron,
            };
            if committer_tx.send(req).await.is_err() {
                // Committer task is gone; this scheduler is now useless.
                tracing::warn!(db = %db, "scheduler: committer channel closed, exiting");
                return;
            }
        }
    }
}
```

> The scheduler imports `crate::committer::CommitterRequest` and `committer.rs` imports `crate::scheduler` — this is a **circular module reference**, which Rust allows (they are separate modules), but confirm `committer.rs`'s `CommitterRequest` is `pub` (it is currently `pub enum`). No rebuild-order problem; both are in the same crate.

- [ ] **Step 4: Spawn the scheduler alongside the committer**

In `Committers::channel_for` (`committer.rs`), right after the `tokio::spawn(run_committer(...))` call and before `guard.insert(...)`, spawn the scheduler with a clone of the brand-new sender:

```rust
        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        tokio::spawn(run_committer(
            self.pool.clone(),
            db.to_string(),
            self.subs.clone(),
            self.schemas.clone(),
            rx,
        ));
        tokio::spawn(scheduler::run_scheduler(
            self.pool.clone(),
            db.to_string(),
            tx.clone(),
        ));
        guard.insert(db.to_string(), tx.clone());
        Ok(tx)
```

- [ ] **Step 5: Write the end-to-end firing test**

Append to `server/tests/scheduled_test.rs`. This needs a schema/table to observe the effect. Read `server/tests/txn_test.rs` for the `push_schema` + `Committers` setup helper pattern and reuse it. The committer (and thus scheduler) is lazily spawned on first `submit`/`mutate`/`subscribe`.

```rust
use rtdb_server::committer::{Committers, CommitterRequest};
use rtdb_server::txn::{Step, Transaction};
use rtdb_server::query::{Query, execute_query};
use rtdb_server::subs::SubscriptionManager;
use std::sync::Arc;
use std::collections::BTreeSet;
use tokio::sync::oneshot;

#[tokio::test]
async fn one_shot_fires_and_writes() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await; // table "items" with indexed "n"
    let subs = Arc::new(SubscriptionManager::new());
    let schemas = rtdb_server::db::SchemaCache::new();
    let committers = Committers::new(pool.clone(), subs, schemas);

    // Schedule a one-shot due in the past so it fires immediately.
    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "items".to_string(),
            doc: serde_json::json!({ "n": 42 }).as_object().unwrap().clone(),
        }],
    };
    let id = scheduler::insert(&pool, &db, "oneshot", 1, &txn, None)
        .await
        .unwrap();

    // Spawn the committer+scheduler by submitting any request.
    warm_up_committer(&committers, &db).await;

    // Poll for the write to appear (at-least-once, fires within ~MAX_SLEEP).
    let appeared = poll_for_count(&pool, &db, &schema, 42, Duration::from_secs(5)).await;
    assert!(appeared, "scheduled one-shot never wrote");

    // The one-shot row is gone after firing.
    assert!(scheduler::list(&pool, &db).await.unwrap().is_empty());
    drop(id);
}

#[tokio::test]
async fn cron_fires_repeatedly() {
    let pool = test_pool().await;
    let db = unique_db(&pool).await;
    let schema = push_simple_schema(&pool, &db).await;
    let subs = Arc::new(SubscriptionManager::new());
    let schemas = rtdb_server::db::SchemaCache::new();
    let committers = Committers::new(pool.clone(), subs, schemas);

    // `* * * * *` = every minute. Schedule due in the past; it fires once now,
    // then reschedules to the next minute. We assert it fires at least once and
    // the row remains pending with fired_count >= 1.
    let txn = /* insert into items with n = 7 */;
    scheduler::insert(&pool, &db, "cron", 1, &txn, Some("* * * * *"))
        .await
        .unwrap();
    warm_up_committer(&committers, &db).await;
    let appeared = poll_for_count(&pool, &db, &schema, 7, Duration::from_secs(5)).await;
    assert!(appeared);
    let info = &scheduler::list(&pool, &db).await.unwrap()[0];
    assert_eq!(info.kind, "cron");
    assert!(info.fired_count >= 1);
    assert_eq!(info.status, "pending");
}
```

Implement the helpers (`warm_up_committer` sends a no-op `Mutate` with an empty txn via `committers.mutate`; `poll_for_count` loops `execute_query` for the doc with that `n` until found or timeout; `push_simple_schema` pushes a one-field table — copy the exact helper from `txn_test.rs`). Use real code from `txn_test.rs`; do not invent the schema-push API.

- [ ] **Step 6: Run tests to verify they pass**

Run: `make dev-db-up && cd server && cargo test --test scheduled_test`
Expected: PASS (now 6 tests). The firing tests are timing-sensitive; if flaky, increase the poll timeout, never lower `MAX_SLEEP` semantics.

- [ ] **Step 7: Run the full gate**

Run: `make checkall`
Expected: fully green.

- [ ] **Step 8: Commit**

```bash
git add server/src/committer.rs server/src/scheduler.rs server/tests/scheduled_test.rs
git commit -m "feat(server): committer RunScheduled arm + per-db scheduler timer"
```

---

## Task 4: Protocol wire types (WS)

**Files:**
- Modify: `server/src/protocol.rs`

**Interfaces:**
- Produces: `protocol::ScheduleWhen` (tagged enum), `protocol::ScheduleInfo` (struct), five new `ClientMessage` variants, four new `ServerMessage` variants. Promotes `scheduler::ScheduleInfo` to re-use this type (see Step 3).

- [ ] **Step 1: Write the failing wire tests**

Add to `server/src/protocol.rs`'s `#[cfg(test)] mod tests`:

```rust
    #[test]
    fn schedule_when_wire_tags() {
        assert_eq!(
            serde_json::to_value(ScheduleWhen::AfterMs { ms: 5 }).unwrap(),
            serde_json::json!({"type": "afterMs", "ms": 5})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::RunAt { ms: 9 }).unwrap(),
            serde_json::json!({"type": "runAt", "ms": 9})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Cron { expr: "*/5 * * * *".to_string() }).unwrap(),
            serde_json::json!({"type": "cron", "expr": "*/5 * * * *"})
        );
    }

    #[test]
    fn schedule_message_wire_tags() {
        let q = super::sample_query();
        let s = serde_json::to_value(ClientMessage::Schedule {
            schedule_id: "s1".to_string(),
            when: ScheduleWhen::AfterMs { ms: 100 },
            txn: super::sample_txn(),
        })
        .unwrap();
        assert_eq!(s["type"], serde_json::json!("schedule"));
        assert_eq!(s["scheduleId"], serde_json::json!("s1"));
        assert_eq!(s["when"], serde_json::json!({"type": "afterMs", "ms": 100}));
        let _ = q; // silence unused if sample_query unused here

        let ok = serde_json::to_value(ServerMessage::ScheduleOk {
            schedule_id: "s1".to_string(),
            id: "job-9".to_string(),
        })
        .unwrap();
        assert_eq!(ok, serde_json::json!({"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}));

        let ack = serde_json::to_value(ServerMessage::ScheduleAck {
            schedule_id: "s1".to_string(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(ack["type"], serde_json::json!("scheduleAck"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd server && cargo test --lib protocol`
Expected: FAIL — `ScheduleWhen` / new variants not defined.

- [ ] **Step 3: Add the types**

In `server/src/protocol.rs`, near the other types (after `AuthedUser`):

```rust
/// How a caller wants a transaction scheduled. Mirrored byte-for-byte in
/// `ts-client/src/protocol.ts` and `rust-client/src/wire.rs`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScheduleWhen {
    /// Fire `ms` milliseconds from now.
    AfterMs { ms: i64 },
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    RunAt { ms: i64 },
    /// Fire on this 5-field cron schedule (UTC, min-first).
    Cron { expr: String },
}

/// A scheduled job's public view (returned by `listSchedules`). `cron` and
/// `last_error` are omitted on the wire when absent.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleInfo {
    pub id: String,
    pub kind: String, // "oneshot" | "cron"
    pub due_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    pub status: String, // "pending" | "running" | "paused" | "error"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: i64,
    pub fired_count: i64,
}
```

Add to `ClientMessage`:

```rust
    Schedule {
        schedule_id: String,
        when: ScheduleWhen,
        txn: Transaction,
    },
    CancelSchedule {
        schedule_id: String,
        id: String,
    },
    PauseSchedule {
        schedule_id: String,
        id: String,
    },
    ResumeSchedule {
        schedule_id: String,
        id: String,
    },
    ListSchedules {
        schedule_id: String,
    },
```

Add to `ServerMessage`:

```rust
    ScheduleOk {
        schedule_id: String,
        id: String,
    },
    ScheduleErr {
        schedule_id: String,
        error: RtDbError,
    },
    /// Reply to cancel/pause/resume. `error` is omitted on the wire when `ok`.
    ScheduleAck {
        schedule_id: String,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<RtDbError>,
    },
    ListSchedulesOk {
        schedule_id: String,
        schedules: Vec<ScheduleInfo>,
    },
```

Then unify with the scheduler's copy: in `server/src/scheduler.rs`, delete the local `ScheduleInfo` struct and `use crate::protocol::ScheduleInfo;` (make the `list` return type `Vec<crate::protocol::ScheduleInfo>`). Update the `list` body's struct literal to `ScheduleInfo { ... }` (same fields). If clippy complains the import is unused in non-test, gate appropriately.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd server && cargo test --lib protocol && make checkall`
Expected: PASS, gate green.

- [ ] **Step 5: Commit**

```bash
git add server/src/protocol.rs server/src/scheduler.rs
git commit -m "feat(server): schedule wire types (ScheduleWhen, ScheduleInfo, WS variants)"
```

---

## Task 5: WS handlers

**Files:**
- Modify: `server/src/ws.rs`
- Modify: `server/tests/ws_test.rs` (read it first for the client-connect + send/recv helper pattern)

**Interfaces:**
- Consumes: Task 2's `scheduler::{insert, list, cancel, set_paused}`, Task 1's `next_fire`, Task 4's wire types. `txn::Transaction` deserializes from the WS `txn` field.

- [ ] **Step 1: Write the failing WS test**

Append to `server/tests/ws_test.rs`. Reuse its existing connect/auth/send/recv helpers verbatim.

```rust
#[tokio::test]
async fn schedule_one_shot_over_ws() {
    let (pool, _db_name) = ws_setup().await; // existing helper: creates db, mints token, returns pool
    let db = &_db_name;
    let mut sock = ws_connect_authed(db).await; // existing helper
    push_simple_schema(&pool, db).await;

    let txn = serde_json::json!({
        "type": "schedule", "scheduleId": "s1",
        "when": {"type": "afterMs", "ms": 0},
        "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 5}}]}
    });
    send_json(&mut sock, &txn).await;
    let reply = recv_json(&mut sock).await;
    assert_eq!(reply["type"], serde_json::json!("scheduleOk"));
    let id = reply["id"].as_str().unwrap().to_string();

    // cancel it before it can matter for the test's purposes, then list is empty.
    send_json(&mut sock, &serde_json::json!({
        "type": "cancelSchedule", "scheduleId": "s2", "id": id
    })).await;
    let ack = recv_json(&mut sock).await;
    assert_eq!(ack["type"], serde_json::json!("scheduleAck"));
    assert_eq!(ack["ok"], serde_json::json!(true));
}

#[tokio::test]
async fn schedule_rejects_bad_cron() {
    let (_, db_name) = ws_setup().await;
    let db = &db_name;
    let mut sock = ws_connect_authed(db).await;
    send_json(&mut sock, &serde_json::json!({
        "type": "schedule", "scheduleId": "s1",
        "when": {"type": "cron", "expr": "garbage"},
        "txn": {"steps": []}
    })).await;
    let reply = recv_json(&mut sock).await;
    assert_eq!(reply["type"], serde_json::json!("scheduleErr"));
}

#[tokio::test]
async fn list_schedules_over_ws() {
    let (pool, db_name) = ws_setup().await;
    let db = &db_name;
    let mut sock = ws_connect_authed(db).await;
    let _ = pool;
    send_json(&mut sock, &serde_json::json!({
        "type": "listSchedules", "scheduleId": "l1"
    })).await;
    let reply = recv_json(&mut sock).await;
    assert_eq!(reply["type"], serde_json::json!("listSchedulesOk"));
    assert_eq!(reply["scheduleId"], serde_json::json!("l1"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make dev-db-up && cd server && cargo test --test ws_test schedule_`
Expected: FAIL — server closes/errors on unknown message type.

- [ ] **Step 3: Add the handler arms**

In `server/src/ws.rs`'s `handle_text_frame` match (after `ClientMessage::Mutate`), add arms. `scheduler::resolve_when` (a shared `pub(crate)` fn added in Step 3b) resolves `ScheduleWhen` → `(kind, due_at, cron)`, validating cron and rejecting negative delays; a past `runAt` is allowed (fires immediately).

- [ ] **Step 3a: Add the WS arms**

Add `use crate::protocol::ScheduleWhen;` and `use crate::scheduler;` and `use crate::db::now_ms;` to ws.rs imports.

```rust
        ClientMessage::Schedule { schedule_id, when, txn } => {
            let reply = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::resolve_when(when, now_ms()) {
                    Ok((kind, due_at, cron)) => {
                        match scheduler::insert(&state.pool, db, kind, due_at, &txn, cron.as_deref())
                            .await
                        {
                            Ok(id) => ServerMessage::ScheduleOk { schedule_id, id },
                            Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                        }
                    }
                    Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                },
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            };
            let _ = out_tx.send(reply);
            false
        }
        ClientMessage::CancelSchedule { schedule_id, id } => {
            let (ok, error) = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::cancel(&state.pool, db, &id).await {
                    Ok(ok) => (ok, None),
                    Err(error) => (false, Some(error)),
                },
                Err(error) => (false, Some(error)),
            };
            let _ = out_tx.send(ServerMessage::ScheduleAck { schedule_id, ok, error });
            false
        }
        ClientMessage::PauseSchedule { schedule_id, id } => {
            let (ok, error) = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::set_paused(&state.pool, db, &id, true).await {
                    Ok(ok) => (ok, None),
                    Err(error) => (false, Some(error)),
                },
                Err(error) => (false, Some(error)),
            };
            let _ = out_tx.send(ServerMessage::ScheduleAck { schedule_id, ok, error });
            false
        }
        ClientMessage::ResumeSchedule { schedule_id, id } => {
            let (ok, error) = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::set_paused(&state.pool, db, &id, false).await {
                    Ok(ok) => (ok, None),
                    Err(error) => (false, Some(error)),
                },
                Err(error) => (false, Some(error)),
            };
            let _ = out_tx.send(ServerMessage::ScheduleAck { schedule_id, ok, error });
            false
        }
        ClientMessage::ListSchedules { schedule_id } => {
            let reply = match authorize(&state.pool, principal, db).await {
                Ok(()) => match scheduler::list(&state.pool, db).await {
                    Ok(schedules) => ServerMessage::ListSchedulesOk { schedule_id, schedules },
                    Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
                },
                Err(error) => ServerMessage::ScheduleErr { schedule_id, error },
            };
            let _ = out_tx.send(reply);
            false
        }
```

- [ ] **Step 3b: Add the shared `resolve_when` helper**

The three manage arms are deliberately inlined (three 8-line bodies) rather than funneled through a higher-ranked-future helper — that pattern fights the borrow checker and clippy for no readability win. Add to `server/src/scheduler.rs`:

```rust
use crate::protocol::ScheduleWhen;

/// Resolves a `ScheduleWhen` to `(kind, due_at, cron)` row fields. Validates
/// the cron expression and rejects negative `afterMs`. A past `runAt` is
/// allowed — it fires immediately (the catch-up path). Shared by WS and HTTP.
pub(crate) fn resolve_when(
    when: ScheduleWhen,
    now: i64,
) -> Result<(&'static str, i64, Option<String>), RtDbError> {
    match when {
        ScheduleWhen::AfterMs { ms } => {
            if ms < 0 {
                return Err(RtDbError::bad_request("afterMs must be non-negative"));
            }
            Ok(("oneshot", now + ms, None))
        }
        ScheduleWhen::RunAt { ms } => Ok(("oneshot", ms, None)),
        ScheduleWhen::Cron { expr } => {
            let due = next_fire(&expr, now)?;
            Ok(("cron", due, Some(expr)))
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make dev-db-up && cd server && cargo test --test ws_test`
Expected: PASS (including the 3 new tests). If `manage`'s higher-ranked-future signature trips clippy, inline authorize+op in each of the three arms instead (three small duplicates beat a clever signature).

- [ ] **Step 5: Run the full gate and commit**

Run: `make checkall`
```bash
git add server/src/ws.rs server/tests/ws_test.rs
git commit -m "feat(server): WS schedule/cancel/pause/resume/list handlers"
```

---

## Task 6: HTTP routes

**Files:**
- Modify: `server/src/http_api.rs`
- Modify: `server/tests/http_api_test.rs` (read it first for the request helper pattern)

**Interfaces:**
- Produces: `POST /api/schedule`, `POST /api/schedule/:id/{cancel,pause,resume}`, `POST /api/schedules`.

- [ ] **Step 1: Write the failing HTTP test**

Append to `server/tests/http_api_test.rs` (reuse its auth/route helpers):

```rust
#[tokio::test]
async fn schedule_and_manage_over_http() {
    let (pool, db, token) = http_setup().await; // existing: creates db, mints machine token
    push_simple_schema(&pool, &db).await;

    let create: serde_json::Value = http_post(
        &token,
        "/api/schedule",
        serde_json::json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 1}}]}
        }),
    )
    .await;
    let id = create["id"].as_str().unwrap().to_string();

    let listed = http_post(&token, "/api/schedules", serde_json::json!({"db": db})).await;
    assert_eq!(listed["schedules"].as_array().unwrap().len(), 1);

    let paused = http_post(&token, &format!("/api/schedule/{id}/pause"), serde_json::json!({"db": db})).await;
    assert_eq!(paused["ok"], serde_json::json!(true));

    let resumed = http_post(&token, &format!("/api/schedule/{id}/resume"), serde_json::json!({"db": db})).await;
    assert_eq!(resumed["ok"], serde_json::json!(true));

    let cancelled = http_post(&token, &format!("/api/schedule/{id}/cancel"), serde_json::json!({"db": db})).await;
    assert_eq!(cancelled["ok"], serde_json::json!(true));
}

#[tokio::test]
async fn schedule_rejects_negative_after_ms_http() {
    let (_, db, token) = http_setup().await;
    let err = http_post_expect_error(
        &token,
        "/api/schedule",
        serde_json::json!({"db": db, "when": {"type": "afterMs", "ms": -1}, "txn": {"steps": []}}),
    )
    .await;
    assert!(err.contains("BAD_REQUEST") || err.contains("afterMs"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `make dev-db-up && cd server && cargo test --test http_api_test schedule_`
Expected: FAIL — routes don't exist.

- [ ] **Step 3: Add the routes + handlers**

In `server/src/http_api.rs`, add request/response structs and handlers mirroring `mutate_handler`:

```rust
use axum::extract::Path;
use crate::protocol::{ScheduleWhen, ScheduleInfo};
use crate::scheduler;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScheduleRequest {
    db: String,
    when: ScheduleWhen,
    txn: Transaction,
}

#[derive(Serialize)]
struct ScheduleResponse {
    id: String,
}

async fn schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ScheduleRequest>,
) -> Result<Json<ScheduleResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;

    let (kind, due_at, cron) = scheduler::resolve_when(body.when, now_ms())?;
    let id = scheduler::insert(&state.pool, &body.db, kind, due_at, &body.txn, cron.as_deref()).await?;
    Ok(Json(ScheduleResponse { id }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManageRequest {
    db: String,
}

#[derive(Serialize)]
struct ManageResponse {
    ok: bool,
}

/// Shared authorize-then-op body for the three boolean manage handlers.
async fn run_manage_op(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
    id: &str,
    op: ManageOp,
) -> Result<Json<ManageResponse>, RtDbError> {
    let token = bearer_token(headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, db).await?;
    let ok = match op {
        ManageOp::Cancel => scheduler::cancel(&state.pool, db, id).await?,
        ManageOp::Pause => scheduler::set_paused(&state.pool, db, id, true).await?,
        ManageOp::Resume => scheduler::set_paused(&state.pool, db, id, false).await?,
    };
    Ok(Json(ManageResponse { ok }))
}

enum ManageOp {
    Cancel,
    Pause,
    Resume,
}

async fn cancel_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Cancel).await
}

async fn pause_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Pause).await
}

async fn resume_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Resume).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListRequest {
    db: String,
}
#[derive(Serialize)]
struct ListResponse {
    schedules: Vec<ScheduleInfo>,
}

async fn list_schedules_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ListRequest>,
) -> Result<Json<ListResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    let schedules = scheduler::list(&state.pool, &body.db).await?;
    Ok(Json(ListResponse { schedules }))
}
```

`run_manage_op` uses a plain `ManageOp` enum (no boxed futures) — the three handlers are one-liners. `resolve_when` is the shared `pub(crate)` fn added in Task 5 Step 3b; add `use crate::db::now_ms;` to http_api.rs imports. Register routes in `http_api_routes`:

```rust
        .route("/api/schedule", post(schedule_handler))
        .route("/api/schedule/:id/cancel", post(cancel_handler))
        .route("/api/schedule/:id/pause", post(pause_handler))
        .route("/api/schedule/:id/resume", post(resume_handler))
        .route("/api/schedules", post(list_schedules_handler))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make dev-db-up && cd server && cargo test --test http_api_test`
Expected: PASS.

- [ ] **Step 5: Run the full gate and commit**

Run: `make checkall`
```bash
git add server/src/http_api.rs server/src/scheduler.rs server/src/ws.rs server/tests/http_api_test.rs
git commit -m "feat(server): HTTP schedule/cancel/pause/resume/list routes"
```

---

## Task 7: Scheduler semantics integration tests

**Files:**
- Modify: `server/tests/scheduled_test.rs`

Covers the spec's behavioral guarantees end-to-end through the real committer + scheduler.

- [ ] **Step 1: Add the semantic tests**

```rust
#[tokio::test]
async fn one_shot_catches_up_after_being_past_due() {
    // Insert a one-shot with due_at far in the past; it must fire on the very
    // next sweep (catch-up), not be dropped.
    // ... set up committer, insert due_at=1, warm_up, poll for effect.
}

#[tokio::test]
async fn cron_skips_missed_windows() {
    // Insert a `* * * * *` cron with due_at 1 hour in the past. It fires once
    // (next-after-now), NOT 60 times for the missed hour. Assert fired_count
    // increments by exactly 1 over one sweep.
}

#[tokio::test]
async fn failing_txn_marks_error_one_shot() {
    // Schedule a one-shot whose txn has an expectVersion that won't hold.
    // After firing, the row has status='error' and last_error set, and the
    // (failing) write did not occur.
}

#[tokio::test]
async fn failing_cron_reschedules_anyway() {
    // A cron whose txn fails still reschedules (status='pending', due_at moved
    // forward) and records last_error; it is NOT stuck in 'error'.
}

#[tokio::test]
async fn crash_recovery_refires() {
    // Insert + claim_due (status='running'), then call run_scheduler's
    // reset_running path (or restart by re-warming), assert the job fires.
}
```

Flesh each out using the helpers established in Task 3 (`warm_up_committer`, `poll_for_count`, `push_simple_schema`). For `failing_txn`, use `Step::ExpectVersion { table, id, version: 999 }` against a non-existent/low-version row to force a `NotFound`/precondition failure.

- [ ] **Step 2: Run + gate + commit**

Run: `make dev-db-up && cd server && cargo test --test scheduled_test && make checkall`
```bash
git add server/tests/scheduled_test.rs
git commit -m "test(server): scheduler catch-up/cron-skip/failure/crash-recovery semantics"
```

---

## Task 8: TS client `protocol.ts` wire types

**Files:**
- Modify: `ts-client/src/protocol.ts`
- Create: `ts-client/tests/schedule.test.ts`

- [ ] **Step 1: Add the types**

In `ts-client/src/protocol.ts`, after `SearchQuery`:

```ts
/** Mirrors server `protocol::ScheduleWhen` byte-for-byte. */
export type ScheduleWhen =
  | { type: "afterMs"; ms: number }
  | { type: "runAt"; ms: number }
  | { type: "cron"; expr: string };

/** Mirrors server `protocol::ScheduleInfo`. */
export interface ScheduleInfo {
  id: string;
  kind: "oneshot" | "cron";
  dueAt: number;
  cron?: string;
  status: "pending" | "running" | "paused" | "error";
  lastError?: string;
  createdAt: number;
  firedCount: number;
}
```

Extend `ClientMessage`:

```ts
  | { type: "schedule"; scheduleId: string; when: ScheduleWhen; txn: TransactionJson }
  | { type: "cancelSchedule"; scheduleId: string; id: string }
  | { type: "pauseSchedule"; scheduleId: string; id: string }
  | { type: "resumeSchedule"; scheduleId: string; id: string }
  | { type: "listSchedules"; scheduleId: string };
```

Extend `ServerMessage`:

```ts
  | { type: "scheduleOk"; scheduleId: string; id: string }
  | { type: "scheduleErr"; scheduleId: string; error: RtDbErrorEnvelope }
  | { type: "scheduleAck"; scheduleId: string; ok: boolean; error?: RtDbErrorEnvelope }
  | { type: "listSchedulesOk"; scheduleId: string; schedules: ScheduleInfo[] };
```

Export `ScheduleWhen` and `ScheduleInfo` from `ts-client/src/index.ts`.

- [ ] **Step 2: Write wire-shape test**

`ts-client/tests/schedule.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import type { ClientMessage, ScheduleWhen } from "../src/protocol.js";

describe("schedule wire types", () => {
  it("schedule message shape", () => {
    const when: ScheduleWhen = { type: "afterMs", ms: 100 };
    const msg: ClientMessage = {
      type: "schedule",
      scheduleId: "s1",
      when,
      txn: { steps: [] },
    };
    expect(msg.type).toBe("schedule");
    expect((msg as any).when.type).toBe("afterMs");
  });
});
```

- [ ] **Step 3: Run + gate + commit**

Run: `cd ts-client && bunx vitest run tests/schedule.test.ts && cd .. && make checkall`
```bash
git add ts-client/src/protocol.ts ts-client/src/index.ts ts-client/tests/schedule.test.ts
git commit -m "feat(ts-client): schedule wire types (ScheduleWhen, ScheduleInfo, variants)"
```

---

## Task 9: TS client `client.ts` + `http.ts` + `in_memory.ts`

**Files:**
- Modify: `ts-client/src/client.ts` (mirror the `mutate` pattern at line ~249), `ts-client/src/http.ts` (mirror `mutate` at line ~38), `ts-client/src/in_memory.ts`
- Modify: `ts-client/tests/client.test.ts`, `ts-client/tests/http.test.ts`, `ts-client/tests/in_memory.test.ts`

- [ ] **Step 1: Reactive client (`client.ts`)**

Add a `scheduleId` counter and a `pendingSchedules: Map<string, {resolve, reject}>` plus a `pendingAcks`/`pendingList` map (or one map keyed by scheduleId carrying a discriminated handler). Mirror `mutate`/`dispatchMutate` and the `mutateOk`/`mutateErr` arms in the `onMessage` switch.

Public API:

```ts
schedule(txn: TransactionJson, when: ScheduleWhen): Promise<{ id: string }> {
  const scheduleId = `sch-${++this.counter}`;
  // queue + dispatch like mutate; resolve with {id} on scheduleOk,
  // reject on scheduleErr.
}
cancelSchedule(id: string): Promise<void>   // resolve on scheduleAck.ok, reject otherwise
pauseSchedule(id: string): Promise<void>
resumeSchedule(id: string): Promise<void>
listSchedules(): Promise<ScheduleInfo[]>     // resolve on listSchedulesOk
```

Add `onMessage` arms for `scheduleOk`/`scheduleErr`/`scheduleAck`/`listSchedulesOk`, dispatching to the pending map by `scheduleId`. `scheduleAck` with `ok:false` rejects with the `error` envelope (use `RtDbError.fromEnvelope`).

- [ ] **Step 2: HTTP client (`http.ts`)**

```ts
async schedule(txn: TransactionJson, when: ScheduleWhen): Promise<{ id: string }> {
  const body = await this.post("/api/schedule", { db: this.db, when, txn });
  return { id: (body as { id: string }).id };
}
async cancelSchedule(id: string): Promise<void> { await this.post(`/api/schedule/${id}/cancel`, { db: this.db }); }
async pauseSchedule(id: string): Promise<void> { await this.post(`/api/schedule/${id}/pause`, { db: this.db }); }
async resumeSchedule(id: string): Promise<void> { await this.post(`/api/schedule/${id}/resume`, { db: this.db }); }
async listSchedules(): Promise<ScheduleInfo[]> {
  const body = await this.post("/api/schedules", { db: this.db });
  return (body as { schedules: ScheduleInfo[] }).schedules;
}
```

- [ ] **Step 3: In-memory harness (`in_memory.ts`)**

Timer-less: `schedule` stores the job in an internal map (returns a generated id); `listSchedules`/`cancelSchedule`/`pauseSchedule`/`resumeSchedule` operate on that map; add a `tick(nowMs?)` method that fires every due non-paused one-shot (and reschedules crons by computing `next_fire` — port the 5-field min-first logic, or simply advance crons by a fixed step for the stub, documented as approximate). Mirror the server's status names. Keep `InMemoryRtDbClient` semantically aligned but mark the cron timing approximate in a doc comment.

- [ ] **Step 4: Tests**

- `client.test.ts`: using the existing fake-WS harness, assert `schedule` resolves with `{id}` on `scheduleOk`, rejects on `scheduleErr`; `cancelSchedule` resolves on `scheduleAck.ok:true`.
- `http.test.ts`: using the existing fetch mock, assert `schedule` POSTs `/api/schedule` with the right body; `listSchedules` parses `schedules`.
- `in_memory.test.ts`: `schedule` + `tick` fires a one-shot and the write is visible via `query`; pause then tick does not fire; cancel removes.

- [ ] **Step 5: Run + gate + commit**

Run: `cd ts-client && bunx vitest run && cd .. && make checkall`
```bash
git add ts-client/src/client.ts ts-client/src/http.ts ts-client/src/in_memory.ts ts-client/src/index.ts ts-client/tests/client.test.ts ts-client/tests/http.test.ts ts-client/tests/in_memory.test.ts
git commit -m "feat(ts-client): schedule/cancel/pause/resume/listSchedules on reactive + http + in-memory"
```

---

## Task 10: Rust client `wire.rs` + `http.rs` + `ws.rs`

**Files:**
- Modify: `rust-client/src/wire.rs`, `rust-client/src/http.rs`, `rust-client/src/ws.rs`, `rust-client/src/lib.rs`
- Modify/add: `rust-client/tests/` (mirror existing wire/http/ws tests)

- [ ] **Step 1: Wire types (`wire.rs`)**

Mirror Task 4 exactly. Add `ScheduleWhen` (tagged, camelCase, deny_unknown_fields), `ScheduleInfo` (camelCase, `Option` fields `#[serde(default, skip_serializing_if = "Option::is_none")]`), and the five `ClientMessage` + four `ServerMessage` variants. Re-export `ScheduleWhen`, `ScheduleInfo` from `lib.rs`.

- [ ] **Step 2: HTTP client (`http.rs`)**

Mirror `mutate` (line ~90). `next_fire` is server-side only — the client trusts the server. Add:

```rust
pub async fn schedule(&self, txn: Transaction, when: ScheduleWhen) -> Result<String, RtDbError> { /* POST /api/schedule, parse {id} */ }
pub async fn cancel_schedule(&self, id: &str) -> Result<(), RtDbError> { /* POST /api/schedule/{id}/cancel */ }
pub async fn pause_schedule(&self, id: &str) -> Result<(), RtDbError>
pub async fn resume_schedule(&self, id: &str) -> Result<(), RtDbError>
pub async fn list_schedules(&self) -> Result<Vec<ScheduleInfo>, RtDbError> { /* POST /api/schedules */ }
```

- [ ] **Step 3: Reactive client (`ws.rs`)**

Mirror `mutate` (line ~425): `schedule` sends `ClientMessage::Schedule { schedule_id, when, txn }` and awaits `ScheduleOk { id }`/`ScheduleErr`; the manage ops send their variant and await `ScheduleAck` (err if `!ok`). `list_schedules` awaits `ListSchedulesOk`. Add the four `ServerMessage` arms to the inbound dispatcher.

- [ ] **Step 4: Tests**

- `wire.rs` round-trip tests for the new variants (mirror existing wire tests).
- `http.rs` test with `wiremock` for `/api/schedule` (mirror the existing mutate wiremock test).
- `ws.rs`: mirror the existing mutate reactive test for `schedule`.

- [ ] **Step 5: Run + gate + commit**

Run: `cd rust-client && cargo test && cd .. && make checkall`
```bash
git add rust-client/src/wire.rs rust-client/src/http.rs rust-client/src/ws.rs rust-client/src/lib.rs rust-client/tests/
git commit -m "feat(rust-client): schedule/cancel/pause/resume/list_schedules on http + ws"
```

---

## Task 11: Docs — flip FEATURE_MATRIX #9/#10 + READMEs + CLAUDE.md

**Files:**
- Modify: `FEATURE_MATRIX.md`, `server/README.md`, `ts-client/README.md`, `rust-client/README.md`, `CLAUDE.md`, any par-rt-db skill doc.

- [ ] **Step 1: Update `FEATURE_MATRIX.md`**

Flip rows #9 and #10 from ❌ to ✅ with a one-paragraph "Implemented —" note (mirror the style of the other ✅ rows: name the files — `scheduler.rs`, the `RunScheduled` committer arm, the per-db timer, the WS/HTTP surfaces, and client-mirror status). Update §5 "Recommended order" to remove #9/#10 from "Remaining gaps" and add them to the "done" list with a one-line summary.

- [ ] **Step 2: READMEs + CLAUDE.md**

Add a "Scheduling" subsection to each README (server: the `scheduled_txns` table + scheduler + at-least-once semantics; clients: the `schedule`/manage API). In `CLAUDE.md`'s architecture/invariants, note the scheduler as a second per-db task that writes only the side table and routes execution through the committer.

- [ ] **Step 3: Run the full gate + commit**

Run: `make checkall`
```bash
git add FEATURE_MATRIX.md server/README.md ts-client/README.md rust-client/README.md CLAUDE.md
git commit -m "docs: scheduled & cron transactions (#9/#10) — flip FEATURE_MATRIX, READMEs, CLAUDE.md"
```

---

## Verification (whole plan)

`make checkall` green from the repo root after Task 11 is the definition of done. Spot-check the live behavior with `make dev-db-up` + a quick script that schedules a one-shot `afterMs: 1000` and a `*/1 * * * *` cron over WS, printing `listSchedules` output.
