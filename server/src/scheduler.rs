//! Per-database scheduled/cron transaction store + timer. Jobs are *data*
//! (a declarative `Transaction` plus a `due_at`), not code — the scheduler
//! drains due rows through the single-writer committer, which executes them
//! via the normal `execute_txn` path. Since FM-29 the same timer also polls
//! the `workflows` side table (`crate::workflows`) and enqueues claimed runs
//! as `RunWorkflowAdvance`; the committer alone advances them. See
//! `docs/superpowers/specs/2026-07-23-scheduled-cron-transactions-design.md`.

use crate::db::{new_id, now_ms, validate_db_name};
use crate::error::RtDbError;
use crate::protocol::{ScheduleKind, ScheduleStatus, ScheduleWhen};

/// Computes the next fire time (UTC epoch ms) for a 5-field cron expression,
/// strictly after `now_ms`. Also validates the expression: a parse failure or
/// an expression with no future fire times is `BadRequest`.
pub fn next_fire(expr: &str, now_ms: i64) -> Result<i64, RtDbError> {
    use chrono::{DateTime, Utc};
    // `Cron::new` is infallible; `parse()` does the actual validation and
    // rejects malformed expressions. `croner` reads a 5-field expression
    // min-first (seconds default to 0), so `*/5 * * * *` means every 5
    // minutes, not every 5 seconds.
    let mut cron = croner::Cron::new(expr);
    cron.parse()
        .map_err(|_| RtDbError::bad_request("invalid cron expression"))?;
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms)
        .ok_or_else(|| RtDbError::internal("invalid timestamp"))?;
    let next = cron
        .find_next_occurrence(&now, false)
        .map_err(|_| RtDbError::bad_request("cron expression has no future fire times"))?;
    Ok(next.timestamp_millis())
}

/// Upper bound on an interval job's `everyMs`: one year in ms. Guards
/// `now + every_ms` against i64 overflow and bounds the horizon a recurring
/// job can occupy a row for. Mirrored as a constant in all four clients.
pub const MAX_EVERY_MS: i64 = 365 * 24 * 60 * 60 * 1000;

/// Row fields `resolve_when` produces: `(kind, due_at, cron, every_ms)`.
pub(crate) type ResolvedWhen = (&'static str, i64, Option<String>, Option<i64>);

/// Resolves a `ScheduleWhen` to `(kind, due_at, cron, every_ms)` row fields.
/// Validates the cron expression, rejects negative `afterMs`, and rejects a
/// non-positive or over-cap `everyMs`. A past `runAt` is allowed — it fires
/// immediately (the catch-up path). Shared by WS, HTTP, the `Schedule` txn
/// step, and the admin CRUD.
pub(crate) fn resolve_when(when: ScheduleWhen, now: i64) -> Result<ResolvedWhen, RtDbError> {
    match when {
        ScheduleWhen::AfterMs { ms } => {
            if ms < 0 {
                return Err(RtDbError::bad_request("afterMs must be non-negative"));
            }
            Ok(("oneshot", now + ms, None, None))
        }
        ScheduleWhen::RunAt { ms } => Ok(("oneshot", ms, None, None)),
        ScheduleWhen::Cron { expr } => {
            let due = next_fire(&expr, now)?;
            Ok(("cron", due, Some(expr), None))
        }
        ScheduleWhen::Interval { every_ms } => {
            if every_ms <= 0 {
                return Err(RtDbError::bad_request("everyMs must be positive"));
            }
            if every_ms > MAX_EVERY_MS {
                return Err(RtDbError::bad_request(format!(
                    "everyMs must be at most {MAX_EVERY_MS}"
                )));
            }
            Ok(("interval", now + every_ms, None, Some(every_ms)))
        }
    }
}

use sqlx::{PgConnection, PgPool};

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
    pub kind: String, // "oneshot" | "cron" | "interval"
    pub txn: Transaction,
    pub cron: Option<String>,
    pub every_ms: Option<i64>,
}

/// Canonical scheduled-job view. Promoted to the wire type in Task 4 and
/// re-exported here so `list` and existing call sites resolve one struct.
pub use crate::protocol::ScheduleInfo;

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
            every_ms    bigint,
            status      text NOT NULL,
            last_error  text,
            created_at  bigint NOT NULL,
            fired_count bigint NOT NULL DEFAULT 0
        )"
    ))
    .execute(pool)
    .await?;
    // Databases created before interval jobs lack the column; additive-only,
    // same IF NOT EXISTS discipline as the schema DDL path.
    sqlx::query(&format!(
        "ALTER TABLE \"{schema}\".scheduled_txns
         ADD COLUMN IF NOT EXISTS every_ms bigint"
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
    every_ms: Option<i64>,
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
            (id, kind, due_at, txn, cron, every_ms, status, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7)"
    ))
    .bind(&id)
    .bind(kind)
    .bind(due_at)
    .bind(txn_json)
    .bind(cron)
    .bind(every_ms)
    .bind(now_ms())
    .execute(&mut *conn)
    .await?;
    Ok(id)
}

pub async fn insert(
    pool: &PgPool,
    db: &str,
    kind: &str,
    due_at: i64,
    txn: &Transaction,
    cron: Option<&str>,
    every_ms: Option<i64>,
) -> Result<String, RtDbError> {
    let mut conn = pool.acquire().await?;
    insert_on(&mut conn, db, kind, due_at, txn, cron, every_ms).await
}

pub async fn list(pool: &PgPool, db: &str) -> Result<Vec<ScheduleInfo>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Column order matches the SELECT list above.
    type ScheduleRow = (
        String,
        String,
        i64,
        Option<String>,
        Option<i64>,
        String,
        Option<String>,
        i64,
        i64,
    );
    let rows: Vec<ScheduleRow> = sqlx::query_as(&format!(
        "SELECT id, kind, due_at, cron, every_ms, status, last_error, created_at, fired_count
             FROM \"{schema}\".scheduled_txns ORDER BY due_at, created_at"
    ))
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(
            |(id, kind, due_at, cron, every_ms, status, last_error, created_at, fired_count)| {
                let kind = kind.parse::<ScheduleKind>().map_err(|err| {
                    RtDbError::internal(format!("invalid scheduled_txns.kind: {err}"))
                })?;
                let status = status.parse::<ScheduleStatus>().map_err(|err| {
                    RtDbError::internal(format!("invalid scheduled_txns.status: {err}"))
                })?;
                Ok(ScheduleInfo {
                    id,
                    kind,
                    due_at,
                    cron,
                    every_ms,
                    status,
                    last_error,
                    created_at,
                    fired_count,
                })
            },
        )
        .collect()
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

/// Pause (`paused=true`) or resume (`paused=false`) a job. Resuming a cron job
/// recomputes `due_at` to the next fire after now; resuming an interval job
/// shifts `due_at` to `now + every_ms` (windows elapsed while paused are
/// skipped, never backfilled); resuming a one-shot leaves its `due_at` alone.
/// Returns true if a row was updated.
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
        // Resume: recompute next fire for cron, shift from resume for
        // interval; one-shot keeps its due_at.
        let row: Option<(String, Option<String>, Option<i64>)> = sqlx::query_as(&format!(
            "SELECT kind, cron, every_ms FROM \"{schema}\".scheduled_txns
             WHERE id = $1 AND status = 'paused'"
        ))
        .bind(id)
        .fetch_optional(pool)
        .await?;
        let next = match row {
            Some((kind, Some(expr), _)) if kind == "cron" => Some(next_fire(&expr, now_ms())?),
            Some((kind, _, Some(every_ms))) if kind == "interval" => Some(now_ms() + every_ms),
            Some(_) => None,
            None => return Ok(false),
        };
        match next {
            Some(next) => {
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
            None => {
                sqlx::query(&format!(
                    "UPDATE \"{schema}\".scheduled_txns SET status = 'pending'
                     WHERE id = $1"
                ))
                .bind(id)
                .execute(pool)
                .await?
            }
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
    // Column order matches the RETURNING list below.
    type ClaimRow = (
        String,
        String,
        serde_json::Value,
        Option<String>,
        Option<i64>,
    );
    let rows: Vec<ClaimRow> = sqlx::query_as(&format!(
        "UPDATE \"{schema}\".scheduled_txns SET status = 'running'
             WHERE id IN (
                 SELECT id FROM \"{schema}\".scheduled_txns
                 WHERE status = 'pending' AND due_at <= $1
                 ORDER BY due_at LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, kind, txn, cron, every_ms"
    ))
    .bind(now)
    .bind(batch)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(id, kind, txn_json, cron, every_ms)| {
            let txn: Transaction = serde_json::from_value(txn_json).map_err(|err| {
                tracing::error!(error = %err, db, %id, "failed to deserialize scheduled txn");
                RtDbError::internal("failed to read scheduled txn")
            })?;
            Ok(ClaimedJob {
                id,
                kind,
                txn,
                cron,
                every_ms,
            })
        })
        .collect()
}

pub async fn finalize_one_shot_done(pool: &PgPool, db: &str, id: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "DELETE FROM \"{schema}\".scheduled_txns WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// Finalizes a recurring (cron/interval) job after a successful fire: back to
/// `pending` at `next_due`, bump `fired_count`, clear `last_error`.
pub async fn finalize_recurring_next(
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
    if let Err(e) = sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns SET status = 'error', last_error = $2 WHERE id = $1"
    ))
    .bind(id)
    .bind(msg)
    .execute(pool)
    .await
    {
        tracing::warn!(db = %db, id = %id, error = %e, "scheduled_txns mark_error write failed");
        return Err(e.into());
    }
    Ok(())
}

/// Reschedules a recurring (cron/interval) job whose execution FAILED: advance
/// `due_at` to the next fire and record `last_error`, but stay `pending` so
/// the job keeps firing (unlike `mark_error`, which stops it). `fired_count`
/// is NOT bumped — it counts successful fires only.
pub async fn reschedule_recurring_error(
    pool: &PgPool,
    db: &str,
    id: &str,
    next_due: i64,
    msg: &str,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    if let Err(e) = sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns
         SET status = 'pending', due_at = $2, last_error = $3
         WHERE id = $1"
    ))
    .bind(id)
    .bind(next_due)
    .bind(msg)
    .execute(pool)
    .await
    {
        tracing::warn!(db = %db, id = %id, error = %e, "scheduled_txns reschedule_recurring_error write failed");
        return Err(e.into());
    }
    Ok(())
}

use tokio::sync::mpsc::Sender;
use tokio::time::Duration;

use crate::committer::CommitterRequest;

/// Maximum sleep between wakes when a db has at least one pending (future) job.
/// Bounds the latency of a job inserted with a sooner `due_at` than the current
/// sleep target (the loop re-reads the min `due_at` each wake, so this only
/// costs an occasional early wake).
const MAX_SLEEP: Duration = Duration::from_secs(2);

/// The per-db scheduler loop. Owns recovery on start (both `scheduled_txns`
/// and, since FM-29, `workflows`: ensure tables + reset orphaned `running`
/// rows), then repeatedly: read the nearest due time across BOTH tables,
/// sleep until it (capped), claim due rows from each table that has them,
/// and enqueue each as a fire-and-forget `RunScheduled` /
/// `RunWorkflowAdvance` on the committer channel. Exits when the committer
/// channel closes (its task died) — the next request to this db respawns
/// both.
///
/// ARC-102: when a table's `next_due` reports nothing due (either `None` =
/// no rows, or a future due time), the loop skips THAT table's `claim_due`
/// query entirely — there is nothing to claim. This removes the heavier
/// `UPDATE … FOR UPDATE SKIP LOCKED` write from every poll on a db whose
/// nearest job is not yet due (the idle write-every-2s the audit flagged).
/// The `next_due` reads still run at the `MAX_SLEEP` cadence so a
/// newly-inserted due job is caught promptly; reducing that read frequency
/// further would require a notify-on-insert so an idle loop wakes
/// immediately when `scheduler::insert` adds a row.
pub async fn run_scheduler(pool: PgPool, db: String, committer_tx: Sender<CommitterRequest>) {
    if let Err(err) = ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: ensure_table failed");
    }
    if let Err(err) = reset_running(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: reset_running failed");
    }
    if let Err(err) = crate::workflows::ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: workflows ensure_table failed");
    }
    if let Err(err) = crate::workflows::reset_running(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "scheduler: workflows reset_running failed");
    }
    loop {
        // `claim_sched`/`claim_wf` are true only when THAT table actually has
        // something due; in all other cases its claim_due query is skipped
        // (ARC-102 per-table). The wake target is the min of the two tables'
        // nearest due times.
        let sched_next = next_due(&pool, &db).await;
        let wf_next = crate::workflows::next_due(&pool, &db).await;
        // Per-table error handling mirrors the single-table loop this
        // replaced: a failed read may mean the db was dropped (DROP SCHEMA)
        // out from under this scheduler — exit cleanly in that case; else
        // log per table and degrade that table to "nothing known this tick"
        // while the other still participates in the wake/claim decisions.
        if sched_next.is_err() || wf_next.is_err() {
            if matches!(crate::db::database_exists(&pool, &db).await, Ok(false)) {
                tracing::info!(db = %db, "scheduler: database removed, exiting");
                return;
            }
            for (table, res) in [("scheduled_txns", &sched_next), ("workflows", &wf_next)] {
                if let Err(err) = res {
                    tracing::error!(db = %db, table, error = %err, "scheduler: next_due failed");
                }
            }
        }
        let now = now_ms();
        let claim_sched = matches!(sched_next, Ok(Some(due)) if due <= now);
        let claim_wf = matches!(wf_next, Ok(Some(due)) if due <= now);
        let nearest = [sched_next.ok().flatten(), wf_next.ok().flatten()]
            .into_iter()
            .flatten()
            .min();
        let sleep = match nearest {
            Some(due) if due <= now => Duration::ZERO,
            // Future work: re-check at the 2s cadence so a newly-inserted
            // sooner item is picked up quickly, but skip the zero-row
            // claim_due queries. ARC-102.
            Some(due) => Duration::from_millis((due - now) as u64).min(MAX_SLEEP),
            // Nothing pending in either table: poll at MAX_SLEEP so a
            // newly-inserted due item is caught promptly, but skip the
            // claim_due writes. ARC-102.
            None => MAX_SLEEP,
        };
        if !sleep.is_zero() {
            // Select on the committer channel close so the longer IDLE_SLEEP
            // can't delay exit detection — a closed channel means the committer
            // task is gone and this scheduler is useless.
            tokio::select! {
                _ = tokio::time::sleep(sleep) => {}
                _ = committer_tx.closed() => {
                    tracing::debug!(db = %db, "scheduler: committer channel closed, exiting");
                    return;
                }
            }
        }
        if claim_sched {
            let now = now_ms();
            let claimed = match claim_due(&pool, &db, now, CLAIM_BATCH).await {
                Ok(jobs) => jobs,
                Err(err) => {
                    if matches!(crate::db::database_exists(&pool, &db).await, Ok(false)) {
                        tracing::info!(db = %db, "scheduler: database removed, exiting");
                        return;
                    }
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
                    every_ms: job.every_ms,
                };
                if committer_tx.send(req).await.is_err() {
                    // Committer task is gone; this scheduler is now useless.
                    tracing::warn!(db = %db, "scheduler: committer channel closed, exiting");
                    return;
                }
            }
        }
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-07-23T12:00:00Z = 1784808000000 ms (a Thursday). A fixed anchor so
    /// the minute/hour/day math is deterministic.
    const ANCHOR_MS: i64 = 1_784_808_000_000;

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

    #[test]
    fn resolve_when_interval_first_due_is_one_interval_out() {
        let (kind, due, cron, every_ms) =
            resolve_when(ScheduleWhen::Interval { every_ms: 5_000 }, ANCHOR_MS).unwrap();
        assert_eq!(kind, "interval");
        assert_eq!(due, ANCHOR_MS + 5_000);
        assert!(cron.is_none());
        assert_eq!(every_ms, Some(5_000));
    }

    #[test]
    fn resolve_when_interval_rejects_non_positive() {
        for bad in [0i64, -1] {
            let err = resolve_when(ScheduleWhen::Interval { every_ms: bad }, ANCHOR_MS)
                .expect_err("non-positive everyMs must be rejected");
            assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
        }
    }

    #[test]
    fn resolve_when_interval_rejects_over_cap() {
        let err = resolve_when(
            ScheduleWhen::Interval {
                every_ms: MAX_EVERY_MS + 1,
            },
            ANCHOR_MS,
        )
        .expect_err("over-cap everyMs must be rejected");
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
        // The cap itself is in-bounds.
        assert!(
            resolve_when(
                ScheduleWhen::Interval {
                    every_ms: MAX_EVERY_MS
                },
                ANCHOR_MS
            )
            .is_ok()
        );
    }
}
