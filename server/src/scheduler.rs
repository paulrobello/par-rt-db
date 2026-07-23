//! Per-database scheduled/cron transaction store + timer. Jobs are *data*
//! (a declarative `Transaction` plus a `due_at`), not code — the scheduler
//! drains due rows through the single-writer committer, which executes them
//! via the normal `execute_txn` path. See
//! `docs/superpowers/specs/2026-07-23-scheduled-cron-transactions-design.md`.

use crate::db::{new_id, now_ms, validate_db_name};
use crate::error::RtDbError;
use crate::protocol::ScheduleWhen;

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
    // Column order matches the SELECT list above.
    type ScheduleRow = (
        String,
        String,
        i64,
        Option<String>,
        String,
        Option<String>,
        i64,
        i64,
    );
    let rows: Vec<ScheduleRow> = sqlx::query_as(&format!(
        "SELECT id, kind, due_at, cron, status, last_error, created_at, fired_count
             FROM \"{schema}\".scheduled_txns ORDER BY due_at, created_at"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(id, kind, due_at, cron, status, last_error, created_at, fired_count)| ScheduleInfo {
                id,
                kind,
                due_at,
                cron,
                status,
                last_error,
                created_at,
                fired_count,
            },
        )
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
            Some((kind, Some(expr))) if kind == "cron" => {
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
    let rows: Vec<(String, String, serde_json::Value, Option<String>)> = sqlx::query_as(&format!(
        "UPDATE \"{schema}\".scheduled_txns SET status = 'running'
             WHERE id IN (
                 SELECT id FROM \"{schema}\".scheduled_txns
                 WHERE status = 'pending' AND due_at <= $1
                 ORDER BY due_at LIMIT $2
                 FOR UPDATE SKIP LOCKED
             )
             RETURNING id, kind, txn, cron"
    ))
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
            Ok(ClaimedJob {
                id,
                kind,
                txn,
                cron,
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

/// Reschedules a cron job whose execution FAILED: advance `due_at` to the next
/// fire and record `last_error`, but stay `pending` so the cron keeps firing
/// (unlike `mark_error`, which stops it). `fired_count` is NOT bumped — it
/// counts successful fires only.
pub async fn reschedule_cron_error(
    pool: &PgPool,
    db: &str,
    id: &str,
    next_due: i64,
    msg: &str,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".scheduled_txns
         SET status = 'pending', due_at = $2, last_error = $3
         WHERE id = $1"
    ))
    .bind(id)
    .bind(next_due)
    .bind(msg)
    .execute(pool)
    .await?;
    Ok(())
}

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
}
