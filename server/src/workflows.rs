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
    StepOutcome, StepRetry, WorkflowInfo, WorkflowInfoFull, WorkflowSpec, WorkflowStatus,
};
use crate::txn::{MAX_STEPS, count_steps};
use sqlx::{PgConnection, PgPool};

/// Max steps in one workflow spec (spec: bounds).
pub const MAX_WORKFLOW_STEPS: usize = 64;

/// Submit-time validation: 1..=MAX_WORKFLOW_STEPS steps, retry fields in
/// bounds, and the recursive step count summed across every step's txn
/// within `txn::MAX_STEPS` (the FM-28 counter — bounds body size and the
/// nesting bomb).
pub fn validate_spec(spec: &WorkflowSpec) -> Result<(), RtDbError> {
    if spec.steps.is_empty() {
        return Err(RtDbError::bad_request(
            "workflow must have at least one step",
        ));
    }
    if spec.steps.len() > MAX_WORKFLOW_STEPS {
        return Err(RtDbError::bad_request(format!(
            "workflow exceeds {MAX_WORKFLOW_STEPS} steps"
        )));
    }
    for (i, step) in spec.steps.iter().enumerate() {
        if let Some(r) = &step.retry {
            if r.max_attempts == 0 {
                return Err(RtDbError::bad_request(format!(
                    "steps[{i}].retry.maxAttempts must be >= 1"
                )));
            }
            if r.initial_retry_ms == 0 || r.max_retry_ms < r.initial_retry_ms {
                return Err(RtDbError::bad_request(format!(
                    "steps[{i}].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs"
                )));
            }
        }
    }
    let total: usize = spec.steps.iter().map(|s| count_steps(&s.txn)).sum();
    if total > MAX_STEPS {
        return Err(RtDbError::bad_request(format!(
            "workflow recursive step count {total} exceeds MAX_STEPS {MAX_STEPS}"
        )));
    }
    Ok(())
}

/// Backoff before attempt `attempts + 1` (i.e. after `attempts` failures):
/// `min(initial * 2^(attempts-1), max)`, saturating. `attempts` is always
/// >= 1 at call sites; `saturating_sub` keeps a 0 from panicking in debug.
pub fn backoff_ms(retry: &StepRetry, attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(32);
    retry
        .initial_retry_ms
        .saturating_mul(1u64 << shift)
        .min(retry.max_retry_ms)
}

/// A row claimed for advancement by `claim_due`. The scheduler hands the full
/// payload to the committer so the committer never has to re-read the row.
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

/// `CREATE TABLE IF NOT EXISTS` for databases that predate this feature.
/// Mirrors `scheduler::ensure_table`; called once at scheduler startup
/// (`run_scheduler`), alongside the `scheduled_txns` ensure.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".workflows (
            id            text PRIMARY KEY,
            name          text NOT NULL,
            status        text NOT NULL,
            spec          jsonb NOT NULL,
            current_step  int  NOT NULL DEFAULT 0,
            attempts      int  NOT NULL DEFAULT 0,
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

/// Connection-bound variant of [`insert`] — runs on an OPEN sqlx transaction
/// so a `Step` that starts a workflow (FM-29) enqueues its row atomically
/// with the caller's document writes: the row becomes visible exactly at
/// `tx.commit()` and rolls back with it.
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
    // Clamp before the u64→i64 cast: a serde-accepted u64 above i64::MAX
    // would wrap negative ⇒ an instantly-due gate.
    let sleep_ms = spec
        .steps
        .first()
        .and_then(|s| s.sleep_before_ms)
        .unwrap_or(0)
        .min(i64::MAX as u64) as i64;
    let gate = now_ms().saturating_add(sleep_ms);
    let mut conn = pool.acquire().await?;
    insert_on(&mut conn, db, spec, gate).await
}

/// Atomically claims up to `batch` due rows: `pending`+`sleep_until <= now` →
/// `running`. `FOR UPDATE SKIP LOCKED` makes the claim safe even if a second
/// claimer ever exists (today there is exactly one scheduler poller per db).
pub async fn claim_due(
    pool: &PgPool,
    db: &str,
    now: i64,
    batch: i64,
) -> Result<Vec<WorkflowRow>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(
        String,
        String,
        serde_json::Value,
        i32,
        i32,
        i64,
        serde_json::Value,
    )> = sqlx::query_as(&format!(
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
        .map(
            |(id, name, spec, current_step, attempts, sleep_until, outcomes)| {
                Ok(WorkflowRow {
                    id: id.clone(),
                    name,
                    status: WorkflowStatus::Running,
                    spec: serde_json::from_value(spec).map_err(deser_err(db, &id))?,
                    current_step: current_step.max(0) as u32,
                    attempts: attempts.max(0) as u32,
                    sleep_until,
                    step_outcomes: serde_json::from_value(outcomes).map_err(deser_err(db, &id))?,
                })
            },
        )
        .collect()
}

fn deser_err<'a>(db: &'a str, id: &'a str) -> impl Fn(serde_json::Error) -> RtDbError + 'a {
    move |err| {
        tracing::error!(error = %err, db, %id, "failed to deserialize workflow row");
        RtDbError::internal("failed to read workflow")
    }
}

/// Crash recovery: a `running` row was orphaned mid-advance — return it to
/// `pending` so the run re-advances (at-least-once, like the scheduler).
pub async fn reset_running(pool: &PgPool, db: &str) -> Result<u64, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows SET status = 'pending', updated_at = $1
         WHERE status = 'running'"
    ))
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(res.rows_affected())
}

/// Min `sleep_until` among `pending` rows, or `None` if nothing is queued.
/// `MIN` is SQL `NULL` when no rows match, which sqlx deserializes as `None`.
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

/// Per-step-boundary write while the row stays `running` (the scheduler only
/// claims `pending`, so the committer's in-turn loop cannot be double-claimed).
pub async fn record_step_success(
    pool: &PgPool,
    db: &str,
    id: &str,
    current_step: u32,
    outcome: &StepOutcome,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json =
        serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
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
pub async fn set_pending(
    pool: &PgPool,
    db: &str,
    id: &str,
    sleep_until: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'pending', sleep_until = $2, updated_at = $3
         WHERE id = $1"
    ))
    .bind(id)
    .bind(sleep_until)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// A failed attempt that has retries left: bump attempts, schedule the
/// backoff gate, release to `pending`. Does NOT append an outcome (retries
/// are carried by the attempts counters).
pub async fn schedule_retry(
    pool: &PgPool,
    db: &str,
    id: &str,
    attempts: u32,
    sleep_until: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'pending', attempts = $2, sleep_until = $3, updated_at = $4
         WHERE id = $1"
    ))
    .bind(id)
    .bind(attempts as i32)
    .bind(sleep_until)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn finalize_success(
    pool: &PgPool,
    db: &str,
    id: &str,
    outcome: &StepOutcome,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json =
        serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'success', attempts = 0, last_error = NULL,
             step_outcomes = step_outcomes || $2::jsonb, finished_at = $3, updated_at = $3
         WHERE id = $1"
    ))
    .bind(id)
    .bind(&outcome_json)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(())
}

/// Terminal failure: `status = failed` with the final attempt count (bound
/// from `outcome.attempts`, so `WorkflowInfo.attempts` matches the trail's
/// last entry — `schedule_retry` carried the pre-terminal value).
pub async fn mark_failed(
    pool: &PgPool,
    db: &str,
    id: &str,
    outcome: &StepOutcome,
    error: &str,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let outcome_json =
        serde_json::to_value(outcome).map_err(|_| RtDbError::internal("serialize outcome"))?;
    sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'failed', last_error = $2, attempts = $5,
             step_outcomes = step_outcomes || $3::jsonb, finished_at = $4, updated_at = $4
         WHERE id = $1"
    ))
    .bind(id)
    .bind(error)
    .bind(&outcome_json)
    .bind(now_ms())
    .bind(outcome.attempts as i32)
    .execute(pool)
    .await?;
    Ok(())
}

/// Cancel: flip a non-terminal row to `cancelled`. Returns false for a
/// missing or already-terminal run. An in-flight `running` arm notices at
/// its next step boundary (`status_of`).
pub async fn cancel(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'cancelled', finished_at = $2, updated_at = $2
         WHERE id = $1 AND status IN ('pending', 'running')"
    ))
    .bind(id)
    .bind(now_ms())
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// Connection-bound variant of [`cancel`] for `Step::CancelWorkflow` — the
/// UPDATE rides the caller's open sqlx transaction.
// No caller yet: `Step::CancelWorkflow` lands in Task 4 (FM-29 plan) and
// consumes this; remove the attribute then.
#[allow(dead_code)]
pub(crate) async fn cancel_on(
    conn: &mut PgConnection,
    db: &str,
    id: &str,
) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!(
        "UPDATE \"{schema}\".workflows
         SET status = 'cancelled', finished_at = $2, updated_at = $2
         WHERE id = $1 AND status IN ('pending', 'running')"
    ))
    .bind(id)
    .bind(now_ms())
    .execute(&mut *conn)
    .await?;
    Ok(res.rows_affected() > 0)
}

pub async fn status_of(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<WorkflowStatus>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(String,)> = sqlx::query_as(&format!(
        "SELECT status FROM \"{schema}\".workflows WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(|(s,)| parse_status(&s)).transpose()
}

fn parse_status(s: &str) -> Result<WorkflowStatus, RtDbError> {
    s.parse()
        .map_err(|e| RtDbError::internal(format!("invalid workflows.status: {e}")))
}

// Column order matches the SELECT lists in `list` and `get`.
type InfoRow = (
    String,         // id
    String,         // name
    String,         // status
    i32,            // current_step
    i32,            // step_count = jsonb_array_length(spec->'steps')
    i32,            // attempts
    Option<i64>,    // sleep_until
    Option<String>, // last_error
    i64,            // created_at
    i64,            // updated_at
    Option<i64>,    // started_at
    Option<i64>,    // finished_at
);

#[allow(clippy::type_complexity)]
fn info_from_row(row: InfoRow) -> Result<WorkflowInfo, RtDbError> {
    let (
        id,
        name,
        status,
        current_step,
        step_count,
        attempts,
        sleep_until,
        last_error,
        created_at,
        updated_at,
        started_at,
        finished_at,
    ) = row;
    Ok(WorkflowInfo {
        id,
        name,
        status: parse_status(&status)?,
        current_step: current_step.max(0) as u32,
        step_count: step_count.max(0) as u32,
        attempts: attempts.max(0) as u32,
        sleep_until,
        last_error,
        created_at,
        updated_at,
        started_at,
        finished_at,
    })
}

pub async fn list(
    pool: &PgPool,
    db: &str,
    status: Option<&WorkflowStatus>,
    limit: u32,
) -> Result<Vec<WorkflowInfo>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<InfoRow> = sqlx::query_as(&format!(
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
    rows.into_iter().map(info_from_row).collect()
}

/// Full row for `GET .../{id}` — the same columns as [`list`] plus the
/// outcome trail.
pub async fn get(pool: &PgPool, db: &str, id: &str) -> Result<Option<WorkflowInfoFull>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    #[allow(clippy::type_complexity)]
    type FullRow = (
        String,
        String,
        String,
        i32,
        i32,
        i32,
        Option<i64>,
        Option<String>,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        serde_json::Value,
    );
    let row: Option<FullRow> = sqlx::query_as(&format!(
        "SELECT id, name, status, current_step, jsonb_array_length(spec->'steps'), attempts,
                sleep_until, last_error, created_at, updated_at, started_at, finished_at,
                step_outcomes
         FROM \"{schema}\".workflows WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.map(
        |(
            id,
            name,
            status,
            current_step,
            step_count,
            attempts,
            sleep_until,
            last_error,
            created_at,
            updated_at,
            started_at,
            finished_at,
            outcomes,
        )| {
            let step_outcomes =
                serde_json::from_value::<Vec<StepOutcome>>(outcomes).map_err(deser_err(db, &id))?;
            let info = info_from_row((
                id,
                name,
                status,
                current_step,
                step_count,
                attempts,
                sleep_until,
                last_error,
                created_at,
                updated_at,
                started_at,
                finished_at,
            ))?;
            Ok(WorkflowInfoFull {
                info,
                step_outcomes,
            })
        },
    )
    .transpose()
}

pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".workflows WHERE id = $1"))
        .bind(id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Per-status counts for the admin metrics JSON (spec: per-db breakdown,
/// admin JSON only — never the Prometheus scrape).
pub async fn count_by_status(
    pool: &PgPool,
    db: &str,
) -> Result<Vec<(WorkflowStatus, i64)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
        "SELECT status, COUNT(*) FROM \"{schema}\".workflows GROUP BY status"
    ))
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|(s, n)| Ok((parse_status(&s)?, n)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{StepRetry, WorkflowSpec, WorkflowStepSpec};
    use crate::txn::{Step, Transaction};

    fn spec_with(steps: Vec<WorkflowStepSpec>) -> WorkflowSpec {
        WorkflowSpec {
            name: "t".into(),
            steps,
        }
    }
    fn step_txn() -> WorkflowStepSpec {
        WorkflowStepSpec {
            txn: Transaction {
                steps: vec![Step::Insert {
                    table: "t".into(),
                    doc: serde_json::Map::new(),
                }],
            },
            retry: None,
            sleep_before_ms: None,
        }
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
        s.retry = Some(StepRetry {
            max_attempts: 0,
            initial_retry_ms: 100,
            max_retry_ms: 200,
        });
        assert!(validate_spec(&spec_with(vec![s])).is_err());
        let mut s = step_txn();
        s.retry = Some(StepRetry {
            max_attempts: 2,
            initial_retry_ms: 0,
            max_retry_ms: 200,
        });
        assert!(validate_spec(&spec_with(vec![s])).is_err());
        let mut s = step_txn();
        s.retry = Some(StepRetry {
            max_attempts: 2,
            initial_retry_ms: 500,
            max_retry_ms: 100,
        });
        assert!(validate_spec(&spec_with(vec![s])).is_err()); // max < initial
    }

    #[test]
    fn validate_enforces_recursive_step_budget() {
        let big = Transaction {
            steps: (0..600)
                .map(|_| Step::Insert {
                    table: "t".into(),
                    doc: serde_json::Map::new(),
                })
                .collect(),
        };
        let s1 = WorkflowStepSpec {
            txn: big.clone(),
            retry: None,
            sleep_before_ms: None,
        };
        let s2 = WorkflowStepSpec {
            txn: big,
            retry: None,
            sleep_before_ms: None,
        };
        assert!(validate_spec(&spec_with(vec![s1, s2])).is_err()); // 1200 > MAX_STEPS(1024)
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let r = StepRetry {
            max_attempts: 9,
            initial_retry_ms: 1_000,
            max_retry_ms: 6_000,
        };
        assert_eq!(backoff_ms(&r, 1), 1_000);
        assert_eq!(backoff_ms(&r, 2), 2_000);
        assert_eq!(backoff_ms(&r, 3), 4_000);
        assert_eq!(backoff_ms(&r, 4), 6_000); // capped
        assert_eq!(backoff_ms(&r, 8), 6_000); // saturating shift, no overflow
    }
}
