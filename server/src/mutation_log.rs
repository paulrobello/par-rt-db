use serde_json::Value;
use sqlx::PgPool;

use crate::db::{now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// Default dedup TTL (5 min) — the env-seed default for
/// `HotConfig::idempotency_ttl_ms` (`RTDB_IDEMPOTENCY_TTL_MS`). The live value
/// used at the `store()` call site is read from hot config
/// (`ctx.hot.load().idempotency_ttl_ms`), not this constant.
pub const DEFAULT_DEDUP_TTL_MS: i64 = 5 * 60 * 1000;

/// Ensures the per-db mutation dedup table exists. Idempotent — safe to call
/// once per committer task startup, covering databases created before this
/// table existed (new databases get it from `db::create_database` instead).
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".mutations (
            mut_id text PRIMARY KEY,
            result jsonb NOT NULL,
            expires_at bigint NOT NULL
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Looks up `mut_id` in `db`'s dedup table. `Some` means this exact mutation
/// already ran (and is still within its TTL) and its results should be replayed
/// as-is, with no re-execution and no fan-out. `None` means it's safe to
/// execute. Pure SELECT — no `DELETE` write-acquire on the hot path: ARC-007
/// moved physical cleanup to `run_cleanup` (spawned once per db by the
/// committer, sweeping at 60s). Expired rows are still filtered at read time
/// (`expires_at > now`) so a stale entry is treated as absent even before the
/// next cleanup sweep reclaims its space.
pub async fn check(pool: &PgPool, db: &str, mut_id: &str) -> Result<Option<Vec<Value>>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);

    let row: Option<(Value,)> = sqlx::query_as(&format!(
        "SELECT result FROM \"{schema}\".mutations WHERE mut_id = $1 AND expires_at > $2"
    ))
    .bind(mut_id)
    .bind(now_ms())
    .fetch_optional(pool)
    .await?;

    match row {
        Some((value,)) => {
            let results: Vec<Value> = serde_json::from_value(value).map_err(|err| {
                tracing::error!(error = %err, db, mut_id, "failed to deserialize cached mutation result");
                RtDbError::internal("failed to read cached mutation result")
            })?;
            Ok(Some(results))
        }
        None => Ok(None),
    }
}

/// Deletes `db`'s expired dedup entries. Called only by the per-db background
/// cleanup task spawned in `committer::Committers::channel_for` (ARC-007) —
/// not on the mutation hot path. At-least-once semantics: a row may briefly
/// outlive its `expires_at` until the next cleanup tick, which is harmless
/// (a replay after TTL merely re-reads a slightly-still-cached result).
pub async fn cleanup_expired(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let now = now_ms();
    sqlx::query(&format!(
        "DELETE FROM \"{schema}\".mutations WHERE expires_at < $1"
    ))
    .bind(now)
    .execute(pool)
    .await?;
    Ok(())
}

/// Periodic background loop that drains expired dedup rows for one database.
/// Spawns alongside `scheduler::run_scheduler` per db (see
/// `committer::Committers::channel_for`). Exits when the committer channel
/// closes — same lifecycle signal the scheduler uses (`committer_tx.closed()`
/// resolves when the per-db committer task, the sole `Receiver` owner, dies)
/// — so a dying committer task takes its cleanup task with it and the next
/// request respawns both.
pub async fn run_cleanup(
    pool: PgPool,
    db: String,
    committer_tx: tokio::sync::mpsc::Sender<crate::committer::CommitterRequest>,
) {
    // Re-use the table-ensure idempotently so a database created before this
    // table existed (or a schema where the table was dropped) is covered.
    if let Err(err) = ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "mutation_log cleanup: ensure_table failed");
    }
    let mut tick = tokio::time::interval(CLEANUP_INTERVAL);
    tick.tick().await; // interval's first tick fires immediately; skip it
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(err) = cleanup_expired(&pool, &db).await {
                    if matches!(crate::db::database_exists(&pool, &db).await, Ok(false)) {
                        tracing::info!(db = %db, "mutation_log cleanup: database removed, exiting");
                        return;
                    }
                    tracing::warn!(db = %db, error = %err, "mutation_log cleanup failed");
                }
            }
            _ = committer_tx.closed() => {
                tracing::debug!(db = %db, "mutation_log cleanup: committer channel closed, exiting");
                return;
            }
        }
    }
}

/// How often `run_cleanup` sweeps expired dedup rows for one database.
const CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// Caches `results` under `mut_id` for `ttl_ms`. Uses `ON CONFLICT DO NOTHING`
/// as a safety net only — the per-db committer already serializes every
/// mutation for `db`, so two concurrent stores of the same `mut_id` cannot
/// actually happen.
pub async fn store(
    pool: &PgPool,
    db: &str,
    mut_id: &str,
    results: &[Value],
    ttl_ms: i64,
) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let expires_at = now_ms() + ttl_ms;
    let value = serde_json::to_value(results).map_err(|err| {
        tracing::error!(error = %err, db, mut_id, "failed to serialize mutation result for caching");
        RtDbError::internal("failed to cache mutation result")
    })?;

    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".mutations (mut_id, result, expires_at) VALUES ($1, $2, $3)
         ON CONFLICT (mut_id) DO NOTHING"
    ))
    .bind(mut_id)
    .bind(value)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}
