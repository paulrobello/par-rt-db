use serde_json::Value;
use sqlx::PgPool;

use crate::db::now_ms;
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// How long a cached mutation result stays valid for dedup.
pub const DEDUP_TTL_MS: i64 = 5 * 60 * 1000;

/// Ensures the per-db mutation dedup table exists. Idempotent — safe to call
/// once per committer task startup, covering databases created before this
/// table existed (new databases get it from `db::create_database` instead).
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
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

/// Deletes `db`'s expired dedup entries, then looks up `mut_id`. `Some` means
/// this exact mutation already ran and its results should be replayed as-is,
/// with no re-execution and no fan-out. `None` means it's safe to execute.
pub async fn check(pool: &PgPool, db: &str, mut_id: &str) -> Result<Option<Vec<Value>>, RtDbError> {
    let schema = pg_schema(db);
    let now = now_ms();

    sqlx::query(&format!(
        "DELETE FROM \"{schema}\".mutations WHERE expires_at < $1"
    ))
    .bind(now)
    .execute(pool)
    .await?;

    let row: Option<(Value,)> = sqlx::query_as(&format!(
        "SELECT result FROM \"{schema}\".mutations WHERE mut_id = $1"
    ))
    .bind(mut_id)
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
