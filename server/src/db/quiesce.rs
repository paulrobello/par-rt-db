//! The quiesce/admission-epoch fence: advisory-lock-ordered write admission,
//! generation-checked freeze flags, and the per-database quiesce lock
//! lifecycle used by migrations, deletes, and the committer's admission
//! preamble.

use sqlx::{Connection, PgConnection, PgPool};

use super::bootstrap::BOOTSTRAP_LOCK_KEY;
use crate::error::RtDbError;
use crate::util::sha256_hex;

pub async fn quiesce_database(pool: &PgPool, name: &str) -> Result<Option<i64>, RtDbError> {
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    let exists: Option<(bool,)> =
        sqlx::query_as("SELECT quiesced FROM rtdb_auth.databases WHERE name = $1 FOR UPDATE")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((quiesced,)) = exists else {
        tx.rollback().await?;
        return Ok(None);
    };
    if quiesced {
        tx.rollback().await?;
        return Err(RtDbError::conflict("database is already quiesced"));
    }
    let epoch: i64 = sqlx::query_scalar(
        "UPDATE rtdb_auth.bootstrap_meta SET value = (value::bigint + 1)::text WHERE key = 'rtdb_admission_epoch' RETURNING value::bigint",
    )
    .fetch_one(&mut *tx)
    .await?;
    let generation: i64 = sqlx::query_scalar(
        "UPDATE rtdb_auth.databases SET quiesced = TRUE, quiesce_generation = quiesce_generation + 1, admission_fence_epoch = $2 WHERE name = $1 RETURNING quiesce_generation",
    )
    .bind(name)
    .bind(epoch)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(Some(generation))
}

pub async fn clear_quiesce(pool: &PgPool, name: &str, generation: i64) -> Result<bool, RtDbError> {
    Ok(sqlx::query(
        "UPDATE rtdb_auth.databases SET quiesced = FALSE WHERE name = $1 AND quiesced = TRUE AND quiesce_generation = $2",
    )
    .bind(name)
    .bind(generation)
    .execute(pool)
    .await?
    .rows_affected()
        == 1)
}

pub async fn quiesce_state(pool: &PgPool, name: &str) -> Result<Option<(bool, i64)>, RtDbError> {
    Ok(sqlx::query_as(
        "SELECT quiesced, quiesce_generation FROM rtdb_auth.databases WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?)
}

pub fn quiesce_key(name: &str) -> i64 {
    // Keep the quiesce mutex in a separate advisory-key namespace from the
    // multi-instance ownership lease. Both locks may be held by the owner
    // while a scheduler or client write takes the transactional quiesce lock.
    let hex = sha256_hex(&format!("quiesce:{name}"));
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0) as i64
}

pub async fn acquire_quiesce_lock(pool: &PgPool, name: &str) -> Result<PgConnection, RtDbError> {
    let mut connection = pool.acquire().await?.detach();
    if let Err(error) = sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(quiesce_key(name))
        .execute(&mut connection)
        .await
    {
        let _ = connection.close().await;
        return Err(error.into());
    }
    Ok(connection)
}

pub async fn release_quiesce_lock(
    mut connection: PgConnection,
    name: &str,
) -> Result<(), RtDbError> {
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(quiesce_key(name))
        .execute(&mut connection)
        .await?;
    connection.close().await?;
    Ok(())
}

/// Non-blocking counterpart to [`acquire_quiesce_lock`] for release recovery:
/// returns `None` when another live session still holds the quiesce lease, so
/// the caller reports a conflict instead of queueing behind an active owner.
pub async fn try_acquire_quiesce_lock(
    pool: &PgPool,
    name: &str,
) -> Result<Option<PgConnection>, RtDbError> {
    let mut connection = pool.acquire().await?.detach();
    let acquired: (bool,) = sqlx::query_as("SELECT pg_try_advisory_lock($1)")
        .bind(quiesce_key(name))
        .fetch_one(&mut connection)
        .await?;
    if acquired.0 {
        Ok(Some(connection))
    } else {
        let _ = connection.close().await;
        Ok(None)
    }
}

pub async fn clear_quiesce_on_connection(
    connection: &mut PgConnection,
    name: &str,
    generation: i64,
) -> Result<bool, RtDbError> {
    Ok(sqlx::query(
        "UPDATE rtdb_auth.databases SET quiesced = FALSE WHERE name = $1 AND quiesced = TRUE AND quiesce_generation = $2",
    )
    .bind(name)
    .bind(generation)
    .execute(connection)
    .await?
    .rows_affected()
        == 1)
}

pub async fn quiesce_matches(
    pool: &PgPool,
    name: &str,
    generation: i64,
) -> Result<bool, RtDbError> {
    Ok(sqlx::query_scalar(
        "SELECT quiesced AND quiesce_generation = $2 FROM rtdb_auth.databases WHERE name = $1",
    )
    .bind(name)
    .bind(generation)
    .fetch_optional(pool)
    .await?
    .unwrap_or(false))
}

pub async fn is_quiesced(pool: &PgPool, name: &str) -> Result<bool, RtDbError> {
    Ok(sqlx::query_scalar(
        "SELECT COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $1), FALSE)",
    )
    .bind(name)
    .fetch_one(pool)
    .await?)
}

pub async fn quiesce_admission(pool: &PgPool, name: &str) -> Result<(bool, i64), RtDbError> {
    Ok(sqlx::query_as(
        "SELECT quiesced, quiesce_generation FROM rtdb_auth.databases WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?
    .unwrap_or((false, 0)))
}

pub async fn admission_epoch(pool: &PgPool) -> Result<i64, RtDbError> {
    let epoch: Option<i64> = sqlx::query_scalar(
        "SELECT value::bigint FROM rtdb_auth.bootstrap_meta WHERE key = 'rtdb_admission_epoch'",
    )
    .fetch_optional(pool)
    .await?;
    epoch.ok_or_else(|| RtDbError::internal("admission epoch metadata is missing"))
}

pub async fn ensure_admission_epoch(
    connection: &mut PgConnection,
    name: &str,
    admission_epoch: i64,
) -> Result<(), RtDbError> {
    let fence: Option<i64> =
        sqlx::query_scalar("SELECT admission_fence_epoch FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *connection)
            .await?;
    let Some(fence) = fence else {
        return Err(RtDbError::not_found("unknown database"));
    };
    if admission_epoch < fence {
        return Err(RtDbError::read_only());
    }
    Ok(())
}

pub async fn ensure_quiesce_open(
    connection: &mut PgConnection,
    name: &str,
) -> Result<(), RtDbError> {
    let quiesced: bool = sqlx::query_scalar(
        "SELECT COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $1), TRUE)",
    )
    .bind(name)
    .fetch_one(&mut *connection)
    .await?;
    if quiesced {
        return Err(RtDbError::read_only());
    }
    Ok(())
}

pub async fn ensure_quiesce_generation(
    connection: &mut PgConnection,
    name: &str,
    generation: i64,
) -> Result<(), RtDbError> {
    let (quiesced, current): (bool, i64) = sqlx::query_as(
        "SELECT COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $1), FALSE), COALESCE((SELECT quiesce_generation FROM rtdb_auth.databases WHERE name = $1), 0)",
    )
    .bind(name)
    .fetch_one(connection)
    .await?;
    if quiesced || current != generation {
        return Err(RtDbError::read_only());
    }
    Ok(())
}
