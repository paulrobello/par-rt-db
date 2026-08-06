//! Schema change history — captures a full `SchemaDef` snapshot on every push,
//! migrate, and restore, in a per-db `schema_history` table co-located with
//! `meta`. Always on (no config flag): low-volume, and its value is being
//! present when a revert is needed. Best-effort like the audit tap — a capture
//! failure is warned, never propagated (the schema change already committed).

use sqlx::PgPool;

use crate::db::{now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;
use crate::schema::SchemaDef;

/// Newest N snapshots kept per database. Cheap insurance against a schema
/// pushed in a loop; well above any realistic revert depth.
const MAX_VERSIONS: i64 = 100;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySummary {
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    pub principal: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    pub principal: Option<String>,
    pub schema: serde_json::Value,
}

/// Idempotent. Self-heals databases created before this feature shipped.
/// Mirrors the per-db side tables created in `db::create_database`.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema_name = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_name}\".schema_history (\
            version     BIGSERIAL PRIMARY KEY,\
            captured_at BIGINT NOT NULL,\
            source      TEXT NOT NULL,\
            principal   TEXT,\
            schema      JSONB NOT NULL\
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a snapshot, then prune to the retention cap. Best-effort (callers
/// warn on `Err`). `source` is "push" | "migrate" | "restore".
pub async fn capture(
    pool: &PgPool,
    db: &str,
    source: &str,
    principal: Option<&str>,
    schema: &SchemaDef,
) -> Result<(), RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let value = serde_json::to_value(schema)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".schema_history (captured_at, source, principal, schema) \
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(now_ms())
    .bind(source)
    .bind(principal)
    .bind(value)
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "DELETE FROM \"{schema_name}\".schema_history WHERE version NOT IN \
         (SELECT version FROM \"{schema_name}\".schema_history ORDER BY version DESC LIMIT $1)"
    ))
    .bind(MAX_VERSIONS)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list(
    pool: &PgPool,
    db: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<HistorySummary>, RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let rows: Vec<(i64, i64, String, Option<String>)> = sqlx::query_as(&format!(
        "SELECT version, captured_at, source, principal \
         FROM \"{schema_name}\".schema_history ORDER BY version DESC LIMIT $1 OFFSET $2"
    ))
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(version, captured_at, source, principal)| HistorySummary {
            version,
            captured_at,
            source,
            principal,
        })
        .collect())
}

pub async fn get(pool: &PgPool, db: &str, version: i64) -> Result<Option<HistoryEntry>, RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let row: Option<(i64, i64, String, Option<String>, serde_json::Value)> =
        sqlx::query_as(&format!(
            "SELECT version, captured_at, source, principal, schema \
             FROM \"{schema_name}\".schema_history WHERE version = $1"
        ))
        .bind(version)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(
        |(version, captured_at, source, principal, schema)| HistoryEntry {
            version,
            captured_at,
            source,
            principal,
            schema,
        },
    ))
}
