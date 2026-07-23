//! Per-database blob storage (file storage, FEATURE_MATRIX #16). Bytes live in
//! Postgres `bytea` (TOAST-managed) in a per-db `storage` table; a global
//! `rtdb.storage_index(id -> db_name)` resolves the unauthenticated public serve
//! URL to the owning database. See
//! docs/superpowers/specs/2026-07-23-file-storage-design.md.

use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// `CREATE TABLE IF NOT EXISTS` for the per-db `storage` table, for databases
/// that predate this feature. Mirrors `mutation_log::ensure_table`; called once
/// at committer startup.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".storage (
            id           text PRIMARY KEY,
            sha256       text NOT NULL,
            size         bigint NOT NULL,
            content_type text,
            bytes        bytea NOT NULL,
            created_at   bigint NOT NULL
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Lowercase-hex sha256 over raw bytes (for content hashing on upload).
pub fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Wire metadata for a stored file. `contentType` is omitted on the wire when
/// `None` (the upload supplied no Content-Type header).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMeta {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub creation_time: i64,
}

/// Inserts a blob + metadata and records the global index row. Returns the id.
pub async fn put(
    pool: &PgPool,
    db: &str,
    sha256: &str,
    size: i64,
    content_type: Option<&str>,
    bytes: &[u8],
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let id = new_id();
    let mut tx = pool.begin().await?;
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".storage (id, sha256, size, content_type, bytes, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(&id)
    .bind(sha256)
    .bind(size)
    .bind(content_type)
    .bind(bytes)
    .bind(now_ms())
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO rtdb.storage_index (id, db_name) VALUES ($1, $2)")
        .bind(&id)
        .bind(db)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(id)
}

/// Reads a blob and its content type for serving. `None` if the id is absent.
pub async fn get(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<(Vec<u8>, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Vec<u8>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Reads just the metadata. `None` if absent.
pub async fn get_meta(pool: &PgPool, db: &str, id: &str) -> Result<Option<FileMeta>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(String, String, i64, Option<String>, i64)> = sqlx::query_as(&format!(
        "SELECT id, sha256, size, content_type, created_at
         FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(
        row.map(|(id, sha256, size, content_type, creation_time)| FileMeta {
            id,
            sha256,
            size,
            content_type,
            creation_time,
        }),
    )
}

/// Deletes a blob and its index row. Returns true if a blob row was removed.
pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".storage WHERE id = $1"))
        .bind(id)
        .execute(pool)
        .await?;
    if res.rows_affected() > 0 {
        sqlx::query("DELETE FROM rtdb.storage_index WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Resolves an opaque public id to its owning database. `None` if unknown.
pub async fn resolve_db(pool: &PgPool, id: &str) -> Result<Option<String>, RtDbError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT db_name FROM rtdb.storage_index WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(db,)| db))
}
