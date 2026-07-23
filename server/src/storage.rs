//! Per-database blob storage (file storage, FEATURE_MATRIX #16). Bytes live in
//! Postgres `bytea` (TOAST-managed) in a per-db `storage` table; a global
//! `rtdb.storage_index(id -> db_name)` resolves the unauthenticated public serve
//! URL to the owning database. See
//! docs/superpowers/specs/2026-07-23-file-storage-design.md.

use sqlx::PgPool;

use crate::db::validate_db_name;
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
