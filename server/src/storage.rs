//! Per-database blob storage (file storage, FEATURE_MATRIX #16). Bytes live in
//! Postgres `bytea` (TOAST-managed) in a per-db `storage` table; a global
//! `rtdb.storage_index(id -> db_name)` resolves the unauthenticated public serve
//! URL to the owning database. See
//! docs/superpowers/specs/2026-07-23-file-storage-design.md. On-the-fly image
//! transforms on the serve routes (`?w=&h=&fit=&q=&format=`) live in
//! `image_transform.rs`.

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// `CREATE TABLE IF NOT EXISTS` for the per-db `storage` table, for databases
/// that predate this feature. Mirrors `mutation_log::ensure_table`; called once
/// at committer startup and before every upload (`upload_handler`), so it also
/// retrofits the content-addressed dedup index (`ENH-008`) onto tables created
/// before the index existed, and the nullable `owner_id` column (SEC-118) onto
/// tables created before per-row blob authorization shipped.
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
            created_at   bigint NOT NULL,
            owner_id     text
        )"
    ))
    .execute(pool)
    .await?;
    // SEC-118: retrofit `owner_id` onto tables created before the column
    // existed. Idempotent — Postgres >= 9.6 supports ADD COLUMN IF NOT EXISTS.
    sqlx::query(&format!(
        "ALTER TABLE \"{schema}\".storage ADD COLUMN IF NOT EXISTS owner_id text"
    ))
    .execute(pool)
    .await?;
    // Best-effort: a database that predates dedup and already holds duplicate
    // hashes cannot build the index, so dedup stays off for it (uploads keep
    // working, `put` simply stores a copy) until an operator clears the dupes.
    if let Err(e) = sqlx::query(&format!(
        "CREATE UNIQUE INDEX IF NOT EXISTS \"{schema}_storage_sha256_idx\"
         ON \"{schema}\".storage (sha256)"
    ))
    .execute(pool)
    .await
    {
        tracing::warn!(
            db = %db,
            error = %e,
            "storage: sha256 dedup index not built (duplicate content present?); \
             dedup disabled for this db",
        );
    }
    Ok(())
}

/// Lowercase-hex sha256 over raw bytes (for content hashing on upload).
pub fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Wire metadata for a stored file. `contentType` is omitted on the wire when
/// `None` (the upload supplied no Content-Type header). `ownerId` is omitted
/// when the blob has no per-row owner (machine token / admin / pre-SEC-118
/// rows); the metadata route never exposes it to a caller who could not pass
/// the owner check anyway (the route 403s first), but it is also not part of
/// the public shape, so it stays server-side by default.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMeta {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub creation_time: i64,
    /// Per-row owner (SEC-118). `None` for machine-token / admin uploads and
    /// for rows that predate the column. Never serialized to the wire — the
    /// HTTP metadata route redacts it before responding.
    #[serde(skip)]
    pub owner_id: Option<String>,
}

/// Inserts a blob + metadata and records the global index row, returning the
/// id. Content-addressed dedup (`ENH-008`): if bytes with this `sha256` are
/// already stored in the database, the existing id is returned and no second
/// copy is written — so re-uploading identical bytes yields the same public
/// URL. Dedup needs the per-db unique index on `sha256` (added by
/// `create_database` / `ensure_table`); `ON CONFLICT DO NOTHING` (no target)
/// keeps this safe even when that index is absent — the insert then always
/// lands and dedup is simply off for that database.
///
/// SEC-118: `owner_id` carries the uploading principal's user_id for
/// `Principal::User`, and is `None` for machine tokens / admin (system
/// uploads). Enforcement is at the authed serve / delete / metadata routes,
/// not here — a NULL owner is "anyone authorized for the db may touch it",
/// matching how `ownerField` treats system-initiated document writes.
pub async fn put(
    pool: &PgPool,
    db: &str,
    sha256: &str,
    size: i64,
    content_type: Option<&str>,
    owner_id: Option<&str>,
    bytes: &[u8],
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let id = new_id();
    let mut tx = pool.begin().await?;
    let inserted: Option<(String,)> = sqlx::query_as(&format!(
        "INSERT INTO \"{schema}\".storage (id, sha256, size, content_type, bytes, created_at, owner_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT DO NOTHING
         RETURNING id"
    ))
    .bind(&id)
    .bind(sha256)
    .bind(size)
    .bind(content_type)
    .bind(bytes)
    .bind(now_ms())
    .bind(owner_id)
    .fetch_optional(&mut *tx)
    .await?;
    let id = match inserted {
        // Won the insert — record the global index row for the new id.
        Some((row_id,)) => {
            sqlx::query("INSERT INTO rtdb.storage_index (id, db_name) VALUES ($1, $2)")
                .bind(&row_id)
                .bind(db)
                .execute(&mut *tx)
                .await?;
            row_id
        }
        // A blob with this sha256 already exists — reuse its id (dedup hit).
        None => {
            let (existing,): (String,) = sqlx::query_as(&format!(
                "SELECT id FROM \"{schema}\".storage WHERE sha256 = $1"
            ))
            .bind(sha256)
            .fetch_one(&mut *tx)
            .await?;
            existing
        }
    };
    tx.commit().await?;
    Ok(id)
}

/// Reads a blob and its content type for serving. `None` if the id is absent.
/// Returns `Bytes` so the serve path and transform cache can hand the buffer to
/// the HTTP body without cloning the (potentially large) payload.
pub async fn get(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<(Bytes, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Vec<u8>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(bytes, ct)| (Bytes::from(bytes), ct)))
}

/// Reads just the total byte length of a blob via `octet_length(bytes)` —
/// cheap (no bytea is shipped over the wire), used by the Range path to resolve
/// a `Range: bytes=...` header against the resource size before fetching any
/// slice. `None` if the id is absent. SEC-123.
pub async fn total_bytes(pool: &PgPool, db: &str, id: &str) -> Result<Option<u64>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // `octet_length` on a `bytea` returns the underlying byte count (not the
    // hex-encoded length), even when the column is TOAST-compressed out-of-line
    // — so this does not materialize the blob. The cast to `bigint` keeps the
    // sqlx decode trivial (`i64`); the server-side `MAX_FILE_SIZE` cap means a
    // real blob never overflows `i64`.
    let row: Option<(i64,)> = sqlx::query_as(&format!(
        "SELECT octet_length(bytes)::bigint FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(n,)| n as u64))
}

/// Reads just a `[start, end]` inclusive byte slice of a blob (SEC-123). Uses
/// Postgres `substring(bytes FROM $start FOR $len)` so a Range request never
/// materializes the whole bytea in server memory — only the requested slice +
/// the content type cross the wire. PostgreSQL `substring` is 1-indexed, so
/// `start=0` becomes `FROM 1`; the FOR-clamp to `len = end - start + 1` lands
/// exactly the inclusive `start..=end` window. Returns `None` if the id is
/// absent. A window past EOF returns an empty slice (the caller has already
/// resolved the range against `total_bytes` and would not ask for one, but the
/// SQL is safe either way).
pub async fn get_range(
    pool: &PgPool,
    db: &str,
    id: &str,
    start: u64,
    end: u64,
) -> Result<Option<(Bytes, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Saturate-clamp the length to defend against an `end` far past EOF: a
    // huge `FOR` value is legal in Postgres but the cast through i64 must not
    // overflow on a maliciously-crafted range. `start + (end - start) + 1`
    // cannot overflow when both fit in u64 and end >= start.
    let len = end.saturating_sub(start).saturating_add(1);
    let row: Option<(Vec<u8>, Option<String>)> = sqlx::query_as(&format!(
        // Cast the bind params to int4: Postgres's `substring(bytea FROM int
        // FOR int)` only resolves for integer (int4), and sqlx binds an i64 as
        // bigint which otherwise fails function resolution.
        "SELECT substring(bytes FROM $1::int FOR $2::int), content_type \
         FROM \"{schema}\".storage WHERE id = $3"
    ))
    .bind((start as i64) + 1)
    .bind(len as i64)
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(bytes, ct)| (Bytes::from(bytes), ct)))
}

/// Reads just the metadata. `None` if absent.
pub async fn get_meta(pool: &PgPool, db: &str, id: &str) -> Result<Option<FileMeta>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<StorageMetaRow> = sqlx::query_as(&format!(
        "SELECT id, sha256, size, content_type, created_at, owner_id
         FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(storage_meta_to_file_meta))
}

/// Lists every file's metadata in a database, newest-first. Capped at 1000 so a
/// huge store can't OOM the admin surface (operator listing, not a data path).
/// Same column set as `get_meta`; identifiers are the validated schema + the
/// fixed `storage` table name, so double-quoting via `format!` is safe.
pub async fn list(pool: &PgPool, db: &str) -> Result<Vec<FileMeta>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let rows: Vec<StorageMetaRow> = sqlx::query_as(&format!(
        "SELECT id, sha256, size, content_type, created_at, owner_id
         FROM \"{schema}\".storage
         ORDER BY created_at DESC
         LIMIT 1000"
    ))
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(storage_meta_to_file_meta).collect())
}

/// Row shape shared by `get_meta` and `list` (one column per `FileMeta` field,
/// including the SEC-118 `owner_id`). Factored out so clippy's `type_complexity`
/// stays quiet on the 6-tuple; kept private — callers consume `FileMeta`, not
/// the raw row.
type StorageMetaRow = (String, String, i64, Option<String>, i64, Option<String>);

fn storage_meta_to_file_meta(
    (id, sha256, size, content_type, creation_time, owner_id): StorageMetaRow,
) -> FileMeta {
    FileMeta {
        id,
        sha256,
        size,
        content_type,
        creation_time,
        owner_id,
    }
}

/// Deletes a blob and its index row. Returns true if a blob row was removed.
pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Atomic with `put`: the per-db blob delete and the global index delete are
    // one transaction, so a failure between them can't leave an orphan index
    // row pointing at a gone blob.
    let mut tx = pool.begin().await?;
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".storage WHERE id = $1"))
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let removed = res.rows_affected() > 0;
    if removed {
        sqlx::query("DELETE FROM rtdb.storage_index WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(removed)
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
