//! Per-database blob storage (file storage, FEATURE_MATRIX #16). Bytes live in
//! Postgres `bytea` (TOAST-managed) in a per-db `storage` table; a global
//! `rtdb.storage_index(id -> db_name)` resolves the unauthenticated public serve
//! URL to the owning database. See
//! docs/superpowers/specs/2026-07-23-file-storage-design.md. On-the-fly image
//! transforms on the serve routes (`?w=&h=&fit=&q=&format=`) live in
//! `image_transform.rs`.
//!
//! ENH-021: blobs written via the streaming upload path are stored as a row of
//! 1 MiB `bytea` chunks in a per-db `storage_chunks` table, keyed by
//! `(blob_id, seq)`, with the `storage` row's `bytes` column left NULL (it is
//! now nullable). Legacy blobs — written before this change, or via the
//! small-buffer admin path — keep their inline `bytes` and have no chunk rows.
//! The read path probes `storage_chunks` first and falls back to the inline
//! `bytes` column when no chunk rows exist, so both layouts are readable
//! without an eager migration (the ARC-102 anti-pattern).

use bytes::Bytes;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::db::{new_id, now_ms, validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// Chunk size for the streaming upload path (ENH-021). 1 MiB: small enough to
/// bound per-upload memory, large enough that a 1 GiB upload is ~1k chunk rows
/// rather than ~1M.
pub const STORAGE_CHUNK_BYTES: usize = 1024 * 1024;

/// `CREATE TABLE IF NOT EXISTS` for the per-db `storage` + `storage_chunks`
/// tables, for databases that predate this feature (and for fresh dbs so the
/// committer path that calls `ensure_table` is idempotent with
/// `create_database`). Mirrors `mutation_log::ensure_table`; called once at
/// committer startup and before every upload (`upload_handler`), so it also
/// retrofits the content-addressed dedup index (`ENH-008`) onto tables created
/// before the index existed, the nullable `owner_id` column (SEC-118), and the
/// `storage_chunks` table (ENH-021) onto tables created before chunked storage
/// shipped. The legacy `bytes` column is made nullable in place so a chunked
/// blob (which keeps its bytes in `storage_chunks`) can store NULL there —
/// idempotent via `ALTER COLUMN ... DROP NOT NULL`.
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
    // ENH-021: chunked uploads keep their bytes in `storage_chunks` and leave
    // the inline column NULL, so the NOT NULL constraint on legacy schemas must
    // be dropped. Idempotent — re-running on an already-nullable column is a
    // no-op. A legacy row written via the buffer-accepting path still carries
    // inline bytes; the read path falls back to it when no chunk rows exist.
    sqlx::query(&format!(
        "ALTER TABLE \"{schema}\".storage ALTER COLUMN bytes DROP NOT NULL"
    ))
    .execute(pool)
    .await?;
    // ENH-021: per-db chunk table. Additive — never alters the legacy `storage`
    // row shape. The composite PRIMARY KEY (blob_id, seq) gives the ordered
    // scan the read path streams over; there is no secondary index because
    // every chunk read is a point lookup on that PK.
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".storage_chunks (
            blob_id text NOT NULL,
            seq     int  NOT NULL,
            bytes   bytea NOT NULL,
            PRIMARY KEY (blob_id, seq)
        )"
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
/// This is the legacy inline path: the full `bytes` are stored in the
/// `storage.bytes` column (NOT NULL on legacy schemas, but a chunked upload
/// via `put_stream` leaves it NULL). It is retained for the admin upload path
/// (small bodies, no need for streaming) and for tests that build rows
/// directly. New HTTP uploads route through `put_stream`.
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

/// Streaming upload (ENH-021). Consumes `chunk_stream` — a sequence of byte
/// segments arriving from `request.into_body().into_data_stream()` — and writes
/// each 1 MiB accumulation as one `storage_chunks` row under a provisional id,
/// hashing as bytes arrive. After the stream ends the final sha256 is known and
/// a dedup-aware metadata insert runs: on a content hit the provisional chunks
/// are deleted (no orphaned rows — orphans would bill against the storage
/// quota) and the existing id is returned; otherwise the provisional id is
/// promoted to the new blob's id and the global index row is recorded. All
/// chunk + metadata writes run in one transaction, so a failed or aborted
/// upload commits nothing.
///
/// `limit` is the upload size ceiling (already clamped to
/// `HARD_MAX_FILE_SIZE.min(hot.max_file_size)` by the caller) — checked
/// incrementally as bytes arrive, so an oversized upload is rejected the moment
/// it crosses the line rather than after buffering. `quota_check` is an async
/// closure the caller supplies so this path can re-evaluate `used + running >
/// cap` as chunks land; it returns `Err(QUOTA_EXCEEDED)` to abort mid-stream.
/// Both early-reject paths commit nothing (the txn is rolled back).
///
/// SEC-118: `owner_id` stamps the uploading user's `user_id` for
/// `Principal::User` and is `None` for machine tokens / admin.
pub async fn put_stream<F, Fut, S, E>(
    pool: &PgPool,
    db: &str,
    content_type: Option<&str>,
    owner_id: Option<&str>,
    limit: u64,
    quota_check: F,
    chunk_stream: S,
) -> Result<StreamPutResult, RtDbError>
where
    F: FnMut(u64) -> Fut,
    Fut: std::future::Future<Output = Result<(), RtDbError>>,
    S: futures::Stream<Item = Result<Bytes, E>>,
    E: std::fmt::Display,
{
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let provisional_id = new_id();
    let mut tx = pool.begin().await?;
    // Insert each 1 MiB accumulation as a chunk row under the provisional id.
    let mut hasher = Sha256::new();
    let mut buf: Vec<u8> = Vec::with_capacity(STORAGE_CHUNK_BYTES);
    let mut total: u64 = 0;
    let mut seq: i32 = 0;
    let mut quota_check = quota_check;
    // Drive the request body stream. `futures::StreamExt` keeps this agnostic
    // to axum/hyper internals; the body's `into_data_stream()` yields
    // `Result<Bytes, BoxError>` which we surface as io::Error for a single
    // error type.
    use futures::StreamExt;
    let mut stream = Box::pin(chunk_stream);
    while let Some(chunk_res) = stream.next().await {
        let chunk = chunk_res.map_err(|e| {
            // A body read failure (client disconnect, network error) aborts the
            // upload — the transaction is dropped (rolled back) on return.
            tracing::warn!(error = %e, "storage: upload body stream error");
            RtDbError::bad_request("upload body read failed")
        })?;
        // Incremental size + quota enforcement: reject the moment the running
        // total crosses the line, rather than buffering the whole body first.
        total = total
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| RtDbError::bad_request("upload size overflow"))?;
        if total > limit {
            // Roll back by returning; the txn guard drops uncommitted.
            return Err(RtDbError::bad_request("upload exceeds max file size"));
        }
        // Quota check: caller's closure re-evaluates `used + total > cap`
        // against the live cache (cheap; no `pg_total_relation_size` scan on
        // this path).
        quota_check(total).await?;
        hasher.update(&chunk);
        buf.extend_from_slice(&chunk);
        // Flush full chunks as they form.
        while buf.len() >= STORAGE_CHUNK_BYTES {
            let split: Vec<u8> = buf.drain(..STORAGE_CHUNK_BYTES).collect();
            sqlx::query(&format!(
                "INSERT INTO \"{schema}\".storage_chunks (blob_id, seq, bytes)
                 VALUES ($1, $2, $3)"
            ))
            .bind(&provisional_id)
            .bind(seq)
            .bind(&split)
            .execute(&mut *tx)
            .await?;
            seq += 1;
        }
    }
    // Final partial chunk (if any). An empty upload (zero bytes) is allowed and
    // produces a blob with zero chunk rows — the metadata row still carries the
    // authoritative size, and the read path returns an empty body.
    if !buf.is_empty() {
        sqlx::query(&format!(
            "INSERT INTO \"{schema}\".storage_chunks (blob_id, seq, bytes)
             VALUES ($1, $2, $3)"
        ))
        .bind(&provisional_id)
        .bind(seq)
        .bind(&buf)
        .execute(&mut *tx)
        .await?;
    }
    let sha256 = hex::encode(hasher.finalize());
    let size = total as i64;
    // Dedup via metadata insert. `ON CONFLICT (sha256) DO NOTHING` needs the
    // unique index built by `ensure_table` / `create_database`; when the index
    // is absent the insert always lands and dedup is simply off (no target).
    // On a dedup hit we drop the provisional chunks and return the existing id
    // — orphaned chunk rows would bill against the storage quota.
    let inserted: Option<(String,)> = sqlx::query_as(&format!(
        "INSERT INTO \"{schema}\".storage (id, sha256, size, content_type, bytes, created_at, owner_id)
         VALUES ($1, $2, $3, $4, NULL, $5, $6)
         ON CONFLICT (sha256) DO NOTHING
         RETURNING id"
    ))
    .bind(&provisional_id)
    .bind(&sha256)
    .bind(size)
    .bind(content_type)
    .bind(now_ms())
    .bind(owner_id)
    .fetch_optional(&mut *tx)
    .await?;
    let id = match inserted {
        Some((row_id,)) => {
            sqlx::query("INSERT INTO rtdb.storage_index (id, db_name) VALUES ($1, $2)")
                .bind(&row_id)
                .bind(db)
                .execute(&mut *tx)
                .await?;
            row_id
        }
        None => {
            // Dedup hit: a blob with this sha256 already exists. Drop the
            // provisional chunks so they don't leak (orphans would bill against
            // the storage quota via `pg_total_relation_size`).
            sqlx::query(&format!(
                "DELETE FROM \"{schema}\".storage_chunks WHERE blob_id = $1"
            ))
            .bind(&provisional_id)
            .execute(&mut *tx)
            .await?;
            let (existing,): (String,) = sqlx::query_as(&format!(
                "SELECT id FROM \"{schema}\".storage WHERE sha256 = $1"
            ))
            .bind(&sha256)
            .fetch_one(&mut *tx)
            .await?;
            existing
        }
    };
    tx.commit().await?;
    let deduped = id != provisional_id;
    Ok(StreamPutResult {
        id,
        sha256,
        size,
        deduped,
    })
}

/// Result of a streaming upload: the resolved id (existing on a dedup hit, new
/// otherwise), the computed sha256, the byte count, and whether dedup fired.
#[derive(Debug, Clone)]
pub struct StreamPutResult {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    pub deduped: bool,
}

/// Reads a blob and its content type for serving. `None` if the id is absent.
/// Returns `Bytes` so the serve path and transform cache can hand the buffer to
/// the HTTP body without cloning the (potentially large) payload.
///
/// ENH-021: chunked blobs (metadata row's `bytes` IS NULL) are reassembled by
/// concatenating `storage_chunks` rows ordered by `seq`; legacy inline blobs
/// read the `bytes` column directly. A non-chunked caller (image transform,
/// legacy test helper) wants one contiguous buffer, so this function pays the
/// materialization cost deliberately. For streaming HTTP serve, prefer
/// `get_chunk_stream` / `get_range`.
pub async fn get(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<(Bytes, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Read metadata + inline bytes in one round-trip. `bytes` is nullable now
    // (chunked uploads leave it NULL); a NULL inline bytea means "look in
    // storage_chunks".
    let row: Option<(Option<Vec<u8>>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some((inline, content_type)) = row else {
        return Ok(None);
    };
    match inline {
        // Legacy inline blob — bytes already in hand.
        Some(bytes) => Ok(Some((Bytes::from(bytes), content_type))),
        // Chunked blob — reassemble from storage_chunks ordered by seq.
        None => {
            let chunks: Vec<(Vec<u8>,)> = sqlx::query_as(&format!(
                "SELECT bytes FROM \"{schema}\".storage_chunks WHERE blob_id = $1 ORDER BY seq"
            ))
            .bind(id)
            .fetch_all(pool)
            .await?;
            // A chunked blob with zero chunk rows is an empty-body upload (size
            // 0) — return an empty Bytes rather than None (the metadata row
            // exists, the blob is just empty).
            let total: usize = chunks.iter().map(|(c,)| c.len()).sum();
            let mut out = Vec::with_capacity(total);
            for (c,) in chunks {
                out.extend_from_slice(&c);
            }
            Ok(Some((Bytes::from(out), content_type)))
        }
    }
}

/// Reads just the total byte length of a blob — cheap (no bytea is shipped over
/// the wire), used by the Range path to resolve a `Range: bytes=...` header
/// against the resource size before fetching any slice. `None` if the id is
/// absent. SEC-123.
///
/// ENH-021: the metadata row's `size` column is authoritative and free (a
/// point read on the PK), so it is preferred over `octet_length(bytes)` for
/// both layouts. The legacy inline path also still works when `size` is somehow
/// stale (it is not — `put`/`put_stream` write it exactly) via `octet_length`.
pub async fn total_bytes(pool: &PgPool, db: &str, id: &str) -> Result<Option<u64>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(i64,)> = sqlx::query_as(&format!(
        "SELECT size FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(n,)| n as u64))
}

/// Outcome of `probe_layout`: how a blob's bytes are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobLayout {
    /// Bytes live in the inline `storage.bytes` column (legacy path, or the
    /// buffer-accepting `put` used by the admin route).
    Inline,
    /// Bytes live in `storage_chunks` (streaming upload path, ENH-021).
    Chunked,
}

/// Probe a blob's layout without fetching its bytes. `None` if the id is
/// absent. Cheap: a point read on the PK returning just the nullable `bytes`
/// column — NULL means chunked, non-NULL means legacy inline. Used by the serve
/// path to decide between the inline serve and a chunk stream.
pub async fn probe_layout(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<(BlobLayout, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Option<Vec<u8>>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(inline, content_type)| {
        let layout = if inline.is_some() {
            BlobLayout::Inline
        } else {
            BlobLayout::Chunked
        };
        (layout, content_type)
    }))
}

/// Streams a chunked blob's bytes as an async stream of `Bytes` chunks, ordered
/// by `seq`. Uses a background task that runs the `SELECT ... ORDER BY seq`
/// inside a per-row `fetch` and forwards each chunk over an mpsc channel, so
/// rows are streamed from Postgres rather than materialized — the serve path
/// can hand the stream straight to `axum::body::Body::from_stream` and a 1 GiB
/// download never holds more than one chunk (1 MiB) in memory at a time.
///
/// This is the chunked-layout counterpart to the legacy inline `get`. For
/// transforms and other callers that need one contiguous buffer, use `get` —
/// it reassembles both layouts into a single `Bytes`. The caller MUST have
/// already validated `db` (the serve path does via `resolve_db` / `authorize`).
///
/// The channel decouples the sqlx fetch (which borrows the query string) from
/// the returned stream's lifetime, avoiding the schema-qualified query lifetime
/// problem (sqlx's `fetch` borrows the `&str` query; a channel lets the task
/// own the query string and feed rows to a `ReceiverStream` the serve path
/// owns outright). On client disconnect, axum drops the `Body`, the
/// `ReceiverStream` is dropped, the sender errors on next send, and the task
/// exits early — the Postgres connection returns to the pool.
pub fn stream_chunks(
    pool: PgPool,
    db: String,
    id: String,
) -> impl futures::Stream<Item = Result<Bytes, std::io::Error>> + Send {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
    tokio::spawn(async move {
        // Validate inside the task; a bad db name surfaces as an error on the
        // channel. The serve path has already validated via `resolve_db`.
        if let Err(e) = validate_db_name(&db) {
            tracing::warn!(error = ?e, "storage stream read failed");
            let _ = tx
                .send(Err(std::io::Error::other("storage read failed")))
                .await;
            return;
        }
        let schema = pg_schema(&db);
        let query = format!(
            "SELECT bytes FROM \"{schema}\".storage_chunks WHERE blob_id = $1 ORDER BY seq"
        );
        use futures::stream::StreamExt as _;
        let mut rows = sqlx::query_as::<_, (Vec<u8>,)>(&query)
            .bind(&id)
            .fetch(&pool);
        while let Some(row_res) = rows.next().await {
            match row_res {
                Ok((bytes,)) => {
                    if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                        // Receiver dropped (client disconnect) — stop reading.
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = ?e, "storage stream read failed");
                    let _ = tx
                        .send(Err(std::io::Error::other("storage read failed")))
                        .await;
                    break;
                }
            }
        }
    });
    tokio_stream::wrappers::ReceiverStream::new(rx)
}

/// Reads just a `[start, end]` inclusive byte slice of a blob (SEC-123). For a
/// legacy inline blob, uses `substring(bytes FROM ... FOR ...)` so only the
/// requested slice crosses the wire. For a chunked blob, reads ONLY the chunk
/// span covering `[start, end]` and byte-trims first/last — a 1-byte range on a
/// 1 GiB file reads one 1 MiB chunk. Returns `None` if the id is absent. The
/// caller has already resolved the range against `total_bytes`, so `end` is
/// within bounds; a window past EOF returns an empty slice (safe either way).
pub async fn get_range(
    pool: &PgPool,
    db: &str,
    id: &str,
    start: u64,
    end: u64,
) -> Result<Option<(Bytes, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Resolve layout: a NULL inline bytes column means chunked. The metadata
    // row's content_type rides along either way.
    let row: Option<(Option<Vec<u8>>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    let Some((inline, content_type)) = row else {
        return Ok(None);
    };
    match inline {
        // Legacy inline blob — `substring` reads only the requested slice.
        Some(_) => {
            // Re-fetch just the slice (we discarded the inline bytes above in
            // favor of a cheap NULL check; the `substring` path keeps memory
            // bounded on legacy blobs too — SEC-123).
            let len = end.saturating_sub(start).saturating_add(1);
            let slice_row: Option<(Vec<u8>,)> = sqlx::query_as(&format!(
                "SELECT substring(bytes FROM $1::int FOR $2::int)
                 FROM \"{schema}\".storage WHERE id = $3"
            ))
            .bind((start as i64) + 1)
            .bind(len as i64)
            .bind(id)
            .fetch_optional(pool)
            .await?;
            Ok(slice_row.map(|(bytes,)| (Bytes::from(bytes), content_type)))
        }
        // Chunked blob — fetch only the covering chunk span.
        None => {
            let chunk_size = STORAGE_CHUNK_BYTES as u64;
            let seq_lo = (start / chunk_size) as i32;
            let seq_hi = (end / chunk_size) as i32;
            let rows: Vec<(i32, Vec<u8>)> = sqlx::query_as(&format!(
                "SELECT seq, bytes FROM \"{schema}\".storage_chunks
                 WHERE blob_id = $1 AND seq BETWEEN $2 AND $3
                 ORDER BY seq"
            ))
            .bind(id)
            .bind(seq_lo)
            .bind(seq_hi)
            .fetch_all(pool)
            .await?;
            // Byte-trim: drop bytes before `start` within the first chunk and
            // after `end` within the last. Each chunk's logical byte range is
            // [seq*CHUNK, seq*CHUNK + bytes.len()); clamp against [start, end].
            let mut out: Vec<u8> = Vec::new();
            for (idx, (seq, bytes)) in rows.into_iter().enumerate() {
                // Detect a gap (shouldn't happen — uploads write contiguous
                // seqs — but defend against a partial/corrupt blob by stopping
                // at the first missing seq rather than producing a silent
                // partial slice).
                if seq != seq_lo + idx as i32 {
                    break;
                }
                let chunk_start = (seq as u64) * chunk_size;
                let chunk_end_excl = chunk_start + bytes.len() as u64;
                let window_start = chunk_start.max(start);
                let window_end_excl = chunk_end_excl.min(end + 1);
                if window_start < window_end_excl {
                    let s = (window_start - chunk_start) as usize;
                    let e = (window_end_excl - chunk_start) as usize;
                    out.extend_from_slice(&bytes[s..e]);
                }
            }
            Ok(Some((Bytes::from(out), content_type)))
        }
    }
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
/// ENH-021: also drops any `storage_chunks` rows for the blob (chunked layout).
pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    // Atomic with `put`: the per-db blob delete, the per-db chunk delete, and
    // the global index delete are one transaction, so a failure between them
    // can't leave orphan chunk rows or an index row pointing at a gone blob.
    let mut tx = pool.begin().await?;
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".storage WHERE id = $1"))
        .bind(id)
        .execute(&mut *tx)
        .await?;
    let removed = res.rows_affected() > 0;
    if removed {
        sqlx::query(&format!(
            "DELETE FROM \"{schema}\".storage_chunks WHERE blob_id = $1"
        ))
        .bind(id)
        .execute(&mut *tx)
        .await?;
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
