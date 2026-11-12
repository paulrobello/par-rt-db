//! Tenant database lifecycle on the shared `rtdb_auth.databases` registry:
//! create (schema + registry row + extensions), drop, existence and
//! read-only-freeze queries, schema loading, and listing.

use sqlx::PgPool;

use super::bootstrap::{BOOTSTRAP_LOCK_KEY, EXTENSION_LOCK_KEY};
use super::quiesce::quiesce_key;
use crate::ddl::pg_schema;
use crate::error::RtDbError;
use crate::schema::SchemaDef;
use crate::util::{new_id, now_ms};

/// Database name identifier: `^[a-z][a-z0-9_]{0,32}$`.
pub(crate) fn validate_db_name(name: &str) -> Result<(), RtDbError> {
    let mut chars = name.chars();
    let starts_ok = matches!(chars.next(), Some(c) if c.is_ascii_lowercase());
    let rest_ok = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');

    if name.is_empty() || name.len() > 33 || !starts_ok || !rest_ok {
        return Err(RtDbError::bad_request(format!(
            "invalid database name '{name}'"
        )));
    }
    Ok(())
}

/// Registers a new tenant database: validates `name`, creates its Postgres schema
/// and `meta` table, and records it in the `rtdb_auth.databases` registry, all in
/// one transaction. Returns the database's creation identity (see
/// `rtdb_auth.databases.creation_id`).
pub async fn create_database(pool: &PgPool, name: &str) -> Result<String, RtDbError> {
    validate_db_name(name)?;

    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(EXTENSION_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quiesce_key(name))
        .execute(&mut *tx)
        .await?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT name FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
    if existing.is_some() {
        return Err(RtDbError::bad_request("database already exists"));
    }

    let schema_name = pg_schema(name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE SCHEMA \"{schema_name}\""
    )))
    .execute(&mut *tx)
    .await
    .map_err(map_duplicate_database_error)?;
    // Extensions are database-level in Postgres, and every par-rt-db "database" is
    // a schema in the single `rtdb` Postgres database — so this installs `vector`
    // and `pg_trgm` once into `rtdb`, available to all schemas. `IF NOT EXISTS`
    // makes them idempotent (serialized by EXTENSION_LOCK_KEY above).
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
    sqlx::query("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        .execute(&mut *tx)
        .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".meta (key text PRIMARY KEY, value jsonb NOT NULL)"
    )))
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".mutation_cache_v2 (
            mut_id text PRIMARY KEY,
            result jsonb NOT NULL,
            expires_at bigint NOT NULL
        )"
    )))
    .execute(&mut *tx)
    .await?;

    // The durable change feed (see `change_log`): counter row seeded at 0 so
    // the first write's UPDATE always matches, `log_id` minted once. The
    // ensure_table path covers dbs created before this existed.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".change_head (
            ok     boolean PRIMARY KEY DEFAULT true,
            seq    bigint NOT NULL DEFAULT 0,
            log_id text   NOT NULL DEFAULT ''
        )"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO \"{schema_name}\".change_head (ok, seq, log_id)
         VALUES (true, 0, $1) ON CONFLICT (ok) DO NOTHING"
    )))
    .bind(&new_id()[..16])
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".changes (
            seq        bigint PRIMARY KEY,
            table_name text NOT NULL,
            doc_id     text NOT NULL,
            kind       text NOT NULL,
            doc        jsonb,
            ts         bigint NOT NULL
        )"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        // CREATE INDEX takes no schema qualifier — the index lands in the
        // table's schema automatically.
        "CREATE INDEX changes_table_seq
         ON \"{schema_name}\".changes (table_name, seq)"
    )))
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".scheduled_txns (
            id          text PRIMARY KEY,
            kind        text NOT NULL,
            due_at      bigint NOT NULL,
            txn         jsonb NOT NULL,
            cron        text,
            tz          text,
            every_ms    bigint,
            status      text NOT NULL,
            last_error  text,
            created_at  bigint NOT NULL,
            fired_count bigint NOT NULL DEFAULT 0,
            external    boolean NOT NULL DEFAULT false,
            claim_generation bigint NOT NULL DEFAULT 0,
            lease_deadline_ms bigint,
            missed_count bigint NOT NULL DEFAULT 0,
            last_missed_at bigint,
            enqueuer jsonb
        )"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE INDEX \"{schema_name}_scheduled_due_idx\"
         ON \"{schema_name}\".scheduled_txns (status, due_at)"
    )))
    .execute(&mut *tx)
    .await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".storage (
            id           text PRIMARY KEY,
            sha256       text NOT NULL,
            size         bigint NOT NULL,
            content_type text,
            bytes        bytea,
            created_at   bigint NOT NULL,
            owner_id     text
        )"
    )))
    .execute(&mut *tx)
    .await?;
    // Content-addressed dedup: one blob per sha256 so re-uploaded bytes reuse
    // the existing id/URL (ENH-008). See `storage::put` / `ensure_table`.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE UNIQUE INDEX \"{schema_name}_storage_sha256_idx\"
         ON \"{schema_name}\".storage (sha256)"
    )))
    .execute(&mut *tx)
    .await?;
    // ENH-021: chunked storage. The streaming upload path writes 1 MiB chunk
    // rows under a provisional id, computes the sha256 as bytes arrive, then
    // promotes or dedups. The legacy `bytes` column is now nullable because a
    // chunked upload keeps its bytes here and leaves the inline column NULL.
    // `ensure_table` retrofits both changes (DROP NOT NULL + CREATE TABLE IF
    // NOT EXISTS) onto databases created before this shipped.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE \"{schema_name}\".storage_chunks (
            blob_id text NOT NULL,
            seq     int  NOT NULL,
            bytes   bytea NOT NULL,
            PRIMARY KEY (blob_id, seq)
        )"
    )))
    .execute(&mut *tx)
    .await?;

    let creation_id = new_id();
    let admission_epoch: i64 = sqlx::query_scalar(
        "SELECT COALESCE((SELECT value::bigint FROM rtdb_auth.bootstrap_meta WHERE key = 'rtdb_admission_epoch'), 0)",
    )
    .fetch_one(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.databases (name, created_at, creation_id, admission_fence_epoch) VALUES ($1, $2, $3, $4)",
    )
    .bind(name)
    .bind(now_ms())
    .bind(&creation_id)
    .bind(admission_epoch)
    .execute(&mut *tx)
    .await
    .map_err(map_duplicate_database_error)?;

    tx.commit().await?;
    Ok(creation_id)
}

/// The creation identity of a registered database, or `None` if unregistered.
pub async fn creation_id(pool: &PgPool, name: &str) -> Result<Option<String>, RtDbError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT creation_id FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(id,)| id))
}

/// Drops a tenant database: validates `name`, requires it exists, then in one
/// transaction `DROP SCHEMA ... CASCADE`s the per-db Postgres schema (removing
/// `meta`, `mutation_cache_v2`, legacy `mutations`, `scheduled_txns`, `storage`, and every document table
/// `push_schema` created) and deletes its row in `rtdb_auth.databases` plus
/// its per-db rows in `rtdb_auth.machine_tokens`, `rtdb_auth.allowlist`, and
/// `rtdb.storage_index`. `IF EXISTS` on the schema drop keeps a partially-
/// deleted db (schema already gone) recoverable — the DELETEs still run and
/// the registry row is removed. In-memory eviction (schema cache,
/// subscriptions, committer channel) is the caller's responsibility — see
/// `admin::delete_db`. Durable cleanup only happens here.
pub async fn drop_database(pool: &PgPool, name: &str) -> Result<(), RtDbError> {
    drop_database_if(pool, name, None).await
}

/// [`drop_database`] with an optional expected creation identity. The check
/// reads the registry row under the same `FOR UPDATE` lock the drop
/// transaction holds, so no delete-and-recreate can slip between check and
/// drop. A mismatch returns `CONFLICT` and deletes nothing.
pub async fn drop_database_if(
    pool: &PgPool,
    name: &str,
    expected_creation_id: Option<&str>,
) -> Result<(), RtDbError> {
    validate_db_name(name)?;

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(quiesce_key(name))
        .execute(&mut *tx)
        .await?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT creation_id FROM rtdb_auth.databases WHERE name = $1 FOR UPDATE")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
    let Some((current_id,)) = existing else {
        return Err(RtDbError::not_found("database not found"));
    };
    if let Some(expected) = expected_creation_id
        && expected != current_id
    {
        return Err(RtDbError::conflict(
            "database creation identity does not match; it was deleted and recreated",
        ));
    }

    let epoch_update = sqlx::query(
        "UPDATE rtdb_auth.bootstrap_meta SET value = (value::bigint + 1)::text WHERE key = 'rtdb_admission_epoch'",
    )
    .execute(&mut *tx)
    .await?;
    if epoch_update.rows_affected() != 1 {
        return Err(RtDbError::internal("admission epoch metadata is missing"));
    }

    let schema_name = pg_schema(name);
    // CASCADE drops the schema's tables (`meta`, `mutation_cache_v2`, legacy `mutations`, `scheduled_txns`,
    // `storage`, plus any document tables `push_schema` created) and any indexes
    // or views depending on them, in one statement. The schema identifier is
    // double-quoted because `pg_schema` produces a validated physical name that
    // may contain characters requiring it; the name value itself is bound.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA IF EXISTS \"{schema_name}\" CASCADE"
    )))
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM rtdb_auth.databases WHERE name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM rtdb_auth.machine_tokens WHERE db_name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM rtdb_auth.allowlist WHERE db_name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    // `rtdb.storage_index` is the global id→db map for the unauthenticated
    // public-serve URL; the per-schema `storage` blob table was already dropped
    // by `DROP SCHEMA CASCADE` above, so these rows are now orphans.
    sqlx::query("DELETE FROM rtdb.storage_index WHERE db_name = $1")
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// Maps a Postgres unique-violation (`23505`, the `rtdb_auth.databases` name
/// primary key) or duplicate-schema (`42P06`, a concurrent `CREATE SCHEMA`)
/// error to a `BadRequest` — both are symptoms of two concurrent
/// `create_database` calls racing past the pre-check above for the same
/// name. Any other error passes through as the usual `Internal` mapping.
fn map_duplicate_database_error(err: sqlx::Error) -> RtDbError {
    let is_duplicate = matches!(
        &err,
        sqlx::Error::Database(db_err)
            if matches!(db_err.code().as_deref(), Some("23505") | Some("42P06"))
    );
    if is_duplicate {
        RtDbError::bad_request("database already exists")
    } else {
        RtDbError::from(err)
    }
}

pub async fn database_exists(pool: &PgPool, name: &str) -> Result<bool, RtDbError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT name FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(pool)
            .await?;
    Ok(row.is_some())
}

/// Whether the database's operator froze it read-only. Unknown/missing
/// databases read as FALSE — mirroring `get_anonymous_access`, the gate
/// treats an unregistered name as not frozen, and the write that would create
/// it is rejected at registration time instead.
pub async fn is_read_only(pool: &PgPool, name: &str) -> Result<bool, RtDbError> {
    let (ro,): (bool,) = sqlx::query_as(
        "SELECT COALESCE((
            SELECT read_only FROM rtdb_auth.databases WHERE name = $1
        ), FALSE)",
    )
    .bind(name)
    .fetch_one(pool)
    .await?;
    Ok(ro)
}

/// Sets the per-database read-only freeze flag; returns FALSE when the
/// database is not registered so the admin handler can 404. The flag is read
/// fresh per write by the committer's Mutate arm — no cache, no invalidation.
pub async fn set_read_only(pool: &PgPool, name: &str, read_only: bool) -> Result<bool, RtDbError> {
    let result = sqlx::query("UPDATE rtdb_auth.databases SET read_only = $1 WHERE name = $2")
        .bind(read_only)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

/// Names of every registered database, in a stable order.
pub async fn list_databases(pool: &PgPool) -> Result<Vec<String>, RtDbError> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT name FROM rtdb_auth.databases ORDER BY name")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// Loads the pushed schema for `db` from its `meta` table (`key = 'schema'`),
/// or `Ok(None)` if no schema has been pushed yet.
/// Postgres reports a query against a schema or relation that does not exist
/// as `3F000` (invalid_schema_name) or `42P01` (undefined_table).
fn is_missing_relation(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .and_then(|d| d.code())
        .is_some_and(|c| c == "3F000" || c == "42P01")
}

pub async fn load_schema(pool: &PgPool, db: &str) -> Result<Option<SchemaDef>, RtDbError> {
    validate_db_name(db)?;
    let schema_name = pg_schema(db);

    let row: Option<(serde_json::Value,)> = match sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT value FROM \"{schema_name}\".meta WHERE key = 'schema'"
    )))
    .fetch_optional(pool)
    .await
    {
        Ok(row) => row,
        // A registered db whose backing schema is gone (dropped outside
        // `delete-db`) is indistinguishable from one that was never pushed:
        // answer "no schema" so callers surface NOT_FOUND instead of the
        // generic 500 a raw 3F000/42P01 would map to.
        Err(err) if is_missing_relation(&err) => {
            tracing::debug!(db, "schema load: backing schema is gone");
            None
        }
        Err(err) => return Err(err.into()),
    };

    match row {
        Some((value,)) => {
            let schema = serde_json::from_value(value).map_err(|err| {
                tracing::error!(error = %err, db, "failed to deserialize stored schema");
                RtDbError::internal("failed to read stored schema")
            })?;
            Ok(Some(schema))
        }
        None => Ok(None),
    }
}
