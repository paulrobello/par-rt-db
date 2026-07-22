use std::collections::HashMap;
use std::sync::Arc;

use rand::RngCore;
use sha2::{Digest, Sha256};
use sqlx::{PgConnection, PgPool};
use tokio::sync::RwLock;

use crate::ddl::pg_schema;
use crate::error::RtDbError;
use crate::schema::SchemaDef;

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

/// Advisory lock key serializing `bootstrap`'s DDL. Postgres's `IF NOT EXISTS`
/// DDL is not atomic under concurrent sessions (a concurrent `CREATE SCHEMA IF
/// NOT EXISTS` can still race into a duplicate-object error), which matters
/// here because integration tests call `bootstrap` from many parallel threads
/// against the same shared dev Postgres.
const BOOTSTRAP_LOCK_KEY: i64 = 727_001;

/// Creates the `rtdb_auth` schema and its tables if they do not already exist.
/// Safe to call on every startup and in every test setup, including
/// concurrently: `pg_advisory_xact_lock` serializes the DDL and is released
/// automatically on commit or rollback.
pub async fn bootstrap(pool: &PgPool) -> Result<(), RtDbError> {
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    bootstrap_ddl(&mut tx).await?;

    tx.commit().await?;
    Ok(())
}

async fn bootstrap_ddl(conn: &mut PgConnection) -> Result<(), RtDbError> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb_auth")
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.databases (
            name text PRIMARY KEY,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.users (
            id text PRIMARY KEY,
            github_id bigint UNIQUE,
            login text NOT NULL,
            email text UNIQUE,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.sessions (
            token_hash text PRIMARY KEY,
            user_id text NOT NULL REFERENCES rtdb_auth.users(id),
            expires_at bigint NOT NULL,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.allowlist (
            db_name text NOT NULL,
            email text NOT NULL,
            PRIMARY KEY (db_name, email)
        )",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.machine_tokens (
            id text PRIMARY KEY,
            db_name text NOT NULL,
            name text NOT NULL,
            token_hash text UNIQUE NOT NULL,
            revoked boolean NOT NULL DEFAULT false,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Registers a new tenant database: validates `name`, creates its Postgres schema
/// and `meta` table, and records it in the `rtdb_auth.databases` registry, all in
/// one transaction.
pub async fn create_database(pool: &PgPool, name: &str) -> Result<(), RtDbError> {
    validate_db_name(name)?;

    let mut tx = pool.begin().await?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT name FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
    if existing.is_some() {
        return Err(RtDbError::bad_request("database already exists"));
    }

    let schema_name = pg_schema(name);
    sqlx::query(&format!("CREATE SCHEMA \"{schema_name}\""))
        .execute(&mut *tx)
        .await
        .map_err(map_duplicate_database_error)?;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".meta (key text PRIMARY KEY, value jsonb NOT NULL)"
    ))
    .execute(&mut *tx)
    .await?;

    sqlx::query("INSERT INTO rtdb_auth.databases (name, created_at) VALUES ($1, $2)")
        .bind(name)
        .bind(now_ms())
        .execute(&mut *tx)
        .await
        .map_err(map_duplicate_database_error)?;

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

/// Names of every registered database, in a stable order.
pub async fn list_databases(pool: &PgPool) -> Result<Vec<String>, RtDbError> {
    let rows: Vec<(String,)> = sqlx::query_as("SELECT name FROM rtdb_auth.databases ORDER BY name")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|(name,)| name).collect())
}

/// Loads the pushed schema for `db` from its `meta` table (`key = 'schema'`),
/// or `Ok(None)` if no schema has been pushed yet.
pub async fn load_schema(pool: &PgPool, db: &str) -> Result<Option<SchemaDef>, RtDbError> {
    validate_db_name(db)?;
    let schema_name = pg_schema(db);

    let row: Option<(serde_json::Value,)> = sqlx::query_as(&format!(
        "SELECT value FROM \"{schema_name}\".meta WHERE key = 'schema'"
    ))
    .fetch_optional(pool)
    .await?;

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

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn new_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

pub fn sha256_hex(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// In-memory cache of pushed schemas, keyed by database name, backed by Postgres
/// as the source of truth (see `load_schema`).
#[derive(Clone)]
pub struct SchemaCache(Arc<RwLock<HashMap<String, Arc<SchemaDef>>>>);

impl SchemaCache {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub async fn get(&self, pool: &PgPool, db: &str) -> Result<Arc<SchemaDef>, RtDbError> {
        if let Some(schema) = self.0.read().await.get(db) {
            return Ok(schema.clone());
        }

        let schema = load_schema(pool, db)
            .await?
            .ok_or_else(|| RtDbError::not_found("no schema pushed"))?;
        let schema = Arc::new(schema);
        self.0.write().await.insert(db.to_string(), schema.clone());
        Ok(schema)
    }

    pub async fn put(&self, db: &str, schema: SchemaDef) {
        self.0
            .write()
            .await
            .insert(db.to_string(), Arc::new(schema));
    }

    /// Drops any cached schema for `db`, forcing the next `get` to reload from
    /// Postgres. Used when a write to `db`'s schema may have partially applied
    /// outside the cache's knowledge (e.g. `snapshot::import_database` failing
    /// after its internal `push_schema` already committed) — safe to call even
    /// when nothing is cached.
    pub async fn invalidate(&self, db: &str) {
        self.0.write().await.remove(db);
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
