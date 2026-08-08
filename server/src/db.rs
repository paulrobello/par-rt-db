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
            apple_sub text UNIQUE,
            login text NOT NULL,
            email text UNIQUE,
            anonymous boolean NOT NULL DEFAULT FALSE,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    // ENH-001: Apple's stable identifier (`apple_sub`), mirroring `github_id`.
    // Apple may relay the email through @privaterelay.appleid.com (and rotates
    // it if the user re-hides), so `sub` is the durable key, not email. The
    // partial unique index tolerates the many NULLs of non-Apple users.
    // Idempotent so an existing deployment adds the column on boot; a new
    // deployment already has it (and the inline UNIQUE) from CREATE TABLE.
    sqlx::query("ALTER TABLE rtdb_auth.users ADD COLUMN IF NOT EXISTS apple_sub TEXT NULL")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS rtdb_auth_users_apple_sub_key \
         ON rtdb_auth.users (apple_sub) WHERE apple_sub IS NOT NULL",
    )
    .execute(&mut *conn)
    .await?;

    // Anonymous auth (2026-08-08): marks a user row minted by
    // `POST /auth/anonymous` (no OAuth identity, no email). `false` for every
    // existing/OAuth user. Idempotent ALTER so a running deployment adds it on
    // boot; a new deployment gets it inline below.
    sqlx::query("ALTER TABLE rtdb_auth.users ADD COLUMN IF NOT EXISTS anonymous BOOLEAN NOT NULL DEFAULT FALSE")
        .execute(&mut *conn)
        .await?;
    // Also ensure new tables have it inline (the CREATE TABLE above predates it).
    sqlx::query("ALTER TABLE rtdb_auth.users ALTER COLUMN anonymous SET DEFAULT FALSE")
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

    // ENH-005: additive capability columns on machine_tokens. Idempotent alters
    // so existing deployments pick them up on boot; NULL/empty/false defaults
    // preserve full-access semantics for tokens minted before the upgrade.
    sqlx::query(
        "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS expires_at BIGINT NULL",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS read_only BOOLEAN NOT NULL DEFAULT false",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query("ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS tables TEXT[] NULL")
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.admins (
            email text PRIMARY KEY,
            github_id bigint,
            added_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb")
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.storage_index (
            id      text PRIMARY KEY,
            db_name text NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    // Single-row hot-config store. The CHECK + DEFAULT pin it to id = 1 so the
    // upsert in `config::save_hot` is always `WHERE id = 1` / `VALUES (1, $1)`.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_config (
            id int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            hot jsonb NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    Ok(())
}

/// Creates the global `rtdb.audit_log` table idempotently. Called at boot only
/// when `Config::audit_log_enabled` — the table is permitted to not exist when
/// audit is off (and the admin endpoint then returns an empty list). One row
/// per durable `DocOp`, written from the committer's two tap sites
/// (`handle_mutate`/`handle_scheduled`); the durable counterpart to the
/// ephemeral `OpFeed` ring. The `(db, ts_ms DESC)` index backs the
/// `GET /admin/audit?db=...` newest-first scan without a full-table sort.
pub async fn ensure_audit_table(pool: &PgPool) -> Result<(), RtDbError> {
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb")
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.audit_log (
            id        BIGSERIAL PRIMARY KEY,
            ts_ms     BIGINT NOT NULL,
            db        TEXT NOT NULL,
            tbl       TEXT NOT NULL,
            op        TEXT,
            doc_id    TEXT NOT NULL,
            principal TEXT,
            source    TEXT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS audit_log_db_ts_idx \
         ON rtdb.audit_log (db, ts_ms DESC)",
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

/// Creates the webhook registry + delivery-outbox tables idempotently. Called
/// at boot only when `Config::webhooks_enabled` — both tables are permitted to
/// not exist when webhooks are off (the admin endpoints return empty lists and
/// the committer tap is skipped). The committer's enqueue and the delivery
/// worker both read/write here; the `(status, next_attempt)` index backs the
/// worker's due-row scan without a full-table scan, and the `(db)` index backs
/// the admin list-by-db lookup. `tbl` NULL means "all tables"; `events`
/// contains op names (`insert`/`patch`/`replace`/`delete`/`upsert`) or the
/// single element `*` to match every event.
pub async fn ensure_webhooks_tables(pool: &PgPool) -> Result<(), RtDbError> {
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb")
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.webhooks (
            id         BIGSERIAL PRIMARY KEY,
            db         TEXT NOT NULL,
            tbl        TEXT,
            url        TEXT NOT NULL,
            events     TEXT[] NOT NULL,
            created_at BIGINT NOT NULL
        )",
    )
    .execute(&mut *tx)
    .await?;

    // Additive column (ENH-003): existing webhooks default to enabled so the
    // delivery worker's behavior on pre-flag rows is unchanged.
    sqlx::query(
        "ALTER TABLE rtdb.webhooks \
         ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true",
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query("CREATE INDEX IF NOT EXISTS webhooks_db_idx ON rtdb.webhooks (db)")
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.webhook_deliveries (
            id           BIGSERIAL PRIMARY KEY,
            webhook_id   BIGINT NOT NULL REFERENCES rtdb.webhooks(id) ON DELETE CASCADE,
            payload      JSONB NOT NULL,
            attempts     INT NOT NULL DEFAULT 0,
            next_attempt BIGINT NOT NULL,
            status       TEXT NOT NULL,
            last_error   TEXT
        )",
    )
    .execute(&mut *tx)
    .await?;

    sqlx::query(
        "CREATE INDEX IF NOT EXISTS webhook_deliveries_due_idx \
         ON rtdb.webhook_deliveries (status, next_attempt)",
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
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
    // Extensions are database-level in Postgres, and every par-rt-db "database" is
    // a schema in the single `rtdb` Postgres database — so this installs `vector`
    // once into `rtdb`, available to all schemas. `IF NOT EXISTS` makes it idempotent.
    sqlx::query("CREATE EXTENSION IF NOT EXISTS vector")
        .execute(&mut *tx)
        .await?;
    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".meta (key text PRIMARY KEY, value jsonb NOT NULL)"
    ))
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".mutations (
            mut_id text PRIMARY KEY,
            result jsonb NOT NULL,
            expires_at bigint NOT NULL
        )"
    ))
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".scheduled_txns (
            id          text PRIMARY KEY,
            kind        text NOT NULL,
            due_at      bigint NOT NULL,
            txn         jsonb NOT NULL,
            cron        text,
            status      text NOT NULL,
            last_error  text,
            created_at  bigint NOT NULL,
            fired_count bigint NOT NULL DEFAULT 0
        )"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX \"{schema_name}_scheduled_due_idx\"
         ON \"{schema_name}\".scheduled_txns (status, due_at)"
    ))
    .execute(&mut *tx)
    .await?;

    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".storage (
            id           text PRIMARY KEY,
            sha256       text NOT NULL,
            size         bigint NOT NULL,
            content_type text,
            bytes        bytea NOT NULL,
            created_at   bigint NOT NULL
        )"
    ))
    .execute(&mut *tx)
    .await?;
    // Content-addressed dedup: one blob per sha256 so re-uploaded bytes reuse
    // the existing id/URL (ENH-008). See `storage::put` / `ensure_table`.
    sqlx::query(&format!(
        "CREATE UNIQUE INDEX \"{schema_name}_storage_sha256_idx\"
         ON \"{schema_name}\".storage (sha256)"
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

/// Drops a tenant database: validates `name`, requires it exists, then in one
/// transaction `DROP SCHEMA ... CASCADE`s the per-db Postgres schema (removing
/// `meta`, `mutations`, `scheduled_txns`, `storage`, and every document table
/// `push_schema` created) and deletes its row in `rtdb_auth.databases` plus
/// its per-db rows in `rtdb_auth.machine_tokens`, `rtdb_auth.allowlist`, and
/// `rtdb.storage_index`. `IF EXISTS` on the schema drop keeps a partially-
/// deleted db (schema already gone) recoverable — the DELETEs still run and
/// the registry row is removed. In-memory eviction (schema cache,
/// subscriptions, committer channel) is the caller's responsibility — see
/// `admin::delete_db`. Durable cleanup only happens here.
pub async fn drop_database(pool: &PgPool, name: &str) -> Result<(), RtDbError> {
    validate_db_name(name)?;

    let mut tx = pool.begin().await?;

    let existing: Option<(String,)> =
        sqlx::query_as("SELECT name FROM rtdb_auth.databases WHERE name = $1")
            .bind(name)
            .fetch_optional(&mut *tx)
            .await?;
    if existing.is_none() {
        return Err(RtDbError::not_found("database not found"));
    }

    let schema_name = pg_schema(name);
    // CASCADE drops the schema's tables (`meta`, `mutations`, `scheduled_txns`,
    // `storage`, plus any document tables `push_schema` created) and any indexes
    // or views depending on them, in one statement. The schema identifier is
    // double-quoted because `pg_schema` produces a validated physical name that
    // may contain characters requiring it; the name value itself is bound.
    sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema_name}\" CASCADE"))
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
