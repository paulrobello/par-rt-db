//! Database lifecycle and shared storage helpers: name validation
//! (`^[a-z][a-z0-9_]{0,32}$`), id generation (UUIDv7), `now_ms` timestamps, the
//! per-database schema cache (`SchemaCache`), and the existence check the per-db
//! tasks (committer, scheduler, reaper, mutation-log cleanup, quota warmer) use
//! to self-terminate when their database is deleted. Physical names are
//! lowercased and length-capped in `ddl` to fit Postgres's 63-byte limit.

use std::sync::Arc;

use rand::Rng;
use rand::rand_core::UnwrapErr;
use rand::rngs::SysRng;
use sha2::{Digest, Sha256};
use sqlx::{Connection, PgConnection, PgPool};

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

/// Per-component bootstrap versions gating the shared-schema DDL fast path.
/// Postgres takes an `ALTER TABLE`/`CREATE INDEX` lock BEFORE evaluating `IF
/// NOT EXISTS`, so a warm no-op bootstrap still flashes `ACCESS EXCLUSIVE`/
/// `SHARE` locks on the shared registry tables — which deadlocks (40P01)
/// against a concurrent seed `INSERT`'s FK check across nextest's
/// process-per-test model, where `serial_test` cannot serialize. The version
/// check returns before opening any transaction on a current schema, so the
/// DDL (and its locks) only ever runs on a genuinely stale or fresh cluster.
/// Bump the version when changing the DDL of its bootstrap fn, or the new
/// statements will be skipped on already-bootstrapped clusters.
const RTDB_AUTH_BOOTSTRAP_VERSION: &str = "7";
const AUDIT_LOG_BOOTSTRAP_VERSION: &str = "1";
const WEBHOOKS_BOOTSTRAP_VERSION: &str = "1";

/// Reads a stamped bootstrap version; `None` when the marker table (or its
/// schema) does not exist yet — the first-boot case.
async fn read_bootstrap_version(pool: &PgPool, key: &str) -> Result<Option<String>, RtDbError> {
    match sqlx::query_scalar::<_, String>(
        "SELECT value FROM rtdb_auth.bootstrap_meta WHERE key = $1",
    )
    .bind(key)
    .fetch_optional(pool)
    .await
    {
        Ok(v) => Ok(v),
        // 42P01 = undefined table/schema: nothing bootstrapped yet.
        Err(e)
            if e.as_database_error()
                .map(|d| d.code().as_deref() == Some("42P01"))
                .unwrap_or(false) =>
        {
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

/// Stamps a bootstrap version inside the open DDL transaction, creating the
/// marker table on first use.
async fn stamp_bootstrap_version(
    conn: &mut PgConnection,
    key: &str,
    version: &str,
) -> Result<(), RtDbError> {
    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb_auth")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.bootstrap_meta \
         (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.bootstrap_meta (key, value) VALUES ($1, $2) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(key)
    .bind(version)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Creates the `rtdb_auth` schema and its tables if they do not already exist.
/// Safe to call on every startup and in every test setup, including
/// concurrently: `pg_advisory_xact_lock` serializes the DDL and is released
/// automatically on commit or rollback. A current schema short-circuits before
/// any transaction (see the bootstrap-version consts above).
pub async fn bootstrap(pool: &PgPool) -> Result<(), RtDbError> {
    if read_bootstrap_version(pool, "rtdb_auth").await? == Some(RTDB_AUTH_BOOTSTRAP_VERSION.into())
    {
        return Ok(());
    }
    let mut tx = pool.begin().await?;

    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(BOOTSTRAP_LOCK_KEY)
        .execute(&mut *tx)
        .await?;

    bootstrap_ddl(&mut tx).await?;
    stamp_bootstrap_version(&mut tx, "rtdb_auth", RTDB_AUTH_BOOTSTRAP_VERSION).await?;
    sqlx::query(
        "INSERT INTO rtdb_auth.bootstrap_meta (key, value) VALUES ('rtdb_admission_epoch', '0') \
         ON CONFLICT (key) DO NOTHING",
    )
    .execute(&mut *tx)
    .await?;

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

    // ENH-022 Stage 4: cross-process rate-limit counters. Only written when
    // `RTDB_MULTI_INSTANCE=true` AND a limit is configured — the single-instance
    // in-memory limiter is the default path and never touches this table. The
    // `minute_bucket` index serves the sweep DELETE; lookups go through the PK.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.rate_counters (
            key_type text NOT NULL,
            key text NOT NULL,
            minute_bucket bigint NOT NULL,
            count bigint NOT NULL,
            PRIMARY KEY (key_type, key, minute_bucket)
        )",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS rate_counters_bucket_idx \
         ON rtdb_auth.rate_counters (minute_bucket)",
    )
    .execute(&mut *conn)
    .await?;

    // ARC-002/ARC-001: spool for forwarded writes, their replies, and
    // oversized cross-replica write sets (ENH-022 Stage 4c).
    // A `pg_notify` payload is capped at 8000 bytes, which a real mutation or a
    // schema push blows through routinely, so the NOTIFY carries only a row id
    // and the body travels through this table. Instance-level, not per-db:
    // forwarding is a property of the fleet, not of a database. Rows are
    // deleted by their consumer and swept on a timer
    // (`forward::run_forward_sweeper`) so a crashed consumer cannot leak them.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.forward_queue (
            id uuid PRIMARY KEY,
            kind text NOT NULL CHECK (kind IN ('request','reply','writeset')),
            target text NOT NULL,
            payload jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )",
    )
    .execute(&mut *conn)
    .await?;
    // The `kind` domain widens as new spool users land (ARC-001 added
    // 'writeset'), and `CREATE TABLE IF NOT EXISTS` cannot revise a constraint
    // on a table that already exists. Drop-then-add is the idempotent form:
    // the DROP is a no-op on a fresh database, and on an existing one it
    // replaces the older, narrower domain.
    sqlx::query(
        "ALTER TABLE rtdb_auth.forward_queue DROP CONSTRAINT IF EXISTS forward_queue_kind_check",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "ALTER TABLE rtdb_auth.forward_queue ADD CONSTRAINT forward_queue_kind_check \
         CHECK (kind IN ('request','reply','writeset'))",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS forward_queue_sweep_idx \
         ON rtdb_auth.forward_queue (kind, target, created_at)",
    )
    .execute(&mut *conn)
    .await?;

    // SEC-103: per-database anonymous-auth gate. An anonymous principal is
    // authorized for a given database ONLY when this flag is TRUE for that db.
    // The instance-wide `RTDB_AUTH_ANONYMOUS_ENABLED` remains a master kill
    // switch (checked at mint + at authorize), so the effective gate is
    // `master_kill AND per_db_flag`. Defaults FALSE — a deployment that turns
    // on anonymous auth instance-wide does NOT open every database to the
    // internet; an operator must opt-in each db explicitly via
    // `PATCH /admin/db/{db}/anonymous-access`. Idempotent ALTER so an existing
    // deployment adds the column on boot.
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS anonymous_enabled boolean NOT NULL DEFAULT FALSE",
    )
    .execute(&mut *conn)
    .await?;

    // Stable, non-reusable creation identity minted per `create_database`. A
    // deleted-and-recreated name gets a fresh value, so `drop_database_if` can
    // refuse to delete a replacement. The volatile default populates existing
    // rows (each distinct) when the column is first added.
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS creation_id text NOT NULL \
         DEFAULT replace(gen_random_uuid()::text, '-', '')",
    )
    .execute(&mut *conn)
    .await?;

    // Per-database read-only freeze: when TRUE, the committer's Mutate arm
    // rejects every client-plane document write with READ_ONLY while reads,
    // subscriptions, admin surfaces, and system writes (scheduled fires,
    // workflow advances, TTL reaping) continue. Toggled via
    // `GET|PATCH /admin/db/{db}/readonly`. Defaults FALSE; idempotent ALTER so
    // an existing deployment adds the column on boot.
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS read_only boolean NOT NULL DEFAULT FALSE",
    )
    .execute(&mut *conn)
    .await?;

    // Per-database quiescence barrier: a persisted generation closes new
    // client-plane writes across restarts. The committer barrier is still
    // required to wait behind already-queued work.
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS quiesced boolean NOT NULL DEFAULT FALSE",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS quiesce_generation bigint NOT NULL DEFAULT 0",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "ALTER TABLE rtdb_auth.databases \
         ADD COLUMN IF NOT EXISTS admission_fence_epoch bigint NOT NULL DEFAULT 0",
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

    // SEC-102: Microsoft's stable subject identifier. The id_token `sub` is
    // immutable per (user, app) pair and tied to the tenant (`tid`) — unlike
    // the `email` claim, which Entra ID populates from the tenant-admin-settable
    // `mail`/`otherMails` attribute and is therefore spoofable (the "nOAuth"
    // account-takeover). Keying Microsoft identity on `microsoft_sub` instead
    // of `email` defeats the takeover: a spoofed email can no longer adopt an
    // existing row via `ON CONFLICT (email) DO UPDATE`. The composite value
    // is `{tid}.{sub}` so a `sub` collision across tenants is never confused
    // for the same identity. The partial unique index tolerates the many NULLs
    // of non-Microsoft users. Idempotent ALTER so a running deployment adds it
    // on boot; new deployments get the column via this same guard.
    sqlx::query("ALTER TABLE rtdb_auth.users ADD COLUMN IF NOT EXISTS microsoft_sub TEXT NULL")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS rtdb_auth_users_microsoft_sub_key \
         ON rtdb_auth.users (microsoft_sub) WHERE microsoft_sub IS NOT NULL",
    )
    .execute(&mut *conn)
    .await?;

    // Stable per-provider subject ids for Google, GitLab and the generic OIDC
    // provider. These three used to key identity on the UNIQUE `email` column
    // alone, so a provider-side email change forked a second account on the
    // next login. Each provider's subject is immutable across an email change
    // (Google's `sub`, GitLab's numeric user `id`, the IdP's `sub`), so it is
    // the durable key — exactly as `github_id`/`apple_sub`/`microsoft_sub`
    // already are for the other three providers.
    //
    // All three are TEXT, including `gitlab_id`: `github_id` is `bigint` for
    // historical reasons and forces a `::bigint` cast in `auth::resolve_user`;
    // keeping these text means the shared resolver needs no per-column cast.
    //
    // Additive and idempotent: nullable columns, no backfill. An existing
    // Google/GitLab/OIDC user has a NULL subject, so their first login after
    // the upgrade takes the resolver's email-link step and adopts the existing
    // row instead of inserting a new one; every later login matches on the
    // now-populated subject. The partial unique index tolerates the many NULLs
    // of users who signed in through a different provider.
    for column in ["google_sub", "gitlab_id", "oidc_sub"] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE rtdb_auth.users ADD COLUMN IF NOT EXISTS {column} TEXT NULL"
        )))
        .execute(&mut *conn)
        .await?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS rtdb_auth_users_{column}_key \
             ON rtdb_auth.users ({column}) WHERE {column} IS NOT NULL"
        )))
        .execute(&mut *conn)
        .await?;
    }

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

    // The provider's free-form display name, surfaced as `AuthedUser.name`.
    // Display-only: identity still keys on the per-provider subject columns
    // above, so this is neither unique nor indexed and may legitimately be
    // NULL (Apple rarely relays a name; a GitHub user may never have set one).
    // Additive and idempotent, with no backfill — an existing user's name
    // fills in on their next sign-in.
    sqlx::query("ALTER TABLE rtdb_auth.users ADD COLUMN IF NOT EXISTS name TEXT NULL")
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

    // SEC-120: admin-key login mints a hashed, revocable session row here (not
    // the raw admin_key). Parallel to `sessions` but with no FK to `users` —
    // the admin key is a configured secret, not a user row, and a synthetic
    // user would pollute the OAuth `users` table and its `email`/`github_id`
    // uniqueness constraints. Surfaced in `/admin/sessions` and revocable via
    // `DELETE /admin/sessions/{token_hash}` alongside OAuth sessions.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.admin_sessions (
            token_hash text PRIMARY KEY,
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

    // Host machine authorization (Task 1): the token's optional host
    // capability as JSONB. NULL means legacy full-access semantics — every
    // pre-existing row reads back exactly as before, and legacy mint callers
    // store NULL. JSONB (not TEXT) so the stored capability stays queryable;
    // it is written only from a validated `HostCapability` and parsed back
    // with the same strict deserializer.
    sqlx::query(
        "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS capability JSONB NULL",
    )
    .execute(&mut *conn)
    .await?;

    // Enrollment grants (host-machine-auth Task 1): single-use, short-TTL
    // grants whose `token_hash` is the sha256 digest of the one-time grant
    // secret (the plaintext is returned once at creation and never stored).
    // Redemption claims the row with a conditional UPDATE inside the same
    // transaction that inserts the machine token, so concurrent redemptions
    // yield exactly one success.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.enrollment_grants (
            id text PRIMARY KEY,
            db_name text NOT NULL,
            name text NOT NULL,
            token_hash text UNIQUE NOT NULL,
            capability jsonb NOT NULL,
            expires_at bigint NOT NULL,
            token_expires_at bigint,
            redeemed_at bigint,
            revoked boolean NOT NULL DEFAULT false,
            created_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    // Old grants are digest-only and carry no useful authority without their
    // parent database. Remove orphans before validating the lifecycle FK.
    sqlx::query("DELETE FROM rtdb_auth.enrollment_grants g WHERE NOT EXISTS (SELECT 1 FROM rtdb_auth.databases d WHERE d.name = g.db_name)")
        .execute(&mut *conn).await?;
    sqlx::query("DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'rtdb_auth.enrollment_grants'::regclass AND conname = 'enrollment_grants_database_fk') THEN
            ALTER TABLE rtdb_auth.enrollment_grants ADD CONSTRAINT enrollment_grants_database_fk FOREIGN KEY (db_name) REFERENCES rtdb_auth.databases(name) ON DELETE CASCADE;
        END IF;
    END $$")
        .execute(&mut *conn).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.admins (
            email text PRIMARY KEY,
            github_id bigint,
            added_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    // ENH-022 Stage 1: single-use OAuth login state in Postgres so a login
    // begun on one replica can complete the callback on another. `/begin`
    // inserts a `pending` row; `/callback` flips `pending` → `claiming`
    // (UPDATE ... WHERE status = 'pending' RETURNING — single-use enforced by
    // the DB, not a Mutex) then sets the terminal outcome; `/auth/state`
    // consumes a terminal row in one statement. A background sweep deletes
    // rows older than the 10-minute TTL (closes the SEC-132 unbounded-map
    // note). A row is mutated only through its lifecycle; there is no second
    // writer onto document tables, so the committer's single-writer
    // invariant is untouched.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.oauth_states (
            state       text PRIMARY KEY,
            provider    text NOT NULL,
            status      text NOT NULL DEFAULT 'pending',
            session_token text,
            created_at  bigint NOT NULL,
            expires_at  bigint NOT NULL,
            consumed_at bigint,
            cookie_mode boolean NOT NULL DEFAULT false,
            delivery    text NOT NULL DEFAULT 'default',
            revoke_secret_verifier text,
            callback_settled_at bigint
        )",
    )
    .execute(&mut *conn)
    .await?;
    // FM-27: retrofit for existing installs — the anon-merge binding column
    // (NULL when the login began from a non-anonymous session). Same pattern
    // as storage.rs's `owner_id` retrofit.
    sqlx::query("ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS anon_user_id text")
        .execute(&mut *conn)
        .await?;
    // SEC-207: retrofit — a cookie-mode begin keeps the session token out of
    // the `/auth/state` poll body (the HttpOnly cookie is the only carrier).
    sqlx::query(
        "ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS cookie_mode boolean NOT NULL DEFAULT false",
    )
    .execute(&mut *conn)
    .await?;
    // Bearer delivery (opt-in cross-tab hardening): additive retrofit —
    // `delivery` generalizes `cookie_mode` ('default' | 'cookie' | 'bearer').
    // Existing rows read 'default', byte-identical to their old
    // cookie_mode=false behavior; `cookie_mode` stays NOT NULL and is kept in
    // sync on INSERT with delivery='cookie' so a rolling multi-instance
    // deploy's older replicas (still reading the bool) see the right value.
    sqlx::query(
        "ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS delivery text NOT NULL DEFAULT 'default'",
    )
    .execute(&mut *conn)
    .await?;
    // Client-only revoke capability for bearer rows: the plaintext secret is
    // minted at /begin and delivered ONLY in the begin response body (never a
    // URL, cookie, provider redirect, or proxy log); only its sha256 verifier
    // is stored — same convention as `sessions.token_hash`. NULL for
    // non-bearer rows.
    sqlx::query(
        "ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS revoke_secret_verifier text",
    )
    .execute(&mut *conn)
    .await?;
    // Durable callback-settlement evidence: set by `set_outcome` (the
    // callback finished, outcome recorded) and by the fenced roll-forward
    // (mint recorded AND its tracked session deleted). A revoked row with
    // this NULL may still be mid-mint — revocation retries answer
    // `unconfirmed` until it is set; never inferred from elapsed time.
    sqlx::query(
        "ALTER TABLE rtdb_auth.oauth_states ADD COLUMN IF NOT EXISTS callback_settled_at bigint",
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
    if read_bootstrap_version(pool, "audit_log").await? == Some(AUDIT_LOG_BOOTSTRAP_VERSION.into())
    {
        return Ok(());
    }
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

    stamp_bootstrap_version(&mut tx, "audit_log", AUDIT_LOG_BOOTSTRAP_VERSION).await?;

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
    if read_bootstrap_version(pool, "webhooks").await? == Some(WEBHOOKS_BOOTSTRAP_VERSION.into()) {
        return Ok(());
    }
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

    // Additive column (SEC-115): per-webhook HMAC secret used to sign each
    // delivery (`X-Rtdb-Signature`). Nullable so the ALTER is idempotent and
    // pre-existing rows survive; `webhook::backfill_webhook_secrets` (called
    // from boot after this) generates a value for every NULL row, and
    // `create_webhook` always sets one, so the column is effectively non-null
    // once boot completes.
    sqlx::query("ALTER TABLE rtdb.webhooks ADD COLUMN IF NOT EXISTS secret TEXT")
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

    stamp_bootstrap_version(&mut tx, "webhooks", WEBHOOKS_BOOTSTRAP_VERSION).await?;

    tx.commit().await?;
    Ok(())
}

/// Advisory lock key serializing extension installation. `CREATE EXTENSION IF
/// NOT EXISTS` is check-then-insert and not atomic under concurrency: two
/// sessions that both see the extension absent both insert into `pg_extension`,
/// and the loser fails on `pg_extension_name_index` — observed in CI when
/// parallel test binaries create databases simultaneously. Shared with
/// `ddl::push_schema`'s backfill so every pairing (create↔create,
/// create↔push, push↔push) serializes on the same key.
pub const EXTENSION_LOCK_KEY: i64 = 727_002;

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

/// Mints a 64-hex-char (256-bit) token from the OS CSPRNG. Backs session
/// tokens, machine tokens, OAuth state, and CSRF nonces — all security
/// values (SEC-206: the OS CSPRNG, `SysRng`, by contract, not just in practice).
pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    UnwrapErr(SysRng).fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// In-memory cache of pushed schemas, keyed by database name, backed by Postgres
/// as the source of truth (see `load_schema`). Bounded by a max entry count via
/// `moka` (ARC-119): a multi-tenant instance that creates and drops databases
/// no longer grows this map indefinitely — `moka`'s LRU eviction reclaims the
/// least-recently-used schema when the cap is reached, and the next `get` for an
/// evicted db transparently reloads from Postgres. `moka::future::Cache` is
/// cheaply cloneable (internal `Arc` handle), so `#[derive(Clone)]` is a ref
/// bump — same shape the in-tree `image_transform::TransformCache` uses.
#[derive(Clone)]
pub struct SchemaCache {
    cache: moka::future::Cache<String, Arc<SchemaDef>>,
}

/// Default max entry count when constructed via [`SchemaCache::new`] or
/// [`SchemaCache::default`]. Production wiring ([`SchemaCache::with_capacity`])
/// reads `RTDB_SCHEMA_CACHE_MAX_ENTRIES` so operators of very large multi-tenant
/// instances can raise it.
const DEFAULT_SCHEMA_CACHE_MAX_ENTRIES: u64 = 1024;

impl SchemaCache {
    /// Constructs a cache with the default entry cap
    /// ([`DEFAULT_SCHEMA_CACHE_MAX_ENTRIES`]). Kept parameterless so existing
    /// test call sites are unchanged; production uses [`Self::with_capacity`].
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SCHEMA_CACHE_MAX_ENTRIES)
    }

    /// Constructs a cache bounded to `max_entries` schemas (LRU-evicted past the
    /// cap). `max_entries == 0` is treated as unbounded — moka's behavior when
    /// no `max_capacity` is set — preserving the prior unbounded semantics for
    /// operators who explicitly disable eviction.
    pub fn with_capacity(max_entries: u64) -> Self {
        let mut builder = moka::future::Cache::builder();
        if max_entries > 0 {
            builder = builder.max_capacity(max_entries);
        }
        Self {
            cache: builder.build(),
        }
    }

    pub async fn get(&self, pool: &PgPool, db: &str) -> Result<Arc<SchemaDef>, RtDbError> {
        if let Some(schema) = self.cache.get(db).await {
            return Ok(schema);
        }

        let schema = load_schema(pool, db)
            .await?
            .ok_or_else(|| RtDbError::not_found("no schema pushed"))?;
        let schema = Arc::new(schema);
        self.cache.insert(db.to_string(), schema.clone()).await;
        Ok(schema)
    }

    pub async fn put(&self, db: &str, schema: SchemaDef) {
        self.cache.insert(db.to_string(), Arc::new(schema)).await;
    }

    /// Drops any cached schema for `db`, forcing the next `get` to reload from
    /// Postgres. Used when a write to `db`'s schema may have partially applied
    /// outside the cache's knowledge (e.g. `snapshot::import_database` failing
    /// after its internal `push_schema` already committed) — safe to call even
    /// when nothing is cached.
    pub async fn invalidate(&self, db: &str) {
        self.cache.invalidate(db).await;
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
