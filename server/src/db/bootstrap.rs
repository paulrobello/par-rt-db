//! Boot-time global DDL: the `rtdb_auth` registry schema, the shared
//! `rtdb.audit_log` and webhook tables, and the advisory lock keys that
//! serialize bootstrap DDL and extension installation across concurrent
//! sessions and parallel test binaries.

use sqlx::{PgConnection, PgPool};

use crate::error::RtDbError;

/// Advisory lock key serializing `bootstrap`'s DDL. Postgres's `IF NOT EXISTS`
/// DDL is not atomic under concurrent sessions (a concurrent `CREATE SCHEMA IF
/// NOT EXISTS` can still race into a duplicate-object error), which matters
/// here because integration tests call `bootstrap` from many parallel threads
/// against the same shared dev Postgres.
pub(super) const BOOTSTRAP_LOCK_KEY: i64 = 727_001;

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
const RTDB_AUTH_BOOTSTRAP_VERSION: &str = "9";
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
            created_at bigint NOT NULL,
            session_id text NULL,
            revoke_secret_hash text NULL,
            revoked_at bigint NULL
        )",
    )
    .execute(&mut *conn)
    .await?;

    // Durable per-session revoke capability (opt-in, bearer-mode logins only):
    // the identifier and the sha256 verifier of the begin-time revoke secret
    // are stamped onto the row at settlement; `revoked_at` marks the fence.
    // NULL on every pre-existing row and every non-bearer login — those
    // sessions answer `unknown` to the capability endpoint, exactly as before
    // this feature. A revoked row KEEPS its token_hash (the PRIMARY KEY cannot
    // be nulled and the digest is harmless) and becomes a tombstone: every
    // session resolver excludes `revoked_at IS NOT NULL`, so the token is dead
    // everywhere, while the retained capability answers revocation retries
    // until the TTL sweep prunes the row. `session_id` is a fresh random token
    // per login, so the unique index never collides and Postgres treats its
    // NULLs as distinct.
    sqlx::query("ALTER TABLE rtdb_auth.sessions ADD COLUMN IF NOT EXISTS session_id text NULL")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "ALTER TABLE rtdb_auth.sessions ADD COLUMN IF NOT EXISTS revoke_secret_hash text NULL",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query("ALTER TABLE rtdb_auth.sessions ADD COLUMN IF NOT EXISTS revoked_at bigint NULL")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS sessions_session_id_idx \
         ON rtdb_auth.sessions (session_id)",
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
