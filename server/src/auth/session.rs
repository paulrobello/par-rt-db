//! Interactive session persistence on `rtdb_auth.sessions`: mint, lookup by
//! sha256 token hash, list, and revoke. `expires_at` is enforced on every
//! `authorize` re-run, so an expired session dies on its next op.

use serde::Serialize;
use sqlx::PgPool;

use crate::auth::Principal;
use crate::db::{now_ms, random_token, sha256_hex};
use crate::error::RtDbError;

const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000;

/// Mints a new session for `user_id`, valid for `ttl_days`. The plaintext
/// token is returned exactly once, here — only its sha256 digest is
/// persisted (see `rtdb_auth.sessions.token_hash`). Callers must not log the
/// returned plaintext.
pub async fn create_session(
    pool: &PgPool,
    user_id: &str,
    ttl_days: i64,
) -> Result<String, RtDbError> {
    let token = random_token();
    let hash = sha256_hex(&token);
    let now = now_ms();
    let expires_at = now + ttl_days * MS_PER_DAY;

    sqlx::query(
        "INSERT INTO rtdb_auth.sessions (token_hash, user_id, expires_at, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(&hash)
    .bind(user_id)
    .bind(expires_at)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(token)
}

/// SEC-120: mints a hashed, revocable admin-key login session. The plaintext
/// token is returned exactly once and goes into the HttpOnly cookie (never the
/// raw `config.admin_key`); only its sha256 digest is persisted in
/// `rtdb_auth.admin_sessions`. Mirrors `create_session` but carries no
/// `user_id` — the admin key is a configured secret, not an OAuth user. The row
/// appears in `/admin/sessions` and is revocable via `DELETE /admin/sessions/{hash}`,
/// so a stolen cookie is revocable without rotating the admin key (which would
/// also invalidate outstanding signed storage URLs).
pub async fn create_admin_session(pool: &PgPool, ttl_days: i64) -> Result<String, RtDbError> {
    let token = random_token();
    let hash = sha256_hex(&token);
    let now = now_ms();
    let expires_at = now + ttl_days * MS_PER_DAY;

    sqlx::query(
        "INSERT INTO rtdb_auth.admin_sessions (token_hash, expires_at, created_at) \
         VALUES ($1, $2, $3)",
    )
    .bind(&hash)
    .bind(expires_at)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(token)
}

/// SEC-120: resolves an admin-key session token against `rtdb_auth.admin_sessions`.
/// Returns `true` if a valid (non-expired) row matches; lazily deletes an expired
/// row so it never needs a separate sweep. `false` if absent or expired (the
/// caller falls through to the OAuth-user path).
pub async fn resolve_admin_session(pool: &PgPool, token: &str) -> Result<bool, RtDbError> {
    let hash = sha256_hex(token);
    let row: Option<(i64,)> =
        sqlx::query_as("SELECT expires_at FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
            .bind(&hash)
            .fetch_optional(pool)
            .await?;

    let Some((expires_at,)) = row else {
        return Ok(false);
    };
    if expires_at < now_ms() {
        sqlx::query("DELETE FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
            .bind(&hash)
            .execute(pool)
            .await?;
        return Ok(false);
    }
    Ok(true)
}

/// Resolves a session token to a `Principal::User`. `Ok(None)` if the token
/// is absent or expired; an expired row is deleted lazily as part of this
/// call so it never needs a separate sweep.
///
/// `u.github_id` and `u.login` are read alongside the session so the resolved
/// principal carries GitHub identity. `login` is treated as a GitHub handle
/// only when paired with a `github_id` — for a Google-only user `login` holds
/// the display name, so `github_login` stays `None`.
///
/// `u.name` is the provider's display name, read here so every identity
/// surface built from a principal (`GET /auth/me`, the WS `authOk` frame,
/// presence members) carries it. `None` for a user whose provider supplied
/// none and for anonymous users.
pub async fn resolve_session(pool: &PgPool, token: &str) -> Result<Option<Principal>, RtDbError> {
    let hash = sha256_hex(token);

    // `(user_id, expires_at, email, github_id, login, anonymous, name)`.
    type SessionUserRow = (
        String,
        i64,
        Option<String>,
        Option<i64>,
        String,
        bool,
        Option<String>,
    );
    let row: Option<SessionUserRow> = sqlx::query_as(
        "SELECT s.user_id, s.expires_at, u.email, u.github_id, u.login, u.anonymous, u.name \
         FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
         WHERE s.token_hash = $1 AND s.revoked_at IS NULL",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    let Some((user_id, expires_at, email, github_id, login, anonymous, name)) = row else {
        return Ok(None);
    };

    if expires_at < now_ms() {
        sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
            .bind(&hash)
            .execute(pool)
            .await?;
        return Ok(None);
    }

    Ok(Some(Principal::User {
        user_id,
        email,
        name,
        expires_at,
        anonymous,
        github_id,
        github_login: github_id.is_some().then_some(login),
        session_hash: Some(hash),
    }))
}

/// Deletes a session by its plaintext token — the logout resolver. Not an
/// error if the token doesn't exist — logout is idempotent (see
/// `auth::provider::logout`).
///
/// Capability-bearing rows (`revoke_secret_hash IS NOT NULL`) are TOMBSTONED
/// rather than deleted: the row is the only durable anchor
/// `POST /auth/session/revoke` resolves against, and hard-deleting it turns
/// every future capability retry into `unknown` — indistinguishable from
/// never-matched. The tombstone sets the same `revoked_at` marker every
/// resolver excludes, so the session dies exactly as a delete would; only the
/// anchor survives (until the TTL sweep prunes it). Rows without a capability
/// keep the plain delete, and an already-tombstoned row is left untouched.
pub async fn delete_session(pool: &PgPool, token: &str) -> Result<(), RtDbError> {
    let hash = sha256_hex(token);
    tombstone_or_delete_by_hash(pool, &hash).await
}

/// The shared revocation-by-deletion resolver keyed on `token_hash`: tombstone
/// live capability rows, delete the rest. Logout (by plaintext token), the
/// admin revoke-one path, and `delete_sessions_for_user` all resolve through
/// this so every deletion path preserves the capability anchor the same way
/// `POST /auth/session/revoke` does.
async fn tombstone_or_delete_by_hash(pool: &PgPool, hash: &str) -> Result<(), RtDbError> {
    let now = now_ms();
    sqlx::query(
        "UPDATE rtdb_auth.sessions \
         SET revoked_at = $2 \
         WHERE token_hash = $1 AND revoked_at IS NULL AND revoke_secret_hash IS NOT NULL",
    )
    .bind(hash)
    .bind(now)
    .execute(pool)
    .await?;
    sqlx::query(
        "DELETE FROM rtdb_auth.sessions WHERE token_hash = $1 AND revoke_secret_hash IS NULL",
    )
    .bind(hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes one session by its token_hash (the admin revoke-one path). SEC-120:
/// tries both `sessions` (OAuth user) and `admin_sessions` (admin-key login) so
/// either kind is revocable through the same `DELETE /admin/sessions/{hash}`.
/// Capability-bearing rows are tombstoned rather than deleted (same anchor
/// rule as `delete_session`); admin-key rows have no capability and keep the
/// plain delete. Idempotent: returns 1 when the row existed, 0 if already
/// gone — never an error.
pub async fn delete_session_by_hash(pool: &PgPool, token_hash: &str) -> Result<u64, RtDbError> {
    let now = now_ms();
    let r1 = sqlx::query(
        "UPDATE rtdb_auth.sessions \
         SET revoked_at = $2 \
         WHERE token_hash = $1 AND revoked_at IS NULL AND revoke_secret_hash IS NOT NULL",
    )
    .bind(token_hash)
    .bind(now)
    .execute(pool)
    .await?;
    let r2 = sqlx::query(
        "DELETE FROM rtdb_auth.sessions WHERE token_hash = $1 AND revoke_secret_hash IS NULL",
    )
    .bind(token_hash)
    .execute(pool)
    .await?;
    let r3 = sqlx::query("DELETE FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
        .bind(token_hash)
        .execute(pool)
        .await?;
    Ok(r1.rows_affected() + r2.rows_affected() + r3.rows_affected())
}

/// Deletes every session for `user_id` (the admin revoke-all path). Idempotent.
/// Capability-bearing rows are tombstoned rather than deleted (same anchor
/// rule as `delete_session`).
pub async fn delete_sessions_for_user(pool: &PgPool, user_id: &str) -> Result<u64, RtDbError> {
    let now = now_ms();
    let r1 = sqlx::query(
        "UPDATE rtdb_auth.sessions \
         SET revoked_at = $2 \
         WHERE user_id = $1 AND revoked_at IS NULL AND revoke_secret_hash IS NOT NULL",
    )
    .bind(user_id)
    .bind(now)
    .execute(pool)
    .await?;
    let r2 = sqlx::query(
        "DELETE FROM rtdb_auth.sessions WHERE user_id = $1 AND revoke_secret_hash IS NULL",
    )
    .bind(user_id)
    .execute(pool)
    .await?;
    Ok(r1.rows_affected() + r2.rows_affected())
}

/// Deletes an admin session by its plaintext token (SEC-004: `POST
/// /admin/logout` must revoke the server-side row, not just clear the
/// cookie). Not an error if the row doesn't exist — logout is idempotent,
/// mirroring `delete_session`.
pub async fn delete_admin_session(pool: &PgPool, token: &str) -> Result<(), RtDbError> {
    let hash = sha256_hex(token);
    sqlx::query("DELETE FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(pool)
        .await?;
    Ok(())
}

/// One row of the admin sessions list. `token_hash` is a non-reversible sha256
/// digest (the plaintext token is never stored), so it is safe to surface to an
/// authenticated admin and lets the UI target a specific row.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub token_hash: String,
    pub user_id: String,
    pub email: Option<String>,
    /// The provider's display name (`users.name`), when one was supplied.
    /// Unlike `login` this is never a handle or an email fallback, so an
    /// operator UI can prefer it and fall back to `login`/`email` itself.
    pub name: Option<String>,
    /// Display hint: GitHub handle when the user has a `github_id`, else the
    /// stored display name (same convention as `resolve_session`).
    pub login: Option<String>,
    pub anonymous: bool,
    pub created_at: i64,
    pub expires_at: i64,
}

/// Lists sessions newest-first. When `user_filter` is `Some`, matches rows whose
/// `user_id` OR `users.email` equals it (an operator may paste either). `limit`
/// is clamped to `[1, 1000]` by the caller. SEC-120: admin-key login sessions
/// (no user_id) are UNIONed in with a synthetic `(admin key)` marker so an
/// operator sees and can revoke them alongside OAuth sessions.
pub async fn list_sessions(
    pool: &PgPool,
    user_filter: Option<&str>,
    limit: i64,
) -> Result<Vec<SessionInfo>, RtDbError> {
    // (token_hash, user_id, email, name, login, anonymous, created_at, expires_at)
    type Row = (
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
        i64,
        i64,
    );
    let rows: Vec<Row> = if let Some(u) = user_filter {
        // User-filtered listing: only that user's OAuth sessions. Admin-key
        // sessions carry no user_id and don't belong to any user, so they are
        // excluded from a filtered query (they DO appear in the unfiltered list).
        // Revoked tombstones (kept so capability retries confirm) are
        // bookkeeping, not sessions — never listed.
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.name, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             WHERE (s.user_id = $1 OR u.email = $1) AND s.revoked_at IS NULL \
             ORDER BY s.created_at DESC LIMIT $2",
        )
        .bind(u)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.name, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             WHERE s.revoked_at IS NULL \
             UNION ALL \
             SELECT token_hash, '(admin key)', NULL, NULL, 'Admin key', false, \
                    created_at, expires_at \
             FROM rtdb_auth.admin_sessions \
             ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    Ok(rows
        .into_iter()
        .map(
            |(token_hash, user_id, email, name, login, anonymous, created_at, expires_at)| {
                SessionInfo {
                    token_hash,
                    user_id,
                    email,
                    name,
                    login,
                    anonymous,
                    created_at,
                    expires_at,
                }
            },
        )
        .collect())
}

/// Deletes every EXPIRED session row (the admin remove-all-expired path).
/// Covers both `sessions` (OAuth/anonymous) and `admin_sessions` (admin-key
/// logins) — `list_sessions` unions both, so the dashboard's expired rows can
/// come from either. Untouched-but-expired rows are never reaped otherwise
/// (lazy deletion fires only when that session is used again), so this is the
/// operator's cleanup for the long tail. Idempotent.
pub async fn delete_expired_sessions(pool: &PgPool) -> Result<u64, RtDbError> {
    let now = now_ms();
    let r1 = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE expires_at <= $1")
        .bind(now)
        .execute(pool)
        .await?;
    let r2 = sqlx::query("DELETE FROM rtdb_auth.admin_sessions WHERE expires_at <= $1")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(r1.rows_affected() + r2.rows_affected())
}

/// Stamps the durable revoke capability onto a freshly minted OAuth session.
/// Called from the bearer-mode settlement path only: `session_id` is a fresh
/// random token (the capability's non-secret identifier) and
/// `revoke_secret_hash` is the sha256 verifier already stored on the login's
/// `oauth_states` row — the same verifier `POST /auth/state/revoke` compares
/// against, so no plaintext secret exists anywhere server-side beyond what
/// begin/persist already held. Rows that never get stamped (cookie/default
/// logins, pre-upgrade sessions) carry NULLs and answer `unknown` to
/// `POST /auth/session/revoke`.
pub async fn stamp_revoke_capability(
    pool: &PgPool,
    token: &str,
    session_id: &str,
    revoke_secret_hash: &str,
) -> Result<(), RtDbError> {
    let hash = sha256_hex(token);
    sqlx::query(
        "UPDATE rtdb_auth.sessions \
         SET session_id = $1, revoke_secret_hash = $2 \
         WHERE token_hash = $3",
    )
    .bind(session_id)
    .bind(revoke_secret_hash)
    .bind(&hash)
    .execute(pool)
    .await?;
    Ok(())
}

/// The `session_id` for a live session token, for the one-shot poll delivery.
/// `None` when the token does not resolve or the row carries no capability
/// (never stamped — cookie/default logins and pre-upgrade rows).
pub async fn lookup_session_id(pool: &PgPool, token: &str) -> Result<Option<String>, RtDbError> {
    let hash = sha256_hex(token);
    let session_id: Option<Option<String>> =
        sqlx::query_scalar("SELECT session_id FROM rtdb_auth.sessions WHERE token_hash = $1")
            .bind(&hash)
            .fetch_optional(pool)
            .await?;
    Ok(session_id.flatten())
}

/// Prunes revoked-session tombstones (`revoked_at` set by
/// `POST /auth/session/revoke`) whose `expires_at` has passed, so revoked
/// capability rows do not accumulate past the session TTL they are bounded by.
/// Runs on the same gated background loop as `sweep_oauth_states`.
pub async fn sweep_revoked_tombstones(pool: &PgPool) -> Result<u64, RtDbError> {
    let now = now_ms();
    let r = sqlx::query(
        "DELETE FROM rtdb_auth.sessions WHERE revoked_at IS NOT NULL AND expires_at <= $1",
    )
    .bind(now)
    .execute(pool)
    .await?;
    Ok(r.rows_affected())
}
