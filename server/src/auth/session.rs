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
pub async fn resolve_session(pool: &PgPool, token: &str) -> Result<Option<Principal>, RtDbError> {
    let hash = sha256_hex(token);

    // `(user_id, expires_at, email, github_id, login, anonymous)`.
    type SessionUserRow = (String, i64, Option<String>, Option<i64>, String, bool);
    let row: Option<SessionUserRow> = sqlx::query_as(
        "SELECT s.user_id, s.expires_at, u.email, u.github_id, u.login, u.anonymous \
         FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
         WHERE s.token_hash = $1",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    let Some((user_id, expires_at, email, github_id, login, anonymous)) = row else {
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
        name: None,
        expires_at,
        anonymous,
        github_id,
        github_login: github_id.is_some().then_some(login),
        session_hash: Some(hash),
    }))
}

/// Deletes a session by its plaintext token. Not an error if the token
/// doesn't exist — logout is idempotent (see `auth::provider::logout`).
pub async fn delete_session(pool: &PgPool, token: &str) -> Result<(), RtDbError> {
    let hash = sha256_hex(token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
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
    // (token_hash, user_id, email, login, anonymous, created_at, expires_at)
    type Row = (
        String,
        String,
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
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             WHERE s.user_id = $1 OR u.email = $1 \
             ORDER BY s.created_at DESC LIMIT $2",
        )
        .bind(u)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             UNION ALL \
             SELECT token_hash, '(admin key)', NULL, 'Admin key', false, \
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
            |(token_hash, user_id, email, login, anonymous, created_at, expires_at)| SessionInfo {
                token_hash,
                user_id,
                email,
                login,
                anonymous,
                created_at,
                expires_at,
            },
        )
        .collect())
}

/// Deletes one session by its token_hash (the admin revoke-one path). SEC-120:
/// tries both `sessions` (OAuth user) and `admin_sessions` (admin-key login) so
/// either kind is revocable through the same `DELETE /admin/sessions/{hash}`.
/// Idempotent: returns 0 if the row is already gone — never an error.
pub async fn delete_session_by_hash(pool: &PgPool, token_hash: &str) -> Result<u64, RtDbError> {
    let r1 = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(token_hash)
        .execute(pool)
        .await?;
    let r2 = sqlx::query("DELETE FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
        .bind(token_hash)
        .execute(pool)
        .await?;
    Ok(r1.rows_affected() + r2.rows_affected())
}

/// Deletes every session for `user_id` (the admin revoke-all path). Idempotent.
pub async fn delete_sessions_for_user(pool: &PgPool, user_id: &str) -> Result<u64, RtDbError> {
    let result = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
