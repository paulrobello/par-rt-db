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

/// Resolves a session token to a `Principal::User`. `Ok(None)` if the token
/// is absent or expired; an expired row is deleted lazily as part of this
/// call so it never needs a separate sweep.
pub async fn resolve_session(pool: &PgPool, token: &str) -> Result<Option<Principal>, RtDbError> {
    let hash = sha256_hex(token);

    let row: Option<(String, i64, String)> = sqlx::query_as(
        "SELECT s.user_id, s.expires_at, u.email \
         FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
         WHERE s.token_hash = $1",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    let Some((user_id, expires_at, email)) = row else {
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
    }))
}

/// Deletes a session by its plaintext token. Not an error if the token
/// doesn't exist — logout is idempotent (see `auth::github::logout`).
pub async fn delete_session(pool: &PgPool, token: &str) -> Result<(), RtDbError> {
    let hash = sha256_hex(token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(pool)
        .await?;
    Ok(())
}
