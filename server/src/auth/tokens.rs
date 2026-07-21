use sqlx::PgPool;

use crate::db::{new_id, now_ms, random_token, sha256_hex};
use crate::error::RtDbError;

/// Mints a new machine token for `db`. The plaintext is never stored — only
/// its sha256 digest is persisted — and is returned exactly once, here, at
/// mint time. Callers must not log the returned plaintext.
pub async fn mint_token(
    pool: &PgPool,
    db: &str,
    name: &str,
) -> Result<(String /* id */, String /* plaintext */), RtDbError> {
    let id = new_id();
    let token = random_token();
    let hash = sha256_hex(&token);

    sqlx::query(
        "INSERT INTO rtdb_auth.machine_tokens (id, db_name, name, token_hash, revoked, created_at) \
         VALUES ($1, $2, $3, $4, false, $5)",
    )
    .bind(&id)
    .bind(db)
    .bind(name)
    .bind(&hash)
    .bind(now_ms())
    .execute(pool)
    .await?;

    Ok((id, token))
}

/// Revokes a machine token by id. `NotFound` if `token_id` doesn't exist.
pub async fn revoke_token(pool: &PgPool, token_id: &str) -> Result<(), RtDbError> {
    let result = sqlx::query("UPDATE rtdb_auth.machine_tokens SET revoked = true WHERE id = $1")
        .bind(token_id)
        .execute(pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(RtDbError::not_found(format!(
            "token '{token_id}' not found"
        )));
    }
    Ok(())
}
