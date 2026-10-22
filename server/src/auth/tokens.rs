//! Per-database machine tokens on `rtdb_auth.tokens`: mint (with optional
//! scope/expiry and host capability), verify via sha256 hash, list, and revoke
//! — the `Principal::Machine` source that bypasses per-row rules but never the
//! db-level gate.

use sqlx::PgPool;

use par_rt_db_core::host_capability::HostCapability;

use crate::db::{new_id, now_ms, random_token, sha256_hex};
use crate::error::RtDbError;

/// Mints a new machine token for `db`, the full form carrying an optional
/// [`HostCapability`]. The plaintext is never stored — only its sha256 digest
/// is persisted — and is returned exactly once, here, at mint time. Callers
/// must not log the returned plaintext.
///
/// `expires_at` / `read_only` / `tables` are the ENH-005 capability columns;
/// `capability` is the host-machine-auth column — `None` stores NULL, which
/// resolves to exactly the pre-feature legacy semantics.
pub async fn mint_token_with_capability(
    pool: &PgPool,
    db: &str,
    name: &str,
    expires_at: Option<i64>,
    read_only: bool,
    tables: Option<&[String]>,
    capability: Option<&HostCapability>,
) -> Result<(String /* id */, String /* plaintext */), RtDbError> {
    let id = new_id();
    let token = random_token();
    let hash = sha256_hex(&token);
    let capability_json = match capability {
        Some(c) => Some(
            serde_json::to_value(c)
                .map_err(|_| RtDbError::internal("failed to serialize host capability"))?,
        ),
        None => None,
    };

    sqlx::query(
        "INSERT INTO rtdb_auth.machine_tokens \
         (id, db_name, name, token_hash, revoked, created_at, expires_at, read_only, tables, capability) \
         VALUES ($1, $2, $3, $4, false, $5, $6, $7, $8, $9)",
    )
    .bind(&id)
    .bind(db)
    .bind(name)
    .bind(&hash)
    .bind(now_ms())
    .bind(expires_at)
    .bind(read_only)
    .bind(tables)
    .bind(capability_json)
    .execute(pool)
    .await?;

    Ok((id, token))
}

/// The legacy mint, preserved byte-identically for existing callers: no
/// capability is attached, so the stored row and the resolved
/// `Principal::Machine` are exactly as before this feature.
pub async fn mint_token(
    pool: &PgPool,
    db: &str,
    name: &str,
    expires_at: Option<i64>,
    read_only: bool,
    tables: Option<&[String]>,
) -> Result<(String /* id */, String /* plaintext */), RtDbError> {
    mint_token_with_capability(pool, db, name, expires_at, read_only, tables, None).await
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
