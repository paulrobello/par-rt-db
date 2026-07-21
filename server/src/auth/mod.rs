pub mod github;
pub mod session;
pub mod tokens;

use sqlx::PgPool;

use crate::db::sha256_hex;
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

/// Who is making a request: a per-database machine token, or a
/// GitHub-authenticated user session.
#[derive(Debug, Clone)]
pub enum Principal {
    Machine {
        db: String,
        token_id: String,
    },
    User {
        user_id: String,
        email: String,
        name: Option<String>,
    },
}

/// Resolves a bearer token to a `Principal`: first a machine-token digest
/// lookup (`rtdb_auth.machine_tokens`, revoked tokens excluded), then falls
/// back to session tokens. Errors `Unauthorized` if neither resolves.
pub async fn resolve_bearer(pool: &PgPool, token: &str) -> Result<Principal, RtDbError> {
    let hash = sha256_hex(token);

    let machine: Option<(String, String)> = sqlx::query_as(
        "SELECT id, db_name FROM rtdb_auth.machine_tokens WHERE token_hash = $1 AND NOT revoked",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    if let Some((token_id, db)) = machine {
        return Ok(Principal::Machine { db, token_id });
    }

    if let Some(principal) = session::resolve_session(pool, token).await? {
        return Ok(principal);
    }

    Err(RtDbError::unauthorized("invalid token"))
}

/// Authorization for a database: a machine token must match `db` exactly;
/// a user must be present in `rtdb_auth.allowlist` for `db`. Allowlist emails
/// are stored lowercase (see `admin::allowlist_write`), so the principal's
/// email is lowercased here before the lookup — the sole choke point for
/// case-insensitive comparison.
pub async fn authorize(pool: &PgPool, principal: &Principal, db: &str) -> Result<(), RtDbError> {
    match principal {
        Principal::Machine { db: token_db, .. } => {
            if token_db == db {
                Ok(())
            } else {
                Err(RtDbError::forbidden("token is not valid for this database"))
            }
        }
        Principal::User { email, .. } => {
            let row: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM rtdb_auth.allowlist WHERE db_name = $1 AND email = $2",
            )
            .bind(db)
            .bind(email.to_lowercase())
            .fetch_optional(pool)
            .await?;

            if row.is_some() {
                Ok(())
            } else {
                Err(RtDbError::forbidden(
                    "user is not allowlisted for this database",
                ))
            }
        }
    }
}

/// Wire-facing identity for a resolved principal (see `protocol::AuthedUser`).
/// A machine token's name is not an identity, so `name` is always `None` for
/// `Machine`.
pub fn authed_user(p: &Principal) -> AuthedUser {
    match p {
        Principal::Machine { .. } => AuthedUser {
            kind: "machine".to_string(),
            email: None,
            name: None,
        },
        Principal::User { email, name, .. } => AuthedUser {
            kind: "user".to_string(),
            email: Some(email.clone()),
            name: name.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authed_user_for_machine_has_no_email_or_name() {
        let principal = Principal::Machine {
            db: "d".to_string(),
            token_id: "t".to_string(),
        };
        let user = authed_user(&principal);
        assert_eq!(user.kind, "machine");
        assert_eq!(user.email, None);
        assert_eq!(user.name, None);
    }

    #[test]
    fn authed_user_for_user_carries_email_and_name() {
        let principal = Principal::User {
            user_id: "u".to_string(),
            email: "a@b.com".to_string(),
            name: Some("Alice".to_string()),
        };
        let user = authed_user(&principal);
        assert_eq!(user.kind, "user");
        assert_eq!(user.email, Some("a@b.com".to_string()));
        assert_eq!(user.name, Some("Alice".to_string()));
    }
}
