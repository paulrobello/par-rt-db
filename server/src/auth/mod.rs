pub mod github;
pub mod session;
pub mod tokens;

use sqlx::PgPool;

use crate::db::{now_ms, sha256_hex};
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
        expires_at: i64,
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

/// Authorization for a database: a machine token must match `db` exactly and
/// still be un-revoked — checked live against `rtdb_auth.machine_tokens` on
/// every call, so a token revoked mid-session is denied on its very next
/// operation rather than only at the next fresh connection; a user must hold
/// an unexpired session and be present in `rtdb_auth.allowlist` for `db`.
/// Session expiry is checked against `expires_at`, captured once at session
/// resolution — a session's expiry is immutable once minted, so this cached
/// comparison is exactly as live as a fresh DB query, without costing one
/// per operation. Allowlist emails are stored lowercase (see
/// `admin::allowlist_write`), so the principal's email is lowercased here
/// before the lookup — the sole choke point for case-insensitive comparison.
pub async fn authorize(pool: &PgPool, principal: &Principal, db: &str) -> Result<(), RtDbError> {
    match principal {
        Principal::Machine {
            db: token_db,
            token_id,
        } => {
            if token_db != db {
                return Err(RtDbError::forbidden("token is not valid for this database"));
            }
            let (live,): (bool,) = sqlx::query_as(
                "SELECT EXISTS(SELECT 1 FROM rtdb_auth.machine_tokens WHERE id = $1 AND NOT revoked)",
            )
            .bind(token_id)
            .fetch_one(pool)
            .await?;
            if live {
                Ok(())
            } else {
                Err(RtDbError::unauthorized("token revoked"))
            }
        }
        Principal::User {
            email, expires_at, ..
        } => {
            if *expires_at < now_ms() {
                return Err(RtDbError::unauthorized("session expired"));
            }

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
            expires_at: i64::MAX,
        };
        let user = authed_user(&principal);
        assert_eq!(user.kind, "user");
        assert_eq!(user.email, Some("a@b.com".to_string()));
        assert_eq!(user.name, Some("Alice".to_string()));
    }
}
