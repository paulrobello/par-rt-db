pub mod cookie;
pub mod github;
pub mod gitlab;
pub mod google;
pub mod provider;
pub mod session;
pub mod tokens;

use sqlx::PgPool;

use crate::db::{now_ms, sha256_hex};
use crate::error::RtDbError;
use crate::protocol::{AuthedUser, UserKind};

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
        /// GitHub numeric id; `None` for users who authenticated through a
        /// non-GitHub provider (Google). Pairs with `github_login`.
        github_id: Option<i64>,
        /// GitHub login (`users.login`), but only when `github_id` is `Some` —
        /// `login` also stores the display name for Google users, so it is a
        /// genuine GitHub handle only when paired with a github id.
        github_login: Option<String>,
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

/// Per-row authorization identity for `principal`. `Some(user_id)` means
/// "enforce owner-field equality against this user" on any table that
/// declares an `ownerField`; `None` means bypass (machine tokens). Scheduled
/// jobs pass `None` directly — they have no caller.
pub fn owner_of(principal: &Principal) -> Option<&str> {
    match principal {
        Principal::User { user_id, .. } => Some(user_id.as_str()),
        Principal::Machine { .. } => None,
    }
}

/// Idempotently seeds `RTDB_ADMIN_EMAILS` into `rtdb_auth.admins` at startup
/// (see `main.rs`). Emails are lowercased and trimmed; blanks are skipped.
/// Seeded rows carry a NULL `github_id` and are matched by email at login.
pub async fn seed_admin_emails(pool: &PgPool, emails: &[String]) -> Result<(), RtDbError> {
    let now = now_ms();
    for raw in emails {
        let email = raw.trim().to_lowercase();
        if email.is_empty() {
            continue;
        }
        sqlx::query(
            "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2) \
             ON CONFLICT (email) DO NOTHING",
        )
        .bind(&email)
        .bind(now)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Whether `principal` is a server-wide dashboard admin — present in
/// `rtdb_auth.admins` by email (lowercased) or GitHub id. Machine principals are
/// never admin. Used by the admin gate (`admin::require_admin`) and the dashboard
/// WS bypass (Phase 5). Returns `false` on DB error rather than propagating, so a
/// transient failure degrades to "not admin" (deny) without an error envelope —
/// but the error is logged so a Postgres outage locking out admins is observable
/// and not silently misdiagnosed as "user is not an admin".
pub async fn is_admin(pool: &PgPool, principal: &Principal) -> bool {
    let Principal::User {
        email, github_id, ..
    } = principal
    else {
        return false;
    };
    let email = email.to_lowercase();
    match sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM rtdb_auth.admins WHERE email = $1 OR github_id = $2)",
    )
    .bind(&email)
    .bind(*github_id)
    .fetch_one(pool)
    .await
    {
        Ok(exists) => exists,
        Err(err) => {
            tracing::warn!(
                error = %err,
                admin_email = %email,
                "is_admin lookup failed; denying as not-admin (safe default)"
            );
            false
        }
    }
}

/// Wire-facing identity for a resolved principal (see `protocol::AuthedUser`).
/// A machine token's name is not an identity, so `name` is always `None` for
/// `Machine`.
pub fn authed_user(p: &Principal) -> AuthedUser {
    match p {
        Principal::Machine { .. } => AuthedUser {
            kind: UserKind::Machine,
            email: None,
            name: None,
            github_login: None,
            github_id: None,
        },
        Principal::User {
            email,
            name,
            github_id,
            github_login,
            ..
        } => AuthedUser {
            kind: UserKind::User,
            email: Some(email.clone()),
            name: name.clone(),
            github_login: github_login.clone(),
            github_id: *github_id,
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
        assert_eq!(user.kind, UserKind::Machine);
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
            github_id: None,
            github_login: None,
        };
        let user = authed_user(&principal);
        assert_eq!(user.kind, UserKind::User);
        assert_eq!(user.email, Some("a@b.com".to_string()));
        assert_eq!(user.name, Some("Alice".to_string()));
        assert_eq!(user.github_id, None);
        assert_eq!(user.github_login, None);
    }

    #[test]
    fn authed_user_for_user_surfaces_github_identity() {
        let principal = Principal::User {
            user_id: "u".to_string(),
            email: "a@b.com".to_string(),
            name: Some("Alice".to_string()),
            expires_at: i64::MAX,
            github_id: Some(42),
            github_login: Some("alice".to_string()),
        };
        let user = authed_user(&principal);
        assert_eq!(user.github_id, Some(42));
        assert_eq!(user.github_login, Some("alice".to_string()));
    }
}
