pub mod cookie;
pub mod github;
pub mod gitlab;
pub mod google;
pub mod oidc;
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
        /// ENH-005: when `true`, the token may only read; writes are rejected
        /// (enforced by the executors, not here). Captured once at resolution.
        read_only: bool,
        /// ENH-005: when `Some`, the token is scoped to exactly these tables;
        /// `None` means all tables. Captured once at resolution.
        tables: Option<Vec<String>>,
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

impl Principal {
    /// ENH-005: whether this principal is read-only. Only `Machine` tokens
    /// minted with `read_only = true` are; `User` principals are never
    /// read-only. Used by the executors to gate writes.
    pub fn is_read_only(&self) -> bool {
        matches!(
            self,
            Principal::Machine {
                read_only: true,
                ..
            }
        )
    }
}

/// Resolves a bearer token to a `Principal`: first a machine-token digest
/// lookup (`rtdb_auth.machine_tokens`, revoked tokens excluded), then falls
/// back to session tokens. Errors `Unauthorized` if neither resolves.
///
/// ENH-005: the machine-token row also carries `read_only` and `tables`,
/// which are threaded onto `Principal::Machine` for the executors. Expiry is
/// NOT resolved here — it stays a live check in `authorize` so an expired
/// token that still resolves (for the live-reject path) is denied per-op.
pub async fn resolve_bearer(pool: &PgPool, token: &str) -> Result<Principal, RtDbError> {
    let hash = sha256_hex(token);

    // `(id, db_name, read_only, tables, expires_at)`. `expires_at` is read
    // here only so the row shape matches the SELECT — it is NOT threaded onto
    // the Principal; `authorize` re-queries it live per-op.
    type MachineRow = (String, String, bool, Option<Vec<String>>, Option<i64>);
    let machine: Option<MachineRow> = sqlx::query_as(
        "SELECT id, db_name, read_only, tables, expires_at \
             FROM rtdb_auth.machine_tokens WHERE token_hash = $1 AND NOT revoked",
    )
    .bind(&hash)
    .fetch_optional(pool)
    .await?;

    if let Some((token_id, db, read_only, tables, _expires_at)) = machine {
        return Ok(Principal::Machine {
            db,
            token_id,
            read_only,
            tables,
        });
    }

    if let Some(principal) = session::resolve_session(pool, token).await? {
        return Ok(principal);
    }

    Err(RtDbError::unauthorized("invalid token"))
}

/// Authorization for a database: a machine token must match `db` exactly and
/// still be un-revoked AND un-expired — checked live against
/// `rtdb_auth.machine_tokens` on every call, so a token revoked or expired
/// mid-session is denied on its very next operation rather than only at the
/// next fresh connection; a user must hold an unexpired session and be present
/// in `rtdb_auth.allowlist` for `db`.
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
            ..
        } => {
            if token_db != db {
                return Err(RtDbError::forbidden("token is not valid for this database"));
            }
            // Live check: the token must still be un-revoked AND un-expired.
            // `expires_at IS NULL` ⇒ never expires (legacy full-access path);
            // otherwise it must be in the future. Re-queried per op so a token
            // that expires or is revoked mid-session is denied on its next use.
            let (live,): (bool,) = sqlx::query_as(
                "SELECT EXISTS(SELECT 1 FROM rtdb_auth.machine_tokens \
                 WHERE id = $1 AND NOT revoked AND (expires_at IS NULL OR expires_at > $2))",
            )
            .bind(token_id)
            .bind(now_ms())
            .fetch_one(pool)
            .await?;
            if live {
                Ok(())
            } else {
                Err(RtDbError::unauthorized("token revoked or expired"))
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

/// Per-row auth view threaded into the executors for model C's `authorize`
/// predicate evaluation. `user_id == None` ⇒ bypass (`Machine`/admin/scheduled);
/// `Some` ⇒ `$user`/`$email` markers in a `FilterExpr` resolve to this identity.
///
/// ENH-005 Task 4: `tables` carries the machine-token table allowlist so the
/// executor boundary can gate reads/writes/subscriptions without a separate
/// `&Principal` thread. `None` ⇒ all tables (admin/scheduled/`User`/full-access
/// machine tokens); `Some(non-empty)` ⇒ only those tables. An empty list is
/// treated as "no restriction" (mint-time contract: empty ⇒ `None`).
///
/// Held as owned `Option<String>` (rather than the brief's `&'a str` sketch) so
/// Task 5 can thread it through the executors without lifetime gymnastics.
/// `Default` is the bypass view (`user_id`/`email`/`tables` all `None`); the
/// `..Default::default()` spread keeps the many test literals that construct a
/// `PrincipalCtx` for per-row-auth scenarios stable when new fields are added.
#[derive(Debug, Clone, Default)]
pub struct PrincipalCtx {
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub tables: Option<Vec<String>>,
}

impl PrincipalCtx {
    /// Bypass context: no user identity, no table restriction. Used for machine
    /// tokens (the table allowlist is populated separately by `row_ctx` for
    /// scoped tokens), scheduled jobs, the TTL reaper, schema migrations, and
    /// the WS admin bypass — every path that must NOT enforce per-row ownership
    /// (ownerField / collaboratorsField) or resolve `$user`/`$email` markers.
    /// Equivalent to the pre-Task-5 `owner = None`.
    pub fn bypass() -> Self {
        PrincipalCtx {
            user_id: None,
            email: None,
            tables: None,
        }
    }
}

impl Principal {
    /// Builds the per-row auth view. `Machine` ⇒ identity bypass
    /// (`user_id = None`) but carries the table allowlist so the executor
    /// boundary can enforce it. `User` ⇒ `Some(user_id)` and `Some(email)` so
    /// `$email` predicates resolve; `User` principals are never table-scoped.
    pub fn row_ctx(&self) -> PrincipalCtx {
        match self {
            Principal::User { user_id, email, .. } => PrincipalCtx {
                user_id: Some(user_id.clone()),
                email: Some(email.clone()),
                tables: None,
            },
            Principal::Machine { tables, .. } => PrincipalCtx {
                user_id: None,
                email: None,
                tables: tables.clone(),
            },
        }
    }
}

/// ENH-005 Task 4: table allowlist gate for machine tokens. The executor
/// boundary (reads in `query::execute_query`, every write step in
/// `txn::execute_txn`, subscription registration in `subs::register`) calls this
/// with the `PrincipalCtx` already in scope. `tables = None` (admin, scheduled,
/// `User`, full-access machine tokens) and `tables = Some([])` (treated as
/// no-restriction, matching the mint-time contract) bypass; only
/// `tables = Some(non-empty)` restricts, rejecting a table not on the list with
/// `Forbidden`. A pure read-only gate — writes nothing, leaves the single-writer
/// invariant untouched.
pub fn authorize_table(ctx: &PrincipalCtx, table: &str) -> Result<(), RtDbError> {
    if let Some(list) = &ctx.tables
        && !list.is_empty()
        && !list.iter().any(|t| t == table)
    {
        return Err(RtDbError::forbidden("token is not scoped for this table"));
    }
    Ok(())
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
            read_only: false,
            tables: None,
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
