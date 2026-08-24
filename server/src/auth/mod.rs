//! Per-database authentication and authorization: machine tokens, OAuth
//! sessions, and optional anonymous. The WS handler re-runs `authorize` on
//! every Subscribe and Mutate — not just at connect — so revocation, allowlist
//! changes, and session expiry take effect on open connections. This module
//! owns the shared `authorize` gate every transport calls.

pub mod apple;
pub mod cookie;
pub mod github;
pub mod gitlab;
pub mod google;
pub mod jwks;
pub mod microsoft;
pub mod oidc;
pub mod provider;
pub mod session;
pub mod tokens;

use sqlx::PgPool;

use crate::db::{new_id, now_ms, sha256_hex};
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
        /// `None` for an anonymous user (no OAuth identity, no email). OAuth
        /// users always carry a verified email.
        email: Option<String>,
        name: Option<String>,
        expires_at: i64,
        /// `true` for a credential-less guest minted by `POST /auth/anonymous`.
        /// An anonymous user is authorized for any database via the
        /// `RTDB_AUTH_ANONYMOUS_ENABLED` boot gate (no allowlist entry) and owns
        /// its own documents via per-row `ownerField` (the anon `user_id`).
        anonymous: bool,
        /// GitHub numeric id; `None` for users who authenticated through a
        /// non-GitHub provider (Google). Pairs with `github_login`.
        github_id: Option<i64>,
        /// GitHub login (`users.login`), but only when `github_id` is `Some` —
        /// `login` also stores the display name for Google users, so it is a
        /// genuine GitHub handle only when paired with a github id.
        github_login: Option<String>,
        /// sha256 digest of the session token == `rtdb_auth.sessions.token_hash`
        /// PK. `None` for principals not built from a session row (test fixtures,
        /// the OAuth-callback principal that is never the connection principal).
        /// Set by `session::resolve_session`; the per-op `session_still_valid`
        /// check reads it to deny a revoked session on its next op.
        session_hash: Option<String>,
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

    /// The `rtdb_auth.sessions.token_hash` backing this principal, if any.
    /// `None` for `Machine` and for `User` principals not built from a session
    /// row. Used by the per-op live-revocation check (`session_still_valid`).
    pub fn session_hash(&self) -> Option<&str> {
        match self {
            Principal::User { session_hash, .. } => session_hash.as_deref(),
            Principal::Machine { .. } => None,
        }
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
            email,
            expires_at,
            anonymous,
            ..
        } => {
            // Live revocation: a session deleted via the admin surface must be
            // denied on its next op. (Non-admin path; admins bypass `authorize`
            // per-op and are covered by the explicit check in the WS arms.)
            session_still_valid(pool, principal).await?;
            if *expires_at < now_ms() {
                return Err(RtDbError::unauthorized("session expired"));
            }
            // SEC-103: an anonymous user is authorized for a database ONLY when
            // that database has opted in via `rtdb_auth.databases.anonymous_enabled`.
            // The instance-wide boot gate `RTDB_AUTH_ANONYMOUS_ENABLED` remains a
            // master kill switch enforced at mint time (`POST /auth/anonymous`
            // refuses when off), so no new anonymous principals arise while it is
            // off; this per-db check is the additional gate that closes the
            // "enabling anon for one guest app opens EVERY database" hole. The
            // column defaults FALSE (safe), so a db must be explicitly opted in
            // by an operator via `PATCH /admin/db/{db}/anonymous-access`. An
            // anonymous principal authorized for db A is rejected for db B.
            // Per-row `ownerField` still scopes it to its own documents.
            if *anonymous {
                let (allowed,): (bool,) = sqlx::query_as(
                    "SELECT COALESCE((
                        SELECT anonymous_enabled FROM rtdb_auth.databases WHERE name = $1
                    ), FALSE)",
                )
                .bind(db)
                .fetch_one(pool)
                .await?;
                if allowed {
                    return Ok(());
                }
                return Err(RtDbError::forbidden(
                    "anonymous access is not enabled for this database",
                ));
            }
            let Some(email) = email else {
                return Err(RtDbError::forbidden(
                    "user has no verified email and is not allowlisted for this database",
                ));
            };

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

/// Live check that the session backing `principal` still exists (has not been
/// revoked via the admin surface). Mirrors the machine-token per-op re-check:
/// a session deleted mid-connection must be denied on its very next op over an
/// already-open `/sync`. `Ok(())` for principals with no session hash. Errors
/// `Unauthorized` ("session revoked") when the row is gone. Expiry is handled
/// separately by the cached `expires_at` comparison in `authorize`; this check
/// is purely for revocation (row deletion).
pub async fn session_still_valid(pool: &PgPool, principal: &Principal) -> Result<(), RtDbError> {
    let Some(hash) = principal.session_hash() else {
        return Ok(());
    };
    let (live,): (bool,) =
        sqlx::query_as("SELECT EXISTS(SELECT 1 FROM rtdb_auth.sessions WHERE token_hash = $1)")
            .bind(hash)
            .fetch_one(pool)
            .await?;
    if live {
        Ok(())
    } else {
        Err(RtDbError::unauthorized("session revoked"))
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
///
/// `Serialize`/`Deserialize` (ENH-022 Stage 4c): a forwarded write carries the
/// origin's principal to the lease owner over NOTIFY so per-row authz
/// (`ownerField`/`collaboratorsField`, `$user`/`$email` markers) evaluates
/// against the SAME identity that authorized the write at the edge.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
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
                email: email.clone(),
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
    // An anonymous user (no email) is never a dashboard admin.
    let Some(email) = email else {
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
            email: email.clone(),
            name: name.clone(),
            github_login: github_login.clone(),
            github_id: *github_id,
        },
    }
}

// --- QA-004: one `resolve_user` for every OAuth provider -------------------
//
// Every provider used to hand-roll its own "find or create the user" block.
// Three (GitHub, Apple, Microsoft) persist a stable per-provider identifier
// (`github_id`/`apple_sub`/`microsoft_sub`) and resolve by that column first,
// falling back to linking an existing email-keyed row; three (Google, GitLab,
// OIDC) persist no such column and resolve purely by the UNIQUE `email`
// column. `resolve_user` below serves both shapes through one call so the
// resolution rules can't silently drift between providers again.

/// The fixed set of columns `resolve_user` may splice into SQL as
/// `provider_id_column`. Every provider passes one of these `&'static str`
/// constants — never a caller-supplied string — so the interpolation can
/// never carry attacker-controlled text; the identifier *value* still binds
/// through `$n` like any other parameter.
pub const PROVIDER_COL_GITHUB_ID: &str = "github_id";
pub const PROVIDER_COL_APPLE_SUB: &str = "apple_sub";
pub const PROVIDER_COL_MICROSOFT_SUB: &str = "microsoft_sub";
/// Sentinel for the three providers with no persisted per-provider id
/// (Google, GitLab, OIDC): `resolve_user` recognizes this value and takes the
/// single email-keyed upsert path instead of the three-step resolution.
pub const PROVIDER_COL_EMAIL: &str = "email";

/// How `resolve_user` reports a unique-violation race on the final
/// insert/update. The three providers disagreed before consolidation
/// (GitHub and Microsoft returned `PRECONDITION_FAILED`, Apple returned
/// `CONFLICT`); both are preserved verbatim here per provider — GitHub's is
/// asserted by `oauth_test.rs`, Microsoft's by its own DB-level tests — so no
/// existing wire contract shifts. Google/GitLab/OIDC (previously unmapped;
/// their `ON CONFLICT (email) DO UPDATE` couldn't 23505 on email) get
/// `Conflict` as the more descriptive default for the one dormant path this
/// still leaves (a same-transaction race on another constraint).
#[derive(Debug, Clone, Copy)]
pub enum ConflictStyle {
    Precondition,
    Conflict,
}

/// The identity a provider extracted from its token exchange / claims,
/// normalized for `resolve_user`. `provider_id_column`/`provider_id` are
/// meaningless (and ignored) when `provider_id_column == PROVIDER_COL_EMAIL`.
pub struct ProviderIdentity<'a> {
    pub provider_id_column: &'static str,
    pub provider_id: &'a str,
    pub login: &'a str,
    pub email: &'a str,
    /// Whether step (b) below — linking an existing email-keyed row that has
    /// no value for `provider_id_column` — is permitted at all. `true` for
    /// every provider except Microsoft, which passes
    /// `xms_edov` ("email domain owner verified"): SEC-102's nOAuth defense
    /// requires that a tenant-spoofable, domain-unverified email can never
    /// adopt an existing account. This is a deliberate deviation from the
    /// audit's literal 4-field struct sketch — the plan's flat "match by
    /// provider_id_column, else email-link, else insert" would have silently
    /// reintroduced the nOAuth hole for Microsoft if applied uniformly.
    pub allow_email_link: bool,
    pub conflict_style: ConflictStyle,
}

/// Resolves (finds or creates) the `rtdb_auth.users` row for a completed
/// OAuth login and returns its id.
///
/// Two resolution shapes, chosen by `id.provider_id_column`:
///
/// **Id-keyed** (GitHub/Apple/Microsoft — `provider_id_column` is
/// `github_id`/`apple_sub`/`microsoft_sub`):
/// 1. An existing user with this `provider_id_column` value (a returning
///    user of this provider) is reused, with `login`/`email` refreshed — so a
///    provider-side email change follows the account instead of forking it.
/// 2. Otherwise, when `id.allow_email_link` is true and the verified email
///    already belongs to an account not yet linked to this provider
///    (`provider_id_column IS NULL`), that account is linked by setting its
///    `provider_id_column`. Both providers verified the email, so this is the
///    same person.
/// 3. Otherwise a new row is inserted.
///
/// **Email-keyed** (Google/GitLab/OIDC — `provider_id_column ==
/// PROVIDER_COL_EMAIL`, no persisted per-provider id): a single upsert keyed
/// on the UNIQUE `email` column, identical to each provider's pre-
/// consolidation `ON CONFLICT (email) DO UPDATE`.
///
/// A UNIQUE violation on the final write — the email already linked to a
/// *different* account, or a concurrent login racing past the checks — is
/// mapped to a deliberate conflict per `id.conflict_style` rather than leaked
/// as a 500.
pub async fn resolve_user(pool: &PgPool, id: ProviderIdentity<'_>) -> Result<String, RtDbError> {
    if id.provider_id_column == PROVIDER_COL_EMAIL {
        return resolve_by_email_only(pool, &id).await;
    }

    // Only `github_id` is a non-text column (bigint); the value still arrives
    // as `&str` (the plan's struct shape), so it is cast explicitly rather
    // than bound as a mismatched type. `apple_sub`/`microsoft_sub` are text
    // and need no cast.
    let cast = if id.provider_id_column == PROVIDER_COL_GITHUB_ID {
        "::bigint"
    } else {
        ""
    };

    let mut tx = pool.begin().await?;

    // (1) returning user of this provider: reuse the row, refresh login/email.
    let select_sql = format!(
        "SELECT id FROM rtdb_auth.users WHERE {col} = $1{cast}",
        col = id.provider_id_column,
    );
    if let Some((row_id,)) = sqlx::query_as::<_, (String,)>(&select_sql)
        .bind(id.provider_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        sqlx::query("UPDATE rtdb_auth.users SET login = $1, email = $2 WHERE id = $3")
            .bind(id.login)
            .bind(id.email)
            .bind(&row_id)
            .execute(&mut *tx)
            .await
            .map_err(|e| map_conflict(e, id.conflict_style))?;
        tx.commit().await?;
        return Ok(row_id);
    }

    // (2) link an email-keyed account not yet linked to this provider.
    if id.allow_email_link {
        let link_sql = format!(
            "UPDATE rtdb_auth.users \
             SET {col} = $1{cast}, login = $2 \
             WHERE email = $3 AND {col} IS NULL \
             RETURNING id",
            col = id.provider_id_column,
        );
        if let Some((row_id,)) = sqlx::query_as::<_, (String,)>(&link_sql)
            .bind(id.provider_id)
            .bind(id.login)
            .bind(id.email)
            .fetch_optional(&mut *tx)
            .await?
        {
            tx.commit().await?;
            return Ok(row_id);
        }
    }

    // (3) brand-new user.
    let row_id = new_id();
    let now = now_ms();
    let insert_sql = format!(
        "INSERT INTO rtdb_auth.users (id, {col}, login, email, created_at) \
         VALUES ($1, $2{cast}, $3, $4, $5)",
        col = id.provider_id_column,
    );
    sqlx::query(&insert_sql)
        .bind(&row_id)
        .bind(id.provider_id)
        .bind(id.login)
        .bind(id.email)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|e| map_conflict(e, id.conflict_style))?;
    tx.commit().await?;
    Ok(row_id)
}

/// The email-keyed resolution path for providers with no persisted
/// per-provider id (Google/GitLab/OIDC): identical to each provider's
/// pre-consolidation single-statement upsert.
async fn resolve_by_email_only(
    pool: &PgPool,
    id: &ProviderIdentity<'_>,
) -> Result<String, RtDbError> {
    let row_id = new_id();
    let now = now_ms();
    let (user_id,): (String,) = sqlx::query_as(
        "INSERT INTO rtdb_auth.users (id, login, email, created_at) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (email) DO UPDATE SET login = EXCLUDED.login \
         RETURNING id",
    )
    .bind(&row_id)
    .bind(id.login)
    .bind(id.email)
    .bind(now)
    .fetch_one(pool)
    .await
    .map_err(|e| map_conflict(e, id.conflict_style))?;
    Ok(user_id)
}

/// Maps a Postgres unique-violation (`23505`) from a `resolve_user` write to
/// a deliberate conflict response per `style` (see `ConflictStyle`). Any
/// other database error passes through as the usual internal-error mapping
/// (logged, never leaked).
fn map_conflict(err: sqlx::Error, style: ConflictStyle) -> RtDbError {
    let is_unique_violation = matches!(
        &err,
        sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505")
    );
    if !is_unique_violation {
        return RtDbError::from(err);
    }
    match style {
        ConflictStyle::Precondition => {
            RtDbError::precondition("email already linked to another sign-in method")
        }
        ConflictStyle::Conflict => RtDbError::conflict("account conflict"),
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
            email: Some("a@b.com".to_string()),
            name: Some("Alice".to_string()),
            expires_at: i64::MAX,
            anonymous: false,
            github_id: None,
            github_login: None,
            session_hash: None,
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
            email: Some("a@b.com".to_string()),
            name: Some("Alice".to_string()),
            expires_at: i64::MAX,
            anonymous: false,
            github_id: Some(42),
            github_login: Some("alice".to_string()),
            session_hash: None,
        };
        let user = authed_user(&principal);
        assert_eq!(user.github_id, Some(42));
        assert_eq!(user.github_login, Some("alice".to_string()));
    }
}

/// QA-004 `resolve_user` coverage. Shared dev Postgres (never
/// created/dropped); every value is uuid-unique so tests never collide.
#[cfg(test)]
mod resolve_user_tests {
    use super::*;
    use crate::error::ErrorCode;

    async fn users_pool() -> PgPool {
        let url = std::env::var("RTDB_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
        let pool = sqlx::PgPool::connect(&url)
            .await
            .expect("connect to dev postgres");
        crate::db::bootstrap(&pool)
            .await
            .expect("bootstrap rtdb_auth");
        pool
    }

    fn uniq(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
    }

    async fn user_row(
        pool: &PgPool,
        id: &str,
    ) -> (String, String, Option<i64>, Option<String>, Option<String>) {
        sqlx::query_as::<_, (String, String, Option<i64>, Option<String>, Option<String>)>(
            "SELECT login, email, github_id, apple_sub, microsoft_sub \
             FROM rtdb_auth.users WHERE id = $1",
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("user row exists")
    }

    async fn insert_email_user(pool: &PgPool, login: &str, email: &str) -> String {
        let id = new_id();
        sqlx::query(
            "INSERT INTO rtdb_auth.users (id, login, email, created_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&id)
        .bind(login)
        .bind(email)
        .bind(now_ms())
        .execute(pool)
        .await
        .expect("pre-insert email-keyed user");
        id
    }

    fn apple_identity<'a>(sub: &'a str, login: &'a str, email: &'a str) -> ProviderIdentity<'a> {
        ProviderIdentity {
            provider_id_column: PROVIDER_COL_APPLE_SUB,
            provider_id: sub,
            login,
            email,
            allow_email_link: true,
            conflict_style: ConflictStyle::Conflict,
        }
    }

    // --- id-keyed path (github_id / apple_sub / microsoft_sub) -------------

    #[tokio::test]
    async fn id_keyed_insert_creates_a_brand_new_user() {
        let pool = users_pool().await;
        let sub = uniq("sub");
        let login = uniq("login");
        let email = format!("{}@resolve-user-test.example", uniq("a"));

        let id = resolve_user(&pool, apple_identity(&sub, &login, &email))
            .await
            .expect("insert brand-new user");

        let (row_login, row_email, _, row_sub, _) = user_row(&pool, &id).await;
        assert_eq!(row_login, login);
        assert_eq!(row_email, email);
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    /// The scenario QA-004 exists to fix: a returning user is found by their
    /// stable provider id even though the provider's email for them changed —
    /// the row is reused and the email follows the account instead of
    /// forking a new row.
    #[tokio::test]
    async fn id_keyed_returning_user_is_reused_when_provider_email_changed() {
        let pool = users_pool().await;
        let sub = uniq("sub");
        let old_email = format!("{}@resolve-user-test.example", uniq("old"));
        let first_id = resolve_user(&pool, apple_identity(&sub, &uniq("login-a"), &old_email))
            .await
            .expect("initial insert");

        let new_login = uniq("login-b");
        let new_email = format!("{}@resolve-user-test.example", uniq("new"));
        let second_id = resolve_user(&pool, apple_identity(&sub, &new_login, &new_email))
            .await
            .expect("returning-user resolve");

        assert_eq!(second_id, first_id, "same provider id reuses the row");
        let (row_login, row_email, _, row_sub, _) = user_row(&pool, &first_id).await;
        assert_eq!(
            row_login, new_login,
            "login follows the provider-side change"
        );
        assert_eq!(
            row_email, new_email,
            "email follows the provider-side change"
        );
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    #[tokio::test]
    async fn id_keyed_links_an_unlinked_email_keyed_account_when_allowed() {
        let pool = users_pool().await;
        let email = format!("{}@resolve-user-test.example", uniq("carol"));
        let existing_id = insert_email_user(&pool, &uniq("gh-carol"), &email).await;

        let sub = uniq("sub");
        let id = resolve_user(&pool, apple_identity(&sub, &uniq("login-carol"), &email))
            .await
            .expect("link by email");

        assert_eq!(
            id, existing_id,
            "the existing account is adopted, not forked"
        );
        let (_, _, _, row_sub, _) = user_row(&pool, &existing_id).await;
        assert_eq!(row_sub.as_deref(), Some(sub.as_str()));
    }

    /// SEC-102 regression guard: with `allow_email_link: false` (Microsoft's
    /// nOAuth defense), the victim's existing row must never be adopted even
    /// though the identity's provider id is new — a fresh account is created
    /// instead. Mirrors the real caller contract: a provider that cannot
    /// verify the email domain (`microsoft.rs::parse_identity`) never passes
    /// the raw, potentially spoofed victim email through to `resolve_user`
    /// in the first place — it substitutes a safe, provider-namespaced
    /// contact address (the UPN) — so the identity here uses a distinct
    /// email, exactly as every real caller does.
    #[tokio::test]
    async fn id_keyed_never_links_by_email_when_link_is_disallowed() {
        let pool = users_pool().await;
        let victim_email = format!("{}@resolve-user-test.example", uniq("victim"));
        let victim_id = insert_email_user(&pool, &uniq("gh-victim"), &victim_email).await;

        let sub = uniq("sub");
        let login = uniq("login-attacker");
        let safe_email = format!("{}@resolve-user-test.example", uniq("attacker"));
        let mut identity = apple_identity(&sub, &login, &safe_email);
        identity.allow_email_link = false;
        let id = resolve_user(&pool, identity)
            .await
            .expect("a fresh account is created instead");

        assert_ne!(id, victim_id, "the victim account is never adopted");
        let (_, row_email, _, row_sub, _) = user_row(&pool, &victim_id).await;
        assert_eq!(row_email, victim_email);
        assert_eq!(row_sub, None, "the victim row stays unlinked");
    }

    #[tokio::test]
    async fn id_keyed_insert_conflict_maps_to_the_requested_style() {
        let pool = users_pool().await;
        let taken_email = format!("{}@resolve-user-test.example", uniq("taken"));
        insert_email_user(&pool, &uniq("gh-owner"), &taken_email).await;

        let sub = uniq("sub");
        let login = uniq("login");
        let mut identity = apple_identity(&sub, &login, &taken_email);
        identity.allow_email_link = false; // force the race onto the INSERT
        identity.conflict_style = ConflictStyle::Precondition;
        let err = resolve_user(&pool, identity).await.unwrap_err();

        assert_eq!(err.code, ErrorCode::PreconditionFailed);
        assert_ne!(err.code, ErrorCode::Internal);
    }

    #[tokio::test]
    async fn github_id_column_casts_the_text_provider_id_to_bigint() {
        let pool = users_pool().await;
        let github_id: i64 = 900_000_000 + (uuid::Uuid::now_v7().as_u128() % 90_000_000) as i64;
        let github_id_str = github_id.to_string();
        let login = uniq("gh-login");
        let email = format!("{}@resolve-user-test.example", uniq("gh"));

        let id = resolve_user(
            &pool,
            ProviderIdentity {
                provider_id_column: PROVIDER_COL_GITHUB_ID,
                provider_id: &github_id_str,
                login: &login,
                email: &email,
                allow_email_link: true,
                conflict_style: ConflictStyle::Precondition,
            },
        )
        .await
        .expect("insert with a bigint-cast provider id");

        let (_, _, row_github_id, _, _) = user_row(&pool, &id).await;
        assert_eq!(row_github_id, Some(github_id));
    }

    // --- email-keyed path (Google / GitLab / OIDC: no persisted id) --------

    fn email_identity<'a>(login: &'a str, email: &'a str) -> ProviderIdentity<'a> {
        ProviderIdentity {
            provider_id_column: PROVIDER_COL_EMAIL,
            provider_id: email,
            login,
            email,
            allow_email_link: true,
            conflict_style: ConflictStyle::Conflict,
        }
    }

    #[tokio::test]
    async fn email_keyed_insert_creates_a_brand_new_user() {
        let pool = users_pool().await;
        let login = uniq("login");
        let email = format!("{}@resolve-user-test.example", uniq("g"));

        let id = resolve_user(&pool, email_identity(&login, &email))
            .await
            .expect("insert brand-new user");

        let (row_login, row_email, ..) = user_row(&pool, &id).await;
        assert_eq!(row_login, login);
        assert_eq!(row_email, email);
    }

    /// Google/GitLab/OIDC persist no per-provider id, so their durable key
    /// IS the email: a second login with the same email always reuses the
    /// row (this is the pre-consolidation `ON CONFLICT (email) DO UPDATE`
    /// behavior, preserved verbatim) and refreshes the display login.
    #[tokio::test]
    async fn email_keyed_returning_user_with_the_same_email_reuses_the_row() {
        let pool = users_pool().await;
        let email = format!("{}@resolve-user-test.example", uniq("h"));
        let first_id = resolve_user(&pool, email_identity(&uniq("login-a"), &email))
            .await
            .expect("initial insert");

        let new_login = uniq("login-b");
        let second_id = resolve_user(&pool, email_identity(&new_login, &email))
            .await
            .expect("returning-user resolve");

        assert_eq!(second_id, first_id, "same email reuses the row");
        let (row_login, row_email, ..) = user_row(&pool, &first_id).await;
        assert_eq!(
            row_login, new_login,
            "login follows the provider-side change"
        );
        assert_eq!(row_email, email);
    }
}
