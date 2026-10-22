//! Enrollment grants (host-machine-auth Task 1): short-TTL, single-use,
//! digest-only grants that mint one host-scoped machine credential each.
//!
//! Creation and revocation are admin-only (the REST handlers live in
//! `admin::tokens`); redemption is the public, unauthenticated
//! `POST /auth/enroll`. The grant secret is returned once at creation and only
//! its sha256 digest is stored — the same one-time-plaintext contract as
//! machine tokens. Redemption claims the grant with a conditional `UPDATE`
//! inside the same SQL transaction that inserts the machine token, so
//! concurrent redemptions of one grant yield exactly one success and one
//! credential.

use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use par_rt_db_core::host_capability::{HostCapability, validate_host};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::AppState;
use crate::db::{new_id, now_ms, random_token, sha256_hex};
use crate::error::RtDbError;
use crate::http_api::ApiJson;

/// Maximum window between grant creation and expiry.
const MAX_GRANT_TTL_MS: i64 = 15 * 60_000;

/// A freshly created grant: `grant` is the one-time plaintext secret, returned
/// exactly once and never stored (only its sha256 digest is persisted).
pub struct CreatedGrant {
    pub grant_id: String,
    pub grant: String,
    pub db: String,
    pub capability: HostCapability,
    pub expires_at: i64,
}

/// Validates and persists an enrollment grant. `expires_at` must be in the
/// future and at most 15 minutes out; `db` must exist; the capability's host
/// is validated against par-mesh's component grammar (deserialization already
/// enforces this — re-checked here for direct callers).
pub async fn create_grant(
    pool: &PgPool,
    db: &str,
    name: &str,
    capability: &HostCapability,
    expires_at: i64,
    token_expires_at: Option<i64>,
) -> Result<CreatedGrant, RtDbError> {
    let mut tx = pool.begin().await?;
    // Database first, grant second: deletion takes the same lifecycle lock
    // before cleaning tokens, so no authority can be inserted after cleanup.
    let exists: Option<String> =
        sqlx::query_scalar("SELECT name FROM rtdb_auth.databases WHERE name = $1 FOR KEY SHARE")
            .bind(db)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let HostCapability::ParMeshV1 { host } = capability;
    validate_host(host).map_err(RtDbError::bad_request)?;
    let now = now_ms();
    if expires_at <= now {
        return Err(RtDbError::bad_request("expiresAt must be in the future"));
    }
    if expires_at > now + MAX_GRANT_TTL_MS {
        return Err(RtDbError::bad_request(
            "expiresAt must be at most 15 minutes from now",
        ));
    }
    // A supplied token expiry in the past is REJECTED, never silently
    // dropped: NULL `token_expires_at` means "never expires" on the minted
    // credential, so filtering a bad value down to NULL would widen the
    // admin's intent into a never-expiring token.
    if token_expires_at.is_some_and(|t| t <= now) {
        return Err(RtDbError::bad_request(
            "tokenExpiresAt must be in the future",
        ));
    }
    let grant_id = new_id();
    let grant = random_token();
    let hash = sha256_hex(&grant);
    let capability_json = serde_json::to_value(capability)
        .map_err(|_| RtDbError::internal("failed to serialize host capability"))?;

    sqlx::query(
        "INSERT INTO rtdb_auth.enrollment_grants \
         (id, db_name, name, token_hash, capability, expires_at, token_expires_at, redeemed_at, revoked, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, false, $8)",
    )
    .bind(&grant_id)
    .bind(db)
    .bind(name)
    .bind(&hash)
    .bind(&capability_json)
    .bind(expires_at)
    .bind(token_expires_at)
    .bind(now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(CreatedGrant {
        grant_id,
        grant,
        db: db.to_string(),
        capability: capability.clone(),
        expires_at,
    })
}

/// A successful redemption: the one-time machine-token plaintext plus the
/// grant's fixed scope, returned after the claim+insert transaction commits.
pub struct RedeemedGrant {
    pub token_id: String,
    pub token: String,
    pub db: String,
    pub capability: HostCapability,
}

/// Redeems a grant secret: atomically claims one unexpired, unrevoked,
/// unredeemed grant and inserts exactly one machine token carrying the
/// grant's stored capability, in ONE sqlx transaction. Any grant that is not
/// claimable yields the same generic `Unauthorized` — the response never
/// distinguishes unknown, expired, revoked, and already-redeemed grants.
pub async fn redeem_grant(pool: &PgPool, grant_secret: &str) -> Result<RedeemedGrant, RtDbError> {
    let hash = sha256_hex(grant_secret);
    // Expiry is evaluated with the DATABASE clock AFTER the grant row is
    // locked — never against a timestamp captured before the pool/row wait.
    // Postgres evaluates an UPDATE's predicate before waiting on a plain
    // row lock (EvalPlanQual only re-runs the predicate when the tuple was
    // concurrently modified), so an in-statement clock_timestamp() alone
    // can go stale across the wait. The claim is therefore lock-then-check:
    // FOR UPDATE first, then a fresh clock read, then the conditional
    // UPDATE. The lock read also becomes the claim timestamp and the
    // minted token's created_at.
    let mut tx = pool.begin().await?;
    // Discover the parent without locking the grant: all lifecycle operations
    // acquire database before grant locks, avoiding cascade/claim deadlocks.
    let parent: Option<String> =
        sqlx::query_scalar("SELECT db_name FROM rtdb_auth.enrollment_grants WHERE token_hash = $1")
            .bind(&hash)
            .fetch_optional(&mut *tx)
            .await?;
    let Some(parent) = parent else {
        return Err(RtDbError::unauthorized("invalid grant"));
    };
    let exists: Option<String> =
        sqlx::query_scalar("SELECT name FROM rtdb_auth.databases WHERE name = $1 FOR KEY SHARE")
            .bind(&parent)
            .fetch_optional(&mut *tx)
            .await?;
    if exists.is_none() {
        return Err(RtDbError::unauthorized("invalid grant"));
    }

    let locked: Option<(String, String, String, serde_json::Value, Option<i64>)> = sqlx::query_as(
        "SELECT id, db_name, name, capability, token_expires_at \
         FROM rtdb_auth.enrollment_grants \
         WHERE token_hash = $1 AND redeemed_at IS NULL AND NOT revoked \
         FOR UPDATE",
    )
    .bind(&hash)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((grant_id, db, name, capability_json, token_expires_at)) = locked else {
        return Err(RtDbError::unauthorized("invalid grant"));
    };

    let db_now: i64 =
        sqlx::query_scalar("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint")
            .fetch_one(&mut *tx)
            .await?;
    let claimed: Option<(String,)> = sqlx::query_as(
        "UPDATE rtdb_auth.enrollment_grants SET redeemed_at = $2 \
         WHERE id = $1 AND expires_at > $2 RETURNING id",
    )
    .bind(&grant_id)
    .bind(db_now)
    .fetch_optional(&mut *tx)
    .await?;
    let Some((_,)) = claimed else {
        return Err(RtDbError::unauthorized("invalid grant"));
    };

    let capability: HostCapability = serde_json::from_value(capability_json.clone())
        .map_err(|_| RtDbError::internal("stored enrollment capability is invalid"))?;

    let token_id = new_id();
    let token = random_token();
    let token_hash = sha256_hex(&token);
    sqlx::query(
        "INSERT INTO rtdb_auth.machine_tokens \
         (id, db_name, name, token_hash, revoked, created_at, expires_at, read_only, tables, capability) \
         VALUES ($1, $2, $3, $4, false, $5, $6, false, NULL, $7)",
    )
    .bind(&token_id)
    .bind(&db)
    .bind(&name)
    .bind(&token_hash)
    .bind(db_now)
    .bind(token_expires_at)
    .bind(&capability_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // No plaintext in logs — only ids.
    tracing::info!(grant_id = %grant_id, db = %db, "enrollment grant redeemed");

    Ok(RedeemedGrant {
        token_id,
        token,
        db,
        capability,
    })
}

/// Admin-only grant revocation. `NotFound` for an unknown `grant_id`.
pub async fn revoke_grant(pool: &PgPool, grant_id: &str) -> Result<(), RtDbError> {
    let result = sqlx::query("UPDATE rtdb_auth.enrollment_grants SET revoked = true WHERE id = $1")
        .bind(grant_id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(RtDbError::not_found(format!(
            "grant '{grant_id}' not found"
        )));
    }
    Ok(())
}

/// `POST /auth/enroll` request: accepts ONLY the grant secret. The strict DTO
/// (`deny_unknown_fields`) rejects any attempt to override the host,
/// database, capability, or scope that the grant already fixes.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnrollRequest {
    grant: String,
}

/// `POST /auth/enroll` response after a successful claim-and-mint.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollResponse {
    token_id: String,
    token: String,
    db: String,
    capability: HostCapability,
}

/// The public, unauthenticated redemption route. Carries the same per-IP rate
/// limit as `POST /auth/anonymous` (the `RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM`
/// ceiling under the distinct `enroll` route key), plus the shared request-body
/// protections every `ApiJson` handler gets.
pub(crate) async fn enroll(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    ApiJson(body): ApiJson<EnrollRequest>,
) -> Result<Json<EnrollResponse>, RtDbError> {
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    let limit = state.config.limits.anonymous_per_ip_rpm;
    if limit > 0 {
        match state
            .limits
            .rate_limiter
            .check(
                crate::rate_limit::RateKey::Ip {
                    route: "enroll",
                    ip: ip_key.clone(),
                },
                limit,
            )
            .await
        {
            crate::rate_limit::RateDecision::Denied { retry_after_secs } => {
                return Err(RtDbError::rate_limited(retry_after_secs));
            }
            crate::rate_limit::RateDecision::Allowed => {}
        }
    }

    let redeemed = redeem_grant(&state.pool, &body.grant).await?;
    Ok(Json(EnrollResponse {
        token_id: redeemed.token_id,
        token: redeemed.token,
        db: redeemed.db,
        capability: redeemed.capability,
    }))
}
