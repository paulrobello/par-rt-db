//! Per-row public share grants — principal-free share validation.
//!
//! A share grant is a server-minted, unguessable token that lets an
//! UNAUTHENTICATED caller read EXACTLY one granted document row (and, when the
//! grant names a blob, mint a signed URL for EXACTLY that blob) — never anything
//! db-wide. The nearest prior primitive, `POST /auth/anonymous`, grants a full
//! db-authorized principal (every ownerless table becomes readable, signed-url
//! minting is db-scoped), so it can never back a public share link.
//!
//! Grants live in the server-global `rtdb.share_grants` table (not a document
//! table — no committer, no op-feed tap, same tier as `rtdb_auth.sessions`):
//!
//! ```text
//! token_hash text PRIMARY KEY   -- SHA-256 hex of the 256-bit token
//! db         text NOT NULL      -- validated db name
//! table_name text NOT NULL      -- schema-declared table at mint time
//! doc_id     text NOT NULL
//! blob_id    text               -- optional; the ONLY blob a share can sign
//! expires_at bigint NOT NULL    -- epoch-ms
//! ```
//!
//! Security model:
//! - **Deny by default.** Gated on `RTDB_SHARE_GRANTS_ENABLED` (default false).
//!   With the flag off, mint/revoke return `Forbidden` and the public routes
//!   return the SAME generic failure as an invalid token — the flag state is
//!   not observable through the public surface.
//! - **The token is the credential.** 256 bits from the OS CSPRNG
//!   (`db::random_token`); only its SHA-256 hash is stored. No plaintext
//!   comparison ever happens — validation is a point lookup on the hash, the
//!   same model as `rtdb_auth.sessions` (the lookup's timing reveals properties
//!   of the digest, never of the preimage, which SHA-256 preimage resistance
//!   makes useless to an attacker). Document entropy expectation: 128+ bits;
//!   this module mints 256.
//! - **No enumeration oracle.** Every public-surface failure — flag off,
//!   unknown/expired/revoked token, revoked-or-missing table, deleted row,
//!   grant without a blob, deleted blob — returns the identical
//!   `401 UNAUTHORIZED "invalid share token"` envelope.
//! - **Strict scoping.** The public routes take no db/table/id parameters;
//!   the grant row alone addresses the target. Blob signing signs only the
//!   grant's `blob_id`, which was verified at mint time to live in the
//!   grant's db (`SEC-113` discipline: the mint is the capability-granting
//!   step) and is re-verified at mint-url time in case the blob was deleted
//!   or moved.
//! - **Revocation** = `DELETE /api/db/{db}/share-grants/{token}` (idempotent,
//!   db-scoped) or expiry; expired grants are lazily deleted on read.
//! - Rate limiting: the public routes share the per-IP public-storage budget
//!   (`RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM`) — both are unauthenticated
//!   capability-bearing surfaces keyed only by client IP.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query as AxumQuery, State};
use axum::http::HeaderMap;

use crate::AppState;
use crate::auth::PrincipalCtx;
use crate::db::{now_ms, random_token, sha256_hex, validate_db_name};
use crate::dsl::{Query, QueryResult};
use crate::error::RtDbError;
use crate::http_api::storage::SignedUrlResponse;
use crate::http_api::{ApiJson, authed, client_ip_key};
use crate::query::execute_query;
use crate::rate_limit::{check_http_rate_limits, check_storage_public_rate_limit};
use crate::signed_url;
use crate::storage;

/// The one generic failure every unauthenticated share-route rejection returns.
/// Never vary the code, status, or message with the underlying cause — the
/// variance itself would be an enumeration oracle.
const SHARE_REJECTED_MSG: &str = "invalid share token";

fn grant_rejected() -> RtDbError {
    RtDbError::unauthorized(SHARE_REJECTED_MSG)
}

/// Default grant lifetime: 7 days (a share link is meant to be rotated, not
/// permanent; revocation is the primary lifecycle control).
const DEFAULT_SHARE_TTL_SECS: i64 = 7 * 24 * 60 * 60;

/// Upper bound on a grant's lifetime: 30 days. The cap bounds how long a
/// forgotten grant can linger.
const MAX_SHARE_TTL_SECS: i64 = 30 * 24 * 60 * 60;

/// Creates the server-global grants table if absent. Called from the authed
/// mint path (the only writer); the public read path maps the table-missing
/// error (`42P01`) to the generic rejection instead of ensuring, so an
/// unauthenticated caller can never induce DDL.
async fn ensure_grants_table(pool: &sqlx::PgPool) -> Result<(), RtDbError> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.share_grants (
            token_hash text PRIMARY KEY,
            db text NOT NULL,
            table_name text NOT NULL,
            doc_id text NOT NULL,
            blob_id text,
            expires_at bigint NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn is_undefined_table(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .map(|d| d.code().as_deref() == Some("42P01"))
        .unwrap_or(false)
}

/// A validated, unexpired grant.
struct Grant {
    db: String,
    table_name: String,
    doc_id: String,
    blob_id: Option<String>,
}

/// Validates `token` against the grants table. Every failure mode collapses to
/// [`grant_rejected`]; a missing grants table (no grant can exist yet) is the
/// same rejection rather than a 500 that would disclose mint history.
async fn resolve_grant(state: &Arc<AppState>, token: &str) -> Result<Grant, RtDbError> {
    if !state.config.storage.share_grants_enabled {
        return Err(grant_rejected());
    }
    // Length-bounded so a hostile caller cannot stream unbounded tokens into
    // hashing; 128 chars comfortably exceeds the 64-char minted form.
    if token.is_empty() || token.len() > 128 {
        return Err(grant_rejected());
    }
    let token_hash = sha256_hex(token);
    let row = sqlx::query_as::<_, (String, String, String, Option<String>, i64)>(
        "SELECT db, table_name, doc_id, blob_id, expires_at
         FROM rtdb.share_grants WHERE token_hash = $1",
    )
    .bind(&token_hash)
    .fetch_optional(&state.pool)
    .await;
    let Some((db, table_name, doc_id, blob_id, expires_at)) = (match row {
        Ok(r) => r,
        Err(err) if is_undefined_table(&err) => return Err(grant_rejected()),
        Err(err) => return Err(err.into()),
    }) else {
        return Err(grant_rejected());
    };
    if now_ms() > expires_at {
        // Lazy expiry cleanup — best effort; the rejection below is the
        // contract, the delete is hygiene.
        let _ = sqlx::query("DELETE FROM rtdb.share_grants WHERE token_hash = $1")
            .bind(&token_hash)
            .execute(&state.pool)
            .await;
        return Err(grant_rejected());
    }
    Ok(Grant {
        db,
        table_name,
        doc_id,
        blob_id,
    })
}

/// Reads the granted document through the normal read path (`execute_query`
/// with a bypass context — the grant IS the authorization, the same way a
/// machine token bypasses per-row rules). Soft-deleted rows are filtered by
/// the same compiled SQL every other read uses. `Ok(None)` = row gone or
/// table no longer declared; callers decide the envelope (mint: specific
/// NotFound; public: generic rejection).
async fn read_granted_doc(
    state: &Arc<AppState>,
    db: &str,
    table_name: &str,
    doc_id: &str,
) -> Result<Option<serde_json::Value>, RtDbError> {
    validate_db_name(db)?;
    let schema = state.schemas.get(&state.pool, db).await?;
    if !schema.tables.contains_key(table_name) {
        return Ok(None);
    }
    let query = Query {
        table: table_name.to_string(),
        get: Some(doc_id.to_string()),
        ..Default::default()
    };
    match execute_query(
        &state.pool,
        db,
        &schema,
        &query,
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    {
        Ok(QueryResult::Doc(doc)) => Ok(doc),
        Ok(_) => Ok(None),
        Err(err) => {
            tracing::debug!(error = %err, "share grant doc read failed");
            Ok(None)
        }
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShareGrantRequest {
    /// Schema-declared table the grant targets.
    table: String,
    /// The document id to grant.
    id: String,
    /// Optional storage id the grant may mint a signed URL for. Must live in
    /// the same db (verified here and re-verified at mint-url time).
    #[serde(default)]
    blob_id: Option<String>,
    /// Grant lifetime in seconds; default 7d, clamped to [1s, 30d].
    #[serde(default)]
    ttl_seconds: Option<i64>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ShareGrantResponse {
    token: String,
    /// Ready-to-serve public URL (`{public_url}/share/{token}`).
    url: String,
    expires_at: i64,
}

/// `POST /api/db/{db}/share-grants` — mint a share grant (authed; flag-gated).
/// The token is returned ONCE; only its hash is stored. Specific errors are
/// fine here — the caller is a db-authorized principal, so "unknown table" /
/// "unknown document" leak nothing to an attacker.
pub(crate) async fn mint_share_grant_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<ShareGrantRequest>,
) -> Result<Json<ShareGrantResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    if !state.config.storage.share_grants_enabled {
        return Err(RtDbError::forbidden(
            "share grants are not enabled on this server",
        ));
    }
    validate_db_name(&db)?;
    // The table must be declared in the pushed schema — the grant row's
    // table_name later parameterizes a read, so it only ever holds a
    // schema-validated name.
    let schema = state.schemas.get(&state.pool, &db).await?;
    if !schema.tables.contains_key(&body.table) {
        return Err(RtDbError::not_found("unknown table"));
    }
    // The row must exist at mint time — a grant is a capability for real
    // content, not a speculative handle.
    let doc = read_granted_doc(&state, &db, &body.table, &body.id).await?;
    if doc.is_none() {
        return Err(RtDbError::not_found("unknown document"));
    }
    // SEC-113 discipline at the grant level: a named blob must exist and live
    // in this db — a db-A grant can never sign a db-B blob.
    if let Some(blob_id) = &body.blob_id {
        let owner = storage::resolve_db(&state.pool, blob_id)
            .await?
            .ok_or_else(|| RtDbError::not_found("unknown file"))?;
        if owner != db {
            return Err(RtDbError::not_found("unknown file"));
        }
    }
    let ttl = body
        .ttl_seconds
        .unwrap_or(DEFAULT_SHARE_TTL_SECS)
        .clamp(1, MAX_SHARE_TTL_SECS);
    let expires_at = now_ms() + ttl * 1000;
    ensure_grants_table(&state.pool).await?;
    let token = random_token();
    let token_hash = sha256_hex(&token);
    sqlx::query(
        "INSERT INTO rtdb.share_grants (token_hash, db, table_name, doc_id, blob_id, expires_at)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (token_hash) DO NOTHING",
    )
    .bind(&token_hash)
    .bind(&db)
    .bind(&body.table)
    .bind(&body.id)
    .bind(&body.blob_id)
    .bind(expires_at)
    .execute(&state.pool)
    .await?;
    let base = state.config.public_url.trim_end_matches('/');
    Ok(Json(ShareGrantResponse {
        url: format!("{base}/share/{token}"),
        token,
        expires_at,
    }))
}

/// `DELETE /api/db/{db}/share-grants/{token}` — revoke a grant (authed;
/// flag-gated; idempotent; db-scoped so a db-A principal cannot revoke db-B
/// grants even holding a valid token for one). Returning `{ok: true}` for a
/// token that never existed mirrors the storage delete's no-disclosure
/// behavior.
pub(crate) async fn revoke_share_grant_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, token)): Path<(String, String)>,
) -> Result<Json<crate::http_api::storage::OkResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    if !state.config.storage.share_grants_enabled {
        return Err(RtDbError::forbidden(
            "share grants are not enabled on this server",
        ));
    }
    validate_db_name(&db)?;
    if token.is_empty() || token.len() > 128 {
        return Ok(Json(crate::http_api::storage::OkResponse { ok: true }));
    }
    let token_hash = sha256_hex(&token);
    match sqlx::query("DELETE FROM rtdb.share_grants WHERE token_hash = $1 AND db = $2")
        .bind(&token_hash)
        .bind(&db)
        .execute(&state.pool)
        .await
    {
        Ok(_) => {}
        Err(err) if is_undefined_table(&err) => {}
        Err(err) => return Err(err.into()),
    }
    Ok(Json(crate::http_api::storage::OkResponse { ok: true }))
}

/// `GET /share/{token}` — public, unauthenticated read of the granted row.
/// Every failure is the identical generic envelope; the flag-off state is
/// indistinguishable from an invalid token.
pub(crate) async fn share_read_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(token): Path<String>,
) -> Result<Json<serde_json::Value>, RtDbError> {
    check_storage_public_rate_limit(
        &state,
        &client_ip_key(&headers, addr.ip(), state.config.trusted_proxy),
    )
    .await?;
    let grant = resolve_grant(&state, &token).await?;
    let doc = read_granted_doc(&state, &grant.db, &grant.table_name, &grant.doc_id)
        .await?
        .ok_or_else(grant_rejected)?;
    Ok(Json(doc))
}

/// `GET /share/{token}/signed-url[?ttlSeconds=]` — public, unauthenticated
/// mint of a signed URL for EXACTLY the grant's `blob_id` (the route takes no
/// blob parameter; db-wide minting is impossible by construction). A grant
/// without a `blob_id` gets the same generic rejection as an invalid token.
///
/// Interplay with `RTDB_STORAGE_REQUIRE_SIGNED_URLS`: the share mints a normal
/// signed URL, so it serves under either mode — under require-signature mode
/// it is in fact the ONLY way a share visitor can fetch the granted blob,
/// which is exactly the coherent composition (the token grants the doc, the
/// signed URL grants the bytes, both single-target capabilities).
pub(crate) async fn share_signed_url_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(token): Path<String>,
    AxumQuery(q): AxumQuery<HashMap<String, String>>,
) -> Result<Json<SignedUrlResponse>, RtDbError> {
    check_storage_public_rate_limit(
        &state,
        &client_ip_key(&headers, addr.ip(), state.config.trusted_proxy),
    )
    .await?;
    let grant = resolve_grant(&state, &token).await?;
    let Some(blob_id) = grant.blob_id else {
        return Err(grant_rejected());
    };
    // Re-verify the blob still exists and still lives in the grant's db — the
    // blob could have been deleted, or a same-id re-upload could have landed
    // elsewhere after mint time. A failure here is the generic rejection (the
    // blob's existence is not disclosed to an unauthenticated caller).
    let owner = match storage::resolve_db(&state.pool, &blob_id).await {
        Ok(Some(owner)) => owner,
        Ok(None) => return Err(grant_rejected()),
        Err(err) => {
            tracing::debug!(error = %err, "share grant blob resolve failed");
            return Err(grant_rejected());
        }
    };
    if owner != grant.db {
        return Err(grant_rejected());
    }
    let ttl = q
        .get("ttlSeconds")
        .and_then(|v| v.parse::<i64>().ok())
        .map(|v| v.clamp(1, signed_url::MAX_SIGNED_URL_TTL_SECS as i64) as u64)
        .unwrap_or(signed_url::DEFAULT_SIGNED_URL_TTL_SECS);
    let exp = now_ms() + (ttl as i64) * 1000;
    let sig = signed_url::sign(&state.limits.signed_url_key, &blob_id, exp, "");
    let base = state.config.public_url.trim_end_matches('/');
    Ok(Json(SignedUrlResponse {
        url: format!("{base}/storage/{blob_id}?exp={exp}&sig={sig}"),
        expires_at: exp,
    }))
}
