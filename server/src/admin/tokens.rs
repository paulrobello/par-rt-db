//! Admin machine-token routes: mint, revoke, and list a database's tokens,
//! plus the enrollment-grant create/revoke admin surface.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use par_rt_db_core::host_capability::HostCapability;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::{AppState, auth, db};

use super::OkResponse;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MintTokenRequest {
    db: String,
    name: String,
    #[serde(default)]
    expires_at: Option<i64>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    tables: Option<Vec<String>>,
    /// Host machine authorization: optional host capability; the field's
    /// absence (`None`) mints legacy semantics unchanged.
    #[serde(default)]
    host_capability: Option<HostCapability>,
}

#[derive(Serialize)]
pub(super) struct MintTokenResponse {
    #[serde(rename = "tokenId")]
    token_id: String,
    token: String,
}

pub(super) async fn mint_token(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<MintTokenRequest>,
) -> Result<Json<MintTokenResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &body.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let (token_id, token) = auth::tokens::mint_token_with_capability(
        &state.pool,
        &body.db,
        &body.name,
        body.expires_at,
        body.read_only,
        body.tables.as_deref(),
        body.host_capability.as_ref(),
    )
    .await?;
    Ok(Json(MintTokenResponse { token_id, token }))
}

#[derive(Deserialize)]
pub(super) struct RevokeTokenRequest {
    #[serde(rename = "tokenId")]
    token_id: String,
}

pub(super) async fn revoke_token(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<RevokeTokenRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    auth::tokens::revoke_token(&state.pool, &body.token_id).await?;
    Ok(Json(OkResponse { ok: true }))
}

// --- Enrollment grants (host-machine-auth) ---------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateEnrollmentGrantRequest {
    db: String,
    name: String,
    capability: HostCapability,
    expires_at: i64,
    #[serde(default)]
    token_expires_at: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EnrollmentGrantResponse {
    grant_id: String,
    grant: String,
    db: String,
    capability: HostCapability,
    expires_at: i64,
}

pub(super) async fn create_enrollment_grant(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<CreateEnrollmentGrantRequest>,
) -> Result<Json<EnrollmentGrantResponse>, RtDbError> {
    let created = auth::enrollment::create_grant(
        &state.pool,
        &body.db,
        &body.name,
        &body.capability,
        body.expires_at,
        body.token_expires_at,
    )
    .await?;
    Ok(Json(EnrollmentGrantResponse {
        grant_id: created.grant_id,
        grant: created.grant,
        db: created.db,
        capability: created.capability,
        expires_at: created.expires_at,
    }))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RevokeEnrollmentGrantRequest {
    #[serde(rename = "grantId")]
    grant_id: String,
}

pub(super) async fn revoke_enrollment_grant(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<RevokeEnrollmentGrantRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    auth::enrollment::revoke_grant(&state.pool, &body.grant_id).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TokenRow {
    id: String,
    name: String,
    created_at: i64,
    revoked: bool,
    expires_at: Option<i64>,
    read_only: bool,
    tables: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host_capability: Option<HostCapability>,
}

#[derive(Serialize)]
pub(super) struct TokensResponse {
    tokens: Vec<TokenRow>,
}

#[derive(Deserialize)]
pub(super) struct TokensParams {
    db: String,
}

pub(super) async fn list_tokens(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<TokensParams>,
) -> Result<Json<TokensResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    // Column order matches the SELECT below; aliased to keep clippy's
    // type-complexity lint happy without inlining an 8-tuple into the
    // signature.
    type TokenRowDb = (
        String,
        String,
        i64,
        bool,
        Option<i64>,
        bool,
        Option<Vec<String>>,
        Option<serde_json::Value>,
    );
    let rows: Vec<TokenRowDb> = sqlx::query_as(
        "SELECT id, name, created_at, revoked, expires_at, read_only, tables, capability \
         FROM rtdb_auth.machine_tokens WHERE db_name = $1 ORDER BY created_at",
    )
    .bind(&params.db)
    .fetch_all(&state.pool)
    .await?;
    let mut tokens = Vec::with_capacity(rows.len());
    for (id, name, created_at, revoked, expires_at, read_only, tables, capability) in rows {
        // Stored capabilities were validated at mint time; a row that fails
        // to parse is a server bug and fails closed rather than listing a
        // capability the server would not enforce.
        let host_capability = capability
            .map(serde_json::from_value::<HostCapability>)
            .transpose()
            .map_err(|_| RtDbError::internal("stored machine-token capability is invalid"))?;
        tokens.push(TokenRow {
            id,
            name,
            created_at,
            revoked,
            expires_at,
            read_only,
            tables,
            host_capability,
        });
    }
    Ok(Json(TokensResponse { tokens }))
}
