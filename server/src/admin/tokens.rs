//! Admin machine-token routes: mint, revoke, and list a database's tokens.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
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
    let (token_id, token) = auth::tokens::mint_token(
        &state.pool,
        &body.db,
        &body.name,
        body.expires_at,
        body.read_only,
        body.tables.as_deref(),
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
    // type-complexity lint happy without inlining a 7-tuple into the signature.
    type TokenRowDb = (
        String,
        String,
        i64,
        bool,
        Option<i64>,
        bool,
        Option<Vec<String>>,
    );
    let rows: Vec<TokenRowDb> = sqlx::query_as(
        "SELECT id, name, created_at, revoked, expires_at, read_only, tables \
         FROM rtdb_auth.machine_tokens WHERE db_name = $1 ORDER BY created_at",
    )
    .bind(&params.db)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(TokensResponse {
        tokens: rows
            .into_iter()
            .map(
                |(id, name, created_at, revoked, expires_at, read_only, tables)| TokenRow {
                    id,
                    name,
                    created_at,
                    revoked,
                    expires_at,
                    read_only,
                    tables,
                },
            )
            .collect(),
    }))
}
