//! Admin interactive-session routes: list, revoke one, revoke all for a user.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::session::{self, SessionInfo};
use crate::error::RtDbError;

use super::{OkResponse, require_admin};

const DEFAULT_LIMIT: i64 = 200;
const MAX_LIMIT: i64 = 1000;

#[derive(Deserialize)]
pub(super) struct SessionsParams {
    /// Optional: match `user_id` OR `email`. Omitted => all sessions (server-wide).
    #[serde(default)]
    user: Option<String>,
    /// Optional; clamped to [1, 1000], default 200.
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Serialize)]
pub(super) struct SessionsResponse {
    sessions: Vec<SessionInfo>,
}

pub(super) async fn list_sessions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<SessionsParams>,
) -> Result<Json<SessionsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let sessions = session::list_sessions(&state.pool, params.user.as_deref(), limit).await?;
    Ok(Json(SessionsResponse { sessions }))
}

/// Revoke a single session by its `token_hash` (path param).
pub(super) async fn revoke_session_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(token_hash): Path<String>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    session::delete_session_by_hash(&state.pool, &token_hash).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct RevokeUserParams {
    user: String,
}

#[derive(Serialize)]
pub(super) struct RevokeUserResponse {
    ok: bool,
    revoked: u64,
}

/// Revoke every session for a user. Requires `?user=` — a bare DELETE is a 400
/// (refuse to revoke every session instance-wide from one unscoped call).
pub(super) async fn revoke_user_sessions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<RevokeUserParams>,
) -> Result<Json<RevokeUserResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let revoked = session::delete_sessions_for_user(&state.pool, &params.user).await?;
    Ok(Json(RevokeUserResponse { ok: true, revoked }))
}
