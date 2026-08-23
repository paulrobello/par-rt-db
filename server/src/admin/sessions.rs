//! Admin interactive-session routes: list, revoke one, revoke all for a user.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::session::{self, SessionInfo};
use crate::error::RtDbError;

use super::OkResponse;

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
    _headers: HeaderMap,
    QueryParams(params): QueryParams<SessionsParams>,
) -> Result<Json<SessionsResponse>, RtDbError> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let sessions = session::list_sessions(&state.pool, params.user.as_deref(), limit).await?;
    Ok(Json(SessionsResponse { sessions }))
}

/// Revoke a single session by its `token_hash` (path param).
pub(super) async fn revoke_session_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(token_hash): Path<String>,
) -> Result<Json<OkResponse>, RtDbError> {
    session::delete_session_by_hash(&state.pool, &token_hash).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct RevokeBulkParams {
    /// Revoke every session for this user id.
    #[serde(default)]
    user: Option<String>,
    /// `true` — revoke every EXPIRED session instance-wide (the dashboard's
    /// "remove all expired" action; expired rows otherwise linger until each
    /// is individually used or revoked).
    #[serde(default)]
    expired: Option<bool>,
}

#[derive(Serialize)]
pub(super) struct RevokeBulkResponse {
    ok: bool,
    revoked: u64,
}

/// Bulk revoke. Exactly one scope: `?user=` (every session of one user) or
/// `?expired=true` (every expired session, `sessions` + `admin_sessions`). A
/// bare DELETE — or both params at once — is a 400: refuse to revoke every
/// session instance-wide from one unscoped or ambiguous call.
pub(super) async fn revoke_user_sessions_handler(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<RevokeBulkParams>,
) -> Result<Json<RevokeBulkResponse>, RtDbError> {
    let revoked = match (params.user.as_deref(), params.expired.unwrap_or(false)) {
        (Some(user), false) => session::delete_sessions_for_user(&state.pool, user).await?,
        (None, true) => session::delete_expired_sessions(&state.pool).await?,
        (Some(_), true) => {
            return Err(RtDbError::bad_request(
                "pass exactly one of ?user= or ?expired=true",
            ));
        }
        (None, false) => {
            return Err(RtDbError::bad_request(
                "missing scope: pass ?user= or ?expired=true",
            ));
        }
    };
    Ok(Json(RevokeBulkResponse { ok: true, revoked }))
}
