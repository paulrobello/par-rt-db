//! Admin identity & access: key login/logout, dashboard admins, db allowlist.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::{AppState, auth, db};

use super::{OkResponse, require_admin};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminLoginRequest {
    admin_key: String,
}

/// `POST /admin/login` — validates the admin key (constant-time, the same
/// compare `authenticate_admin` runs) and, on success, issues the SEC-001
/// HttpOnly session cookie. On a bad key we 401 without touching the cookie.
/// The credential written is `state.config.admin_key` (the trusted configured
/// value), never the raw request body, so a `;`-laden guess cannot inject cookie
/// attributes — `set_session_cookie` validates regardless.
pub(super) async fn admin_login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AdminLoginRequest>,
) -> Result<Response, RtDbError> {
    let valid = body
        .admin_key
        .as_bytes()
        .ct_eq(state.config.admin_key.as_bytes());
    if !bool::from(valid) {
        return Err(RtDbError::unauthorized("invalid admin key"));
    }
    let cookie = auth::cookie::set_session_cookie(
        &state.config.admin_key,
        auth::cookie::request_is_secure(&headers),
    )?;
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut().insert(SET_COOKIE, cookie);
    Ok(resp)
}

/// `POST /admin/logout` — clears the SEC-001 session cookie.
pub(super) async fn admin_logout() -> Response {
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut()
        .insert(SET_COOKIE, auth::cookie::clear_session_cookie());
    resp
}

#[derive(Deserialize)]
pub(super) struct AllowlistWriteRequest {
    db: String,
    action: String,
    email: String,
}

pub(super) async fn allowlist_write(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AllowlistWriteRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &body.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let email = body.email.to_lowercase();

    match body.action.as_str() {
        "add" => {
            sqlx::query(
                "INSERT INTO rtdb_auth.allowlist (db_name, email) VALUES ($1, $2) \
                 ON CONFLICT (db_name, email) DO NOTHING",
            )
            .bind(&body.db)
            .bind(&email)
            .execute(&state.pool)
            .await?;
        }
        "remove" => {
            sqlx::query("DELETE FROM rtdb_auth.allowlist WHERE db_name = $1 AND email = $2")
                .bind(&body.db)
                .bind(&email)
                .execute(&state.pool)
                .await?;
        }
        other => {
            return Err(RtDbError::bad_request(format!("unknown action '{other}'")));
        }
    }

    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct AllowlistListParams {
    db: String,
}

#[derive(Serialize)]
pub(super) struct AllowlistListResponse {
    emails: Vec<String>,
}

pub(super) async fn allowlist_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<AllowlistListParams>,
) -> Result<Json<AllowlistListResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }

    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT email FROM rtdb_auth.allowlist WHERE db_name = $1 ORDER BY email")
            .bind(&params.db)
            .fetch_all(&state.pool)
            .await?;

    Ok(Json(AllowlistListResponse {
        emails: rows.into_iter().map(|(email,)| email).collect(),
    }))
}

#[derive(Serialize)]
pub(super) struct AdminMember {
    email: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "githubId")]
    github_id: Option<i64>,
}

#[derive(Serialize)]
pub(super) struct AdminsResponse {
    admins: Vec<AdminMember>,
}

/// All dashboard admins, email-ordered. Shared by `list_admins` and the config
/// read-back so the dashboard can render the allowlist alongside hot config.
pub(super) async fn admin_members(pool: &sqlx::PgPool) -> Result<Vec<AdminMember>, RtDbError> {
    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(email, github_id)| AdminMember { email, github_id })
        .collect())
}

pub(super) async fn list_admins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<AdminsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(AdminsResponse {
        admins: admin_members(&state.pool).await?,
    }))
}

#[derive(Deserialize)]
pub(super) struct AddAdminRequest {
    email: String,
    #[serde(rename = "githubId")]
    github_id: Option<i64>,
}

pub(super) async fn add_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AddAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let email = body.email.trim().to_lowercase();
    if email.is_empty() {
        return Err(RtDbError::bad_request("email is required"));
    }
    // ON CONFLICT merge: keep any existing github_id if the new one is absent.
    sqlx::query(
        "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, $2, $3) \
         ON CONFLICT (email) DO UPDATE SET \
            github_id = COALESCE(EXCLUDED.github_id, rtdb_auth.admins.github_id)",
    )
    .bind(&email)
    .bind(body.github_id)
    .bind(crate::db::now_ms())
    .execute(&state.pool)
    .await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct RemoveAdminRequest {
    email: String,
}

pub(super) async fn remove_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RemoveAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    sqlx::query("DELETE FROM rtdb_auth.admins WHERE email = $1")
        .bind(body.email.trim().to_lowercase())
        .execute(&state.pool)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}
