//! Admin identity & access: key login/logout, dashboard admins, db allowlist.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Query as QueryParams, State};
use axum::http::{HeaderMap, StatusCode, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::{AppState, auth, db};

use super::OkResponse;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminLoginRequest {
    admin_key: String,
}

/// `POST /admin/login` — validates the admin key (constant-time, the same
/// compare `authenticate_admin` runs) and, on success, issues the SEC-001
/// HttpOnly session cookie plus the readable SEC-106 admin CSRF nonce. On a bad
/// key we 401 without touching the cookies. The credential written is
/// `state.config.admin_key` (the trusted configured value), never the raw
/// request body, so a `;`-laden guess cannot inject cookie attributes —
/// `set_session_cookie` validates regardless.
///
/// SEC-109: per-IP rate limiting bounds brute-force over this public endpoint.
/// Each failure (rate-limited or bad key) increments the
/// `rtdb_admin_auth_failures_total` counter and logs at WARN — a burst of those
/// is the brute-force signal operators alert on.
pub(super) async fn admin_login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    ApiJson(body): ApiJson<AdminLoginRequest>,
) -> Result<Response, RtDbError> {
    // SEC-109: per-IP fixed-window rate limit. 0 = off (the default); a
    // non-zero value like 10 means one IP gets 10 attempts/minute before 429.
    // Checked BEFORE the ct_eq so a flood of guesses never reaches the compare.
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    let limit = state.config.admin_rate_limit_per_ip_rpm;
    if limit > 0 {
        match state
            .limits
            .rate_limiter
            .check(
                crate::rate_limit::RateKey::Ip {
                    route: "admin_login",
                    ip: ip_key.clone(),
                },
                limit,
            )
            .await
        {
            crate::rate_limit::RateDecision::Denied { retry_after_secs } => {
                state.runtime.metrics.record_admin_auth_failure();
                tracing::warn!(
                    ip = %ip_key,
                    "admin login rate-limited (limit {} rpm)",
                    limit
                );
                return Err(RtDbError::rate_limited(retry_after_secs));
            }
            crate::rate_limit::RateDecision::Allowed => {}
        }
    }

    // SEC-110: defense-in-depth — never authenticate against an empty key.
    // `Config::from_env` rejects this at boot, but `admin_login` does its own
    // ct_eq (not through `authenticate_admin`), so guard here too.
    if state.config.admin_key.trim().is_empty() {
        state.runtime.metrics.record_admin_auth_failure();
        tracing::warn!(ip = %ip_key, "admin login rejected: admin key not configured");
        return Err(RtDbError::unauthorized("invalid admin key"));
    }

    let valid = body
        .admin_key
        .as_bytes()
        .ct_eq(state.config.admin_key.as_bytes());
    if !bool::from(valid) {
        state.runtime.metrics.record_admin_auth_failure();
        tracing::warn!(ip = %ip_key, "admin login rejected: bad key");
        return Err(RtDbError::unauthorized("invalid admin key"));
    }
    // SEC-120: mint a hashed, revocable admin session row and put its plaintext
    // token in the cookie — never the raw `config.admin_key`. The row lands in
    // `/admin/sessions` and is revocable via `DELETE /admin/sessions/{hash}`,
    // so a stolen cookie is revocable without rotating the admin key (which
    // would also invalidate every outstanding signed storage URL). The TTL
    // mirrors the OAuth session TTL (`session_ttl_days`, default 30) so the
    // cookie Max-Age and the server-side row expire together.
    let ttl_days = state.runtime.hot.load().session_ttl_days;
    let admin_session_token = auth::session::create_admin_session(&state.pool, ttl_days).await?;
    let secure = state.config.cookie_secure
        || auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    let cookie = auth::cookie::set_session_cookie(&admin_session_token, secure)?;
    // SEC-106: mint the readable CSRF nonce alongside the session cookie. The
    // dashboard reads it via `document.cookie` and echoes it in the
    // `X-Rtdb-Csrf` header on mutating admin requests; a cross-site forge
    // cannot read the cookie and so cannot set the header. `append` (not
    // `insert`) so both Set-Cookie values reach the browser.
    let csrf_cookie = auth::cookie::set_admin_csrf_cookie(&db::random_token(), secure)?;
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut().append(SET_COOKIE, cookie);
    resp.headers_mut().append(SET_COOKIE, csrf_cookie);
    Ok(resp)
}

/// `POST /admin/logout` — clears the SEC-001 session cookie and SEC-106 CSRF nonce.
pub(super) async fn admin_logout() -> Response {
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut()
        .append(SET_COOKIE, auth::cookie::clear_session_cookie());
    resp.headers_mut()
        .append(SET_COOKIE, auth::cookie::clear_admin_csrf_cookie());
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
    _headers: HeaderMap,
    ApiJson(body): ApiJson<AllowlistWriteRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
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
    _headers: HeaderMap,
    QueryParams(params): QueryParams<AllowlistListParams>,
) -> Result<Json<AllowlistListResponse>, RtDbError> {
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
    _headers: HeaderMap,
) -> Result<Json<AdminsResponse>, RtDbError> {
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
    _headers: HeaderMap,
    ApiJson(body): ApiJson<AddAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
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
    _headers: HeaderMap,
    ApiJson(body): ApiJson<RemoveAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    sqlx::query("DELETE FROM rtdb_auth.admins WHERE email = $1")
        .bind(body.email.trim().to_lowercase())
        .execute(&state.pool)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}
