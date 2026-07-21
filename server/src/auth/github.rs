use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::LOCATION};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::{Principal, authed_user, resolve_bearer, session};
use crate::db::{new_id, now_ms, random_token};
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

const STATE_TTL_MS: i64 = 10 * 60 * 1000;

/// One pending `/auth/github` -> `/auth/callback` round trip: the origin the
/// popup was opened from (echoed back into the callback HTML) and when this
/// entry expires. Held in `AppState.oauth_states`, keyed by the state token;
/// consumed (removed) exactly once by the callback, whichever request gets
/// the lock first — see `consume_state`.
pub struct OAuthStateEntry {
    pub origin: String,
    pub expires_at: i64,
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Builds a bare 302 redirect (axum's `Redirect::to` is a 303, which doesn't
/// match the GitHub OAuth flow's contract of a 302 to the authorize page).
fn redirect_found(url: &str) -> Response {
    match Response::builder()
        .status(StatusCode::FOUND)
        .header(LOCATION, url)
        .body(Body::empty())
    {
        Ok(response) => response,
        Err(_) => RtDbError::internal("failed to build redirect").into_response(),
    }
}

/// Removes and returns the origin for `state_token`, but only if it exists
/// and has not expired — single-use by construction: a replayed token was
/// already removed by the first successful call, so it resolves to `None`
/// (see Step 1(e)). Concurrent callers race on the same `Mutex`; whichever
/// acquires it first wins the entry.
async fn consume_state(state: &Arc<AppState>, state_token: &str) -> Option<String> {
    let mut states = state.oauth_states.lock().await;
    match states.remove(state_token) {
        Some(entry) if entry.expires_at > now_ms() => Some(entry.origin),
        _ => None,
    }
}

#[derive(Deserialize)]
struct GithubStartParams {
    origin: String,
}

/// `GET /auth/github?origin=<url>`: redirects the browser to GitHub's OAuth
/// authorize page. `origin` must be an exact member of
/// `config.allowed_origins` (else 403) — it is never taken from the eventual
/// callback request, only from this validated start step, so the popup can
/// only ever be told to postMessage back to an origin we approved here.
async fn github_start(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GithubStartParams>,
) -> Response {
    let Some(client_id) = state.config.github_client_id.as_deref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(RtDbError::internal("github oauth not configured")),
        )
            .into_response();
    };

    if !state
        .config
        .allowed_origins
        .iter()
        .any(|allowed| allowed == &params.origin)
    {
        return RtDbError::forbidden("origin not allowed").into_response();
    }

    let state_token = random_token();
    let now = now_ms();
    {
        let mut states = state.oauth_states.lock().await;
        states.retain(|_, entry| entry.expires_at > now);
        states.insert(
            state_token.clone(),
            OAuthStateEntry {
                origin: params.origin.clone(),
                expires_at: now + STATE_TTL_MS,
            },
        );
    }

    let redirect_uri = format!("{}/auth/callback", state.config.public_url);
    let url = format!(
        "{}/login/oauth/authorize?client_id={client_id}&redirect_uri={redirect_uri}&scope=read:user%20user:email&state={state_token}",
        state.config.github_base_url,
    );

    redirect_found(&url)
}

#[derive(Deserialize)]
struct GithubCallbackParams {
    code: String,
    state: String,
}

#[derive(Serialize)]
struct TokenExchangeRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    code: &'a str,
    redirect_uri: &'a str,
}

#[derive(Deserialize)]
struct TokenExchangeResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct GithubUser {
    id: i64,
    login: String,
    email: Option<String>,
}

#[derive(Deserialize)]
struct GithubEmail {
    email: String,
    primary: bool,
    verified: bool,
}

/// Exchanges `code` for a GitHub access token, resolves the user's identity
/// and best email, upserts `rtdb_auth.users`, and mints a session. Returns
/// the plaintext session token.
async fn complete_github_login(
    state: &Arc<AppState>,
    client_id: &str,
    client_secret: &str,
    code: &str,
) -> Result<String, RtDbError> {
    let client = reqwest::Client::new();
    let redirect_uri = format!("{}/auth/callback", state.config.public_url);

    let token_resp: TokenExchangeResponse = client
        .post(format!(
            "{}/login/oauth/access_token",
            state.config.github_base_url
        ))
        .header(reqwest::header::ACCEPT, "application/json")
        .form(&TokenExchangeRequest {
            client_id,
            client_secret,
            code,
            redirect_uri: &redirect_uri,
        })
        .send()
        .await
        .map_err(|_| RtDbError::internal("github token exchange failed"))?
        .json()
        .await
        .map_err(|_| RtDbError::internal("github token exchange failed"))?;

    let access_token = token_resp.access_token;

    let user: GithubUser = client
        .get(format!("{}/user", state.config.github_api_url))
        .header(reqwest::header::USER_AGENT, "par-rt-db")
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {access_token}"),
        )
        .send()
        .await
        .map_err(|_| RtDbError::internal("github user fetch failed"))?
        .json()
        .await
        .map_err(|_| RtDbError::internal("github user fetch failed"))?;

    let emails: Vec<GithubEmail> = client
        .get(format!("{}/user/emails", state.config.github_api_url))
        .header(reqwest::header::USER_AGENT, "par-rt-db")
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {access_token}"),
        )
        .send()
        .await
        .map_err(|_| RtDbError::internal("github email fetch failed"))?
        .json()
        .await
        .map_err(|_| RtDbError::internal("github email fetch failed"))?;

    let email = emails
        .iter()
        .find(|e| e.primary && e.verified)
        .or_else(|| emails.iter().find(|e| e.verified))
        .map(|e| e.email.clone())
        .or(user.email.clone())
        .ok_or_else(|| RtDbError::forbidden("no verified email"))?
        .to_lowercase();

    let id = new_id();
    let now = now_ms();
    let (user_id,): (String,) = sqlx::query_as(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5) \
         ON CONFLICT (github_id) DO UPDATE SET login = EXCLUDED.login, email = EXCLUDED.email \
         RETURNING id",
    )
    .bind(&id)
    .bind(user.id)
    .bind(&user.login)
    .bind(&email)
    .bind(now)
    .fetch_one(&state.pool)
    .await?;

    session::create_session(&state.pool, &user_id, state.config.session_ttl_days).await
}

/// Renders the popup-closing HTML the callback returns on success. `token`
/// is hex (from `random_token()`) and `origin` is copied verbatim from the
/// validated `oauth_states` entry — never from the callback request itself —
/// so both interpolations are injection-safe by construction: neither can
/// contain `"`, `<`, or `>`.
fn callback_html_response(token: &str, origin: &str) -> Response {
    let html = format!(
        "<script>window.opener.postMessage({{type:\"rtdb-auth\",token:\"{token}\"}},\"{origin}\");window.close();</script>"
    );

    let mut response = Html(html).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; script-src 'unsafe-inline'"),
    );
    response
}

/// `GET /auth/callback?code=&state=`: verifies and consumes the state token,
/// completes the GitHub exchange, and responds with HTML that posts the new
/// session token back to the opener window.
async fn github_callback(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<GithubCallbackParams>,
) -> Response {
    let Some(origin) = consume_state(&state, &params.state).await else {
        return RtDbError::bad_request("invalid or expired state").into_response();
    };

    // A state entry only ever exists if `github_start` saw a configured
    // client_id/secret, and config is immutable for the process lifetime.
    let client_id = state.config.github_client_id.as_deref().unwrap_or_default();
    let client_secret = state
        .config
        .github_client_secret
        .as_deref()
        .unwrap_or_default();

    match complete_github_login(&state, client_id, client_secret, &params.code).await {
        Ok(token) => callback_html_response(&token, &origin),
        Err(err) => err.into_response(),
    }
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

/// `POST /auth/logout`: idempotent regardless of whether the bearer token is
/// present, valid, or already expired — no information is leaked either way.
async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Json<OkResponse> {
    if let Some(token) = bearer_token(&headers) {
        let _ = session::delete_session(&state.pool, token).await;
    }
    Json(OkResponse { ok: true })
}

#[derive(Serialize)]
struct MeResponse {
    user: AuthedUser,
}

/// `GET /auth/me`: session-only. A machine token resolves fine via
/// `resolve_bearer` but is rejected here (401) since it isn't a user.
async fn me(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(token) = bearer_token(&headers) else {
        return RtDbError::unauthorized("missing bearer token").into_response();
    };

    let principal = match resolve_bearer(&state.pool, token).await {
        Ok(principal) => principal,
        Err(err) => return err.into_response(),
    };

    match principal {
        Principal::User { .. } => Json(MeResponse {
            user: authed_user(&principal),
        })
        .into_response(),
        Principal::Machine { .. } => RtDbError::unauthorized("session required").into_response(),
    }
}

/// GitHub OAuth + session routes: `/auth/github`, `/auth/callback`,
/// `/auth/logout`, `/auth/me`.
pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/auth/github", get(github_start))
        .route("/auth/callback", get(github_callback))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
}
