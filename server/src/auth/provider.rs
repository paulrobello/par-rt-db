use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::LOCATION};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use serde::Deserialize;

use crate::AppState;
use crate::auth::{Principal, authed_user, resolve_bearer, session};
use crate::config::Config;
use crate::db::{now_ms, random_token};
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

use super::github::GithubProvider;
use super::google::GoogleProvider;

const STATE_TTL_MS: i64 = 10 * 60 * 1000;

/// One pending `/auth/{provider}` -> `/auth/{provider}/callback` round trip:
/// the origin the popup was opened from (echoed back into the callback HTML)
/// and when this entry expires. Held in `AppState.oauth_states`, keyed by the
/// state token; consumed (removed) exactly once by the callback, whichever
/// request gets the lock first — see `consume_state`.
pub struct OAuthStateEntry {
    pub origin: String,
    pub expires_at: i64,
}

/// A pluggable OAuth provider. Each implementation owns its authorize URL,
/// its callback path, and the full code-for-session exchange (`complete_login`)
/// — the HTTP dance is provider-specific, so it stays there. The shared route
/// plumbing in this module (state tokens, redirects, popup HTML, logout, me)
/// is provider-agnostic and generic over `Self`.
///
/// Sessions created by any provider flow through the same `session.rs` /
/// `tokens.rs` machinery, so revocation and per-Subscribe/Mutate `authorize`
/// keep working unchanged regardless of which provider minted the token.
#[async_trait]
pub trait OAuthProvider: Send + Sync {
    /// Human-readable slug for status/error bodies (e.g. "github", "google").
    /// Associated (not `&self`) so it's available even when the provider is
    /// unconfigured and has no instance — used for the "not configured" 503.
    fn name() -> &'static str;

    /// `Some(self)` when this provider is fully configured in `config`, else
    /// `None` — the route handlers treat `None` as "503 not configured".
    fn from_config(config: &Config) -> Option<Self>
    where
        Self: Sized;

    /// Path of this provider's callback, relative to the site root — joined to
    /// `public_url` to form the `redirect_uri` sent at authorize time and the
    /// URL the provider redirects back to.
    fn callback_path(&self) -> &'static str;

    /// Fully-formed authorize URL the browser is 302'd to, including scopes.
    fn authorize_url(&self, redirect_uri: &str, state: &str) -> String;

    /// Exchanges `code` for an access token, resolves the user identity,
    /// upserts `rtdb_auth.users`, and mints a session. Returns the plaintext
    /// session token. Client-facing errors stay generic; internal failures
    /// surface as `RtDbError::internal` (logged via `tracing`).
    async fn complete_login(&self, state: &Arc<AppState>, code: &str) -> Result<String, RtDbError>;
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Builds a bare 302 redirect (axum's `Redirect::to` is a 303, which doesn't
/// match the OAuth flow's contract of a 302 to the authorize page).
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

/// The 503 body returned when a route is hit for a provider that has no
/// `client_id`/`client_secret` configured — matches the original GitHub
/// handler's status + envelope.
fn unconfigured_response(name: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(RtDbError::internal(format!("{name} oauth not configured"))),
    )
        .into_response()
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
struct StartParams {
    origin: String,
}

/// `GET /auth/{provider}?origin=<url>`: redirects the browser to the
/// provider's OAuth authorize page. `origin` must be an exact member of the
/// live `hot.allowed_origins` (else 403) — it is never taken from the eventual
/// callback request, only from this validated start step, so the popup can
/// only ever be told to postMessage back to an origin we approved here.
async fn provider_start<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<StartParams>,
) -> Response {
    let Some(provider) = P::from_config(&state.config) else {
        return unconfigured_response(P::name());
    };

    if !state
        .hot
        .load()
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

    let redirect_uri = format!("{}{}", state.config.public_url, provider.callback_path());
    let url = provider.authorize_url(&redirect_uri, &state_token);

    redirect_found(&url)
}

#[derive(Deserialize)]
struct CallbackParams {
    code: String,
    state: String,
}

/// `GET /auth/{provider}/callback?code=&state=`: verifies and consumes the
/// state token, completes the provider exchange, and responds with HTML that
/// posts the new session token back to the opener window.
async fn provider_callback<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<CallbackParams>,
) -> Response {
    let Some(origin) = consume_state(&state, &params.state).await else {
        return RtDbError::bad_request("invalid or expired state").into_response();
    };

    let Some(provider) = P::from_config(&state.config) else {
        return unconfigured_response(P::name());
    };

    match provider.complete_login(&state, &params.code).await {
        Ok(token) => callback_html_response(&token, &origin),
        Err(err) => err.into_response(),
    }
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

#[derive(serde::Serialize)]
struct OkResponse {
    ok: bool,
}

/// `POST /auth/logout`: idempotent regardless of whether the bearer token is
/// present, valid, or already expired — a `DELETE` matching zero rows is not
/// an error, so only a genuine query failure (not a merely-absent session)
/// produces a 500 here.
async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Some(token) = bearer_token(&headers)
        && let Err(err) = session::delete_session(&state.pool, token).await
    {
        return err.into_response();
    }
    Json(OkResponse { ok: true }).into_response()
}

#[derive(serde::Serialize)]
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

/// `GET /auth/validate`: validates an arbitrary presented session/machine
/// token through the same machinery `/auth/me` uses — `resolve_bearer` checks
/// machine-token revocation and session expiry live — and returns the
/// `AuthedUser`. Unlike `/auth/me` (session-only, because it represents the
/// caller's own connection), this also accepts a machine token, so a trusted
/// backend can validate either kind of player token it is handed. The token
/// is supplied by the player being validated, via the bearer header; an
/// invalid/expired token surfaces as the standard `RtDbError` auth envelope.
async fn validate(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(token) = bearer_token(&headers) else {
        return RtDbError::unauthorized("missing bearer token").into_response();
    };

    match resolve_bearer(&state.pool, token).await {
        Ok(principal) => Json(MeResponse {
            user: authed_user(&principal),
        })
        .into_response(),
        Err(err) => err.into_response(),
    }
}

/// OAuth + session routes. GitHub keeps its original paths
/// (`/auth/github`, `/auth/callback`) so deployed clients are unaffected;
/// Google mounts at `/auth/google` + `/auth/google/callback`. `/auth/logout`
/// and `/auth/me` are provider-agnostic.
pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/auth/github", get(provider_start::<GithubProvider>))
        .route("/auth/callback", get(provider_callback::<GithubProvider>))
        .route("/auth/google", get(provider_start::<GoogleProvider>))
        .route(
            "/auth/google/callback",
            get(provider_callback::<GoogleProvider>),
        )
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/validate", get(validate))
}
