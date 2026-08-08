use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{Form, Query as QueryParams, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header::SET_COOKIE};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::auth::{Principal, authed_user, resolve_bearer, session};
use crate::config::Config;
use crate::db::{now_ms, random_token};
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

use super::apple::AppleProvider;
use super::github::GithubProvider;
use super::gitlab::GitlabProvider;
use super::google::GoogleProvider;
use super::microsoft::MicrosoftProvider;
use super::oidc::OidcProvider;

const STATE_TTL_MS: i64 = 10 * 60 * 1000;

/// Outcome of a pending OAuth login, driven by the callback. `Pending` →
/// `Claiming` (first callback wins) → `Completed` | `Failed`.
pub enum LoginOutcome {
    Pending,
    Claiming,
    Completed(String),
    Failed,
}

/// One pending `/auth/{provider}/begin` -> `/auth/callback` round trip: the
/// expiry and the current `LoginOutcome`. Held in `AppState.auth.oauth_states`,
/// keyed by the single-use state token minted at `begin`. The first callback
/// flips `Pending` → `Claiming` (see `claim_pending`); after `complete_login`
/// resolves it sets the terminal `Completed` | `Failed`. The poll endpoint
/// removes the entry on a terminal outcome (one-shot retrieval).
pub struct OAuthStateEntry {
    pub expires_at: i64,
    pub outcome: LoginOutcome,
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
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Some(v);
    }
    // SEC-001 phase 2: dashboard cookie path (HttpOnly `rtdb_session`) — used by
    // `/auth/me` and `/auth/logout` when the OAuth session token is cookie-only.
    crate::auth::cookie::session_cookie(headers)
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

/// `Pending → Claiming` for the first caller; `false` for a replay or an
/// already-terminal/expired entry. This is the single-use claim that makes a
/// replayed callback reject.
async fn claim_pending(state: &Arc<AppState>, state_token: &str) -> bool {
    let mut states = state.auth.oauth_states.lock().await;
    let now = now_ms();
    match states.get_mut(state_token) {
        Some(entry) if entry.expires_at > now && matches!(entry.outcome, LoginOutcome::Pending) => {
            entry.outcome = LoginOutcome::Claiming;
            true
        }
        _ => false,
    }
}

/// Sets the terminal outcome after `complete_login` (`Claiming → Completed | Failed`).
async fn set_outcome(state: &Arc<AppState>, state_token: &str, outcome: LoginOutcome) {
    let mut states = state.auth.oauth_states.lock().await;
    if let Some(entry) = states.get_mut(state_token) {
        entry.outcome = outcome;
    }
}

enum PollResult {
    Pending,
    Complete { token: String, user: AuthedUser },
    Failed,
    Expired,
}

/// One-shot retrieval for the `/auth/state` polling endpoint. Removes the
/// entry on a terminal outcome; leaves it in place while pending. The
/// `resolve_bearer` call happens after the lock is released so no Mutex is
/// held across the await.
async fn poll_login(state: &Arc<AppState>, state_token: &str) -> PollResult {
    let taken: Option<Result<String, ()>> = {
        let mut states = state.auth.oauth_states.lock().await;
        let now = now_ms();
        states.retain(|_, e| e.expires_at > now);
        match states.remove(state_token) {
            None => None,
            Some(entry) => match entry.outcome {
                LoginOutcome::Pending | LoginOutcome::Claiming => {
                    states.insert(state_token.to_string(), entry); // not ready — put back
                    return PollResult::Pending;
                }
                LoginOutcome::Completed(t) => Some(Ok(t)),
                LoginOutcome::Failed => Some(Err(())),
            },
        }
    };
    match taken {
        None => PollResult::Expired,
        Some(Err(())) => PollResult::Failed,
        Some(Ok(token)) => match resolve_bearer(&state.pool, &token).await {
            Ok(principal @ Principal::User { .. }) => PollResult::Complete {
                token,
                user: authed_user(&principal),
            },
            _ => PollResult::Expired, // token did not resolve — treat as gone
        },
    }
}

#[derive(Deserialize)]
struct BeginParams {
    origin: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BeginResponse {
    authorize_url: String,
    state: String,
}

/// `GET /auth/{provider}/begin?origin=<parent origin>`: validates the origin
/// against the live allowlist, mints a single-use state token, and returns the
/// provider authorize URL + the state. The parent opens the authorize URL in a
/// `noopener` popup and polls `/auth/state`. SEC-012 replaces the prior
/// `window.opener` postMessage relay — `origin` is validated here and discarded
/// (never interpolated anywhere), retiring the SEC-005 self-XSS surface.
async fn provider_begin<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<BeginParams>,
) -> Response {
    let Some(provider) = P::from_config(&state.config) else {
        return unconfigured_response(P::name());
    };

    if !state
        .runtime
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
        let mut states = state.auth.oauth_states.lock().await;
        states.retain(|_, entry| entry.expires_at > now);
        states.insert(
            state_token.clone(),
            OAuthStateEntry {
                expires_at: now + STATE_TTL_MS,
                outcome: LoginOutcome::Pending,
            },
        );
    }

    let redirect_uri = format!("{}{}", state.config.public_url, provider.callback_path());
    let authorize_url = provider.authorize_url(&redirect_uri, &state_token);
    let mut response = Json(BeginResponse {
        authorize_url,
        state: state_token.clone(),
    })
    .into_response();
    // Login-CSRF: bind this `state` to the initiating browser so a callback the
    // attacker induced the victim to load (the attacker's own exchange) carries no
    // matching cookie and is rejected at `provider_callback`. SameSite=None so it
    // survives the provider → callback cross-site redirect.
    let secure = crate::auth::cookie::request_is_secure(&headers);
    if let Ok(csrf) = crate::auth::cookie::set_oauth_csrf_cookie(&state_token, secure) {
        response.headers_mut().append(SET_COOKIE, csrf);
    }
    response
}

#[derive(Deserialize)]
struct CallbackParams {
    code: String,
    state: String,
}

/// `GET /auth/callback?code=&state=`: claims the state entry (Pending →
/// Claiming; replays reject 400), runs the provider exchange, and on success
/// sets the session cookie and returns the popup-closing HTML. The terminal
/// outcome (`Completed` | `Failed`) is set before returning so the polling
/// parent sees the result — the token itself is delivered via the HttpOnly
/// cookie AND via the one-shot `/auth/state` poll.
async fn provider_callback<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<CallbackParams>,
) -> Response {
    // Login-CSRF gate: the browser that hits the callback must be the one that hit
    // begin (it carries the matching nonce cookie). Checked before the entry is
    // claimed, so a rejected callback leaves the state Pending and a legit retry
    // still works. Constant-time compare for hygiene.
    if state.config.oauth_login_csrf {
        let ok = crate::auth::cookie::oauth_csrf_cookie(&headers)
            .is_some_and(|c| bool::from(c.as_bytes().ct_eq(params.state.as_bytes())));
        if !ok {
            return RtDbError::bad_request("login CSRF check failed").into_response();
        }
    }

    if !claim_pending(&state, &params.state).await {
        return RtDbError::bad_request("invalid or expired state").into_response();
    }

    let Some(provider) = P::from_config(&state.config) else {
        set_outcome(&state, &params.state, LoginOutcome::Failed).await;
        return unconfigured_response(P::name());
    };

    let secure = crate::auth::cookie::request_is_secure(&headers);
    match provider.complete_login(&state, &params.code).await {
        Ok(token) => {
            set_outcome(
                &state,
                &params.state,
                LoginOutcome::Completed(token.clone()),
            )
            .await;
            callback_close_response(&token, secure)
        }
        Err(err) => {
            set_outcome(&state, &params.state, LoginOutcome::Failed).await;
            err.into_response()
        }
    }
}

/// `POST /auth/apple/callback`: Apple's `response_mode=form_post` variant of
/// `provider_callback`. Apple POSTs `code` + `state` (URL-encoded form body) to
/// the redirect URI instead of appending them as query params, so the GET
/// generic can't serve it. The flow is otherwise identical: the login-CSRF
/// nonce cookie (SameSite=None, so it survives Apple's cross-site POST) is
/// constant-time-checked, the state entry is claimed single-use, and on success
/// the popup-closing HTML + HttpOnly session cookie are returned exactly as the
/// GET path returns them.
async fn apple_callback(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Form(form): Form<AppleCallbackForm>,
) -> Response {
    if state.config.oauth_login_csrf {
        let ok = crate::auth::cookie::oauth_csrf_cookie(&headers)
            .is_some_and(|c| bool::from(c.as_bytes().ct_eq(form.state.as_bytes())));
        if !ok {
            return RtDbError::bad_request("login CSRF check failed").into_response();
        }
    }

    if !claim_pending(&state, &form.state).await {
        return RtDbError::bad_request("invalid or expired state").into_response();
    }

    let Some(provider) = AppleProvider::from_config(&state.config) else {
        set_outcome(&state, &form.state, LoginOutcome::Failed).await;
        return unconfigured_response(AppleProvider::name());
    };

    let secure = crate::auth::cookie::request_is_secure(&headers);
    match provider.complete_login(&state, &form.code).await {
        Ok(token) => {
            set_outcome(&state, &form.state, LoginOutcome::Completed(token.clone())).await;
            callback_close_response(&token, secure)
        }
        Err(err) => {
            set_outcome(&state, &form.state, LoginOutcome::Failed).await;
            err.into_response()
        }
    }
}

/// Apple POSTs the authorization code + state as a URL-encoded form body
/// (`response_mode=form_post`).
#[derive(Deserialize)]
struct AppleCallbackForm {
    code: String,
    state: String,
}

/// The popup-closing HTML the callback returns on success. Nothing is
/// interpolated (no `origin`, no token) — the token rides the HttpOnly
/// `Set-Cookie`, so there is no self-XSS surface (SEC-005 fully retired by
/// SEC-012) and the parent learns of completion by polling `/auth/state`, not
/// via `window.opener` postMessage.
fn callback_close_response(token: &str, secure: bool) -> Response {
    let html = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Signed in</title>\
                </head><body><script>window.close();</script>\
                <p style=\"font-family:sans-serif\">Sign-in complete. You may close this window.</p>\
                </body></html>";
    let mut response = Html(html).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; script-src 'unsafe-inline'"),
    );
    match crate::auth::cookie::set_session_cookie(token, secure) {
        Ok(cookie) => {
            response.headers_mut().append(SET_COOKIE, cookie);
            response
                .headers_mut()
                .append(SET_COOKIE, crate::auth::cookie::clear_oauth_csrf_cookie());
            response
        }
        Err(err) => err.into_response(),
    }
}

#[derive(Deserialize)]
struct StateQuery {
    state: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum StateResponse {
    Pending,
    Complete { token: String, user: AuthedUser },
    Expired,
    Error,
}

/// `GET /auth/state?state=`: provider-agnostic polling endpoint. The `state`
/// token (minted at `/auth/{provider}/begin`) is the capability — no cookie
/// required, so this works cross-origin (the SDK on a different origin) where
/// the `SameSite=Lax` session cookie would not be sent. Returns
/// `pending | complete | expired | error`.
async fn auth_state(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<StateQuery>,
) -> Response {
    match poll_login(&state, &params.state).await {
        PollResult::Pending => Json(StateResponse::Pending).into_response(),
        PollResult::Complete { token, user } => {
            Json(StateResponse::Complete { token, user }).into_response()
        }
        PollResult::Failed => Json(StateResponse::Error).into_response(),
        PollResult::Expired => Json(StateResponse::Expired).into_response(),
    }
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
    let mut resp = Json(OkResponse { ok: true }).into_response();
    // SEC-001 phase 2: clear the HttpOnly session cookie alongside the
    // server-side session row.
    resp.headers_mut()
        .insert(SET_COOKIE, crate::auth::cookie::clear_session_cookie());
    resp
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

/// `POST /auth/anonymous`: mints an ephemeral anonymous user + session for a
/// credential-less guest. The HttpOnly session cookie is set (browser path) AND
/// the plaintext session token is returned in the body (SDK/bearer path — an
/// SDK passes it as the WS/HTTP bearer, exactly like a machine token). Gated by
/// `RTDB_AUTH_ANONYMOUS_ENABLED` (default off): disabled ⇒ 403 FORBIDDEN. An
/// anonymous user is authorized for any database via that boot gate (no
/// allowlist entry) and owns its own documents via per-row `ownerField` (the
/// anon `user_id`). The anon→real merge on a later OAuth sign-in is a follow-up.
#[derive(Serialize)]
struct AnonymousResponse {
    user: AuthedUser,
    token: String,
}

async fn anonymous(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if !state.config.auth_anonymous_enabled {
        return RtDbError::forbidden("anonymous authentication is disabled").into_response();
    }
    let user_id = crate::db::new_id();
    let now = now_ms();
    // `login` is NOT NULL (store the literal "anonymous"); `email` is UNIQUE but
    // nullable (NULL for an anon user); `anonymous = TRUE` marks the row.
    let insert = sqlx::query(
        "INSERT INTO rtdb_auth.users (id, login, email, anonymous, created_at) \
         VALUES ($1, $2, NULL, TRUE, $3)",
    )
    .bind(&user_id)
    .bind("anonymous")
    .bind(now)
    .execute(&state.pool)
    .await;
    if let Err(err) = insert {
        tracing::error!(error = %err, "anonymous user insert failed");
        return RtDbError::internal("failed to create anonymous user").into_response();
    }
    let ttl_days = state.runtime.hot.load().session_ttl_days;
    let token = match session::create_session(&state.pool, &user_id, ttl_days).await {
        Ok(t) => t,
        Err(err) => return err.into_response(),
    };
    let expires_at = now + ttl_days * 86_400_000;
    let principal = Principal::User {
        user_id,
        email: None,
        name: None,
        expires_at,
        anonymous: true,
        github_id: None,
        github_login: None,
    };
    let secure = crate::auth::cookie::request_is_secure(&headers);
    let mut response = Json(AnonymousResponse {
        user: authed_user(&principal),
        token: token.clone(),
    })
    .into_response();
    if let Ok(cookie) = crate::auth::cookie::set_session_cookie(&token, secure) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    response
}

/// OAuth + session routes. SEC-012: each provider mounts a `begin` endpoint
/// that returns `{authorizeUrl, state}` (the parent opens it in a `noopener`
/// popup and polls `/auth/state`); the callback handler is generic over the
/// provider, mounted per-provider — GitHub keeps `/auth/callback`, Google and
/// GitLab use `/<provider>/callback`. `/auth/state`, `/auth/logout`, and
/// `/auth/me` are provider-agnostic.
pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/auth/github/begin", get(provider_begin::<GithubProvider>))
        .route("/auth/callback", get(provider_callback::<GithubProvider>))
        .route("/auth/google/begin", get(provider_begin::<GoogleProvider>))
        .route(
            "/auth/google/callback",
            get(provider_callback::<GoogleProvider>),
        )
        .route("/auth/gitlab/begin", get(provider_begin::<GitlabProvider>))
        .route(
            "/auth/gitlab/callback",
            get(provider_callback::<GitlabProvider>),
        )
        .route("/auth/oidc/begin", get(provider_begin::<OidcProvider>))
        .route(
            "/auth/oidc/callback",
            get(provider_callback::<OidcProvider>),
        )
        .route(
            "/auth/microsoft/begin",
            get(provider_begin::<MicrosoftProvider>),
        )
        .route(
            "/auth/microsoft/callback",
            get(provider_callback::<MicrosoftProvider>),
        )
        .route("/auth/apple/begin", get(provider_begin::<AppleProvider>))
        .route("/auth/apple/callback", post(apple_callback))
        .route("/auth/anonymous", post(anonymous))
        .route("/auth/state", get(auth_state))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/validate", get(validate))
}
