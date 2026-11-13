//! Login-begin routes: `GET /auth/{provider}/begin` (state mint + authorize
//! URL) and the CLI bootstrap page `GET /auth/login`. Part of the ARC-003
//! provider-module split — the handler bodies are moved verbatim from the
//! former `auth/provider.rs` monolith.

use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Query as QueryParams, State};
use axum::http::{HeaderMap, header::SET_COOKIE};
use axum::response::{IntoResponse, Redirect, Response};
use serde::Deserialize;

use crate::AppState;
use crate::auth::apple::AppleProvider;
use crate::auth::github::GithubProvider;
use crate::auth::gitlab::GitlabProvider;
use crate::auth::google::GoogleProvider;
use crate::auth::microsoft::MicrosoftProvider;
use crate::auth::oidc::OidcProvider;
use crate::db::{now_ms, random_token, sha256_hex};
use crate::error::RtDbError;

use super::{
    DELIVERY_BEARER, DELIVERY_COOKIE, DELIVERY_DEFAULT, MAX_PENDING_STATES, OAuthProvider,
    STATE_TTL_MS, STATUS_CLAIMING, STATUS_PENDING, bearer_token, sweep_oauth_states,
    unconfigured_response,
};

#[derive(Deserialize)]
pub(super) struct BeginParams {
    origin: String,
    /// Credential delivery for this login: absent (historical default — token
    /// via poll AND cookie), `cookie` (SEC-207 — cookie only), or `bearer`
    /// (opt-in — poll only, no cookie is ever set). Any other value rejects
    /// with `400`.
    mode: Option<String>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct BeginResponse {
    authorize_url: String,
    /// CLI bootstrap entry (`GET /auth/login?state=…`), minted ONLY for
    /// bearer-delivery logins: the URL a human opens in their browser when the
    /// login was initiated by a non-browser client (a CLI). Serving it mints
    /// the login-CSRF + poll cookies in THAT browser and redirects to the
    /// provider authorize URL — re-establishing the begin-browser binding the
    /// login-CSRF callback gate requires, without the initiating client ever
    /// needing a cookie jar. Absent for default/cookie deliveries (the
    /// browser that called begin is the authorizing browser there), keeping
    /// those responses byte-identical.
    #[serde(skip_serializing_if = "Option::is_none")]
    login_url: Option<String>,
    state: String,
    /// Client-only revoke capability for bearer-mode logins: minted at begin,
    /// delivered ONLY in this response body — never in a URL, cookie,
    /// provider redirect, or proxy log. Only its sha256 verifier is stored
    /// server-side. Omitted (field absent) for default/cookie deliveries,
    /// keeping those responses byte-identical. If this response is lost the
    /// flow was never navigated and the row simply ages out pending until the
    /// TTL sweep — correct, nothing to clean.
    #[serde(rename = "revokeSecret", skip_serializing_if = "Option::is_none")]
    revoke_secret: Option<String>,
}

/// `GET /auth/{provider}/begin?origin=<parent origin>`: validates the origin
/// against the live allowlist, mints a single-use state token, and returns the
/// provider authorize URL + the state. The parent opens the authorize URL in a
/// `noopener` popup and polls `/auth/state`. SEC-012 replaces the prior
/// `window.opener` postMessage relay.
///
/// `origin` is a caller-supplied query parameter, NOT the browser-verified
/// `Origin` header — so it only proves the caller knows an allowlisted value,
/// not who they are. It is used solely to pick the post-login redirect target
/// and is discarded (never interpolated), which retired the SEC-005 self-XSS
/// surface; it is not an authentication of the caller.
pub(super) async fn provider_begin<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    QueryParams(params): QueryParams<BeginParams>,
) -> Response {
    let Some(provider) = P::from_config(&state.config) else {
        return unconfigured_response(P::name());
    };

    // SEC-003: per-IP rate limit on the unauthenticated `/begin` route, so a
    // single attacker cannot mint state rows in a loop (the global
    // MAX_PENDING_STATES cap bounds the table; this bounds one IP). Same
    // canonicalization and limiter as the anonymous mint (SEC-103). Disabled
    // when `limits.oauth_begin_per_ip_rpm == 0` (code default; the shipped
    // `.env.example`/`docker-compose.yml` set a non-zero default).
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    let limit = state.config.limits.oauth_begin_per_ip_rpm;
    if limit > 0 {
        match state
            .limits
            .rate_limiter
            .check(
                crate::rate_limit::RateKey::Ip {
                    route: "oauth_begin",
                    ip: ip_key.clone(),
                },
                limit,
            )
            .await
        {
            crate::rate_limit::RateDecision::Denied { retry_after_secs } => {
                return RtDbError::rate_limited(retry_after_secs).into_response();
            }
            crate::rate_limit::RateDecision::Allowed => {}
        }
    }

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

    // SEC-207: a declared value, not a free-form flag — an unrecognized mode
    // is a client bug and rejects rather than silently delivering the token.
    let delivery = match params.mode.as_deref() {
        None => DELIVERY_DEFAULT,
        Some("cookie") => DELIVERY_COOKIE,
        Some("bearer") => DELIVERY_BEARER,
        Some(_) => {
            return RtDbError::bad_request("mode must be 'cookie' or 'bearer'").into_response();
        }
    };
    let cookie_mode = delivery == DELIVERY_COOKIE;

    let state_token = random_token();
    let now = now_ms();
    // Opportunistic TTL prune (cheap DELETE; the background sweep is the
    // primary pruner). Keeps the pending-row cap below honest.
    let _ = sweep_oauth_states(&state.pool).await;
    // SEC-132: bound the pending-states table so an attacker spamming
    // `/begin` cannot grow it unbounded. Count live `pending`/`claiming` rows
    // and reject the new mint at the cap.
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM rtdb_auth.oauth_states \
         WHERE status IN ($1, $2) AND expires_at > $3",
    )
    .bind(STATUS_PENDING)
    .bind(STATUS_CLAIMING)
    .bind(now)
    .fetch_one(&state.pool)
    .await
    .unwrap_or(0);
    if pending >= MAX_PENDING_STATES {
        return RtDbError::internal("too many pending login states; retry").into_response();
    }
    // FM-27: if the caller holds an anonymous session, record its user id so the
    // callback can merge the anon footprint into the real account after login.
    // Server-side resolution of a verified session — never caller-supplied.
    let anon_user_id = match bearer_token(&headers).map(|t| t.to_string()) {
        Some(token) => match crate::auth::resolve_bearer(&state.pool, &token).await {
            Ok(crate::auth::Principal::User {
                anonymous: true,
                user_id,
                ..
            }) => Some(user_id),
            Ok(_) => None,
            Err(err) => {
                // Fail-open on purpose (a transient DB hiccup must not block
                // login), but the merge binding is lost — make that visible.
                tracing::warn!(error = %err, "oauth: failed to resolve anon binding at /begin");
                None
            }
        },
        None => None,
    };
    // Client-only revoke capability, minted only for bearer rows (the state
    // token transits the provider redirect URL; this secret never does). Only
    // the sha256 verifier is persisted — the plaintext rides the begin
    // response exactly once.
    let revoke_secret = (delivery == DELIVERY_BEARER).then(random_token);
    let revoke_secret_verifier = revoke_secret.as_deref().map(sha256_hex);
    if let Err(err) = sqlx::query(
        "INSERT INTO rtdb_auth.oauth_states \
         (state, provider, status, created_at, expires_at, anon_user_id, cookie_mode, delivery, revoke_secret_verifier) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(&state_token)
    .bind(P::name())
    .bind(STATUS_PENDING)
    .bind(now)
    .bind(now + STATE_TTL_MS)
    .bind(&anon_user_id)
    .bind(cookie_mode)
    .bind(delivery)
    .bind(&revoke_secret_verifier)
    .execute(&state.pool)
    .await
    {
        tracing::warn!(error = %err, "oauth: failed to insert pending state");
        return RtDbError::internal("failed to begin login").into_response();
    }

    let redirect_uri = format!("{}{}", state.config.public_url, provider.callback_path());
    let authorize_url = provider.authorize_url(&redirect_uri, &state_token);
    let login_url = (delivery == DELIVERY_BEARER)
        .then(|| format!("{}/auth/login?state={state_token}", state.config.public_url));
    let mut response = Json(BeginResponse {
        authorize_url,
        login_url,
        state: state_token.clone(),
        revoke_secret: revoke_secret.clone(),
    })
    .into_response();
    // Login-CSRF: bind this `state` to the initiating browser so a callback the
    // attacker induced the victim to load (the attacker's own exchange) carries no
    // matching cookie and is rejected at `provider_callback`. SameSite=None so it
    // survives the provider → callback cross-site redirect.
    let secure = state.config.cookie_secure
        || crate::auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    if let Ok(csrf) = crate::auth::cookie::set_oauth_csrf_cookie(&state_token, secure) {
        response.headers_mut().append(SET_COOKIE, csrf);
    }
    // SEC-121: bind the `/auth/state` poll to the SAME browser. The `state` token
    // alone transit URLs, edge logs, and the server's TraceLayer — a leaked URL
    // without this cookie cannot poll for the resulting session token. Unlike the
    // CSRF cookie (cleared at /callback for one-shot hygiene), this one survives
    // the callback so the post-callback poll — which is how the parent receives
    // the token — still carries it. Same value, so an attacker needs the cookie,
    // not just the URL.
    if let Ok(state_cookie) = crate::auth::cookie::set_oauth_state_cookie(&state_token, secure) {
        response.headers_mut().append(SET_COOKIE, state_cookie);
    }
    response
}

#[derive(Deserialize)]
pub(super) struct LoginBootstrapParams {
    state: String,
}

/// `GET /auth/login?state=`: the CLI login bootstrap page. A non-browser
/// client (a CLI) initiates the login at `/auth/{provider}/begin?mode=bearer`
/// and receives `{authorizeUrl, loginUrl, state, revokeSecret}` — but the
/// begin-minted cookies land on the CALLER's response, so the browser that
/// eventually authorizes carries none of them and the login-CSRF callback
/// gate rejects the flow structurally (every time, with
/// `oauth_login_csrf=true`). This route closes that gap: the human opens
/// `loginUrl` — never `authorizeUrl` — in their browser; serving it mints the
/// two cookies in THAT browser and 303-redirects the navigation to the
/// provider authorize URL, so the begin-browser == callback-browser binding
/// the login-CSRF gate enforces holds again. The initiating CLI polls
/// `/auth/state` presenting the revoke secret as the
/// `x-rtdb-oauth-revoke-secret` header (see `auth_state`).
///
/// Binding semantics (documented, accepted risk): whoever learns `state` can
/// serve this page and thus bind the next authorizing browser to that login —
/// the same capability class as the `authorizeUrl` itself, and the same
/// phishing shape RFC 8628 device codes have; bounded by `state` being a
/// 256-bit secret with a 10-minute TTL that transits exactly the URLs
/// `authorize_url` already transits. The reverse attack — inducing a victim's
/// browser to complete the ATTACKER's login, the thing login-CSRF exists to
/// stop — still fails: the victim browser's csrf cookie matches only the
/// victim's own pending state, never an attacker-induced callback state.
///
/// Serves only `pending`, unexpired rows; anything else (unknown, claimed,
/// completed, failed, expired) answers a plain 400 envelope, the same shape
/// the callback's errors use. The redirect target is re-derived from the row's
/// `provider` column, so a flow begun on one replica bootstraps on any
/// replica.
pub(super) async fn login_bootstrap(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<LoginBootstrapParams>,
) -> Response {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT provider FROM rtdb_auth.oauth_states \
         WHERE state = $1 AND status = $2 AND expires_at > $3",
    )
    .bind(&params.state)
    .bind(STATUS_PENDING)
    .bind(now_ms())
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();
    let Some((provider_name,)) = row else {
        return RtDbError::bad_request("invalid or expired login state").into_response();
    };

    let authorize_url = match provider_name.as_str() {
        "github" => GithubProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        "google" => GoogleProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        "gitlab" => GitlabProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        "microsoft" => MicrosoftProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        "apple" => AppleProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        "oidc" => OidcProvider::from_config(&state.config).map(|p| {
            p.authorize_url(
                &format!("{}{}", state.config.public_url, p.callback_path()),
                &params.state,
            )
        }),
        _ => None,
    };
    let Some(authorize_url) = authorize_url else {
        return RtDbError::bad_request("provider not configured").into_response();
    };

    let secure = state.config.cookie_secure
        || crate::auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    let mut response = Redirect::to(&authorize_url).into_response();
    if let Ok(csrf) = crate::auth::cookie::set_oauth_csrf_cookie(&params.state, secure) {
        response.headers_mut().append(SET_COOKIE, csrf);
    }
    if let Ok(state_cookie) = crate::auth::cookie::set_oauth_state_cookie(&params.state, secure) {
        response.headers_mut().append(SET_COOKIE, state_cookie);
    }
    response
}
