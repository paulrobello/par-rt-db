//! OAuth callback routes: the generic `GET /auth/callback` /
//! `GET /auth/{provider}/callback` handler and the shared popup-closing
//! response builders. Part of the ARC-003 provider-module split — handler
//! bodies are moved verbatim from the former `auth/provider.rs` monolith.
//! Apple's `form_post` variant lives in `auth/apple.rs` and reaches the
//! shared pieces through this module.

use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, header::SET_COOKIE};
use axum::response::{Html, IntoResponse, Response};
use serde::Deserialize;
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::error::RtDbError;

use super::{
    DELIVERY_BEARER, OAuthProvider, claim_pending, merge_anon_into_real, set_outcome,
    stamp_session_capability, unconfigured_response,
};

#[derive(Deserialize)]
pub(super) struct CallbackParams {
    code: String,
    state: String,
}

/// `GET /auth/callback?code=&state=`: claims the state entry (Pending →
/// Claiming; replays reject 400), runs the provider exchange, and on success
/// sets the session cookie and returns the popup-closing HTML. The terminal
/// outcome (`Completed` | `Failed`) is set before returning so the polling
/// parent sees the result — the token itself is delivered via the HttpOnly
/// cookie AND via the one-shot `/auth/state` poll.
pub(super) async fn provider_callback<P: OAuthProvider>(
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

    let (anon_user_id, delivery, revoke_verifier) =
        match claim_pending(&state, &params.state, P::name()).await {
            Some((anon, delivery, verifier)) => (anon, delivery, verifier),
            None => return RtDbError::bad_request("invalid or expired state").into_response(),
        };

    let Some(provider) = P::from_config(&state.config) else {
        set_outcome(&state, &params.state, None).await;
        return unconfigured_response(P::name());
    };

    let secure = state.config.cookie_secure
        || crate::auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    match provider.complete_login(&state, &params.code).await {
        Ok(token) => {
            // FM-27: merge the anon footprint into the real account before the
            // terminal outcome is recorded (see merge_anon_into_real).
            if let Some(anon_id) = &anon_user_id {
                merge_anon_into_real(&state, anon_id, &token).await;
            }
            // Bearer deliveries stamp the durable session-revoke capability
            // BEFORE the terminal outcome — the one-shot poll delivers
            // `sessionId`, so it must exist by poll time. Failure is non-fatal
            // (the session works; it is just not capability-revocable, the
            // same posture as every pre-existing session).
            if let Some(verifier) = revoke_verifier.as_deref() {
                stamp_session_capability(&state, &token, verifier).await;
            }
            set_outcome(&state, &params.state, Some(&token)).await;
            if delivery == DELIVERY_BEARER {
                bearer_callback_close_response()
            } else {
                callback_close_response(&token, secure)
            }
        }
        Err(err) => {
            set_outcome(&state, &params.state, None).await;
            err.into_response()
        }
    }
}

/// The popup-closing HTML shared by every delivery mode. Nothing is
/// interpolated (no `origin`, no token) — SEC-005 stays retired: the token
/// never appears in the body regardless of how the response carries it.
/// SEC-003: the close responses' CSP allows ONLY this exact script, via a
/// sha256 hash source — changing the script means recomputing the hash in
/// both close-response builders below.
fn callback_close_html() -> &'static str {
    "<!doctype html><html><head><meta charset=\"utf-8\"><title>Signed in</title>\
     </head><body><script>window.close();</script>\
     <p style=\"font-family:sans-serif\">Sign-in complete. You may close this window.</p>\
     </body></html>"
}

/// Close response for the historical default and SEC-207 cookie deliveries:
/// identical popup HTML + CSP, plus the HttpOnly `rtdb_session` cookie, the
/// SEC-106 admin-CSRF nonce, and the login-CSRF nonce clear.
pub(crate) fn callback_close_response(token: &str, secure: bool) -> Response {
    let mut response = Html(callback_close_html()).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'none'; script-src 'sha256-Wn5wULmWNAS84lRO4JMQ9jE/kYWz1ADWjWy4z2jjosA='",
        ),
    );
    match crate::auth::cookie::set_session_cookie(token, secure) {
        Ok(cookie) => {
            response.headers_mut().append(SET_COOKIE, cookie);
            // SEC-106: mint the readable admin-CSRF nonce alongside the session
            // cookie. The OAuth user may or may not be a dashboard admin; if
            // they are, the nonce is already in place. Independent random value
            // — leaking one does not leak the other.
            let csrf_token = crate::db::random_token();
            if let Ok(csrf) = crate::auth::cookie::set_admin_csrf_cookie(&csrf_token, secure) {
                response.headers_mut().append(SET_COOKIE, csrf);
            }
            response
                .headers_mut()
                .append(SET_COOKIE, crate::auth::cookie::clear_oauth_csrf_cookie());
            response
        }
        Err(err) => err.into_response(),
    }
}

/// Close response for the opt-in bearer delivery (`mode=bearer`): the same
/// popup-closing HTML + CSP, but NO `rtdb_session` cookie and NO admin-CSRF
/// nonce — the one-shot `/auth/state` poll is the only credential carrier, so
/// other same-origin tabs cannot adopt the session via the shared cookie. The
/// login-CSRF nonce is still cleared (same one-shot hygiene as the default
/// path).
pub(crate) fn bearer_callback_close_response() -> Response {
    let mut response = Html(callback_close_html()).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static(
            "default-src 'none'; script-src 'sha256-Wn5wULmWNAS84lRO4JMQ9jE/kYWz1ADWjWy4z2jjosA='",
        ),
    );
    response
        .headers_mut()
        .append(SET_COOKIE, crate::auth::cookie::clear_oauth_csrf_cookie());
    response
}
