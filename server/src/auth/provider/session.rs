//! Session-credential routes: `POST /auth/session/revoke` (the durable
//! bearer-flow session revoke), `POST /auth/logout`, `GET /auth/me`,
//! `GET /auth/validate`, and `POST /auth/anonymous`. Part of the ARC-003
//! provider-module split — handler bodies are moved verbatim from the former
//! `auth/provider.rs` monolith.

use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::auth::{Principal, authed_user, resolve_bearer, session};
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

use super::bearer_token;

/// Body of `POST /auth/session/revoke`. `sessionId` is the non-secret
/// identifier the one-shot poll delivered alongside the token; `revokeSecret`
/// is the begin response's client-only secret — the same preimage whose
/// sha256 verifier secures `POST /auth/state/revoke`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionRevokeBody {
    session_id: String,
    revoke_secret: Option<String>,
}

/// Response of `POST /auth/session/revoke`. `revoked` is a CONFIRMED fence:
/// the session matching the (sessionId, revokeSecret) capability is verifiably
/// dead — its `revoked_at` marker was set (or was already a tombstone, or the
/// row was past expiry and got pruned in the same transaction) — in the
/// transaction that proved the capability. `unknown` means no row matched:
/// never-stamped session (cookie/default deliveries, pre-upgrade rows), wrong
/// or missing secret, an identifier that never existed, or a row already
/// removed by background cleanup (the 60s tombstone sweep or the lazy expiry
/// delete — so a retry after a confirmed revoke CAN turn `unknown` once the
/// tombstone is swept past TTL; safe, since expiry was already enforced by
/// every resolver, but not evidentiary) — indistinguishable by design (no
/// existence leak). There is no `unconfirmed` here: unlike the state flow
/// there is no ambiguous-insert window. The-cube client rule carries over:
/// any non-`revoked` outcome is treated as unconfirmed and retried.
#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum SessionRevokeResponse {
    Revoked,
    Unknown,
}

/// Per-IP budget for `/auth/session/revoke` — same limiter, key shape, and
/// sizing rationale as `STATE_REVOKE_PER_IP_RPM` (30/min covers legitimate
/// retries many times over while capping a garbage-capability flood).
const SESSION_REVOKE_PER_IP_RPM: u32 = 30;
/// Total-body bound: same 4 KiB rationale as the state-revoke gate.
const SESSION_REVOKE_MAX_BODY_BYTES: usize = 4096;
/// Per-field bound (`sessionId`, `revokeSecret`): 43-char random tokens.
const SESSION_REVOKE_MAX_FIELD_CHARS: usize = 256;

/// `POST /auth/session/revoke` `{sessionId, revokeSecret}`: durably revokes
/// ONE OAuth session for its actual lifetime — the capability the 10-minute
/// state window cannot provide. The client stores the poll response's
/// `sessionId` next to the begin response's `revokeSecret` and needs neither
/// the bearer nor any admin credential to sign the session out later.
///
/// Mechanics mirror `state_revoke`: unauthenticated (capability = the pair),
/// per-IP fixed-window guard, strict body gate before any transaction,
/// constant-time digest compare under a `FOR UPDATE` row lock, one atomic
/// transaction. Revocation sets `revoked_at` — the marker every session
/// resolver excludes, so the session dies everywhere immediately — while the
/// row stays as a tombstone carrying the capability, so a retry after a lost
/// response or a concurrent duplicate answers `revoked` (idempotent, no
/// ambiguity). An already-expired row is pruned in the same transaction; the
/// tombstone itself is bounded by the session TTL and swept on the background
/// loop. This endpoint sets no cookies and never touches a browser's
/// pre-existing `rtdb_session`.
pub(super) async fn session_revoke(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    match state
        .limits
        .rate_limiter
        .check(
            crate::rate_limit::RateKey::Ip {
                route: "session_revoke",
                ip: ip_key,
            },
            SESSION_REVOKE_PER_IP_RPM,
        )
        .await
    {
        crate::rate_limit::RateDecision::Allowed => {}
        crate::rate_limit::RateDecision::Denied { .. } => {
            return Json(SessionRevokeResponse::Unknown).into_response();
        }
    }

    let bytes = match axum::body::to_bytes(req.into_body(), SESSION_REVOKE_MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return Json(SessionRevokeResponse::Unknown).into_response(),
    };
    let body: SessionRevokeBody = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        Err(_) => return Json(SessionRevokeResponse::Unknown).into_response(),
    };
    if body.session_id.len() > SESSION_REVOKE_MAX_FIELD_CHARS
        || body
            .revoke_secret
            .as_deref()
            .is_some_and(|s| s.len() > SESSION_REVOKE_MAX_FIELD_CHARS)
    {
        return Json(SessionRevokeResponse::Unknown).into_response();
    }

    let now = now_ms();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            tracing::warn!(error = %err, "oauth: session revoke tx begin failed");
            return RtDbError::internal("failed to revoke session").into_response();
        }
    };
    // Lock the capability row first, decide in Rust (constant-time compare is
    // not expressible in SQL), mutate under the same lock.
    type CapabilityRow = (Option<i64>, Option<String>, i64);
    let row: Result<Option<CapabilityRow>, sqlx::Error> = sqlx::query_as(
        "SELECT revoked_at, revoke_secret_hash, expires_at \
         FROM rtdb_auth.sessions \
         WHERE session_id = $1 \
         FOR UPDATE",
    )
    .bind(&body.session_id)
    .fetch_optional(&mut *tx)
    .await;
    let row = match row {
        Ok(r) => r,
        Err(err) => {
            tracing::warn!(error = %err, "oauth: session revoke query failed");
            return RtDbError::internal("failed to revoke session").into_response();
        }
    };
    let Some((revoked_at, stored_hash, expires_at)) = row else {
        return Json(SessionRevokeResponse::Unknown).into_response();
    };

    let secret_ok = body
        .revoke_secret
        .as_deref()
        .zip(stored_hash.as_deref())
        .is_some_and(|(presented, stored)| {
            bool::from(
                crate::db::sha256_hex(presented)
                    .as_bytes()
                    .ct_eq(stored.as_bytes()),
            )
        });
    if !secret_ok {
        return Json(SessionRevokeResponse::Unknown).into_response();
    }

    if expires_at <= now {
        // Already expired: prove the capability, prune the row, answer
        // `revoked` — the caller's goal (the session must not authenticate
        // anyone) already holds and stays held.
        if let Err(err) = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE session_id = $1")
            .bind(&body.session_id)
            .execute(&mut *tx)
            .await
        {
            tracing::warn!(error = %err, "oauth: session revoke expired-row prune failed");
            return RtDbError::internal("failed to revoke session").into_response();
        }
    } else if revoked_at.is_none() {
        // Live session: set the revoked marker that every session resolver
        // excludes, in the same transaction that proved the capability — the
        // fence. The dead token_hash stays (it is the PRIMARY KEY and just a
        // digest). A crash or failure rolls the fence back, leaving the row
        // live so a retry re-runs the whole cleanup.
        if let Err(err) =
            sqlx::query("UPDATE rtdb_auth.sessions SET revoked_at = $2 WHERE session_id = $1")
                .bind(&body.session_id)
                .bind(now)
                .execute(&mut *tx)
                .await
        {
            tracing::warn!(error = %err, "oauth: session revoke fence failed");
            return RtDbError::internal("failed to revoke session").into_response();
        }
    }
    // else: already a tombstone (revoked_at set, unexpired) — the previous
    // revoke committed; re-answer `revoked` as the retryable confirmation.
    match tx.commit().await {
        Ok(()) => Json(SessionRevokeResponse::Revoked).into_response(),
        Err(err) => {
            tracing::warn!(error = %err, "oauth: session revoke commit failed");
            RtDbError::internal("failed to revoke session").into_response()
        }
    }
}

#[derive(serde::Serialize)]
struct OkResponse {
    ok: bool,
}

/// `POST /auth/logout`: idempotent regardless of whether the bearer token is
/// present, valid, or already expired — a `DELETE` matching zero rows is not
/// an error, so only a genuine query failure (not a merely-absent session)
/// produces a 500 here. The cookie clears are attached only when the request
/// presented `rtdb_session`: a pure-bearer logout from a browser context would
/// otherwise wipe a different tab's cookie-backed session browser-wide.
pub(super) async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let presented_cookie = crate::auth::cookie::session_cookie(&headers).is_some();
    if let Some(token) = bearer_token(&headers)
        && let Err(err) = session::delete_session(&state.pool, token).await
    {
        return err.into_response();
    }
    let mut resp = Json(OkResponse { ok: true }).into_response();
    if presented_cookie {
        // SEC-001 phase 2: clear the HttpOnly session cookie alongside the
        // server-side session row. SEC-106: clear the CSRF nonce with it.
        resp.headers_mut()
            .append(SET_COOKIE, crate::auth::cookie::clear_session_cookie());
        resp.headers_mut()
            .append(SET_COOKIE, crate::auth::cookie::clear_admin_csrf_cookie());
    }
    resp
}

#[derive(serde::Serialize)]
struct MeResponse {
    user: AuthedUser,
}

/// `GET /auth/me`: session-only. A machine token resolves fine via
/// `resolve_bearer` but is rejected here (401) since it isn't a user.
pub(super) async fn me(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
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
pub(super) async fn validate(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(token) = bearer_token(&headers) else {
        return RtDbError::unauthorized("missing bearer token").into_response();
    };

    match resolve_bearer(&state.pool, token).await {
        Ok(principal) => {
            if let Err(error) = crate::auth::host_policy::non_document(&principal.row_ctx()) {
                return error.into_response();
            }
            Json(MeResponse {
                user: authed_user(&principal),
            })
            .into_response()
        }
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
/// anon `user_id`). On a later OAuth sign-in with this session's bearer
/// presented at `/begin`, the anon footprint is merged into the real account
/// (`merge::merge_users`).
#[derive(Serialize)]
struct AnonymousResponse {
    user: AuthedUser,
    token: String,
}

pub(super) async fn anonymous(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
) -> Response {
    if !state.config.auth_anonymous_enabled {
        return RtDbError::forbidden("anonymous authentication is disabled").into_response();
    }
    // SEC-103: per-IP rate limit on the unauthenticated mint route. Without
    // it, an attacker can mint unbounded anonymous users/sessions by hitting
    // this endpoint in a loop. The IP key is canonicalized by `client_ip_key`
    // (CF-Connecting-IP preferred, rightmost XFF fallback, then the connection
    // peer) — the same canonicalization the public storage route uses. Disabled
    // when `limits.anonymous_per_ip_rpm == 0` (code default; the shipped
    // `.env.example`/`docker-compose.yml` set a non-zero default).
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    let limit = state.config.limits.anonymous_per_ip_rpm;
    if limit > 0 {
        match state
            .limits
            .rate_limiter
            .check(
                crate::rate_limit::RateKey::Ip {
                    route: "anon_mint",
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
    // SEC-103: short independent TTL for anonymous sessions (default 1 day)
    // rather than the standard `session_ttl_days` (30), so the ephemeral rows
    // minted by this unauthenticated route expire quickly.
    let ttl_days = state.config.anonymous_session_ttl_days;
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
        session_hash: None,
    };
    let secure = state.config.cookie_secure
        || crate::auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    let mut response = Json(AnonymousResponse {
        user: authed_user(&principal),
        token: token.clone(),
    })
    .into_response();
    if let Ok(cookie) = crate::auth::cookie::set_session_cookie(&token, secure) {
        response.headers_mut().append(SET_COOKIE, cookie);
    }
    // SEC-106: anonymous sessions cannot be dashboard admins (no email, not on
    // `rtdb_auth.admins`), but mint the CSRF nonce for symmetry with the OAuth
    // path — the dashboard JS reads it unconditionally, and the cookie is
    // inert if the bearer is the credential.
    let csrf_token = crate::db::random_token();
    if let Ok(csrf) = crate::auth::cookie::set_admin_csrf_cookie(&csrf_token, secure) {
        response.headers_mut().append(SET_COOKIE, csrf);
    }
    response
}
