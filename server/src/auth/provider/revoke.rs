//! State-poll and state-revoke routes: `GET /auth/state` (the one-shot login
//! poll) and `POST /auth/state/revoke`. Part of the ARC-003 provider-module
//! split — handler bodies are moved verbatim from the former
//! `auth/provider.rs` monolith; the state-row machinery they drive
//! (`claim_pending`/`set_outcome`/`poll_login`) stays in `super`.

use std::sync::Arc;

use axum::Json;
use axum::extract::{ConnectInfo, Query as QueryParams, Request, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use subtle::ConstantTimeEq;

use crate::AppState;
use crate::db::{now_ms, sha256_hex};
use crate::error::RtDbError;
use crate::protocol::AuthedUser;

use super::{
    DELIVERY_BEARER, PollResult, STATUS_FAILED, STATUS_PENDING, STATUS_REVOKED, poll_login,
};

#[derive(Deserialize)]
pub(super) struct StateQuery {
    state: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum StateResponse {
    Pending,
    /// SEC-207: `token` is omitted entirely for a cookie-mode login — the
    /// HttpOnly session cookie set at the callback stamps the only credential
    /// carrier, so the poll body never exposes a script-readable copy.
    /// `revokeSecret` is likewise NOT carried here: it was delivered once in
    /// the begin response (client-only channel) and only its sha256 verifier
    /// is stored server-side. `sessionId` (bearer logins only) is the
    /// non-secret half of the durable session-revoke capability — the client
    /// pairs it with the begin response's `revokeSecret` for
    /// `POST /auth/session/revoke`.
    Complete {
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        user: AuthedUser,
        #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// The capability's absolute expiry bound (bearer logins only): the
        /// session's `expires_at` on SERVER time, when the tombstone is
        /// swept. Not a client-side proof — a client clock ahead of the
        /// server reaches it early. The sound client rule: retry until
        /// `revoked` (the server answers `revoked` for an already-expired
        /// row it still holds, proving and pruning it); only after this
        /// instant plus a clock-skew margin AND continued `unknown` may the
        /// client stop, as resolved-unverifiable — never as proof.
        #[serde(rename = "expiresAt", skip_serializing_if = "Option::is_none")]
        expires_at: Option<i64>,
    },
    Expired,
    Error,
}

/// `GET /auth/state?state=`: provider-agnostic polling endpoint keyed on the
/// single-use `state` token minted at `/auth/{provider}/begin`. Returns
/// `pending | complete | expired | error`.
///
/// Authorization, in order: (1) SEC-121 cookie — the `state` value appears in
/// URLs, edge logs, and the server's TraceLayer, so a leaked URL alone is not
/// enough to poll; the request must ALSO carry the `rtdb-oauth-state` cookie
/// set at `/begin` (or `/auth/login`) with the same value, constant-time
/// compared. (2) CLI bootstrap path — failing the cookie check, a request
/// carrying the `x-rtdb-oauth-revoke-secret` header whose sha256 matches the
/// begin-time verifier stored on the row is authorized the same way
/// `POST /auth/state/revoke` is: `{state, revokeSecret}` together, the secret
/// delivered once in the begin response body (never a URL). Default/cookie
/// rows have no verifier and unknown states match nothing, both answering the
/// same `expired` as a cookie miss (no mode or existence leak). A
/// missing/mismatched attempt at both returns `Expired` (the caller already
/// handles that indistinguishably from a timed-out flow).
pub(super) async fn auth_state(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<StateQuery>,
) -> Response {
    let cookie_ok = crate::auth::cookie::oauth_state_cookie(&headers)
        .is_some_and(|c| bool::from(c.as_bytes().ct_eq(params.state.as_bytes())));
    if !cookie_ok && !authorize_secret_poll(state.as_ref(), &headers, &params.state).await {
        return Json(StateResponse::Expired).into_response();
    }
    match poll_login(&state, &params.state).await {
        PollResult::Pending => Json(StateResponse::Pending).into_response(),
        PollResult::Complete {
            token,
            user,
            cookie_mode,
            session_id,
            expires_at,
        } => {
            let token = if cookie_mode { None } else { Some(token) };
            // The capability fields are bearer-only: a cookie-mode login
            // carries no stamped capability, so never leak its absence as a
            // field. (lookup_session_capability returns None for those rows
            // anyway — this also keeps a hypothetical stamp failure from
            // leaking mode.)
            let (session_id, expires_at) = if cookie_mode {
                (None, None)
            } else {
                (session_id, expires_at)
            };
            Json(StateResponse::Complete {
                token,
                user,
                session_id,
                expires_at,
            })
            .into_response()
        }
        PollResult::Failed => Json(StateResponse::Error).into_response(),
        PollResult::Expired => Json(StateResponse::Expired).into_response(),
    }
}

/// Header-path authorization for the `/auth/state` poll (the CLI bootstrap
/// flow): the presented `x-rtdb-oauth-revoke-secret` header must hash to the
/// begin-time verifier stored on the row — the same `{state, revokeSecret}`
/// capability `POST /auth/state/revoke` requires, so it is exactly as strong
/// as that fence: the secret was delivered once in the begin response body
/// (never in a URL), and only its sha256 verifier is stored. Runs only when
/// the cookie check already failed; browser pollers never reach it.
///
/// Cost note: one indexed SELECT per cookie-less poll that carries the header
/// — the same cost class `/auth/state/revoke` pays per request (there behind
/// a 30 rpm IP guard). Polls without the header — the common garbage-flood
/// shape — still fail the cookie check for free, so no limiter is added here.
/// One-shot semantics are untouched: a second secret-authorized poll of an
/// already-consumed row re-authorizes but finds nothing terminal-unconsumed,
/// answering `expired` like any other second poll.
async fn authorize_secret_poll(state: &AppState, headers: &HeaderMap, state_token: &str) -> bool {
    let Some(presented) = headers
        .get("x-rtdb-oauth-revoke-secret")
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let verifier: Option<Option<String>> = sqlx::query_scalar(
        "SELECT revoke_secret_verifier FROM rtdb_auth.oauth_states \
         WHERE state = $1 AND expires_at > $2",
    )
    .bind(state_token)
    .bind(now_ms())
    .fetch_optional(&state.pool)
    .await
    .ok();
    let Some(Some(verifier)) = verifier else {
        return false;
    };
    bool::from(sha256_hex(presented).as_bytes().ct_eq(verifier.as_bytes()))
}

/// Body of `POST /auth/state/revoke`. `revokeSecret` is required: `state`
/// transits the provider redirect URL (browser history, referrers, proxy
/// logs, provider telemetry), so state alone is not a revocation capability —
/// the secret is a client-only channel value.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateRevokeBody {
    state: String,
    revoke_secret: Option<String>,
}

/// Response of `POST /auth/state/revoke`. `revoked` is a CONFIRMED fence: a
/// live bearer row (pending/claiming/completed/consumed — or a previously
/// revoked one still owed cleanup) was transitioned to `revoked` and its
/// tracked session was verifiably deleted (or provably never existed) in the
/// same transaction. `unconfirmed` is the FAILED-state answer: the state is
/// terminal-dead (the poll can never deliver), but `complete_login` returning
/// Err does not prove no session row was committed (the ambiguous-insert
/// case — a minted session whose plaintext was dropped when the error
/// surfaced), so session absence is not provable. `unknown` means no bearer
/// row matched the presented (state, revokeSecret) pair — never minted, a
/// `default`/`cookie` delivery (revoke is scoped to the bearer flow), wrong
/// or missing secret, or past its TTL / swept; the caller cannot distinguish
/// these (no existence leak). The-cube's client rule holds: any non-`revoked`
/// outcome is treated as unconfirmed.
#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum StateRevokeResponse {
    Revoked,
    Unconfirmed,
    Unknown,
}

/// Per-IP budget for `/auth/state/revoke` (fixed-window rpm, same limiter and
/// key shape as `/begin`'s SEC-003 guard). Sized for legitimate retry
/// capacity: the real client re-sends a handful of times on `unconfirmed`
/// outcomes, so 30/min per IP is several times the retry need while still
/// capping a garbage-capability flood — every request past it pays only the
/// limiter check, never a DB transaction.
const STATE_REVOKE_PER_IP_RPM: u32 = 30;
/// Total-body bound for the revoke request. The capability pair is two ~43
/// char strings; 4 KiB bounds parser work per request while dwarfing any
/// legitimate body.
const STATE_REVOKE_MAX_BODY_BYTES: usize = 4096;
/// Per-field bound (`state`, `revokeSecret`): real values are 43-char random
/// tokens; anything longer is shape-garbage and answers the uniform response.
const STATE_REVOKE_MAX_FIELD_CHARS: usize = 256;

/// `POST /auth/state/revoke` `{state, revokeSecret}`: state AND revoke secret
/// together are the capability (only sha256(presented) is stored). Scoped to
/// BEARER-delivery rows — a revoke racing a default/cookie callback would
/// make `set_outcome` delete a session whose token `callback_close_response`
/// was about to set as the browser's `rtdb_session` cookie, replacing a live
/// cookie with a dead one; non-bearer rows answer the same `unknown`-shaped
/// body as never-minted (no mode-leaking distinction). Atomically transitions
/// a live bearer row (`pending`/`claiming`/`completed`) to `revoked` and
/// deletes its tracked session in ONE transaction — a crash or failure
/// between the fence and the delete rolls the fence back, leaving the row
/// unconsumed so a retry re-runs the whole cleanup.
///
/// Response mapping (durable settlement evidence, never inferred): a
/// `pending` row is cancelled before claim and answers `revoked` (the
/// callback can never claim it); `claiming` is fenced but answers
/// `unconfirmed` — the mint may already be committed with the plaintext still
/// in flight; `completed` deletes its tracked session and answers `revoked`;
/// an already-`revoked` row answers `revoked` only once
/// `callback_settled_at` is set, deleting any owed token first, and stays
/// `unconfirmed` before that — including forever after a crash between the
/// session INSERT and the settlement/marker write (never inferred from token
/// absence or elapsed time); `failed` answers `unconfirmed` (ambiguous-insert
/// is not provably clean). The-cube client rule: any non-`revoked` outcome is
/// unconfirmed. Expired or TTL-swept rows, wrong or missing secrets, and
/// non-bearer deliveries all answer `unknown`. The digest compare is
/// constant-time (`ct_eq`, same hygiene as the CSRF gates). This
/// endpoint sets no cookies, so it never disturbs a browser's pre-existing
/// `rtdb_session` — and `POST /auth/logout` likewise emits its
/// cookie-clearing headers only when the request presented that cookie.
///
/// The route is unauthenticated (capability = state + secret), so it carries
/// the same SEC-003-style per-IP fixed-window guard as `/begin` (30 rpm,
/// tighter than begin's configurable ceiling) and a strict body-shape gate
/// BEFORE any transaction: bodies over 4 KiB, non-object JSON, non-string or
/// over-length `state`/`revokeSecret` fields, and limiter denials ALL answer
/// the same unknown-shaped 200 — garbage capabilities never reach the fence,
/// never open a transaction, and leak nothing about what failed.
pub(super) async fn state_revoke(
    State(state): State<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    req: Request,
) -> Response {
    // SEC-003-style per-IP guard, mirroring `/begin`: garbage capabilities
    // must not consume DB connections. Same key shape, fixed-window mechanism,
    // a tighter const budget. Denied requests answer the SAME unknown-shaped
    // 200 as everything else (uniformity — no distinct error envelope, no
    // confirmation of any capability).
    let ip_key = crate::http_api::client_ip_key(&headers, addr.ip(), state.config.trusted_proxy);
    match state
        .limits
        .rate_limiter
        .check(
            crate::rate_limit::RateKey::Ip {
                route: "state_revoke",
                ip: ip_key,
            },
            STATE_REVOKE_PER_IP_RPM,
        )
        .await
    {
        crate::rate_limit::RateDecision::Allowed => {}
        crate::rate_limit::RateDecision::Denied { .. } => {
            return Json(StateRevokeResponse::Unknown).into_response();
        }
    }

    // Body shape gate BEFORE any transaction: read at most
    // `STATE_REVOKE_MAX_BODY_BYTES`, require a JSON object with string
    // `state`/`revokeSecret` fields in bounds — any failure answers the same
    // unknown-shaped 200 (no new status codes, no shape leakage).
    let bytes = match axum::body::to_bytes(req.into_body(), STATE_REVOKE_MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return Json(StateRevokeResponse::Unknown).into_response(),
    };
    let body: StateRevokeBody = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        Err(_) => return Json(StateRevokeResponse::Unknown).into_response(),
    };
    if body.state.len() > STATE_REVOKE_MAX_FIELD_CHARS
        || body
            .revoke_secret
            .as_deref()
            .is_some_and(|s| s.len() > STATE_REVOKE_MAX_FIELD_CHARS)
    {
        return Json(StateRevokeResponse::Unknown).into_response();
    }

    let now = now_ms();
    let mut tx = match state.pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            tracing::warn!(error = %err, "oauth: state revoke tx begin failed");
            return RtDbError::internal("failed to revoke state").into_response();
        }
    };
    // Lock the row first (FOR UPDATE), then decide in Rust — the secret
    // compare must be constant-time, which SQL cannot express, and the
    // row lock keeps the fence atomic through the compare.
    type RevokeRow = (String, Option<String>, Option<String>, String, Option<i64>);
    let row: Result<Option<RevokeRow>, sqlx::Error> = sqlx::query_as(
        "SELECT status, session_token, revoke_secret_verifier, delivery, callback_settled_at \
         FROM rtdb_auth.oauth_states \
         WHERE state = $1 AND expires_at > $2 \
         FOR UPDATE",
    )
    .bind(&body.state)
    .bind(now)
    .fetch_optional(&mut *tx)
    .await;

    let row = match row {
        Ok(r) => r,
        Err(err) => {
            tracing::warn!(error = %err, "oauth: state revoke query failed");
            return RtDbError::internal("failed to revoke state").into_response();
        }
    };
    let Some((status, session_token, stored_verifier, delivery, settled_at)) = row else {
        // No row — roll back (a no-op) and answer unknown.
        return Json(StateRevokeResponse::Unknown).into_response();
    };

    // Capability check: sha256(presented secret) must match the stored
    // verifier, constant-time (same digest hygiene as sessions.token_hash).
    // A missing/wrong secret answers the same unknown-shaped body regardless
    // of whether the state exists (no existence leak). Revocation is also
    // scoped to bearer deliveries.
    let secret_ok = body
        .revoke_secret
        .as_deref()
        .zip(stored_verifier.as_deref())
        .is_some_and(|(presented, stored)| {
            bool::from(sha256_hex(presented).as_bytes().ct_eq(stored.as_bytes()))
        });
    if !secret_ok || delivery != DELIVERY_BEARER || status == STATUS_FAILED {
        if status == STATUS_FAILED && secret_ok && delivery == DELIVERY_BEARER {
            // Terminal-dead but session absence is not provable
            // (ambiguous-insert) — a distinct, honest answer.
            return Json(StateRevokeResponse::Unconfirmed).into_response();
        }
        return Json(StateRevokeResponse::Unknown).into_response();
    }

    let confirmed = status == STATUS_PENDING || settled_at.is_some();
    // The fence: flip status under the row lock we already hold. Already-
    // revoked rows re-match (retryable cleanup); RETURNING is not needed —
    // the token came from the locked read.
    let fenced = sqlx::query(
        "UPDATE rtdb_auth.oauth_states \
         SET status = $1, callback_settled_at = $3 \
         WHERE state = $2",
    )
    .bind(STATUS_REVOKED)
    .bind(&body.state)
    .bind(if status == STATUS_PENDING {
        Some(now)
    } else {
        settled_at
    })
    .execute(&mut *tx)
    .await;
    if let Err(err) = fenced {
        tracing::warn!(error = %err, "oauth: state revoke fence failed");
        return RtDbError::internal("failed to revoke state").into_response();
    }
    if let Some(token) = session_token {
        // Same transaction as the fence: a crash or failure here rolls the
        // fence back, leaving the row unconsumed so a retry re-runs the
        // cleanup. Zero deleted rows is fine (the session was already gone —
        // the caller's goal holds either way).
        let hash = sha256_hex(&token);
        if let Err(err) = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
            .bind(&hash)
            .execute(&mut *tx)
            .await
        {
            tracing::warn!(error = %err, "oauth: state revoke session delete failed");
            return RtDbError::internal("failed to revoke state").into_response();
        }
        // Hygiene: drop the recorded plaintext token in the same transaction.
        if let Err(err) =
            sqlx::query("UPDATE rtdb_auth.oauth_states SET session_token = NULL WHERE state = $1")
                .bind(&body.state)
                .execute(&mut *tx)
                .await
        {
            tracing::warn!(error = %err, "oauth: state revoke token clear failed");
            return RtDbError::internal("failed to revoke state").into_response();
        }
    }
    match tx.commit().await {
        Ok(()) => Json(if confirmed {
            StateRevokeResponse::Revoked
        } else {
            StateRevokeResponse::Unconfirmed
        })
        .into_response(),
        Err(err) => {
            tracing::warn!(error = %err, "oauth: state revoke commit failed");
            RtDbError::internal("failed to revoke state").into_response()
        }
    }
}
