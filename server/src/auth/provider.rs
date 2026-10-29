//! The `OAuthProvider` trait and the dispatcher all six providers sit behind.
//! Carries the login-ledger invariants: the OAuth `state` token is single-use
//! (stored in `rtdb_auth.oauth_states`, so a login begun on one replica
//! completes on another), login-CSRF is a double-submit nonce cookie
//! constant-time-verified at `/callback`, and the anonymous mint is gated and
//! rate-limited. Bearer-delivery logins additionally mint a client-only
//! revoke secret at begin (delivered only in the begin response body; only
//! its sha256 verifier is persisted) — `POST /auth/state/revoke` requires
//! `{state, revokeSecret}` together, because `state` alone transits the
//! provider redirect URL and is not a revocation capability. A lost begin
//! response means the flow was never navigated and the pending row simply
//! ages out via the TTL sweep — correct, nothing to clean.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{ConnectInfo, Form, Query as QueryParams, Request, State};
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

/// Lifecycle a single-use OAuth login `state` token passes through, persisted
/// as the `status` column of `rtdb_auth.oauth_states` (ENH-022 Stage 1).
/// `pending` → `claiming` (first callback wins, single-use enforced by the
/// `UPDATE ... WHERE status = 'pending'` predicate) → `completed` | `failed`.
/// Storing this in Postgres — not an in-process map — is what lets a login
/// begun on one replica complete the callback on another.
const STATUS_PENDING: &str = "pending";
const STATUS_CLAIMING: &str = "claiming";
const STATUS_COMPLETED: &str = "completed";
const STATUS_FAILED: &str = "failed";
/// Terminal state recorded by `POST /auth/state/revoke` (the state token is
/// the capability). Nothing reads a `revoked` row back to life: `claim_pending`
/// only claims `pending`, the poll only consumes `completed`/`failed`, and
/// `set_outcome` refuses to overwrite `revoked` — a callback that was mid-mint
/// when the revoke landed rolls forward into the cancellation instead, deleting
/// the session it just minted. Rows age out via the ordinary TTL sweep.
const STATUS_REVOKED: &str = "revoked";

/// Credential delivery for a login, persisted per `oauth_states` row (the
/// `delivery` column; the legacy `cookie_mode` bool stays as a mirror of
/// `delivery == "cookie"`).
///
/// - `default` (the historical flow): the callback sets the HttpOnly
///   `rtdb_session` cookie + SEC-106 admin-CSRF nonce AND records the token
///   for the one-shot poll — the credential reaches the browser twice.
/// - `cookie` (SEC-207): the poll omits the token; the cookie is the only
///   carrier.
/// - `bearer` (opt-in, cross-tab hardening): the inverse of `cookie` — the
///   one-shot poll is the ONLY carrier. The callback sets no `rtdb_session`
///   cookie and mints no admin-CSRF nonce (it still clears the login-CSRF
///   nonce), so other same-origin tabs cannot adopt the session via the
///   shared cookie, and revoking this login's own state cannot disturb a
///   pre-existing cookie-backed session.
const DELIVERY_DEFAULT: &str = "default";
const DELIVERY_COOKIE: &str = "cookie";
const DELIVERY_BEARER: &str = "bearer";

/// Cap on concurrently-pending OAuth state entries. `/begin` mints one per
/// login attempt and rows are swept after `STATE_TTL_MS`; this bound is a
/// defense-in-depth against an attacker spamming `/begin` to grow the table
/// (closes the SEC-132 unbounded-map note — the prior in-memory map was pruned
/// only opportunistically).
const MAX_PENDING_STATES: i64 = 10_000;

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

// --- ARC-114: shared OIDC exchange + Template Method scope -----------------
//
// The six providers split into two shapes:
//
// 1. The common OIDC shape (Google, GitLab, generic OIDC): a standards-form
//    `authorization_code` token exchange, then a single authenticated userinfo
//    GET, then `auth::resolve_user`. The token-exchange + userinfo-fetch half
//    is byte-identical across these three (only URLs + the error-message slug
//    differ), so it lives in `oidc_exchange_and_fetch_userinfo` below. Each
//    provider keeps its own `parse_userinfo`, which differs in both the
//    verified-email signal (Google's `email_verified`, GitLab's `confirmed_at`,
//    OIDC's required `email_verified`) and the stable subject it extracts
//    (Google's/OIDC's `sub`, GitLab's numeric `id`).
//
// 2. Divergent flows that deliberately do NOT use the helper:
//    - GitHub fetches `/user` AND `/user/emails` (two GETs, a User-Agent
//      header).
//    - Microsoft verifies an id_token against a tenant JWKS and keys identity
//      on `{tid}.{sub}` (SEC-102 — must NOT regress to email keying).
//    - Apple signs an ES256 client_secret JWT and reads identity from the
//      id_token (no userinfo GET at all).
//
// Forcing those three into a single template would either bloat it with
// optional hooks or risk the very identity-keying regressions SEC-102 closed.
// The shared helper below serves the clean trio; the rest keep their own
// `complete_login`.
//
// The upsert itself is no longer per-provider: all six resolve through
// `auth::resolve_user`, which keys on the provider's own stable subject column
// and never on the email. A new provider MUST supply such a subject — see
// "Adding a new provider" in `docs/OAUTH_SETUP.md`.

#[derive(Serialize)]
struct AuthorizationCodeRequest<'a> {
    client_id: &'a str,
    client_secret: &'a str,
    code: &'a str,
    redirect_uri: &'a str,
    grant_type: &'a str,
}

/// Extracts the `access_token` from a token-exchange response. Success ⇒ the
/// token; an error/empty body ⇒ `{slug} token exchange failed` (generic, never
/// leaking the OAuth error text). Shared by the three OIDC-shape providers.
fn extract_access_token(
    slug: &'static str,
    value: &serde_json::Value,
) -> Result<String, RtDbError> {
    match value.get("access_token").and_then(|v| v.as_str()) {
        Some(token) => Ok(token.to_string()),
        None => {
            // SEC-205: log only the response SHAPE (which keys are present),
            // never the body — a response missing access_token can still carry
            // id_token/refresh_token fragments (same pattern as apple.rs).
            let keys: Vec<&str> = value
                .as_object()
                .map(|m| m.keys().map(String::as_str).collect())
                .unwrap_or_default();
            tracing::warn!(present_keys = ?keys, "{slug} token exchange returned no access_token");
            Err(RtDbError::internal(format!("{slug} token exchange failed")))
        }
    }
}

/// The shared token-exchange + userinfo-fetch half of the common OIDC login
/// dance (ARC-114). Used by the Google, GitLab, and generic OIDC providers —
/// the three whose flow is a standards-form `authorization_code` POST followed
/// by a single authenticated userinfo GET. GitHub/Microsoft/Apple have
/// divergent flows and do not call this.
///
/// `slug` is the provider name used in generic error bodies and tracing (e.g.
/// "google"), preserving the per-provider error strings the callers previously
/// emitted. Returns the userinfo JSON for the provider's own `parse_userinfo`.
pub(crate) struct OidcExchange<'a> {
    pub http: &'a reqwest::Client,
    pub slug: &'static str,
    pub token_url: &'a str,
    pub userinfo_url: &'a str,
    pub client_id: &'a str,
    pub client_secret: &'a str,
    pub code: &'a str,
    pub redirect_uri: &'a str,
}

pub(crate) async fn oidc_exchange_and_fetch_userinfo(
    req: OidcExchange<'_>,
) -> Result<serde_json::Value, RtDbError> {
    let OidcExchange {
        http,
        slug,
        token_url,
        userinfo_url,
        client_id,
        client_secret,
        code,
        redirect_uri,
    } = req;
    let token_resp: serde_json::Value = http
        .post(token_url)
        .form(&AuthorizationCodeRequest {
            client_id,
            client_secret,
            code,
            redirect_uri,
            grant_type: "authorization_code",
        })
        .send()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "{slug} token exchange request failed");
            RtDbError::internal(format!("{slug} token exchange failed"))
        })?
        .json()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "{slug} token exchange response decode failed");
            RtDbError::internal(format!("{slug} token exchange failed"))
        })?;

    let access_token = extract_access_token(slug, &token_resp)?;

    let userinfo: serde_json::Value = http
        .get(userinfo_url)
        .header(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {access_token}"),
        )
        .send()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "{slug} userinfo fetch request failed");
            RtDbError::internal(format!("{slug} userinfo fetch failed"))
        })?
        .json()
        .await
        .map_err(|err| {
            tracing::warn!(error = %err, "{slug} userinfo fetch response decode failed");
            RtDbError::internal(format!("{slug} userinfo fetch failed"))
        })?;

    Ok(userinfo)
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

/// `pending → claiming` for the first caller; `None` for a replay, an
/// already-terminal/revoked/expired row, or a cross-provider claim (SEC-132:
/// the state's minting provider must match the callback's provider). Single-use
/// is enforced by the database — the `WHERE status = 'pending'` predicate means
/// a second callback (a replay, or a concurrent race across two replicas)
/// matches zero rows and returns `None`. On success returns the row's
/// `anon_user_id` binding (FM-27): `Some(id)` when the login began from an
/// anonymous session, `None` otherwise — plus the row's credential delivery
/// mode, which the callback honors when closing the popup.
async fn claim_pending(
    state: &Arc<AppState>,
    state_token: &str,
    expected_provider: &'static str,
) -> Option<(Option<String>, String)> {
    let now = now_ms();
    let row: Option<(Option<String>, String)> = sqlx::query_as(
        "UPDATE rtdb_auth.oauth_states SET status = $1 \
         WHERE state = $2 AND provider = $3 AND status = $4 AND expires_at > $5 \
         RETURNING anon_user_id, delivery",
    )
    .bind(STATUS_CLAIMING)
    .bind(state_token)
    .bind(expected_provider)
    .bind(STATUS_PENDING)
    .bind(now)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();
    row
}

/// FM-27: after a successful provider login whose state row was minted from
/// an anonymous session, synchronously merge the anon footprint into the real
/// account BEFORE `set_outcome` records the terminal state (so a crash before
/// the merge simply leaves the login claiming and the next sign-in re-runs it —
/// every merge step is idempotent). Merge failures are logged at ERROR and
/// never fail the login.
async fn merge_anon_into_real(state: &Arc<AppState>, anon_id: &str, session_token: &str) {
    let real_id = match resolve_bearer(&state.pool, session_token).await {
        Ok(Principal::User { user_id, .. }) if user_id != anon_id => user_id,
        Ok(_) => return, // anon row was already merged away; nothing to do
        Err(err) => {
            tracing::error!(error = %err, "anon merge: could not resolve the fresh session");
            return;
        }
    };
    if let Err(err) = crate::merge::merge_users(state, anon_id, &real_id).await {
        tracing::error!(
            anon = %anon_id,
            real = %real_id,
            error = %err,
            "anon->real merge failed; recovered by the next sign-in"
        );
    }
}

/// Sets the terminal outcome after `complete_login` (`claiming → completed |
/// failed`). A `completed` row carries the minted session token the
/// `/auth/state` poll reads; a `failed` row carries none. This does NOT set
/// `consumed_at` — consumption is the poll's act (one-shot retrieval by the
/// client), so a completed-but-unpolled row remains consumable.
async fn set_outcome(state: &Arc<AppState>, state_token: &str, completed: Option<&str>) {
    let now = now_ms();
    let (status, token): (&str, Option<&str>) = match completed {
        Some(t) => (STATUS_COMPLETED, Some(t)),
        None => (STATUS_FAILED, None),
    };
    // Fence: a row revoked while this callback was in flight (`POST
    // /auth/state/revoke` landing during the provider exchange) must not be
    // resurrected. Matching zero rows means the row is `revoked` or already
    // TTL-swept, so the just-minted session rolls forward into the
    // cancellation instead of going live. The same statement records durable
    // callback-settlement evidence (`callback_settled_at`) — the response
    // mapping in `state_revoke` treats settlement as provable cleanup only
    // from this marker, never from token absence or elapsed time. A mid-flight
    // TTL sweep has the same effect: the poll is already dead for a swept row,
    // so nobody ever legitimately holds that token.
    match sqlx::query(
        "UPDATE rtdb_auth.oauth_states \
         SET status = $1, session_token = $2, callback_settled_at = $3 \
         WHERE state = $4 AND status <> $5",
    )
    .bind(status)
    .bind(token)
    .bind(now)
    .bind(state_token)
    .bind(STATUS_REVOKED)
    .execute(&state.pool)
    .await
    {
        Ok(res) if res.rows_affected() > 0 => {}
        Ok(_) => {
            if let Some(t) = completed {
                roll_forward_into_revocation(state, state_token, t).await;
            } else {
                // Failed outcome on a revoked/swept row: record settlement so
                // retries see the callback finished (still `unconfirmed` —
                // session absence is not provable).
                let _ = sqlx::query(
                    "UPDATE rtdb_auth.oauth_states \
                     SET status = 'failed', callback_settled_at = $1 \
                     WHERE state = $2 AND status = $3",
                )
                .bind(now)
                .bind(state_token)
                .bind(STATUS_REVOKED)
                .execute(&state.pool)
                .await;
            }
        }
        Err(err) => tracing::warn!(error = %err, "oauth: failed to record terminal outcome"),
    }
}

/// Roll-forward for a session minted by a callback whose state row was revoked
/// mid-mint. Marker-first: the token AND the settlement marker are written
/// together and that UPDATE commits BEFORE the delete is attempted, so a crash
/// after the mint but before the delete leaves a durable, retry-recoverable
/// debt rather than an orphaned session. Deleting zero rows is fine (already
/// cleaned); on delete success the recorded plaintext is cleared while the
/// settlement marker stays (a historical fact).
async fn roll_forward_into_revocation(state: &Arc<AppState>, state_token: &str, token: &str) {
    let now = now_ms();
    let marker = sqlx::query(
        "UPDATE rtdb_auth.oauth_states \
         SET session_token = $1, callback_settled_at = $2 \
         WHERE state = $3 AND status = $4",
    )
    .bind(token)
    .bind(now)
    .bind(state_token)
    .bind(STATUS_REVOKED)
    .execute(&state.pool)
    .await;
    match marker {
        Ok(res) if res.rows_affected() > 0 => {
            // Row present and revoked — delete, then clear the recorded
            // plaintext. On delete failure the marker STAYS: the next
            // state/revoke retry deletes the session and confirms.
            if let Err(err) = session::delete_session(&state.pool, token).await {
                tracing::warn!(
                    error = %err,
                    "oauth: failed to delete a session minted after revoke; token left recorded for revoke-retry cleanup"
                );
                return;
            }
            if let Err(err) = sqlx::query(
                "UPDATE rtdb_auth.oauth_states \
                 SET session_token = NULL \
                 WHERE state = $1 AND status = $2",
            )
            .bind(state_token)
            .bind(STATUS_REVOKED)
            .execute(&state.pool)
            .await
            {
                tracing::warn!(error = %err, "oauth: failed to clear a rolled-forward token");
            }
        }
        Ok(_) => {
            // Row already swept/missing — the poll was dead anyway; clean up
            // best-effort so no live session lingers.
            if let Err(err) = session::delete_session(&state.pool, token).await {
                tracing::warn!(error = %err, "oauth: failed to delete a session minted after a swept revoke");
            }
        }
        Err(err) => {
            // Marker query itself failed — fall through to the delete so the
            // fence's cleanup intent still applies. Settlement stays unset, so
            // retries keep answering `unconfirmed` (honest: untracked debt).
            tracing::warn!(error = %err, "oauth: roll-forward marker write failed");
            if let Err(derr) = session::delete_session(&state.pool, token).await {
                tracing::warn!(error = %derr, "oauth: failed to delete a session after a failed marker");
            }
        }
    }
}

enum PollResult {
    Pending,
    /// `cookie_mode` (SEC-207): the login began with `mode=cookie`, so the
    /// poll response omits the token — the HttpOnly cookie is the credential.
    Complete {
        token: String,
        user: AuthedUser,
        cookie_mode: bool,
    },
    Failed,
    Expired,
}

/// One-shot retrieval for the `/auth/state` polling endpoint. Consumes a
/// terminal row single-use: the `UPDATE ... WHERE status IN ('completed',
/// 'failed') AND consumed_at IS NULL RETURNING` is the one-shot gate — exactly
/// the first poll after completion matches, so a second poll for the same
/// token does not re-deliver the session token. When no terminal row matches
/// (the row is still pending/claiming, already consumed, missing, or expired)
/// a fallback read distinguishes `Pending` (login still in flight) from
/// `Expired` (consumed, never begun, or timed out).
async fn poll_login(state: &Arc<AppState>, state_token: &str) -> PollResult {
    let now = now_ms();
    // The single-use consume: only an unconsumed, unexpired, terminal row
    // matches, so only the first poll after completion wins it.
    let consumed: Option<(Option<String>, String, String)> = sqlx::query_as(
        "UPDATE rtdb_auth.oauth_states \
         SET consumed_at = $1 \
         WHERE state = $2 \
           AND status IN ($3, $4) \
           AND consumed_at IS NULL \
           AND expires_at > $1 \
         RETURNING session_token, status, delivery",
    )
    .bind(now)
    .bind(state_token)
    .bind(STATUS_COMPLETED)
    .bind(STATUS_FAILED)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();

    if let Some((token, status, delivery)) = consumed {
        if status == STATUS_COMPLETED {
            return match token {
                Some(t) => match resolve_bearer(&state.pool, &t).await {
                    Ok(principal @ Principal::User { .. }) => PollResult::Complete {
                        token: t,
                        user: authed_user(&principal),
                        cookie_mode: delivery == DELIVERY_COOKIE,
                    },
                    _ => PollResult::Expired, // token did not resolve — treat as gone
                },
                None => PollResult::Expired,
            };
        }
        return PollResult::Failed;
    }

    // No terminal row consumed: distinguish pending (login in flight) from
    // expired (missing / already consumed / timed out).
    let live: Option<String> = sqlx::query_scalar(
        "SELECT status FROM rtdb_auth.oauth_states \
         WHERE state = $1 AND status IN ($2, $3) AND expires_at > $4",
    )
    .bind(state_token)
    .bind(STATUS_PENDING)
    .bind(STATUS_CLAIMING)
    .bind(now)
    .fetch_optional(&state.pool)
    .await
    .ok()
    .flatten();

    if live.is_some() {
        PollResult::Pending
    } else {
        PollResult::Expired
    }
}

/// Deletes OAuth state rows past their TTL. Called opportunistically from
/// `/begin` (keeps the pending-row cap honest) and by a gated background sweep
/// task (ARC-102: no ungated poller — the task runs only while the server is
/// up and writes nothing to document tables). Closes the SEC-132 note that the
/// prior in-memory map was pruned only opportunistically.
pub async fn sweep_oauth_states(pool: &sqlx::PgPool) -> Result<u64, RtDbError> {
    let now = now_ms();
    let r = sqlx::query("DELETE FROM rtdb_auth.oauth_states WHERE expires_at <= $1")
        .bind(now)
        .execute(pool)
        .await?;
    Ok(r.rows_affected())
}

#[derive(Deserialize)]
struct BeginParams {
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
async fn provider_begin<P: OAuthProvider>(
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
        Some(token) => match resolve_bearer(&state.pool, &token).await {
            Ok(Principal::User {
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
    let revoke_secret_verifier = revoke_secret.as_deref().map(crate::db::sha256_hex);
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
    let mut response = Json(BeginResponse {
        authorize_url,
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

    let (anon_user_id, delivery) = match claim_pending(&state, &params.state, P::name()).await {
        Some((anon, delivery)) => (anon, delivery),
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

    let (anon_user_id, delivery) =
        match claim_pending(&state, &form.state, AppleProvider::name()).await {
            Some((anon, delivery)) => (anon, delivery),
            None => return RtDbError::bad_request("invalid or expired state").into_response(),
        };

    let Some(provider) = AppleProvider::from_config(&state.config) else {
        set_outcome(&state, &form.state, None).await;
        return unconfigured_response(AppleProvider::name());
    };

    let secure = state.config.cookie_secure
        || crate::auth::cookie::request_is_secure(&headers, state.config.trusted_proxy);
    match provider.complete_login(&state, &form.code).await {
        Ok(token) => {
            // FM-27: merge the anon footprint into the real account before the
            // terminal outcome is recorded (see merge_anon_into_real).
            if let Some(anon_id) = &anon_user_id {
                merge_anon_into_real(&state, anon_id, &token).await;
            }
            set_outcome(&state, &form.state, Some(&token)).await;
            if delivery == DELIVERY_BEARER {
                bearer_callback_close_response()
            } else {
                callback_close_response(&token, secure)
            }
        }
        Err(err) => {
            set_outcome(&state, &form.state, None).await;
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

/// The popup-closing HTML shared by every delivery mode. Nothing is
/// interpolated (no `origin`, no token) — SEC-005 stays retired: the token
/// never appears in the body regardless of how the response carries it.
fn callback_close_html() -> &'static str {
    "<!doctype html><html><head><meta charset=\"utf-8\"><title>Signed in</title>\
     </head><body><script>window.close();</script>\
     <p style=\"font-family:sans-serif\">Sign-in complete. You may close this window.</p>\
     </body></html>"
}

/// Close response for the historical default and SEC-207 cookie deliveries:
/// identical popup HTML + CSP, plus the HttpOnly `rtdb_session` cookie, the
/// SEC-106 admin-CSRF nonce, and the login-CSRF nonce clear.
fn callback_close_response(token: &str, secure: bool) -> Response {
    let mut response = Html(callback_close_html()).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; script-src 'unsafe-inline'"),
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
fn bearer_callback_close_response() -> Response {
    let mut response = Html(callback_close_html()).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("content-security-policy"),
        HeaderValue::from_static("default-src 'none'; script-src 'unsafe-inline'"),
    );
    response
        .headers_mut()
        .append(SET_COOKIE, crate::auth::cookie::clear_oauth_csrf_cookie());
    response
}

#[derive(Deserialize)]
struct StateQuery {
    state: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum StateResponse {
    Pending,
    /// SEC-207: `token` is omitted entirely for a cookie-mode login — the
    /// HttpOnly session cookie set at the callback is the only credential
    /// carrier, so the poll body never exposes a script-readable copy.
    /// The revoke capability is NOT carried here: `revokeSecret` was delivered
    /// once in the begin response (client-only channel) and only its sha256
    /// verifier is stored server-side.
    Complete {
        #[serde(skip_serializing_if = "Option::is_none")]
        token: Option<String>,
        user: AuthedUser,
    },
    Expired,
    Error,
}

/// `GET /auth/state?state=`: provider-agnostic polling endpoint keyed on the
/// single-use `state` token minted at `/auth/{provider}/begin`. Returns
/// `pending | complete | expired | error`.
///
/// SEC-121: the `state` value appears in URLs, edge logs, and the server's
/// TraceLayer, so a leaked URL alone is not enough to poll — the request must
/// ALSO carry the `rtdb-oauth-state` cookie set at `/begin` with the same
/// value. Constant-time compare for hygiene; a missing/mismatched cookie
/// returns `Expired` (the caller already handles that indistinguishably from a
/// timed-out flow, and the legit parent — which loaded `/begin` — always has
/// the cookie).
async fn auth_state(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<StateQuery>,
) -> Response {
    let cookie_ok = crate::auth::cookie::oauth_state_cookie(&headers)
        .is_some_and(|c| bool::from(c.as_bytes().ct_eq(params.state.as_bytes())));
    if !cookie_ok {
        return Json(StateResponse::Expired).into_response();
    }
    match poll_login(&state, &params.state).await {
        PollResult::Pending => Json(StateResponse::Pending).into_response(),
        PollResult::Complete {
            token,
            user,
            cookie_mode,
        } => {
            let token = if cookie_mode { None } else { Some(token) };
            Json(StateResponse::Complete { token, user }).into_response()
        }
        PollResult::Failed => Json(StateResponse::Error).into_response(),
        PollResult::Expired => Json(StateResponse::Expired).into_response(),
    }
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

/// `POST /auth/state/revoke` `{state, revokeSecret}`: state AND revoke secret
/// together are the capability (only sha256(presented) is stored). Scoped to BEARER-delivery rows — a revoke
/// racing a default/cookie callback would make `set_outcome` delete a session
/// whose token `callback_close_response` was about to set as the browser's
/// `rtdb_session` cookie, replacing a live cookie with a dead one; non-bearer
/// rows answer the same `unknown`-shaped body as never-minted (no
/// mode-leaking distinction). Atomically transitions a live bearer row
/// (`pending`/`claiming`/`completed`) to `revoked` and deletes its tracked
/// session in ONE transaction — a crash or failure between the fence and the
/// delete rolls the fence back, leaving the row unconsumed so a retry re-runs
/// the whole cleanup.
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
/// `rtdb_session` — unlike `POST /auth/logout`, which always attaches
/// cookie-clearing headers (suppressing those when the request presented no
/// cookie is a separate, deferred follow-up card).
///
/// The route is unauthenticated (capability = state + secret), so it carries
/// the same SEC-003-style per-IP fixed-window guard as `/begin` (30 rpm,
/// tighter than begin's configurable ceiling) and a strict body-shape gate
/// BEFORE any transaction: bodies over 4 KiB, non-object JSON, non-string or
/// over-length `state`/`revokeSecret` fields, and limiter denials ALL answer
/// the same unknown-shaped 200 — garbage capabilities never reach the fence,
/// never open a transaction, and leak nothing about what failed.
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

async fn state_revoke(
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
            bool::from(
                crate::db::sha256_hex(presented)
                    .as_bytes()
                    .ct_eq(stored.as_bytes()),
            )
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
        let hash = crate::db::sha256_hex(&token);
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
    // server-side session row. SEC-106: clear the CSRF nonce with it.
    resp.headers_mut()
        .append(SET_COOKIE, crate::auth::cookie::clear_session_cookie());
    resp.headers_mut()
        .append(SET_COOKIE, crate::auth::cookie::clear_admin_csrf_cookie());
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

async fn anonymous(
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
        .route("/auth/enroll", post(crate::auth::enrollment::enroll))
        .route("/auth/state", get(auth_state))
        .route("/auth/state/revoke", post(state_revoke))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/validate", get(validate))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_access_token_returns_token_on_success() {
        let resp = json!({"access_token": "ya29.x", "token_type": "Bearer", "expires_in": 3599});
        assert_eq!(extract_access_token("google", &resp).unwrap(), "ya29.x");
    }

    #[test]
    fn extract_access_token_fails_on_error_body() {
        let resp = json!({"error": "invalid_grant", "error_description": "bad code"});
        assert!(extract_access_token("google", &resp).is_err());
    }

    #[test]
    fn extract_access_token_fails_when_access_token_absent() {
        // A response with other keys but no access_token still fails — never
        // admits on the strength of a malformed exchange.
        let resp = json!({"token_type": "Bearer", "expires_in": 3599});
        assert!(extract_access_token("oidc", &resp).is_err());
    }
}
