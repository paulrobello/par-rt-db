//! The `OAuthProvider` trait and the dispatcher all six providers sit behind.
//! Carries the login-ledger invariants: the OAuth `state` token is single-use
//! (stored in `rtdb_auth.oauth_states`, so a login begun on one replica
//! completes on another), login-CSRF is a double-submit nonce cookie
//! constant-time-verified at `/callback`, and the anonymous mint is gated and
//! rate-limited. Bearer-delivery logins additionally mint a client-only
//! revoke secret at begin (delivered only in the begin response body; only
//! its sha256 verifier is persisted) — `POST /auth/state/revoke` requires
//! `{state, revokeSecret}` together, because `state` alone transits the
//! provider redirect URL and is not a revocation capability. The same
//! verifier is also stamped onto the session row at settlement, where
//! `POST /auth/session/revoke` `{sessionId, revokeSecret}` revokes the
//! minted session for its actual lifetime (bounded by the session TTL, not
//! the 10-minute state window). A lost begin
//! response means the flow was never navigated and the pending row simply
//! ages out via the TTL sweep — correct, nothing to clean.
//!
//! CLI bootstrap flow: when `/begin` is called by a non-browser client (a CLI
//! holding `state` + `revokeSecret`), the browser that eventually authorizes
//! carries none of the begin-minted cookies and the login-CSRF callback gate
//! rejects the flow structurally. `GET /auth/login?state=` (the begin
//! response's `loginUrl`, bearer delivery only) re-establishes the binding:
//! serving it mints the CSRF + poll cookies in the browser that opens it and
//! redirects to the provider authorize URL. The initiating CLI then polls
//! `/auth/state` presenting the revoke secret as a header — the same
//! `{state, revokeSecret}` capability `/auth/state/revoke` requires.
//!
//! ARC-003 layout: the route handlers live in the `begin`, `callback`,
//! `revoke`, and `session` submodules (the committer/arms pattern), and
//! Apple's `form_post` callback lives in `auth/apple.rs`. This file keeps
//! the `OAuthProvider` trait, the shared login-ledger state machinery
//! (`claim_pending`/`set_outcome`/`poll_login` and friends), the shared
//! OIDC token-exchange helper (ARC-114), and `auth_routes()`.

use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use serde::Serialize;

use crate::AppState;
use crate::auth::session as session_machinery;
use crate::auth::{Principal, authed_user, resolve_bearer};
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

mod begin;
pub(crate) mod callback;
mod revoke;
mod session;

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
pub(super) const DELIVERY_BEARER: &str = "bearer";

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
pub(super) fn unconfigured_response(name: &str) -> Response {
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
/// mode, which the callback honors when closing the popup, and the row's
/// begin-minted revoke-secret verifier, which the callback stamps onto the
/// session row as the durable capability hash (bearer deliveries only).
pub(super) async fn claim_pending(
    state: &Arc<AppState>,
    state_token: &str,
    expected_provider: &'static str,
) -> Option<(Option<String>, String, Option<String>)> {
    let now = now_ms();
    let row: Option<(Option<String>, String, Option<String>)> = sqlx::query_as(
        "UPDATE rtdb_auth.oauth_states SET status = $1 \
         WHERE state = $2 AND provider = $3 AND status = $4 AND expires_at > $5 \
         RETURNING anon_user_id, delivery, revoke_secret_verifier",
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
pub(super) async fn merge_anon_into_real(
    state: &Arc<AppState>,
    anon_id: &str,
    session_token: &str,
) {
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
pub(super) async fn set_outcome(state: &Arc<AppState>, state_token: &str, completed: Option<&str>) {
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
pub(super) async fn roll_forward_into_revocation(
    state: &Arc<AppState>,
    state_token: &str,
    token: &str,
) {
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
            if let Err(err) = session_machinery::purge_session_row(&state.pool, token).await {
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
            if let Err(err) = session_machinery::purge_session_row(&state.pool, token).await {
                tracing::warn!(error = %err, "oauth: failed to delete a session minted after a swept revoke");
            }
        }
        Err(err) => {
            // Marker query itself failed — fall through to the delete so the
            // fence's cleanup intent still applies. Settlement stays unset, so
            // retries keep answering `unconfirmed` (honest: untracked debt).
            tracing::warn!(error = %err, "oauth: roll-forward marker write failed");
            if let Err(derr) = session_machinery::purge_session_row(&state.pool, token).await {
                tracing::warn!(error = %derr, "oauth: failed to delete a session after a failed marker");
            }
        }
    }
}

/// Bearer deliveries stamp the durable session-revoke capability onto the
/// minted session BEFORE the terminal outcome is recorded — the one-shot poll
/// delivers `sessionId`, so it must exist by poll time. `session_id` is a
/// fresh random identifier; the begin-time verifier becomes the stored hash
/// `POST /auth/session/revoke` compares against. Failure is non-fatal (the
/// session works; it is just not capability-revocable — the same posture as
/// every pre-existing session) and the poll then simply omits `sessionId`.
pub(super) async fn stamp_session_capability(state: &Arc<AppState>, token: &str, verifier: &str) {
    let session_id = random_token();
    if let Err(err) =
        session_machinery::stamp_revoke_capability(&state.pool, token, &session_id, verifier).await
    {
        tracing::warn!(error = %err, "oauth: failed to stamp the session revoke capability");
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
        /// The durable capability identifier stamped at settlement (bearer
        /// logins only); `None` when the session predates the feature or the
        /// stamp failed.
        session_id: Option<String>,
        /// The capability's provable expiry (the session's absolute
        /// `expires_at`, bearer logins only): the instant the tombstone is
        /// swept, bounding the retry's evidentiary window.
        expires_at: Option<i64>,
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
                    Ok(principal @ Principal::User { .. }) => {
                        // The capability identifier rides the same one-shot
                        // delivery as the token; a session without a stamped
                        // capability (cookie/default, pre-upgrade) omits both
                        // halves — `sessionId` and the provable `expiresAt`.
                        let capability =
                            session_machinery::lookup_session_capability(&state.pool, &t)
                                .await
                                .unwrap_or(None);
                        let (session_id, expires_at) = capability
                            .map(|(sid, exp)| (Some(sid), Some(exp)))
                            .unwrap_or((None, None));
                        PollResult::Complete {
                            token: t,
                            user: authed_user(&principal),
                            cookie_mode: delivery == DELIVERY_COOKIE,
                            session_id,
                            expires_at,
                        }
                    }
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

/// OAuth + session routes. SEC-012: each provider mounts a `begin` endpoint
/// that returns `{authorizeUrl, state}` (the parent opens it in a `noopener`
/// popup and polls `/auth/state`); the callback handler is generic over the
/// provider, mounted per-provider — GitHub keeps `/auth/callback`, Google and
/// GitLab use `/<provider>/callback`. `/auth/state`, `/auth/logout`, and
/// `/auth/me` are provider-agnostic.
pub fn auth_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/auth/github/begin",
            get(begin::provider_begin::<GithubProvider>),
        )
        .route(
            "/auth/callback",
            get(callback::provider_callback::<GithubProvider>),
        )
        .route(
            "/auth/google/begin",
            get(begin::provider_begin::<GoogleProvider>),
        )
        .route(
            "/auth/google/callback",
            get(callback::provider_callback::<GoogleProvider>),
        )
        .route(
            "/auth/gitlab/begin",
            get(begin::provider_begin::<GitlabProvider>),
        )
        .route(
            "/auth/gitlab/callback",
            get(callback::provider_callback::<GitlabProvider>),
        )
        .route(
            "/auth/oidc/begin",
            get(begin::provider_begin::<OidcProvider>),
        )
        .route(
            "/auth/oidc/callback",
            get(callback::provider_callback::<OidcProvider>),
        )
        .route(
            "/auth/microsoft/begin",
            get(begin::provider_begin::<MicrosoftProvider>),
        )
        .route(
            "/auth/microsoft/callback",
            get(callback::provider_callback::<MicrosoftProvider>),
        )
        .route(
            "/auth/apple/begin",
            get(begin::provider_begin::<AppleProvider>),
        )
        .route("/auth/apple/callback", post(super::apple::apple_callback))
        .route("/auth/anonymous", post(session::anonymous))
        .route("/auth/enroll", post(crate::auth::enrollment::enroll))
        .route("/auth/login", get(begin::login_bootstrap))
        .route("/auth/state", get(revoke::auth_state))
        .route("/auth/state/revoke", post(revoke::state_revoke))
        .route("/auth/session/revoke", post(session::session_revoke))
        .route("/auth/logout", post(session::logout))
        .route("/auth/me", get(session::me))
        .route("/auth/validate", get(session::validate))
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
