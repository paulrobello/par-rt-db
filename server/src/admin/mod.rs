//! The `/admin/*` control plane: the shared admin auth core (an
//! `AdminPrincipal` accepts the root admin key or an OAuth session on the
//! server-wide `rtdb_auth.admins` allowlist — cookie/CSRF mode for the
//! dashboard, bearer mode for the CLI/SDKs) plus the assembled admin router
//! over the per-domain submodules (login, dbs, schema_ops, tokens, docs,
//! schedules, storage_ops, webhooks, backups, settings, observability,
//! sessions, merge, workflows).

use std::sync::Arc;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, HeaderValue, Method};
use axum::middleware::{Next, from_fn, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use serde::Serialize;
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::{AppState, auth};

mod backups;
mod dbs;
mod docs;
mod login;
mod merge;
mod observability;
mod schedules;
mod schema_ops;
mod sessions;
mod settings;
mod storage_ops;
mod tokens;
mod webhooks;
mod workflows;

// Re-export each domain's handlers into the module scope so `admin_routes`
// below resolves them unqualified (e.g. `mint_token` -> `tokens::mint_token`)
// without editing the route table — every route stays byte-identical to the
// pre-split file. DTOs stay private to their own submodule.
use backups::*;
use dbs::*;
use docs::*;
use login::*;
use merge::*;
use observability::*;
use schedules::*;
use schema_ops::*;
use sessions::*;
use settings::*;
use storage_ops::*;
use tokens::*;
use webhooks::*;
use workflows::*;

/// Who an admin request was made as: the raw admin key (CLI/automation) or an
/// OAuth user on the server-wide admin allowlist (browser dashboard). The
/// `User` variant is unit today — admin activity is currently attributed only
/// through the op-feed's `owner` field (which is `None` for admin writes); if
/// per-principal audit logging is added later, thread the resolved `Principal`
/// back in here.
#[derive(Clone, Copy)]
pub(crate) enum AdminPrincipal {
    Key,
    User,
}

pub(super) fn bearer_value(headers: &HeaderMap) -> Result<&str, RtDbError> {
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Ok(v);
    }
    // SEC-001: dashboard cookie path. The browser sends the HttpOnly
    // `rtdb_session` cookie automatically on same-origin requests — including
    // the `/admin/stream` WS upgrade — so JS never holds the admin key. Header
    // still wins (CLI/automation/machine tokens).
    auth::cookie::session_cookie(headers)
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))
}

/// Bearer credential carried in a WebSocket subprotocol. Browsers cannot set
/// the `Authorization` header on a WS handshake, so the dashboard offers
/// `Sec-WebSocket-Protocol: rtdb-admin.<token>` instead (a header browsers CAN
/// set); this pulls the token back out. The subprotocol is an HTTP header during
/// the handshake — it never enters the URL, so it is not captured by access logs
/// the way a `?token=` query param would be.
pub(super) fn bearer_from_subprotocol(headers: &HeaderMap) -> Result<&str, RtDbError> {
    let proto = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))?;
    for entry in proto.split(',') {
        if let Some(rest) = entry.trim().strip_prefix("rtdb-admin.")
            && !rest.is_empty()
        {
            return Ok(rest);
        }
    }
    Err(RtDbError::unauthorized("missing admin bearer token"))
}

/// Authenticate a raw bearer credential as an admin: the admin key first
/// (constant-time compare, no DB lookup), then a hashed admin-key login session
/// (SEC-120 — the cookie path carries a session token, not the raw key), then a
/// resolved session/machine principal admitted only if it is an OAuth user on
/// `rtdb_auth.admins`. Shared by the header path and the WS-subprotocol path so
/// both enforce identically.
pub(crate) async fn authenticate_admin(
    state: &AppState,
    token: &str,
) -> Result<AdminPrincipal, RtDbError> {
    // SEC-110: defense-in-depth — never authenticate against an empty/whitespace
    // key. `Config::from_env` rejects this at boot, but if an empty key reaches
    // here (test harness constructing Config directly, stale config), this
    // short-circuit ensures `ct_eq(b"", b"")` cannot silently pass.
    if state.config.admin_key.trim().is_empty() {
        return Err(RtDbError::unauthorized("admin key not configured"));
    }
    if bool::from(token.as_bytes().ct_eq(state.config.admin_key.as_bytes())) {
        return Ok(AdminPrincipal::Key);
    }
    // SEC-120: the dashboard cookie now carries a hashed admin session token
    // (never the raw admin key). Resolve it against `rtdb_auth.admin_sessions`
    // — a valid, non-expired row admits the request as `AdminPrincipal::Key`
    // (same privilege tier as the raw key). Falls through to the OAuth path on
    // a miss so a non-admin session is still rejected with the right code.
    if auth::session::resolve_admin_session(&state.pool, token).await? {
        return Ok(AdminPrincipal::Key);
    }
    let principal = match auth::resolve_bearer(&state.pool, token).await {
        Ok(principal) => principal,
        Err(_) => return Err(RtDbError::unauthorized("invalid admin credential")),
    };
    if auth::is_admin(&state.pool, &principal).await {
        Ok(AdminPrincipal::User)
    } else {
        Err(RtDbError::forbidden("not a dashboard admin"))
    }
}

/// Admin gate for ordinary HTTP routes — reads `Authorization: Bearer <token>`.
pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AdminPrincipal, RtDbError> {
    authenticate_admin(state, bearer_value(headers)?).await
}

/// SEC-108: middleware that gates every admin route at the router layer instead
/// of relying on per-handler `require_admin(...)` calls (one omission = silent
/// auth bypass). `/admin/login` and `/admin/logout` are exempt (they mint/clear
/// credentials); `/admin/stream` is exempt (the WS upgrade authenticates inline
/// via `bearer_value`/`bearer_from_subprotocol`, the latter unreachable from
/// `require_admin`). The resolved `AdminPrincipal` is stashed in request
/// extensions for handlers that need the Key-vs-User distinction (e.g.
/// `admin_migrate`'s `evalExpr` gate, SEC-107).
///
/// ARC-013 follow-up: the protocol-version check runs first, ahead of both the
/// exemption list and auth, mirroring `http_api::authed` on `/api/*`. All four
/// SDKs send `X-Rtdb-Protocol` on admin calls, so a client declaring a version
/// this build cannot speak gets the same typed `UNSUPPORTED_PROTOCOL` (400)
/// here that it already gets on the data plane.
pub(super) async fn require_admin_mw(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    if let Err(e) = crate::http_api::check_protocol_version(req.headers()) {
        return e.into_response();
    }
    if matches!(
        req.uri().path(),
        "/admin/login" | "/admin/logout" | "/admin/stream"
    ) {
        return next.run(req).await;
    }
    let auth = {
        let headers = req.headers();
        require_admin(&state, headers).await
    };
    match auth {
        Ok(principal) => {
            // Mint the SEC-106 nonce for authenticated cookie-mode requests
            // that lack it (browsers logged in before the CSRF deploy) so the
            // next mutation can echo it. Computed before `req` moves into
            // `next`; appended to whatever response comes back.
            let heal = csrf_heal_cookie(
                req.headers(),
                state.config.cookie_secure,
                state.config.trusted_proxy,
            );
            req.extensions_mut().insert(principal);
            let mut response = next.run(req).await;
            if let Some(cookie) = heal
                && response.status().is_success()
            {
                response
                    .headers_mut()
                    .append(axum::http::header::SET_COOKIE, cookie);
            }
            response
        }
        Err(e) => e.into_response(),
    }
}

/// HTTP header name the dashboard echoes the admin-CSRF nonce back in
/// (SEC-106). Paired with the readable `rtdb-admin-csrf` cookie: a same-origin
/// script reads the cookie via `document.cookie` and sets this header on every
/// mutating admin request. A cross-site forge cannot read the cookie (different
/// origin) and so cannot set the header — the request is rejected before any
/// state changes. Requests authenticating with an explicit
/// `Authorization: Bearer` header skip the check (non-browser, non-ambient).
const ADMIN_CSRF_HEADER: &str = "x-rtdb-csrf";

/// True when this request carries a non-browser bearer credential — an explicit
/// `Authorization: Bearer …` header. Such requests do not rely on the ambient
/// session cookie, so the CSRF defense is moot for them (CLI/automation/machine
/// tokens). Keep this branch-first: `bearer_value` tries the header before the
/// cookie for the same reason.
fn has_explicit_bearer(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| !t.is_empty())
}

/// True when the `rtdb-admin-csrf` cookie and `X-Rtdb-Csrf` header are both
/// present and equal (constant-time compare). False otherwise (missing either,
/// or a mismatch). The constant-time compare closes a timing oracle that could
/// otherwise probe the nonce byte by byte.
fn admin_csrf_matches(headers: &HeaderMap) -> bool {
    let Some(cookie) = auth::cookie::admin_csrf_cookie(headers) else {
        return false;
    };
    let Some(header) = headers.get(ADMIN_CSRF_HEADER).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    if cookie.len() != header.len() {
        return false;
    }
    bool::from(cookie.as_bytes().ct_eq(header.as_bytes()))
}

/// True when the ambient `rtdb_session` cookie is present on this request —
/// i.e. it could be a cookie-authenticated browser request the CSRF defense
/// needs to gate. Absent cookie = non-browser or pre-login request; fall
/// through to the normal auth gate (401), don't 403.
fn has_session_cookie(headers: &HeaderMap) -> bool {
    auth::cookie::session_cookie(headers).is_some()
}

/// Self-heal for browsers whose login predates SEC-106: they hold a valid
/// session cookie but no `rtdb-admin-csrf` cookie (it did not exist when they
/// logged in), so every mutating admin request 403s forever. On an
/// authenticated cookie-mode request that lacks the nonce cookie, mint one on
/// the response — the dashboard's 20s `/admin/dbs` poll heals the browser
/// within one page load, and the next mutation carries the echoed header.
///
/// Security: minting requires an already-authenticated admin request, and the
/// nonce is independent random — a cross-site attacker who caused the mint
/// still cannot read the cookie (same-origin policy), so the double-submit
/// defense is unchanged. A MISMATCH (cookie and header both present, different
/// values) is never healed — that is the attack signal and stays a hard 403.
fn csrf_heal_cookie(
    headers: &HeaderMap,
    cookie_secure: bool,
    trusted_proxy: bool,
) -> Option<HeaderValue> {
    if !has_session_cookie(headers) || has_explicit_bearer(headers) {
        return None;
    }
    if auth::cookie::admin_csrf_cookie(headers).is_some() {
        return None;
    }
    let secure = cookie_secure || auth::cookie::request_is_secure(headers, trusted_proxy);
    auth::cookie::set_admin_csrf_cookie(&crate::db::random_token(), secure).ok()
}

/// Middleware: require the admin CSRF double-submit nonce on mutating
/// `/admin/*` requests when the credential is the ambient session cookie
/// (SEC-106). Skipped for:
/// - GET/HEAD/OPTIONS: no state change (read-only admin routes);
/// - `/admin/login` + `/admin/logout`: they mint/clear the cookies and so
///   cannot carry a nonce (login-CSRF on the OAuth flow has its own defense in
///   `auth/cookie.rs`); and logout's only effect is the cookie clear itself;
/// - Explicit `Authorization: Bearer` requests: non-browser, non-ambient;
/// - No session cookie at all: falls through to the normal admin auth gate
///   (401), so unauthenticated probes don't get a 403 that would mask the real
///   reason.
///
/// On a cookie-authenticated mutating request, the nonce header must match the
/// nonce cookie (constant-time) or the request is rejected with 403.
pub(super) async fn admin_csrf_guard(req: Request, next: Next) -> Response {
    let method = req.method();
    let path = req.uri().path();
    let is_mutating = matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    let is_auth_endpoint = path == "/admin/login" || path == "/admin/logout";
    if !is_mutating
        || is_auth_endpoint
        || has_explicit_bearer(req.headers())
        || !has_session_cookie(req.headers())
    {
        return next.run(req).await;
    }
    if !admin_csrf_matches(req.headers()) {
        return RtDbError::forbidden("missing or mismatched admin CSRF token").into_response();
    }
    next.run(req).await
}

#[derive(Serialize)]
pub(super) struct OkResponse {
    ok: bool,
}

/// Admin routes, gated at the router layer by `require_admin_mw` (SEC-108) and
/// the CSRF double-submit guard (SEC-106). `state` is threaded into the
/// auth middleware via `from_fn_with_state` so it can resolve `State<Arc<AppState>>`.
pub fn admin_routes(state: Arc<AppState>) -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/login", post(admin_login))
        .route("/admin/logout", post(admin_logout))
        .route("/admin/create-db", post(create_db))
        .route("/admin/delete-db", post(delete_db))
        .route("/admin/merge-users", post(merge_users_handler))
        .route("/admin/push-schema", post(push_schema))
        .route("/admin/dbs", get(list_dbs))
        .route("/admin/mint-token", post(mint_token))
        .route("/admin/revoke-token", post(revoke_token))
        .route(
            "/admin/allowlist",
            get(allowlist_list).post(allowlist_write),
        )
        .route(
            "/admin/admins",
            get(list_admins).post(add_admin).delete(remove_admin),
        )
        .route("/admin/dbs/{db}/schema", get(get_schema))
        .route("/admin/db/{db}/schema/preview", post(preview_schema))
        .route("/admin/db/{db}/schema/history", get(schema_history_list))
        .route(
            "/admin/db/{db}/schema/history/{version}",
            get(schema_history_get),
        )
        .route("/admin/dbs/{db}/stats", get(db_stats))
        .route("/admin/db/{db}/query", post(admin_query))
        .route("/admin/db/{db}/mutate", post(admin_mutate))
        .route("/admin/db/{db}/migrate", post(admin_migrate))
        .route("/admin/db/{db}/schema/restore", post(restore_schema))
        .route(
            "/admin/db/{db}/anonymous-access",
            get(get_anonymous_access).patch(patch_anonymous_access),
        )
        .route(
            "/admin/db/{db}/storage",
            get(admin_storage_list)
                .post(admin_storage_upload)
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/admin/db/{db}/storage/{id}", delete(admin_storage_delete))
        .route(
            "/admin/db/{db}/webhooks",
            get(admin_list_webhooks).post(admin_create_webhook),
        )
        .route(
            "/admin/db/{db}/webhooks/{id}",
            put(admin_edit_webhook).delete(admin_delete_webhook),
        )
        .route(
            "/admin/db/{db}/webhooks/{id}/deliveries",
            get(admin_list_deliveries),
        )
        .route(
            "/admin/db/{db}/schedules",
            get(admin_list_schedules).post(admin_create_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/cancel",
            post(admin_cancel_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/pause",
            post(admin_pause_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/resume",
            post(admin_resume_schedule),
        )
        .route(
            "/admin/db/{db}/workflows",
            get(admin_list_workflows).post(admin_create_workflow),
        )
        .route(
            "/admin/db/{db}/workflows/{id}/cancel",
            post(admin_cancel_workflow),
        )
        .route(
            "/admin/db/{db}/workflows/{id}/signal",
            post(admin_signal_workflow),
        )
        .route(
            "/admin/db/{db}/workflows/{id}",
            get(admin_get_workflow).delete(admin_delete_workflow),
        )
        .route("/admin/metrics", get(metrics_handler))
        // ENH-019: query introspection. `/explain` compiles a Query DSL body
        // for inspection (no execution); `/slow-queries` reads the bounded
        // slow-query log. Both are admin-gated at the router layer like every
        // other route in this table.
        .route("/admin/db/{db}/explain", post(admin_explain))
        .route("/admin/slow-queries", get(list_slow_queries))
        .route("/admin/config", get(get_config).patch(patch_config))
        .route("/admin/backup", post(create_backup))
        .route("/admin/backups", get(list_backups))
        .route(
            "/admin/backups/{name}",
            get(download_backup).delete(delete_backup),
        )
        .route("/admin/restore", post(restore_backup))
        .route("/admin/ops/recent", get(ops_recent))
        .route("/admin/audit", get(audit_recent))
        .route("/admin/subscriptions", get(list_subscriptions))
        .route("/admin/stream", get(admin_stream))
        .route("/admin/tokens", get(list_tokens))
        .route(
            "/admin/sessions",
            get(list_sessions_handler).delete(revoke_user_sessions_handler),
        )
        .route(
            "/admin/sessions/{token_hash}",
            delete(revoke_session_handler),
        )
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
        .route("/admin/clone-db", post(clone_db))
        // SEC-108: gate every admin route at the router layer. Login/logout/
        // stream are exempt (see `require_admin_mw`). Runs inside the CSRF
        // guard so a cookie-authenticated CSRF attack is caught first.
        .layer(from_fn_with_state(state, require_admin_mw))
        // SEC-106: require the admin-CSRF double-submit nonce on cookie-
        // authenticated mutating requests. Login/logout and bearer-authenticated
        // requests skip the check (see `admin_csrf_guard`).
        .layer(from_fn(admin_csrf_guard))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(
        cookie: Option<&str>,
        csrf_header: Option<&str>,
        bearer: Option<&str>,
    ) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(c) = cookie {
            h.insert(axum::http::header::COOKIE, c.parse().unwrap());
        }
        if let Some(csrf) = csrf_header {
            h.insert(ADMIN_CSRF_HEADER, csrf.parse().unwrap());
        }
        if let Some(b) = bearer {
            h.insert(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {b}").parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn has_explicit_bearer_true_when_authorization_present() {
        assert!(has_explicit_bearer(&headers_with(None, None, Some("tok"))));
    }

    #[test]
    fn has_explicit_bearer_false_when_absent() {
        assert!(!has_explicit_bearer(&headers_with(None, None, None)));
    }

    #[test]
    fn has_explicit_bearer_false_when_only_cookie() {
        let h = headers_with(Some("rtdb_session=abc"), None, None);
        assert!(!has_explicit_bearer(&h));
    }

    #[test]
    fn csrf_matches_when_cookie_equals_header() {
        let h = headers_with(Some("rtdb-admin-csrf=deadbeef"), Some("deadbeef"), None);
        assert!(admin_csrf_matches(&h));
    }

    #[test]
    fn csrf_rejects_when_header_missing() {
        let h = headers_with(Some("rtdb-admin-csrf=deadbeef"), None, None);
        assert!(!admin_csrf_matches(&h));
    }

    #[test]
    fn csrf_rejects_when_cookie_missing() {
        let h = headers_with(None, Some("deadbeef"), None);
        assert!(!admin_csrf_matches(&h));
    }

    #[test]
    fn csrf_rejects_when_values_differ() {
        let h = headers_with(Some("rtdb-admin-csrf=deadbeef"), Some("feedface"), None);
        assert!(!admin_csrf_matches(&h));
    }

    #[test]
    fn csrf_rejects_when_length_differs() {
        // Different lengths short-circuit before ct_eq (lengths must match for
        // a constant-time compare). Confirms the guard is sound under unequal
        // inputs, not leaking timing.
        let h = headers_with(Some("rtdb-admin-csrf=deadbeef"), Some("deadbee"), None);
        assert!(!admin_csrf_matches(&h));
    }

    #[test]
    fn session_cookie_detector_distinguishes_present_from_absent() {
        assert!(has_session_cookie(&headers_with(
            Some("rtdb_session=abc"),
            None,
            None
        )));
        assert!(!has_session_cookie(&headers_with(None, None, None)));
    }

    #[test]
    fn csrf_heal_mints_nonce_for_stale_cookie_browser() {
        let h = headers_with(Some("rtdb_session=abc"), None, None);
        let cookie = csrf_heal_cookie(&h, false, false)
            .expect("heal mints a nonce when only the session cookie is present");
        let value = cookie.to_str().unwrap();
        assert!(value.starts_with("rtdb-admin-csrf="));
        assert!(
            !value.contains("HttpOnly"),
            "dashboard JS must read the nonce"
        );
    }

    #[test]
    fn csrf_heal_skips_when_nonce_cookie_already_present() {
        let h = headers_with(Some("rtdb_session=abc; rtdb-admin-csrf=xyz"), None, None);
        assert!(csrf_heal_cookie(&h, false, false).is_none());
    }

    #[test]
    fn csrf_heal_skips_bearer_requests() {
        let h = headers_with(Some("rtdb_session=abc"), None, Some("tok"));
        assert!(csrf_heal_cookie(&h, false, false).is_none());
    }

    #[test]
    fn csrf_heal_skips_when_no_session_cookie() {
        let h = headers_with(None, None, None);
        assert!(csrf_heal_cookie(&h, false, false).is_none());
    }

    #[test]
    fn csrf_heal_adds_secure_attribute_when_proxied_https() {
        let mut h = headers_with(Some("rtdb_session=abc"), None, None);
        h.insert("x-forwarded-proto", "https".parse().unwrap());
        let cookie = csrf_heal_cookie(&h, false, true).unwrap();
        assert!(cookie.to_str().unwrap().contains("Secure"));
    }
}
