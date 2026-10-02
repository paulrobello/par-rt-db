//! One-shot HTTP API. Query and mutate routes route mutations through
//! `Committers::mutate` so subscriptions fire regardless of which transport
//! wrote (the WS handler in `ws` is the other transport, with one shared
//! vocabulary — see `protocol`). Also carries the storage upload/serve routes,
//! signed-URL minting, image-transform serving, the admin surface, and
//! per-machine-token / per-db rate limiting
//! (`RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`; over-limit →
//! 429 `RATE_LIMITED` + `Retry-After`).

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, FromRequest, Path, Request, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::AppState;
use crate::auth::{Principal, authorize, resolve_bearer};
use crate::error::RtDbError;

/// ARC-013: the request header carrying the client's wire protocol version.
/// Lowercase is load-bearing — `HeaderName::from_static` (used by the CORS
/// layer in `lib`) panics on any uppercase byte.
pub(crate) const PROTOCOL_HEADER: &str = "x-rtdb-protocol";

/// ARC-013: parse the optional `X-Rtdb-Protocol` request header and reject a
/// version newer than this build's `PROTOCOL_VERSION`. Absent or non-numeric
/// (never sent by a real client) is treated as version 1 — mirrors the WS
/// `Auth` frame's `protocolVersion` handling in `ws::authenticate`.
///
/// Called from `authed` for the per-db `/api/*` routes and from
/// `admin::require_admin_mw` for the `/admin/*` control plane, so a version
/// skew is diagnosable on both surfaces rather than only the data plane.
pub(crate) fn check_protocol_version(headers: &HeaderMap) -> Result<(), RtDbError> {
    let Some(requested) = headers
        .get(PROTOCOL_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok())
    else {
        return Ok(());
    };
    if requested > crate::protocol::PROTOCOL_VERSION {
        return Err(RtDbError::unsupported_protocol(format!(
            "requested protocol version {requested} is newer than this server's {}",
            crate::protocol::PROTOCOL_VERSION
        )));
    }
    Ok(())
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, RtDbError> {
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Ok(v);
    }
    // SEC-001: dashboard cookie path (HttpOnly `rtdb_session`). Same-origin
    // browser requests carry it automatically; CLI/SDK/machine tokens keep using
    // the Authorization header. Only session tokens resolve via `resolve_bearer`
    // (an admin-key cookie authenticates `/admin/*`, not these per-db routes).
    crate::auth::cookie::session_cookie(headers)
        .ok_or_else(|| RtDbError::unauthorized("missing bearer token"))
}

/// The per-db auth prologue shared by the HTTP query/mutate/manage handlers:
/// extract the bearer token, resolve it to a `Principal`, and authorize it for
/// `db`. Returns the resolved principal so the caller can run per-row checks,
/// rate-limit, or branch on `is_read_only()` — those stay at the call site
/// because they vary per handler. ARC-115: collapses the 11 copy-pasted
/// `bearer_token → resolve_bearer → authorize` triplets into one place.
async fn authed(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
) -> Result<Principal, RtDbError> {
    check_protocol_version(headers)?;
    let token = bearer_token(headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, db).await?;
    Ok(principal)
}

/// Like `axum::Json`, but maps deserialization failures (unknown fields,
/// unknown enum tags, malformed JSON) to the `RtDbError` wire envelope with
/// `BadRequest` (400) instead of axum's default split between 400 and 422.
/// Shared with `admin.rs` so malformed admin bodies get the same envelope.
pub(crate) struct ApiJson<T>(pub(crate) T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = RtDbError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) => Err(RtDbError::bad_request(rejection.to_string())),
        }
    }
}

/// HTTP one-shot routes, authorized via `Authorization: Bearer <token>`
/// (machine token or user session) resolved and checked per-request.
pub fn http_api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/query", post(query_handler))
        .route("/api/query-batch", post(batch_query_handler))
        .route("/api/mutate", post(mutate_handler))
        .route("/api/mutate-batch", post(mutate_batch_handler))
        .route("/api/schedule", post(schedule_handler))
        .route("/api/schedule/claim", post(claim_schedules_handler))
        .route(
            "/api/schedule/{id}/complete",
            post(complete_schedule_handler),
        )
        .route("/api/schedule/{id}/retry", post(retry_schedule_handler))
        .route("/api/schedule/{id}/fail", post(fail_schedule_handler))
        .route("/api/schedule/{id}/cancel", post(cancel_handler))
        .route("/api/schedule/{id}/pause", post(pause_handler))
        .route("/api/schedule/{id}/resume", post(resume_handler))
        .route("/api/schedules", post(list_schedules_handler))
        .route("/api/workflows", post(start_workflow_handler))
        .route("/api/workflows/list", post(list_workflows_handler))
        .route("/api/workflows/{id}/cancel", post(cancel_workflow_handler))
        .route("/api/workflows/{id}/signal", post(signal_workflow_handler))
        // The pushed SchemaDef for a db, for machine tokens — the admin twin is
        // `/admin/dbs/{db}/schema`. Serves tooling that holds only a data-plane
        // credential (the CLI's `import --dry-run` validates lines against it).
        .route("/api/db/{db}/schema", get(db_schema_handler))
        // The durable per-db change feed (seq-cursor polling for HTTP-only
        // clients). Machine tokens only; see the change-feed design spec.
        .route("/api/db/{db}/changes", get(changes_handler))
        // Upload bypasses axum's 2 MiB default body limit; `to_bytes` inside
        // the handler enforces `RTDB_MAX_FILE_SIZE` as the sole ceiling.
        .route(
            "/api/storage/{db}",
            post(upload_handler).layer(DefaultBodyLimit::disable()),
        )
        // Authed serve — bearer authorizes `{db}`; id must live in that db's
        // table (404 otherwise, enforcing cross-db isolation).
        .route(
            "/api/storage/{db}/{id}",
            get(serve_authed_handler).delete(delete_handler),
        )
        // Metadata — same auth + cross-db isolation as authed serve.
        .route("/api/storage/{db}/{id}/metadata", get(metadata_handler))
        // Mint a signed, time-limited URL — same auth as authed serve; the
        // holder fetches via `GET /storage/{id}?exp=&sig=` until expiry.
        .route("/api/storage/{db}/{id}/signed-url", get(signed_url_handler))
        // Public, unauthenticated serve — the one unauthenticated route in the
        // server, by design. The opaque id resolves to its owning db via the
        // global index.
        .route("/storage/{id}", get(serve_public_handler))
}

async fn db_schema_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<crate::schema::SchemaDef>, RtDbError> {
    authed(&state, &headers, &db).await?;
    let schema = state.schemas.get(&state.pool, &db).await?;
    Ok(Json((*schema).clone()))
}

/// Canonical per-IP rate-limit key for an unauthenticated request (SEC-112).
///
/// SEC-201: the forwarding headers below are only consulted when
/// `trusted_proxy` is true (`RTDB_TRUSTED_PROXY`) — i.e. the deploy sits
/// behind a reverse proxy that actually sets them. On a directly reachable
/// port they are caller-controlled, and trusting them would let an attacker
/// mint a fresh rate-limit bucket per request; the peer address wins
/// outright instead.
///
/// Order of preference (trusted-proxy deploys only):
/// 1. `CF-Connecting-IP` — set by the Cloudflare tunnel edge to the connecting
///    client. The deploy runs behind that tunnel, and CF appends (does not
///    replace) this header, so it is the most trustworthy identifier on the
///    route and not spoofable by the caller.
/// 2. **Rightmost** hop of `X-Forwarded-For`. Cloudflare appends the real
///    client IP to whatever chain the caller supplied, so the *leftmost* entry
///    is attacker-controlled and the *rightmost* is CF's observation. The prior
///    implementation took the leftmost and was therefore trivially bypassable
///    by varying the XFF header per request (SEC-004 recurrence).
/// 3. The connection's peer IP, for direct (non-tunneled) calls.
///
/// `Forwarded:` (RFC 7239) is intentionally NOT consulted — its `for=` value
/// has the same spoofing shape as XFF, and the prior implementation parsed it
/// as if it were a comma-separated IP list, which is simply wrong. CF-Connecting-IP
/// + XFF cover every observed deployment shape.
pub(crate) fn client_ip_key(
    headers: &HeaderMap,
    peer: std::net::IpAddr,
    trusted_proxy: bool,
) -> String {
    if !trusted_proxy {
        return peer.to_string();
    }
    if let Some(cf) = headers
        .get("cf-connecting-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return cf.to_string();
    }
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(last) = xff.split(',').map(str::trim).rfind(|s| !s.is_empty())
    {
        return last.to_string();
    }
    peer.to_string()
}

/// SEC-118: enforce per-row authorization on a stored blob. `blob_owner` is the
/// row's `owner_id` (`None` for system-initiated uploads and rows that predate
/// the column). The rule mirrors document per-row auth:
/// - `blob_owner == None` → allow (system-initiated; anyone authorized for the
///   db may touch it).
/// - `blob_owner == Some(_)` and the caller is a `Machine` token → allow
///   (machine bypass, same as document ownerField).
#[cfg(test)]
mod client_ip_tests {
    use super::client_ip_key;
    use axum::http::{HeaderMap, HeaderValue};

    fn peer() -> std::net::IpAddr {
        "203.0.113.99".parse().unwrap()
    }

    fn headers_with(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    // SEC-112: CF-Connecting-IP wins outright. A varying XFF with a constant
    // CF-Connecting-IP must share one bucket — the spoofable XFF entries are
    // ignored entirely when CF-Connecting-IP is present.
    #[test]
    fn cf_connecting_ip_wins_over_xff() {
        let h = headers_with(&[
            ("cf-connecting-ip", "198.51.100.10"),
            ("x-forwarded-for", "10.1.1.1, 10.2.2.2, 10.3.3.3"),
        ]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.10");
    }

    // SEC-112: the prior bug took the LEFTMOST XFF entry, which Cloudflare does
    // not set — it appends the real client as the RIGHTMOST hop. So under a
    // spoofed XFF like "fake-attacker-ip, real-client", the rightmost is what
    // Cloudflare observed.
    #[test]
    fn xff_takes_rightmost_hop_not_leftmost() {
        let h = headers_with(&[("x-forwarded-for", "10.4.4.4, 10.5.5.5, 198.51.100.20")]);
        assert_eq!(
            client_ip_key(&h, peer(), true),
            "198.51.100.20",
            "rightmost XFF hop is Cloudflare's observation"
        );
    }

    // SEC-112: a single-hop XFF (the common case) returns that hop.
    #[test]
    fn xff_single_hop() {
        let h = headers_with(&[("x-forwarded-for", "198.51.100.30")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.30");
    }

    // SEC-112: falling back to the connection peer when neither trusted header
    // is present (direct calls, no tunnel).
    #[test]
    fn falls_back_to_peer_when_no_proxy_headers() {
        let h = HeaderMap::new();
        assert_eq!(client_ip_key(&h, peer(), true), peer().to_string());
    }

    // SEC-112: empty/whitespace CF-Connecting-IP is ignored, not parsed as
    // the bucket key (which would create an empty-string bucket shared by
    // every malformed request).
    #[test]
    fn empty_cf_connecting_ip_falls_through() {
        let h = headers_with(&[
            ("cf-connecting-ip", "  "),
            ("x-forwarded-for", "198.51.100.40"),
        ]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.40");
    }

    // SEC-112: trailing/leading whitespace is trimmed so "1.2.3.4 " and
    // "1.2.3.4" share a bucket.
    #[test]
    fn cf_connecting_ip_is_trimmed() {
        let h = headers_with(&[("cf-connecting-ip", "  198.51.100.50  ")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.50");
    }

    // SEC-112: Forwarded (RFC 7239) is intentionally NOT consulted. The prior
    // implementation parsed it as a comma-list of IPs (wrong) and trusting it
    // reopens the spoofing vector. Confirm it is ignored in favor of peer.
    #[test]
    fn forwarded_header_is_ignored() {
        let h = headers_with(&[("forwarded", "for=198.51.100.99")]);
        assert_eq!(
            client_ip_key(&h, peer(), true),
            peer().to_string(),
            "Forwarded header must not be trusted — peer fallback wins"
        );
    }

    // SEC-201: with RTDB_TRUSTED_PROXY=false (the code default), the
    // forwarding headers are caller-controlled and must be ignored entirely —
    // the peer address wins, so header rotation cannot mint fresh rate-limit
    // buckets on a directly reachable deploy.
    #[test]
    fn untrusted_proxy_ignores_forwarding_headers() {
        let h = headers_with(&[
            ("cf-connecting-ip", "198.51.100.60"),
            ("x-forwarded-for", "10.6.6.6, 198.51.100.61"),
        ]);
        assert_eq!(
            client_ip_key(&h, peer(), false),
            peer().to_string(),
            "untrusted deploy must key on the peer address, not spoofable headers"
        );
    }

    // SEC-201: with RTDB_TRUSTED_PROXY=true the header path is active again
    // (the tests above all pass `true` — they cover the trusted-proxy path).
    #[test]
    fn trusted_proxy_reads_cf_connecting_ip() {
        let h = headers_with(&[("cf-connecting-ip", "198.51.100.70")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.70");
    }
}

mod changes;
mod data;
mod schedule;
mod storage;
mod workflow;

pub(crate) use changes::changes_handler;
pub(crate) use data::{batch_query_handler, mutate_batch_handler, mutate_handler, query_handler};
pub(crate) use schedule::{
    cancel_handler, claim_schedules_handler, complete_schedule_handler, fail_schedule_handler,
    list_schedules_handler, pause_handler, resume_handler, retry_schedule_handler,
    schedule_handler,
};
pub(crate) use storage::{
    delete_handler, metadata_handler, serve_authed_handler, serve_public_handler,
    signed_url_handler, upload_handler,
};
pub(crate) use workflow::{
    cancel_workflow_handler, list_workflows_handler, signal_workflow_handler,
    start_workflow_handler,
};
