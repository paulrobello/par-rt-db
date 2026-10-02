//! Storage handlers: upload, authed serve/delete/metadata, signed-URL
//! minting, public serve, Range handling, and image-transform serving
//! (ARC-008 split; pure move from the former single-file `http_api.rs`).

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;

use crate::AppState;
use crate::http_api::{authed, client_ip_key};
use axum::Json;
use axum::body::Body;
use axum::extract::{ConnectInfo, Path, Query as AxumQuery, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;

use crate::auth::Principal;
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::image_transform::{Resolved, TransformParams};
use crate::rate_limit::{check_http_rate_limits, check_storage_public_rate_limit};
use crate::signed_url;
use crate::storage;
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UploadResponse {
    id: String,
    sha256: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

pub(crate) async fn upload_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    request: Request,
) -> Result<Json<UploadResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &db).await?;
    storage::ensure_table(&state.pool, &db).await?; // revive storage for old dbs

    // `max_file_size` is admin-mutable via PATCH /admin/config; clamp to the
    // compile-time HARD_MAX_FILE_SIZE so a misconfigured persisted row (or a
    // compromised admin token) cannot accept arbitrarily large uploads. The
    // bearer is already authorized above; clamp ordering preserves the
    // auth-before-buffering invariant (SEC-008). ENH-021: the streaming upload
    // path enforces this incrementally as bytes arrive (rejecting the moment
    // the running total crosses the line) rather than buffering the whole body
    // first, so this is now a disk-quota/DoS guard rather than a memory guard.
    let limit = crate::config::HARD_MAX_FILE_SIZE.min(state.runtime.hot.load().max_file_size);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    // SEC-118: stamp the uploading user's user_id onto the row so the authed
    // serve/delete/metadata routes can enforce per-row authorization. Machine
    // tokens and admin uploads stay NULL (system-initiated) — matching how
    // `ownerField` treats system writes.
    let owner_id = match &principal {
        Principal::User { user_id, .. } => Some(user_id.as_str()),
        Principal::Machine { .. } => None,
    };
    // ENH-021: stream the request body straight into the chunked storage path.
    // 1 MiB at a time is resident — N concurrent uploads is N × 1 MiB, not N ×
    // filesize. The quota check closure runs on every chunk so an over-cap
    // upload aborts mid-stream and commits nothing (no orphaned chunks). The
    // size check (against `limit`) is enforced inside `put_stream` on every
    // chunk arrival.
    // ENH-011 / ENH-021: per-db storage cap, enforced incrementally during the
    // streaming upload. Sample the cached `used` once at the start (TTL-bounded,
    // measure-on-miss — the per-db warmer keeps it fresh between uploads); the
    // per-chunk check is then a pure in-memory `used + running > cap` so the hot
    // path does no DB work per chunk. A concurrent upload can race this sample,
    // but the post-upload cache refresh + the next request's check bound the
    // overshoot. Aborting mid-stream commits nothing (the txn rolls back).
    let storage_cap = state.runtime.hot.load().max_storage_bytes_per_db;
    let baseline_used = if storage_cap > 0 {
        state
            .limits
            .quotas
            .current_usage(&state.pool, &db, state.config.quota_cache_ttl_secs)
            .await?
    } else {
        0
    };
    let quota_db = db.clone();
    let quota_metrics = state.runtime.metrics.clone();
    let quota_check = move |running: u64| {
        let db = quota_db.clone();
        let metrics = quota_metrics.clone();
        async move {
            if storage_cap == 0 {
                return Ok(());
            }
            if baseline_used + running > storage_cap {
                metrics.record_quota_rejection(&db, crate::metrics::QuotaKind::Storage);
                return Err(RtDbError::quota_exceeded(format!(
                    "upload would exceed storage quota for db '{db}' ({baseline_used} used, \
                     +{running} in flight, limit {storage_cap})"
                )));
            }
            Ok(())
        }
    };
    let body_stream = request.into_body().into_data_stream();
    let result = storage::put_stream(
        &state.pool,
        &db,
        content_type.as_deref(),
        owner_id,
        limit as u64,
        quota_check,
        body_stream,
    )
    .await?;
    // ENH-011: best-effort post-upload refresh of the storage cache so the next
    // check sees fresh bytes. Fire-and-forget — mirrors the committer refresh
    // spawn; a failure here just leaves the entry stale (TTL-bounded self-heal).
    {
        let quotas = state.limits.quotas.clone();
        let pool = state.pool.clone();
        let db = db.clone();
        tokio::spawn(async move {
            let _ = quotas.refresh(&pool, &db).await;
        });
    }
    state.runtime.metrics.record_upload();
    Ok(Json(UploadResponse {
        id: result.id,
        sha256: result.sha256,
        size: result.size,
        content_type,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignedUrlResponse {
    url: String,
    expires_at: i64,
}

/// Mint a signed, time-limited URL for `{id}`. Same auth as authed serve
/// (`bearer → authorize(db)`); the returned URL is fetched via
/// `GET /storage/{id}?exp=&sig=` until `expiresAt`. Minting is pure
/// computation — no DB write, no committer.
///
/// SEC-113: `{id}` must belong to `{db}` — a caller authorized for db A cannot
/// mint a signed URL for a blob that lives in db B. The mint is the
/// capability-granting step, so it is the right place to scope; the public
/// serve route is unauthenticated by design. Cross-db mismatch returns 404
/// (matching the authed-serve behavior for a foreign id) rather than 403, so
/// the existence of an id in another db is not disclosed.
///
/// SEC-003: image-transform params supplied on the mint request (`w`, `h`,
/// `q`, `fit`, `format`) are bound into the signature and echoed into the
/// returned URL, so one signature authorizes exactly one render. A mint with no
/// transform params yields a URL valid only for the un-transformed blob.
pub(crate) async fn signed_url_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    AxumQuery(q): AxumQuery<HashMap<String, String>>,
) -> Result<Json<SignedUrlResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    // SEC-113: resolve the owning db and reject cross-db. A caller authorized
    // for db A must not be able to mint a URL for an id that lives in db B,
    // even if they somehow know the id — the mint is the capability.
    let owner_db = storage::resolve_db(&state.pool, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    if owner_db != db {
        return Err(RtDbError::not_found("unknown file"));
    }
    let ttl = q
        .get("ttlSeconds")
        .and_then(|v| v.parse::<i64>().ok())
        .map(|v| v.clamp(1, signed_url::MAX_SIGNED_URL_TTL_SECS as i64) as u64)
        .unwrap_or(signed_url::DEFAULT_SIGNED_URL_TTL_SECS);
    let exp = now_ms() + (ttl as i64) * 1000;
    // Canonicalize whatever render was requested; `None` (no transform params)
    // canonicalizes to the empty string, which is what the serve path computes
    // for a plain fetch.
    let transform = TransformParams::parse(&q, state.limits.image.cfg())?
        .map(|p| p.canonical())
        .unwrap_or_default();
    let sig = signed_url::sign(&state.limits.signed_url_key, &id, exp, &transform);
    let base = state.config.public_url.trim_end_matches('/');
    let url = if transform.is_empty() {
        format!("{base}/storage/{id}?exp={exp}&sig={sig}")
    } else {
        format!("{base}/storage/{id}?{transform}&exp={exp}&sig={sig}")
    };
    Ok(Json(SignedUrlResponse {
        url,
        expires_at: exp,
    }))
}

/// Public, unauthenticated serve: anyone with the URL fetches the bytes. The
/// opaque id resolves to its owning db via the global index. Query params, if
/// present, request an on-the-fly image transform (ENH-014). Rate-limited per
/// client IP (SEC-004 / SEC-112) when `RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM > 0`;
/// off by default.
///
/// SEC-113: when `RTDB_STORAGE_REQUIRE_SIGNED_URLS=true`, every request must
/// carry a complete, valid `?exp=&sig=` pair — the opaque id alone is no longer
/// sufficient. The default (false) preserves today's behavior: opaque bearer
/// URLs are a deliberate Convex-parity feature. Either way, a request that
/// supplies `exp` or `sig` is signed-URL-shaped and must verify completely.
pub(crate) async fn serve_public_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<std::net::SocketAddr>,
    Path(id): Path<String>,
    AxumQuery(q): AxumQuery<HashMap<String, String>>,
) -> Result<Response, RtDbError> {
    check_storage_public_rate_limit(
        &state,
        &client_ip_key(&headers, addr.ip(), state.config.trusted_proxy),
    )
    .await?;
    let has_exp = q.contains_key("exp");
    let has_sig = q.contains_key("sig");
    // SEC-113: require-signature mode flips the default — without a complete,
    // valid signature, the request is rejected before resolving the blob.
    if state.config.storage.require_signed_urls && !(has_exp && has_sig) {
        return Err(RtDbError::forbidden("signed url required"));
    }
    // Additive signed-URL verification: if either `exp` or `sig` is present,
    // the request is signed-URL-shaped and must supply a complete, valid HMAC
    // signature that has not expired. If neither param is present (only
    // reachable when require-signed mode is off), behavior is unchanged
    // (public by opaque id) — `?sig=` alone is treated as a malformed signed
    // URL, never as a public fetch.
    if has_exp || has_sig {
        let exp_s = q
            .get("exp")
            .ok_or_else(|| RtDbError::forbidden("invalid or expired signature"))?;
        let sig = q
            .get("sig")
            .ok_or_else(|| RtDbError::forbidden("invalid or expired signature"))?;
        let exp: i64 = exp_s
            .parse()
            .map_err(|_| RtDbError::forbidden("invalid or expired signature"))?;
        if now_ms() > exp {
            return Err(RtDbError::forbidden("invalid or expired signature"));
        }
        // SEC-003: verify against the render this request actually asks for.
        // A signature minted for `w=100` does not authorize `w=200` or a
        // full-resolution fetch.
        let transform = TransformParams::parse(&q, state.limits.image.cfg())?
            .map(|p| p.canonical())
            .unwrap_or_default();
        // The transform kill switch widens a signature's scope if ignored here:
        // `serve_bytes` skips the transform when `enabled` is false and hands
        // back the ORIGINAL bytes, while `canonical()` — derived from `parse`
        // alone — still verifies. A signature minted for `w=100` would then
        // authorize the full-resolution blob. Reject the signed request instead;
        // the unsigned kill-switch path (public by opaque id) is unchanged.
        if !transform.is_empty() && !state.limits.image.cfg().enabled {
            return Err(RtDbError::forbidden("invalid or expired signature"));
        }
        if !signed_url::verify(&state.limits.signed_url_key, &id, exp, &transform, sig) {
            return Err(RtDbError::forbidden("invalid or expired signature"));
        }
    }
    let db = storage::resolve_db(&state.pool, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    serve_bytes(&state, &db, &id, &q, range).await
}

/// - `blob_owner == Some(owner)` and the caller is a `User` whose `user_id`
///   matches → allow.
/// - Otherwise → `Forbidden`. Admin reaches storage via the `/admin/*` routes
///   (which bypass per-row rules through `PrincipalCtx::bypass()`), not here.
fn enforce_blob_owner(principal: &Principal, blob_owner: &Option<String>) -> Result<(), RtDbError> {
    match (blob_owner, principal) {
        (None, _) => Ok(()),
        (Some(_), Principal::Machine { .. }) => Ok(()),
        (Some(owner), Principal::User { user_id, .. }) if owner == user_id => Ok(()),
        _ => Err(RtDbError::forbidden("not the blob owner")),
    }
}

/// Authed serve: the caller's principal must be authorized for `{db}`; the id
/// must live in that db's table (404 otherwise — cross-db isolation). Query
/// params, if present, request an on-the-fly image transform (ENH-014).
///
/// SEC-118: per-row authorization runs after `authorize(db)` — fetch the row's
/// `owner_id` via `get_meta` (cheap; no bytea) and enforce before serving.
pub(crate) async fn serve_authed_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
    AxumQuery(q): AxumQuery<HashMap<String, String>>,
) -> Result<Response, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    // SEC-118: per-row owner check. Unknown id → 404 (matches today's
    // `serve_bytes` behavior for a missing blob).
    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    enforce_blob_owner(&principal, &meta.owner_id)?;
    let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
    serve_bytes(&state, &db, &id, &q, range).await
}

async fn serve_bytes(
    state: &Arc<AppState>,
    db: &str,
    id: &str,
    q: &HashMap<String, String>,
    range_header: Option<&str>,
) -> Result<Response, RtDbError> {
    // Immutable: serve URLs are opaque ids (no enumeration), and any change to
    // a stored blob produces a fresh id, so a cached response is always valid.
    const IMMUTABLE: &str = "public, max-age=31536000, immutable";
    // Parse params (None ⇒ passthrough); honor the enabled kill switch.
    let params = TransformParams::parse(q, state.limits.image.cfg())?;
    let resolved = match params {
        None => None,
        Some(_) if !state.limits.image.cfg().enabled => None,
        Some(p) => Some(
            state
                .limits
                .image
                .get_or_transform(&state.pool, db, id, p)
                .await?,
        ),
    };
    // Range requests apply only to plain blob fetches (no transform params).
    // Transformed images are cache-keyed as whole renders, so a Range header on
    // them is ignored and `Accept-Ranges` is not advertised.
    let supports_range = resolved.is_none();
    // SEC-123: when a Range header is present on a plain blob fetch, resolve
    // the range against `octet_length(bytes)` first (cheap — no bytea crosses
    // the wire), then fetch ONLY the requested slice via `substring(bytes FROM
    // ... FOR ...)` (legacy inline) or the covering chunk span (ENH-021). A
    // Range request on a multi-GB blob must not load the whole bytea into
    // server memory just to slice it.
    if supports_range {
        let raw_range = range_header.map(str::trim).filter(|s| !s.is_empty());
        if let Some(raw_range) = raw_range {
            return build_range_response(&state.pool, db, id, raw_range, IMMUTABLE).await;
        }
    }
    match resolved {
        Some(Resolved::Transformed(cached)) => build_serve_response(
            cached.bytes.to_vec(),
            cached.content_type,
            IMMUTABLE,
            supports_range,
            range_header,
        ),
        Some(Resolved::Raw {
            bytes,
            content_type,
        }) => build_serve_response(
            bytes.to_vec(),
            &content_type,
            IMMUTABLE,
            supports_range,
            range_header,
        ),
        None => {
            // Plain non-range serve. ENH-021: for a chunked blob, stream the
            // chunks straight to the HTTP body via `Body::from_stream` so a 1
            // GiB download never holds more than ~1 chunk (1 MiB) in memory at
            // a time. Legacy inline blobs still reassemble the bytea (they have
            // no chunk rows; the whole column is one TOAST row either way).
            let probe = storage::probe_layout(&state.pool, db, id)
                .await?
                .ok_or_else(|| RtDbError::not_found("unknown file"))?;
            let content_type = probe
                .1
                .unwrap_or_else(|| "application/octet-stream".to_string());
            let (served_ct, force_attachment) = resolve_served_content_type(&content_type);
            let disposition = if force_attachment {
                "attachment"
            } else {
                "inline"
            };
            match probe.0 {
                storage::BlobLayout::Chunked => {
                    // Stream chunks directly — no materialization. The
                    // `stream_chunks` stream spawns a task that holds one
                    // connection from the pool for the lifetime of the response;
                    // axum drives the body stream to completion (and on client
                    // disconnect the receiver drops, the sender errors, and the
                    // task exits early — the connection returns to the pool).
                    let chunk_stream =
                        storage::stream_chunks(state.pool.clone(), db.to_string(), id.to_string());
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, served_ct)
                        .header(header::CACHE_CONTROL, IMMUTABLE)
                        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
                        .header(header::CONTENT_DISPOSITION, disposition)
                        .header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES)
                        .body(Body::from_stream(chunk_stream))
                        .map_err(|err| {
                            tracing::error!(error = %err, "failed to build streaming serve response");
                            RtDbError::internal(
                                "failed to build streaming serve response; see server logs",
                            )
                        })
                }
                storage::BlobLayout::Inline => {
                    // Legacy inline bytea — fetch and serve as one buffer. The
                    // serve path for an inline blob was always whole-body; this
                    // matches pre-ENH-021 behavior.
                    let (raw, _) = storage::get(&state.pool, db, id)
                        .await?
                        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
                    build_serve_response(
                        raw.to_vec(),
                        &content_type,
                        IMMUTABLE,
                        supports_range,
                        range_header,
                    )
                }
            }
        }
    }
}

/// SEC-123: builds the response for a `Range:` request against a stored blob by
/// fetching ONLY the requested byte slice from Postgres (`substring(...)`) — the
/// whole bytea is never materialized in server memory. The total resource size
/// is resolved cheaply up front via `octet_length(bytes)`; then the byte slice
/// fetch uses `substring(bytes FROM $start FOR $len)` for the `Partial`
/// outcome, fetches nothing for `Unsatisfiable`, and falls through to the
/// whole-blob path for `Full` (a malformed/unsupported Range the server is
/// entitled to ignore per RFC 7233).
async fn build_range_response(
    pool: &sqlx::PgPool,
    db: &str,
    id: &str,
    raw_range: &str,
    cache_control: &'static str,
) -> Result<Response, RtDbError> {
    // Cheap total via octet_length — does not materialize the bytea.
    let total = match storage::total_bytes(pool, db, id).await? {
        Some(t) => t,
        None => return Err(RtDbError::not_found("unknown file")),
    };
    let outcome = resolve_byte_range(Some(raw_range), total);
    match outcome {
        RangeOutcome::Full => {
            // Range was malformed/unsupported — RFC 7233 says ignore and serve
            // the full body. The whole bytea must be loaded here.
            let (bytes, content_type) = match storage::get(pool, db, id).await? {
                Some(t) => t,
                None => return Err(RtDbError::not_found("unknown file")),
            };
            let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
            // Pass `range_header=None` so `build_serve_response` does not try
            // to re-resolve (it would, to Full again, but this is clearer).
            build_serve_response(bytes.to_vec(), &ct, cache_control, true, None)
        }
        RangeOutcome::Partial { start, end } => {
            // Fetch ONLY the requested slice — the whole bytea stays in Postgres.
            let (slice, content_type) = match storage::get_range(pool, db, id, start, end).await? {
                Some(t) => t,
                None => return Err(RtDbError::not_found("unknown file")),
            };
            let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
            let (served_ct, force_attachment) = resolve_served_content_type(&ct);
            let disposition = if force_attachment {
                "attachment"
            } else {
                "inline"
            };
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, served_ct)
                .header(header::CACHE_CONTROL, cache_control)
                .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
                .header(header::CONTENT_DISPOSITION, disposition)
                .header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{total}"),
                )
                .header(header::CONTENT_LENGTH, slice.len() as u64)
                .body(Body::from(slice))
                .map_err(|err| {
                    tracing::error!(error = %err, "failed to build range response");
                    RtDbError::internal("failed to build range response; see server logs")
                })
        }
        RangeOutcome::Unsatisfiable => Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES)
            .header(header::CONTENT_RANGE, format!("bytes */{total}"))
            .body(Body::empty())
            .map_err(|err| {
                tracing::error!(error = %err, "failed to build 416 response");
                RtDbError::internal("failed to build 416 response; see server logs")
            }),
    }
}

/// The range unit advertised on responses that honor `Range` requests.
const ACCEPT_RANGES_BYTES: &str = "bytes";

/// Content types safe to serve inline from the storage routes (SEC-101).
/// Anything not on this list — including `text/html`, `image/svg+xml` (SVG
/// executes script in a browsing context), and any `application/*` script type
/// — is forced to `application/octet-stream` with `Content-Disposition:
/// attachment` so a stored blob can never become an attacker-authored page on
/// the console's own origin. Applied at read time in `build_serve_response` so
/// it covers existing rows (uploaded before this fix) too.
const INLINE_SAFE_CONTENT_TYPES: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/avif",
    "application/pdf",
    "text/plain",
];

/// Resolves a stored `content_type` to the value + disposition to actually
/// serve (SEC-101). Returns `(served_content_type, attachment)`: when the
/// stored type is on the inline-safe allowlist it is preserved and served
/// inline; everything else is downgraded to `application/octet-stream` and
/// served as an attachment. `text/html`, `image/svg+xml`, and script-bearing
/// `application/*` types are the threat this closes — same-origin stored XSS
/// on the admin console. The check ignores parameters (`;charset=...`), so a
/// stored `text/plain; charset=utf-8` stays inline.
fn resolve_served_content_type(stored: &str) -> (&'static str, bool) {
    let ess = stored
        .split(';')
        .next()
        .unwrap_or(stored)
        .trim()
        .to_ascii_lowercase();
    // Return the canonical static slice matching the allowlist entry so the
    // header value borrows 'static — case-normalized.
    match INLINE_SAFE_CONTENT_TYPES
        .iter()
        .copied()
        .find(|safe| *safe == ess)
    {
        Some(canonical) => (canonical, false),
        None => ("application/octet-stream", true),
    }
}

/// Resolved outcome of a `Range: bytes=...` request against a blob of `total`
/// bytes (RFC 7233).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeOutcome {
    /// No range, or an unsupported/malformed range the server ignores → 200.
    Full,
    /// A satisfiable single range, inclusive `start..=end` (already clamped).
    Partial { start: u64, end: u64 },
    /// A syntactically valid range whose start is at or past the end → 416.
    Unsatisfiable,
}

/// Parse a single `Range: bytes=...` header for a resource of `total` bytes.
/// Non-`bytes` units, multipart (multi-range) requests, and malformed specs are
/// ignored → [`RangeOutcome::Full`] (a 200 with the full body): RFC 7233 §2.1
/// permits an origin server to ignore a Range header it does not support. Only a
/// range whose start is at or past the resource end is unsatisfiable → 416.
fn resolve_byte_range(raw: Option<&str>, total: u64) -> RangeOutcome {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return RangeOutcome::Full;
    };
    let Some(spec) = raw.strip_prefix("bytes=") else {
        return RangeOutcome::Full; // unsupported range unit → ignore
    };
    let spec = spec.trim();
    if spec.contains(',') {
        return RangeOutcome::Full; // multipart not supported → ignore
    }
    let Some(dash) = spec.find('-') else {
        return RangeOutcome::Full; // no '-' → malformed, ignore
    };
    let (start_s, end_s) = (&spec[..dash], &spec[dash + 1..]);

    let (start, end) = if start_s.is_empty() {
        // Suffix `-N` = the last N bytes.
        let Ok(n) = end_s.parse::<u64>() else {
            return RangeOutcome::Full;
        };
        if n == 0 || total == 0 {
            return RangeOutcome::Unsatisfiable;
        }
        (total - n.min(total), total - 1)
    } else {
        let Ok(start) = start_s.parse::<u64>() else {
            return RangeOutcome::Full;
        };
        if total == 0 || start >= total {
            return RangeOutcome::Unsatisfiable;
        }
        let end = if end_s.is_empty() {
            total - 1 // `start-` → through the end
        } else {
            let Ok(end) = end_s.parse::<u64>() else {
                return RangeOutcome::Full;
            };
            if end < start {
                return RangeOutcome::Full; // malformed → ignore
            }
            end.min(total - 1)
        };
        (start, end)
    };
    RangeOutcome::Partial { start, end }
}

/// Build the storage serve response, honoring a `Range` header when
/// `supports_range` (plain blob fetches only). Transformed-image responses pass
/// `supports_range = false` — they are cache-keyed as whole renders, so a Range
/// header is ignored and `Accept-Ranges` is not advertised.
///
/// Applies the content-type allowlist (SEC-101): a stored `content_type` not on
/// the inline-safe list is forced to `application/octet-stream` with
/// `Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`, so
/// a stored HTML/SVG/script blob can never render same-origin in the console.
fn build_serve_response(
    bytes: Vec<u8>,
    content_type: &str,
    cache_control: &str,
    supports_range: bool,
    range_header: Option<&str>,
) -> Result<Response, RtDbError> {
    let total = bytes.len() as u64;
    let outcome = if supports_range {
        resolve_byte_range(range_header, total)
    } else {
        RangeOutcome::Full
    };
    let (served_content_type, force_attachment) = resolve_served_content_type(content_type);
    // SEC-101: nosniff on every storage response (defense in depth even with
    // the router-wide layer — this is the one route where sniffing is the
    // whole attack), and Content-Disposition: attachment when the stored type
    // is not inline-safe.
    let disposition = if force_attachment {
        "attachment"
    } else {
        "inline"
    };
    let result: Result<Response, axum::http::Error> = match outcome {
        RangeOutcome::Full => {
            let mut builder = Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, served_content_type)
                .header(header::CACHE_CONTROL, cache_control)
                .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
                .header(header::CONTENT_DISPOSITION, disposition);
            if supports_range {
                builder = builder.header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES);
            }
            builder.body(Body::from(bytes))
        }
        RangeOutcome::Partial { start, end } => {
            let slice = bytes[(start as usize)..=(end as usize)].to_vec();
            Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header(header::CONTENT_TYPE, served_content_type)
                .header(header::CACHE_CONTROL, cache_control)
                .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
                .header(header::CONTENT_DISPOSITION, disposition)
                .header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes {start}-{end}/{total}"),
                )
                .header(header::CONTENT_LENGTH, slice.len() as u64)
                .body(Body::from(slice))
        }
        RangeOutcome::Unsatisfiable => Response::builder()
            .status(StatusCode::RANGE_NOT_SATISFIABLE)
            .header(header::ACCEPT_RANGES, ACCEPT_RANGES_BYTES)
            .header(header::CONTENT_RANGE, format!("bytes */{total}"))
            .body(Body::empty()),
    };
    result.map_err(|err| {
        tracing::error!(error = %err, "failed to build serve response");
        RtDbError::internal("failed to build serve response; see server logs")
    })
}

#[derive(Serialize)]
pub(crate) struct OkResponse {
    ok: bool,
}

/// Delete a stored file. Idempotent: deleting a missing id still returns
/// `{ ok: true }`. Both the per-db blob row and the global `storage_index`
/// row are removed, so the public URL 404s afterward.
///
/// SEC-118: per-row authorization runs before the delete — a non-owner
/// interactive caller gets 403, a missing id is a successful no-op (idempotent
/// — does not disclose existence either way).
pub(crate) async fn delete_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &db).await?;
    // SEC-118: per-row owner check before the destructive op. A missing row
    // short-circuits to the idempotent `{ ok: true }` — the existence of an
    // id is not disclosed to a non-owner.
    if let Some(meta) = storage::get_meta(&state.pool, &db, &id).await? {
        enforce_blob_owner(&principal, &meta.owner_id)?;
        storage::delete(&state.pool, &db, &id).await?;
    }
    Ok(Json(OkResponse { ok: true }))
}

/// Fetch a stored file's metadata. `contentType` is omitted from the response
/// when the upload supplied no content-type. Unknown id → `NotFound`.
///
/// SEC-118: per-row authorization runs before the metadata is returned — a
/// non-owner interactive caller gets 403.
pub(crate) async fn metadata_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<storage::FileMeta>, RtDbError> {
    let principal = authed(&state, &headers, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    enforce_blob_owner(&principal, &meta.owner_id)?;
    Ok(Json(meta))
}
#[cfg(test)]
mod range_tests {
    use super::{RangeOutcome, resolve_byte_range};

    fn partial(start: u64, end: u64) -> RangeOutcome {
        RangeOutcome::Partial { start, end }
    }

    #[test]
    fn no_or_empty_range_is_full() {
        assert!(matches!(resolve_byte_range(None, 100), RangeOutcome::Full));
        assert!(matches!(
            resolve_byte_range(Some(""), 100),
            RangeOutcome::Full
        ));
        assert!(matches!(
            resolve_byte_range(Some("   "), 100),
            RangeOutcome::Full
        ));
    }

    #[test]
    fn non_bytes_unit_is_ignored() {
        assert!(matches!(
            resolve_byte_range(Some("items=0-99"), 100),
            RangeOutcome::Full
        ));
    }

    #[test]
    fn multipart_range_is_ignored() {
        assert!(matches!(
            resolve_byte_range(Some("bytes=0-1,3-4"), 100),
            RangeOutcome::Full
        ));
    }

    #[test]
    fn basic_inclusive_range() {
        assert_eq!(resolve_byte_range(Some("bytes=0-99"), 100), partial(0, 99));
    }

    #[test]
    fn open_ended_range() {
        assert_eq!(resolve_byte_range(Some("bytes=50-"), 100), partial(50, 99));
    }

    #[test]
    fn suffix_range() {
        assert_eq!(resolve_byte_range(Some("bytes=-10"), 100), partial(90, 99));
    }

    #[test]
    fn suffix_larger_than_total_is_whole() {
        assert_eq!(resolve_byte_range(Some("bytes=-200"), 100), partial(0, 99));
    }

    #[test]
    fn end_is_clamped_to_total() {
        assert_eq!(
            resolve_byte_range(Some("bytes=90-1000"), 100),
            partial(90, 99)
        );
    }

    #[test]
    fn single_byte_range() {
        assert_eq!(resolve_byte_range(Some("bytes=0-0"), 100), partial(0, 0));
        assert_eq!(resolve_byte_range(Some("bytes=5-5"), 100), partial(5, 5));
    }

    #[test]
    fn start_at_or_past_end_is_unsatisfiable() {
        assert!(matches!(
            resolve_byte_range(Some("bytes=100-"), 100),
            RangeOutcome::Unsatisfiable
        ));
        assert!(matches!(
            resolve_byte_range(Some("bytes=150-200"), 100),
            RangeOutcome::Unsatisfiable
        ));
    }

    #[test]
    fn zero_byte_resource_is_unsatisfiable() {
        assert!(matches!(
            resolve_byte_range(Some("bytes=0-0"), 0),
            RangeOutcome::Unsatisfiable
        ));
        assert!(matches!(
            resolve_byte_range(Some("bytes=-5"), 0),
            RangeOutcome::Unsatisfiable
        ));
    }

    #[test]
    fn zero_length_suffix_is_unsatisfiable() {
        assert!(matches!(
            resolve_byte_range(Some("bytes=-0"), 100),
            RangeOutcome::Unsatisfiable
        ));
    }

    #[test]
    fn malformed_ranges_are_ignored() {
        assert!(matches!(
            resolve_byte_range(Some("bytes=5-2"), 100),
            RangeOutcome::Full
        ));
        assert!(matches!(
            resolve_byte_range(Some("bytes=abc-2"), 100),
            RangeOutcome::Full
        ));
        assert!(matches!(
            resolve_byte_range(Some("bytes=0_9"), 100),
            RangeOutcome::Full
        ));
    }
}
