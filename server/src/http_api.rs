use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Request, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::{authorize, resolve_bearer};
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::protocol::{ScheduleInfo, ScheduleWhen};
use crate::query::{Query, QueryResult, execute_query};
use crate::rate_limit::check_http_rate_limits;
use crate::scheduler;
use crate::storage;
use crate::txn::Transaction;

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

#[derive(Deserialize)]
struct QueryRequest {
    db: String,
    query: Query,
}

#[derive(Serialize)]
struct QueryResponse {
    result: QueryResult,
}

async fn query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<QueryRequest>,
) -> Result<Json<QueryResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let schema = state.schemas.get(&state.pool, &body.db).await?;
    let principal_ctx = principal.row_ctx();
    let t = Instant::now();
    let result = execute_query(&state.pool, &body.db, &schema, &body.query, &principal_ctx).await?;
    state
        .runtime
        .metrics
        .record_query_duration(t.elapsed().as_micros() as u64);
    state.runtime.metrics.record_query();
    Ok(Json(QueryResponse { result }))
}

#[derive(Deserialize)]
struct BatchQueryRequest {
    db: String,
    queries: Vec<Query>,
}

/// One slot of a `/api/query-batch` response. `ok` is always present; exactly
/// one of `result` / `error` accompanies it. `error` reuses the `RtDbError`
/// wire shape (`{code, message}`) verbatim — both fields are omitted from the
/// JSON when `None`, so an `ok` slot is `{"ok":true,"result":...}` and an
/// errored slot is `{"ok":false,"error":{"code":"...","message":"..."}}`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchQueryOutcome {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<QueryResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RtDbError>,
}

#[derive(Serialize)]
struct BatchQueryResponse {
    results: Vec<BatchQueryOutcome>,
}

/// Fan out over many queries against one db in a single round trip. Auth and
/// owner resolution run once for the whole request (same db, same principal —
/// mirrors `query_handler`); each query's outcome lands in its own aligned slot.
/// A per-query execution error becomes that slot's `{ok:false,error}` and never
/// fails the batch — only the db-level bearer/authorize gate returns a non-200
/// for the whole request.
async fn batch_query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<BatchQueryRequest>,
) -> Result<Json<BatchQueryResponse>, RtDbError> {
    if body.queries.is_empty() {
        return Err(RtDbError::bad_request("queries must not be empty"));
    }
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let schema = state.schemas.get(&state.pool, &body.db).await?;
    let principal_ctx = principal.row_ctx();
    let mut results = Vec::with_capacity(body.queries.len());
    for query in &body.queries {
        // Per-query timing: each successful execute_query feeds
        // query_latency individually (mirrors the per-query counter bump).
        let t = Instant::now();
        let outcome =
            match execute_query(&state.pool, &body.db, &schema, query, &principal_ctx).await {
                Ok(result) => {
                    state
                        .runtime
                        .metrics
                        .record_query_duration(t.elapsed().as_micros() as u64);
                    state.runtime.metrics.record_query();
                    BatchQueryOutcome {
                        ok: true,
                        result: Some(result),
                        error: None,
                    }
                }
                Err(err) => BatchQueryOutcome {
                    ok: false,
                    result: None,
                    error: Some(err),
                },
            };
        results.push(outcome);
    }
    Ok(Json(BatchQueryResponse { results }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutateRequest {
    db: String,
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct MutateResponse {
    results: Vec<serde_json::Value>,
}

async fn mutate_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<MutateRequest>,
) -> Result<Json<MutateResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let t = Instant::now();
    let outcome = state
        .realtime
        .committers
        .mutate(
            &body.db,
            body.idempotency_key,
            body.txn,
            principal.row_ctx(),
        )
        .await?;
    state
        .runtime
        .metrics
        .record_mutation_duration(t.elapsed().as_micros() as u64);
    state.runtime.metrics.record_mutation();
    Ok(Json(MutateResponse {
        results: outcome.results,
    }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScheduleRequest {
    db: String,
    when: ScheduleWhen,
    txn: Transaction,
}

#[derive(Serialize)]
struct ScheduleResponse {
    id: String,
}

async fn schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ScheduleRequest>,
) -> Result<Json<ScheduleResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let (kind, due_at, cron) = scheduler::resolve_when(body.when, now_ms())?;
    let id = scheduler::insert(
        &state.pool,
        &body.db,
        kind,
        due_at,
        &body.txn,
        cron.as_deref(),
    )
    .await?;
    Ok(Json(ScheduleResponse { id }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManageRequest {
    db: String,
}

#[derive(Serialize)]
struct ManageResponse {
    ok: bool,
}

enum ManageOp {
    Cancel,
    Pause,
    Resume,
}

/// Shared authorize-then-op body for the three boolean manage handlers.
async fn run_manage_op(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
    id: &str,
    op: ManageOp,
) -> Result<Json<ManageResponse>, RtDbError> {
    let token = bearer_token(headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, db).await?;
    check_http_rate_limits(state, &principal, db).await?;
    let ok = match op {
        ManageOp::Cancel => scheduler::cancel(&state.pool, db, id).await?,
        ManageOp::Pause => scheduler::set_paused(&state.pool, db, id, true).await?,
        ManageOp::Resume => scheduler::set_paused(&state.pool, db, id, false).await?,
    };
    Ok(Json(ManageResponse { ok }))
}

async fn cancel_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Cancel).await
}

async fn pause_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Pause).await
}

async fn resume_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Resume).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListRequest {
    db: String,
}

#[derive(Serialize)]
struct ListResponse {
    schedules: Vec<ScheduleInfo>,
}

async fn list_schedules_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ListRequest>,
) -> Result<Json<ListResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    let schedules = scheduler::list(&state.pool, &body.db).await?;
    Ok(Json(ListResponse { schedules }))
}

/// HTTP one-shot routes, authorized via `Authorization: Bearer <token>`
/// (machine token or user session) resolved and checked per-request.
pub fn http_api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/query", post(query_handler))
        .route("/api/query-batch", post(batch_query_handler))
        .route("/api/mutate", post(mutate_handler))
        .route("/api/schedule", post(schedule_handler))
        .route("/api/schedule/{id}/cancel", post(cancel_handler))
        .route("/api/schedule/{id}/pause", post(pause_handler))
        .route("/api/schedule/{id}/resume", post(resume_handler))
        .route("/api/schedules", post(list_schedules_handler))
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
        // Public, unauthenticated serve — the one unauthenticated route in the
        // server, by design. The opaque id resolves to its owning db via the
        // global index.
        .route("/storage/{id}", get(serve_public_handler))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadResponse {
    id: String,
    sha256: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

async fn upload_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    request: Request,
) -> Result<Json<UploadResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &db).await?;
    storage::ensure_table(&state.pool, &db).await?; // revive storage for old dbs

    // `max_file_size` is admin-mutable via PATCH /admin/config; clamp to the
    // compile-time HARD_MAX_FILE_SIZE so a misconfigured persisted row (or a
    // compromised admin token) cannot buffer arbitrarily large blobs into
    // Postgres bytea. The bearer is already authorized above; clamp ordering
    // preserves the auth-before-buffering invariant (SEC-008).
    let limit = crate::config::HARD_MAX_FILE_SIZE.min(state.runtime.hot.load().max_file_size);
    let bytes = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| RtDbError::bad_request("upload exceeds max file size"))?;

    let size = bytes.len() as i64;
    let sha256 = storage::sha256_hex_bytes(&bytes);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let id = storage::put(
        &state.pool,
        &db,
        &sha256,
        size,
        content_type.as_deref(),
        &bytes,
    )
    .await?;
    state.runtime.metrics.record_upload();
    Ok(Json(UploadResponse {
        id,
        sha256,
        size,
        content_type,
    }))
}

/// Public, unauthenticated serve: anyone with the URL fetches the bytes. The
/// opaque id resolves to its owning db via the global index.
async fn serve_public_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, RtDbError> {
    let db = storage::resolve_db(&state.pool, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    serve_bytes(&state, &db, &id).await
}

/// Authed serve: the caller's principal must be authorized for `{db}`; the id
/// must live in that db's table (404 otherwise — cross-db isolation).
async fn serve_authed_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Response, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    serve_bytes(&state, &db, &id).await
}

async fn serve_bytes(state: &Arc<AppState>, db: &str, id: &str) -> Result<Response, RtDbError> {
    let (bytes, content_type) = storage::get(&state.pool, db, id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    let ct = content_type.unwrap_or_else(|| "application/octet-stream".to_string());
    Response::builder()
        .header(header::CONTENT_TYPE, ct)
        .body(Body::from(bytes))
        .map_err(|err| RtDbError::internal(format!("failed to build serve response: {err}")))
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

/// Delete a stored file. Idempotent: deleting a missing id still returns
/// `{ ok: true }`. Both the per-db blob row and the global `storage_index`
/// row are removed, so the public URL 404s afterward.
async fn delete_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &db).await?;
    storage::delete(&state.pool, &db, &id).await?;
    Ok(Json(OkResponse { ok: true }))
}

/// Fetch a stored file's metadata. `contentType` is omitted from the response
/// when the upload supplied no content-type. Unknown id → `NotFound`.
async fn metadata_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<storage::FileMeta>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    check_http_rate_limits(&state, &principal, &db).await?;
    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    Ok(Json(meta))
}
