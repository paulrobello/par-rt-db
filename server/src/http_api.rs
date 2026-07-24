use std::sync::Arc;

use axum::body::Body;
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Request, State};
use axum::http::{HeaderMap, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::{authorize, owner_of, resolve_bearer};
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::protocol::{ScheduleInfo, ScheduleWhen};
use crate::query::{Query, QueryResult, execute_query};
use crate::scheduler;
use crate::storage;
use crate::txn::Transaction;

fn bearer_token(headers: &HeaderMap) -> Result<&str, RtDbError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
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

    let schema = state.schemas.get(&state.pool, &body.db).await?;
    let result = execute_query(
        &state.pool,
        &body.db,
        &schema,
        &body.query,
        owner_of(&principal),
    )
    .await?;
    Ok(Json(QueryResponse { result }))
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

    let outcome = state
        .committers
        .mutate(
            &body.db,
            body.idempotency_key,
            body.txn,
            owner_of(&principal).map(|s| s.to_string()),
        )
        .await?;
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
    let schedules = scheduler::list(&state.pool, &body.db).await?;
    Ok(Json(ListResponse { schedules }))
}

/// HTTP one-shot routes, authorized via `Authorization: Bearer <token>`
/// (machine token or user session) resolved and checked per-request.
pub fn http_api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/query", post(query_handler))
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
    storage::ensure_table(&state.pool, &db).await?; // revive storage for old dbs

    let limit = state.config.max_file_size;
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
    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    Ok(Json(meta))
}
