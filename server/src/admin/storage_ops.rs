//! Admin file-storage routes: list, upload (raw body), and idempotent delete.
//! Mirrors the per-db machine-token handlers in `http_api`; storage is not
//! per-row so there is no owner to bypass — the admin gate alone guards these.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, header::CONTENT_TYPE};
use serde::Serialize;

use crate::error::RtDbError;
use crate::{AppState, db, storage};

use super::OkResponse;

#[derive(Serialize)]
pub(super) struct AdminStorageListResponse {
    files: Vec<storage::FileMeta>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminStorageUploadResponse {
    id: String,
}

/// `GET /admin/db/{db}/storage` — list stored files (metadata only), newest
/// first. `ensure_table` first so a database that predates the storage feature
/// (or had its table dropped) returns an empty list rather than erroring.
pub(super) async fn admin_storage_list(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<AdminStorageListResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::ensure_table(&state.pool, &db).await?;
    let files = storage::list(&state.pool, &db).await?;
    Ok(Json(AdminStorageListResponse { files }))
}

/// `POST /admin/db/{db}/storage` — admin upload (raw body). Mirrors
/// `http_api::upload_handler` exactly: ensure_table, the live `max_file_size`
/// check (clamped to `HARD_MAX_FILE_SIZE`), sha256, `storage::put`, and
/// `metrics.record_upload()`. The route carries `DefaultBodyLimit::disable` so
/// `to_bytes` is the sole ceiling (SEC-008).
pub(super) async fn admin_storage_upload(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    request: Request,
) -> Result<Json<AdminStorageUploadResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::ensure_table(&state.pool, &db).await?;
    let limit = crate::config::HARD_MAX_FILE_SIZE.min(state.runtime.hot.load().max_file_size);
    let bytes = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| RtDbError::bad_request("upload exceeds max file size"))?;
    let size = bytes.len() as i64;
    let sha256 = storage::sha256_hex_bytes(&bytes);
    let content_type = _headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let id = storage::put(
        &state.pool,
        &db,
        &sha256,
        size,
        content_type.as_deref(),
        // SEC-118: admin uploads stay owner-less (NULL) — system-initiated,
        // matching how ownerField treats admin writes. Admin reads/writes via
        // this route bypass per-row enforcement either way.
        None,
        &bytes,
    )
    .await?;
    state.runtime.metrics.record_upload();
    Ok(Json(AdminStorageUploadResponse { id }))
}

/// `DELETE /admin/db/{db}/storage/{id}` — idempotent delete. Both the per-db
/// blob row and the global `storage_index` row are removed (atomic, in one tx
/// inside `storage::delete`), so the public serve URL 404s afterward.
pub(super) async fn admin_storage_delete(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::delete(&state.pool, &db, &id).await?;
    Ok(Json(OkResponse { ok: true }))
}
