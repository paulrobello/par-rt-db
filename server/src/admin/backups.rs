//! Admin backup/restore routes: trigger one `pg_dump`, list/download/delete
//! dumps, and restore a dump into a fresh DB. Manual backup runs outside the
//! committer (a read); restore targets a fresh DB, never the live one.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::Response;
use serde::{Deserialize, Serialize};
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::AppState;
use crate::error::RtDbError;
use crate::http_api::ApiJson;

use super::OkResponse;

#[derive(Serialize)]
pub(super) struct BackupsResponse {
    running: bool,
    backups: Vec<crate::backup::BackupFile>,
}

/// `GET /admin/backups` — lists the managed `pg_dump` files in
/// `config.backup_dir` newest-first, with size and parsed created-time, plus
/// the in-progress flag for the manual trigger. A missing dir (no run yet, or
/// backups disabled) returns an empty list rather than 404/500 — the endpoint
/// describes what is on disk, not what is configured. Whether the scheduler is
/// enabled at boot is already visible at `/admin/config`.
pub(super) async fn list_backups(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Result<Json<BackupsResponse>, RtDbError> {
    let backups = crate::backup::list_backups(&state.config.backup_dir).await?;
    let running = state.backup_running.load(Ordering::Acquire);
    Ok(Json(BackupsResponse { running, backups }))
}

/// RAII guard that clears `AppState::backup_running` on drop, so the flag
/// releases even if the spawned backup task panics (Drop runs during unwind) —
/// a panic in the backup path can't lock out manual triggers until restart.
pub(super) struct BackupRunningGuard(Arc<AtomicBool>);
impl Drop for BackupRunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// `POST /admin/backup` — trigger one `pg_dump` now. Returns 202 immediately;
/// the dump runs in a detached task and the in-progress flag is cleared on
/// completion (success, failure, or panic). A second call while one is running → 409.
/// Runs outside the committer (pg_dump is a read), exactly like the cron backup
/// task — no document tables or subscriptions are touched.
pub(super) async fn create_backup(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Result<(StatusCode, Json<OkResponse>), RtDbError> {
    // `swap` to set-and-test: returns the PRIOR value. If it was already true,
    // a backup is in progress — reject without disturbing the flag.
    if state.backup_running.swap(true, Ordering::AcqRel) {
        return Err(RtDbError::conflict("backup already running"));
    }
    let url = state.config.database_url.clone();
    let dir = state.config.backup_dir.clone();
    let flag = state.backup_running.clone();
    tokio::spawn(async move {
        let _guard = BackupRunningGuard(flag);
        match crate::backup::perform_backup(&url, &dir).await {
            Ok(p) => tracing::info!(path = %p.display(), "manual backup completed"),
            Err(e) => tracing::error!(error = %e, "manual backup failed"),
        }
    });
    Ok((StatusCode::ACCEPTED, Json(OkResponse { ok: true })))
}

/// `GET /admin/backups/{name}` — stream a dump file (admin-gated).
/// `validate_dump_name` runs first, so a traversal-shaped or malformed name is
/// rejected at the API edge before any filesystem access. Streams via
/// `Body::from_stream` so a large dump does not have to fit in memory.
pub(super) async fn download_backup(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, RtDbError> {
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(RtDbError::not_found("backup file not found"));
        }
        Err(_) => return Err(RtDbError::internal("failed to open backup")),
    };
    let body = Body::from_stream(FramedRead::new(file, BytesCodec::new()));
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        // `name` passed `validate_dump_name` (rtdb-<stamp>.dump), so it cannot
        // contain `"`, `\`, or any control char that would break this header.
        HeaderValue::from_str(&format!("attachment; filename=\"{name}\""))
            .map_err(|_| RtDbError::internal("invalid backup filename for header"))?,
    );
    Ok(resp)
}

/// `DELETE /admin/backups/{name}` — remove one dump (admin-gated). Same
/// `validate_dump_name` short-circuit as download. Returns 204 on success; 404
/// if the file is gone.
pub(super) async fn delete_backup(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, RtDbError> {
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    match tokio::fs::remove_file(&path).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(RtDbError::not_found("backup file not found"))
        }
        Err(_) => Err(RtDbError::internal("failed to delete backup")),
    }
}

#[derive(Deserialize)]
pub(super) struct RestoreRequest {
    name: String,
    confirm: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RestoreResponse {
    target: String,
    instructions: String,
}

/// `POST /admin/restore` — restore a dump into a fresh `rtdb_restored_<stamp>`
/// DB. `confirm` must equal `name` (typed guard, mirroring `delete_db`). The
/// live DB is never touched — `restore_to_new_db` creates a fresh target DB
/// and `pg_restore`s into it, leaving the committer and all live connections
/// undisturbed. Returns the target DB name and cutover instructions.
pub(super) async fn restore_backup(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<RestoreRequest>,
) -> Result<Json<RestoreResponse>, RtDbError> {
    if body.confirm != body.name {
        return Err(RtDbError::bad_request(
            "confirmation does not match backup filename",
        ));
    }
    let target = crate::backup::restore_to_new_db(
        &state.config.database_url,
        &state.config.backup_dir,
        &body.name,
    )
    .await?;
    Ok(Json(RestoreResponse {
        instructions: format!(
            "Restore complete into database '{target}'. To cut over: set RTDB_DATABASE_URL to connect to '{target}', then restart the server."
        ),
        target,
    }))
}
