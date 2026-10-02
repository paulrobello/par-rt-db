//! Admin scheduled-job routes: thin admin-gated wrappers over the same
//! `scheduler` accessors the per-db machine-token handlers use. Scheduled jobs
//! carry no owner at the table level — `scheduler::insert` has no owner param
//! and `committer::handle_scheduled` always executes with `owner = None`.
//! Create runs the same enqueue-time recursive table-allowlist check as the
//! other schedule surfaces (a no-op for the admin bypass principal).

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::db;
use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::protocol::{ScheduleInfo, ScheduleWhen};
use crate::scheduler;
use crate::txn::Transaction;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminScheduleCreateRequest {
    when: ScheduleWhen,
    txn: Transaction,
    /// External-claim job mode — same semantics as the per-db
    /// `POST /api/schedule` surface (never internally executed; claimed by
    /// app workers over the claim API with a fencing token).
    #[serde(default)]
    external: bool,
}

#[derive(Serialize)]
pub(super) struct AdminScheduleCreateResponse {
    id: String,
}

#[derive(Serialize)]
pub(super) struct AdminScheduleListResponse {
    schedules: Vec<ScheduleInfo>,
}

#[derive(Serialize)]
pub(super) struct AdminScheduleManageResponse {
    ok: bool,
}

/// `GET /admin/db/{db}/schedules` — list scheduled jobs for a database.
pub(super) async fn admin_list_schedules(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<AdminScheduleListResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // The `scheduled_txns` table is ensured at database creation, but dbs
    // created before that side-table rollout (and any path that skips it)
    // rely on the per-db scheduler's startup ensure — which never ran on a
    // cold db. Ensure inline (the `admin_storage_list` precedent) instead of
    // 500ing.
    scheduler::ensure_table(&state.pool, &db).await?;
    let schedules = scheduler::list(&state.pool, &db).await?;
    Ok(Json(AdminScheduleListResponse { schedules }))
}

/// `POST /admin/db/{db}/schedules` — create a scheduled job. Mirrors
/// `http_api::schedule_handler` exactly, minus the per-db bearer/authorize gate
/// (admin-gated instead). ARC-006: routes through the single `scheduler::enqueue`
/// path with `principal: None` (the admin bypass principal) and
/// `FreezePolicy::Skip` — the admin surface's documented freeze exemption.
pub(super) async fn admin_create_schedule(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminScheduleCreateRequest>,
) -> Result<Json<AdminScheduleCreateResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let id = scheduler::enqueue(
        &state,
        None,
        &db,
        body.when,
        &body.txn,
        body.external,
        scheduler::FreezePolicy::Skip,
    )
    .await?;
    Ok(Json(AdminScheduleCreateResponse { id }))
}

/// `POST /admin/db/{db}/schedules/{id}/cancel` — delete a scheduled job.
pub(super) async fn admin_cancel_schedule(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // Cold-db guard, as `admin_list_schedules` above.
    scheduler::ensure_table(&state.pool, &db).await?;
    let ok = scheduler::cancel(&state.pool, &db, &id).await?;
    Ok(Json(AdminScheduleManageResponse { ok }))
}

/// Shared pause/resume path — `paused = true` pauses, `false` resumes.
async fn admin_set_schedule_paused(
    state: &Arc<AppState>,
    db: &str,
    id: &str,
    paused: bool,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    if !db::database_exists(&state.pool, db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // Cold-db guard, as `admin_list_schedules` above.
    scheduler::ensure_table(&state.pool, db).await?;
    let ok = scheduler::set_paused(&state.pool, db, id, paused).await?;
    Ok(Json(AdminScheduleManageResponse { ok }))
}

/// `POST /admin/db/{db}/schedules/{id}/pause` — pause a pending job.
pub(super) async fn admin_pause_schedule(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    admin_set_schedule_paused(&state, &db, &id, true).await
}

/// `POST /admin/db/{db}/schedules/{id}/resume` — resume a paused job.
pub(super) async fn admin_resume_schedule(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    admin_set_schedule_paused(&state, &db, &id, false).await
}
