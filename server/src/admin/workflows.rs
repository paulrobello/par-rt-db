//! Admin workflow-run routes: thin admin-gated wrappers over the same
//! `workflows` accessors the per-db machine-token surfaces use. Runs carry no
//! owner at the table level — steps fire as the system principal
//! (`committer::handle_workflow_advance` executes with `owner = None`).
//! Create runs the same submit-time validation + recursive table-allowlist
//! check as the other start surfaces (a no-op for the admin bypass principal).

use std::str::FromStr;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::PrincipalCtx;
use crate::db;
use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::protocol::{WorkflowInfo, WorkflowInfoFull, WorkflowSpec, WorkflowStatus};
use crate::workflows;

#[derive(Deserialize)]
pub(super) struct AdminWorkflowListParams {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Serialize)]
pub(super) struct AdminWorkflowListResponse {
    workflows: Vec<WorkflowInfo>,
}

#[derive(Serialize)]
pub(super) struct AdminWorkflowCreateResponse {
    id: String,
}

#[derive(Serialize)]
pub(super) struct AdminWorkflowManageResponse {
    ok: bool,
}

/// `GET /admin/db/{db}/workflows?status=&limit=` — list runs for a database.
/// `status` parses via `WorkflowStatus::from_str` (a bad value is
/// `BadRequest`, not a 500); `limit` defaults to 100 and caps at 500.
pub(super) async fn admin_list_workflows(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    Query(params): Query<AdminWorkflowListParams>,
) -> Result<Json<AdminWorkflowListResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let status = params
        .status
        .as_deref()
        .map(WorkflowStatus::from_str)
        .transpose()
        .map_err(|e| RtDbError::bad_request(format!("invalid status filter: {e}")))?;
    let limit = params.limit.unwrap_or(100).min(500);
    let workflows = workflows::list(&state.pool, &db, status.as_ref(), limit).await?;
    Ok(Json(AdminWorkflowListResponse { workflows }))
}

/// `POST /admin/db/{db}/workflows` — start a run. Mirrors the other three
/// start surfaces (WS/HTTP/txn step): `validate_spec` + the recursive
/// table-allowlist check, then `insert`.
pub(super) async fn admin_create_workflow(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(spec): ApiJson<WorkflowSpec>,
) -> Result<Json<AdminWorkflowCreateResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    workflows::validate_spec(&spec)?;
    // Uniform with the other start surfaces (FM-28's tightened pattern).
    // Admin is a bypass principal (`tables = None`) so this is a no-op today —
    // it exists so the four surfaces cannot drift if admin principals ever
    // carry scopes.
    crate::txn::authorize_spec_tables(&PrincipalCtx::bypass(), &spec)?;
    let id = workflows::insert(&state.pool, &db, &spec).await?;
    Ok(Json(AdminWorkflowCreateResponse { id }))
}

/// `GET /admin/db/{db}/workflows/{id}` — full run row (info + outcome trail).
pub(super) async fn admin_get_workflow(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<WorkflowInfoFull>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let workflow = workflows::get(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown workflow"))?;
    Ok(Json(workflow))
}

/// `POST /admin/db/{db}/workflows/{id}/cancel` — flip a non-terminal run to
/// `cancelled` (`ok: false` for a missing or already-terminal run).
pub(super) async fn admin_cancel_workflow(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminWorkflowManageResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let ok = workflows::cancel(&state.pool, &db, &id).await?;
    Ok(Json(AdminWorkflowManageResponse { ok }))
}

/// `DELETE /admin/db/{db}/workflows/{id}` — hard-delete one run row (unlike
/// cancel, the audit trail does not survive; `ok: false` when already gone).
pub(super) async fn admin_delete_workflow(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminWorkflowManageResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let ok = workflows::delete(&state.pool, &db, &id).await?;
    Ok(Json(AdminWorkflowManageResponse { ok }))
}
