//! Workflow handlers: start, list, cancel, signal (ARC-008 split; pure
//! move from the former single-file `http_api.rs`).

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use crate::AppState;
use crate::error::RtDbError;
use crate::http_api::schedule::ManageRequest;
use crate::http_api::{ApiJson, authed};
use crate::protocol::{WorkflowInfo, WorkflowSpec, WorkflowStatus};
use crate::rate_limit::check_http_rate_limits;
use crate::workflows;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StartWorkflowRequest {
    db: String,
    spec: WorkflowSpec,
}

#[derive(Serialize)]
pub(crate) struct StartWorkflowResponse {
    id: String,
}

/// `POST /api/workflows`: start a run. ARC-006: the single start path —
/// spec validation, the FM-29 table-scope check, the freeze gate, scheduler
/// warm-up, and the SEC-001 enqueuer capture all live in `workflows::start`.
pub(crate) async fn start_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<StartWorkflowRequest>,
) -> Result<Json<StartWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    let id = workflows::start(
        &state,
        Some(&principal),
        &body.db,
        &body.spec,
        workflows::FreezePolicy::Enforce,
    )
    .await?;
    Ok(Json(StartWorkflowResponse { id }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListWorkflowsRequest {
    db: String,
    #[serde(default)]
    status: Option<WorkflowStatus>,
}

#[derive(Serialize)]
pub(crate) struct ListWorkflowsResponse {
    workflows: Vec<WorkflowInfo>,
}

/// `POST /api/workflows/list`: the db's runs, newest first (optional status
/// filter; capped at 100 like the WS arm).
pub(crate) async fn list_workflows_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ListWorkflowsRequest>,
) -> Result<Json<ListWorkflowsResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // Cold-db guard (the table is ensured only at scheduler startup): ensure
    // inline so a db with no spawned tasks lists empty, not 500.
    workflows::ensure_table(&state.pool, &body.db).await?;
    let workflows = workflows::list(&state.pool, &body.db, body.status.as_ref(), 100).await?;
    Ok(Json(ListWorkflowsResponse { workflows }))
}

#[derive(Serialize)]
pub(crate) struct CancelWorkflowResponse {
    cancelled: bool,
}

/// `POST /api/workflows/{id}/cancel`: flip a non-terminal run to `cancelled`
/// (`false` for a missing or already-terminal run — a no-op, not an error).
pub(crate) async fn cancel_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<CancelWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    // SEC-002: only the creator may cancel a run.
    workflows::check_owner_of(&state.pool, &body.db, &id, &principal).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // Cold-db guard (the table is ensured only at scheduler startup): ensure
    // inline so cancel on a db with no spawned tasks is a clean `false`, not
    // a 500.
    workflows::ensure_table(&state.pool, &body.db).await?;
    let cancelled = workflows::cancel(&state.pool, &body.db, &id).await?;
    Ok(Json(CancelWorkflowResponse { cancelled }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignalWorkflowRequest {
    db: String,
    name: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

#[derive(Serialize)]
pub(crate) struct SignalWorkflowResponse {
    delivered: bool,
}

/// `POST /api/workflows/{id}/signal`: deliver an out-of-band signal to a
/// waiting run (spec §HTTP) — typed 404/409s, latest-wins payload.
pub(crate) async fn signal_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<SignalWorkflowRequest>,
) -> Result<Json<SignalWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    // SEC-002: only the creator may signal a run.
    workflows::check_owner_of(&state.pool, &body.db, &id, &principal).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // A signal injects the trigger for a frozen database's next document
    // write: reject delivery under the per-db read-only freeze so a waiting
    // run stays parked at the signal boundary until unfreeze (retry then).
    // In-flight runs that are NOT waiting keep advancing via the exempt
    // system arm.
    if crate::db::is_read_only(&state.pool, &body.db).await? {
        return Err(RtDbError::read_only());
    }
    // Cold-db guard (the table is ensured only at scheduler startup): ensure
    // inline so signal on a db with no spawned tasks is a typed 404, not a 500.
    workflows::ensure_table(&state.pool, &body.db).await?;
    match workflows::deliver_signal(&state.pool, &body.db, &id, &body.name, body.payload).await? {
        workflows::SignalDelivery::Delivered => {
            Ok(Json(SignalWorkflowResponse { delivered: true }))
        }
        workflows::SignalDelivery::NotFound => Err(RtDbError::not_found("unknown workflow")),
        workflows::SignalDelivery::NotWaiting => {
            Err(RtDbError::conflict("workflow is not waiting for a signal"))
        }
        workflows::SignalDelivery::NameMismatch { waiting_on } => Err(RtDbError::conflict(
            format!("workflow waiting on '{waiting_on}', got '{}'", body.name),
        )),
    }
}
