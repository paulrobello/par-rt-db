//! HTTP surface for the ARC-005 background bulk delete: `POST /api/bulk/delete`
//! starts an op (freeze + register + kick the per-op driver), and
//! `GET /api/bulk/{id}?db=` polls it. Auth mirrors the mutate surface: user
//! sessions and machine tokens with write access; read-only tokens rejected;
//! the FM-28 table-scope gate applies at submit time. The registry is
//! per-instance (see `bulk`'s module docs).

use std::collections::HashMap;
use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query as AxumQuery, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::auth::Principal;
use crate::bulk::{BulkOpState, BulkPhase, MAX_BULK_DELETE_IDS};
use crate::committer::CommitterRequest;
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::http_api::{AdmissionEpoch, ApiJson, authed};
use crate::rate_limit::check_http_rate_limits;
use crate::{AppState, bulk};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkDeleteRequest {
    db: String,
    table: String,
    /// The DSL filter selecting the rows to remove (same wire shape as a
    /// Query's `where`). Required — an unfiltered whole-table delete goes
    /// through the admin purge route instead.
    filter: crate::query::FilterExpr,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkDeleteResponse {
    op_id: String,
    matched: usize,
    state: &'static str,
}

/// `POST /api/bulk/delete` — freeze the filter's matching ids (read-only
/// serialized turn, over-cap → 400 "narrow the filter"), register the op, and
/// kick the per-op driver task. Replies immediately with the op id; the delete
/// then progresses chunk-by-chunk through the committer queue, so interactive
/// writes interleave between chunk turns.
pub(crate) async fn bulk_delete_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Extension(admission_epoch): Extension<AdmissionEpoch>,
    ApiJson(body): ApiJson<BulkDeleteRequest>,
) -> Result<Json<BulkDeleteResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // FM-28: the same table-scope gate a mutate step enforces at execution,
    // applied at submit so an out-of-scope token cannot even freeze ids.
    crate::auth::authorize_table(&principal.row_ctx(), &body.table)?;

    let (reply, reply_rx) = oneshot::channel();
    state
        .realtime
        .committers
        .submit(
            &body.db,
            CommitterRequest::RunBulkDeleteStart {
                table: body.table.clone(),
                filter: Some(body.filter),
                principal_ctx: principal.row_ctx(),
                id_cap: MAX_BULK_DELETE_IDS,
                admission_epoch: admission_epoch.0,
                reply,
            },
        )
        .await?;
    let started = reply_rx
        .await
        .map_err(|_| RtDbError::internal("committer task dropped the reply"))??;
    if started.matched_over_cap {
        return Err(RtDbError::bad_request(format!(
            "filter matches more than {MAX_BULK_DELETE_IDS} documents; narrow the filter"
        )));
    }

    let op_id = uuid::Uuid::now_v7().to_string();
    let matched = started.ids.len();
    state.bulk.register(BulkOpState {
        id: op_id.clone(),
        db: body.db.clone(),
        table: body.table.clone(),
        // Machine tokens carry no user id (identity bypass, matching a
        // machine-initiated mutate); user sessions attribute their chunks.
        owner: match &principal {
            Principal::User { user_id, .. } => Some(user_id.clone()),
            Principal::Machine { .. } => None,
        },
        ids: started.ids,
        admission_epoch: admission_epoch.0,
        cursor: 0,
        phase: BulkPhase::Running,
        matched,
        deleted: 0,
        error_text: None,
        created_at: now_ms(),
    });
    let registry = state.bulk.clone();
    let committers = state.realtime.committers.clone();
    let db = body.db.clone();
    let spawned_id = op_id.clone();
    tokio::spawn(bulk::run_op(registry, committers, db, spawned_id));
    Ok(Json(BulkDeleteResponse {
        op_id,
        matched,
        state: "running",
    }))
}

/// Wire projection of the registry's status (bulk::BulkStatus + the op id).
/// `state` ∈ running | complete | failed; `error` present only on failed.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BulkStatusResponse {
    id: String,
    state: &'static str,
    matched: usize,
    deleted: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn phase_str(phase: BulkPhase) -> &'static str {
    match phase {
        BulkPhase::Running => "running",
        BulkPhase::Complete => "complete",
        BulkPhase::Failed => "failed",
    }
}

/// `GET /api/bulk/{id}?db=` — poll one op. Authed read on the db; an unknown
/// id (or one the restart lost) is NotFound. A registered op answering under a
/// different db name is also NotFound — the id is unguessable, but the db gate
/// never leaks cross-db existence.
pub(crate) async fn bulk_status_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(op_id): Path<String>,
    AxumQuery(query): AxumQuery<HashMap<String, String>>,
) -> Result<Json<BulkStatusResponse>, RtDbError> {
    let Some(db) = query.get("db") else {
        return Err(RtDbError::bad_request("`db` query parameter is required"));
    };
    let principal = authed(&state, &headers, db).await?;
    check_http_rate_limits(&state, &principal, db).await?;
    let status = state
        .bulk
        .status(&op_id)
        .ok_or_else(|| RtDbError::not_found("bulk op not found"))?;
    if status.db != *db {
        return Err(RtDbError::not_found("bulk op not found"));
    }
    Ok(Json(BulkStatusResponse {
        id: op_id,
        state: phase_str(status.phase),
        matched: status.matched,
        deleted: status.deleted,
        error: status.error,
    }))
}
