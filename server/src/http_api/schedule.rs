//! Schedule handlers: create, external claim, finalize (complete/retry/
//! fail), cancel/pause/resume, list (ARC-008 split; pure move from the
//! former single-file `http_api.rs`).

use serde::{Deserialize, Serialize};
use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;

use crate::AppState;
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::http_api::{ApiJson, authed};
use crate::protocol::{ClaimedSchedule, ScheduleInfo, ScheduleKind, ScheduleWhen};
use crate::rate_limit::check_http_rate_limits;
use crate::scheduler;
use crate::txn::Transaction;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ScheduleRequest {
    pub(crate) db: String,
    when: ScheduleWhen,
    txn: Transaction,
    /// External-claim job mode: never internally executed — an application
    /// worker claims the job via `POST /api/schedule/claim` and finalizes it
    /// with the returned `leaseGeneration` fencing token.
    #[serde(default)]
    external: bool,
}

#[derive(Serialize)]
pub(crate) struct ScheduleResponse {
    id: String,
}

pub(crate) async fn schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ScheduleRequest>,
) -> Result<Json<ScheduleResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // ARC-006: the single enqueue path — read-only rejection, freeze gate,
    // FM-28 table-scope check, scheduler warm-up, resolve_when, insert with
    // the SEC-001 enqueuer capture all live in `scheduler::enqueue`.
    let id = scheduler::enqueue(
        &state,
        Some(&principal),
        &body.db,
        body.when,
        &body.txn,
        body.external,
        scheduler::FreezePolicy::Enforce,
    )
    .await?;
    Ok(Json(ScheduleResponse { id }))
}

/// `POST /api/schedule/claim` — atomically claim due external jobs for an
/// application worker. Each claim assigns the per-job monotonic
/// `leaseGeneration` fencing token and stamps the lease deadline; the
/// finalize surface rejects any transition whose token is no longer current.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaimSchedulesRequest {
    db: String,
    /// Max jobs to claim (default [`scheduler::CLAIM_DEFAULT`], capped at
    /// [`scheduler::CLAIM_BATCH`]).
    #[serde(default)]
    limit: Option<i64>,
    /// Lease length in ms (default [`scheduler::LEASE_DEFAULT_MS`], bounded by
    /// [`scheduler::LEASE_MIN_MS`] / [`scheduler::LEASE_MAX_MS`]).
    #[serde(default)]
    lease_ms: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ClaimSchedulesResponse {
    jobs: Vec<ClaimedSchedule>,
}

pub(crate) async fn claim_schedules_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ClaimSchedulesRequest>,
) -> Result<Json<ClaimSchedulesResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    // SEC-002: the external worker is a machine token by design — a user
    // session cannot claim (or later finalize) external jobs.
    if matches!(principal, crate::auth::Principal::User { .. }) {
        return Err(RtDbError::forbidden(
            "external schedule claims require a machine token",
        ));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // Cold-db guard (claim is direct table access; no scheduler task needed —
    // external jobs are never internally executed).
    scheduler::ensure_table(&state.pool, &body.db).await?;

    let limit = body
        .limit
        .unwrap_or(scheduler::CLAIM_DEFAULT)
        .clamp(1, scheduler::CLAIM_BATCH);
    let lease_ms = body
        .lease_ms
        .unwrap_or(scheduler::LEASE_DEFAULT_MS)
        .clamp(scheduler::LEASE_MIN_MS, scheduler::LEASE_MAX_MS);
    let now = now_ms();
    let claimed = scheduler::claim_external(&state.pool, &body.db, now, limit, lease_ms).await?;
    let jobs = claimed
        .into_iter()
        .map(|job| {
            Ok::<ClaimedSchedule, RtDbError>(ClaimedSchedule {
                id: job.id,
                kind: job.kind.parse::<ScheduleKind>().map_err(|err| {
                    RtDbError::internal(format!("invalid scheduled_txns.kind: {err}"))
                })?,
                due_at: job.due_at,
                txn: job.txn,
                cron: job.cron,
                every_ms: job.every_ms,
                lease_generation: job.lease_generation,
                lease_deadline_ms: job.lease_deadline_ms,
            })
        })
        .collect::<Result<Vec<_>, RtDbError>>()?;
    Ok(Json(ClaimSchedulesResponse { jobs }))
}

/// Shared authorize-then-finalize body for the three external transitions.
/// `outcome` and `error_text` come from the concrete handler; the fence
/// (external + running + token) is enforced inside `finalize_external`.
pub(crate) async fn run_finalize_op(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
    id: &str,
    lease: i64,
    outcome: scheduler::ExternalOutcome,
    error_text: Option<&str>,
) -> Result<Json<ManageResponse>, RtDbError> {
    let principal = authed(state, headers, db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    // SEC-002: the external worker is a machine token by design — a user
    // session cannot finalize (complete/retry/fail) external jobs.
    if matches!(principal, crate::auth::Principal::User { .. }) {
        return Err(RtDbError::forbidden(
            "external schedule claims require a machine token",
        ));
    }
    check_http_rate_limits(state, &principal, db).await?;
    scheduler::ensure_table(&state.pool, db).await?;
    scheduler::finalize_external(&state.pool, db, id, lease, outcome, error_text).await?;
    Ok(Json(ManageResponse { ok: true }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FinalizeScheduleRequest {
    db: String,
    /// The per-job fencing token returned by the claim.
    lease: i64,
    /// `retry` only: re-arm delay in ms (default 60_000, max MAX_EVERY_MS).
    #[serde(default)]
    delay_ms: Option<i64>,
    /// `retry`/`fail`: recorded on the job as `last_error`.
    #[serde(default)]
    error: Option<String>,
}

pub(crate) async fn complete_schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<FinalizeScheduleRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_finalize_op(
        &state,
        &headers,
        &body.db,
        &id,
        body.lease,
        scheduler::ExternalOutcome::Complete,
        None,
    )
    .await
}

pub(crate) async fn retry_schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<FinalizeScheduleRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    let delay_ms = body
        .delay_ms
        .unwrap_or(scheduler::RETRY_DEFAULT_MS)
        .clamp(1, scheduler::MAX_EVERY_MS);
    run_finalize_op(
        &state,
        &headers,
        &body.db,
        &id,
        body.lease,
        scheduler::ExternalOutcome::Retry { delay_ms },
        body.error.as_deref(),
    )
    .await
}

pub(crate) async fn fail_schedule_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<FinalizeScheduleRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    if body.error.is_none() {
        return Err(RtDbError::bad_request("`error` is required to fail a job"));
    }
    run_finalize_op(
        &state,
        &headers,
        &body.db,
        &id,
        body.lease,
        scheduler::ExternalOutcome::Fail,
        body.error.as_deref(),
    )
    .await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ManageRequest {
    pub(crate) db: String,
}

#[derive(Serialize)]
pub(crate) struct ManageResponse {
    ok: bool,
}

pub(crate) enum ManageOp {
    Cancel,
    Pause,
    Resume,
}

/// Shared authorize-then-op body for the three boolean manage handlers.
pub(crate) async fn run_manage_op(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
    id: &str,
    op: ManageOp,
) -> Result<Json<ManageResponse>, RtDbError> {
    let principal = authed(state, headers, db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    // SEC-002: only the creator may cancel/pause/resume.
    scheduler::check_owner_of(&state.pool, db, id, &principal).await?;
    check_http_rate_limits(state, &principal, db).await?;
    // Cold-db guard (the table is ensured only at scheduler startup for dbs
    // predating the create-time side-table rollout): ensure inline so manage
    // ops on a db with no spawned tasks are a clean `false`, not a 500.
    scheduler::ensure_table(&state.pool, db).await?;
    let ok = match op {
        ManageOp::Cancel => scheduler::cancel(&state.pool, db, id).await?,
        ManageOp::Pause => scheduler::set_paused(&state.pool, db, id, true).await?,
        ManageOp::Resume => scheduler::set_paused(&state.pool, db, id, false).await?,
    };
    Ok(Json(ManageResponse { ok }))
}

pub(crate) async fn cancel_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Cancel).await
}

pub(crate) async fn pause_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Pause).await
}

pub(crate) async fn resume_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<ManageResponse>, RtDbError> {
    run_manage_op(&state, &headers, &body.db, &id, ManageOp::Resume).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListRequest {
    db: String,
}

#[derive(Serialize)]
pub(crate) struct ListResponse {
    schedules: Vec<ScheduleInfo>,
}

pub(crate) async fn list_schedules_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<ListRequest>,
) -> Result<Json<ListResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // Cold-db guard (the table is ensured only at scheduler startup for dbs
    // predating the create-time side-table rollout): ensure inline so a db
    // with no spawned tasks lists empty, not 500.
    scheduler::ensure_table(&state.pool, &body.db).await?;
    let schedules = scheduler::list(&state.pool, &body.db).await?;
    Ok(Json(ListResponse { schedules }))
}
