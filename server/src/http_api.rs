//! One-shot HTTP API. Query and mutate routes route mutations through
//! `Committers::mutate` so subscriptions fire regardless of which transport
//! wrote (the WS handler in `ws` is the other transport, with one shared
//! vocabulary — see `protocol`). Also carries the storage upload/serve routes,
//! signed-URL minting, image-transform serving, the admin surface, and
//! per-machine-token / per-db rate limiting
//! (`RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`; over-limit →
//! 429 `RATE_LIMITED` + `Retry-After`).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{
    ConnectInfo, DefaultBodyLimit, FromRequest, Path, Query as AxumQuery, Request, State,
};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::auth::{Principal, PrincipalCtx, authorize, resolve_bearer};
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::image_transform::{Resolved, TransformParams};
use crate::metrics::SlowQueryRecord;
use crate::protocol::{ScheduleInfo, ScheduleWhen, WorkflowInfo, WorkflowSpec, WorkflowStatus};
use crate::query::{Query, QueryResult, compile_query, execute_query};
use crate::rate_limit::{check_http_rate_limits, check_storage_public_rate_limit};
use crate::scheduler;
use crate::signed_url;
use crate::storage;
use crate::txn::Transaction;
use crate::workflows;

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

/// The per-db auth prologue shared by the HTTP query/mutate/manage handlers:
/// extract the bearer token, resolve it to a `Principal`, and authorize it for
/// `db`. Returns the resolved principal so the caller can run per-row checks,
/// rate-limit, or branch on `is_read_only()` — those stay at the call site
/// because they vary per handler. ARC-115: collapses the 11 copy-pasted
/// `bearer_token → resolve_bearer → authorize` triplets into one place.
async fn authed(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
) -> Result<Principal, RtDbError> {
    let token = bearer_token(headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, db).await?;
    Ok(principal)
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
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let schema = state.schemas.get(&state.pool, &body.db).await?;
    let principal_ctx = principal.row_ctx();
    let started = Instant::now();
    let started_at_ms = now_ms();
    let result = execute_query(
        &state.pool,
        &body.db,
        &schema,
        &body.query,
        &principal_ctx,
        false,
    )
    .await?;
    let elapsed_us = started.elapsed().as_micros() as u64;
    state.runtime.metrics.record_query_duration(elapsed_us);
    state.runtime.metrics.record_query();
    record_slow_query_if_threshold(
        &state,
        &body.db,
        &body.query,
        &schema,
        &principal_ctx,
        started_at_ms,
        elapsed_us,
    );
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

/// SEC-119: hard cap on the number of queries a single
/// `POST /api/query-batch` may carry. Rejects an oversized batch BEFORE the
/// bearer/authorize gate (fail-fast on abuse) — a caller supplying thousands
/// of queries otherwise amplifies one HTTP request into a serial fan-out that
/// monopolizes a committer-adjacent worker. 64 mirrors a generous round-trip
/// budget; raise via code change if a real workload needs more (use multiple
/// batched requests rather than one giant one).
const MAX_BATCH_QUERIES: usize = 64;

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
    // SEC-119: reject an oversized batch before the bearer/authorize gate so an
    // unauthenticated abuser can't pin a worker on a 10k-query fan-out. The
    // cap is server-side and uniform across all callers (no admin bypass — an
    // admin-level need for more should split into multiple batched requests).
    if body.queries.len() > MAX_BATCH_QUERIES {
        return Err(RtDbError::bad_request(format!(
            "query batch size {} exceeds maximum of {MAX_BATCH_QUERIES}",
            body.queries.len()
        )));
    }
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let schema = state.schemas.get(&state.pool, &body.db).await?;
    let principal_ctx = principal.row_ctx();
    let mut results = Vec::with_capacity(body.queries.len());
    for query in &body.queries {
        // Per-query timing: each successful execute_query feeds
        // query_latency individually (mirrors the per-query counter bump).
        let started = Instant::now();
        let started_at_ms = now_ms();
        let outcome =
            match execute_query(&state.pool, &body.db, &schema, query, &principal_ctx, false).await
            {
                Ok(result) => {
                    let elapsed_us = started.elapsed().as_micros() as u64;
                    state.runtime.metrics.record_query_duration(elapsed_us);
                    state.runtime.metrics.record_query();
                    record_slow_query_if_threshold(
                        &state,
                        &body.db,
                        query,
                        &schema,
                        &principal_ctx,
                        started_at_ms,
                        elapsed_us,
                    );
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

/// Slow-query log hook (ENH-019). Called after every successful query in
/// `query_handler` and each iteration of `batch_query_handler`. When the query
/// exceeded `Config::slow_query_ms`, re-compiles via the same `compile_query`
/// the `/explain` route uses (the SQL string IS the executed SQL — compile is
/// pure/deterministic, so a second compile yields the same string), formats
/// the bound parameters (only when `slow_query_log_params` is set, to keep
/// document content out of the log by default), and pushes a [`SlowQueryRecord`]
/// into the bounded ring buffer on [`Metrics`]. The threshold check runs first
/// and short-circuits both the re-compile and the record construction when the
/// log is off (`slow_query_ms == 0`) or the query was fast — the common case.
fn record_slow_query_if_threshold(
    state: &Arc<AppState>,
    db: &str,
    q: &Query,
    schema: &crate::schema::SchemaDef,
    principal_ctx: &PrincipalCtx,
    started_at_ms: i64,
    elapsed_us: u64,
) {
    let threshold_ms = state.config.slow_query_ms;
    if threshold_ms == 0 {
        return;
    }
    let elapsed_ms = elapsed_us / 1000;
    if elapsed_ms < threshold_ms {
        return;
    }
    // Re-compile the same query to capture the exact SQL + ordered binds the
    // real execute path used. Pure and non-async — no pool, no I/O. A compile
    // error here would be a bug (the execute just succeeded), so on the
    // off-chance it happens we drop the slow-query record rather than the
    // successful query result.
    let Ok((cq, _warnings)) = compile_query(db, schema, q, principal_ctx, false) else {
        return;
    };
    let params = if state.config.slow_query_log_params {
        Some(
            cq.binds
                .iter()
                .map(|bind| match bind {
                    crate::txn::EqBind::Text(v) => v.clone(),
                    crate::txn::EqBind::Num(v) => v.to_string(),
                    crate::txn::EqBind::Bool(v) => v.to_string(),
                    crate::txn::EqBind::I64(v) => v.to_string(),
                })
                .collect::<Vec<String>>(),
        )
    } else {
        None
    };
    state.runtime.metrics.record_slow_query(SlowQueryRecord {
        started_at_ms,
        duration_ms: elapsed_ms,
        db: db.to_string(),
        table: q.table.clone(),
        terminal: cq.terminal.to_string(),
        sql: cq.sql,
        params,
    });
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
    let principal = authed(&state, &headers, &body.db).await?;
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
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;

    // FM-28 tightening: a scoped machine token cannot smuggle a future write
    // into a table outside its allowlist via a scheduled job (matches the
    // per-step gate `execute_txn` applies at fire time — but fire time runs
    // as bypass, so enqueue time is the only scoped check).
    crate::txn::authorize_txn_tables(&principal.row_ctx(), &body.txn)?;

    // Fire time runs on the per-db scheduler, which only exists once the
    // per-db tasks spawn — ensure that before insert or the job sits pending
    // forever on a cold db (no Mutate/Subscribe since creation). The spawned
    // scheduler's startup ensure is NOT ordered against this insert, so
    // ensure the table inline too or a cold-db insert can lose the race and
    // error once.
    state.realtime.committers.ensure_spawned(&body.db).await?;
    scheduler::ensure_table(&state.pool, &body.db).await?;

    let (kind, due_at, cron, every_ms) = scheduler::resolve_when(body.when, now_ms())?;
    let id = scheduler::insert(
        &state.pool,
        &body.db,
        kind,
        due_at,
        &body.txn,
        cron.as_deref(),
        every_ms,
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
    let principal = authed(state, headers, db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
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
    let principal = authed(&state, &headers, &body.db).await?;
    check_http_rate_limits(&state, &principal, &body.db).await?;
    // Cold-db guard (the table is ensured only at scheduler startup for dbs
    // predating the create-time side-table rollout): ensure inline so a db
    // with no spawned tasks lists empty, not 500.
    scheduler::ensure_table(&state.pool, &body.db).await?;
    let schedules = scheduler::list(&state.pool, &body.db).await?;
    Ok(Json(ListResponse { schedules }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartWorkflowRequest {
    db: String,
    spec: WorkflowSpec,
}

#[derive(Serialize)]
struct StartWorkflowResponse {
    id: String,
}

/// `POST /api/workflows`: start a run. Mirrors `schedule_handler` — FM-29's
/// version of the FM-28 tightening: a scoped machine token cannot smuggle a
/// future write into a table outside its allowlist via a workflow step (steps
/// fire later as bypass, so submit time is the only scoped check).
async fn start_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<StartWorkflowRequest>,
) -> Result<Json<StartWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
    workflows::validate_spec(&body.spec)?;
    crate::txn::authorize_spec_tables(&principal.row_ctx(), &body.spec)?;
    // Steps fire from the per-db scheduler, which only exists once the per-db
    // tasks spawn — ensure that before insert or the run sits `pending`
    // forever on a cold db. The spawned scheduler's startup ensure is NOT
    // ordered against this insert, so ensure the table inline too or a
    // cold-db insert can lose the race and error once.
    state.realtime.committers.ensure_spawned(&body.db).await?;
    workflows::ensure_table(&state.pool, &body.db).await?;
    let id = workflows::insert(&state.pool, &body.db, &body.spec).await?;
    Ok(Json(StartWorkflowResponse { id }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListWorkflowsRequest {
    db: String,
    #[serde(default)]
    status: Option<WorkflowStatus>,
}

#[derive(Serialize)]
struct ListWorkflowsResponse {
    workflows: Vec<WorkflowInfo>,
}

/// `POST /api/workflows/list`: the db's runs, newest first (optional status
/// filter; capped at 100 like the WS arm).
async fn list_workflows_handler(
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
struct CancelWorkflowResponse {
    cancelled: bool,
}

/// `POST /api/workflows/{id}/cancel`: flip a non-terminal run to `cancelled`
/// (`false` for a missing or already-terminal run — a no-op, not an error).
async fn cancel_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<ManageRequest>,
) -> Result<Json<CancelWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
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
struct SignalWorkflowRequest {
    db: String,
    name: String,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct SignalWorkflowResponse {
    delivered: bool,
}

/// `POST /api/workflows/{id}/signal`: deliver an out-of-band signal to a
/// waiting run (spec §HTTP) — typed 404/409s, latest-wins payload.
async fn signal_workflow_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    ApiJson(body): ApiJson<SignalWorkflowRequest>,
) -> Result<Json<SignalWorkflowResponse>, RtDbError> {
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;
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
        .route("/api/workflows", post(start_workflow_handler))
        .route("/api/workflows/list", post(list_workflows_handler))
        .route("/api/workflows/{id}/cancel", post(cancel_workflow_handler))
        .route("/api/workflows/{id}/signal", post(signal_workflow_handler))
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
        // Mint a signed, time-limited URL — same auth as authed serve; the
        // holder fetches via `GET /storage/{id}?exp=&sig=` until expiry.
        .route("/api/storage/{db}/{id}/signed-url", get(signed_url_handler))
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
struct SignedUrlResponse {
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
async fn signed_url_handler(
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
async fn serve_public_handler(
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
    if state.config.storage_require_signed_urls && !(has_exp && has_sig) {
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

/// Canonical per-IP rate-limit key for an unauthenticated request (SEC-112).
///
/// SEC-201: the forwarding headers below are only consulted when
/// `trusted_proxy` is true (`RTDB_TRUSTED_PROXY`) — i.e. the deploy sits
/// behind a reverse proxy that actually sets them. On a directly reachable
/// port they are caller-controlled, and trusting them would let an attacker
/// mint a fresh rate-limit bucket per request; the peer address wins
/// outright instead.
///
/// Order of preference (trusted-proxy deploys only):
/// 1. `CF-Connecting-IP` — set by the Cloudflare tunnel edge to the connecting
///    client. The deploy runs behind that tunnel, and CF appends (does not
///    replace) this header, so it is the most trustworthy identifier on the
///    route and not spoofable by the caller.
/// 2. **Rightmost** hop of `X-Forwarded-For`. Cloudflare appends the real
///    client IP to whatever chain the caller supplied, so the *leftmost* entry
///    is attacker-controlled and the *rightmost* is CF's observation. The prior
///    implementation took the leftmost and was therefore trivially bypassable
///    by varying the XFF header per request (SEC-004 recurrence).
/// 3. The connection's peer IP, for direct (non-tunneled) calls.
///
/// `Forwarded:` (RFC 7239) is intentionally NOT consulted — its `for=` value
/// has the same spoofing shape as XFF, and the prior implementation parsed it
/// as if it were a comma-separated IP list, which is simply wrong. CF-Connecting-IP
/// + XFF cover every observed deployment shape.
pub(crate) fn client_ip_key(
    headers: &HeaderMap,
    peer: std::net::IpAddr,
    trusted_proxy: bool,
) -> String {
    if !trusted_proxy {
        return peer.to_string();
    }
    if let Some(cf) = headers
        .get("cf-connecting-ip")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        return cf.to_string();
    }
    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
        && let Some(last) = xff.split(',').map(str::trim).rfind(|s| !s.is_empty())
    {
        return last.to_string();
    }
    peer.to_string()
}

/// SEC-118: enforce per-row authorization on a stored blob. `blob_owner` is the
/// row's `owner_id` (`None` for system-initiated uploads and rows that predate
/// the column). The rule mirrors document per-row auth:
/// - `blob_owner == None` → allow (system-initiated; anyone authorized for the
///   db may touch it).
/// - `blob_owner == Some(_)` and the caller is a `Machine` token → allow
///   (machine bypass, same as document ownerField).
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
async fn serve_authed_handler(
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
    if INLINE_SAFE_CONTENT_TYPES.iter().any(|safe| *safe == ess) {
        // Return the canonical static slice matching the allowlist entry so
        // the header value borrows 'static — case-normalized.
        let canonical = INLINE_SAFE_CONTENT_TYPES
            .iter()
            .copied()
            .find(|safe| *safe == ess)
            .expect("matched above");
        (canonical, false)
    } else {
        ("application/octet-stream", true)
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

#[cfg(test)]
mod client_ip_tests {
    use super::client_ip_key;
    use axum::http::{HeaderMap, HeaderValue};

    fn peer() -> std::net::IpAddr {
        "203.0.113.99".parse().unwrap()
    }

    fn headers_with(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(*k, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    // SEC-112: CF-Connecting-IP wins outright. A varying XFF with a constant
    // CF-Connecting-IP must share one bucket — the spoofable XFF entries are
    // ignored entirely when CF-Connecting-IP is present.
    #[test]
    fn cf_connecting_ip_wins_over_xff() {
        let h = headers_with(&[
            ("cf-connecting-ip", "198.51.100.10"),
            ("x-forwarded-for", "10.1.1.1, 10.2.2.2, 10.3.3.3"),
        ]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.10");
    }

    // SEC-112: the prior bug took the LEFTMOST XFF entry, which Cloudflare does
    // not set — it appends the real client as the RIGHTMOST hop. So under a
    // spoofed XFF like "fake-attacker-ip, real-client", the rightmost is what
    // Cloudflare observed.
    #[test]
    fn xff_takes_rightmost_hop_not_leftmost() {
        let h = headers_with(&[("x-forwarded-for", "10.4.4.4, 10.5.5.5, 198.51.100.20")]);
        assert_eq!(
            client_ip_key(&h, peer(), true),
            "198.51.100.20",
            "rightmost XFF hop is Cloudflare's observation"
        );
    }

    // SEC-112: a single-hop XFF (the common case) returns that hop.
    #[test]
    fn xff_single_hop() {
        let h = headers_with(&[("x-forwarded-for", "198.51.100.30")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.30");
    }

    // SEC-112: falling back to the connection peer when neither trusted header
    // is present (direct calls, no tunnel).
    #[test]
    fn falls_back_to_peer_when_no_proxy_headers() {
        let h = HeaderMap::new();
        assert_eq!(client_ip_key(&h, peer(), true), peer().to_string());
    }

    // SEC-112: empty/whitespace CF-Connecting-IP is ignored, not parsed as
    // the bucket key (which would create an empty-string bucket shared by
    // every malformed request).
    #[test]
    fn empty_cf_connecting_ip_falls_through() {
        let h = headers_with(&[
            ("cf-connecting-ip", "  "),
            ("x-forwarded-for", "198.51.100.40"),
        ]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.40");
    }

    // SEC-112: trailing/leading whitespace is trimmed so "1.2.3.4 " and
    // "1.2.3.4" share a bucket.
    #[test]
    fn cf_connecting_ip_is_trimmed() {
        let h = headers_with(&[("cf-connecting-ip", "  198.51.100.50  ")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.50");
    }

    // SEC-112: Forwarded (RFC 7239) is intentionally NOT consulted. The prior
    // implementation parsed it as a comma-list of IPs (wrong) and trusting it
    // reopens the spoofing vector. Confirm it is ignored in favor of peer.
    #[test]
    fn forwarded_header_is_ignored() {
        let h = headers_with(&[("forwarded", "for=198.51.100.99")]);
        assert_eq!(
            client_ip_key(&h, peer(), true),
            peer().to_string(),
            "Forwarded header must not be trusted — peer fallback wins"
        );
    }

    // SEC-201: with RTDB_TRUSTED_PROXY=false (the code default), the
    // forwarding headers are caller-controlled and must be ignored entirely —
    // the peer address wins, so header rotation cannot mint fresh rate-limit
    // buckets on a directly reachable deploy.
    #[test]
    fn untrusted_proxy_ignores_forwarding_headers() {
        let h = headers_with(&[
            ("cf-connecting-ip", "198.51.100.60"),
            ("x-forwarded-for", "10.6.6.6, 198.51.100.61"),
        ]);
        assert_eq!(
            client_ip_key(&h, peer(), false),
            peer().to_string(),
            "untrusted deploy must key on the peer address, not spoofable headers"
        );
    }

    // SEC-201: with RTDB_TRUSTED_PROXY=true the header path is active again
    // (the tests above all pass `true` — they cover the trusted-proxy path).
    #[test]
    fn trusted_proxy_reads_cf_connecting_ip() {
        let h = headers_with(&[("cf-connecting-ip", "198.51.100.70")]);
        assert_eq!(client_ip_key(&h, peer(), true), "198.51.100.70");
    }
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

/// Delete a stored file. Idempotent: deleting a missing id still returns
/// `{ ok: true }`. Both the per-db blob row and the global `storage_index`
/// row are removed, so the public URL 404s afterward.
///
/// SEC-118: per-row authorization runs before the delete — a non-owner
/// interactive caller gets 403, a missing id is a successful no-op (idempotent
/// — does not disclose existence either way).
async fn delete_handler(
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
async fn metadata_handler(
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
