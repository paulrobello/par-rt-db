//! One-shot data-plane handlers: `/api/query`, `/api/query-batch`,
//! `/api/mutate`, `/api/mutate-batch` (ARC-008 split; pure move from
//! the former single-file `http_api.rs`).

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue};

use crate::AppState;
use crate::auth::PrincipalCtx;
use crate::db::now_ms;
use crate::error::RtDbError;
use crate::http_api::{ApiJson, authed};
use crate::metrics::SlowQueryRecord;
use crate::query::{Query, QueryResult, compile_query, execute_query};
use crate::rate_limit::check_http_rate_limits;
use crate::txn::Transaction;

#[derive(Deserialize)]
pub(crate) struct QueryRequest {
    db: String,
    query: Query,
}

#[derive(Serialize)]
pub(crate) struct QueryResponse {
    result: QueryResult,
}

pub(crate) async fn query_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<QueryRequest>,
) -> Result<(HeaderMap, Json<QueryResponse>), RtDbError> {
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
    let server_time = HeaderValue::from_bytes(now_ms().to_string().as_bytes())
        .map_err(|_| RtDbError::internal("failed to encode server time header"))?;
    let mut response_headers = HeaderMap::new();
    response_headers.insert(
        HeaderName::from_static("x-rtdb-server-time-ms"),
        server_time,
    );
    Ok((response_headers, Json(QueryResponse { result })))
}

#[derive(Deserialize)]
pub(crate) struct BatchQueryRequest {
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
pub(crate) struct BatchQueryOutcome {
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
pub(crate) struct BatchQueryResponse {
    results: Vec<BatchQueryOutcome>,
}

/// Fan out over many queries against one db in a single round trip. Auth and
/// owner resolution run once for the whole request (same db, same principal —
/// mirrors `query_handler`); each query's outcome lands in its own aligned slot.
/// A per-query execution error becomes that slot's `{ok:false,error}` and never
/// fails the batch — only the db-level bearer/authorize gate returns a non-200
/// for the whole request.
pub(crate) async fn batch_query_handler(
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
pub(crate) struct MutateRequest {
    db: String,
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct MutateResponse {
    results: Vec<serde_json::Value>,
}

pub(crate) async fn mutate_handler(
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

/// SEC-119 mirror for the mutation transport: hard cap on the number of
/// transactions a single `POST /api/mutate-batch` may carry, rejected BEFORE
/// the bearer/authorize gate (fail-fast on abuse) — same reasoning as
/// `MAX_BATCH_QUERIES` above. 64 mirrors a generous round-trip budget; split
/// into multiple batched requests rather than one giant one.
const MAX_BATCH_TXNS: usize = 64;

/// One entry of a `/api/mutate-batch` request. `idempotencyKey`, when present,
/// opts THIS entry into the per-db dedup table: a retry of the batch (or the
/// whole request) replays that entry's first outcome instead of re-executing.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchMutateEntry {
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct BatchMutateRequest {
    db: String,
    txns: Vec<BatchMutateEntry>,
}

/// One slot of a `/api/mutate-batch` response, aligned by position with the
/// request's `txns` (same convention as `/api/query-batch` — no index field;
/// the client mirrors are extra=forbid on this shape). `ok` is always
/// present; on success `results` carries the txn's step results (the same
/// shape `/api/mutate` returns); on failure `error` carries the standard
/// `RtDbError` wire envelope. Exactly one of `results` / `error` accompanies
/// `ok` (omit-when-None).
#[derive(Serialize)]
pub(crate) struct BatchMutateOutcome {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    results: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RtDbError>,
}

#[derive(Serialize)]
pub(crate) struct BatchMutateResponse {
    results: Vec<BatchMutateOutcome>,
}

/// Fan out over N independent transactions against one db in a single round
/// trip. Deliberately NOT atomic — an atomic multi-step write is what the txn
/// DSL is for; this is transport efficiency only. Auth, the read-only gate,
/// and rate limits run once for the whole request (same db, same principal —
/// mirrors `batch_query_handler`); each entry executes through the same
/// `Committers::mutate` path as `/api/mutate`, so subscriptions, op-feed
/// taps, and per-entry idempotency replay all fire. A per-entry error becomes
/// that slot's `{ok:false,error}` and never fails the batch — only the
/// request-level gates return a non-200 for the whole request.
pub(crate) async fn mutate_batch_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<BatchMutateRequest>,
) -> Result<Json<BatchMutateResponse>, RtDbError> {
    if body.txns.is_empty() {
        return Err(RtDbError::bad_request("txns must not be empty"));
    }
    // SEC-119: reject an oversized batch before the bearer/authorize gate so an
    // unauthenticated abuser can't pin a worker on an N-txn serial fan-out.
    if body.txns.len() > MAX_BATCH_TXNS {
        return Err(RtDbError::bad_request(format!(
            "txn batch size {} exceeds maximum of {MAX_BATCH_TXNS}",
            body.txns.len()
        )));
    }
    let principal = authed(&state, &headers, &body.db).await?;
    if principal.is_read_only() {
        return Err(RtDbError::forbidden("read-only token cannot mutate"));
    }
    check_http_rate_limits(&state, &principal, &body.db).await?;

    let ctx = principal.row_ctx();
    let mut results = Vec::with_capacity(body.txns.len());
    for entry in body.txns.into_iter() {
        let t = Instant::now();
        let outcome = match state
            .realtime
            .committers
            .mutate(&body.db, entry.idempotency_key, entry.txn, ctx.clone())
            .await
        {
            Ok(committed) => BatchMutateOutcome {
                ok: true,
                results: Some(committed.results),
                error: None,
            },
            Err(err) => BatchMutateOutcome {
                ok: false,
                results: None,
                error: Some(err),
            },
        };
        if outcome.ok {
            state
                .runtime
                .metrics
                .record_mutation_duration(t.elapsed().as_micros() as u64);
            state.runtime.metrics.record_mutation();
        }
        results.push(outcome);
    }
    Ok(Json(BatchMutateResponse { results }))
}
