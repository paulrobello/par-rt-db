//! Admin document access: query and mutate across every database with
//! `owner = None` (bypassing per-row `ownerField`), through the normal query /
//! committer paths.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::query::{Query, QueryResult, execute_query};
use crate::txn::Transaction;
use crate::{AppState, db};

use super::require_admin;

#[derive(Deserialize)]
pub(super) struct AdminQueryRequest {
    query: Query,
}

#[derive(Serialize)]
pub(super) struct AdminQueryResponse {
    result: QueryResult,
}

/// `POST /admin/db/{db}/query` — admin document read. `owner = None` bypasses
/// per-row `ownerField`, so an admin sees every row in every table.
pub(super) async fn admin_query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminQueryRequest>,
) -> Result<Json<AdminQueryResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    let result = execute_query(
        &state.pool,
        &db,
        &schema,
        &body.query,
        &crate::auth::PrincipalCtx::bypass(),
    )
    .await?;
    state.runtime.metrics.record_query();
    Ok(Json(AdminQueryResponse { result }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AdminMutateRequest {
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
pub(super) struct AdminMutateResponse {
    results: Vec<serde_json::Value>,
}

/// `POST /admin/db/{db}/mutate` — admin document write through the existing
/// committer with `owner = None`. The step-count cap is the server-side
/// guardrail: each step touches at most one document, so rejecting over-cap
/// here guarantees an over-cap mutation never reaches the committer (never
/// becomes durable).
pub(super) async fn admin_mutate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminMutateRequest>,
) -> Result<Json<AdminMutateResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let cap = state.config.max_affected_docs;
    if body.txn.steps.len() > cap {
        return Err(RtDbError::bad_request(format!(
            "mutation has {} step(s), exceeding the limit of {cap}",
            body.txn.steps.len()
        )));
    }
    let outcome = state
        .realtime
        .committers
        .mutate(
            &db,
            body.idempotency_key,
            body.txn,
            crate::auth::PrincipalCtx::bypass(),
        )
        .await?;
    state.runtime.metrics.record_mutation();
    Ok(Json(AdminMutateResponse {
        results: outcome.results,
    }))
}
