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
use crate::txn::{Transaction, worst_case_affected};
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
/// committer with `owner = None`. `max_affected_docs` bounds the worst-case
/// number of documents this mutation could touch (per-id steps count 1 each;
/// each by-query step counts up to its `limit`, default `MAX_BY_QUERY_ROWS`),
/// not the raw step count. Rejecting over-budget here, before the committer
/// turn, keeps a runaway admin mutation off the single-writer. A by-query step
/// can touch many rows, so the prior step-count comparison silently allowed a
/// 100-step admin mutation to affect up to 100,000 documents under a cap
/// advertised as 100. `execute_txn` re-checks its own hard aggregate budget
/// (`MAX_AFFECTED_ROWS_PER_TXN`), so this is a per-instance tightening on top.
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
    let worst = worst_case_affected(&body.txn);
    if worst > cap {
        return Err(RtDbError::bad_request(format!(
            "mutation could affect up to {worst} document(s), exceeding the limit of {cap}"
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
