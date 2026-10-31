//! Admin purge route: early reclamation of insertOnly rows by predicate —
//! the sanctioned answer to "a client bug flooded an append-only table" that
//! does not wait out the TTL and never touches Postgres directly. Owner
//! direction 2026-10-31: the route is admin-key-only (an OAuth allowlist
//! session may browse, never purge), matching the key-only gate on schema
//! restore; direct Postgres remains permanently off-limits.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use super::AdminPrincipal;
use crate::committer::PurgeResult;
use crate::error::RtDbError;
use crate::http_api::{AdmissionEpoch, ApiJson};
use crate::query::FilterExpr;
use crate::{AppState, db};

/// Per-call cap on rows one purge call may delete; a reply with
/// `truncated: true` means rows still match and the call should be re-run.
/// Above `MAX_BY_QUERY_ROWS` because the admin gate is the authorization and
/// the serialized committer turn is the bound, but bounded like the TTL
/// reaper's batch so one call cannot monopolize the single writer.
pub(crate) const MAX_PURGE_ROWS: u32 = 10_000;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PurgeRequest {
    table: String,
    /// The DSL filter (same wire shape as a Query's `where`) selecting the
    /// rows to remove. Omit to purge every row of the table.
    #[serde(default)]
    filter: Option<FilterExpr>,
    /// Per-call row cap, clamped to [1, MAX_PURGE_ROWS]; default
    /// [`MAX_PURGE_ROWS`]. Bound into the confirm token, so confirming with a
    /// different limit than the preview refuses with 409.
    #[serde(default)]
    limit: Option<u32>,
    /// The `confirmToken` a preview returns. Absent → preview only (count
    /// + token, nothing deleted).
    #[serde(default)]
    confirm: Option<String>,
}

/// Phase-shaped reply: a preview carries `matching`/`confirmToken`/`limit`;
/// a confirmed purge carries `deleted`/`truncated`. Each phase sets exactly
/// its own fields (skipped when absent) — the wire contract the SDKs mirror.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PurgeResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    matching: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    confirm_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deleted: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    truncated: Option<bool>,
}

/// `POST /admin/db/{db}/purge` — two-phase reclamation of insertOnly rows.
/// Call without `confirm` for a preview (`{matching, confirmToken, limit}`);
/// call again with the returned `confirmToken` to delete up to `limit` rows
/// (default and cap [`MAX_PURGE_ROWS`]). A `truncated: true` reply means rows
/// still match and the confirm call should be re-run until it reports
/// `truncated: false` and `deleted: 0`. Audit rows land in `rtdb.audit_log`
/// with `source = "purge"` in the delete's own transaction; the durable
/// change feed records the deletes the same way.
pub(super) async fn admin_purge(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Extension(admission_epoch): Extension<AdmissionEpoch>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<PurgeRequest>,
) -> Result<Json<PurgeResponse>, RtDbError> {
    // Owner direction 2026-10-31: admin-key only — an OAuth allowlist session
    // (User) may never purge, matching the key-only gate on schema restore.
    if !matches!(principal, AdminPrincipal::Key) {
        return Err(RtDbError::forbidden("purge requires the admin key"));
    }
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // Pool fast-fails only — the committer arm re-checks both transactionally
    // inside the guarded transaction.
    if db::is_quiesced(&state.pool, &db).await? {
        return Err(RtDbError::read_only());
    }
    if db::is_read_only(&state.pool, &db).await? {
        return Err(RtDbError::read_only());
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    let table_def = schema
        .tables
        .get(&body.table)
        .ok_or_else(|| RtDbError::not_found(format!("table '{}' not found", body.table)))?;
    if !table_def.insert_only {
        return Err(RtDbError::bad_request(format!(
            "purge applies only to insertOnly tables ('{}' is not insertOnly); \
             use admin mutate with a DeleteByQuery step for other tables",
            body.table
        )));
    }
    let limit = body
        .limit
        .unwrap_or(MAX_PURGE_ROWS)
        .clamp(1, MAX_PURGE_ROWS);
    let result = state
        .realtime
        .committers
        .purge(
            &db,
            body.table.clone(),
            body.filter,
            limit,
            body.confirm,
            admission_epoch.0,
        )
        .await?;
    if let PurgeResult::Confirmed { .. } = &result {
        state.runtime.metrics.record_mutation();
    }
    // Wire shapes the SDK agents are implementing against (unchanged):
    // preview → {matching, confirmToken, limit}; confirmed → {deleted, truncated}.
    Ok(Json(match result {
        PurgeResult::Preview {
            matching,
            confirm_token,
            limit,
        } => PurgeResponse {
            matching: Some(matching),
            confirm_token: Some(confirm_token),
            limit: Some(limit),
            deleted: None,
            truncated: None,
        },
        PurgeResult::Confirmed { deleted, truncated } => PurgeResponse {
            matching: None,
            confirm_token: None,
            limit: None,
            deleted: Some(deleted),
            truncated: Some(truncated),
        },
    }))
}
