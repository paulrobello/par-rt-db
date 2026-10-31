//! Admin purge route: early reclamation of insertOnly rows by predicate —
//! the sanctioned answer to "a client bug flooded an append-only table" that
//! does not wait out the TTL and never touches Postgres directly. Owner
//! direction 2026-10-31: the cleanup path is an admin-key rt-db route;
//! direct Postgres remains permanently off-limits.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::{AdmissionEpoch, ApiJson};
use crate::query::{FilterExpr, compile_scan_where};
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
    /// [`MAX_PURGE_ROWS`].
    #[serde(default)]
    limit: Option<u32>,
    /// The `confirmToken` a preview returns. Absent → preview only (count
    /// + token, nothing deleted).
    #[serde(default)]
    confirm: Option<String>,
}

/// Phase-shaped reply: a preview carries `matching`/`confirmToken`/`limit`;
/// a confirmed purge carries `deleted`/`truncated`. Each phase sets exactly
/// its own fields (skipped when absent).
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

/// Compiles the predicate against the live schema and counts the matching
/// rows. Uses the bypass principal (`owner = None`) so the count is the full
/// matching set — the admin gate is the authorization, matching the purge
/// execution itself.
async fn count_matching(
    state: &AppState,
    db: &str,
    table: &str,
    filter: Option<&FilterExpr>,
) -> Result<i64, RtDbError> {
    let schema = state.schemas.get(&state.pool, db).await?;
    let table_def = schema
        .tables
        .get(table)
        .ok_or_else(|| RtDbError::not_found(format!("table '{table}' not found")))?;
    let (where_sql, binds, _limit_ph) = compile_scan_where(
        table_def,
        &crate::auth::PrincipalCtx::bypass(),
        None,
        filter,
    )?;
    let pg_schema_name = crate::ddl::pg_schema(db);
    let table_ident = crate::ddl::pg_table(table);
    let sql = if where_sql.is_empty() {
        format!("SELECT count(*) FROM \"{pg_schema_name}\".\"{table_ident}\"")
    } else {
        format!("SELECT count(*) FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {where_sql}")
    };
    let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
    for bind in binds {
        query = match bind {
            crate::dsl::EqBind::Text(v) => query.bind(v),
            crate::dsl::EqBind::Num(v) => query.bind(v),
            crate::dsl::EqBind::Bool(v) => query.bind(v),
            crate::dsl::EqBind::I64(v) => query.bind(v),
        };
    }
    Ok(query.fetch_one(&state.pool).await?)
}

/// Deterministic, stateless confirm token over the purge's identity: db,
/// table, serialized filter, and the current matching count. Changing any
/// input (rows added or removed since the preview) changes the token, so a
/// stale handshake refuses with 409 and forces a re-preview. Deterministic is
/// fine here: the admin key already authorizes the action, so the token is an
/// operator-safety handshake, not a security boundary.
fn confirm_token(db: &str, table: &str, filter: Option<&FilterExpr>, matching: i64) -> String {
    let filter_json =
        serde_json::to_string(&filter).unwrap_or_else(|_| "unserializable-filter".to_string());
    let material = format!("purge|{db}|{table}|{filter_json}|{matching}");
    crate::storage::sha256_hex_bytes(material.as_bytes())
}

/// `POST /admin/db/{db}/purge` — two-phase reclamation of insertOnly rows.
/// Call without `confirm` for a preview (`{matching, confirmToken, limit}`);
/// call again with the returned `confirmToken` to delete up to `limit` rows
/// (default and cap [`MAX_PURGE_ROWS`]). A `truncated: true` reply means rows
/// still match and the confirm call should be re-run until it reports
/// `truncated: false` and `deleted: 0`. Audit rows land in `rtdb.audit_log`
/// with `source = "purge"` via the committer's four-tap publication.
pub(super) async fn admin_purge(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Extension(admission_epoch): Extension<AdmissionEpoch>,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<PurgeRequest>,
) -> Result<Json<PurgeResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
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
    let matching = count_matching(&state, &db, &body.table, body.filter.as_ref()).await?;
    let expected = confirm_token(&db, &body.table, body.filter.as_ref(), matching);
    match body.confirm {
        None => Ok(Json(PurgeResponse {
            matching: Some(matching),
            confirm_token: Some(expected),
            limit: Some(limit),
            deleted: None,
            truncated: None,
        })),
        Some(confirm) if confirm == expected => {
            let outcome = state
                .realtime
                .committers
                .purge(
                    &db,
                    body.table.clone(),
                    body.filter,
                    limit,
                    admission_epoch.0,
                )
                .await?;
            state.runtime.metrics.record_mutation();
            Ok(Json(PurgeResponse {
                matching: None,
                confirm_token: None,
                limit: None,
                deleted: Some(outcome.deleted),
                truncated: Some(outcome.truncated),
            }))
        }
        Some(_) => Err(RtDbError::precondition(format!(
            "confirm token does not match the current state ({matching} row(s) now match \
             '{}'); re-run the preview to get a fresh token",
            body.table
        ))),
    }
}
