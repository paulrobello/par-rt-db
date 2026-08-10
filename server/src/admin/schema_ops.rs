//! Admin schema routes: read/preview schema, browse schema history, restore to
//! a captured snapshot, and run a directive migration through the committer.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Extension, Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::schema::SchemaDef;
use crate::{AppState, db, schema_history};

use super::AdminPrincipal;

pub(super) async fn get_schema(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<crate::schema::SchemaDef>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    Ok(Json((*schema).clone()))
}

#[derive(Deserialize)]
pub(super) struct HistoryParams {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
}

/// `GET /admin/db/{db}/schema/history` — newest-first list of captured schema
/// snapshots (metadata only; the blob lives at the per-version route). Clamps
/// `limit` to [1, 1000] and floors `offset` at 0. Always on: `schema_history`
/// self-heals on first read for databases created before this feature shipped.
pub(super) async fn schema_history_list(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    QueryParams(params): QueryParams<HistoryParams>,
) -> Result<Json<serde_json::Value>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0).max(0);
    let entries = schema_history::list(&state.pool, &db, limit, offset).await?;
    Ok(Json(serde_json::json!({ "entries": entries })))
}

/// `GET /admin/db/{db}/schema/history/{version}` — one full snapshot, including
/// the `schema` JSON blob. 404 on a database or version that does not exist.
pub(super) async fn schema_history_get(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path((db, version)): Path<(String, i64)>,
) -> Result<Json<schema_history::HistoryEntry>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    schema_history::get(&state.pool, &db, version)
        .await?
        .map(Json)
        .ok_or_else(|| RtDbError::not_found("schema version not found"))
}

#[derive(Deserialize)]
pub(super) struct PreviewSchemaRequest {
    schema: SchemaDef,
}

/// `POST /admin/db/{db}/schema/preview` — advisory diff of a pending schema
/// against the database's currently-applied one. Validates the pending schema
/// (invalid → 400) and reports what an additive-only push would ADD and what it
/// would have to drop or change (and therefore would reject). Does NOT apply,
/// does NOT touch `state.schemas` — `ddl::push_schema` remains the
/// authoritative gate.
pub(super) async fn preview_schema(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<PreviewSchemaRequest>,
) -> Result<Json<crate::schema_diff::SchemaDiff>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    body.schema.validate()?;
    let current = db::load_schema(&state.pool, &db).await?;
    let diff = crate::schema_diff::diff(current.as_ref(), &body.schema);
    Ok(Json(diff))
}

/// `POST /admin/db/{db}/migrate` — admin schema migration through the committer
/// (serialized with concurrent writes; runs the subs/op-feed/audit/webhook taps
/// on the durable result). `dryRun` rolls back and publishes nothing. Reuses
/// `migrate::MigrateRequest` directly: it already carries `rename_all =
/// "camelCase"`, so the wire body is `{directives, dryRun}`.
pub(super) async fn admin_migrate(
    State(state): State<Arc<AppState>>,
    Extension(principal): Extension<AdminPrincipal>,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<crate::migrate::MigrateRequest>,
) -> Result<Json<crate::migrate::MigrateResult>, RtDbError> {
    // SEC-108: `principal` is stashed by `require_admin_mw`.
    // SEC-107: an `evalExpr` directive interpolates client-supplied SQL text
    // (`expr`/`where`) directly into an `UPDATE … WHERE` executed inside the
    // committer's serialized turn. A denylist over SQL text cannot be made
    // sound, so containment is enforced structurally here: `evalExpr` is
    // admitted only under the root `admin_key` (`AdminPrincipal::Key`), never
    // under a delegated/OAuth-allowlist dashboard admin (`AdminPrincipal::User`).
    // The root admin_key holder already has full server/DB access, so evalExpr
    // under it does not expand their reach; a delegated admin must not reach
    // `rtdb_auth.machine_tokens`/`sessions`/`admins` or other tenants' documents
    // through it. All other directives (addIndex/renameField/etc.) remain
    // available to allowlist admins.
    if body
        .directives
        .iter()
        .any(|d| matches!(d, crate::migrate::Directive::EvalExpr { .. }))
        && !matches!(principal, AdminPrincipal::Key)
    {
        return Err(RtDbError::forbidden(
            "evalExpr directive requires the root admin key",
        ));
    }
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let result = state.realtime.committers.migrate(&db, body).await?;
    Ok(Json(result))
}

#[derive(Deserialize)]
pub(super) struct RestoreSchemaRequest {
    version: i64,
    confirm: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RestoreSchemaResponse {
    ok: bool,
    restored_to: i64,
}

/// `POST /admin/db/{db}/schema/restore` — restore the database's schema shape
/// (and therefore its live `doc`-jsonb-compatible table/index shape) to a
/// captured `schema_history` snapshot. Serialized through the committer like
/// `admin_migrate`; the destructive reconcile runs inside the committer's
/// serialized turn. `confirm` must equal the db name (typed guard, mirrors
/// `delete-db`). Body: `{version, confirm}`; the target snapshot is resolved by
/// `version` from `schema_history`. The restore captures the OUTGOING schema
/// first (so it is itself undoable) and the INCOMING schema after (so the
/// latest history row still equals the live schema). Document jsonb data on
/// surviving tables is preserved — only redundant typed/index copies are
/// dropped, never the `doc` column.
pub(super) async fn restore_schema(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<RestoreSchemaRequest>,
) -> Result<Json<RestoreSchemaResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // Typed guard: confirm must equal the db name (mirrors delete-db).
    if body.confirm != db {
        return Err(RtDbError::bad_request(
            "confirm must equal the database name",
        ));
    }
    let restored_to = state
        .realtime
        .committers
        .restore_schema(&db, body.version)
        .await?;
    Ok(Json(RestoreSchemaResponse {
        ok: true,
        restored_to,
    }))
}
