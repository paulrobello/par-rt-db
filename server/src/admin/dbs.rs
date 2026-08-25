//! Database lifecycle admin routes: create/delete/list, push schema, export/
//! import/clone, and per-db stats (table rows/sizes + quota usage).

use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::{Deserialize, Serialize};

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::schema::{SchemaDef, SchemaDefExt};
use crate::{AppState, db, snapshot};

use super::OkResponse;

#[derive(Deserialize)]
pub(super) struct CreateDbRequest {
    name: String,
}

pub(super) async fn create_db(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<CreateDbRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    db::create_database(&state.pool, &body.name).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct DeleteDbRequest {
    name: String,
    confirm: String,
}

/// `POST /admin/delete-db` — admin-gated, typed-confirmation-guarded deletion
/// of a database. `confirm` must equal `name` exactly (a typed guard against
/// accidental deletion; the dashboard gates its delete button on the same
/// match). Beyond `drop_database`'s durable cleanup (schema CASCADE + rows in
/// `rtdb_auth.databases` / `machine_tokens` / `allowlist` / `rtdb.storage_index`),
/// evicts the in-memory state too: cached schema, the subscription shard, and
/// the committer channel mapping. Live `/sync` connections to the deleted db
/// will fail on their next op — acceptable for a deleted database.
pub(super) async fn delete_db(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<DeleteDbRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    if body.confirm != body.name {
        return Err(RtDbError::bad_request(
            "confirmation does not match database name",
        ));
    }
    db::drop_database(&state.pool, &body.name).await?;
    state.schemas.invalidate(&body.name).await;
    state.realtime.subs.drop_db(&body.name).await;
    state.realtime.committers.drop_db(&body.name).await;
    // ENH-011: drop the cached storage-usage entry so a future db that reuses
    // this name doesn't read a stale byte count from before the drop.
    state.limits.quotas.evict(&body.name);
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct PushSchemaRequest {
    db: String,
    schema: SchemaDef,
}

pub(super) async fn push_schema(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(body): ApiJson<PushSchemaRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    body.schema
        .check_table_quota(state.runtime.hot.load().max_tables_per_db)
        .inspect_err(|_e| {
            state
                .runtime
                .metrics
                .record_quota_rejection(&body.db, crate::metrics::QuotaKind::Tables);
        })?;
    // Routed through the committer (not a direct `ddl::push_schema`): a push
    // can backfill document values (ttl defaultDurationMs, computed fields),
    // and those writes belong in the single-writer turn — which also re-runs
    // the backfilled tables' subscriptions (`handle_push_schema`).
    state
        .realtime
        .committers
        .push_schema(&body.db, body.schema)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Serialize)]
pub(super) struct DatabasesResponse {
    databases: Vec<String>,
}

pub(super) async fn list_dbs(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Result<Json<DatabasesResponse>, RtDbError> {
    let databases = db::list_databases(&state.pool).await?;
    Ok(Json(DatabasesResponse { databases }))
}

#[derive(Deserialize)]
pub(super) struct ExportDbParams {
    db: String,
}

/// Streams `db`'s current schema and every document in every table as JSONL (see
/// `snapshot::export_database`); a plain app-level companion to host-level
/// `pg_dump` for seed data and clone-to-dev workflows.
pub(super) async fn export_db(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<ExportDbParams>,
) -> Result<Response, RtDbError> {
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &params.db).await?;
    let body = snapshot::export_database(&state.pool, &params.db, &schema).await?;

    Response::builder()
        .header(axum::http::header::CONTENT_TYPE, "application/x-ndjson")
        .body(Body::from(body))
        .map_err(|err| RtDbError::internal(format!("failed to build export response: {err}")))
}

#[derive(Deserialize)]
pub(super) struct ImportDbParams {
    db: String,
}

/// Loads a JSONL snapshot produced by `export_db` back into `db` (see
/// `snapshot::import_database`), refreshing the schema cache with whatever schema
/// the snapshot applied.
pub(super) async fn import_db(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<ImportDbParams>,
    body: String,
) -> Result<Json<OkResponse>, RtDbError> {
    match snapshot::import_database(&state.pool, &params.db, &body).await {
        Ok(applied) => {
            state.schemas.put(&params.db, applied).await;
            Ok(Json(OkResponse { ok: true }))
        }
        Err(err) => {
            state.schemas.invalidate(&params.db).await;
            Err(err)
        }
    }
}

#[derive(Deserialize)]
pub(super) struct CloneDbParams {
    from: String,
    to: String,
}

/// Clones `from` into a freshly created `to` in one server-side step: exports
/// `from`'s schema + documents and replays them into `to` (which must not
/// already exist), preserving ids, `createdAt`, and `version`. Scope matches
/// `export-db`/`import-db` exactly — schema and documents only; storage blobs
/// and scheduled transactions are not part of the snapshot format and are not
/// copied. On an import failure after `to` is created, the empty `to` database
/// is left in place for the operator to delete (consistent with `import-db`'s
/// no-cleanup behavior) and its cache entry is dropped. See ENH-009.
pub(super) async fn clone_db(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    QueryParams(params): QueryParams<CloneDbParams>,
) -> Result<Json<OkResponse>, RtDbError> {
    if params.from == params.to {
        return Err(RtDbError::bad_request(
            "cannot clone a database onto itself; choose a different destination name",
        ));
    }
    if !db::database_exists(&state.pool, &params.from).await? {
        return Err(RtDbError::not_found("source database not found"));
    }
    if db::database_exists(&state.pool, &params.to).await? {
        return Err(RtDbError::bad_request(
            "destination database already exists",
        ));
    }
    // Export before creating `to` so a read failure leaves nothing behind.
    let schema = state.schemas.get(&state.pool, &params.from).await?;
    let jsonl = snapshot::export_database(&state.pool, &params.from, &schema).await?;
    db::create_database(&state.pool, &params.to).await?;
    match snapshot::import_database(&state.pool, &params.to, &jsonl).await {
        Ok(applied) => {
            state.schemas.put(&params.to, applied).await;
            Ok(Json(OkResponse { ok: true }))
        }
        Err(err) => {
            state.schemas.invalidate(&params.to).await;
            Err(err)
        }
    }
}

#[derive(Serialize)]
pub(super) struct TableStat {
    name: String,
    #[serde(rename = "rowCount")]
    row_count: i64,
    #[serde(rename = "sizeBytes")]
    size_bytes: i64,
}

#[derive(Serialize)]
pub(super) struct DbStatsResponse {
    tables: Vec<TableStat>,
    #[serde(rename = "totalSizeBytes")]
    total_size_bytes: i64,
    #[serde(rename = "tablesQuota")]
    tables_quota: usize,
    #[serde(rename = "tablesUsed")]
    tables_used: usize,
    #[serde(rename = "storageQuotaBytes")]
    storage_quota_bytes: u64,
    #[serde(rename = "storageUsedBytes")]
    storage_used_bytes: u64,
    #[serde(rename = "subsQuota")]
    subs_quota: usize,
    #[serde(rename = "subsUsed")]
    subs_used: usize,
}

pub(super) async fn db_stats(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<DbStatsResponse>, RtDbError> {
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema_def = state.schemas.get(&state.pool, &db).await?;
    let pg_schema = crate::ddl::pg_schema(&db);

    let mut tables = Vec::with_capacity(schema_def.tables.len());
    let mut total_size_bytes: i64 = 0;
    for name in schema_def.tables.keys() {
        let pg_table = crate::ddl::pg_table(name);
        // Identifiers are system-generated from the validated db name + pushed (lowercased,
        // length-capped) table name, so double-quoting via format! is safe — same pattern as
        // mutation_log.rs. COUNT always returns exactly one row.
        let count_sql = format!("SELECT COUNT(*) FROM \"{pg_schema}\".\"{pg_table}\"");
        let row_count: i64 = sqlx::query_scalar(&count_sql)
            .fetch_one(&state.pool)
            .await?;
        // Size via the injection-safe %I.%I regclass form, names $n-bound.
        let size_bytes: i64 =
            sqlx::query_scalar("SELECT pg_total_relation_size(format('%I.%I', $1, $2))::bigint")
                .bind(&pg_schema)
                .bind(&pg_table)
                .fetch_one(&state.pool)
                .await?;
        total_size_bytes += size_bytes;
        tables.push(TableStat {
            name: name.clone(),
            row_count,
            size_bytes,
        });
    }
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let hot = state.runtime.hot.load();
    let subs_used = state.realtime.subs.count_for_db(&db).await;
    Ok(Json(DbStatsResponse {
        tables,
        total_size_bytes,
        tables_quota: hot.max_tables_per_db,
        tables_used: schema_def.tables.len(),
        storage_quota_bytes: hot.max_storage_bytes_per_db,
        storage_used_bytes: total_size_bytes.max(0) as u64,
        subs_quota: hot.max_subs_per_db,
        subs_used,
    }))
}

// ============================================================================
// SEC-103: per-database anonymous-access toggle.
// ============================================================================

/// `GET /admin/db/{db}/anonymous-access` — returns whether the database has
/// opted in to anonymous principal access. The instance-wide boot gate
/// `RTDB_AUTH_ANONYMOUS_ENABLED` is NOT reflected here (it is a boot-time
/// config, not a per-db property); this reports only the per-db flag.
#[derive(Serialize)]
pub(super) struct AnonymousAccessResponse {
    enabled: bool,
}

pub(super) async fn get_anonymous_access(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<AnonymousAccessResponse>, RtDbError> {
    let (enabled,): (bool,) = sqlx::query_as(
        "SELECT COALESCE((
            SELECT anonymous_enabled FROM rtdb_auth.databases WHERE name = $1
        ), FALSE)",
    )
    .bind(&db)
    .fetch_one(&state.pool)
    .await?;
    Ok(Json(AnonymousAccessResponse { enabled }))
}

/// `PATCH /admin/db/{db}/anonymous-access` — opts the database in to (or out
/// of) anonymous principal access. The instance-wide boot gate
/// `RTDB_AUTH_ANONYMOUS_ENABLED` must be on for the per-db flag to take effect
/// (anon minting is refused at `POST /auth/anonymous` while the master kill is
/// off); this per-db flag is the additional gate checked at `authorize`. Sets
/// the `rtdb_auth.databases.anonymous_enabled` column; the database must exist.
#[derive(Deserialize)]
pub(super) struct PatchAnonymousAccessRequest {
    enabled: bool,
}

pub(super) async fn patch_anonymous_access(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<PatchAnonymousAccessRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    let result =
        sqlx::query("UPDATE rtdb_auth.databases SET anonymous_enabled = $1 WHERE name = $2")
            .bind(body.enabled)
            .bind(&db)
            .execute(&state.pool)
            .await?;
    if result.rows_affected() == 0 {
        return Err(RtDbError::not_found(format!(
            "database '{db}' is not registered (create it before toggling anonymous access)"
        )));
    }
    tracing::info!(db = %db, enabled = body.enabled, "anonymous-access toggled");
    Ok(Json(OkResponse { ok: true }))
}
