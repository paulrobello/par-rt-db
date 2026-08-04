use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, FromRequest, Path, Query as QueryParams, Request, State};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode, header, header::CONTENT_TYPE, header::SET_COOKIE,
};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::db::now_ms;
use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::protocol::{ScheduleInfo, ScheduleWhen};
use crate::query::{Query, QueryResult, execute_query};
use crate::scheduler;
use crate::schema::SchemaDef;
use crate::txn::Transaction;
use crate::{AppState, auth, db, ddl, snapshot, storage};

/// Who an admin request was made as: the raw admin key (CLI/automation) or an
/// OAuth user on the server-wide admin allowlist (browser dashboard). The
/// `User` variant is unit today — admin activity is currently attributed only
/// through the op-feed's `owner` field (which is `None` for admin writes); if
/// per-principal audit logging is added later, thread the resolved `Principal`
/// back in here.
pub(crate) enum AdminPrincipal {
    Key,
    User,
}

fn bearer_value(headers: &HeaderMap) -> Result<&str, RtDbError> {
    if let Some(v) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Ok(v);
    }
    // SEC-001: dashboard cookie path. The browser sends the HttpOnly
    // `rtdb_session` cookie automatically on same-origin requests — including
    // the `/admin/stream` WS upgrade — so JS never holds the admin key. Header
    // still wins (CLI/automation/machine tokens).
    auth::cookie::session_cookie(headers)
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))
}

/// Bearer credential carried in a WebSocket subprotocol. Browsers cannot set
/// the `Authorization` header on a WS handshake, so the dashboard offers
/// `Sec-WebSocket-Protocol: rtdb-admin.<token>` instead (a header browsers CAN
/// set); this pulls the token back out. The subprotocol is an HTTP header during
/// the handshake — it never enters the URL, so it is not captured by access logs
/// the way a `?token=` query param would be.
fn bearer_from_subprotocol(headers: &HeaderMap) -> Result<&str, RtDbError> {
    let proto = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))?;
    for entry in proto.split(',') {
        if let Some(rest) = entry.trim().strip_prefix("rtdb-admin.")
            && !rest.is_empty()
        {
            return Ok(rest);
        }
    }
    Err(RtDbError::unauthorized("missing admin bearer token"))
}

/// Authenticate a raw bearer credential as an admin: the admin key first
/// (constant-time compare, no DB lookup), then a resolved session/machine
/// principal admitted only if it is an OAuth user on `rtdb_auth.admins`. Shared
/// by the header path and the WS-subprotocol path so both enforce identically.
pub(crate) async fn authenticate_admin(
    state: &AppState,
    token: &str,
) -> Result<AdminPrincipal, RtDbError> {
    if bool::from(token.as_bytes().ct_eq(state.config.admin_key.as_bytes())) {
        return Ok(AdminPrincipal::Key);
    }
    let principal = match auth::resolve_bearer(&state.pool, token).await {
        Ok(principal) => principal,
        Err(_) => return Err(RtDbError::unauthorized("invalid admin credential")),
    };
    if auth::is_admin(&state.pool, &principal).await {
        Ok(AdminPrincipal::User)
    } else {
        Err(RtDbError::forbidden("not a dashboard admin"))
    }
}

/// Admin gate for ordinary HTTP routes — reads `Authorization: Bearer <token>`.
pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AdminPrincipal, RtDbError> {
    authenticate_admin(state, bearer_value(headers)?).await
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminLoginRequest {
    admin_key: String,
}

/// `POST /admin/login` — validates the admin key (constant-time, the same
/// compare `authenticate_admin` runs) and, on success, issues the SEC-001
/// HttpOnly session cookie. On a bad key we 401 without touching the cookie.
/// The credential written is `state.config.admin_key` (the trusted configured
/// value), never the raw request body, so a `;`-laden guess cannot inject cookie
/// attributes — `set_session_cookie` validates regardless.
async fn admin_login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AdminLoginRequest>,
) -> Result<Response, RtDbError> {
    let valid = body
        .admin_key
        .as_bytes()
        .ct_eq(state.config.admin_key.as_bytes());
    if !bool::from(valid) {
        return Err(RtDbError::unauthorized("invalid admin key"));
    }
    let cookie = auth::cookie::set_session_cookie(
        &state.config.admin_key,
        auth::cookie::request_is_secure(&headers),
    )?;
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut().insert(SET_COOKIE, cookie);
    Ok(resp)
}

/// `POST /admin/logout` — clears the SEC-001 session cookie.
async fn admin_logout() -> Response {
    let mut resp = StatusCode::NO_CONTENT.into_response();
    resp.headers_mut()
        .insert(SET_COOKIE, auth::cookie::clear_session_cookie());
    resp
}

#[derive(Serialize)]
struct OkResponse {
    ok: bool,
}

#[derive(Deserialize)]
struct CreateDbRequest {
    name: String,
}

async fn create_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<CreateDbRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    db::create_database(&state.pool, &body.name).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct DeleteDbRequest {
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
async fn delete_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<DeleteDbRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if body.confirm != body.name {
        return Err(RtDbError::bad_request(
            "confirmation does not match database name",
        ));
    }
    db::drop_database(&state.pool, &body.name).await?;
    state.schemas.invalidate(&body.name).await;
    state.realtime.subs.drop_db(&body.name).await;
    state.realtime.committers.drop_db(&body.name).await;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct PushSchemaRequest {
    db: String,
    schema: SchemaDef,
}

async fn push_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<PushSchemaRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let applied = ddl::push_schema(&state.pool, &body.db, body.schema).await?;
    state.schemas.put(&body.db, applied).await;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Serialize)]
struct DatabasesResponse {
    databases: Vec<String>,
}

async fn list_dbs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<DatabasesResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let databases = db::list_databases(&state.pool).await?;
    Ok(Json(DatabasesResponse { databases }))
}

#[derive(Deserialize)]
struct MintTokenRequest {
    db: String,
    name: String,
}

#[derive(Serialize)]
struct MintTokenResponse {
    #[serde(rename = "tokenId")]
    token_id: String,
    token: String,
}

async fn mint_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<MintTokenRequest>,
) -> Result<Json<MintTokenResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &body.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let (token_id, token) = auth::tokens::mint_token(&state.pool, &body.db, &body.name).await?;
    Ok(Json(MintTokenResponse { token_id, token }))
}

#[derive(Deserialize)]
struct RevokeTokenRequest {
    #[serde(rename = "tokenId")]
    token_id: String,
}

async fn revoke_token(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RevokeTokenRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    auth::tokens::revoke_token(&state.pool, &body.token_id).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct AllowlistWriteRequest {
    db: String,
    action: String,
    email: String,
}

async fn allowlist_write(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AllowlistWriteRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &body.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let email = body.email.to_lowercase();

    match body.action.as_str() {
        "add" => {
            sqlx::query(
                "INSERT INTO rtdb_auth.allowlist (db_name, email) VALUES ($1, $2) \
                 ON CONFLICT (db_name, email) DO NOTHING",
            )
            .bind(&body.db)
            .bind(&email)
            .execute(&state.pool)
            .await?;
        }
        "remove" => {
            sqlx::query("DELETE FROM rtdb_auth.allowlist WHERE db_name = $1 AND email = $2")
                .bind(&body.db)
                .bind(&email)
                .execute(&state.pool)
                .await?;
        }
        other => {
            return Err(RtDbError::bad_request(format!("unknown action '{other}'")));
        }
    }

    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct AllowlistListParams {
    db: String,
}

#[derive(Serialize)]
struct AllowlistListResponse {
    emails: Vec<String>,
}

async fn allowlist_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<AllowlistListParams>,
) -> Result<Json<AllowlistListResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }

    let rows: Vec<(String,)> =
        sqlx::query_as("SELECT email FROM rtdb_auth.allowlist WHERE db_name = $1 ORDER BY email")
            .bind(&params.db)
            .fetch_all(&state.pool)
            .await?;

    Ok(Json(AllowlistListResponse {
        emails: rows.into_iter().map(|(email,)| email).collect(),
    }))
}

#[derive(Deserialize)]
struct ExportDbParams {
    db: String,
}

/// Streams `db`'s current schema and every document in every table as JSONL (see
/// `snapshot::export_database`); a plain app-level companion to host-level
/// `pg_dump` for seed data and clone-to-dev workflows.
async fn export_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<ExportDbParams>,
) -> Result<Response, RtDbError> {
    require_admin(&state, &headers).await?;
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
struct ImportDbParams {
    db: String,
}

/// Loads a JSONL snapshot produced by `export_db` back into `db` (see
/// `snapshot::import_database`), refreshing the schema cache with whatever schema
/// the snapshot applied.
async fn import_db(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<ImportDbParams>,
    body: String,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
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

#[derive(Serialize)]
struct AdminMember {
    email: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "githubId")]
    github_id: Option<i64>,
}

#[derive(Serialize)]
struct AdminsResponse {
    admins: Vec<AdminMember>,
}

/// All dashboard admins, email-ordered. Shared by `list_admins` and the config
/// read-back so the dashboard can render the allowlist alongside hot config.
async fn admin_members(pool: &sqlx::PgPool) -> Result<Vec<AdminMember>, RtDbError> {
    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(email, github_id)| AdminMember { email, github_id })
        .collect())
}

async fn list_admins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<AdminsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(AdminsResponse {
        admins: admin_members(&state.pool).await?,
    }))
}

#[derive(Deserialize)]
struct AddAdminRequest {
    email: String,
    #[serde(rename = "githubId")]
    github_id: Option<i64>,
}

async fn add_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AddAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let email = body.email.trim().to_lowercase();
    if email.is_empty() {
        return Err(RtDbError::bad_request("email is required"));
    }
    // ON CONFLICT merge: keep any existing github_id if the new one is absent.
    sqlx::query(
        "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, $2, $3) \
         ON CONFLICT (email) DO UPDATE SET \
            github_id = COALESCE(EXCLUDED.github_id, rtdb_auth.admins.github_id)",
    )
    .bind(&email)
    .bind(body.github_id)
    .bind(crate::db::now_ms())
    .execute(&state.pool)
    .await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct RemoveAdminRequest {
    email: String,
}

async fn remove_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RemoveAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    sqlx::query("DELETE FROM rtdb_auth.admins WHERE email = $1")
        .bind(body.email.trim().to_lowercase())
        .execute(&state.pool)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn get_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<crate::schema::SchemaDef>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    Ok(Json((*schema).clone()))
}

#[derive(Deserialize)]
struct PreviewSchemaRequest {
    schema: SchemaDef,
}

/// `POST /admin/db/{db}/schema/preview` — advisory diff of a pending schema
/// against the database's currently-applied one. Validates the pending schema
/// (invalid → 400) and reports what an additive-only push would ADD and what it
/// would have to drop or change (and therefore would reject). Does NOT apply,
/// does NOT touch `state.schemas` — `ddl::push_schema` remains the
/// authoritative gate.
async fn preview_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<PreviewSchemaRequest>,
) -> Result<Json<crate::schema_diff::SchemaDiff>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    body.schema.validate()?;
    let current = db::load_schema(&state.pool, &db).await?;
    let diff = crate::schema_diff::diff(current.as_ref(), &body.schema);
    Ok(Json(diff))
}

#[derive(Deserialize)]
struct AdminQueryRequest {
    query: Query,
}

#[derive(Serialize)]
struct AdminQueryResponse {
    result: QueryResult,
}

/// `POST /admin/db/{db}/query` — admin document read. `owner = None` bypasses
/// per-row `ownerField`, so an admin sees every row in every table.
async fn admin_query(
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
struct AdminMutateRequest {
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct AdminMutateResponse {
    results: Vec<serde_json::Value>,
}

/// `POST /admin/db/{db}/mutate` — admin document write through the existing
/// committer with `owner = None`. The step-count cap is the server-side
/// guardrail: each step touches at most one document, so rejecting over-cap
/// here guarantees an over-cap mutation never reaches the committer (never
/// becomes durable).
async fn admin_mutate(
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

/// `POST /admin/db/{db}/migrate` — admin schema migration through the committer
/// (serialized with concurrent writes; runs the subs/op-feed/audit/webhook taps
/// on the durable result). `dryRun` rolls back and publishes nothing. Reuses
/// `migrate::MigrateRequest` directly: it already carries `rename_all =
/// "camelCase"`, so the wire body is `{directives, dryRun}`.
async fn admin_migrate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<crate::migrate::MigrateRequest>,
) -> Result<Json<crate::migrate::MigrateResult>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let result = state.realtime.committers.migrate(&db, body).await?;
    Ok(Json(result))
}

// --- Scheduled jobs (admin) ------------------------------------------------
//
// Thin admin-gated wrappers over the same `scheduler` accessors the per-db
// machine-token handlers use (`http_api::schedule_handler` etc.). Scheduled jobs
// carry no owner at the table level — `scheduler::insert` has no owner param and
// `committer::handle_scheduled` always executes with `owner = None`, so an admin
// (or any caller) scheduling a txn just records the row; there is no per-row
// `ownerField` distinction to mirror from `/admin/db/{db}/mutate` here.

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminScheduleCreateRequest {
    when: ScheduleWhen,
    txn: Transaction,
}

#[derive(Serialize)]
struct AdminScheduleCreateResponse {
    id: String,
}

#[derive(Serialize)]
struct AdminScheduleListResponse {
    schedules: Vec<ScheduleInfo>,
}

#[derive(Serialize)]
struct AdminScheduleManageResponse {
    ok: bool,
}

/// `GET /admin/db/{db}/schedules` — list scheduled jobs for a database.
async fn admin_list_schedules(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<AdminScheduleListResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schedules = scheduler::list(&state.pool, &db).await?;
    Ok(Json(AdminScheduleListResponse { schedules }))
}

/// `POST /admin/db/{db}/schedules` — create a scheduled job. Mirrors
/// `http_api::schedule_handler` exactly, minus the per-db bearer/authorize gate
/// (admin-gated instead).
async fn admin_create_schedule(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminScheduleCreateRequest>,
) -> Result<Json<AdminScheduleCreateResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let (kind, due_at, cron) = scheduler::resolve_when(body.when, now_ms())?;
    let id = scheduler::insert(&state.pool, &db, kind, due_at, &body.txn, cron.as_deref()).await?;
    Ok(Json(AdminScheduleCreateResponse { id }))
}

/// `POST /admin/db/{db}/schedules/{id}/cancel` — delete a scheduled job.
async fn admin_cancel_schedule(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let ok = scheduler::cancel(&state.pool, &db, &id).await?;
    Ok(Json(AdminScheduleManageResponse { ok }))
}

/// Shared pause/resume path — `paused = true` pauses, `false` resumes.
async fn admin_set_schedule_paused(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    db: &str,
    id: &str,
    paused: bool,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    require_admin(state, headers).await?;
    if !db::database_exists(&state.pool, db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let ok = scheduler::set_paused(&state.pool, db, id, paused).await?;
    Ok(Json(AdminScheduleManageResponse { ok }))
}

/// `POST /admin/db/{db}/schedules/{id}/pause` — pause a pending job.
async fn admin_pause_schedule(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    admin_set_schedule_paused(&state, &headers, &db, &id, true).await
}

/// `POST /admin/db/{db}/schedules/{id}/resume` — resume a paused job.
async fn admin_resume_schedule(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<AdminScheduleManageResponse>, RtDbError> {
    admin_set_schedule_paused(&state, &headers, &db, &id, false).await
}

// --- File storage (admin) --------------------------------------------------
//
// Thin admin-gated wrappers over `storage` accessors, mirroring the per-db
// machine-token handlers in `http_api` (`upload_handler`, `delete_handler`,
// `metadata_handler`). Storage is not per-row — there is no `ownerField` on the
// `storage` table — so (unlike `/admin/db/{db}/mutate`) there is no owner to
// bypass; the admin gate alone guards these routes.

#[derive(Serialize)]
struct AdminStorageListResponse {
    files: Vec<storage::FileMeta>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AdminStorageUploadResponse {
    id: String,
}

/// `GET /admin/db/{db}/storage` — list stored files (metadata only), newest
/// first. `ensure_table` first so a database that predates the storage feature
/// (or had its table dropped) returns an empty list rather than erroring.
async fn admin_storage_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<AdminStorageListResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::ensure_table(&state.pool, &db).await?;
    let files = storage::list(&state.pool, &db).await?;
    Ok(Json(AdminStorageListResponse { files }))
}

/// `POST /admin/db/{db}/storage` — admin upload (raw body). Mirrors
/// `http_api::upload_handler` exactly: ensure_table, the live `max_file_size`
/// check (clamped to `HARD_MAX_FILE_SIZE`), sha256, `storage::put`, and
/// `metrics.record_upload()`. The route carries `DefaultBodyLimit::disable` so
/// `to_bytes` is the sole ceiling (SEC-008).
async fn admin_storage_upload(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    request: Request,
) -> Result<Json<AdminStorageUploadResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::ensure_table(&state.pool, &db).await?;
    let limit = crate::config::HARD_MAX_FILE_SIZE.min(state.runtime.hot.load().max_file_size);
    let bytes = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| RtDbError::bad_request("upload exceeds max file size"))?;
    let size = bytes.len() as i64;
    let sha256 = storage::sha256_hex_bytes(&bytes);
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let id = storage::put(
        &state.pool,
        &db,
        &sha256,
        size,
        content_type.as_deref(),
        &bytes,
    )
    .await?;
    state.runtime.metrics.record_upload();
    Ok(Json(AdminStorageUploadResponse { id }))
}

/// `DELETE /admin/db/{db}/storage/{id}` — idempotent delete. Both the per-db
/// blob row and the global `storage_index` row are removed (atomic, in one tx
/// inside `storage::delete`), so the public serve URL 404s afterward.
async fn admin_storage_delete(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    storage::delete(&state.pool, &db, &id).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Serialize)]
struct WebhooksResponse {
    webhooks: Vec<crate::webhook::Webhook>,
}

/// `GET /admin/db/{db}/webhooks` — list webhooks registered for `db`. When
/// webhooks are disabled at boot the table is permitted to not exist, so this
/// returns an empty list rather than erroring (mirrors `/admin/audit`).
async fn admin_list_webhooks(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<WebhooksResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    if !state.config.webhooks_enabled {
        return Ok(Json(WebhooksResponse {
            webhooks: Vec::new(),
        }));
    }
    let webhooks = crate::webhook::list_webhooks(&state.pool, &db).await?;
    Ok(Json(WebhooksResponse { webhooks }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminCreateWebhookRequest {
    url: String,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    events: Option<Vec<String>>,
}

#[derive(Serialize)]
struct AdminCreateWebhookResponse {
    id: i64,
}

/// The closed set of event names a webhook may subscribe to: the five op kinds
/// (matching `OpKind`'s lowercase serde form) plus `*` (all events). Used to
/// reject typos at registration time so a misspelled event never silently fails
/// to match.
fn is_valid_event(name: &str) -> bool {
    matches!(
        name,
        "*" | "insert" | "patch" | "replace" | "delete" | "upsert"
    )
}

/// `POST /admin/db/{db}/webhooks` — register a webhook URL for delivery on
/// matching document changes. `table` omitted/null = all tables; `events`
/// omitted defaults to `["*"]` (all events). Rejected with a 400 when webhooks
/// are disabled at boot or the events list contains an unknown name.
async fn admin_create_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminCreateWebhookRequest>,
) -> Result<Json<AdminCreateWebhookResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    if !state.config.webhooks_enabled {
        return Err(RtDbError::bad_request(
            "webhooks are disabled on this server (set RTDB_WEBHOOKS_ENABLED=true at boot)",
        ));
    }
    let url = body.url.trim();
    if url.is_empty() {
        return Err(RtDbError::bad_request("url is required"));
    }
    // An empty `table` string is treated as "all tables" (None), matching how
    // NULL is interpreted by the enqueue matcher.
    let table = body
        .table
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let events = body.events.unwrap_or_else(|| vec!["*".to_string()]);
    if events.is_empty() {
        return Err(RtDbError::bad_request("events must not be empty"));
    }
    for ev in &events {
        if !is_valid_event(ev) {
            return Err(RtDbError::bad_request(format!(
                "unknown event '{ev}'; expected one of insert, patch, replace, delete, upsert, or *"
            )));
        }
    }
    let webhook = crate::webhook::create_webhook(&state.pool, &db, table, url, &events).await?;
    Ok(Json(AdminCreateWebhookResponse { id: webhook.id }))
}

/// `DELETE /admin/db/{db}/webhooks/{id}` — remove a webhook (cascading its
/// pending deliveries via the FK). A non-numeric id is a 400; a missing id is a
/// 404.
async fn admin_delete_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let id: i64 = id
        .parse()
        .map_err(|_| RtDbError::bad_request("webhook id must be an integer"))?;
    if !state.config.webhooks_enabled {
        // Table may not exist; treat as already-gone.
        return Ok(Json(OkResponse { ok: false }));
    }
    let ok = crate::webhook::delete_webhook(&state.pool, id).await?;
    Ok(Json(OkResponse { ok }))
}

#[derive(Serialize)]
struct TokenRow {
    id: String,
    name: String,
    #[serde(rename = "createdAt")]
    created_at: i64,
    revoked: bool,
}

#[derive(Serialize)]
struct TokensResponse {
    tokens: Vec<TokenRow>,
}

#[derive(Deserialize)]
struct TokensParams {
    db: String,
}

async fn list_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<TokensParams>,
) -> Result<Json<TokensResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &params.db).await? {
        return Err(RtDbError::bad_request("unknown database"));
    }
    let rows: Vec<(String, String, i64, bool)> = sqlx::query_as(
        "SELECT id, name, created_at, revoked FROM rtdb_auth.machine_tokens \
         WHERE db_name = $1 ORDER BY created_at",
    )
    .bind(&params.db)
    .fetch_all(&state.pool)
    .await?;
    Ok(Json(TokensResponse {
        tokens: rows
            .into_iter()
            .map(|(id, name, created_at, revoked)| TokenRow {
                id,
                name,
                created_at,
                revoked,
            })
            .collect(),
    }))
}

#[derive(Serialize)]
struct TableStat {
    name: String,
    #[serde(rename = "rowCount")]
    row_count: i64,
    #[serde(rename = "sizeBytes")]
    size_bytes: i64,
}

#[derive(Serialize)]
struct DbStatsResponse {
    tables: Vec<TableStat>,
    #[serde(rename = "totalSizeBytes")]
    total_size_bytes: i64,
}

async fn db_stats(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
) -> Result<Json<DbStatsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
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
    Ok(Json(DbStatsResponse {
        tables,
        total_size_bytes,
    }))
}

async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<crate::metrics::MetricsSnapshot>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        state
            .runtime
            .metrics
            .snapshot(&state.pool, &state.realtime.subs, state.runtime.started_at)
            .await,
    ))
}

#[derive(Serialize)]
struct BackupsResponse {
    running: bool,
    backups: Vec<crate::backup::BackupFile>,
}

/// `GET /admin/backups` — lists the managed `pg_dump` files in
/// `config.backup_dir` newest-first, with size and parsed created-time, plus
/// the in-progress flag for the manual trigger. A missing dir (no run yet, or
/// backups disabled) returns an empty list rather than 404/500 — the endpoint
/// describes what is on disk, not what is configured. Whether the scheduler is
/// enabled at boot is already visible at `/admin/config`.
async fn list_backups(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<BackupsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let backups = crate::backup::list_backups(&state.config.backup_dir).await?;
    let running = state.backup_running.load(Ordering::Acquire);
    Ok(Json(BackupsResponse { running, backups }))
}

/// RAII guard that clears `AppState::backup_running` on drop, so the flag
/// releases even if the spawned backup task panics (Drop runs during unwind) —
/// a panic in the backup path can't lock out manual triggers until restart.
struct BackupRunningGuard(Arc<AtomicBool>);
impl Drop for BackupRunningGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// `POST /admin/backup` — trigger one `pg_dump` now. Returns 202 immediately;
/// the dump runs in a detached task and the in-progress flag is cleared on
/// completion (success, failure, or panic). A second call while one is running → 409.
/// Runs outside the committer (pg_dump is a read), exactly like the cron backup
/// task — no document tables or subscriptions are touched.
async fn create_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OkResponse>), RtDbError> {
    require_admin(&state, &headers).await?;
    // `swap` to set-and-test: returns the PRIOR value. If it was already true,
    // a backup is in progress — reject without disturbing the flag.
    if state.backup_running.swap(true, Ordering::AcqRel) {
        return Err(RtDbError::conflict("backup already running"));
    }
    let url = state.config.database_url.clone();
    let dir = state.config.backup_dir.clone();
    let flag = state.backup_running.clone();
    tokio::spawn(async move {
        let _guard = BackupRunningGuard(flag);
        match crate::backup::perform_backup(&url, &dir).await {
            Ok(p) => tracing::info!(path = %p.display(), "manual backup completed"),
            Err(e) => tracing::error!(error = %e, "manual backup failed"),
        }
    });
    Ok((StatusCode::ACCEPTED, Json(OkResponse { ok: true })))
}

/// `GET /admin/backups/{name}` — stream a dump file (admin-gated).
/// `validate_dump_name` runs first, so a traversal-shaped or malformed name is
/// rejected at the API edge before any filesystem access. Streams via
/// `Body::from_stream` so a large dump does not have to fit in memory.
async fn download_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, RtDbError> {
    require_admin(&state, &headers).await?;
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(RtDbError::not_found("backup file not found"));
        }
        Err(_) => return Err(RtDbError::internal("failed to open backup")),
    };
    let body = Body::from_stream(FramedRead::new(file, BytesCodec::new()));
    let mut resp = Response::new(body);
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        // `name` passed `validate_dump_name` (rtdb-<stamp>.dump), so it cannot
        // contain `"`, `\`, or any control char that would break this header.
        HeaderValue::from_str(&format!("attachment; filename=\"{name}\""))
            .map_err(|_| RtDbError::internal("invalid backup filename for header"))?,
    );
    Ok(resp)
}

/// `DELETE /admin/backups/{name}` — remove one dump (admin-gated). Same
/// `validate_dump_name` short-circuit as download. Returns 204 on success; 404
/// if the file is gone.
async fn delete_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, RtDbError> {
    require_admin(&state, &headers).await?;
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    match tokio::fs::remove_file(&path).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(RtDbError::not_found("backup file not found"))
        }
        Err(_) => Err(RtDbError::internal("failed to delete backup")),
    }
}

#[derive(Deserialize)]
struct RestoreRequest {
    name: String,
    confirm: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RestoreResponse {
    target: String,
    instructions: String,
}

/// `POST /admin/restore` — restore a dump into a fresh `rtdb_restored_<stamp>`
/// DB. `confirm` must equal `name` (typed guard, mirroring `delete_db`). The
/// live DB is never touched — `restore_to_new_db` creates a fresh target DB
/// and `pg_restore`s into it, leaving the committer and all live connections
/// undisturbed. Returns the target DB name and cutover instructions.
async fn restore_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RestoreRequest>,
) -> Result<Json<RestoreResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if body.confirm != body.name {
        return Err(RtDbError::bad_request(
            "confirmation does not match backup filename",
        ));
    }
    let target = crate::backup::restore_to_new_db(
        &state.config.database_url,
        &state.config.backup_dir,
        &body.name,
    )
    .await?;
    Ok(Json(RestoreResponse {
        instructions: format!(
            "Restore complete into database '{target}'. To cut over: set RTDB_DATABASE_URL to connect to '{target}', then restart the server."
        ),
        target,
    }))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigResponse {
    port: u16,
    public_url: String,
    github_base_url: String,
    github_api_url: String,
    database_url_configured: bool,
    admin_key_configured: bool,
    github_configured: bool,
    google_configured: bool,
    gitlab_configured: bool,
    oidc_configured: bool,
    hot: crate::config::HotConfig,
    version: &'static str,
    git_commit: &'static str,
    admins: Vec<AdminMember>,
}

/// Builds the redacted config view from current boot + hot state. Secrets never
/// appear: `admin_key`, OAuth secrets, and `database_url` (which embeds DB
/// credentials) collapse to configured-bools; hot values are shown in full.
async fn build_config_response(state: &AppState) -> Result<ConfigResponse, RtDbError> {
    let cfg = &state.config;
    let hot = state.runtime.hot.load();
    Ok(ConfigResponse {
        port: cfg.port,
        public_url: cfg.public_url.clone(),
        github_base_url: cfg.github_base_url.clone(),
        github_api_url: cfg.github_api_url.clone(),
        database_url_configured: !cfg.database_url.is_empty(),
        admin_key_configured: !cfg.admin_key.is_empty(),
        github_configured: cfg.github_client_id.is_some() && cfg.github_client_secret.is_some(),
        google_configured: cfg.google_client_id.is_some() && cfg.google_client_secret.is_some(),
        gitlab_configured: cfg.gitlab_client_id.is_some() && cfg.gitlab_client_secret.is_some(),
        oidc_configured: cfg.oidc_client_id.is_some()
            && cfg.oidc_client_secret.is_some()
            && cfg.oidc_authorize_url.is_some()
            && cfg.oidc_token_url.is_some()
            && cfg.oidc_userinfo_url.is_some(),
        hot: (**hot).clone(),
        version: env!("CARGO_PKG_VERSION"),
        git_commit: env!("BUILD_GIT_COMMIT"),
        admins: admin_members(&state.pool).await?,
    })
}

/// `GET /admin/config` — redacted running configuration (boot masked, hot shown
/// in full) plus build identity and the admin allowlist.
async fn get_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ConfigResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(build_config_response(&state).await?))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HotConfigPatch {
    allowed_origins: Option<Vec<String>>,
    session_ttl_days: Option<i64>,
    max_file_size: Option<usize>,
    idempotency_ttl_ms: Option<i64>,
}

/// `PATCH /admin/config` — apply a subset patch to the hot config, validate,
/// persist the merged row to `rtdb_config`, swap the `ArcSwap`, and return the
/// new redacted config. Unknown fields (`deny_unknown_fields`) and invalid
/// values are `BadRequest`; each provided field fully replaces the prior value.
async fn patch_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(patch): ApiJson<HotConfigPatch>,
) -> Result<Json<ConfigResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let mut next: crate::config::HotConfig = (**state.runtime.hot.load()).clone();
    if let Some(origins) = &patch.allowed_origins {
        next.allowed_origins = origins
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(ttl) = patch.session_ttl_days {
        if ttl < 1 {
            return Err(RtDbError::bad_request("sessionTtlDays must be >= 1"));
        }
        next.session_ttl_days = ttl;
    }
    if let Some(size) = patch.max_file_size {
        if size == 0 {
            return Err(RtDbError::bad_request("maxFileSize must be > 0"));
        }
        // SEC-008: reject an over-ceiling value at PATCH time so the persisted
        // row can't advertise a limit `http_api` silently clamps back down to
        // `HARD_MAX_FILE_SIZE` (100 MiB). Without this, the configured value
        // and the enforced value disagree.
        if size > crate::config::HARD_MAX_FILE_SIZE {
            return Err(RtDbError::bad_request(format!(
                "maxFileSize must be <= {} bytes (hard ceiling)",
                crate::config::HARD_MAX_FILE_SIZE
            )));
        }
        next.max_file_size = size;
    }
    if let Some(ttl) = patch.idempotency_ttl_ms {
        if ttl <= 0 {
            return Err(RtDbError::bad_request("idempotencyTtlMs must be > 0"));
        }
        next.idempotency_ttl_ms = ttl;
    }
    if !next.origins_valid() {
        return Err(RtDbError::bad_request(
            "allowedOrigins contains an invalid origin",
        ));
    }
    crate::config::save_hot(&state.pool, &next).await?;
    state.runtime.hot.store(Arc::new(next));
    Ok(Json(build_config_response(&state).await?))
}

#[derive(Deserialize)]
struct OpsRecentParams {
    db: Option<String>,
    table: Option<String>,
    #[serde(default = "default_ops_n")]
    n: usize,
}
fn default_ops_n() -> usize {
    100
}

#[derive(Deserialize)]
struct AuditParams {
    db: Option<String>,
    #[serde(default = "default_audit_limit")]
    limit: i64,
    #[serde(default = "default_audit_offset")]
    offset: i64,
}
fn default_audit_limit() -> i64 {
    100
}
fn default_audit_offset() -> i64 {
    0
}

#[derive(Serialize)]
struct AuditResponse {
    entries: Vec<crate::audit::AuditEntry>,
}

/// `GET /admin/audit?db=<optional>&limit=<n>&offset=<m>` — durable audit log,
/// newest-first. `limit` defaults to 100 and is capped at 1000; `offset`
/// defaults to 0. When audit is disabled at boot (`!config.audit_log_enabled`)
/// this short-circuits to an empty list — the `rtdb.audit_log` table may not
/// exist, and an operator who turned audit off should not see stale rows from
/// a previous enabled run either.
async fn audit_recent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<AuditParams>,
) -> Result<Json<AuditResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !state.config.audit_log_enabled {
        return Ok(Json(AuditResponse {
            entries: Vec::new(),
        }));
    }
    // Clamp to [1, 1000]; negative limits/offsets are nonsensical but
    // otherwise accepted by Postgres, so guard at the API edge.
    let limit = params.limit.clamp(1, 1000);
    let offset = params.offset.max(0);
    let entries =
        crate::audit::fetch_audit_rows(&state.pool, params.db.as_deref(), limit, offset).await?;
    Ok(Json(AuditResponse { entries }))
}

#[derive(Serialize)]
struct OpsRecentResponse {
    ops: Vec<crate::op_feed::OpEvent>,
}

/// Recent document-op events from the in-memory ring, filtered by optional
/// `db`/`table`, newest-first, capped at `n` (max 500).
async fn ops_recent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<OpsRecentParams>,
) -> Result<Json<OpsRecentResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let ops = state
        .realtime
        .op_feed
        .recent(
            params.db.as_deref(),
            params.table.as_deref(),
            params.n.min(500),
        )
        .await;
    Ok(Json(OpsRecentResponse { ops }))
}

#[derive(Deserialize)]
struct StreamParams {
    db: Option<String>,
    table: Option<String>,
}

/// `/admin/stream` WebSocket: admin-gated at the HTTP upgrade (a missing/invalid
/// bearer is rejected before WS negotiation), then replays the filtered ring and
/// streams live op events plus a ~1s gauge snapshot. `db`/`table` filter both the
/// replay and the live broadcast.
///
/// The gate runs BEFORE `WebSocketUpgrade` is extracted from the request, so a
/// missing bearer on a plain GET (or a real upgrade attempt) yields 401/403 and
/// never reaches WS negotiation; the WS extractor is invoked by hand after the
/// gate clears. The bearer is taken from the `Authorization` header when present
/// (CLI/automation), otherwise from the `Sec-WebSocket-Protocol: rtdb-admin.<token>`
/// subprotocol — browsers cannot set request headers on a WS handshake, so the
/// dashboard authenticates through that subprotocol instead.
async fn admin_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<StreamParams>,
    req: Request,
) -> Result<Response, RtDbError> {
    // Bearer from the Authorization header (CLI/automation) or, failing that,
    // the `rtdb-admin.<token>` subprotocol (browser dashboard — browsers can't
    // set headers on a WS handshake). When the subprotocol path is used we echo
    // it back: a client that offered a subprotocol requires the server to
    // negotiate one (tokio-tungstenite errors otherwise; browsers are lenient
    // but the echo is the spec-correct 101 response).
    let (token, offered_subprotocol) = match bearer_value(&headers) {
        Ok(t) => (t, None),
        Err(_) => {
            let t = bearer_from_subprotocol(&headers)?;
            (t, Some(format!("rtdb-admin.{t}")))
        }
    };
    let _ = authenticate_admin(&state, token).await?;
    let mut ws = WebSocketUpgrade::from_request(req, &state)
        .await
        .map_err(|_| RtDbError::bad_request("expected websocket upgrade request"))?;
    if let Some(proto) = offered_subprotocol {
        ws = ws.protocols([proto]);
    }
    Ok(ws.on_upgrade(move |socket| run_admin_stream(socket, state, params.db, params.table)))
}

async fn run_admin_stream(
    mut socket: WebSocket,
    state: Arc<AppState>,
    db: Option<String>,
    table: Option<String>,
) {
    for ev in state
        .realtime
        .op_feed
        .recent(db.as_deref(), table.as_deref(), 200)
        .await
    {
        if send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev}))
            .await
            .is_err()
        {
            return;
        }
    }
    let mut rx = state.realtime.op_feed.subscribe();
    let mut gauge_tick = tokio::time::interval(Duration::from_secs(1));
    gauge_tick.tick().await; // skip immediate
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Ok(ev) = ev else { break };
                if db.as_deref().is_none_or(|d| ev.db == d)
                    && table.as_deref().is_none_or(|t| ev.table == t)
                    && send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev}))
                        .await
                        .is_err()
                {
                    break;
                }
            }
            _ = gauge_tick.tick() => {
                let snap = state
                    .runtime
                    .metrics
                    .snapshot(&state.pool, &state.realtime.subs, state.runtime.started_at)
                    .await;
                if send_stream_json(&mut socket, &serde_json::json!({"kind":"gauges","gauges":snap}))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

async fn send_stream_json(
    socket: &mut WebSocket,
    value: &serde_json::Value,
) -> Result<(), axum::Error> {
    use axum::extract::ws::Message;
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    socket.send(Message::Text(text.into())).await
}

/// Admin routes, all gated on `Authorization: Bearer <admin_key>` (constant-time
/// compare).
pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/login", post(admin_login))
        .route("/admin/logout", post(admin_logout))
        .route("/admin/create-db", post(create_db))
        .route("/admin/delete-db", post(delete_db))
        .route("/admin/push-schema", post(push_schema))
        .route("/admin/dbs", get(list_dbs))
        .route("/admin/mint-token", post(mint_token))
        .route("/admin/revoke-token", post(revoke_token))
        .route(
            "/admin/allowlist",
            get(allowlist_list).post(allowlist_write),
        )
        .route(
            "/admin/admins",
            get(list_admins).post(add_admin).delete(remove_admin),
        )
        .route("/admin/dbs/{db}/schema", get(get_schema))
        .route("/admin/db/{db}/schema/preview", post(preview_schema))
        .route("/admin/dbs/{db}/stats", get(db_stats))
        .route("/admin/db/{db}/query", post(admin_query))
        .route("/admin/db/{db}/mutate", post(admin_mutate))
        .route("/admin/db/{db}/migrate", post(admin_migrate))
        .route(
            "/admin/db/{db}/storage",
            get(admin_storage_list)
                .post(admin_storage_upload)
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/admin/db/{db}/storage/{id}", delete(admin_storage_delete))
        .route(
            "/admin/db/{db}/webhooks",
            get(admin_list_webhooks).post(admin_create_webhook),
        )
        .route("/admin/db/{db}/webhooks/{id}", delete(admin_delete_webhook))
        .route(
            "/admin/db/{db}/schedules",
            get(admin_list_schedules).post(admin_create_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/cancel",
            post(admin_cancel_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/pause",
            post(admin_pause_schedule),
        )
        .route(
            "/admin/db/{db}/schedules/{id}/resume",
            post(admin_resume_schedule),
        )
        .route("/admin/metrics", get(metrics_handler))
        .route("/admin/config", get(get_config).patch(patch_config))
        .route("/admin/backup", post(create_backup))
        .route("/admin/backups", get(list_backups))
        .route(
            "/admin/backups/{name}",
            get(download_backup).delete(delete_backup),
        )
        .route("/admin/restore", post(restore_backup))
        .route("/admin/ops/recent", get(ops_recent))
        .route("/admin/audit", get(audit_recent))
        .route("/admin/stream", get(admin_stream))
        .route("/admin/tokens", get(list_tokens))
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
}
