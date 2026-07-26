use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, Path, Query as QueryParams, Request, State};
use axum::http::{HeaderMap, StatusCode, header::SET_COOKIE};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::query::{Query, QueryResult, execute_query};
use crate::schema::SchemaDef;
use crate::txn::Transaction;
use crate::{AppState, auth, db, ddl, snapshot};

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
    let result = execute_query(&state.pool, &db, &schema, &body.query, None).await?;
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
        .mutate(&db, body.idempotency_key, body.txn, None)
        .await?;
    state.runtime.metrics.record_mutation();
    Ok(Json(AdminMutateResponse {
        results: outcome.results,
    }))
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
        .route("/admin/dbs/{db}/stats", get(db_stats))
        .route("/admin/db/{db}/query", post(admin_query))
        .route("/admin/db/{db}/mutate", post(admin_mutate))
        .route("/admin/metrics", get(metrics_handler))
        .route("/admin/config", get(get_config).patch(patch_config))
        .route("/admin/ops/recent", get(ops_recent))
        .route("/admin/stream", get(admin_stream))
        .route("/admin/tokens", get(list_tokens))
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
}
