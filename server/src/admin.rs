use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{FromRequest, Path, Query as QueryParams, Request, State};
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::http_api::ApiJson;
use crate::schema::SchemaDef;
use crate::{AppState, auth, db, ddl, snapshot};

/// Who an admin request was made as: the raw admin key (CLI/automation) or an
/// OAuth user on the server-wide admin allowlist (browser dashboard).
#[allow(dead_code)] // `User`'s payload is consumed by Task 3's admin routes.
pub(crate) enum AdminPrincipal {
    Key,
    User(auth::Principal),
}

fn bearer_value(headers: &HeaderMap) -> Result<&str, RtDbError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))
}

/// Admin gate. Tries the raw admin key first (constant-time compare), then a
/// resolved session/machine principal — admitting only OAuth users present in
/// `rtdb_auth.admins`. Machine tokens and non-allowlisted/expired users are
/// rejected. The admin-key path returns before any DB lookup, so machine/CLI
/// admin calls stay cheap; the session path costs one `resolve_bearer` + one
/// `is_admin` query per request (acceptable for low-frequency dashboard traffic).
pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AdminPrincipal, RtDbError> {
    let provided = bearer_value(headers)?;
    if bool::from(provided.as_bytes().ct_eq(state.config.admin_key.as_bytes())) {
        return Ok(AdminPrincipal::Key);
    }
    let principal = match auth::resolve_bearer(&state.pool, provided).await {
        Ok(principal) => principal,
        Err(_) => return Err(RtDbError::unauthorized("invalid admin credential")),
    };
    if auth::is_admin(&state.pool, &principal).await {
        Ok(AdminPrincipal::User(principal))
    } else {
        Err(RtDbError::forbidden("not a dashboard admin"))
    }
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

async fn list_admins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<AdminsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(&state.pool)
            .await?;
    Ok(Json(AdminsResponse {
        admins: rows
            .into_iter()
            .map(|(email, github_id)| AdminMember { email, github_id })
            .collect(),
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
            .metrics
            .snapshot(&state.pool, &state.subs, state.started_at)
            .await,
    ))
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
/// `require_admin` runs as the first statement, BEFORE `WebSocketUpgrade` is
/// extracted from the request — so a missing bearer on a plain GET (or a real
/// upgrade attempt) yields 401/403 and never reaches WS negotiation. The WS
/// extractor is invoked by hand after the gate clears.
async fn admin_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<StreamParams>,
    req: Request,
) -> Result<Response, RtDbError> {
    require_admin(&state, &headers).await?;
    let ws = WebSocketUpgrade::from_request(req, &state)
        .await
        .map_err(|_| RtDbError::bad_request("expected websocket upgrade request"))?;
    Ok(ws.on_upgrade(move |socket| run_admin_stream(socket, state, params.db, params.table)))
}

async fn run_admin_stream(
    mut socket: WebSocket,
    state: Arc<AppState>,
    db: Option<String>,
    table: Option<String>,
) {
    for ev in state
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
    let mut rx = state.op_feed.subscribe();
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
                    .metrics
                    .snapshot(&state.pool, &state.subs, state.started_at)
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
        .route("/admin/metrics", get(metrics_handler))
        .route("/admin/ops/recent", get(ops_recent))
        .route("/admin/stream", get(admin_stream))
        .route("/admin/tokens", get(list_tokens))
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
}
