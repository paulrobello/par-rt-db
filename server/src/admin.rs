use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query as QueryParams, State};
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
        .route("/admin/tokens", get(list_tokens))
        .route("/admin/export-db", get(export_db))
        .route("/admin/import-db", post(import_db))
}
