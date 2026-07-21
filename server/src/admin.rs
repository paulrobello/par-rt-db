use std::sync::Arc;

use axum::extract::{Query as QueryParams, State};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::schema::SchemaDef;
use crate::{AppState, auth, db, ddl};

fn require_admin(headers: &HeaderMap, expected: &str) -> Result<(), RtDbError> {
    let provided = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))?;

    if bool::from(provided.as_bytes().ct_eq(expected.as_bytes())) {
        Ok(())
    } else {
        Err(RtDbError::unauthorized("invalid admin key"))
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
    Json(body): Json<CreateDbRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
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
    Json(body): Json<PushSchemaRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
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
    require_admin(&headers, &state.config.admin_key)?;
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
    Json(body): Json<MintTokenRequest>,
) -> Result<Json<MintTokenResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
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
    Json(body): Json<RevokeTokenRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
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
    Json(body): Json<AllowlistWriteRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&headers, &state.config.admin_key)?;
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
    require_admin(&headers, &state.config.admin_key)?;
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
}
