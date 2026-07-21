use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;

use crate::error::RtDbError;
use crate::schema::SchemaDef;
use crate::{AppState, db, ddl};

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

/// Admin routes, all gated on `Authorization: Bearer <admin_key>` (constant-time
/// compare). Allowlist and machine-token routes are added in Task 8.
pub fn admin_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/admin/create-db", post(create_db))
        .route("/admin/push-schema", post(push_schema))
        .route("/admin/dbs", get(list_dbs))
}
