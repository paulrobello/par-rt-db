//! Admin hot-config routes: redacted config read-back and a live PATCH that
//! validates, persists, and swaps the runtime `HotConfig`. Secrets collapse to
//! configured-bools; hot values are shown in full.

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::RtDbError;
use crate::http_api::ApiJson;

use super::login::{AdminMember, admin_members};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConfigResponse {
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
pub(super) async fn get_config(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
) -> Result<Json<ConfigResponse>, RtDbError> {
    Ok(Json(build_config_response(&state).await?))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct HotConfigPatch {
    allowed_origins: Option<Vec<String>>,
    session_ttl_days: Option<i64>,
    max_file_size: Option<usize>,
    idempotency_ttl_ms: Option<i64>,
    // Per-database resource quotas (ENH-011). 0 = unlimited.
    max_tables_per_db: Option<usize>,
    max_storage_bytes_per_db: Option<u64>,
    max_subs_per_db: Option<usize>,
}

/// `PATCH /admin/config` — apply a subset patch to the hot config, validate,
/// persist the merged row to `rtdb_config`, swap the `ArcSwap`, and return the
/// new redacted config. Unknown fields (`deny_unknown_fields`) and invalid
/// values are `BadRequest`; each provided field fully replaces the prior value.
pub(super) async fn patch_config(
    State(state): State<Arc<AppState>>,
    _headers: HeaderMap,
    ApiJson(patch): ApiJson<HotConfigPatch>,
) -> Result<Json<ConfigResponse>, RtDbError> {
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
    // Quota caps (ENH-011): unsigned types forbid negatives, and 0 means
    // unlimited (no cap), so there is no lower bound to enforce and no hard
    // upper ceiling — an operator can set whatever limit they want live.
    if let Some(cap) = patch.max_tables_per_db {
        next.max_tables_per_db = cap;
    }
    if let Some(cap) = patch.max_storage_bytes_per_db {
        next.max_storage_bytes_per_db = cap;
    }
    if let Some(cap) = patch.max_subs_per_db {
        next.max_subs_per_db = cap;
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
