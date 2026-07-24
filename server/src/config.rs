use serde::{Deserialize, Serialize};

use crate::error::RtDbError;

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,                            // RTDB_PORT, default 8300
    pub database_url: String,                 // RTDB_DATABASE_URL (required)
    pub admin_key: String,                    // RTDB_ADMIN_KEY (required)
    pub public_url: String,                   // RTDB_PUBLIC_URL, default http://localhost:8300
    pub github_client_id: Option<String>,     // RTDB_GITHUB_CLIENT_ID
    pub github_client_secret: Option<String>, // RTDB_GITHUB_CLIENT_SECRET
    pub github_base_url: String,              // RTDB_GITHUB_BASE_URL, default https://github.com
    pub github_api_url: String,               // RTDB_GITHUB_API_URL, default https://api.github.com
    pub google_client_id: Option<String>,     // RTDB_GOOGLE_CLIENT_ID
    pub google_client_secret: Option<String>, // RTDB_GOOGLE_CLIENT_SECRET
    pub max_affected_docs: usize, // RTDB_MAX_AFFECTED_DOCS, default 100 (admin data-browser guardrail)
}

impl Config {
    /// Reads boot-only values from env. Errors (String) name the missing/invalid variable.
    pub fn from_env() -> Result<Self, String> {
        let port = match std::env::var("RTDB_PORT") {
            Ok(v) => v
                .parse::<u16>()
                .map_err(|_| "RTDB_PORT must be a valid u16".to_string())?,
            Err(_) => 8300,
        };

        let database_url = std::env::var("RTDB_DATABASE_URL")
            .map_err(|_| "RTDB_DATABASE_URL is required".to_string())?;

        let admin_key = std::env::var("RTDB_ADMIN_KEY")
            .map_err(|_| "RTDB_ADMIN_KEY is required".to_string())?;

        let public_url = std::env::var("RTDB_PUBLIC_URL")
            .unwrap_or_else(|_| "http://localhost:8300".to_string());

        let github_client_id = std::env::var("RTDB_GITHUB_CLIENT_ID").ok();
        let github_client_secret = std::env::var("RTDB_GITHUB_CLIENT_SECRET").ok();

        let github_base_url = std::env::var("RTDB_GITHUB_BASE_URL")
            .unwrap_or_else(|_| "https://github.com".to_string());

        let github_api_url = std::env::var("RTDB_GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_string());

        let google_client_id = std::env::var("RTDB_GOOGLE_CLIENT_ID").ok();
        let google_client_secret = std::env::var("RTDB_GOOGLE_CLIENT_SECRET").ok();

        let max_affected_docs = match std::env::var("RTDB_MAX_AFFECTED_DOCS") {
            Ok(v) => v.parse::<usize>().unwrap_or(100),
            Err(_) => 100,
        };

        Ok(Self {
            port,
            database_url,
            admin_key,
            public_url,
            github_client_id,
            github_client_secret,
            github_base_url,
            github_api_url,
            google_client_id,
            google_client_secret,
            max_affected_docs,
        })
    }
}

/// Runtime-mutable, hot-reloadable configuration. Held in `AppState` behind an
/// `Arc<ArcSwap<HotConfig>>` so a `PATCH /admin/config` swap takes effect on the
/// next request with no restart. Persisted as a single jsonb row in `rtdb_config`;
/// seeded from env at first boot.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HotConfig {
    pub allowed_origins: Vec<String>, // RTDB_ALLOWED_ORIGINS, comma-separated, default empty
    pub session_ttl_days: i64,        // RTDB_SESSION_TTL_DAYS, default 30
    pub max_file_size: usize,         // RTDB_MAX_FILE_SIZE, default 50 MiB
}

impl HotConfig {
    /// Seeds defaults from env — the same parses `Config` used to perform before
    /// these three settings became hot-reloadable. Invalid values fall back to the
    /// documented default rather than failing boot.
    pub fn from_env() -> Self {
        let allowed_origins = match std::env::var("RTDB_ALLOWED_ORIGINS") {
            Ok(v) if !v.is_empty() => v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            _ => Vec::new(),
        };
        let session_ttl_days = match std::env::var("RTDB_SESSION_TTL_DAYS") {
            Ok(v) => v.parse::<i64>().unwrap_or(30),
            Err(_) => 30,
        };
        let max_file_size = match std::env::var("RTDB_MAX_FILE_SIZE") {
            Ok(v) => v.parse::<usize>().unwrap_or(50 * 1024 * 1024),
            Err(_) => 50 * 1024 * 1024,
        };
        Self {
            allowed_origins,
            session_ttl_days,
            max_file_size,
        }
    }

    /// True when every origin parses as a valid `HeaderValue` (the CORS layer
    /// would otherwise silently skip a malformed origin at request time).
    pub fn origins_valid(&self) -> bool {
        self.allowed_origins
            .iter()
            .all(|o| axum::http::HeaderValue::from_str(o).is_ok())
    }
}

/// Loads the single persisted hot row, if any. A missing row is `Ok(None)`
/// (first boot); a sqlx or decode failure is an internal error.
pub async fn load_hot(pool: &sqlx::PgPool) -> Result<Option<HotConfig>, RtDbError> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT hot FROM rtdb_config WHERE id = 1")
            .fetch_optional(pool)
            .await
            .map_err(|e| RtDbError::internal(format!("load rtdb_config: {e}")))?;
    match row {
        Some((v,)) => serde_json::from_value::<HotConfig>(v)
            .map(Some)
            .map_err(|e| RtDbError::internal(format!("decode rtdb_config: {e}"))),
        None => Ok(None),
    }
}

/// Upserts the single hot row.
pub async fn save_hot(pool: &sqlx::PgPool, hot: &HotConfig) -> Result<(), RtDbError> {
    let v = serde_json::to_value(hot)
        .map_err(|e| RtDbError::internal(format!("encode rtdb_config: {e}")))?;
    sqlx::query(
        "INSERT INTO rtdb_config (id, hot) VALUES (1, $1) \
         ON CONFLICT (id) DO UPDATE SET hot = EXCLUDED.hot",
    )
    .bind(v)
    .execute(pool)
    .await
    .map_err(|e| RtDbError::internal(format!("save rtdb_config: {e}")))?;
    Ok(())
}
