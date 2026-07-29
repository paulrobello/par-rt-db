use serde::{Deserialize, Serialize};

use crate::error::RtDbError;

/// Hard upper bound on `HotConfig::max_file_size`, enforced at boot seed
/// (`HotConfig::from_env`) and at the upload buffering point
/// (`http_api::upload_handler`). The hot-config value is admin-mutable via
/// `PATCH /admin/config`, so without a compile-time ceiling an admin
/// misconfiguration (or a compromised admin token) could buffer arbitrarily
/// large blobs into Postgres `bytea`. 100 MiB is 2x the default (50 MiB),
/// leaving legitimate operator headroom while bounding the worst case
/// (SEC-008). Raising this is a code change, not a config knob.
pub(crate) const HARD_MAX_FILE_SIZE: usize = 100 * 1024 * 1024;

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
    pub static_dir: Option<String>, // RTDB_STATIC_DIR — unset/empty ⇒ API-only (no SPA served)
    pub pool_max_connections: u32, // RTDB_POOL_MAX_CONNECTIONS, default 75 (multi-tenant; one committer task + N sub re-runs per db)
    // HTTP rate limiting (v1, fixed-window, in-memory): 0 = unlimited.
    // RTDB_RATE_LIMIT_PER_TOKEN_RPM caps each machine token; OAuth sessions
    // carry no token id and are rate-limited per-db only. Default 0 preserves
    // today's unlimited behavior; one noisy app on a multi-db instance can
    // otherwise starve the others.
    pub rate_limit_per_token_rpm: u32,
    pub rate_limit_per_db_rpm: u32, // RTDB_RATE_LIMIT_PER_DB_RPM, shared across all principals of one db
    // Durable audit log (global `rtdb.audit_log` table): when true, the
    // committer writes one row per durable DocOp at both tap sites
    // (`handle_mutate`/`handle_scheduled`). Off by default — the ephemeral
    // op-feed (`OpFeed`) is always on; this is its durable counterpart.
    // RTDB_AUDIT_LOG_ENABLED (accepts "true"/"1"/"yes", case-insensitive).
    pub audit_log_enabled: bool,
    // Webhook delivery registry: when true, the committer enqueues one
    // `rtdb.webhook_deliveries` row per matching webhook at both tap sites,
    // and a background worker POSTs the payload (at-least-once) to each
    // registered URL. Off by default — the native answer for triggering
    // external work in this no-embedded-JS architecture.
    // RTDB_WEBHOOKS_ENABLED (accepts "true"/"1"/"yes", case-insensitive).
    pub webhooks_enabled: bool,
    // Managed pg_dump backup scheduler. Off by default — when true, a
    // background task runs `pg_dump` on `backup_cron` (5-field UTC cron, same
    // format `scheduler::next_fire` already handles) into `backup_dir`,
    // keeping the newest `backup_retention` dumps. RTDB_BACKUP_ENABLED /
    // RTDB_BACKUP_CRON / RTDB_BACKUP_DIR / RTDB_BACKUP_RETENTION.
    pub backup_enabled: bool,
    pub backup_cron: String,
    pub backup_dir: String,
    pub backup_retention: u32,
    // Shadow verification of subscription-invalidation skips: verify 1 skip in
    // every N by re-running the query anyway and comparing its result against
    // the last pushed one. A divergence means the read set under-approximated —
    // a dropped realtime update — so it is logged at ERROR, counted in
    // `rtdb_subs_missed_pushes_total`, and the corrected result is pushed.
    //
    // 0 = off (default). 1 = verify every skip (integration tests). 100 = 1%.
    // Each verification costs exactly the Postgres round-trip the skip avoided,
    // so this trades the optimization back for confidence — keep N large in
    // production. Sampling is deterministic (every Nth skip), not random, so a
    // test can pin it. RTDB_SUBS_VERIFY_SKIP_EVERY.
    pub subs_verify_skip_every: u64,
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

        // Multi-tenant default (75): the committer-per-db model means each
        // active database can hold a connection during fan-out re-runs, so
        // the hardcoded 10 of the original code would saturate at ~11 active
        // dbs. 75 sits in the 50-100 range the audit recommends, leaving
        // headroom for concurrent subscription re-runs and HTTP reads
        // without overcommitting a typical Postgres `max_connections=100`.
        // A non-parseable or out-of-range value falls back to the default
        // (matching the `max_affected_docs` parse style).
        let pool_max_connections = match std::env::var("RTDB_POOL_MAX_CONNECTIONS") {
            Ok(v) => v.parse::<u32>().unwrap_or(75),
            Err(_) => 75,
        };

        let static_dir = std::env::var("RTDB_STATIC_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        // HTTP rate-limit ceilings: 0 = unlimited (the default), preserving
        // today's behavior. Non-parseable values fall back to the default,
        // matching the `max_affected_docs` parse style.
        let rate_limit_per_token_rpm = match std::env::var("RTDB_RATE_LIMIT_PER_TOKEN_RPM") {
            Ok(v) => v.parse::<u32>().unwrap_or(0),
            Err(_) => 0,
        };
        let rate_limit_per_db_rpm = match std::env::var("RTDB_RATE_LIMIT_PER_DB_RPM") {
            Ok(v) => v.parse::<u32>().unwrap_or(0),
            Err(_) => 0,
        };

        // Audit log: default off. Accepts the common truthy spellings
        // case-insensitively and falls back to false on anything else,
        // matching the permissiveness of the other env parses above.
        let audit_log_enabled = match std::env::var("RTDB_AUDIT_LOG_ENABLED") {
            Ok(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
            Err(_) => false,
        };

        // Webhook registry: default off, same truthy-spelling parse as audit.
        let webhooks_enabled = match std::env::var("RTDB_WEBHOOKS_ENABLED") {
            Ok(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
            Err(_) => false,
        };

        // Managed pg_dump backup scheduler. Default off; cron/dir/retention
        // carry their own defaults so an operator can flip just
        // RTDB_BACKUP_ENABLED=true to get daily 03:00 UTC dumps with 7-day
        // retention. An empty RTDB_BACKUP_CRON falls back to the default
        // (a blank cron would surface as `invalid cron expression` from
        // `scheduler::next_fire` on every loop iteration, so clamp here).
        let backup_enabled = match std::env::var("RTDB_BACKUP_ENABLED") {
            Ok(v) => matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1" | "yes"),
            Err(_) => false,
        };
        let backup_cron = match std::env::var("RTDB_BACKUP_CRON") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "0 3 * * *".to_string(),
        };
        let backup_dir = match std::env::var("RTDB_BACKUP_DIR") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "./backups".to_string(),
        };
        let backup_retention = match std::env::var("RTDB_BACKUP_RETENTION") {
            Ok(v) => v.parse::<u32>().unwrap_or(7),
            Err(_) => 7,
        };

        // Skip verification: default off (0). An unparseable value falls back to
        // off rather than to a costly rate, matching the permissiveness of the
        // parses above while failing safe on the expensive side.
        let subs_verify_skip_every = match std::env::var("RTDB_SUBS_VERIFY_SKIP_EVERY") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or(0),
            Err(_) => 0,
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
            static_dir,
            pool_max_connections,
            rate_limit_per_token_rpm,
            rate_limit_per_db_rpm,
            audit_log_enabled,
            webhooks_enabled,
            backup_enabled,
            backup_cron,
            backup_dir,
            backup_retention,
            subs_verify_skip_every,
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
    pub idempotency_ttl_ms: i64, // RTDB_IDEMPOTENCY_TTL_MS, default mutation_log::DEFAULT_DEDUP_TTL_MS (5 min)
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
        // Clamp to the hard ceiling — protects against both an oversized
        // `RTDB_MAX_FILE_SIZE` env seed and (defense-in-depth) any future code
        // path that mutates `HotConfig` without going through `upload_handler`'s
        // own clamp (SEC-008). The PATCH handler in admin.rs still accepts
        // values above this ceiling into the persisted row; `upload_handler`
        // re-clamps at the buffering point so the on-disk worst case is bounded
        // regardless of what the persisted row says.
        let max_file_size = max_file_size.min(HARD_MAX_FILE_SIZE);
        let idempotency_ttl_ms = match std::env::var("RTDB_IDEMPOTENCY_TTL_MS") {
            Ok(v) => v
                .parse::<i64>()
                .unwrap_or(crate::mutation_log::DEFAULT_DEDUP_TTL_MS),
            Err(_) => crate::mutation_log::DEFAULT_DEDUP_TTL_MS,
        };
        Self {
            allowed_origins,
            session_ttl_days,
            max_file_size,
            idempotency_ttl_ms,
        }
    }

    /// True when every origin is a strict `scheme://host[:port]` URL with no
    /// characters that could break out of the OAuth callback's JS string
    /// interpolation (`"`, `<`, `>`, backtick, backslash). The CORS layer would
    /// silently skip a malformed origin at request time, but the OAuth callback
    /// HTML (`provider::callback_html_response`) interpolates each origin into a
    /// JS string literal, so the validator must reject any metacharacter that
    /// could escape that context — `HeaderValue::from_str` alone permits `"`,
    /// `<`, `>`, which is insufficient (SEC-005). Defense in depth: even after
    /// this check, `callback_html_response` JS-escapes its interpolations.
    pub fn origins_valid(&self) -> bool {
        self.allowed_origins.iter().all(|o| origin_is_valid(o))
    }
}

/// Strict per-origin validation for `HotConfig::origins_valid`. Accepts only
/// `http(s)://host[:port]` — ASCII host of letters/digits/dot/hyphen, optional
/// `:`port of digits. Rejects any metacharacter (`"`, `<`, `>`, backtick,
/// backslash) that could break out of the OAuth callback's JS string context,
/// and any path/query/fragment (origins are authority-only by CORS contract).
pub(crate) fn origin_is_valid(origin: &str) -> bool {
    // Reject any byte that could break out of a JS string literal or HTML
    // context, plus control bytes. This guard is load-bearing even when the
    // structural parse below rejects the same input — defense in depth against
    // future relaxations of the structural rule.
    if origin
        .bytes()
        .any(|b| matches!(b, b'"' | b'<' | b'>' | b'`' | b'\\' | 0x00..=0x1f | 0x7f))
    {
        return false;
    }
    let rest = match origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
    {
        Some(rest) => rest,
        None => return false,
    };
    if rest.is_empty() {
        return false;
    }
    // Optional `:port`. Split on the last `:` if present; the host portion must
    // not contain a colon either way.
    let (host, port) = match rest.rfind(':') {
        Some(idx) => (&rest[..idx], Some(&rest[idx + 1..])),
        None => (rest, None),
    };
    if host.is_empty() || host.bytes().any(|b| !is_host_byte(b)) {
        return false;
    }
    match port {
        None => true,
        Some(p) if p.bytes().all(|b| b.is_ascii_digit()) => !p.is_empty(),
        Some(_) => false,
    }
}

fn is_host_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'.' || b == b'-'
}

/// Lenient read form of the persisted `rtdb_config.hot` row: every field is
/// optional, so a row written before a field existed still loads and the absent
/// field falls back to its ENV-seeded value.
///
/// This is what makes adding a `HotConfig` field non-breaking, and it is not
/// theoretical. `load_hot` used to `from_value::<HotConfig>` strictly, so ONE
/// missing key failed the entire decode and boot fell back to env — silently
/// discarding every operator `PATCH /admin/config` from then on. Found live in
/// prod 2026-07-29 (`decode rtdb_config: missing field idempotencyTtlMs`),
/// where the row had been unreadable since that field was introduced.
///
/// Falling back per-field to the env seed rather than to `Default` is
/// deliberate: `session_ttl_days: 0` (every session instantly expired) and
/// `max_file_size: 0` (every upload rejected) are actively harmful, so a
/// blanket `#[serde(default)]` on `HotConfig` would trade one silent failure
/// for a worse one.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PersistedHotConfig {
    #[serde(default)]
    allowed_origins: Option<Vec<String>>,
    #[serde(default)]
    session_ttl_days: Option<i64>,
    #[serde(default)]
    max_file_size: Option<usize>,
    #[serde(default)]
    idempotency_ttl_ms: Option<i64>,
}

impl PersistedHotConfig {
    /// Overlays the persisted values onto `defaults` (the env seed), field by
    /// field. Unknown keys in the row are ignored, so removing a `HotConfig`
    /// field is non-breaking in the other direction too.
    fn merge_onto(self, defaults: HotConfig) -> HotConfig {
        HotConfig {
            allowed_origins: self.allowed_origins.unwrap_or(defaults.allowed_origins),
            session_ttl_days: self.session_ttl_days.unwrap_or(defaults.session_ttl_days),
            max_file_size: self.max_file_size.unwrap_or(defaults.max_file_size),
            idempotency_ttl_ms: self
                .idempotency_ttl_ms
                .unwrap_or(defaults.idempotency_ttl_ms),
        }
    }
}

/// Loads the single persisted hot row, if any, overlaying it onto `defaults`
/// (normally `HotConfig::from_env()`) so a row missing newer fields still
/// applies the ones it does carry. A missing row is `Ok(None)` (first boot); a
/// sqlx error or a STRUCTURALLY invalid row (e.g. `allowedOrigins` holding a
/// number) is still an internal error, which the caller logs before falling
/// back to env.
pub async fn load_hot(
    pool: &sqlx::PgPool,
    defaults: &HotConfig,
) -> Result<Option<HotConfig>, RtDbError> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT hot FROM rtdb_config WHERE id = 1")
            .fetch_optional(pool)
            .await
            .map_err(|e| RtDbError::internal(format!("load rtdb_config: {e}")))?;
    match row {
        Some((v,)) => serde_json::from_value::<PersistedHotConfig>(v)
            .map(|persisted| Some(persisted.merge_onto(defaults.clone())))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn env_seed() -> HotConfig {
        HotConfig {
            allowed_origins: vec!["https://env-origin.example".to_string()],
            session_ttl_days: 30,
            max_file_size: 50 * 1024 * 1024,
            idempotency_ttl_ms: 300_000,
        }
    }

    /// The exact prod row found on 2026-07-29: three origins, no
    /// `idempotencyTtlMs`. It must load, keep its own values, and take the env
    /// seed only for the absent field.
    #[test]
    fn persisted_row_missing_a_newer_field_still_applies_the_rest() {
        let row = serde_json::json!({
            "maxFileSize": 52428800,
            "allowedOrigins": [
                "https://projects.pardev.net",
                "https://hack.pardev.net",
                "https://rtdb.pardev.net"
            ],
            "sessionTtlDays": 30
        });
        let persisted: PersistedHotConfig =
            serde_json::from_value(row).expect("a row missing a newer field must still decode");
        let merged = persisted.merge_onto(env_seed());
        assert_eq!(merged.allowed_origins.len(), 3, "persisted origins win");
        assert!(
            merged
                .allowed_origins
                .contains(&"https://hack.pardev.net".to_string())
        );
        assert_eq!(merged.max_file_size, 52428800);
        assert_eq!(merged.session_ttl_days, 30);
        // The one absent field falls back to the env seed...
        assert_eq!(merged.idempotency_ttl_ms, 300_000);
    }

    /// ...and specifically NOT to the type default, which would be harmful.
    #[test]
    fn absent_fields_fall_back_to_env_not_to_zero() {
        let persisted: PersistedHotConfig =
            serde_json::from_value(serde_json::json!({})).expect("an empty row decodes");
        let merged = persisted.merge_onto(env_seed());
        assert_eq!(
            merged.session_ttl_days, 30,
            "0 would expire every session instantly"
        );
        assert_eq!(
            merged.max_file_size,
            50 * 1024 * 1024,
            "0 would reject every upload"
        );
        assert_eq!(merged.idempotency_ttl_ms, 300_000);
        assert_eq!(merged.allowed_origins, env_seed().allowed_origins);
    }

    /// A field the code no longer knows about must not fail the decode either —
    /// removing a `HotConfig` field stays non-breaking.
    #[test]
    fn unknown_persisted_fields_are_ignored() {
        let row = serde_json::json!({
            "sessionTtlDays": 7,
            "somethingRetired": "whatever"
        });
        let persisted: PersistedHotConfig = serde_json::from_value(row).expect("decodes");
        assert_eq!(persisted.merge_onto(env_seed()).session_ttl_days, 7);
    }

    /// A structurally wrong value is still an error — the caller logs it and
    /// falls back to env wholesale, which is the right response to a corrupt
    /// row (as opposed to a merely older one).
    #[test]
    fn structurally_invalid_row_is_still_rejected() {
        let row = serde_json::json!({ "allowedOrigins": 42 });
        assert!(serde_json::from_value::<PersistedHotConfig>(row).is_err());
    }

    #[test]
    fn origin_is_valid_accepts_https_and_http_with_and_without_port() {
        assert!(origin_is_valid("https://example.com"));
        assert!(origin_is_valid("http://localhost:3000"));
        assert!(origin_is_valid("https://app.example.com:8443"));
        assert!(origin_is_valid("http://127.0.0.1:8080"));
    }

    #[test]
    fn origin_is_valid_rejects_metacharacters_that_break_js_strings() {
        // Each of these is the SEC-005 self-XSS payload class.
        assert!(!origin_is_valid(
            "https://example.com\"];alert(document.domain);//"
        ));
        assert!(!origin_is_valid("https://example.com\""));
        assert!(!origin_is_valid("https://example.com<"));
        assert!(!origin_is_valid("https://example.com>"));
        assert!(!origin_is_valid("https://example.com`"));
        assert!(!origin_is_valid("https://example.com\\"));
    }

    #[test]
    fn origin_is_valid_rejects_missing_scheme_and_paths() {
        assert!(!origin_is_valid("example.com"));
        assert!(!origin_is_valid("ftp://example.com"));
        assert!(!origin_is_valid("https://example.com/"));
        assert!(!origin_is_valid("https://example.com/path"));
        assert!(!origin_is_valid("https://example.com?q=1"));
        assert!(!origin_is_valid(""));
    }

    #[test]
    fn origin_is_valid_rejects_invalid_host_and_port() {
        assert!(!origin_is_valid("https://"));
        assert!(!origin_is_valid("https://exa mple.com"));
        assert!(!origin_is_valid("https://example.com:abc"));
        assert!(!origin_is_valid("https://example.com:"));
    }

    #[test]
    fn origins_valid_aggregates_per_origin() {
        let mut hot = HotConfig {
            allowed_origins: vec!["https://a.com".into(), "https://b.com".into()],
            session_ttl_days: 30,
            max_file_size: 1024,
            idempotency_ttl_ms: crate::mutation_log::DEFAULT_DEDUP_TTL_MS,
        };
        assert!(hot.origins_valid());
        hot.allowed_origins.push("https://c.com\"".into());
        assert!(!hot.origins_valid());
    }
}
