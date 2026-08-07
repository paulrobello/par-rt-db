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
    pub gitlab_client_id: Option<String>,     // RTDB_GITLAB_CLIENT_ID
    pub gitlab_client_secret: Option<String>, // RTDB_GITLAB_CLIENT_SECRET
    pub gitlab_base_url: String,              // RTDB_GITLAB_BASE_URL, default https://gitlab.com
    // Generic OpenID Connect provider (one impl for any standards-compliant
    // IdP: Azure AD, Keycloak, Auth0, Okta, self-hosted). The authorize/token/
    // userinfo URLs come from the IdP's /.well-known/openid-configuration — the
    // trait's sync authorize_url can't do live discovery, so endpoints are
    // configuration. Active only when all five are set; else routes return 503.
    pub oidc_client_id: Option<String>,     // RTDB_OIDC_CLIENT_ID
    pub oidc_client_secret: Option<String>, // RTDB_OIDC_CLIENT_SECRET
    pub oidc_authorize_url: Option<String>, // RTDB_OIDC_AUTHORIZE_URL
    pub oidc_token_url: Option<String>,     // RTDB_OIDC_TOKEN_URL
    pub oidc_userinfo_url: Option<String>,  // RTDB_OIDC_USERINFO_URL
    // Microsoft (Entra ID / Azure AD v2.0) OAuth provider. Models on the generic
    // OIDC provider but derives Microsoft's well-known authorize/token/userinfo
    // endpoints from `microsoft_tenant`, so the operator supplies credentials +
    // tenant only (no four-URL paste). RTDB_MICROSOFT_CLIENT_ID /
    // RTDB_MICROSOFT_CLIENT_SECRET / RTDB_MICROSOFT_TENANT (default "common" =
    // any Microsoft account; a tenant GUID/name restricts to one org).
    pub microsoft_client_id: Option<String>, // RTDB_MICROSOFT_CLIENT_ID
    pub microsoft_client_secret: Option<String>, // RTDB_MICROSOFT_CLIENT_SECRET
    pub microsoft_tenant: String,            // RTDB_MICROSOFT_TENANT, default "common"
    // Sign in with Apple. Apple rejects a static client_secret: the secret sent
    // to Apple's token endpoint is a short-lived ES256 JWT the server signs with
    // the private key registered with Apple, assembled from four config pieces.
    // Identity keys on Apple's stable `sub` (see `apple.rs`), because Apple may
    // relay the email through `@privaterelay.appleid.com`. RTDB_APPLE_* env.
    pub apple_client_id: Option<String>, // RTDB_APPLE_CLIENT_ID (Services ID)
    pub apple_team_id: Option<String>,   // RTDB_APPLE_TEAM_ID
    pub apple_key_id: Option<String>,    // RTDB_APPLE_KEY_ID
    pub apple_private_key: Option<String>, // RTDB_APPLE_PRIVATE_KEY (PEM, \n-escaped)
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
    // Login-CSRF defense: bind the OAuth `state` to the initiating browser via a
    // double-submit nonce cookie set at /begin and verified at /callback. On by
    // default — only "false"/"0"/"no" (case-insensitive) disables it (break-glass,
    // restores pre-hardening behavior). RTDB_OAUTH_LOGIN_CSRF.
    pub oauth_login_csrf: bool,
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
    // Document TTL reaper. RTDB_TTL_SWEEP_INTERVAL_SECS (default 60) is the
    // per-db cadence; RTDB_TTL_BATCH (default 5000) bounds rows deleted per
    // table per sweep. TTL is best-effort, so these are boot-only (not hot).
    pub ttl_sweep_interval_secs: u64,
    pub ttl_batch: i64,
    // On-the-fly image transforms on storage serve (ENH-014). RTDB_IMAGE_*.
    // Boot-time operational knobs (not admin-mutable). All optional w/ defaults.
    pub image_transforms_enabled: bool, // RTDB_IMAGE_TRANSFORMS_ENABLED, default true
    pub image_max_dim: u32,             // RTDB_IMAGE_MAX_DIM, default 2048
    pub image_max_pixels: u64,          // RTDB_IMAGE_MAX_PIXELS, default 25_000_000
    pub image_cache_bytes: u64,         // RTDB_IMAGE_CACHE_BYTES, default 256 MiB
    pub image_concurrency: usize,       // RTDB_IMAGE_CONCURRENCY, default 4
    pub image_default_quality: u8,      // RTDB_IMAGE_DEFAULT_QUALITY, default 80
    // ---- Presence (ENH-015) ----
    // Boot-only operational knobs for realtime presence (not hot). Default-off
    // master switch + caps consumed by `PresenceConfig::from_config` (Task 3).
    /// RTDB_PRESENCE_ENABLED (default true). Master switch.
    pub presence_enabled: bool,
    /// RTDB_PRESENCE_MAX_STATE_BYTES (default 1024).
    pub presence_max_state_bytes: usize,
    /// RTDB_PRESENCE_MAX_ROOM_SIZE (default 100).
    pub presence_max_room_size: usize,
    /// RTDB_PRESENCE_MAX_ROOMS_PER_CONN (default 32).
    pub presence_max_rooms_per_conn: usize,
    /// RTDB_PRESENCE_MAX_ROOM_BYTES (default 256). Room-name length cap.
    pub presence_max_room_bytes: usize,
    /// RTDB_PRESENCE_BROADCAST_INTERVAL_MS (default 50). 0 = immediate.
    pub presence_broadcast_interval_ms: u64,
    /// RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC (default 20).
    pub presence_update_limit_per_sec: u32,
    /// RTDB_PRESENCE_MAX_TTL_MS (default 300000 = 5 min). Upper bound on a
    /// client-supplied presenceState ttlMs; over-cap is rejected (no clamping).
    pub presence_max_ttl_ms: u64,
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

        let gitlab_client_id = std::env::var("RTDB_GITLAB_CLIENT_ID").ok();
        let gitlab_client_secret = std::env::var("RTDB_GITLAB_CLIENT_SECRET").ok();
        let gitlab_base_url = std::env::var("RTDB_GITLAB_BASE_URL")
            .unwrap_or_else(|_| "https://gitlab.com".to_string());

        let oidc_client_id = std::env::var("RTDB_OIDC_CLIENT_ID").ok();
        let oidc_client_secret = std::env::var("RTDB_OIDC_CLIENT_SECRET").ok();
        let oidc_authorize_url = std::env::var("RTDB_OIDC_AUTHORIZE_URL").ok();
        let oidc_token_url = std::env::var("RTDB_OIDC_TOKEN_URL").ok();
        let oidc_userinfo_url = std::env::var("RTDB_OIDC_USERINFO_URL").ok();

        // Microsoft (Entra ID / Azure AD v2.0). `tenant` defaults to "common"
        // (any Microsoft account); an empty value falls back to that default so
        // a blank RTDB_MICROSOFT_TENANT isn't interpolated into the endpoint URL.
        let microsoft_client_id = std::env::var("RTDB_MICROSOFT_CLIENT_ID").ok();
        let microsoft_client_secret = std::env::var("RTDB_MICROSOFT_CLIENT_SECRET").ok();
        let microsoft_tenant = match std::env::var("RTDB_MICROSOFT_TENANT") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "common".to_string(),
        };

        // Sign in with Apple. The private key is a PEM, which can't carry real
        // newlines through most env stores, so `\n` escapes are unescaped here.
        let apple_client_id = std::env::var("RTDB_APPLE_CLIENT_ID").ok();
        let apple_team_id = std::env::var("RTDB_APPLE_TEAM_ID").ok();
        let apple_key_id = std::env::var("RTDB_APPLE_KEY_ID").ok();
        let apple_private_key = std::env::var("RTDB_APPLE_PRIVATE_KEY")
            .ok()
            .map(|v| v.replace("\\n", "\n"));

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

        // Login-CSRF: default ON (security). Only an explicit falsy spelling
        // disables it (break-glass). Anything else, including unset, stays on.
        let oauth_login_csrf = match std::env::var("RTDB_OAUTH_LOGIN_CSRF") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
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

        // Document TTL reaper. Best-effort expiry, so boot-only (not hot). An
        // unparseable value falls back to the default, matching the parses above.
        let ttl_sweep_interval_secs = match std::env::var("RTDB_TTL_SWEEP_INTERVAL_SECS") {
            Ok(v) => v.parse::<u64>().unwrap_or(60),
            Err(_) => 60,
        };
        // `tokio::time::interval` panics on a zero duration, so an explicit 0
        // would crash every db's reaper task on its first poll.
        let ttl_sweep_interval_secs = ttl_sweep_interval_secs.max(1);
        let ttl_batch = match std::env::var("RTDB_TTL_BATCH") {
            Ok(v) => v.parse::<i64>().unwrap_or(5000),
            Err(_) => 5000,
        };
        // `DELETE ... LIMIT 0` is a silent no-op (disables reaping) and a
        // negative batch errors per sweep, so clamp both to at least 1.
        let ttl_batch = ttl_batch.max(1);

        // On-the-fly image transforms on storage serve (ENH-014). Boot-only
        // operational knobs. Default-on (mirror the login-CSRF block above);
        // numerics follow the `ttl_batch` `.unwrap_or(default)` + clamp idiom.
        let image_transforms_enabled = match std::env::var("RTDB_IMAGE_TRANSFORMS_ENABLED") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
        };
        let image_max_dim = match std::env::var("RTDB_IMAGE_MAX_DIM") {
            Ok(v) => v.trim().parse::<u32>().unwrap_or(2048).clamp(1, 8192),
            Err(_) => 2048,
        };
        let image_max_pixels = match std::env::var("RTDB_IMAGE_MAX_PIXELS") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or(25_000_000).max(1_000_000),
            Err(_) => 25_000_000,
        };
        let image_cache_bytes = match std::env::var("RTDB_IMAGE_CACHE_BYTES") {
            Ok(v) => v.trim().parse::<u64>().unwrap_or(256 * 1024 * 1024),
            Err(_) => 256 * 1024 * 1024,
        };
        let image_concurrency = match std::env::var("RTDB_IMAGE_CONCURRENCY") {
            Ok(v) => v.trim().parse::<usize>().unwrap_or(4).max(1),
            Err(_) => 4,
        };
        let image_default_quality = match std::env::var("RTDB_IMAGE_DEFAULT_QUALITY") {
            Ok(v) => v.trim().parse::<u8>().unwrap_or(80).clamp(1, 100),
            Err(_) => 80,
        };

        // Realtime presence (ENH-015). Default-ON master switch
        // (image-transforms style: anything but an explicit false/0/no stays
        // on) + numerics following the `ttl_batch` `.unwrap_or(default)` +
        // `.max(1)` clamp idiom (a 0 size/count/limit would be unusable; a 0
        // broadcast interval means "immediate", so it is NOT clamped).
        let presence_enabled = match std::env::var("RTDB_PRESENCE_ENABLED") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
        };
        let presence_max_state_bytes = std::env::var("RTDB_PRESENCE_MAX_STATE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024)
            .max(1);
        let presence_max_room_size = std::env::var("RTDB_PRESENCE_MAX_ROOM_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100)
            .max(1);
        let presence_max_rooms_per_conn = std::env::var("RTDB_PRESENCE_MAX_ROOMS_PER_CONN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(32)
            .max(1);
        let presence_max_room_bytes = std::env::var("RTDB_PRESENCE_MAX_ROOM_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256)
            .max(1);
        let presence_broadcast_interval_ms = std::env::var("RTDB_PRESENCE_BROADCAST_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50);
        let presence_update_limit_per_sec = std::env::var("RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20)
            .max(1);
        let presence_max_ttl_ms = std::env::var("RTDB_PRESENCE_MAX_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300_000)
            .max(1000);

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
            gitlab_client_id,
            gitlab_client_secret,
            gitlab_base_url,
            oidc_client_id,
            oidc_client_secret,
            oidc_authorize_url,
            oidc_token_url,
            oidc_userinfo_url,
            microsoft_client_id,
            microsoft_client_secret,
            microsoft_tenant,
            apple_client_id,
            apple_team_id,
            apple_key_id,
            apple_private_key,
            max_affected_docs,
            static_dir,
            pool_max_connections,
            rate_limit_per_token_rpm,
            rate_limit_per_db_rpm,
            audit_log_enabled,
            oauth_login_csrf,
            webhooks_enabled,
            backup_enabled,
            backup_cron,
            backup_dir,
            backup_retention,
            subs_verify_skip_every,
            ttl_sweep_interval_secs,
            ttl_batch,
            image_transforms_enabled,
            image_max_dim,
            image_max_pixels,
            image_cache_bytes,
            image_concurrency,
            image_default_quality,
            presence_enabled,
            presence_max_state_bytes,
            presence_max_room_size,
            presence_max_rooms_per_conn,
            presence_max_room_bytes,
            presence_broadcast_interval_ms,
            presence_update_limit_per_sec,
            presence_max_ttl_ms,
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
    /// metacharacters (`"`, `<`, `>`, backtick, backslash). The CORS layer
    /// (`lib::cors_layer`) checks each request's `Origin` against this list by
    /// exact membership, so a malformed entry would never match a real browser
    /// `Origin` — silently broken CORS, caught here at config time instead.
    /// Origins are also the OAuth `begin` allowlist, so the same gate bounds
    /// which parents may start a popup login. Defense in depth:
    /// `HeaderValue::from_str` alone permits `"`, `<`, `>`, which is
    /// insufficient, so the validator rejects them regardless.
    pub fn origins_valid(&self) -> bool {
        self.allowed_origins.iter().all(|o| origin_is_valid(o))
    }
}

/// Strict per-origin validation for `HotConfig::origins_valid`. Accepts only
/// `http(s)://host[:port]` — ASCII host of letters/digits/dot/hyphen, optional
/// `:`port of digits. Rejects any metacharacter (`"`, `<`, `>`, backtick,
/// backslash) — defense in depth, since `HeaderValue::from_str` permits several
/// of them — and any path/query/fragment (origins are authority-only by CORS
/// contract).
pub(crate) fn origin_is_valid(origin: &str) -> bool {
    // Reject any byte that is unsafe in an `Origin` header value or could
    // corrupt header construction, plus control bytes. This guard is
    // load-bearing even when the structural parse below rejects the same input
    // — defense in depth against future relaxations of the structural rule.
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

    /// `RTDB_PRESENCE_*` boot knobs: defaults when unset, env overrides honored.
    /// `from_env` requires `RTDB_DATABASE_URL` and `RTDB_ADMIN_KEY`, so this test
    /// sets them for its lifetime and restores them at the end. All `RTDB_PRESENCE_*`
    /// vars are removed at the end so they don't leak into other lib tests.
    #[test]
    fn presence_env_defaults_and_overrides() {
        // Rust 2024: env mutation is `unsafe` (not thread-safe in general). Within
        // a single test binary where no other test reads these vars, this is sound.
        unsafe {
            // Seed the two required vars (save originals to restore at the end).
            let saved_db = std::env::var("RTDB_DATABASE_URL").ok();
            let saved_key = std::env::var("RTDB_ADMIN_KEY").ok();
            std::env::set_var("RTDB_DATABASE_URL", "postgres://test");
            std::env::set_var("RTDB_ADMIN_KEY", "test-key");

            // Defaults (vars unset): enabled=true (default-on), sensible caps.
            std::env::remove_var("RTDB_PRESENCE_ENABLED");
            std::env::remove_var("RTDB_PRESENCE_MAX_STATE_BYTES");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOM_SIZE");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOMS_PER_CONN");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOM_BYTES");
            std::env::remove_var("RTDB_PRESENCE_BROADCAST_INTERVAL_MS");
            std::env::remove_var("RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC");
            std::env::remove_var("RTDB_PRESENCE_MAX_TTL_MS");
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(c.presence_enabled);
            assert_eq!(c.presence_max_state_bytes, 1024);
            assert_eq!(c.presence_max_room_size, 100);
            assert_eq!(c.presence_max_rooms_per_conn, 32);
            assert_eq!(c.presence_max_room_bytes, 256);
            assert_eq!(c.presence_broadcast_interval_ms, 50);
            assert_eq!(c.presence_update_limit_per_sec, 20);
            assert_eq!(c.presence_max_ttl_ms, 300_000);

            // Overrides: the on/off switch accepts "true", and numerics parse.
            std::env::set_var("RTDB_PRESENCE_ENABLED", "true");
            std::env::set_var("RTDB_PRESENCE_MAX_ROOM_SIZE", "5");
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(c.presence_enabled);
            assert_eq!(c.presence_max_room_size, 5);

            // Default-on: an explicit "false" disables.
            std::env::set_var("RTDB_PRESENCE_ENABLED", "false");
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(!c.presence_enabled);

            // Cleanup: remove presence vars (don't leak into other lib tests) and
            // restore the required vars to their pre-test state.
            std::env::remove_var("RTDB_PRESENCE_ENABLED");
            std::env::remove_var("RTDB_PRESENCE_MAX_STATE_BYTES");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOM_SIZE");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOMS_PER_CONN");
            std::env::remove_var("RTDB_PRESENCE_MAX_ROOM_BYTES");
            std::env::remove_var("RTDB_PRESENCE_BROADCAST_INTERVAL_MS");
            std::env::remove_var("RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC");
            std::env::remove_var("RTDB_PRESENCE_MAX_TTL_MS");
            match saved_db {
                Some(v) => std::env::set_var("RTDB_DATABASE_URL", v),
                None => std::env::remove_var("RTDB_DATABASE_URL"),
            }
            match saved_key {
                Some(v) => std::env::set_var("RTDB_ADMIN_KEY", v),
                None => std::env::remove_var("RTDB_ADMIN_KEY"),
            }
        }
    }
}
