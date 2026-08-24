//! Configuration — boot-time `Config` (env-sourced, immutable) and
//! runtime-mutable `HotConfig` (held on `AppState` as `Arc<ArcSwap<HotConfig>>`,
//! persisted in a single-row `rtdb_config` table). The four hot settings —
//! `allowed_origins`, `session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`
//! — swap live via `PATCH /admin/config` with no restart; the `CorsLayer` origin
//! check re-reads `allowed_origins` per request. `GET /admin/config` is
//! structurally redacted (secrets surface as configured-bools, never values).

mod hot;
mod oauth;
pub(crate) use hot::HARD_MAX_FILE_SIZE;
pub use hot::{HotConfig, load_hot, save_hot};
pub use oauth::{
    AppleOAuth, GithubOAuth, GitlabOAuth, GoogleOAuth, MicrosoftOAuth, OAuthConfig, OidcProvider,
};

/// Minimum admin-key length enforced at boot (SEC-110). 16 chars is the floor
/// below which a key is brute-forceable over the public `POST /admin/login`
/// endpoint within practical time even without the rate limiter; the
/// recommended length is far higher (a 32-byte hex string, e.g. 64 chars).
pub(crate) const MIN_ADMIN_KEY_LEN: usize = 16;

/// Default sampling interval for the subscription-skip shadow verifier
/// (ARC-101). A wrong skip is otherwise silent (no error — the client just
/// never hears the update), so the verifier ships ON at 1-in-1000: one extra
/// query per thousand skips to re-run and diff against the last pushed result.
/// This is the runtime detection for the one failure mode the architecture
/// documents as silent; 0 disables it. A malformed/typo'd value falls back to
/// this default (not 0) so a typo cannot silently disable the safety net
/// (pre-empts ARC-118).
pub(crate) const DEFAULT_SUBS_VERIFY_SKIP_EVERY: u64 = 1000;

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,            // RTDB_PORT, default 8300
    pub database_url: String, // RTDB_DATABASE_URL (required)
    pub admin_key: String,    // RTDB_ADMIN_KEY (required)
    pub public_url: String,   // RTDB_PUBLIC_URL, default http://localhost:8300
    /// The six OAuth/OIDC providers (github/google/gitlab/oidc/microsoft/apple).
    /// See `config::oauth` for each provider's field docs.
    pub oauth: OAuthConfig,
    pub max_affected_docs: usize, // RTDB_MAX_AFFECTED_DOCS, default 100 (admin data-browser guardrail)
    pub static_dir: Option<String>, // RTDB_STATIC_DIR — unset/empty ⇒ API-only (no SPA served)
    pub pool_max_connections: u32, // RTDB_POOL_MAX_CONNECTIONS, default 75 (multi-tenant; one committer task + N sub re-runs per db)
    // In-memory pushed-schema cache bound (ARC-119). Entry-count cap on the
    // per-process `SchemaCache`; LRU-evicted past the cap, transparently
    // reloaded from Postgres on the next `get`. 0 = unbounded (prior
    // behavior); default 1024 covers multi-tenant instances that create and
    // drop databases over time without growing the map forever.
    pub schema_cache_max_entries: u64, // RTDB_SCHEMA_CACHE_MAX_ENTRIES, default 1024
    // Slow-query log (ENH-019). `slow_query_ms` is the threshold that lands a
    // query in the bounded log (0 = off, the default); `slow_query_capacity`
    // bounds the in-memory ring buffer (default 200); `slow_query_log_params`
    // controls whether bound parameter values are captured alongside the SQL
    // (default false — keeps document content out of the log until an operator
    // opts in). The log surfaces at `GET /admin/slow-queries` and the SQL
    // string is the exact text the real query path executed (the same string
    // `/explain` returns), so a slow entry is reproducible via `EXPLAIN`.
    pub slow_query_ms: u64,          // RTDB_SLOW_QUERY_MS, default 0 (off)
    pub slow_query_capacity: usize,  // RTDB_SLOW_QUERY_CAPACITY, default 200
    pub slow_query_log_params: bool, // RTDB_SLOW_QUERY_LOG_PARAMS, default false
    // HTTP rate limiting (v1, fixed-window, in-memory). These two bound
    // *authenticated* traffic, so they deliberately stay 0 = unlimited
    // (SEC-203 carve-out): a surprise non-zero default here can break real
    // apps' legitimate throughput — opt in per deploy. 0 disables.
    // RTDB_RATE_LIMIT_PER_TOKEN_RPM caps each machine token; OAuth sessions
    // carry no token id and are rate-limited per-db only.
    pub rate_limit_per_token_rpm: u32,
    pub rate_limit_per_db_rpm: u32, // RTDB_RATE_LIMIT_PER_DB_RPM, shared across all principals of one db
    // ARC-007: multi-instance-only knobs governing which `RateLimiter::check`
    // path a Postgres-backed limiter uses. Both are inert in single-instance
    // mode (no `pg` pool on the limiter, so `check` never reaches either
    // branch).
    /// RTDB_RATE_LIMIT_EXACT (default false). false = each replica counts
    /// locally and reconciles with `rtdb_auth.rate_counters` every
    /// `rate_limit_sync_ms` (approximate, no per-request Postgres round
    /// trip); overshoot is bounded by roughly (replica count × one sync
    /// window) of extra allowance, since a replica can admit up to its local
    /// budget before reconciling — see `rate_limit::RateLimiter` docs. true =
    /// every checked request pays a synchronous UPSERT for an exact shared
    /// ceiling (the pre-ARC-007 behavior).
    pub rate_limit_exact: bool,
    /// RTDB_RATE_LIMIT_SYNC_MS (default 1000, clamped to >= 50). How often
    /// the approximate limiter (`rate_limit_exact == false`) flushes local
    /// deltas into Postgres and refreshes its shared-count view. Unused when
    /// `rate_limit_exact` is true.
    pub rate_limit_sync_ms: u64,
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
    // Webhook SSRF dev-escape hatch (SEC-001). When true, the registration
    // validator permits `http://` URLs AND skips the private/loopback IP-range
    // denylist so a developer can point a webhook at a local receiver
    // (`http://127.0.0.1:...`). Default false: production enforces HTTPS and
    // rejects any URL whose host resolves to a private, loopback, link-local,
    // multicast, or cloud-metadata address. RTDB_WEBHOOK_ALLOW_HTTP.
    pub webhook_allow_http: bool,
    // Per-IP rate limit on the unauthenticated `GET /storage/{id}` route
    // (SEC-004 / SEC-203). Default 300 RPM — blob serving is high-volume
    // legitimate traffic, but the opaque id is not a license to hammer the
    // route: the on-the-fly image-transform path amplifies cost (each
    // distinct `?w=&h=&...` set misses the cache and burns decode CPU).
    // 0 disables. RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM.
    pub storage_rate_limit_per_ip_rpm: u32,
    // SEC-113: when true, the public storage serve route (`GET /storage/{id}`)
    // requires a valid `?exp=&sig=` pair on every request — a holder of the
    // opaque id alone is no longer enough. Default false so existing public
    // bearer URLs (a deliberate Convex-parity feature) keep working; operators
    // who want signed-URL-only access (e.g. for sensitive content) flip it on.
    // The mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) is unaffected
    // and remains the way to mint time-limited URLs under either mode.
    // RTDB_STORAGE_REQUIRE_SIGNED_URLS.
    pub storage_require_signed_urls: bool,
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
    // 0 = off; ships ON at 1000 by default (ARC-101). 1 = verify every skip
    // (integration tests). Each verification costs exactly the Postgres
    // round-trip the skip avoided, so this trades the optimization back for
    // confidence — keep N large in production. Sampling is deterministic (every
    // Nth skip), not random, so a test can pin it. RTDB_SUBS_VERIFY_SKIP_EVERY.
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
    // Boot-only operational knobs for realtime presence (not hot). Master
    // switch + caps consumed by `PresenceConfig::from_config` (Task 3); the
    // switch ships default-ON (see the field doc just below).
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
    /// RTDB_PRESENCE_BEAT_INTERVAL_MS (default 5000). ENH-022 Stage 3
    /// cross-instance presence gossip: cadence at which each instance
    /// republishes its full per-room membership snapshot to peers via
    /// `pg_notify('rtdb_presence', …)`. A peer that missed an incremental
    /// NOTIFY resyncs off the next beat. Only consulted when
    /// `RTDB_MULTI_INSTANCE=true` AND `RTDB_PRESENCE_ENABLED=true`; a
    /// single-instance deploy never spawns the beat task and never reads this.
    /// Floored at 1000ms so an operator typo cannot hot-spin the channel.
    pub presence_beat_interval_ms: u64,
    /// RTDB_PRESENCE_BEAT_TIMEOUT_MS (default 15000). ENH-022 Stage 3: a peer
    /// whose last beat is older than this is assumed dead (crashed, network
    /// partition) and its shadow entries are evicted from the local peer map,
    /// so its members disappear from the union broadcast within this window.
    /// Defaults to 3 × the beat interval. Floored at the beat interval so an
    /// operator cannot set a timeout shorter than the cadence (which would
    /// evict live peers on every tick).
    pub presence_beat_timeout_ms: u64,
    /// RTDB_AUTH_ANONYMOUS_ENABLED (default false). Master switch for
    /// `POST /auth/anonymous`, which mints an ephemeral user + session for a
    /// credential-less guest. Off ⇒ the endpoint returns 403 FORBIDDEN. An
    /// anonymous user is authorized for a database ONLY when that database has
    /// opted in via `rtdb_auth.databases.anonymous_enabled` (SEC-103) — the
    /// instance-wide boot gate is the master kill (checked at mint), the per-db
    /// column is the additional gate (checked at `authorize`). An anonymous user
    /// owns its own documents via per-row `ownerField`. Opt-in like presence,
    /// but default-OFF (anonymous access is a per-app decision). Boot-only (not
    /// hot-reloadable).
    pub auth_anonymous_enabled: bool,
    /// RTDB_ANONYMOUS_SESSION_TTL_DAYS (default 1). Session TTL for anonymous
    /// principals — deliberately short relative to the standard
    /// `session_ttl_days` (30) so the ephemeral rows minted by the
    /// unauthenticated `POST /auth/anonymous` route expire quickly rather than
    /// accumulating as permanent `rtdb_auth.users`/`rtdb_auth.sessions` rows
    /// (SEC-103). Boot-only (not hot-reloadable).
    pub anonymous_session_ttl_days: i64,
    /// RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM (default 10; 0 disables — SEC-203).
    /// Per-IP fixed-window rate limit on the unauthenticated
    /// `POST /auth/anonymous` route — without it, an attacker can mint
    /// unbounded anonymous users/sessions by hitting the endpoint in a loop
    /// (SEC-103). The IP key is canonicalized by `client_ip_key`
    /// (CF-Connecting-IP preferred, rightmost XFF fallback — trusted-proxy
    /// gated). Boot-only (not hot-reloadable).
    pub anonymous_rate_limit_per_ip_rpm: u32,
    /// RTDB_QUOTA_CACHE_TTL_SECS (default 60). TTL for the per-db quota
    /// counters (table count, storage bytes, active subs) maintained by the
    /// enforcement layer (ENH-011). 0 is interpreted as "no caching" by the
    /// reader; boot-only (not hot-reloadable) because the cache lives outside
    /// `HotConfig` and is rebuilt from `AppState` on its own cadence.
    pub quota_cache_ttl_secs: u64,
    /// RTDB_DB_IDLE_RECLAIM_SECS (default 0 = disabled). When non-zero, a
    /// background sweep retires a database's five per-db tasks (committer +
    /// scheduler + mutation-log cleanup + TTL reaper + quota warmer) once it has
    /// had no client activity for this long AND has no live subscriptions AND no
    /// pending scheduled jobs (ARC-102 step 4). Steps 1–3 already gate each
    /// poller's per-tick work, so an idle db's steady-state cost is near zero;
    /// reclamation additionally releases the task slots and channel entries. The
    /// next request respawns the tasks on demand. 0 preserves today's behavior
    /// (tasks live for the process once spawned). Boot-only (not hot-reloadable).
    pub db_idle_reclaim_secs: u64,
    /// RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM (default 10; 0 disables — SEC-203).
    /// Per-IP fixed-window rate limit on `POST /admin/login` (SEC-109) —
    /// without it, an attacker can brute-force the admin key unbounded over
    /// the public endpoint. 10 means one IP gets 10 admin-login attempts per
    /// minute before 429. The IP key is canonicalized by `client_ip_key`
    /// (CF-Connecting-IP preferred, rightmost XFF fallback — trusted-proxy
    /// gated). Boot-only (not hot-reloadable).
    pub admin_rate_limit_per_ip_rpm: u32,
    /// RTDB_COOKIE_SECURE (default true). When true, the `Secure` attribute is
    /// set on every session/CSRF cookie unconditionally — a misconfigured proxy
    /// that drops `X-Forwarded-Proto` can no longer cause the cookie to be sent
    /// over plain HTTP (SEC-120). When false, `Secure` follows
    /// `request_is_secure` (the `X-Forwarded-Proto: https` check), which is the
    /// local-http-dev escape hatch: a developer on `http://localhost` sets this
    /// to `false` so the browser still accepts the cookie. Boot-only (not hot).
    pub cookie_secure: bool,
    /// RTDB_TRUSTED_PROXY (default false). When true, the server sits behind a
    /// reverse proxy it controls and trusts the forwarding headers that proxy
    /// sets: `CF-Connecting-IP` / `X-Forwarded-For` become the per-IP
    /// rate-limit keys (`client_ip_key`), and `X-Forwarded-Proto` feeds
    /// `request_is_secure` (cookie `Secure`, HSTS). When false (the safe
    /// default), those headers are caller-controlled and ignored — the
    /// connection peer address is used — so a directly reachable deploy
    /// cannot have rate-limit buckets rotated via spoofed headers (SEC-201).
    /// Boot-only (not hot-reloadable), like `cookie_secure`.
    pub trusted_proxy: bool,
    // ---- OpenTelemetry / OTLP tracing export (ENH-018) ----
    // All boot-only. The cargo `otel` feature gates the dependency + subscriber
    // wiring; RTDB_OTEL_ENABLED gates it at runtime so a feature-compiled binary
    // can still run with tracing off. Default off: structured logs go to stdout,
    // aggregates go to Prometheus, and nothing correlates the two across one
    // request unless an operator opts in by pointing at a collector.
    /// RTDB_OTEL_ENABLED (default false). Runtime master switch — when false the
    /// server makes zero OTLP network calls even if built `--features otel`.
    pub otel_enabled: bool,
    /// RTDB_OTEL_ENDPOINT (default `http://127.0.0.1:4317`). OTLP gRPC collector
    /// endpoint (the standard OTLP/gRPC port).
    pub otel_endpoint: String,
    /// RTDB_OTEL_SERVICE_NAME (default `par-rt-db`). The `service.name` resource
    /// attribute attached to every span.
    pub otel_service_name: String,
    /// RTDB_OTEL_SAMPLE_RATIO (default 0.05). Head sampler ratio in [0.0, 1.0].
    /// A malformed value fails boot (ARC-118 via `env_parsed`) rather than
    /// silently defaulting.
    pub otel_sample_ratio: f64,
    // ---- Cross-instance op-feed fan-out (ENH-022 Stage 2) ----
    // Boot-only. When `multi_instance` is false (the default), the committer
    // never calls `pg_notify` and `AppState::new` never spawns the LISTEN task
    // — a single-instance deploy is byte-for-byte unchanged. When true, each
    // durable DocOp also emits one `pg_notify('rtdb_ops', …)` at the committer's
    // tap-site, and a per-process LISTEN task mirrors peer notifications into
    // the local op-feed ring. `instance_id` tags payloads for self-dedupe; an
    // explicit value is recommended in a multi-replica deploy so a restart
    // keeps the same id, otherwise one is generated per boot.
    /// RTDB_MULTI_INSTANCE (default false). Master switch for cross-instance
    /// op-feed via Postgres LISTEN/NOTIFY. Leave false for single-instance
    /// deploys (the default topology).
    pub multi_instance: bool,
    /// RTDB_INSTANCE_ID (default None = auto-generated). Stable replica id for
    /// NOTIFY self-dedupe. Set to a distinct value per replica in a multi-
    /// instance deploy; when unset, `AppState::new` generates a short hex id.
    pub instance_id: Option<String>,
    /// RTDB_FORWARD_TIMEOUT_MS (default 5000, clamped to >= 100). ENH-022
    /// Stage 4c: how long a non-owner replica waits for the lease owner to
    /// answer a forwarded write before attempting the takeover itself. The
    /// owner normally answers in milliseconds — this bounds the owner-dead
    /// failover latency. A reply arriving after the timeout is dropped (the
    /// write may still have committed; clients needing exactly-once retries
    /// should use idempotency keys).
    pub forward_timeout_ms: u64,
    /// RTDB_FORWARD_CONCURRENCY (default 64, clamped to >= 1). ARC-008: caps
    /// the number of forwarded-write executions `run_forward_listener` runs
    /// concurrently (one `tokio::spawn` per in-flight forwarded request).
    /// A request that arrives once the cap is saturated gets an immediate
    /// RATE_LIMITED reply instead of an unbounded task pile-up.
    pub forward_concurrency: usize,
}

/// Defaults for every field that has a real one (matching the same literal
/// defaults `Config::from_env` falls back to when its env var is unset).
/// `database_url` and `admin_key` are the two exceptions: they are required
/// at boot (no env fallback — `from_env` errors if either is missing), so
/// `Default` sets them to an empty string as an obviously-invalid sentinel
/// rather than a usable default. This exists so test/literal `Config`
/// construction (ARC-012) can write `Config { database_url: ..., admin_key:
/// ..., ..Default::default() }` instead of listing all ~90 fields, and so
/// adding a new field with a sensible default no longer breaks every such
/// literal in the test suite.
impl Default for Config {
    fn default() -> Self {
        Self {
            port: 8300,
            database_url: String::new(),
            admin_key: String::new(),
            public_url: "http://localhost:8300".to_string(),
            oauth: OAuthConfig::default(),
            max_affected_docs: 100,
            static_dir: None,
            pool_max_connections: 75,
            schema_cache_max_entries: 1024,
            slow_query_ms: 0,
            slow_query_capacity: 200,
            slow_query_log_params: false,
            rate_limit_per_token_rpm: 0,
            rate_limit_per_db_rpm: 0,
            rate_limit_exact: false,
            rate_limit_sync_ms: 1000,
            audit_log_enabled: false,
            oauth_login_csrf: true,
            webhooks_enabled: false,
            webhook_allow_http: false,
            storage_rate_limit_per_ip_rpm: 300,
            storage_require_signed_urls: false,
            backup_enabled: false,
            backup_cron: "0 3 * * *".to_string(),
            backup_dir: "./backups".to_string(),
            backup_retention: 7,
            subs_verify_skip_every: DEFAULT_SUBS_VERIFY_SKIP_EVERY,
            ttl_sweep_interval_secs: 60,
            ttl_batch: 5000,
            image_transforms_enabled: true,
            image_max_dim: 2048,
            image_max_pixels: 25_000_000,
            image_cache_bytes: 256 * 1024 * 1024,
            image_concurrency: 4,
            image_default_quality: 80,
            presence_enabled: true,
            presence_max_state_bytes: 1024,
            presence_max_room_size: 100,
            presence_max_rooms_per_conn: 32,
            presence_max_room_bytes: 256,
            presence_broadcast_interval_ms: 50,
            presence_update_limit_per_sec: 20,
            presence_max_ttl_ms: 300_000,
            presence_beat_interval_ms: 5000,
            presence_beat_timeout_ms: 15_000,
            auth_anonymous_enabled: false,
            anonymous_session_ttl_days: 1,
            anonymous_rate_limit_per_ip_rpm: 10,
            quota_cache_ttl_secs: 60,
            db_idle_reclaim_secs: 0,
            admin_rate_limit_per_ip_rpm: 10,
            cookie_secure: true,
            trusted_proxy: false,
            otel_enabled: false,
            otel_endpoint: "http://127.0.0.1:4317".to_string(),
            otel_service_name: "par-rt-db".to_string(),
            otel_sample_ratio: 0.05,
            multi_instance: false,
            instance_id: None,
            forward_timeout_ms: 5000,
            forward_concurrency: 64,
        }
    }
}

/// Boot-time env parse for a typed knob (ARC-118, folded with QA-106). Unset
/// ⇒ `default`; PRESENT but unparseable ⇒ an `Err` naming the variable, its
/// raw value, and the parse failure. This is the sharp fix for the failure
/// mode where a typo in e.g. `RTDB_SUBS_VERIFY_SKIP_EVERY` silently reverted
/// to the default and disabled a safety net (ARC-101) — a malformed value now
/// fails boot loudly instead. Leading/trailing whitespace is trimmed before
/// parsing so a value copied with stray spaces does not read as a typo.
///
/// Call sites apply any per-knob clamp (`max(1)`, `clamp(1, 8192)`, …) to the
/// returned `Ok` value, keeping the clamp-or-not policy explicit per knob.
fn env_parsed<T>(key: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(raw) => raw.trim().parse::<T>().map_err(|e| {
            format!(
                "{key}={raw:?} is not a valid {} ({e}); fix the value or unset {key} to use the default",
                std::any::type_name::<T>()
            )
        }),
        Err(_) => Ok(default),
    }
}

/// Boot-time env boolean (QA-106). `default` is returned when the var is
/// unset. Recognized spellings ("true"/"1"/"yes" ⇒ true; "false"/"0"/"no" ⇒
/// false, case-insensitive, trimmed) are honored; an UNRECOGNIZED value
/// resolves to `default`, so a typo cannot flip a knob away from its
/// documented posture — security flags that ship on stay on; opt-in flags
/// that ship off stay off.
fn env_bool(key: &str, default: bool) -> bool {
    let Some(v) = std::env::var(key)
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
    else {
        return default;
    };
    match v.as_str() {
        "true" | "1" | "yes" => true,
        "false" | "0" | "no" => false,
        _ => default,
    }
}

// ============================================================================
// Per-subsystem env parsers (ARC-205)
//
// `Config::from_env` composes these constructors; each subsystem's knobs —
// defaults, clamps, and the comments explaining them — live with its own
// parser, so adding a provider or knob is one parser edit rather than another
// line in the old monolith. `Config` itself stays FLAT: these are parse-time
// groupings only, destructured into `Config`'s fields at the end of
// `Config::from_env`, so no consumer of `Config` changes. Constructors that
// parse numerics via `env_parsed` return `Result<Self, String>` so a
// malformed value still fails boot naming the variable (ARC-118).
// ============================================================================

/// The five fixed-window `*_RPM` rate-limit knobs. 0 disables each limiter;
/// the two unauthenticated-route limits ship non-zero defaults (SEC-203).
struct RateLimitsEnv {
    per_token_rpm: u32,
    per_db_rpm: u32,
    storage_per_ip_rpm: u32,
    anonymous_per_ip_rpm: u32,
    admin_per_ip_rpm: u32,
    exact: bool,
    sync_ms: u64,
}

impl RateLimitsEnv {
    fn from_env() -> Result<Self, String> {
        // HTTP rate-limit ceilings: 0 = unlimited (the default), preserving
        // today's behavior.
        let per_token_rpm = env_parsed("RTDB_RATE_LIMIT_PER_TOKEN_RPM", 0u32)?;
        let per_db_rpm = env_parsed("RTDB_RATE_LIMIT_PER_DB_RPM", 0u32)?;

        // ARC-007: multi-instance-only. false (the default) picks the
        // approximate local-counter path; true keeps the pre-ARC-007 exact
        // per-request Postgres UPSERT.
        let exact = env_bool("RTDB_RATE_LIMIT_EXACT", false);
        // Approximate-limiter flush interval; clamped so a typo'd tiny value
        // can't turn the flush into a tight loop.
        let sync_ms = env_parsed("RTDB_RATE_LIMIT_SYNC_MS", 1000u64)?.max(50);

        // Per-IP rate limit on the public storage route (SEC-004). 0 = off,
        // matching the existing per-token/per-db limiter convention.
        // SEC-203: non-zero default — see the field doc on `Config`.
        let storage_per_ip_rpm = env_parsed("RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM", 300u32)?;

        // SEC-103: per-IP rate limit on `POST /auth/anonymous`. 0 = unlimited
        // (the code default; the shipped `.env.example`/`docker-compose.yml`
        // set a non-zero default so the mitigation is on out-of-the-box).
        let anonymous_per_ip_rpm = env_parsed("RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM", 10u32)?;

        // SEC-109: per-IP rate limit on `POST /admin/login`. 0 = unlimited
        // (the default), preserving today's behavior.
        let admin_per_ip_rpm = env_parsed("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", 10u32)?;

        Ok(Self {
            per_token_rpm,
            per_db_rpm,
            storage_per_ip_rpm,
            anonymous_per_ip_rpm,
            admin_per_ip_rpm,
            exact,
            sync_ms,
        })
    }
}

/// Managed pg_dump backup scheduler knobs.
struct BackupEnv {
    enabled: bool,
    cron: String,
    dir: String,
    retention: u32,
}

impl BackupEnv {
    fn from_env() -> Result<Self, String> {
        // Default off; cron/dir/retention carry their own defaults so an
        // operator can flip just RTDB_BACKUP_ENABLED=true to get daily 03:00
        // UTC dumps with 7-day retention. An empty RTDB_BACKUP_CRON falls
        // back to the default (a blank cron would surface as
        // `invalid cron expression` from `scheduler::next_fire` on every loop
        // iteration, so clamp here).
        let enabled = env_bool("RTDB_BACKUP_ENABLED", false);
        let cron = match std::env::var("RTDB_BACKUP_CRON") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "0 3 * * *".to_string(),
        };
        let dir = match std::env::var("RTDB_BACKUP_DIR") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "./backups".to_string(),
        };
        let retention = env_parsed("RTDB_BACKUP_RETENTION", 7u32)?;
        Ok(Self {
            enabled,
            cron,
            dir,
            retention,
        })
    }
}

/// Document TTL reaper knobs.
struct TtlReaperEnv {
    sweep_interval_secs: u64,
    batch: i64,
}

impl TtlReaperEnv {
    fn from_env() -> Result<Self, String> {
        // Best-effort expiry, so boot-only (not hot).
        // `tokio::time::interval` panics on a zero duration, so an explicit 0
        // would crash every db's reaper task on its first poll — clamp to 1.
        // `DELETE ... LIMIT 0` is a silent no-op (disables reaping) and a
        // negative batch errors per sweep, so clamp the batch to at least 1.
        let sweep_interval_secs = env_parsed("RTDB_TTL_SWEEP_INTERVAL_SECS", 60u64)?.max(1);
        let batch = env_parsed("RTDB_TTL_BATCH", 5000i64)?.max(1);
        Ok(Self {
            sweep_interval_secs,
            batch,
        })
    }
}

/// On-the-fly image transform knobs (ENH-014).
struct ImageTransformEnv {
    enabled: bool,
    max_dim: u32,
    max_pixels: u64,
    cache_bytes: u64,
    concurrency: usize,
    default_quality: u8,
}

impl ImageTransformEnv {
    fn from_env() -> Result<Self, String> {
        // Boot-only operational knobs; default-on master switch + bounded
        // numerics.
        let enabled = env_bool("RTDB_IMAGE_TRANSFORMS_ENABLED", true);
        let max_dim = env_parsed("RTDB_IMAGE_MAX_DIM", 2048u32)?.clamp(1, 8192);
        let max_pixels = env_parsed("RTDB_IMAGE_MAX_PIXELS", 25_000_000u64)?.max(1_000_000);
        let cache_bytes = env_parsed("RTDB_IMAGE_CACHE_BYTES", 256 * 1024 * 1024u64)?;
        let concurrency = env_parsed("RTDB_IMAGE_CONCURRENCY", 4usize)?.max(1);
        let default_quality = env_parsed("RTDB_IMAGE_DEFAULT_QUALITY", 80u8)?.clamp(1, 100);
        Ok(Self {
            enabled,
            max_dim,
            max_pixels,
            cache_bytes,
            concurrency,
            default_quality,
        })
    }
}

/// Realtime presence knobs (ENH-015 / ENH-022 Stage 3).
struct PresenceEnv {
    enabled: bool,
    max_state_bytes: usize,
    max_room_size: usize,
    max_rooms_per_conn: usize,
    max_room_bytes: usize,
    broadcast_interval_ms: u64,
    update_limit_per_sec: u32,
    max_ttl_ms: u64,
    beat_interval_ms: u64,
    beat_timeout_ms: u64,
}

impl PresenceEnv {
    fn from_env() -> Result<Self, String> {
        // Default-ON master switch + numerics; a 0 size/count/limit would be
        // unusable (clamped to ≥1), and a 0 broadcast interval means
        // "immediate" (NOT clamped).
        let enabled = env_bool("RTDB_PRESENCE_ENABLED", true);
        let max_state_bytes = env_parsed("RTDB_PRESENCE_MAX_STATE_BYTES", 1024usize)?.max(1);
        let max_room_size = env_parsed("RTDB_PRESENCE_MAX_ROOM_SIZE", 100usize)?.max(1);
        let max_rooms_per_conn = env_parsed("RTDB_PRESENCE_MAX_ROOMS_PER_CONN", 32usize)?.max(1);
        let max_room_bytes = env_parsed("RTDB_PRESENCE_MAX_ROOM_BYTES", 256usize)?.max(1);
        let broadcast_interval_ms = env_parsed("RTDB_PRESENCE_BROADCAST_INTERVAL_MS", 50u64)?;
        let update_limit_per_sec = env_parsed("RTDB_PRESENCE_UPDATE_LIMIT_PER_SEC", 20u32)?.max(1);
        let max_ttl_ms = env_parsed("RTDB_PRESENCE_MAX_TTL_MS", 300_000u64)?.max(1000);

        // ENH-022 Stage 3: cross-instance presence gossip beat + eviction
        // timeout. Floored: interval ≥ 1s (a sub-second beat would hot-spin
        // the rtdb_presence channel for no value — incremental NOTIFYs already
        // cover changes between beats); timeout ≥ interval (a timeout shorter
        // than the cadence would evict live peers every tick).
        let beat_interval_ms = env_parsed("RTDB_PRESENCE_BEAT_INTERVAL_MS", 5000u64)?.max(1000);
        let beat_timeout_ms =
            env_parsed("RTDB_PRESENCE_BEAT_TIMEOUT_MS", 15_000u64)?.max(beat_interval_ms);

        Ok(Self {
            enabled,
            max_state_bytes,
            max_room_size,
            max_rooms_per_conn,
            max_room_bytes,
            broadcast_interval_ms,
            update_limit_per_sec,
            max_ttl_ms,
            beat_interval_ms,
            beat_timeout_ms,
        })
    }
}

/// OpenTelemetry / OTLP tracing export knobs (ENH-018).
struct OtelEnv {
    enabled: bool,
    endpoint: String,
    service_name: String,
    sample_ratio: f64,
}

impl OtelEnv {
    fn from_env() -> Result<Self, String> {
        // The cargo `otel` feature gates the deps + subscriber wiring;
        // RTDB_OTEL_ENABLED is the runtime switch so a feature-compiled binary
        // still defaults to zero OTLP network calls. Endpoint/service-name
        // carry defaults so an operator only sets RTDB_OTEL_ENABLED=true +
        // RTDB_OTEL_ENDPOINT. The sample ratio is parsed through `env_parsed`
        // so a typo fails boot (ARC-118), then clamped to a valid head-sampler
        // ratio.
        let enabled = env_bool("RTDB_OTEL_ENABLED", false);
        let endpoint = match std::env::var("RTDB_OTEL_ENDPOINT") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "http://127.0.0.1:4317".to_string(),
        };
        let service_name = match std::env::var("RTDB_OTEL_SERVICE_NAME") {
            Ok(v) if !v.trim().is_empty() => v,
            _ => "par-rt-db".to_string(),
        };
        let sample_ratio = env_parsed("RTDB_OTEL_SAMPLE_RATIO", 0.05f64)?.clamp(0.0, 1.0);
        Ok(Self {
            enabled,
            endpoint,
            service_name,
            sample_ratio,
        })
    }
}

impl Config {
    /// Reads boot-only values from env. Errors (String) name the missing or
    /// invalid variable. Per ARC-118, a numeric knob that is PRESENT but
    /// UNPARSEABLE (e.g. a typo) fails boot loudly via [`env_parsed`]; only
    /// absence falls back to the documented default. Booleans use [`env_bool`]
    /// (recognized spellings; unrecognized ⇒ the documented default, so a typo
    /// cannot flip a security posture).
    pub fn from_env() -> Result<Self, String> {
        let port = env_parsed("RTDB_PORT", 8300u16)?;

        let database_url = std::env::var("RTDB_DATABASE_URL")
            .map_err(|_| "RTDB_DATABASE_URL is required".to_string())?;

        let admin_key = std::env::var("RTDB_ADMIN_KEY")
            .map_err(|_| "RTDB_ADMIN_KEY is required".to_string())?;
        // SEC-110: reject weak/placeholder admin keys at boot. An empty key
        // makes the constant-time compare trivially pass (`ct_eq(b"", b"")`),
        // authenticating anyone as admin; a short key is brute-forceable on
        // the public `/admin/login` endpoint; a placeholder copied from
        // `.env.example` / a quickstart is the most common cause of an
        // accidental admin bypass. `authenticate_admin` re-checks for empty
        // as defense-in-depth.
        validate_admin_key(&admin_key)?;

        let public_url = std::env::var("RTDB_PUBLIC_URL")
            .unwrap_or_else(|_| "http://localhost:8300".to_string());

        // OAuth providers (ARC-012/ARC-205: each provider parses in its own
        // constructor — see `config::oauth`).
        let oauth = OAuthConfig::from_env();

        let max_affected_docs = env_parsed("RTDB_MAX_AFFECTED_DOCS", 100usize)?;

        // Multi-tenant default (75): the committer-per-db model means each
        // active database can hold a connection during fan-out re-runs, so
        // the hardcoded 10 of the original code would saturate at ~11 active
        // dbs. 75 sits in the 50-100 range the audit recommends, leaving
        // headroom for concurrent subscription re-runs and HTTP reads
        // without overcommitting a typical Postgres `max_connections=100`.
        let pool_max_connections = env_parsed("RTDB_POOL_MAX_CONNECTIONS", 75u32)?;
        // ARC-119: bound the per-process schema cache. 0 = unbounded.
        let schema_cache_max_entries = env_parsed("RTDB_SCHEMA_CACHE_MAX_ENTRIES", 1024u64)?;
        // ENH-019: slow-query log threshold + capacity + params capture.
        // 0 ms disables the log entirely (the call site short-circuits before
        // re-compiling). Default capacity is 200 entries; default params-off
        // keeps document content out of the admin log.
        let slow_query_ms = env_parsed("RTDB_SLOW_QUERY_MS", 0u64)?;
        let slow_query_capacity = env_parsed("RTDB_SLOW_QUERY_CAPACITY", 200usize)?;
        let slow_query_log_params = env_bool("RTDB_SLOW_QUERY_LOG_PARAMS", false);

        let static_dir = std::env::var("RTDB_STATIC_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let rate_limits = RateLimitsEnv::from_env()?;

        // Audit log: default off (accepts "true"/"1"/"yes" to enable).
        let audit_log_enabled = env_bool("RTDB_AUDIT_LOG_ENABLED", false);

        let oauth_login_csrf = env_bool("RTDB_OAUTH_LOGIN_CSRF", true);
        if !oauth_login_csrf {
            tracing::warn!(
                "RTDB_OAUTH_LOGIN_CSRF is disabled — OAuth login-CSRF (double-submit nonce) is OFF. \
                 This is a local-dev break-glass; do NOT run production with this setting."
            );
        }

        // Webhook registry: default off.
        let webhooks_enabled = env_bool("RTDB_WEBHOOKS_ENABLED", false);
        // Webhook SSRF dev-escape hatch (SEC-001): opt-in to `http://` URLs and
        // private/loopback targets, off by default.
        let webhook_allow_http = env_bool("RTDB_WEBHOOK_ALLOW_HTTP", false);

        // SEC-113: require a valid signed URL on every public storage fetch.
        // Default false (Convex-parity: opaque public bearer URLs); operators
        // who want signed-only access flip it on.
        let storage_require_signed_urls = env_bool("RTDB_STORAGE_REQUIRE_SIGNED_URLS", false);

        let backup = BackupEnv::from_env()?;

        // Skip verification: ships ON at DEFAULT_SUBS_VERIFY_SKIP_EVERY
        // (ARC-101). A wrong skip is otherwise silent, so the verifier is the
        // runtime detection for the documented silent-failure mode. A typo in
        // the value now fails boot (ARC-118 via `env_parsed`) rather than
        // silently reverting to the default.
        let subs_verify_skip_every = env_parsed(
            "RTDB_SUBS_VERIFY_SKIP_EVERY",
            DEFAULT_SUBS_VERIFY_SKIP_EVERY,
        )?;

        let ttl = TtlReaperEnv::from_env()?;
        let image = ImageTransformEnv::from_env()?;
        let presence = PresenceEnv::from_env()?;

        // Anonymous auth master switch. Default-OFF (opt-in per app): only an
        // explicit truthy spelling ("true"/"1"/"yes") enables it; everything
        // else (incl. unset) leaves it off. `env_bool` enforces this so a typo
        // cannot enable anonymous auth.
        let auth_anonymous_enabled = env_bool("RTDB_AUTH_ANONYMOUS_ENABLED", false);

        // SEC-103: short independent TTL for anonymous sessions so the
        // ephemeral rows minted by the unauthenticated anon route expire
        // quickly rather than living for the standard 30-day TTL. Default 1.
        let anonymous_session_ttl_days = env_parsed("RTDB_ANONYMOUS_SESSION_TTL_DAYS", 1i64)?;

        // Quota counter cache TTL (ENH-011). 0 = no caching.
        let quota_cache_ttl_secs = env_parsed("RTDB_QUOTA_CACHE_TTL_SECS", 60u64)?;

        // ARC-102 step 4: idle-database reclamation threshold. 0 = disabled
        // (default); a non-zero value retires a db's per-db tasks once it has
        // been client-idle this long with no live subs and no pending jobs.
        let db_idle_reclaim_secs = env_parsed("RTDB_DB_IDLE_RECLAIM_SECS", 0u64)?;

        // SEC-120: cookie `Secure` attribute ships ON by default. An explicit
        // "false"/"0"/"no" (case-insensitive) is the local-http-dev escape
        // hatch (the browser rejects Secure cookies over plain http); anything
        // else stays on so production never accidentally sends a session
        // cookie in the clear when a proxy strips `X-Forwarded-Proto`.
        let cookie_secure = env_bool("RTDB_COOKIE_SECURE", true);

        // SEC-201: only trust forwarding headers (CF-Connecting-IP,
        // X-Forwarded-For, X-Forwarded-Proto) when the deploy actually sits
        // behind such a proxy. Default false: on a directly reachable port
        // those headers are caller-controlled, and trusting them lets an
        // attacker rotate per-IP rate-limit buckets (or flag cookie Secure /
        // HSTS) with arbitrary header values.
        let trusted_proxy = env_bool("RTDB_TRUSTED_PROXY", false);

        let otel = OtelEnv::from_env()?;

        // ENH-022 Stage 2: cross-instance op-feed fan-out. Off by default — a
        // single-instance deploy is the supported topology. `instance_id` is
        // optional; when unset (or empty), `AppState::new` generates one.
        let multi_instance = env_bool("RTDB_MULTI_INSTANCE", false);
        let instance_id = std::env::var("RTDB_INSTANCE_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        // ENH-022 Stage 4c: forwarded-write reply deadline before the
        // non-owner attempts the lease takeover. Clamped to a floor of 100ms
        // so a typo cannot make every forward fall straight through to
        // takeover (which would ping-pong the lease under a load balancer).
        let forward_timeout_ms = env_parsed("RTDB_FORWARD_TIMEOUT_MS", 5_000u64)?.max(100);
        // ARC-008: bound `run_forward_listener`'s concurrent forwarded-write
        // executions so a burst of forwarded writes cannot spawn an unbounded
        // number of tasks against one owner's committer.
        let forward_concurrency = env_parsed("RTDB_FORWARD_CONCURRENCY", 64usize)?.max(1);

        Ok(Self {
            port,
            database_url,
            admin_key,
            public_url,
            oauth,
            max_affected_docs,
            static_dir,
            pool_max_connections,
            schema_cache_max_entries,
            slow_query_ms,
            slow_query_capacity,
            slow_query_log_params,
            rate_limit_per_token_rpm: rate_limits.per_token_rpm,
            rate_limit_per_db_rpm: rate_limits.per_db_rpm,
            rate_limit_exact: rate_limits.exact,
            rate_limit_sync_ms: rate_limits.sync_ms,
            audit_log_enabled,
            oauth_login_csrf,
            webhooks_enabled,
            webhook_allow_http,
            storage_rate_limit_per_ip_rpm: rate_limits.storage_per_ip_rpm,
            storage_require_signed_urls,
            backup_enabled: backup.enabled,
            backup_cron: backup.cron,
            backup_dir: backup.dir,
            backup_retention: backup.retention,
            subs_verify_skip_every,
            ttl_sweep_interval_secs: ttl.sweep_interval_secs,
            ttl_batch: ttl.batch,
            image_transforms_enabled: image.enabled,
            image_max_dim: image.max_dim,
            image_max_pixels: image.max_pixels,
            image_cache_bytes: image.cache_bytes,
            image_concurrency: image.concurrency,
            image_default_quality: image.default_quality,
            presence_enabled: presence.enabled,
            presence_max_state_bytes: presence.max_state_bytes,
            presence_max_room_size: presence.max_room_size,
            presence_max_rooms_per_conn: presence.max_rooms_per_conn,
            presence_max_room_bytes: presence.max_room_bytes,
            presence_broadcast_interval_ms: presence.broadcast_interval_ms,
            presence_update_limit_per_sec: presence.update_limit_per_sec,
            presence_max_ttl_ms: presence.max_ttl_ms,
            presence_beat_interval_ms: presence.beat_interval_ms,
            presence_beat_timeout_ms: presence.beat_timeout_ms,
            auth_anonymous_enabled,
            anonymous_session_ttl_days,
            anonymous_rate_limit_per_ip_rpm: rate_limits.anonymous_per_ip_rpm,
            quota_cache_ttl_secs,
            db_idle_reclaim_secs,
            admin_rate_limit_per_ip_rpm: rate_limits.admin_per_ip_rpm,
            cookie_secure,
            trusted_proxy,
            otel_enabled: otel.enabled,
            otel_endpoint: otel.endpoint,
            otel_service_name: otel.service_name,
            otel_sample_ratio: otel.sample_ratio,
            multi_instance,
            forward_timeout_ms,
            forward_concurrency,
            instance_id,
        })
    }
}

/// SEC-110: minimum-strength gate for the configured admin key, enforced at
/// boot. Rejects:
/// - empty or whitespace-only (`ct_eq(b"", b"")` would authenticate anyone);
/// - shorter than [`MIN_ADMIN_KEY_LEN`] (brute-forceable over `/admin/login`);
/// - obvious placeholders copied verbatim from `.env.example` / quickstarts.
///
/// The min-length floor is the real strength gate; the placeholder list is a
/// focused denylist of the most common copy-paste offenders (case-insensitive
/// exact match, so a strong random key that happens to contain "admin" as a
/// substring is NOT rejected). An operator who needs a weaker key for a local
/// dev harness should set a 16+ char string — the floor is deliberately low.
pub(crate) fn validate_admin_key(key: &str) -> Result<(), String> {
    let trimmed = key.trim();
    if trimmed.is_empty() {
        return Err(
            "RTDB_ADMIN_KEY must not be empty or whitespace (it would authenticate anyone as admin)"
                .to_string(),
        );
    }
    if trimmed.len() < MIN_ADMIN_KEY_LEN {
        return Err(format!(
            "RTDB_ADMIN_KEY must be at least {MIN_ADMIN_KEY_LEN} characters (got {}); \
             configure a strong random key (e.g. 64 hex chars)",
            trimmed.len()
        ));
    }
    let lower = trimmed.to_ascii_lowercase();
    // Exact-match placeholders only (not substring) so a strong random key
    // that happens to contain one of these words is not falsely rejected.
    const PLACEHOLDERS: &[&str] = &[
        "changeme",
        "changeme-random-hex",
        "password",
        "secret",
        "admin",
        "admin-key",
        "your-admin-key",
        "your-secret-key",
        "replace-me",
        "replace-me-with-a-real-key",
        "test-key",
        "testadmin",
    ];
    if PLACEHOLDERS.iter().any(|p| lower == *p) {
        return Err(format!(
            "RTDB_ADMIN_KEY is set to an obvious placeholder ({trimmed:?}); \
             set a real random key"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ARC-101: the subscription-skip shadow verifier ships ON by default so
    /// `rtdb_subs_missed_pushes_total` is reachable in every deploy (including
    /// production). A non-zero, non-trivial default is the whole fix; if this
    /// regresses to 0 the metric becomes structurally incapable of moving.
    #[test]
    fn subs_verify_skip_default_ships_on_at_a_sampling_interval() {
        assert_eq!(
            DEFAULT_SUBS_VERIFY_SKIP_EVERY, 1000,
            "ARC-101: the shadow verifier must ship ON (non-zero default)"
        );
    }

    /// `RTDB_PRESENCE_*` boot knobs: defaults when unset, env overrides honored.
    /// `from_env` requires `RTDB_DATABASE_URL` and `RTDB_ADMIN_KEY`, so this test
    /// sets them for its lifetime and restores them at the end. All `RTDB_PRESENCE_*`
    /// vars are removed at the end so they don't leak into other lib tests.
    ///
    /// `#[serial]`: this and `otel_env_defaults_and_overrides` both mutate
    /// process-global env and call `Config::from_env()`, so they must not run
    /// concurrently (env mutation is not thread-safe across tests; a left-behind
    /// `RTDB_OTEL_SAMPLE_RATIO`/`RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM` breaks the
    /// other's `from_env`). Same fix the webhook test suite took (b97bfb4).
    #[test]
    #[serial_test::serial]
    fn presence_env_defaults_and_overrides() {
        // Rust 2024: env mutation is `unsafe` (not thread-safe in general). Within
        // a single test binary where no other test reads these vars, this is sound.
        unsafe {
            // Seed the two required vars (save originals to restore at the end).
            let saved_db = std::env::var("RTDB_DATABASE_URL").ok();
            let saved_key = std::env::var("RTDB_ADMIN_KEY").ok();
            let saved_admin_limit = std::env::var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM").ok();
            let saved_max = std::env::var("RTDB_MAX_AFFECTED_DOCS").ok();
            std::env::set_var("RTDB_DATABASE_URL", "postgres://test");
            // SEC-110: the boot validator (validate_admin_key) rejects keys
            // shorter than 16 chars, so use a key that clears the floor.
            std::env::set_var("RTDB_ADMIN_KEY", "test-admin-key-0123");

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

            // SEC-109/SEC-203: RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM defaults to
            // 10 (0 disables) and parses an explicit override. ARC-118
            // (folded with QA-106): a present-but-unparseable value now fails
            // boot loudly via `env_parsed` instead of silently reverting to
            // the default — the silent-default-on-typo failure mode that
            // could disable a safety net (e.g. RTDB_SUBS_VERIFY_SKIP_EVERY,
            // ARC-101).
            std::env::remove_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM");
            let c = Config::from_env().expect("from_env with required vars set");
            assert_eq!(c.admin_rate_limit_per_ip_rpm, 10, "default is 10 (SEC-203)");
            std::env::set_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", "0");
            let c = Config::from_env().expect("from_env with required vars set");
            assert_eq!(c.admin_rate_limit_per_ip_rpm, 0, "explicit 0 disables");
            std::env::set_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", "25");
            let c = Config::from_env().expect("from_env with required vars set");
            assert_eq!(c.admin_rate_limit_per_ip_rpm, 25);
            std::env::set_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", "not-a-number");
            let err = Config::from_env()
                .expect_err("ARC-118: malformed numeric must fail boot, not default");
            assert!(
                err.contains("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM"),
                "error names the var: {err}"
            );
            // Clear the malformed value so it doesn't leak into the next block.
            std::env::remove_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM");

            // ARC-118: same contract for a different numeric knob. A malformed
            // RTDB_MAX_AFFECTED_DOCS (a stand-in for every numeric knob now
            // routed through env_parsed, including the ARC-101 safety net
            // RTDB_SUBS_VERIFY_SKIP_EVERY) must fail boot naming the var and the
            // bad value, instead of silently reverting to the default — the
            // failure mode that could disable a safety net on a typo.
            std::env::set_var("RTDB_MAX_AFFECTED_DOCS", "abc");
            let err = Config::from_env()
                .expect_err("ARC-118: malformed numeric must fail boot, not default");
            assert!(
                err.contains("RTDB_MAX_AFFECTED_DOCS"),
                "error names the var: {err}"
            );
            assert!(err.contains("abc"), "error names the bad value: {err}");
            // Clear so it doesn't leak into the SEC-110 block below.
            std::env::remove_var("RTDB_MAX_AFFECTED_DOCS");

            // SEC-110: from_env rejects empty/short admin keys at boot.
            std::env::set_var("RTDB_ADMIN_KEY", "");
            let err = Config::from_env().expect_err("empty admin key must fail boot");
            assert!(err.contains("RTDB_ADMIN_KEY"), "error names the var: {err}");
            std::env::set_var("RTDB_ADMIN_KEY", "short");
            let err = Config::from_env().expect_err("short admin key must fail boot");
            assert!(err.contains("RTDB_ADMIN_KEY"), "error names the var: {err}");
            // Restore the valid key before cleanup so the next test's save/restore
            // sees a valid state.
            std::env::set_var("RTDB_ADMIN_KEY", "test-admin-key-0123");

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
            match saved_admin_limit {
                Some(v) => std::env::set_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", v),
                None => std::env::remove_var("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM"),
            }
            match saved_max {
                Some(v) => std::env::set_var("RTDB_MAX_AFFECTED_DOCS", v),
                None => std::env::remove_var("RTDB_MAX_AFFECTED_DOCS"),
            }
        }
    }

    /// SEC-110: a strong random key clears the validator.
    #[test]
    fn validate_admin_key_accepts_strong_key() {
        assert!(validate_admin_key("a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4").is_ok());
        // 16 chars exactly (the floor) is accepted.
        assert!(validate_admin_key("abcdef0123456789").is_ok());
    }

    /// SEC-110: empty / whitespace-only keys are rejected (they would make
    /// `ct_eq(b"", b"")` authenticate anyone as admin).
    #[test]
    fn validate_admin_key_rejects_empty() {
        assert!(validate_admin_key("").is_err());
        assert!(validate_admin_key("   ").is_err());
        assert!(validate_admin_key("\t\n").is_err());
    }

    /// SEC-110: keys shorter than 16 chars are rejected (brute-forceable).
    #[test]
    fn validate_admin_key_rejects_short() {
        assert!(validate_admin_key("short").is_err());
        assert!(validate_admin_key("1234567890abcde").is_err()); // 15 chars
    }

    /// SEC-110: obvious placeholders copied from templates are rejected.
    #[test]
    fn validate_admin_key_rejects_placeholders() {
        for placeholder in [
            "changeme",
            "changeme-random-hex",
            "password",
            "secret",
            "admin",
            "admin-key",
            "your-admin-key",
            "replace-me",
            "test-key",
        ] {
            assert!(
                validate_admin_key(placeholder).is_err(),
                "should reject placeholder: {placeholder}"
            );
            // Case-insensitive.
            assert!(
                validate_admin_key(&placeholder.to_uppercase()).is_err(),
                "should reject placeholder (uppercase): {placeholder}"
            );
        }
    }

    /// SEC-110: a strong key that merely CONTAINS a placeholder word as a
    /// substring is NOT rejected — the denylist is exact-match only, so real
    /// random keys are not falsely blocked.
    #[test]
    fn validate_admin_key_substring_is_not_rejected() {
        // Contains "admin" but is long and not an exact match.
        assert!(validate_admin_key("my-admin-key-is-strong-0123").is_ok());
        // Contains "secret" but is long and not an exact match.
        assert!(validate_admin_key("a1b2c3secretd4e5f6a1b2").is_ok());
    }

    /// ENH-018: the four `RTDB_OTEL_*` boot knobs — defaults when unset, env
    /// overrides honored, and a malformed sample ratio fails boot (ARC-118).
    /// The runtime master switch ships OFF so a feature-compiled binary still
    /// makes zero OTLP calls unless an operator opts in. Mirrors the
    /// `presence_env_defaults_and_overrides` env-mutation pattern.
    #[test]
    #[serial_test::serial]
    fn otel_env_defaults_and_overrides() {
        unsafe {
            let saved_db = std::env::var("RTDB_DATABASE_URL").ok();
            let saved_key = std::env::var("RTDB_ADMIN_KEY").ok();
            std::env::set_var("RTDB_DATABASE_URL", "postgres://test");
            std::env::set_var("RTDB_ADMIN_KEY", "test-admin-key-0123");

            // Defaults (vars unset): off, standard OTLP/gRPC endpoint + service
            // name, 5% head sampling.
            for v in [
                "RTDB_OTEL_ENABLED",
                "RTDB_OTEL_ENDPOINT",
                "RTDB_OTEL_SERVICE_NAME",
                "RTDB_OTEL_SAMPLE_RATIO",
            ] {
                std::env::remove_var(v);
            }
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(!c.otel_enabled, "default off — zero OTLP calls");
            assert_eq!(c.otel_endpoint, "http://127.0.0.1:4317");
            assert_eq!(c.otel_service_name, "par-rt-db");
            assert!((c.otel_sample_ratio - 0.05).abs() < f64::EPSILON);

            // Overrides: the switch accepts "true"; strings parse; ratio parses.
            std::env::set_var("RTDB_OTEL_ENABLED", "true");
            std::env::set_var("RTDB_OTEL_ENDPOINT", "http://collector:4317");
            std::env::set_var("RTDB_OTEL_SERVICE_NAME", "rtdb-prod");
            std::env::set_var("RTDB_OTEL_SAMPLE_RATIO", "0.5");
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(c.otel_enabled);
            assert_eq!(c.otel_endpoint, "http://collector:4317");
            assert_eq!(c.otel_service_name, "rtdb-prod");
            assert!((c.otel_sample_ratio - 0.5).abs() < f64::EPSILON);

            // Default-on typo contract does NOT apply to otel_enabled (it's an
            // opt-in, default-off knob), so an unrecognized spelling stays off —
            // a typo cannot enable tracing unexpectedly.
            std::env::set_var("RTDB_OTEL_ENABLED", "yes-please");
            let c = Config::from_env().expect("from_env with required vars set");
            assert!(!c.otel_enabled, "unrecognized spelling stays default-off");

            // ARC-118: a present-but-unparseable ratio must fail boot naming the
            // var, not silently revert to the default.
            std::env::set_var("RTDB_OTEL_SAMPLE_RATIO", "not-a-number");
            let err = Config::from_env()
                .expect_err("ARC-118: malformed ratio must fail boot, not default");
            assert!(
                err.contains("RTDB_OTEL_SAMPLE_RATIO"),
                "error names the var: {err}"
            );
            // Clear the malformed value immediately so it cannot leak into a
            // parallel test that re-enters from_env (env mutation is global to
            // the binary; a left-behind malformed value breaks every other
            // test's from_env call). Mirrors the presence test's discipline.
            std::env::remove_var("RTDB_OTEL_SAMPLE_RATIO");

            // A ratio above 1.0 clamps to 1.0 (full sampling); below 0.0 to 0.0.
            std::env::set_var("RTDB_OTEL_SAMPLE_RATIO", "2.0");
            let c = Config::from_env().expect("clamp, not error");
            assert!((c.otel_sample_ratio - 1.0).abs() < f64::EPSILON);
            std::env::set_var("RTDB_OTEL_SAMPLE_RATIO", "-0.5");
            let c = Config::from_env().expect("clamp, not error");
            assert!(c.otel_sample_ratio.abs() < f64::EPSILON);

            // Cleanup so nothing leaks into other lib tests.
            for v in [
                "RTDB_OTEL_ENABLED",
                "RTDB_OTEL_ENDPOINT",
                "RTDB_OTEL_SERVICE_NAME",
                "RTDB_OTEL_SAMPLE_RATIO",
            ] {
                std::env::remove_var(v);
            }
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
