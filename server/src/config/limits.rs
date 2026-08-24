//! HTTP rate-limit knobs, nested under `Config::limits` (ARC-012/ARC-205).
//! The five fixed-window `*_RPM` limits plus the two ARC-007 multi-instance
//! Postgres-backed-limiter knobs (`exact` / `sync_ms`) all live here — see
//! each field's doc for the SEC-xxx rationale.

use super::{env_bool, env_parsed};

/// The five fixed-window `*_RPM` rate-limit knobs, plus the ARC-007
/// multi-instance limiter mode. 0 disables each limiter; the three
/// unauthenticated-route limits ship non-zero defaults (SEC-203).
#[derive(Clone, Debug)]
pub struct LimitsConfig {
    /// RTDB_RATE_LIMIT_PER_TOKEN_RPM (default 0 = unlimited). Caps each
    /// machine token; OAuth sessions carry no token id and are rate-limited
    /// per-db only.
    pub per_token_rpm: u32,
    /// RTDB_RATE_LIMIT_PER_DB_RPM (default 0 = unlimited), shared across all
    /// principals of one db.
    pub per_db_rpm: u32,
    /// RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM (default 300 — SEC-004/SEC-203).
    /// Per-IP rate limit on the unauthenticated `GET /storage/{id}` route. 0
    /// disables.
    pub storage_per_ip_rpm: u32,
    /// RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM (default 10; 0 disables —
    /// SEC-103/SEC-203). Per-IP rate limit on `POST /auth/anonymous`.
    pub anonymous_per_ip_rpm: u32,
    /// RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM (default 10; 0 disables —
    /// SEC-109/SEC-203). Per-IP rate limit on `POST /admin/login`.
    pub admin_per_ip_rpm: u32,
    /// RTDB_RATE_LIMIT_EXACT (default false). ARC-007: multi-instance-only.
    /// false = each replica counts locally and reconciles with
    /// `rtdb_auth.rate_counters` every `sync_ms` (approximate, no
    /// per-request Postgres round trip); overshoot is bounded by roughly
    /// (replica count × one sync window) of extra allowance. true = every
    /// checked request pays a synchronous UPSERT for an exact shared ceiling
    /// (the pre-ARC-007 behavior). Both are inert in single-instance mode.
    pub exact: bool,
    /// RTDB_RATE_LIMIT_SYNC_MS (default 1000, clamped to >= 50). How often
    /// the approximate limiter (`exact == false`) flushes local deltas into
    /// Postgres and refreshes its shared-count view. Unused when `exact` is
    /// true.
    pub sync_ms: u64,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            per_token_rpm: 0,
            per_db_rpm: 0,
            storage_per_ip_rpm: 300,
            anonymous_per_ip_rpm: 10,
            admin_per_ip_rpm: 10,
            exact: false,
            sync_ms: 1000,
        }
    }
}

impl LimitsConfig {
    pub(super) fn from_env() -> Result<Self, String> {
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
        // SEC-203: non-zero default — see the field doc above.
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
