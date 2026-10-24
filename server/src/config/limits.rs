//! Boot-pinned rate-limit guardrails, nested under `Config::limits`.
//! The per-IP abuse guards and ARC-007 multi-instance limiter mode live here.
//! Per-token and per-db capacity ceilings live on `HotConfig` instead.

use super::{env_bool, env_parsed};

/// Boot-pinned per-IP abuse guards plus the ARC-007 multi-instance limiter
/// mode. 0 disables each guard; the three unauthenticated-route limits ship
/// non-zero defaults (SEC-203).
#[derive(Clone, Debug)]
pub struct LimitsConfig {
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
    /// RTDB_RATE_LIMIT_OAUTH_BEGIN_PER_IP_RPM (default 0 = disabled; the
    /// shipped `.env.example`/`docker-compose.yml` set 30 — SEC-003). Per-IP
    /// rate limit on the unauthenticated `GET /auth/{provider}/begin` route.
    pub oauth_begin_per_ip_rpm: u32,
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
    /// RTDB_RATE_LIMIT_EXEMPT_DBS (default empty; comma-separated db names).
    /// Every request against a listed db skips the per-token and per-db
    /// capacity ceilings entirely; the WS per-connection frame cap and the
    /// per-IP abuse guards below never consult this. Boot-pinned on purpose:
    /// the ceilings themselves are hot config, but naming a trusted db is a
    /// deployment-level decision, so changing the list takes a restart.
    pub rate_limit_exempt_dbs: Vec<String>,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            storage_per_ip_rpm: 300,
            anonymous_per_ip_rpm: 10,
            admin_per_ip_rpm: 10,
            oauth_begin_per_ip_rpm: 0,
            exact: false,
            sync_ms: 1000,
            rate_limit_exempt_dbs: Vec::new(),
        }
    }
}

impl LimitsConfig {
    pub(super) fn from_env() -> Result<Self, String> {
        // ARC-007: multi-instance-only. false (the default) picks the
        // approximate local-counter path; true keeps the pre-ARC-007 exact
        // per-request Postgres UPSERT.
        let exact = env_bool("RTDB_RATE_LIMIT_EXACT", false);
        // Approximate-limiter flush interval; clamped so a typo'd tiny value
        // can't turn the flush into a tight loop.
        let sync_ms = env_parsed("RTDB_RATE_LIMIT_SYNC_MS", 1000u64)?.max(50);

        // Boot-pinned capacity exemption (see the field doc). Parsed via
        // `parse_db_list`, which is unit-tested without mutating env.
        let rate_limit_exempt_dbs =
            parse_db_list(&std::env::var("RTDB_RATE_LIMIT_EXEMPT_DBS").unwrap_or_default());

        // Per-IP rate limit on the public storage route (SEC-004). 0 = off,
        // matching the existing per-token/per-db limiter convention.
        // SEC-203: non-zero default — see the field doc above.
        let storage_per_ip_rpm = env_parsed("RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM", 300u32)?;

        // SEC-103: per-IP rate limit on `POST /auth/anonymous`. The code
        // default is 10; 0 disables. The shipped `.env.example`/
        // `docker-compose.yml` also set 10, so the mitigation is on
        // out-of-the-box.
        let anonymous_per_ip_rpm = env_parsed("RTDB_ANONYMOUS_RATE_LIMIT_PER_IP_RPM", 10u32)?;

        // SEC-109: per-IP rate limit on `POST /admin/login`. The code default
        // is 10; 0 disables.
        let admin_per_ip_rpm = env_parsed("RTDB_ADMIN_RATE_LIMIT_PER_IP_RPM", 10u32)?;

        // SEC-003: per-IP rate limit on `GET /auth/{provider}/begin`. 0 =
        // disabled (the code default; the shipped `.env.example`/
        // `docker-compose.yml` set a non-zero default so the mitigation is
        // on out-of-the-box).
        let oauth_begin_per_ip_rpm = env_parsed("RTDB_RATE_LIMIT_OAUTH_BEGIN_PER_IP_RPM", 0u32)?;

        Ok(Self {
            storage_per_ip_rpm,
            anonymous_per_ip_rpm,
            admin_per_ip_rpm,
            oauth_begin_per_ip_rpm,
            exact,
            sync_ms,
            rate_limit_exempt_dbs,
        })
    }
}

/// Splits a comma-separated db list (`RTDB_RATE_LIMIT_EXEMPT_DBS`) into
/// trimmed, non-empty entries. Factored out of `from_env` so the split rules
/// are unit-testable without mutating process-global env.
fn parse_db_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_db_list_trims_and_drops_empties() {
        assert!(parse_db_list("").is_empty());
        assert!(parse_db_list("   ").is_empty());
        assert_eq!(parse_db_list("the-cube"), vec!["the-cube".to_string()]);
        assert_eq!(
            parse_db_list(" the-cube , other-db ,, x"),
            vec![
                "the-cube".to_string(),
                "other-db".to_string(),
                "x".to_string()
            ]
        );
    }
}
