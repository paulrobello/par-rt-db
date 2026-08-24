//! Runtime-mutable, hot-reloadable configuration (`HotConfig`), split out of
//! `config/mod.rs` (ARC-012). Everything an admin can change via
//! `PATCH /admin/config` without a restart lives here: the persisted-row
//! decode (`PersistedHotConfig`), the origin validator the CORS layer and the
//! OAuth `begin` allowlist both depend on, and the `load_hot`/`save_hot`
//! Postgres round trip. Re-exported from `config::mod` so every existing
//! `crate::config::HotConfig` / `crate::config::HARD_MAX_FILE_SIZE` reference
//! is unaffected.

use serde::{Deserialize, Serialize};

use crate::error::RtDbError;

/// Hard upper bound on `HotConfig::max_file_size`, enforced at boot seed
/// (`HotConfig::from_env`) and at the upload streaming point
/// (`http_api::upload_handler`). The hot-config value is admin-mutable via
/// `PATCH /admin/config`, so without a compile-time ceiling an admin
/// misconfiguration (or a compromised admin token) could accept arbitrarily
/// large uploads. 2 GiB is the ENH-021 post-streaming ceiling: with the upload
/// path now chunked (1 MiB at a time), memory is decoupled from file size, so
/// this is a disk-quota / DoS guard rather than a memory-safety guard.
/// `RTDB_MAX_FILE_SIZE` (default 50 MiB) is the policy limit an operator tunes
/// per deployment; this const is the hard cap a compromised admin token cannot
/// raise (SEC-008). Raising this is a code change, not a config knob.
pub(crate) const HARD_MAX_FILE_SIZE: usize = 2 * 1024 * 1024 * 1024;

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
    /// Per-database resource quotas (ENH-011). 0 = unlimited (quota disabled).
    pub max_tables_per_db: usize, // RTDB_MAX_TABLES_PER_DB,       default 0
    pub max_storage_bytes_per_db: u64, // RTDB_MAX_STORAGE_BYTES_PER_DB, default 0
    pub max_subs_per_db: usize,  // RTDB_MAX_SUBS_PER_DB,        default 0
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
        // Per-database resource quotas (ENH-011). 0 = unlimited. An unparseable
        // value falls back to 0 (quota disabled), matching the permissiveness of
        // the parses above while failing open on the operator side.
        let max_tables_per_db = match std::env::var("RTDB_MAX_TABLES_PER_DB") {
            Ok(v) => v.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };
        let max_storage_bytes_per_db = match std::env::var("RTDB_MAX_STORAGE_BYTES_PER_DB") {
            Ok(v) => v.parse::<u64>().unwrap_or(0),
            Err(_) => 0,
        };
        let max_subs_per_db = match std::env::var("RTDB_MAX_SUBS_PER_DB") {
            Ok(v) => v.parse::<usize>().unwrap_or(0),
            Err(_) => 0,
        };
        Self {
            allowed_origins,
            session_ttl_days,
            max_file_size,
            idempotency_ttl_ms,
            max_tables_per_db,
            max_storage_bytes_per_db,
            max_subs_per_db,
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
    #[serde(default)]
    max_tables_per_db: Option<usize>,
    #[serde(default)]
    max_storage_bytes_per_db: Option<u64>,
    #[serde(default)]
    max_subs_per_db: Option<usize>,
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
            max_tables_per_db: self.max_tables_per_db.unwrap_or(defaults.max_tables_per_db),
            max_storage_bytes_per_db: self
                .max_storage_bytes_per_db
                .unwrap_or(defaults.max_storage_bytes_per_db),
            max_subs_per_db: self.max_subs_per_db.unwrap_or(defaults.max_subs_per_db),
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
            .map_err(|e| {
                tracing::error!(error = %e, "load rtdb_config");
                RtDbError::internal("failed to load rtdb_config; see server logs")
            })?;
    match row {
        Some((v,)) => serde_json::from_value::<PersistedHotConfig>(v)
            .map(|persisted| Some(persisted.merge_onto(defaults.clone())))
            .map_err(|e| {
                tracing::error!(error = %e, "decode rtdb_config");
                RtDbError::internal("failed to decode rtdb_config; see server logs")
            }),
        None => Ok(None),
    }
}

/// Upserts the single hot row.
pub async fn save_hot(pool: &sqlx::PgPool, hot: &HotConfig) -> Result<(), RtDbError> {
    let v = serde_json::to_value(hot).map_err(|e| {
        tracing::error!(error = %e, "encode rtdb_config");
        RtDbError::internal("failed to encode rtdb_config; see server logs")
    })?;
    sqlx::query(
        "INSERT INTO rtdb_config (id, hot) VALUES (1, $1) \
         ON CONFLICT (id) DO UPDATE SET hot = EXCLUDED.hot",
    )
    .bind(v)
    .execute(pool)
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "save rtdb_config");
        RtDbError::internal("failed to save rtdb_config; see server logs")
    })?;
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
            max_tables_per_db: 0,
            max_storage_bytes_per_db: 0,
            max_subs_per_db: 0,
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
                "https://projects.example.com",
                "https://hack.example.com",
                "https://rtdb.example.com"
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
                .contains(&"https://hack.example.com".to_string())
        );
        assert_eq!(merged.max_file_size, 52428800);
        assert_eq!(merged.session_ttl_days, 30);
        // The one absent field falls back to the env seed...
        assert_eq!(merged.idempotency_ttl_ms, 300_000);
        // The absent quota fields (ENH-011) fall back to the env seed (0 =
        // unlimited), not to a structural default that would silently cap.
        assert_eq!(merged.max_tables_per_db, 0);
        assert_eq!(merged.max_storage_bytes_per_db, 0);
        assert_eq!(merged.max_subs_per_db, 0);
    }

    /// `HotConfig` quota fields (ENH-011) round-trip through the persisted
    /// jsonb row: a row carrying `maxTablesPerDb` / `maxStorageBytesPerDb` /
    /// `maxSubsPerDb` decodes and merges so the persisted values win over the
    /// env seed. Regression guard for the prod incident class documented on
    /// `PersistedHotConfig` (a field added to `HotConfig` without a mirror in
    /// `PersistedHotConfig` + `merge_onto` silently reverts to env on every boot).
    #[test]
    fn persisted_quota_fields_round_trip() {
        let row = serde_json::json!({
            "maxTablesPerDb": 25,
            "maxStorageBytesPerDb": 536870912,
            "maxSubsPerDb": 100,
            "sessionTtlDays": 30
        });
        let persisted: PersistedHotConfig = serde_json::from_value(row).expect("quota row decodes");
        let merged = persisted.merge_onto(env_seed());
        assert_eq!(merged.max_tables_per_db, 25);
        assert_eq!(merged.max_storage_bytes_per_db, 536870912);
        assert_eq!(merged.max_subs_per_db, 100);
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
            max_tables_per_db: 0,
            max_storage_bytes_per_db: 0,
            max_subs_per_db: 0,
        };
        assert!(hot.origins_valid());
        hot.allowed_origins.push("https://c.com\"".into());
        assert!(!hot.origins_valid());
    }
}
