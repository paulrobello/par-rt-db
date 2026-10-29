//! Storage-serve knobs, nested under `Config::storage` (ARC-012): the
//! signed-URL requirement plus on-the-fly image transforms (ENH-014), which
//! run on the same `GET /storage/{id}` serve path.

use std::collections::BTreeSet;

use super::{env_bool, env_parsed};

/// Which databases require a signed URL on the public storage serve route
/// (SEC-113). Parsed from `RTDB_STORAGE_REQUIRE_SIGNED_URLS`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SignedUrlsScope {
    /// No database requires signed URLs (opaque public bearer URLs).
    #[default]
    Off,
    /// Every database requires signed URLs.
    All,
    /// Only the listed (lowercased) databases require signed URLs.
    Databases(BTreeSet<String>),
}

impl SignedUrlsScope {
    /// Parses `true`/`1`/`yes`, `false`/`0`/`no`/unset, or a comma-separated
    /// database list. Boolean spellings win over a database of the same name.
    /// Malformed input is an error: a typo must not silently leave a database
    /// public.
    pub(super) fn parse(raw: &str) -> Result<Self, String> {
        let v = raw.trim().to_ascii_lowercase();
        match v.as_str() {
            "true" | "1" | "yes" => return Ok(Self::All),
            "" | "false" | "0" | "no" => return Ok(Self::Off),
            _ => {}
        }
        let mut dbs = BTreeSet::new();
        for entry in v.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            crate::db::validate_db_name(entry).map_err(|_| {
                format!(
                    "RTDB_STORAGE_REQUIRE_SIGNED_URLS={raw:?}: expected true/false or a \
                     comma-separated list of database names, got invalid entry {entry:?}"
                )
            })?;
            dbs.insert(entry.to_string());
        }
        Ok(if dbs.is_empty() {
            Self::Off
        } else {
            Self::Databases(dbs)
        })
    }

    /// Stable name for the admin read-back: `off`, `all`, or `databases`.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::All => "all",
            Self::Databases(_) => "databases",
        }
    }

    /// The listed databases (empty unless `Databases`).
    pub fn databases(&self) -> Vec<String> {
        match self {
            Self::Databases(d) => d.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }
}

/// On-the-fly image transform knobs (ENH-014). Boot-time operational knobs
/// (not admin-mutable). All optional w/ defaults.
#[derive(Clone, Debug)]
pub struct ImageTransformConfig {
    pub enabled: bool,       // RTDB_IMAGE_TRANSFORMS_ENABLED, default true
    pub max_dim: u32,        // RTDB_IMAGE_MAX_DIM, default 2048
    pub max_pixels: u64,     // RTDB_IMAGE_MAX_PIXELS, default 25_000_000
    pub cache_bytes: u64,    // RTDB_IMAGE_CACHE_BYTES, default 256 MiB
    pub concurrency: usize,  // RTDB_IMAGE_CONCURRENCY, default 4
    pub default_quality: u8, // RTDB_IMAGE_DEFAULT_QUALITY, default 80
}

impl Default for ImageTransformConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_dim: 2048,
            max_pixels: 25_000_000,
            cache_bytes: 256 * 1024 * 1024,
            concurrency: 4,
            default_quality: 80,
        }
    }
}

impl ImageTransformConfig {
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

/// Storage-serve knobs: the signed-URL requirement plus on-the-fly image
/// transforms.
#[derive(Clone, Debug, Default)]
pub struct StorageConfig {
    /// RTDB_STORAGE_REQUIRE_SIGNED_URLS (default false). SEC-113: for the
    /// in-scope databases the public storage serve route (`GET /storage/{id}`)
    /// requires a valid `?exp=&sig=` pair on every request — a holder of the
    /// opaque id alone is no longer enough. `true` scopes every database, a
    /// comma-separated list scopes only those. Default off so existing public
    /// bearer URLs (a deliberate Convex-parity feature) keep working. The
    /// mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) is unaffected
    /// and remains the way to mint time-limited URLs under either mode.
    pub require_signed_urls: SignedUrlsScope,
    /// RTDB_SHARE_GRANTS_ENABLED (default false). Per-row public share grants:
    /// when true, a db-authorized caller can mint an unguessable single-row
    /// share token (`POST /api/db/{db}/share-grants`) and an UNAUTHENTICATED
    /// holder of that token can read EXACTLY the granted row
    /// (`GET /share/{token}`) or mint a signed URL for EXACTLY the granted
    /// blob (`GET /share/{token}/signed-url`). Off by default: enabling
    /// exposes grant-targeted content to the public internet — the token is
    /// the credential (256-bit, stored only as a SHA-256 hash), and grants
    /// should be minted only for content meant for public sharing.
    /// `http_api::share` carries the full threat model.
    pub share_grants_enabled: bool,
    /// On-the-fly image transforms on storage serve (ENH-014). RTDB_IMAGE_*.
    pub image: ImageTransformConfig,
}

impl StorageConfig {
    pub(super) fn from_env() -> Result<Self, String> {
        // SEC-113: require a valid signed URL on every public storage fetch.
        // Default off (Convex-parity: opaque public bearer URLs); operators
        // who want signed-only access set it to true or a database list.
        let require_signed_urls = match std::env::var("RTDB_STORAGE_REQUIRE_SIGNED_URLS") {
            Ok(raw) => SignedUrlsScope::parse(&raw)?,
            Err(_) => SignedUrlsScope::Off,
        };
        let share_grants_enabled = env_bool("RTDB_SHARE_GRANTS_ENABLED", false);
        let image = ImageTransformConfig::from_env()?;
        Ok(Self {
            require_signed_urls,
            share_grants_enabled,
            image,
        })
    }
}

impl StorageConfig {
    /// Whether public serve of a blob in `db` requires a signed URL.
    pub fn requires_signed_urls(&self, db: &str) -> bool {
        match &self.require_signed_urls {
            SignedUrlsScope::Off => false,
            SignedUrlsScope::All => true,
            SignedUrlsScope::Databases(dbs) => dbs.contains(&db.to_ascii_lowercase()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dbs(names: &[&str]) -> SignedUrlsScope {
        SignedUrlsScope::Databases(names.iter().map(|s| s.to_string()).collect())
    }

    #[test]
    fn parses_bool_spellings() {
        for t in ["true", "TRUE", " 1 ", "yes"] {
            assert_eq!(SignedUrlsScope::parse(t), Ok(SignedUrlsScope::All), "{t}");
        }
        for f in ["false", "0", "no", "", "  ", " , ,"] {
            assert_eq!(SignedUrlsScope::parse(f), Ok(SignedUrlsScope::Off), "{f:?}");
        }
    }

    #[test]
    fn parses_database_lists() {
        assert_eq!(SignedUrlsScope::parse("alpha"), Ok(dbs(&["alpha"])));
        assert_eq!(
            SignedUrlsScope::parse(" Alpha , beta_2,, alpha "),
            Ok(dbs(&["alpha", "beta_2"]))
        );
    }

    #[test]
    fn malformed_values_fail_closed() {
        for bad in ["alpha beta", "alpha;beta", "9db", "alpha,bad-name"] {
            let err = SignedUrlsScope::parse(bad).expect_err(bad);
            assert!(err.contains("RTDB_STORAGE_REQUIRE_SIGNED_URLS"), "{err}");
        }
    }

    #[test]
    fn requires_signed_urls_respects_scope() {
        let mut c = StorageConfig::default();
        assert!(!c.requires_signed_urls("alpha"));
        c.require_signed_urls = SignedUrlsScope::All;
        assert!(c.requires_signed_urls("alpha"));
        c.require_signed_urls = dbs(&["alpha"]);
        assert!(c.requires_signed_urls("alpha"));
        assert!(c.requires_signed_urls("ALPHA"));
        assert!(!c.requires_signed_urls("beta"));
    }
}
