//! Storage-serve knobs, nested under `Config::storage` (ARC-012): the
//! signed-URL requirement plus on-the-fly image transforms (ENH-014), which
//! run on the same `GET /storage/{id}` serve path.

use super::{env_bool, env_parsed};

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
    /// RTDB_STORAGE_REQUIRE_SIGNED_URLS (default false). SEC-113: when true,
    /// the public storage serve route (`GET /storage/{id}`) requires a valid
    /// `?exp=&sig=` pair on every request — a holder of the opaque id alone
    /// is no longer enough. Default false so existing public bearer URLs (a
    /// deliberate Convex-parity feature) keep working; operators who want
    /// signed-URL-only access (e.g. for sensitive content) flip it on. The
    /// mint endpoint (`GET /api/storage/{db}/{id}/signed-url`) is unaffected
    /// and remains the way to mint time-limited URLs under either mode.
    pub require_signed_urls: bool,
    /// On-the-fly image transforms on storage serve (ENH-014). RTDB_IMAGE_*.
    pub image: ImageTransformConfig,
}

impl StorageConfig {
    pub(super) fn from_env() -> Result<Self, String> {
        // SEC-113: require a valid signed URL on every public storage fetch.
        // Default false (Convex-parity: opaque public bearer URLs); operators
        // who want signed-only access flip it on.
        let require_signed_urls = env_bool("RTDB_STORAGE_REQUIRE_SIGNED_URLS", false);
        let image = ImageTransformConfig::from_env()?;
        Ok(Self {
            require_signed_urls,
            image,
        })
    }
}
