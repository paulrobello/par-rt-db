//! Shared server utilities: millisecond timestamps (`now_ms`), UUIDv7 id
//! minting (`new_id`), SHA-256 hex digests (`sha256_hex`), random tokens
//! (`random_token`), and the per-database schema cache ([`SchemaCache`]).
//! Historical call sites reach these through `crate::db`'s re-exports.

use std::sync::Arc;

use rand::Rng;
use rand::rand_core::UnwrapErr;
use rand::rngs::SysRng;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::db::load_schema;
use crate::error::RtDbError;
use crate::schema::SchemaDef;

pub fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub fn new_id() -> String {
    uuid::Uuid::now_v7().simple().to_string()
}

pub fn sha256_hex(s: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    hex::encode(hasher.finalize())
}

/// Mints a 64-hex-char (256-bit) token from the OS CSPRNG. Backs session
/// tokens, machine tokens, OAuth state, and CSRF nonces — all security
/// values (SEC-206: the OS CSPRNG, `SysRng`, by contract, not just in practice).
pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    UnwrapErr(SysRng).fill_bytes(&mut bytes);
    hex::encode(bytes)
}

/// In-memory cache of pushed schemas, keyed by database name, backed by Postgres
/// as the source of truth (see `load_schema`). Bounded by a max entry count via
/// `moka` (ARC-119): a multi-tenant instance that creates and drops databases
/// no longer grows this map indefinitely — `moka`'s LRU eviction reclaims the
/// least-recently-used schema when the cap is reached, and the next `get` for an
/// evicted db transparently reloads from Postgres. `moka::future::Cache` is
/// cheaply cloneable (internal `Arc` handle), so `#[derive(Clone)]` is a ref
/// bump — same shape the in-tree `image_transform::TransformCache` uses.
#[derive(Clone)]
pub struct SchemaCache {
    cache: moka::future::Cache<String, Arc<SchemaDef>>,
}

/// Default max entry count when constructed via [`SchemaCache::new`] or
/// [`SchemaCache::default`]. Production wiring ([`SchemaCache::with_capacity`])
/// reads `RTDB_SCHEMA_CACHE_MAX_ENTRIES` so operators of very large multi-tenant
/// instances can raise it.
const DEFAULT_SCHEMA_CACHE_MAX_ENTRIES: u64 = 1024;

impl SchemaCache {
    /// Constructs a cache with the default entry cap
    /// ([`DEFAULT_SCHEMA_CACHE_MAX_ENTRIES`]). Kept parameterless so existing
    /// test call sites are unchanged; production uses [`Self::with_capacity`].
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_SCHEMA_CACHE_MAX_ENTRIES)
    }

    /// Constructs a cache bounded to `max_entries` schemas (LRU-evicted past the
    /// cap). `max_entries == 0` is treated as unbounded — moka's behavior when
    /// no `max_capacity` is set — preserving the prior unbounded semantics for
    /// operators who explicitly disable eviction.
    pub fn with_capacity(max_entries: u64) -> Self {
        let mut builder = moka::future::Cache::builder();
        if max_entries > 0 {
            builder = builder.max_capacity(max_entries);
        }
        Self {
            cache: builder.build(),
        }
    }

    pub async fn get(&self, pool: &PgPool, db: &str) -> Result<Arc<SchemaDef>, RtDbError> {
        if let Some(schema) = self.cache.get(db).await {
            return Ok(schema);
        }

        let schema = load_schema(pool, db)
            .await?
            .ok_or_else(|| RtDbError::not_found("no schema pushed"))?;
        let schema = Arc::new(schema);
        self.cache.insert(db.to_string(), schema.clone()).await;
        Ok(schema)
    }

    pub async fn put(&self, db: &str, schema: SchemaDef) {
        self.cache.insert(db.to_string(), Arc::new(schema)).await;
    }

    /// Drops any cached schema for `db`, forcing the next `get` to reload from
    /// Postgres. Used when a write to `db`'s schema may have partially applied
    /// outside the cache's knowledge (e.g. `snapshot::import_database` failing
    /// after its internal `push_schema` already committed) — safe to call even
    /// when nothing is cached.
    pub async fn invalidate(&self, db: &str) {
        self.cache.invalidate(db).await;
    }
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}
