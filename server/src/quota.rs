//! Per-database resource-quota infrastructure (ENH-011).
//!
//! `UsageCache` is the storage-measurement + cache foundation that the
//! transport-layer enforcement points (Tasks 6 and 7) call into. It mirrors
//! the Arc-shared-state pattern of `image::TransformCache` / `HotConfig`:
//! cheap to clone (one `Arc`), lock-free over the async size query, and
//! refreshed lazily (stale-read) + eagerly (post-commit spawn).
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::PgPool;

use crate::error::RtDbError;

#[derive(Clone, Copy, Debug)]
pub struct StorageUsage {
    pub bytes: u64,
    pub computed_at_ms: i64,
}

/// Per-db storage-usage cache. Read on every growing write; refreshed lazily
/// (stale read) + eagerly (post-commit spawn). Mirrors the Arc-shared-state
/// pattern of `image::TransformCache` / `HotConfig`.
#[derive(Clone)]
pub struct UsageCache {
    // RwLock: read-lock for the hot lookup, write-lock only for the brief
    // insert after the async size query completes (never held across .await).
    inner: Arc<RwLock<HashMap<String, StorageUsage>>>,
}

impl Default for UsageCache {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageCache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Cached bytes if an entry exists and is younger than `ttl_secs`, else None.
    pub fn fresh(&self, db: &str, ttl_secs: u64) -> Option<u64> {
        let now = now_ms();
        // unwrap_or_else(into_inner) over .unwrap(): the only way this lock can
        // be poisoned is if a panic occurred inside the (tiny, panic-free)
        // critical section in store/evict. Recovering the inner value preserves
        // the cache across a panic elsewhere in the program instead of
        // propagating a second panic on the next read (QA-007). The data is
        // plain HashMap entries — never an inconsistent half-update.
        let map = self.inner.read().unwrap_or_else(|e| e.into_inner());
        let u = map.get(db)?;
        if (now - u.computed_at_ms) < (ttl_secs as i64) {
            Some(u.bytes)
        } else {
            None
        }
    }

    /// Live on-disk size of one db's user tables + blob table (heap + indexes +
    /// TOAST), framework bookkeeping excluded. Reads `pg_total_relation_size` —
    /// sub-millisecond. Schema name is `$1`-bound (no interpolation).
    pub async fn measure(pool: &PgPool, db: &str) -> Result<u64, RtDbError> {
        let schema = crate::ddl::pg_schema(db);
        let bytes: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind = 'r' \
               AND c.relname NOT IN ('meta','mutations','scheduled_txns','schema_history')",
        )
        .bind(&schema)
        .fetch_one(pool)
        .await
        .map_err(|e| RtDbError::internal(format!("measure db storage: {e}")))?;
        Ok(bytes.max(0) as u64)
    }

    pub fn store(&self, db: &str, bytes: u64) {
        // See `fresh` for the unwrap_or_else(into_inner) rationale (QA-007).
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        map.insert(
            db.to_string(),
            StorageUsage {
                bytes,
                computed_at_ms: now_ms(),
            },
        );
    }

    pub fn evict(&self, db: &str) {
        // See `fresh` for the unwrap_or_else(into_inner) rationale (QA-007).
        let mut map = self.inner.write().unwrap_or_else(|e| e.into_inner());
        map.remove(db);
    }

    /// Current usage: fresh cache hit, else a live measure (then cached).
    pub async fn current_usage(
        &self,
        pool: &PgPool,
        db: &str,
        ttl_secs: u64,
    ) -> Result<u64, RtDbError> {
        if let Some(b) = self.fresh(db, ttl_secs) {
            return Ok(b);
        }
        let b = Self::measure(pool, db).await?;
        self.store(db, b);
        Ok(b)
    }

    /// Enforce a storage cap (0 = unlimited). Returns usage so callers can add
    /// to it (e.g. upload adds the incoming blob size).
    pub async fn enforce(
        &self,
        pool: &PgPool,
        db: &str,
        cap: u64,
        ttl_secs: u64,
    ) -> Result<u64, RtDbError> {
        if cap == 0 {
            return Ok(0);
        }
        let usage = self.current_usage(pool, db, ttl_secs).await?;
        if usage >= cap {
            return Err(RtDbError::quota_exceeded(format!(
                "storage for db '{db}' is {usage} bytes, limit is {cap}"
            )));
        }
        Ok(usage)
    }

    /// Best-effort post-commit refresh (re-measure + cache).
    pub async fn refresh(&self, pool: &PgPool, db: &str) -> Result<(), RtDbError> {
        self.store(db, Self::measure(pool, db).await?);
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_returns_none_when_empty() {
        assert!(UsageCache::new().fresh("db1", 60).is_none());
    }

    #[test]
    fn store_then_fresh_within_ttl() {
        let c = UsageCache::new();
        c.store("db1", 12345);
        assert_eq!(c.fresh("db1", 60), Some(12345));
    }

    #[test]
    fn fresh_returns_none_when_stale() {
        let c = UsageCache::new();
        c.inner.write().unwrap().insert(
            "db1".to_string(),
            StorageUsage {
                bytes: 999,
                computed_at_ms: now_ms() - 120_000,
            },
        );
        assert!(c.fresh("db1", 60).is_none());
    }

    #[test]
    fn evict_drops_entry() {
        let c = UsageCache::new();
        c.store("db1", 1);
        c.evict("db1");
        assert!(c.fresh("db1", 60).is_none());
    }
}
