# ENH-011 Per-Database Resource Quotas — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add optional, hard-enforced, hot-config-driven per-database quotas for max tables, max storage bytes, and max concurrent subscriptions.

**Architecture:** Three global caps on `HotConfig` (`0` = unlimited, off by default), enforced at schema-push/migrate (tables), subscribe (subs), and mutate/scheduled/migrate/upload (storage). Storage is measured by a cached live `pg_total_relation_size` query (60s TTL + write-driven refresh); no maintained counter. New `QUOTA_EXCEEDED` error code (HTTP 507). Mirrored across all four clients + dashboard. No admin bypass — quotas apply uniformly to every principal.

**Tech Stack:** Rust (axum/tokio/sqlx), Postgres 17; TypeScript (ts-client + dashboard, bun); Python client (pydantic); Rust client (serde).

**Spec:** `docs/superpowers/specs/2026-08-07-per-db-resource-quotas-design.md`

## Global Constraints

- **Definition of done:** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) passes. Requires the dev Postgres: `make dev-db-up` first.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings.
- **SQL safety:** every identifier double-quoted or `$n`-bound; physical names via `ddl::pg_schema`/`pg_table` (already validated/lowercased). Never interpolate a user value.
- **Wire parity:** server `server/src/error.rs` + `server/src/config.rs` are authoritative; the four clients mirror byte-identically. `HotConfigPatch` is `deny_unknown_fields` — all client mirrors land together.
- **Hot-config decode:** every new `HotConfig` field MUST also be added to `PersistedHotConfig` + `merge_onto` (config.rs:571/588), or a persisted row missing it silently reverts to env on boot (real prod incident). `0` is a safe default for the three quota fields (= unlimited).
- **Env-drift gate:** any new `RTDB_*` env var goes in **both** `.env.example` and `docker-compose.yml`'s `environment:` block, or `make checkall` fails.
- **No admin bypass.** All three quotas apply uniformly to every principal (admin, machine token, user, scheduled job). `PrincipalCtx` does not distinguish admin from machine token at the committer enforcement point (both arrive as `PrincipalCtx::bypass()`, `user_id == None`), so a bypass guard is unimplementable there. Raise the cap via `PATCH /admin/config` for a large operation.
- **The tables check lives in the HTTP handler (`admin::push_schema`) and `handle_migrate` — NOT in `ddl::push_schema`.** `ddl::push_schema` is also called by the test harness's `fresh_db` (which pushes the kanban fixture regardless of caps); putting the check there would break `fresh_db` whenever a test sets a low cap. The check runs only on the admin push/migrate paths.
- **Commit per task** after its gate is green. Commit on `main` is fine (trunk-based). Client-mirror tasks (9–11) are independent and MAY run as parallel subagents.

## File Structure

**Server:**
- **NEW** `server/src/quota.rs` — `UsageCache` (per-db storage-usage cache) + `StorageUsage` + `now_ms`.
- `server/src/error.rs` — `ErrorCode::QuotaExceeded` + `status()` arm + `RtDbError::quota_exceeded`.
- `server/src/config.rs` — 3 `HotConfig` fields + `from_env` + `PersistedHotConfig`/`merge_onto` + `Config::quota_cache_ttl_secs`; extend unit tests.
- `server/src/admin.rs` — 3 `HotConfigPatch` fields + `patch_config` branches; `db_stats` quota fields; tables-check call in `push_schema`; cache-evict in `delete_db`.
- `server/src/schema.rs` — `SchemaDef::check_table_quota`.
- `server/src/committer.rs` — `CommitterCtx.quotas` + `quota_cache_ttl_secs`; storage check at `handle_mutate`/`handle_scheduled`/`handle_migrate`; tables check at `handle_migrate`; subs check at `handle_subscribe`; thread through `Committers::new`.
- `server/src/subs.rs` — `SubscriptionManager::count_for_db`.
- `server/src/http_api.rs` — storage check at `upload_handler`.
- `server/src/metrics.rs` — `QuotaCounters` + `QuotaKind` + 3 `AtomicU64` globals + `record_quota_rejection`; `MetricsSnapshot` + `render_prometheus`.
- `server/src/lib.rs` — `AppState.quotas` field + `AppState::new` wiring; `mod quota`.
- `.env.example` + `docker-compose.yml` — 4 new `RTDB_*` vars.
- **NEW** `server/tests/quota_test.rs` — integration tests for all three quotas.
- `server/tests/dashboard_test.rs` — extend hot-config round-trip + missing-field regression.

**Clients (mirror):** error-code union + `HotConfig`/`HotConfigPatch` in ts/rust/python + dashboard types.

**Docs:** `FEATURE_MATRIX.md`, `CLAUDE.md`.

---

## Task 1: `QUOTA_EXCEEDED` error code (server)

**Files:** Modify `server/src/error.rs` (enum :7, `status()` :93, constructor region :86).

**Produces:** `ErrorCode::QuotaExceeded`; `RtDbError::quota_exceeded(msg) -> Self`; `status()` → `StatusCode::INSUFFICIENT_STORAGE` (507).

- [ ] **Step 1: Write the failing test**

Append to the `#[cfg(test)]` block in `server/src/error.rs`:

```rust
#[test]
fn quota_exceeded_maps_to_507() {
    let err = RtDbError::quota_exceeded("db over cap");
    assert_eq!(err.code, ErrorCode::QuotaExceeded);
    assert_eq!(err.status(), StatusCode::INSUFFICIENT_STORAGE);
    let json = serde_json::to_value(&err).unwrap();
    assert_eq!(json["code"], "QUOTA_EXCEEDED");
    assert_eq!(json["message"], "db over cap");
}
```

- [ ] **Step 2: Run — fails (compile error)**

`cargo test --manifest-path server/Cargo.toml quota_exceeded_maps_to_507` → `QuotaExceeded`/`quota_exceeded` not defined.

- [ ] **Step 3: Implement**

Add the variant (after `Conflict,`):

```rust
    Conflict,
    QuotaExceeded,
}
```

Add the `status()` arm (no wildcard — adding the variant forces this):

```rust
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::QuotaExceeded => StatusCode::INSUFFICIENT_STORAGE,
```

Add the constructor next to `conflict` (`RtDbError::new` is `pub`):

```rust
    /// A per-database resource quota was exceeded (HTTP 507). Used for
    /// table-count, storage-byte, and concurrent-subscription caps; the message
    /// identifies which. Wire code `QUOTA_EXCEEDED`.
    pub fn quota_exceeded(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::QuotaExceeded, msg)
    }
```

- [ ] **Step 4: Run — passes**

`cargo test --manifest-path server/Cargo.toml quota_exceeded_maps_to_507` → PASS.

- [ ] **Step 5: Gate + commit**

`make -C /Users/probello/Repos/par-rt-db lint` (clippy clean), then:
```bash
git add server/src/error.rs
git commit -m "feat(quotas): add QUOTA_EXCEEDED error code (507)"
```

---

## Task 2: `HotConfig` quota fields + env (server)

**Files:** `server/src/config.rs`, `server/src/admin.rs` (`HotConfigPatch` :1651, `patch_config` :1660), `.env.example`, `docker-compose.yml`; tests `server/src/config.rs` (:654) + `server/tests/dashboard_test.rs:831` + `server/tests/common/mod.rs:75`.

**Produces:** `HotConfig { max_tables_per_db: usize, max_storage_bytes_per_db: u64, max_subs_per_db: usize }` (default 0); `Config::quota_cache_ttl_secs: u64` (`RTDB_QUOTA_CACHE_TTL_SECS`, default 60).

- [ ] **Step 1: Write the failing unit test**

In `server/src/config.rs` test module, add a quota round-trip test:

```rust
    #[test]
    fn persisted_quota_fields_round_trip() {
        let row = serde_json::json!({
            "maxTablesPerDb": 25,
            "maxStorageBytesPerDb": 536870912,
            "maxSubsPerDb": 100,
            "sessionTtlDays": 30
        });
        let persisted: PersistedHotConfig =
            serde_json::from_value(row).expect("quota row decodes");
        let merged = persisted.merge_onto(env_seed());
        assert_eq!(merged.max_tables_per_db, 25);
        assert_eq!(merged.max_storage_bytes_per_db, 536870912);
        assert_eq!(merged.max_subs_per_db, 100);
    }
```

And extend `persisted_row_missing_a_newer_field_still_applies_the_rest` (after the `idempotency_ttl_ms` assertion) to assert the absent quota fields fall back to the env seed (0):

```rust
        assert_eq!(merged.max_tables_per_db, 0);
        assert_eq!(merged.max_storage_bytes_per_db, 0);
        assert_eq!(merged.max_subs_per_db, 0);
```

- [ ] **Step 2: Run — fails**

`cargo test --manifest-path server/Cargo.toml --lib config::tests` → no field `max_tables_per_db`.

- [ ] **Step 3: Add the three fields to `HotConfig` + `from_env`**

Extend the `HotConfig` struct:

```rust
    pub idempotency_ttl_ms: i64, // RTDB_IDEMPOTENCY_TTL_MS, default mutation_log::DEFAULT_DEDUP_TTL_MS (5 min)
    /// Per-database resource quotas (ENH-011). 0 = unlimited (quota disabled).
    pub max_tables_per_db: usize,       // RTDB_MAX_TABLES_PER_DB,       default 0
    pub max_storage_bytes_per_db: u64,  // RTDB_MAX_STORAGE_BYTES_PER_DB, default 0
    pub max_subs_per_db: usize,         // RTDB_MAX_SUBS_PER_DB,        default 0
}
```

In `from_env`, add three parses before the `Self { … }` tail:

```rust
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
```

and include all three in the `Self { … }` literal.

- [ ] **Step 4: Mirror into `PersistedHotConfig` + `merge_onto`** (non-negotiable)

```rust
struct PersistedHotConfig {
    #[serde(default)] allowed_origins: Option<Vec<String>>,
    #[serde(default)] session_ttl_days: Option<i64>,
    #[serde(default)] max_file_size: Option<usize>,
    #[serde(default)] idempotency_ttl_ms: Option<i64>,
    #[serde(default)] max_tables_per_db: Option<usize>,
    #[serde(default)] max_storage_bytes_per_db: Option<u64>,
    #[serde(default)] max_subs_per_db: Option<usize>,
}
```

```rust
            max_tables_per_db: self.max_tables_per_db.unwrap_or(defaults.max_tables_per_db),
            max_storage_bytes_per_db: self
                .max_storage_bytes_per_db
                .unwrap_or(defaults.max_storage_bytes_per_db),
            max_subs_per_db: self.max_subs_per_db.unwrap_or(defaults.max_subs_per_db),
```

- [ ] **Step 5: Add the cache-TTL boot config field**

In `server/src/config.rs`, add to the cold `Config` struct:

```rust
    pub quota_cache_ttl_secs: u64, // RTDB_QUOTA_CACHE_TTL_SECS, default 60
```

Parse it in `Config::from_env` (alongside the other `match std::env::var(...)` parses):

```rust
        let quota_cache_ttl_secs = match std::env::var("RTDB_QUOTA_CACHE_TTL_SECS") {
            Ok(v) => v.parse::<u64>().unwrap_or(60),
            Err(_) => 60,
        };
```

and include `quota_cache_ttl_secs,` in the `Config { … }` literal.

- [ ] **Step 6: `HotConfigPatch` fields + `patch_config` branches**

In `server/src/admin.rs`, add three fields to `HotConfigPatch`:

```rust
    max_tables_per_db: Option<usize>,
    max_storage_bytes_per_db: Option<u64>,
    max_subs_per_db: Option<usize>,
}
```

In `patch_config`, after the `idempotency_ttl_ms` branch and before the `origins_valid()` check, add three assign branches (unsigned types forbid negatives; no hard ceiling):

```rust
    if let Some(cap) = patch.max_tables_per_db {
        next.max_tables_per_db = cap;
    }
    if let Some(cap) = patch.max_storage_bytes_per_db {
        next.max_storage_bytes_per_db = cap;
    }
    if let Some(cap) = patch.max_subs_per_db {
        next.max_subs_per_db = cap;
    }
```

- [ ] **Step 7: Fix every `HotConfig { … }` literal**

The three new fields break every struct literal. Update, at minimum:
- `server/tests/common/mod.rs:75` `test_hot()` — add `max_tables_per_db: 0, max_storage_bytes_per_db: 0, max_subs_per_db: 0,`.
- `server/tests/dashboard_test.rs:831` `hot_config_round_trips_through_rtdb_config`'s `HotConfig { … }` literal — add the same three `: 0` fields.
- Any other literal `cargo build` names.

- [ ] **Step 8: Env-drift gate**

Add to `.env.example` (near `RTDB_MAX_FILE_SIZE`):
```
RTDB_MAX_TABLES_PER_DB=0
RTDB_MAX_STORAGE_BYTES_PER_DB=0
RTDB_MAX_SUBS_PER_DB=0
RTDB_QUOTA_CACHE_TTL_SECS=60
```
Add the same four to `docker-compose.yml`'s `environment:` block.

- [ ] **Step 9: Run + commit**

```bash
make dev-db-up
cargo test --manifest-path server/Cargo.toml --lib config::tests
cargo test --manifest-path server/Cargo.toml --test dashboard_test hot_config
git add server/src/config.rs server/src/admin.rs server/tests/common/mod.rs server/tests/dashboard_test.rs .env.example docker-compose.yml
git commit -m "feat(quotas): add HotConfig quota fields + env (ENH-011)"
```

---

## Task 3: Quota infrastructure — `UsageCache`, measurement, metrics counter (server)

**Files:** Create `server/src/quota.rs`; modify `server/src/lib.rs` (`mod quota;`, `AppState.quotas`, `AppState::new`); `server/src/metrics.rs`.

**Produces:** `quota::UsageCache` (Clone) — `new`, `fresh(db, ttl) -> Option<u64>`, `measure(pool, db) -> Result<u64>`, `store`, `evict`, `current_usage(pool, db, ttl) -> Result<u64>`, `enforce(pool, db, cap, ttl) -> Result<u64>`, `refresh(pool, db) -> Result<()>`. `metrics::QuotaKind` (`Tables`/`Storage`/`Subs`). `Metrics::record_quota_rejection(db, kind)`. `AppState.quotas: Arc<quota::UsageCache>`.

- [ ] **Step 1: Write the failing unit tests (no DB)**

Create `server/src/quota.rs`:

```rust
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
            StorageUsage { bytes: 999, computed_at_ms: now_ms() - 120_000 },
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
```

Register it in `server/src/lib.rs`: add `mod quota;` beside the other `mod` declarations.

- [ ] **Step 2: Run — fails** (`new`/`store`/`fresh`/`evict` undefined)

`cargo test --manifest-path server/Cargo.toml --lib quota::tests`

- [ ] **Step 3: Implement `UsageCache`**

Fill `server/src/quota.rs` above the test module:

```rust
impl UsageCache {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Cached bytes if an entry exists and is younger than `ttl_secs`, else None.
    pub fn fresh(&self, db: &str, ttl_secs: u64) -> Option<u64> {
        let now = now_ms();
        let map = self.inner.read().unwrap();
        let u = map.get(db)?;
        if (now - u.computed_at_ms) < (ttl_secs as i64) {
            Some(u.bytes)
        } else {
            None
        }
    }

    /// Live on-disk size of one db's user tables + blob table (heap + indexes +
    /// TOAST), framework bookkeeping excluded. Reads `pg_class.relpages` —
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
        let mut map = self.inner.write().unwrap();
        map.insert(db.to_string(), StorageUsage { bytes, computed_at_ms: now_ms() });
    }

    pub fn evict(&self, db: &str) {
        let mut map = self.inner.write().unwrap();
        map.remove(db);
    }

    /// Current usage: fresh cache hit, else a live measure (then cached).
    pub async fn current_usage(&self, pool: &PgPool, db: &str, ttl_secs: u64) -> Result<u64, RtDbError> {
        if let Some(b) = self.fresh(db, ttl_secs) {
            return Ok(b);
        }
        let b = Self::measure(pool, db).await?;
        self.store(db, b);
        Ok(b)
    }

    /// Enforce a storage cap (0 = unlimited). Returns usage so callers can add
    /// to it (e.g. upload adds the incoming blob size).
    pub async fn enforce(&self, pool: &PgPool, db: &str, cap: u64, ttl_secs: u64) -> Result<u64, RtDbError> {
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
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
```

- [ ] **Step 4: Run — passes (4 tests)**

`cargo test --manifest-path server/Cargo.toml --lib quota::tests`

- [ ] **Step 5: Add `AppState.quotas` + wire in `AppState::new`**

In `server/src/lib.rs`, add the field beside `image`:

```rust
    /// Per-db storage-usage cache (ENH-011). `Arc` — read on every growing
    /// write, refreshed lazily + eagerly.
    pub quotas: Arc<quota::UsageCache>,
```

In `AppState::new`, after `let image = …`:

```rust
        let quotas = Arc::new(quota::UsageCache::new());
```

and in the struct literal (after `image,`):

```rust
            image,
            quotas,
        })
```

- [ ] **Step 6: Add the metrics counter + `record_quota_rejection`**

In `server/src/metrics.rs`, add (mirror `DbSubCounters` at :86):

```rust
#[derive(Default)]
struct QuotaCounters {
    tables: AtomicU64,
    storage: AtomicU64,
    subs: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub enum QuotaKind {
    Tables,
    Storage,
    Subs,
}
```

Add to the `Metrics` struct (beside `per_db_subs`):

```rust
    per_db_quota: Mutex<HashMap<String, QuotaCounters>>,
    quota_rejections_tables_total: AtomicU64,
    quota_rejections_storage_total: AtomicU64,
    quota_rejections_subs_total: AtomicU64,
```

Add the recorder (mirror `record_subs_skip` at :265):

```rust
    /// A per-db resource quota rejection (ENH-011). Records the global per-kind
    /// counter + a per-db breakdown. The per-db breakdown is JSON-snapshot only;
    /// the Prometheus scrape carries the aggregate-by-kind totals (no per-db
    /// labels on the public export — same convention as `per_db_subs`).
    pub fn record_quota_rejection(&self, db: &str, kind: QuotaKind) {
        let global = match kind {
            QuotaKind::Tables => &self.quota_rejections_tables_total,
            QuotaKind::Storage => &self.quota_rejections_storage_total,
            QuotaKind::Subs => &self.quota_rejections_subs_total,
        };
        global.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut map) = self.per_db_quota.lock() {
            let entry = map.entry(db.to_string()).or_default();
            let per_db = match kind {
                QuotaKind::Tables => &entry.tables,
                QuotaKind::Storage => &entry.storage,
                QuotaKind::Subs => &entry.subs,
            };
            per_db.fetch_add(1, Ordering::Relaxed);
        }
    }
```

- [ ] **Step 7: Expose in `MetricsSnapshot` + Prometheus scrape**

`MetricsSnapshot` is at `server/src/metrics.rs:388`; the `snapshot()` builder fills it at `:353`–`:385` (the fill ends with `per_db_subs: self.per_db_subs_snapshot(),` at `:383`). `render_prometheus` is at `:450`. The `/admin/metrics` JSON handler is `admin.rs:1413`; the Prometheus handler `lib.rs:242` is **aggregate-only (no per-db)** (`lib.rs:239`).

- Add three fields to `MetricsSnapshot` (before `per_db_subs`):
```rust
    pub quota_rejections_tables_total: u64,
    pub quota_rejections_storage_total: u64,
    pub quota_rejections_subs_total: u64,
    pub per_db_quota: Vec<DbQuotaCounterRow>,
```
- In `snapshot()` fill (before the `per_db_subs:` line at `:383`), load them:
```rust
            quota_rejections_tables_total: self.quota_rejections_tables_total.load(Ordering::Relaxed),
            quota_rejections_storage_total: self.quota_rejections_storage_total.load(Ordering::Relaxed),
            quota_rejections_subs_total: self.quota_rejections_subs_total.load(Ordering::Relaxed),
            per_db_quota: self.per_db_quota_snapshot(),
```
- Add `DbQuotaCounterRow { db: String, tables: u64, storage: u64, subs: u64 }` + a `per_db_quota_snapshot()` method mirroring `per_db_subs_snapshot()` exactly (copy its shape, swap the fields).
- In `render_prometheus`, emit the three **aggregate-by-kind** totals only (no per-db), copying the `rtdb_subs_skips_total{class="…"}` triple at `:470`:
```rust
    s.push_str("# HELP rtdb_quota_rejections_total Resource-quota rejections by kind.\n");
    s.push_str("# TYPE rtdb_quota_rejections_total counter\n");
    s.push_str(&format!("rtdb_quota_rejections_total{{kind=\"tables\"}} {}\n", snap.quota_rejections_tables_total));
    s.push_str(&format!("rtdb_quota_rejections_total{{kind=\"storage\"}} {}\n", snap.quota_rejections_storage_total));
    s.push_str(&format!("rtdb_quota_rejections_total{{kind=\"subs\"}} {}\n", snap.quota_rejections_subs_total));
```
(Deliberately do NOT render `per_db_quota` in `render_prometheus` — cardinality. It surfaces only via `/admin/metrics` JSON.)

- [ ] **Step 8: Gate + commit**

`cargo build --manifest-path server/Cargo.toml` (fix any snapshot field the compiler names), then `make -C /Users/probello/Repos/par-rt-db lint && cargo test --manifest-path server/Cargo.toml --lib quota`.
```bash
git add server/src/quota.rs server/src/lib.rs server/src/metrics.rs
git commit -m "feat(quotas): UsageCache + measure + metrics counter (ENH-011)"
```

---

## Task 4: `maxTablesPerDb` enforcement (server)

**Files:** `server/src/schema.rs` (`check_table_quota`); `server/src/admin.rs` (`push_schema` :211); `server/src/committer.rs` (`handle_migrate` after `:788`); test `server/tests/quota_test.rs`.

**Produces:** `SchemaDef::check_table_quota(&self, cap) -> Result<(), RtDbError>` (cap==0 → Ok).

- [ ] **Step 1: Write the failing integration test (HTTP path)**

Create `server/tests/quota_test.rs`. The tables check runs in the admin HTTP handler, so exercise it via `spawn_app` + `admin_post` (NOT `ddl::push_schema` direct — see Global Constraints):

```rust
use axum::http::StatusCode;
use rtdb_server::config::HotConfig;
use rtdb_server::{common, db};
use std::sync::Arc;

fn schema_with_n_tables(n: usize) -> serde_json::Value {
    let mut tables = serde_json::Map::new();
    for i in 0..n {
        tables.insert(format!("t{i}"), serde_json::json!({ "fields": { "name": "string" } }));
    }
    serde_json::json!({ "tables": tables })
}

#[tokio::test]
async fn push_schema_over_table_cap_is_rejected() -> anyhow::Result<()> {
    let state = common::test_state().await;
    state
        .runtime
        .hot
        .store(Arc::new(HotConfig { max_tables_per_db: 3, ..common::test_hot() }));
    let addr = common::spawn_app(state.clone()).await;
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db_name).await?;

    // 3 tables: at cap → ok
    let r = common::admin_post(addr, "/admin/push-schema",
        serde_json::json!({ "db": db_name, "schema": schema_with_n_tables(3) })).await;
    assert_eq!(r.status(), StatusCode::OK);

    // 4 tables: over cap → 507
    let r = common::admin_post(addr, "/admin/push-schema",
        serde_json::json!({ "db": db_name, "schema": schema_with_n_tables(4) })).await;
    assert_eq!(r.status(), StatusCode::INSUFFICIENT_STORAGE);
    Ok(())
}
```

> `common::spawn_app` (`:264`) serves the real router with the test-admin-key bearer that `admin_post` (`:448`) sends. `common::admin_post(addr, path, body)` is the helper signature.

- [ ] **Step 2: Run — fails** (over-cap push returns 200)

`make dev-db-up && cargo test --manifest-path server/Cargo.toml --test quota_test push_schema_over_table_cap`

- [ ] **Step 3: Implement `check_table_quota`**

In `server/src/schema.rs` `impl SchemaDef` (beside `validate`, ends `:775`):

```rust
    /// Reject a schema whose table count exceeds `cap`. `cap == 0` is unlimited.
    /// Counted as `tables.len()` (user-declared tables only).
    pub fn check_table_quota(&self, cap: usize) -> Result<(), RtDbError> {
        if cap > 0 && self.tables.len() > cap {
            return Err(RtDbError::quota_exceeded(format!(
                "db has {} table(s), limit is {cap}",
                self.tables.len()
            )));
        }
        Ok(())
    }
```

- [ ] **Step 4: Call it from the push-schema handler (with metric)**

In `server/src/admin.rs` `push_schema`, between `require_admin(...)` (`:211`) and `ddl::push_schema(...)` (`:212`):

```rust
    require_admin(&state, &headers).await?;
    body.schema
        .check_table_quota(state.runtime.hot.load().max_tables_per_db)
        .map_err(|e| {
            state
                .runtime
                .metrics
                .record_quota_rejection(&body.db, crate::metrics::QuotaKind::Tables);
            e
        })?;
    let applied = ddl::push_schema(&state.pool, &body.db, body.schema).await?;
```

- [ ] **Step 5: Call it from `handle_migrate` (with metric)**

In `server/src/committer.rs` `handle_migrate`, immediately after `derived.validate()?;` (`:788`):

```rust
    derived.validate()?;
    derived
        .check_table_quota(ctx.hot.load().max_tables_per_db)
        .map_err(|e| {
            ctx.metrics.record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Tables);
            e
        })?;
```

- [ ] **Step 6: Run + gate + commit**

```bash
cargo test --manifest-path server/Cargo.toml --test quota_test
make -C /Users/probello/Repos/par-rt-db lint
git add server/src/schema.rs server/src/admin.rs server/src/committer.rs server/tests/quota_test.rs
git commit -m "feat(quotas): enforce maxTablesPerDb at push-schema + migrate (ENH-011)"
```

---

## Task 5: `maxSubsPerDb` enforcement (server)

**Files:** `server/src/subs.rs` (`count_for_db`); `server/src/committer.rs` (`handle_subscribe` before `:1008`); test `server/tests/quota_test.rs`.

**Produces:** `SubscriptionManager::count_for_db(&self, db) -> impl Future<Output = usize>`.

- [ ] **Step 1: Write the failing integration test**

`count_for_db` and `subscribe` use the real committer path (`state.realtime.committers`), so call them directly (as `subs_test.rs` does). The Query/Insert helpers are **not** in `common` — copy them inline from `subs_test.rs:12`–`54`. Append to `server/tests/quota_test.rs`:

```rust
use rtdb_server::query::Query;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction};
use rtdb_server::PrincipalCtx;

fn work_item_doc(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "projectId": "0".repeat(32),
        "title": "item",
        "status": status,
        "order": order,
        "completedAt": null
    })
    .as_object()
    .expect("json object")
    .clone()
}
fn insert_work_item(status: &str, order: f64) -> Transaction {
    Transaction { steps: vec![Step::Insert { table: "workItems".to_string(), doc: work_item_doc(status, order) }] }
}
fn collect_work_items() -> Query {
    serde_json::from_value(serde_json::json!({ "table": "workItems" })).expect("parse query")
}

#[tokio::test]
async fn subscribe_over_cap_is_rejected() -> anyhow::Result<()> {
    let state = common::test_state().await;
    state
        .runtime
        .hot
        .store(Arc::new(HotConfig { max_subs_per_db: 2, ..common::test_hot() }));
    let db = common::fresh_db(&state).await; // pushes the kanban fixture (workItems table)

    for qid in ["q1", "q2"] {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        state
            .realtime
            .committers
            .subscribe(&db, next_conn_id(), qid.to_string(), collect_work_items(), tx, PrincipalCtx::bypass())
            .await?;
    }
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let err = state
        .realtime
        .committers
        .subscribe(&db, next_conn_id(), "q3".to_string(), collect_work_items(), tx, PrincipalCtx::bypass())
        .await
        .expect_err("third subscribe over cap must be rejected");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::QuotaExceeded);
    Ok(())
}
```

- [ ] **Step 2: Run — fails** (third subscribe registers)

`cargo test --manifest-path server/Cargo.toml --test quota_test subscribe_over_cap`

- [ ] **Step 3: Implement `count_for_db`**

In `server/src/subs.rs` `impl SubscriptionManager` (beside `count` at `:920`; combine `count`'s body with `snapshot`'s single-db lookup):

```rust
    /// Active subscription count for one database. Same lock discipline as
    /// [`count`](Self::count): clone the shard `Arc` under the outer lock, drop
    /// it, then lock the shard — the outer lock is never held while waiting on
    /// the shard.
    pub async fn count_for_db(&self, db: &str) -> usize {
        let shard = { self.subs.lock().await.get(db).cloned() };
        match shard {
            Some(s) => s.lock().await.len(),
            None => 0,
        }
    }
```

- [ ] **Step 4: Enforce in `handle_subscribe` (no admin bypass — uniform)**

In `server/src/committer.rs` `handle_subscribe`, immediately before the `ctx.subs.register(...)` chain (the call beginning at `:1008`, after `let table_def = schema.table(&query.table)?;`):

```rust
    let sub_cap = ctx.hot.load().max_subs_per_db;
    if sub_cap > 0 {
        let n = ctx.subs.count_for_db(&ctx.db).await;
        if n >= sub_cap {
            ctx.metrics.record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Subs);
            return Err(RtDbError::quota_exceeded(format!(
                "db '{}' has {} active subscription(s), limit is {sub_cap}",
                ctx.db, n
            )));
        }
    }
```

- [ ] **Step 5: Run + gate + commit**

```bash
cargo test --manifest-path server/Cargo.toml --test quota_test
make -C /Users/probello/Repos/par-rt-db lint
git add server/src/subs.rs server/src/committer.rs server/tests/quota_test.rs
git commit -m "feat(quotas): enforce maxSubsPerDb at subscribe (ENH-011)"
```

---

## Task 6: `maxStorageBytesPerDb` enforcement at committer arms (server)

**Files:** `server/src/committer.rs` (`CommitterCtx`; `handle_mutate` `:477`; `handle_scheduled` `:560`; `handle_migrate` after the tables check); `server/src/lib.rs` (`Committers::new` call). Test `server/tests/quota_test.rs`.

**Produces:** `CommitterCtx.quotas: Arc<quota::UsageCache>` + `quota_cache_ttl_secs: u64`.

- [ ] **Step 1: Write the failing integration test**

Seed a doc first (so the table has non-zero size — deterministic regardless of empty-table accounting), then set a 1-byte cap and assert the next write is rejected. Append to `server/tests/quota_test.rs`:

```rust
#[tokio::test]
async fn mutate_over_storage_cap_is_rejected() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let db = common::fresh_db(&state).await;

    // seed a doc so the table has non-zero on-disk size (deterministic)
    state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0), PrincipalCtx::bypass())
        .await?;

    // now a 1-byte cap: the seeded table's footprint already exceeds it
    state
        .runtime
        .hot
        .store(Arc::new(HotConfig { max_storage_bytes_per_db: 1, ..common::test_hot() }));

    let err = state
        .realtime
        .committers
        .mutate(&db, None, insert_work_item("backlog", 2.0), PrincipalCtx::bypass())
        .await
        .expect_err("write over the 1-byte storage cap must be rejected");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::QuotaExceeded);
    Ok(())
}
```

- [ ] **Step 2: Run — fails** (second write succeeds)

`cargo test --manifest-path server/Cargo.toml --test quota_test mutate_over_storage_cap`

- [ ] **Step 3: Add `quotas` + `quota_cache_ttl_secs` to `CommitterCtx`; thread through `Committers::new`**

In `server/src/committer.rs` `CommitterCtx` (`:346`), add (beside `hot`):

```rust
    hot: Arc<ArcSwap<HotConfig>>,
    quotas: Arc<crate::quota::UsageCache>,
    quota_cache_ttl_secs: u64,
```

Update every `CommitterCtx { … }` literal to set `quotas: quotas.clone(), quota_cache_ttl_secs`. Add `quotas: Arc<crate::quota::UsageCache>` + `quota_cache_ttl_secs: u64` params to `Committers::new(...)`, store them on the `Committers` struct, and pass them into each `CommitterCtx` it builds (mirror how `hot` is threaded). Update the single `Committers::new(...)` call in `server/src/lib.rs` `AppState::new` to pass `quotas.clone()` + `config.quota_cache_ttl_secs`.

- [ ] **Step 4: Enforce at `handle_mutate` entry (uniform — no bypass)**

In `handle_mutate`, after `let schema = ctx.schemas.get(...)` (`:477`) and before `let outcome = execute_txn(...)` (`:478`):

```rust
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        if let Err(e) = ctx
            .quotas
            .enforce(&ctx.pool, &ctx.db, storage_cap, ctx.quota_cache_ttl_secs)
            .await
        {
            ctx.metrics.record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
            return Err(e);
        }
    }
```

- [ ] **Step 5: Enforce at `handle_scheduled` entry**

In `handle_scheduled`, after the schema load (`:560`) and before `execute_txn(...)`. A scheduled job has no principal; on rejection, mirror the existing execute_txn-failure path (`committer.rs:624`–`:649`) — record the metric, mark the job errored, and return `Ok(())` (the scheduler records the error in the job row rather than propagating):

```rust
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        if let Err(e) = ctx
            .quotas
            .enforce(&ctx.pool, &ctx.db, storage_cap, ctx.quota_cache_ttl_secs)
            .await
        {
            ctx.metrics.record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &e.message).await;
            return Ok(());
        }
    }
```

> `scheduler::mark_error(pool, db, id, msg)` is the exact existing signature (`committer.rs:556`, `:624`). `e.message` is the `RtDbError` message field (see `error.rs`).

- [ ] **Step 6: Enforce at `handle_migrate` (after the tables check)**

In `handle_migrate`, after the `check_table_quota(...)` call added in Task 4 (migrate is admin-only, no principal — uniform):

```rust
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        ctx.quotas
            .enforce(&ctx.pool, &ctx.db, storage_cap, ctx.quota_cache_ttl_secs)
            .await
            .map_err(|e| {
                ctx.metrics.record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
                e
            })?;
    }
```

- [ ] **Step 7: Post-commit best-effort refresh**

At the end of `handle_mutate` (after `execute_txn` succeeds, before returning), spawn a refresh (fire-and-forget, mirrors the audit/webhook taps). Add the same spawn to `handle_scheduled` (after a successful execute) and `handle_migrate` (after `apply_migration` succeeds):

```rust
    {
        let quotas = ctx.quotas.clone();
        let pool = ctx.pool.clone();
        let db = ctx.db.clone();
        tokio::spawn(async move {
            let _ = quotas.refresh(&pool, &db).await;
        });
    }
```

- [ ] **Step 8: Run + gate (full suite — touches the committer)**

```bash
cargo test --manifest-path server/Cargo.toml --test quota_test
make -C /Users/probello/Repos/par-rt-db checkall
git add server/src/committer.rs server/src/lib.rs server/tests/quota_test.rs
git commit -m "feat(quotas): enforce maxStorageBytesPerDb at mutate/scheduled/migrate (ENH-011)"
```

---

## Task 7: Storage quota at upload + delete-db cache eviction (server)

**Files:** `server/src/http_api.rs` (`upload_handler` `:417`→`:423`); `server/src/admin.rs` (`delete_db`); test `server/tests/quota_test.rs`.

- [ ] **Step 1: Write the failing integration test**

Upload is HTTP — use `spawn_app` + the machine-token/upload helpers copied inline from `storage_test.rs:134`/`:148`. Append to `server/tests/quota_test.rs`:

```rust
async fn mint_token(addr: std::net::SocketAddr, db: &str) -> String {
    let resp = common::admin_post(addr, "/admin/mint-token",
        serde_json::json!({ "db": db, "name": "test-token" })).await;
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<serde_json::Value>().await.expect("json")["token"]
        .as_str().expect("token").to_string()
}

#[tokio::test]
async fn upload_over_storage_cap_is_rejected() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let db = common::fresh_db(&state).await;
    let addr = common::spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;

    // 1-byte cap: any upload (5 bytes) exceeds it (0 used + 5 > 1)
    state
        .runtime
        .hot
        .store(Arc::new(HotConfig { max_storage_bytes_per_db: 1, ..common::test_hot() }));

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "application/octet-stream")
        .body(b"hello".to_vec())
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::INSUFFICIENT_STORAGE);
    Ok(())
}
```

- [ ] **Step 2: Run — fails** (upload returns 200)

`cargo test --manifest-path server/Cargo.toml --test quota_test upload_over_storage_cap`

- [ ] **Step 3: Enforce in `upload_handler` (uniform — no bypass)**

In `server/src/http_api.rs` `upload_handler`, after `let size = bytes.len() as i64;` (`:417`) and before `storage::put(...)` (`:423`):

```rust
    let storage_cap = state.runtime.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        let used = state
            .quotas
            .current_usage(&state.pool, &db, state.config.quota_cache_ttl_secs)
            .await?;
        if used + (size as u64) > storage_cap {
            state.runtime.metrics.record_quota_rejection(&db, crate::metrics::QuotaKind::Storage);
            return Err(RtDbError::quota_exceeded(format!(
                "upload of {size} bytes would exceed storage quota for db '{db}' ({used} used, limit {storage_cap})"
            )));
        }
    }
```

After the successful `storage::put(...)` call, refresh the cache (best-effort):

```rust
    {
        let quotas = state.quotas.clone();
        let pool = state.pool.clone();
        let db = db.clone();
        tokio::spawn(async move {
            let _ = quotas.refresh(&pool, &db).await;
        });
    }
```

- [ ] **Step 4: Evict on delete-db**

In `server/src/admin.rs` `delete_db` handler, after the database is dropped (where `DROP SCHEMA` / the delete runs), add:

```rust
    state.quotas.evict(&db);
```

- [ ] **Step 5: Run + gate + commit**

```bash
cargo test --manifest-path server/Cargo.toml --test quota_test
make -C /Users/probello/Repos/par-rt-db lint
git add server/src/http_api.rs server/src/admin.rs server/tests/quota_test.rs
git commit -m "feat(quotas): enforce storage cap at upload + evict on delete-db (ENH-011)"
```

---

## Task 8: `db_stats` quota fields (server)

**Files:** `server/src/admin.rs` (`DbStatsResponse` `:1362`; `db_stats` `:1369`); test `server/tests/dashboard_test.rs`.

**Produces:** `DbStatsResponse { tablesQuota, tablesUsed, storageQuotaBytes, storageUsedBytes, subsQuota, subsUsed }`.

- [ ] **Step 1: Write the failing test** (append to `server/tests/dashboard_test.rs`; use `spawn_app` + `admin_get` `:463`):

```rust
#[tokio::test]
async fn db_stats_reports_quota_and_usage() -> anyhow::Result<()> {
    let state = common::test_state().await;
    state.runtime.hot.store(std::sync::Arc::new(
        rtdb_server::config::HotConfig {
            max_tables_per_db: 9, max_storage_bytes_per_db: 1000, max_subs_per_db: 5,
            ..common::test_hot()
        },
    ));
    let db = common::fresh_db(&state).await;
    let addr = common::spawn_app(state).await;

    let resp = common::admin_get(addr, &format!("/admin/db/{db}/stats")).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["tablesQuota"], 9);
    assert_eq!(body["storageQuotaBytes"], 1000);
    assert_eq!(body["subsQuota"], 5);
    Ok(())
}
```

- [ ] **Step 2: Run — fails** (fields absent)

`cargo test --manifest-path server/Cargo.toml --test dashboard_test db_stats_reports_quota`

- [ ] **Step 3: Extend `DbStatsResponse` + populate**

```rust
#[derive(Serialize)]
struct DbStatsResponse {
    tables: Vec<TableStat>,
    #[serde(rename = "totalSizeBytes")] total_size_bytes: i64,
    #[serde(rename = "tablesQuota")] tables_quota: usize,
    #[serde(rename = "tablesUsed")] tables_used: usize,
    #[serde(rename = "storageQuotaBytes")] storage_quota_bytes: u64,
    #[serde(rename = "storageUsedBytes")] storage_used_bytes: u64,
    #[serde(rename = "subsQuota")] subs_quota: usize,
    #[serde(rename = "subsUsed")] subs_used: usize,
}
```

In `db_stats`, before the return (reuse the already-computed `total_size_bytes`; `subs_used` via the new `count_for_db`):

```rust
    let hot = state.runtime.hot.load();
    let subs_used = state.realtime.subs.count_for_db(&db).await;
    Ok(Json(DbStatsResponse {
        tables,
        total_size_bytes,
        tables_quota: hot.max_tables_per_db,
        tables_used: schema_def.tables.len(),
        storage_quota_bytes: hot.max_storage_bytes_per_db,
        storage_used_bytes: total_size_bytes.max(0) as u64,
        subs_quota: hot.max_subs_per_db,
        subs_used,
    }))
```

- [ ] **Step 4: Run + gate + commit**

```bash
cargo test --manifest-path server/Cargo.toml --test dashboard_test db_stats
make -C /Users/probello/Repos/par-rt-db lint
git add server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(quotas): expose quota+usage in db_stats response (ENH-011)"
```

---

## Task 9: ts-client mirror — error code + HotConfig fields

**Files:** `ts-client/src/errors.ts` (`RtDbErrorCode` + `CODES`); `ts-client/src/admin.ts` (`HotConfig` `:96`, `HotConfigPatch` `:118`); test.

- [ ] **Step 1: Failing test** (`ts-client/tests/admin.test.ts` or new `hotconfig.test.ts`):

```ts
import { describe, it, expect } from "vitest";
import type { HotConfig, HotConfigPatch } from "../src/admin";
import { isRtDbErrorCode } from "../src/errors"; // or whatever validates the union

describe("HotConfig quota fields", () => {
  it("carries the three quota fields", () => {
    const hot: HotConfig = {
      allowedOrigins: [], sessionTtlDays: 30, maxFileSize: 100, idempotencyTtlMs: 300000,
      maxTablesPerDb: 10, maxStorageBytesPerDb: 1048576, maxSubsPerDb: 50,
    };
    expect(hot.maxTablesPerDb).toBe(10);
    expect(({ maxSubsPerDb: 5 } as HotConfigPatch).maxSubsPerDb).toBe(5);
  });
  it("accepts QUOTA_EXCEEDED as an error code", () => {
    expect(isRtDbErrorCode("QUOTA_EXCEEDED")).toBe(true);
  });
});
```

- [ ] **Step 2: Run — fails** (`cd ts-client && bunx vitest run tests/admin.test.ts`)

- [ ] **Step 3: Add the error code** — in `ts-client/src/errors.ts`, add `"QUOTA_EXCEEDED"` to the `RtDbErrorCode` union and to the `CODES` set.

- [ ] **Step 4: Add HotConfig fields** — in `ts-client/src/admin.ts`:

```ts
export interface HotConfig {
  allowedOrigins: string[]; sessionTtlDays: number; maxFileSize: number; idempotencyTtlMs: number;
  maxTablesPerDb: number; maxStorageBytesPerDb: number; maxSubsPerDb: number;
}
export interface HotConfigPatch {
  allowedOrigins?: string[]; sessionTtlDays?: number; maxFileSize?: number; idempotencyTtlMs?: number;
  maxTablesPerDb?: number; maxStorageBytesPerDb?: number; maxSubsPerDb?: number;
}
```

- [ ] **Step 5: Run + gate + commit** (`cd ts-client && bunx vitest run && make -C /Users/probello/Repos/par-rt-db typecheck`)
```bash
git add ts-client/src/errors.ts ts-client/src/admin.ts ts-client/tests/
git commit -m "feat(client-ts): mirror QUOTA_EXCEEDED + HotConfig quota fields (ENH-011)"
```

---

## Task 10: rust-client mirror — error code + HotConfig fields

**Files:** `rust-client/src/error.rs` (`ErrorCode` `:6`); `rust-client/src/wire.rs` (`HotConfig` `:726`, `HotConfigPatch` `:756`); test.

- [ ] **Step 1: Failing test** (`rust-client/tests/`):

```rust
#[test]
fn hot_config_round_trips_quota_fields() {
    let json = r#"{"allowedOrigins":[],"sessionTtlDays":30,"maxFileSize":100,"idempotencyTtlMs":300000,"maxTablesPerDb":10,"maxStorageBytesPerDb":1048576,"maxSubsPerDb":50}"#;
    let hot: rtdb_client::HotConfig = serde_json::from_str(json).unwrap();
    assert_eq!(hot.max_tables_per_db, 10);
    assert_eq!(hot.max_subs_per_db, 50);
    assert_eq!(
        serde_json::from_str::<rtdb_client::ErrorCode>("\"QUOTA_EXCEEDED\"").unwrap(),
        rtdb_client::ErrorCode::QuotaExceeded
    );
}
```

- [ ] **Step 2: Run — fails** (`cargo test --manifest-path rust-client/Cargo.toml hot_config_round_trips_quota_fields`)

- [ ] **Step 3: Implement** — in `rust-client/src/error.rs` add `QuotaExceeded,` to `ErrorCode`; in `rust-client/src/wire.rs` add the three fields to `HotConfig` (`max_storage_bytes_per_db: u64`) and the three `Option<…>` mirrors (`#[serde(default, skip_serializing_if = "Option::is_none")]`) to `HotConfigPatch`, matching the existing field style.

- [ ] **Step 4: Run + gate + commit** (`cargo test --manifest-path rust-client/Cargo.toml && make -C /Users/probello/Repos/par-rt-db lint`)
```bash
git add rust-client/src/error.rs rust-client/src/wire.rs rust-client/tests/
git commit -m "feat(client-rust): mirror QUOTA_EXCEEDED + HotConfig quota fields (ENH-011)"
```

---

## Task 11: python-client mirror — error code + HotConfig fields

**Files:** `python-client/src/par_rt_db/errors.py` (`ErrorCode` `:11` + `_STATUS` `:24`); `python-client/src/par_rt_db/http_client.py` (`HotConfig` `:178`, `HotConfigPatch` `:200`); test.

- [ ] **Step 1: Failing test** (`python-client/tests/test_hotconfig.py`):

```python
from par_rt_db.http_client import HotConfig, HotConfigPatch
from par_rt_db.errors import ErrorCode


def test_hot_config_quota_fields():
    hot = HotConfig(
        allowed_origins=[], session_ttl_days=30, max_file_size=100, idempotency_ttl_ms=300000,
        max_tables_per_db=10, max_storage_bytes_per_db=1048576, max_subs_per_db=50,
    )
    assert hot.model_dump(by_alias=True)["maxTablesPerDb"] == 10
    assert HotConfigPatch(max_subs_per_db=5).model_dump(exclude_none=True, by_alias=True) == {"maxSubsPerDb": 5}
    assert ErrorCode.QUOTA_EXCEEDED.value == "QUOTA_EXCEEDED"
```

- [ ] **Step 2: Run — fails** (`cd python-client && uv run pytest -q tests/test_hotconfig.py`)

- [ ] **Step 3: Add the error code + status** — in `errors.py`: add `QUOTA_EXCEEDED = "QUOTA_EXCEEDED"` to the StrEnum; add `ErrorCode.QUOTA_EXCEEDED: 507,` to `_STATUS`.

- [ ] **Step 4: Add HotConfig fields** — in `http_client.py`: add the three fields to `HotConfig` (`max_storage_bytes_per_db: int`) and the three `| None = None` mirrors to `HotConfigPatch`.

- [ ] **Step 5: Run + gate + commit** (`cd python-client && uv run pytest -q && uv run pyright`)
```bash
git add python-client/src/par_rt_db/errors.py python-client/src/par_rt_db/http_client.py python-client/tests/
git commit -m "feat(client-py): mirror QUOTA_EXCEEDED + HotConfig quota fields (ENH-011)"
```

---

## Task 12: Dashboard — types + config panel + usage bar

**Files:** `dashboard/src/lib/types.ts` (`HotConfig` `:118`, `HotConfigPatch` `:226`); the hot-config panel component; the db-stats view.

- [ ] **Step 1: Extend types** — add the three fields to `HotConfig` + optional three to `HotConfigPatch` (same shape as `ts-client/src/admin.ts`).

- [ ] **Step 2: Config panel** — add three number inputs (`maxTablesPerDb`/`maxStorageBytesPerDb`/`maxSubsPerDb`, label `0 = unlimited`) to the `PATCH /admin/config` form, mirroring the existing `maxFileSize` input.

- [ ] **Step 3: Usage bar** — in the db-stats view (consumes `/admin/db/{db}/stats`), render `storageUsedBytes`/`storageQuotaBytes` as "Storage: {used} / {quota}" (hide when quota == 0).

- [ ] **Step 4: Gate + commit** (`make -C /Users/probello/Repos/par-rt-db ts-client-build && make -C /Users/probello/Repos/par-rt-db typecheck`)
```bash
git add dashboard/src/lib/types.ts dashboard/src/
git commit -m "feat(dashboard): quota config inputs + storage usage bar (ENH-011)"
```

---

## Task 13: Docs sync — FEATURE_MATRIX + CLAUDE.md

**Files:** `FEATURE_MATRIX.md`; `CLAUDE.md`.

- [ ] **Step 1: FEATURE_MATRIX** — flip the ENH-011 / per-db-limits row to ✅ with a one-line note (three caps, hot-config, client-mirror status).

- [ ] **Step 2: CLAUDE.md** — add a bullet to "Invariants you must preserve" documenting quotas (three caps in HotConfig; the enforcement points; cached live `pg_total_relation_size` measurement; no admin bypass; the best-effort post-commit refresh). Match surrounding bullet density.

- [ ] **Step 3: Gate + commit** (`make -C /Users/probello/Repos/par-rt-db checkall` — the full gate, definition of done)
```bash
git add FEATURE_MATRIX.md CLAUDE.md
git commit -m "docs(quotas): ENH-011 feature matrix + CLAUDE.md (done)"
```

---

## Self-Review

**Spec coverage:** config (Task 2); tables push+migrate (4); subs (5); storage mutate/scheduled/migrate + upload + delete-db evict + over-quota-at-enable + scheduled-errored (3/6/7); error code 507 (1); no-admin-bypass (Global Constraints + guards in 4/5/6/7); client mirror (9/10/11 + 12); dashboard + metrics (8/3/12); rollout (2 + Global Constraints). ✅

**Key design decisions baked in:** (1) **no admin bypass** — `PrincipalCtx` cannot distinguish admin from machine token at the committer, so all three quotas enforce uniformly; (2) the **tables check lives in the admin handler + `handle_migrate`, NOT `ddl::push_schema`** (which `fresh_db` calls regardless of caps); (3) **storage test seeds a doc first** so it's deterministic regardless of Postgres empty-table accounting; (4) **`handle_scheduled` records the error + returns `Ok`** (the existing failure convention), not `Err`.

**Type/name consistency:** `UsageCache::enforce/current_usage/refresh/evict`, `QuotaKind::{Tables,Storage,Subs}`, `record_quota_rejection`, `count_for_db`, `check_table_quota`, `quota_cache_ttl_secs`, `max_{tables,storage_bytes,subs}_per_db`, wire `maxTablesPerDb`/`maxStorageBytesPerDb`/`maxSubsPerDb` — identical across tasks. `QUOTA_EXCEEDED`/`QuotaExceeded` identical server + 3 clients. `next_conn_id` imported from `rtdb_server::subs`; Query/Insert/upload helpers copied inline from `subs_test.rs`/`storage_test.rs` (they are NOT in `common`).

**One item deferred to the implementer (flagged, not a placeholder):** the exact `MetricsSnapshot`/`render_prometheus` insertion — Task 3 Step 7 cites the exact struct (`:388`), builder fill (`:383`), and `render_prometheus` (`:450`) lines to mirror, and `DbQuotaCounterRow`/`per_db_quota_snapshot` are defined by copying `DbSubCounterRow`/`per_db_subs_snapshot` shape-for-shape. A `cargo build` after Step 7 names any field missed.
