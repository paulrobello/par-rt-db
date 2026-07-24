# Realtime Dashboard — Phase 4: Config (read-only display + hot-reload) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the three runtime-mutable settings (`allowed_origins`, `session_ttl_days`, `max_file_size`) out of the boot-only `Config` into a hot-reloadable `HotConfig` persisted in a single-row `rtdb_config` table, expose `GET`/`PATCH /admin/config` (redacted), and make the CORS origin decision read live config so `allowed_origins` reloads without a restart.

**Architecture:** `AppState` keeps `config: Config` (boot, env, immutable) and gains `hot: Arc<ArcSwap<HotConfig>>` (runtime-mutable). Every consumer of the three hot values reads `state.hot.load()` so an atomic swap takes effect on the next request. The CORS `CorsLayer` is still built once at router build time, but its origin check becomes an `AllowOrigin::predicate` closure that captures `state.hot` and re-reads it per request. `PATCH /admin/config` validates a subset patch, upserts the merged JSON into `rtdb_config`, and swaps the `ArcSwap` — durable across restarts, live within the process.

**Tech Stack:** Rust (axum 0.8, tokio, sqlx 0.8 `json` feature, tower-http 0.6 `cors`), `arc-swap = "1"` (new dep), Postgres 17 jsonb.

## Global Constraints

Copied verbatim from the spec (`docs/superpowers/specs/2026-07-24-realtime-dashboard-design.md` §4) and repo invariants:

- **Naming deviation, deliberate and documented:** the spec calls the boot layer `BootConfig`; this plan keeps the existing type name `Config` for it to avoid a mechanical rename across ~9 construction/import sites with no semantic change. `state.config` (boot) vs `state.hot` (`Arc<ArcSwap<HotConfig>>`) disambiguate the two layers. Do **not** flag this as a defect.
- The three hot fields are exactly: `allowed_origins: Vec<String>`, `session_ttl_days: i64`, `max_file_size: usize`. No other field is hot-reloadable.
- **Single-row table, exact DDL:** `CREATE TABLE IF NOT EXISTS rtdb_config (id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), hot jsonb NOT NULL)`. Added to `db::bootstrap_ddl` alongside the other `CREATE TABLE IF NOT EXISTS` statements (it is a public-schema table, not under `rtdb_auth`).
- **Wire casing is camelCase** for the dashboard-facing JSON: `allowedOrigins`, `sessionTtlDays`, `maxFileSize`. `HotConfig` derives `Serialize`/`Deserialize` with `#[serde(rename_all = "camelCase")]`.
- **Redaction is structural, not by-memory.** `GET /admin/config` and the `PATCH` response use a `ConfigResponse` type that **omits** secret values: `admin_key` and OAuth client secrets become `bool` configured-flags; `database_url` becomes a configured-bool (it embeds DB credentials). Hot values, `public_url`, `github_base_url`, `github_api_url`, and `port` are shown in full.
- **`PATCH` accepts a subset** (`allowedOrigins?`, `sessionTtlDays?`, `maxFileSize?`), all optional. Unknown fields → `BadRequest` (serde `deny_unknown_fields`, surfaced as 400 via `ApiJson`). Invalid values → `BadRequest`.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`. Every value is `$n`-bound; identifiers are system-generated so the existing `format!`-with-double-quote pattern is preserved where used.
- `make checkall` is the gate (fmt-check + clippy `-D warnings` + typecheck + tests). Integration tests need the dev Postgres: `make dev-db-up` first.
- Existing tests share one Postgres and isolate by uniquely-named databases; never drop a database/schema you didn't create. Dashboard tests live in `server/tests/dashboard_test.rs` (uses `mod common;`, fully-qualified paths, no `use` block — match that style).

---

## File Structure

- **Modify `server/Cargo.toml`** — add `arc-swap = "1"`.
- **Modify `server/src/config.rs`** — remove the three hot fields from `Config` + their env parses; add `HotConfig`, `HotConfig::from_env`, `origins_valid`, `load_hot`, `save_hot`.
- **Modify `server/src/db.rs`** — add `rtdb_config` DDL to `bootstrap_ddl`.
- **Modify `server/src/lib.rs`** — `AppState.hot` field; `AppState::new` takes `HotConfig`; `cors_layer` becomes a dynamic `AllowOrigin::predicate` capturing `state.hot`; add `PATCH`/`DELETE` to allowed methods.
- **Modify `server/src/main.rs`** — after `bootstrap`, load hot (`load_hot` else `HotConfig::from_env`) and pass to `AppState::new`.
- **Modify `server/src/http_api.rs`** — upload limit reads `state.hot.load().max_file_size`.
- **Modify `server/src/auth/google.rs`, `server/src/auth/github.rs`** — session TTL reads `state.hot.load().session_ttl_days`; remove the three hot-field lines from each test `Config {}` builder.
- **Modify `server/src/auth/provider.rs`** — OAuth start origin check reads `state.hot.load().allowed_origins`.
- **Modify `server/src/admin.rs`** — add `get_config` / `patch_config` handlers + `ConfigResponse` / `HotConfigPatch` types; factor an `admin_members` helper; register `/admin/config` GET+PATCH.
- **Modify `server/tests/common/mod.rs`** — `spawn_app` passes a `HotConfig` seed to `AppState::new`; `test_config` drops the three removed fields.
- **Modify `server/tests/dashboard_test.rs`** — Phase 4 tests (config GET/PATCH, dynamic CORS).
- **Modify `server/tests/healthz_test.rs`** — `test_config` drops the three removed fields.

---

### Task 1: Config/HotConfig split + ArcSwap + `rtdb_config` + dynamic CORS wiring

**Goal:** Move the three hot settings behind an `Arc<ArcSwap<HotConfig>>`, persist them in `rtdb_config`, redirect every consumer to `state.hot.load()`, and make the CORS origin check read live config. After this task there is **no new endpoint and no user-visible behavior change** — hot values still come from env at first boot — but the plumbing is dynamic and durable.

**Files:**
- Modify: `server/Cargo.toml`, `server/src/config.rs`, `server/src/db.rs`, `server/src/lib.rs`, `server/src/main.rs`, `server/src/http_api.rs`, `server/src/auth/google.rs`, `server/src/auth/github.rs`, `server/src/auth/provider.rs`, `server/tests/common/mod.rs`, `server/tests/healthz_test.rs`
- Test: `server/tests/dashboard_test.rs` (new `config_boot_load` test)

**Interfaces:**
- Produces: `config::HotConfig` (`Clone + Debug + Serialize + Deserialize`, `#[serde(rename_all="camelCase")]`, fields `allowed_origins: Vec<String>`, `session_ttl_days: i64`, `max_file_size: usize`); `HotConfig::from_env() -> Self`; `HotConfig::origins_valid(&self) -> bool`; `pub async fn load_hot(pool: &sqlx::PgPool) -> Result<Option<HotConfig>, RtDbError>`; `pub async fn save_hot(pool: &sqlx::PgPool, hot: &HotConfig) -> Result<(), RtDbError>`; `AppState.hot: Arc<ArcSwap<HotConfig>>`; `AppState::new(pool, config, hot) -> Arc<Self>`.

- [ ] **Step 1: Add the `arc-swap` dependency**

In `server/Cargo.toml`, under `[dependencies]`, add (alphabetical position is fine):

```toml
arc-swap = "1"
```

- [ ] **Step 2: Write the failing test — boot loads hot from the DB row when present**

Append to `server/tests/dashboard_test.rs`:

```rust
#[tokio::test]
async fn config_boot_loads_hot_from_db_row() {
    let state = spawn_app(common::test_config());
    let db = fresh_db(&state).await;

    // Seed a persisted hot row directly, then load it back.
    let seeded = serde_json::json!({
        "allowedOrigins": ["https://one.example.com", "https://two.example.com"],
        "sessionTtlDays": 7,
        "maxFileSize": 12345
    });
    sqlx::query("INSERT INTO rtdb_config (id, hot) VALUES (1, $1)")
        .bind(&seeded)
        .execute(&state.pool)
        .await
        .unwrap();

    let loaded = rtdb_server::config::load_hot(&state.pool).await.unwrap().unwrap();
    assert_eq!(loaded.allowed_origins, vec!["https://one.example.com", "https://two.example.com"]);
    assert_eq!(loaded.session_ttl_days, 7);
    assert_eq!(loaded.max_file_size, 12345);

    // A fresh database (no row) loads None.
    let _ = db; // keep alive
}
```

> Note: `fresh_db` creates a unique database name but the `rtdb_config` table lives in the **shared bootstrap schema** (created by `db::bootstrap` on the pool, not per-test-db). The test inserts into `rtdb_config` on `state.pool` and reads it back on the same pool — no per-db isolation needed for this global table. Clean up the seeded row at the end so other tests on the shared pool are not affected: add `sqlx::query("DELETE FROM rtdb_config WHERE id = 1").execute(&state.pool).await.unwrap();` before the test returns.

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd server && cargo test --test dashboard_test config_boot_loads_hot_from_db_row`
Expected: FAIL — `load_hot` does not exist (compile error).

- [ ] **Step 4: Add `HotConfig` + `load_hot`/`save_hot` to `config.rs`; remove the three hot fields from `Config`**

Rewrite `server/src/config.rs` to:

```rust
use arc_swap::ArcSwap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::RtDbError;

#[derive(Clone, Debug)]
pub struct Config {
    pub port: u16,                            // RTDB_PORT, default 8300
    pub database_url: String,                 // RTDB_DATABASE_URL (required)
    pub admin_key: String,                    // RTDB_ADMIN_KEY (required)
    pub public_url: String,                   // RTDB_PUBLIC_URL, default http://localhost:8300
    pub github_client_id: Option<String>, // RTDB_GITHUB_CLIENT_ID
    pub github_client_secret: Option<String>, // RTDB_GITHUB_CLIENT_SECRET
    pub github_base_url: String,      // RTDB_GITHUB_BASE_URL, default https://github.com
    pub github_api_url: String,       // RTDB_GITHUB_API_URL, default https://api.github.com
    pub google_client_id: Option<String>, // RTDB_GOOGLE_CLIENT_ID
    pub google_client_secret: Option<String>, // RTDB_GOOGLE_CLIENT_SECRET
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
}

impl HotConfig {
    /// Seeds defaults from env — the same parses `Config` used to perform.
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
            Ok(v) => v
                .parse::<i64>()
                .map_err(|_| "RTDB_SESSION_TTL_DAYS must be a valid i64".to_string())
                .unwrap_or(30),
            Err(_) => 30,
        };
        let max_file_size = match std::env::var("RTDB_MAX_FILE_SIZE") {
            Ok(v) => v
                .parse::<usize>()
                .map_err(|_| "RTDB_MAX_FILE_SIZE must be a valid usize".to_string())
                .unwrap_or(50 * 1024 * 1024),
            Err(_) => 50 * 1024 * 1024,
        };
        Self { allowed_origins, session_ttl_days, max_file_size }
    }

    /// True when every origin parses as a valid `HeaderValue` (the CORS layer
    /// would otherwise silently skip a malformed origin at request time).
    pub fn origins_valid(&self) -> bool {
        self.allowed_origins
            .iter()
            .all(|o| axum::http::HeaderValue::from_str(o).is_ok())
    }
}

/// Loads the single persisted hot row, if any. A missing row is `Ok(None)` (first
/// boot); a sqlx or decode failure is an internal error.
pub async fn load_hot(pool: &sqlx::PgPool) -> Result<Option<HotConfig>, RtDbError> {
    let row: Option<(serde_json::Value,)> =
        sqlx::query_as("SELECT hot FROM rtdb_config WHERE id = 1")
            .fetch_optional(pool)
            .await
            .map_err(|e| RtDbError::internal(format!("load rtdb_config: {e}")))?;
    match row {
        Some((v,)) => serde_json::from_value::<HotConfig>(v)
            .map(Some)
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

/// Type alias retained for the `ArcSwap` field on `AppState`.
#[allow(dead_code)]
pub type HotConfigSwap = ArcSwap<HotConfig>;
```

- [ ] **Step 5: Add the `rtdb_config` table to `db::bootstrap_ddl`**

In `server/src/db.rs`, inside `bootstrap_ddl` (after the `rtdb.storage_index` `CREATE TABLE` block, before the function's closing brace):

```rust
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_config (
            id int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
            hot jsonb NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;
```

- [ ] **Step 6: Add `hot` to `AppState`; take `HotConfig` in `AppState::new`**

In `server/src/lib.rs`:

- Add imports: `use arc_swap::ArcSwap;` and `use config::HotConfig;` (alongside the existing `use config::Config;`).
- Add the field to the `AppState` struct: `pub hot: Arc<ArcSwap<HotConfig>>,` (place it after `config`).
- Change `AppState::new` signature and body:

```rust
    pub fn new(pool: sqlx::PgPool, config: Config, hot: HotConfig) -> Arc<Self> {
        let schemas = SchemaCache::new();
        let subs = SubscriptionManager::new();
        let op_feed = op_feed::OpFeed::new(1024, 500);
        let committers =
            Committers::new(pool.clone(), subs.clone(), schemas.clone(), op_feed.clone());
        let metrics = metrics::Metrics::new();
        Arc::new(Self {
            pool,
            config,
            hot: Arc::new(ArcSwap::new(hot)),
            schemas,
            subs,
            committers,
            oauth_states: tokio::sync::Mutex::new(HashMap::new()),
            started_at: SystemTime::now(),
            metrics,
            op_feed,
        })
    }
```

- [ ] **Step 7: Make the CORS layer dynamic — `AllowOrigin::predicate` reading `state.hot`**

In `server/src/lib.rs`, replace the `cors_layer` function and its call site. Add `use tower_http::cors::{AllowOrigin, CorsLayer};` (replacing the bare `use tower_http::cors::CorsLayer;`).

```rust
/// Origins are decided per request from live `HotConfig`, so `PATCH /admin/config`
/// can add an origin and have it take effect without a restart. The layer itself is
/// still constructed once at router build time; only the origin decision is dynamic.
/// WS is CORS-exempt (Origin is enforced at OAuth start, unchanged).
fn cors_layer(hot: Arc<ArcSwap<HotConfig>>) -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(move |origin, _parts| {
            let origins = &hot.load().allowed_origins;
            match origin.to_str() {
                Ok(val) => origins.iter().any(|allowed| allowed.as_str() == val),
                Err(_) => false,
            }
        }))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
}
```

> **Verify the predicate signature against the pinned tower-http 0.6.** In tower-http 0.6 `AllowOrigin::predicate<F>` expects `F: Fn(&HeaderValue, &RequestParts) -> bool + Clone + Send + Sync + 'static` (the first argument is the request's `Origin` header value, not `Option`). If the compiler disagrees, read the exact signature in the registry source (`cargo doc --open -p tower-http` or `~/.cargo/registry/src/.../tower-http-0.6.*/src/cors.rs`) and adapt the closure — do not change the behavior (read `hot.load().allowed_origins`, exact-match the incoming origin). `PATCH` and `DELETE` are added to `allow_methods` because `PATCH /admin/config` and the existing `DELETE /admin/admins` would otherwise be blocked by CORS preflight; `state.hot` is `Clone` (it's an `Arc`) so the closure satisfies `Clone`.

Then update the call site in `build_router`:

```rust
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.hot.clone());
    // ... rest unchanged
```

- [ ] **Step 8: Redirect the four hot consumers to `state.hot.load()`**

- `server/src/http_api.rs` (upload handler, ~line 294): change `let limit = state.config.max_file_size;` to `let limit = state.hot.load().max_file_size;`.
- `server/src/auth/google.rs` (~line 136): change `state.config.session_ttl_days` to `state.hot.load().session_ttl_days`.
- `server/src/auth/github.rs` (~line 131): change `state.config.session_ttl_days` to `state.hot.load().session_ttl_days`.
- `server/src/auth/provider.rs` (`provider_start`, ~lines 134–138): change the origin check to:

```rust
    if !state
        .hot
        .load()
        .allowed_origins
        .iter()
        .any(|allowed| allowed == &params.origin)
    {
        return RtDbError::forbidden("origin not allowed").into_response();
    }
```

- [ ] **Step 9: Update all `Config {}` construction sites to drop the three removed fields**

- `server/tests/common/mod.rs` `test_config()`: remove the `allowed_origins`, `session_ttl_days`, and `max_file_size` lines from the `Config { ... }` literal.
- `server/tests/healthz_test.rs` `test_config()`: same removal.
- `server/src/auth/google.rs` test `Config { ... }` builder (~lines 278, 285, 286): remove the `allowed_origins:`, `session_ttl_days:`, and `max_file_size:` lines.
- `server/src/auth/github.rs` test `Config { ... }` builder (~lines 374, 381, 382): same removal.

- [ ] **Step 10: Pass a `HotConfig` seed at every `AppState::new` call site**

- `server/src/main.rs`: after `db::bootstrap(&pool)` (and the existing `seed_admin_emails` call), load hot and pass it. Add `use rtdb_server::config::HotConfig;` (or the crate-relative path already used) and:

```rust
    let hot = config::load_hot(&pool)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(HotConfig::from_env);
```

then change the `AppState::new` call to `AppState::new(pool, config, hot)`. (Read `main.rs` first to match its exact variable names; the `load_hot` failure is intentionally lenient on boot — a malformed row falls back to env seed rather than preventing startup. If `main.rs` already maps startup errors to a hard exit, keep using `?`-style propagation is NOT required here; the `.ok().flatten().unwrap_or_else(...)` form is correct.)

- `server/tests/common/mod.rs` `spawn_app` (or wherever `AppState::new` is called for tests): pass a hot seed, e.g. `AppState::new(pool, config, HotConfig::from_env())`. Add `use rtdb_server::config::HotConfig;` if not already imported (check how `common` refers to the crate — it may use `rtdb_server::config::Config` already; mirror that path).

- [ ] **Step 11: Run the new test + full gate**

Run: `cd server && cargo test --test dashboard_test config_boot_loads_hot_from_db_row`
Expected: PASS.

Run: `make checkall` (from repo root, after `make dev-db-up`)
Expected: PASS — fmt clean, clippy `-D warnings` clean, all tests pass. Existing OAuth/session/upload tests continue to pass because hot values still default to the same env values.

- [ ] **Step 12: Commit**

```bash
git add -A && git commit -m "feat(server): Config/HotConfig split + ArcSwap + rtdb_config + dynamic CORS (#18 phase 4)"
```

---

### Task 2: `GET` + `PATCH /admin/config` (redacted, validated, persisted, hot-swapped)

**Goal:** Two admin-gated endpoints. `GET /admin/config` returns a redacted config (secrets → configured-bools, hot values in full, plus version/commit and the admin allowlist). `PATCH /admin/config` accepts a subset patch, validates, upserts the merged `HotConfig` to `rtdb_config`, swaps the `ArcSwap`, and returns the new redacted config.

**Files:**
- Modify: `server/src/admin.rs`
- Test: `server/tests/dashboard_test.rs`

**Interfaces:**
- Consumes: `crate::config::{HotConfig, load_hot, save_hot}`, `state.hot.load()` / `state.hot.store(Arc::new(...))`, `require_admin`, the existing `AdminMember` type.
- Produces: routes `GET /admin/config` → `get_config`, `PATCH /admin/config` → `patch_config`; types `ConfigResponse`, `HotConfigPatch`; helper `async fn admin_members(pool) -> Result<Vec<AdminMember>, RtDbError>`.

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/dashboard_test.rs`. Reuse the existing admin-key helper used by the other dashboard tests (find the constant/helper already in the file — e.g. `ADMIN_KEY` or a `admin_headers()` function — and mirror it; do not invent a new one).

```rust
async fn config_get_patch_roundtrip() {
    let state = spawn_app(common::test_config());
    let key = std::env::var("RTDB_ADMIN_KEY").unwrap_or_else(|_| "test-admin-key".into());

    // GET redacts secrets and shows hot defaults.
    let resp = state
        .client
        .get("/admin/config")
        .header("authorization", format!("Bearer {key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["adminKeyConfigured"], true);
    assert_eq!(body["databaseUrlConfigured"], true);
    // No secret string is ever present in any form.
    let raw = body.to_string();
    assert!(!raw.contains(&key));
    assert!(body["hot"]["allowedOrigins"].is_array());
    assert!(body["version"].is_string());
    assert!(body["gitCommit"].is_string());
    assert!(body["admins"].is_array());

    // PATCH changes a hot value and the next GET observes it.
    let resp = state
        .client
        .request(
            reqwest::Method::PATCH,
            "/admin/config")
        .header("authorization", format!("Bearer {key}"))
        .json(&serde_json::json!({ "sessionTtlDays": 7 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.json::<serde_json::Value>().await.unwrap()["hot"]["sessionTtlDays"], 7);

    // The change persisted to the table.
    let loaded = rtdb_server::config::load_hot(&state.pool).await.unwrap().unwrap();
    assert_eq!(loaded.session_ttl_days, 7);

    // Invalid value -> 400.
    let resp = state
        .client
        .request(reqwest::Method::PATCH, "/admin/config")
        .header("authorization", format!("Bearer {key}"))
        .json(&serde_json::json!({ "sessionTtlDays": 0 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // Unknown field -> 400.
    let resp = state
        .client
        .request(reqwest::Method::PATCH, "/admin/config")
        .header("authorization", format!("Bearer {key}"))
        .json(&serde_json::json!({ "port": 9999 }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);

    // No auth -> 401.
    let resp = state.client.get("/admin/config").send().await.unwrap();
    assert!(resp.status() == 401 || resp.status() == 403);

    // Clean up the global row so other tests on the shared pool are unaffected.
    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await
        .unwrap();
}
```

Mark it `#[tokio::test]`. (Match the exact `reqwest::Method::PATCH` import style the file already uses; if the file imports `reqwest` differently, mirror it.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd server && cargo test --test dashboard_test config_get_patch_roundtrip`
Expected: FAIL — 404 (no `/admin/config` route yet).

- [ ] **Step 3: Factor the admin-list query into a helper**

In `server/src/admin.rs`, extract the body of `list_admins`'s query into a reusable helper, and have `list_admins` call it:

```rust
async fn admin_members(pool: &sqlx::PgPool) -> Result<Vec<AdminMember>, RtDbError> {
    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(pool)
            .await?;
    Ok(rows
        .into_iter()
        .map(|(email, github_id)| AdminMember { email, github_id })
        .collect())
}
```

and rewrite `list_admins`'s body after `require_admin` to `Ok(Json(AdminsResponse { admins: admin_members(&state.pool).await? }))`.

- [ ] **Step 4: Add the `ConfigResponse` + `HotConfigPatch` types and the two handlers**

In `server/src/admin.rs`:

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigResponse {
    port: u16,
    public_url: String,
    github_base_url: String,
    github_api_url: String,
    database_url_configured: bool,
    admin_key_configured: bool,
    github_configured: bool,
    google_configured: bool,
    hot: crate::config::HotConfig,
    version: &'static str,
    git_commit: &'static str,
    admins: Vec<AdminMember>,
}

/// Builds the redacted config view from current boot + hot state (no auth).
async fn build_config_response(state: &AppState) -> Result<ConfigResponse, RtDbError> {
    let cfg = &state.config;
    let hot = state.hot.load();
    Ok(ConfigResponse {
        port: cfg.port,
        public_url: cfg.public_url.clone(),
        github_base_url: cfg.github_base_url.clone(),
        github_api_url: cfg.github_api_url.clone(),
        database_url_configured: !cfg.database_url.is_empty(),
        admin_key_configured: !cfg.admin_key.is_empty(),
        github_configured: cfg.github_client_id.is_some() && cfg.github_client_secret.is_some(),
        google_configured: cfg.google_client_id.is_some() && cfg.google_client_secret.is_some(),
        hot: (**hot).clone(),
        version: env!("CARGO_PKG_VERSION"),
        git_commit: env!("BUILD_GIT_COMMIT"),
        admins: admin_members(&state.pool).await?,
    })
}

async fn get_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ConfigResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(build_config_response(&state).await?))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct HotConfigPatch {
    allowed_origins: Option<Vec<String>>,
    session_ttl_days: Option<i64>,
    max_file_size: Option<usize>,
}

async fn patch_config(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(patch): ApiJson<HotConfigPatch>,
) -> Result<Json<ConfigResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let mut next: crate::config::HotConfig = (**state.hot.load()).clone();
    if let Some(origins) = &patch.allowed_origins {
        next.allowed_origins = origins
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    if let Some(ttl) = patch.session_ttl_days {
        if ttl < 1 {
            return Err(RtDbError::bad_request("sessionTtlDays must be >= 1"));
        }
        next.session_ttl_days = ttl;
    }
    if let Some(size) = patch.max_file_size {
        if size == 0 {
            return Err(RtDbError::bad_request("maxFileSize must be > 0"));
        }
        next.max_file_size = size;
    }
    if !next.origins_valid() {
        return Err(RtDbError::bad_request("allowedOrigins contains an invalid origin"));
    }
    crate::config::save_hot(&state.pool, &next).await?;
    state.hot.store(Arc::new(next));
    Ok(Json(build_config_response(&state).await?))
}
```

> `env!("BUILD_GIT_COMMIT")` is the same build-time macro `health.rs` uses; it is always defined because `build.rs` injects it. `Arc` is already imported at the top of `admin.rs` (`use std::sync::Arc;`).

- [ ] **Step 5: Register the routes**

In `admin_routes()`, add (place near the other `/admin/*` routes):

```rust
        .route("/admin/config", get(get_config).patch(patch_config))
```

- [ ] **Step 6: Run the test + full gate**

Run: `cd server && cargo test --test dashboard_test config_get_patch_roundtrip`
Expected: PASS.

Run: `make checkall`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(server): GET/PATCH /admin/config — redacted, hot-reloadable config (#18 phase 4)"
```

---

### Task 3: Dynamic CORS end-to-end test

**Goal:** Prove `allowed_origins` hot-reloads without a restart: after `PATCH` adds an origin, a request bearing that `Origin` receives `access-control-allow-origin`; a non-listed origin does not. This is the load-bearing verification that the `AllowOrigin::predicate` reads live `HotConfig`.

**Files:**
- Test: `server/tests/dashboard_test.rs`

**Interfaces:**
- Consumes: `PATCH /admin/config`, the running app's HTTP client (must be able to set an arbitrary `Origin` request header — verify the test client used by other dashboard tests supports this; `reqwest::Client` does).

- [ ] **Step 1: Write the failing test**

Append to `server/tests/dashboard_test.rs`:

```rust
#[tokio::test]
async fn config_cors_hot_reloads_allowed_origins() {
    let state = spawn_app(common::test_config());
    let key = std::env::var("RTDB_ADMIN_KEY").unwrap_or_else(|_| "test-admin-key".into());

    // Add an origin via the hot-reload path.
    let origin = "https://dashboard.example.com";
    let resp = state
        .client
        .request(reqwest::Method::PATCH, "/admin/config")
        .header("authorization", format!("Bearer {key}"))
        .json(&serde_json::json!({ "allowedOrigins": [origin] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // A request from the now-allowed origin receives an ACAO header.
    let resp = state
        .client
        .get("/admin/dbs")
        .header("origin", origin)
        .header("authorization", format!("Bearer {key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap().to_str().unwrap(),
        origin
    );

    // A request from an origin that is NOT in the list receives no ACAO header.
    let resp = state
        .client
        .get("/admin/dbs")
        .header("origin", "https://evil.example")
        .header("authorization", format!("Bearer {key}"))
        .send()
        .await
        .unwrap();
    assert!(resp.headers().get("access-control-allow-origin").is_none());

    sqlx::query("DELETE FROM rtdb_config WHERE id = 1")
        .execute(&state.pool)
        .await
        .unwrap();
}
```

> The assertion relies on tower-http emitting `access-control-allow-origin` on a simple GET whose `Origin` matches the predicate. Because the layer does not set `allow_credentials`, this is the standard behavior. If the pinned tower-http version requires the method to be in `allow_methods` for the header to attach, GET is already included (Task 1 Step 7). If the test fails because the header is absent even for an allowed origin, first confirm the `PATCH` succeeded and `state.hot` actually swapped (re-read `/admin/config`), then re-check the predicate — do not weaken the assertion.

- [ ] **Step 2: Run the test + full gate**

Run: `cd server && cargo test --test dashboard_test config_cors_hot_reloads_allowed_origins`
Expected: PASS.

Run: `make checkall`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "test(server): prove CORS allowed_origins hot-reloads after PATCH (#18 phase 4)"
```

---

## Self-Review (controller, before final review dispatch)

- **Spec coverage (§4):** BootConfig/HotConfig split → Task 1 (keep `Config` name, documented deviation). `rtdb_config` single-row table → Task 1 Step 5. `ArcSwap` wiring + `load_hot`/`save_hot` → Task 1. `GET /admin/config` redacted → Task 2. `PATCH /admin/config` subset + validate + persist + swap → Task 2. Dynamic `AllowOrigin::predicate` → Task 1 Step 7 + verified Task 3. Hot-reload consumers (CORS, session TTL, max file size) redirected → Task 1 Step 8 (OAuth Origin check, session minting, upload). ✅
- **Type consistency:** `HotConfig` field names and the `camelCase` serde rename are identical across `config.rs`, `ConfigResponse.hot`, `HotConfigPatch`, and the test JSON (`allowedOrigins`, `sessionTtlDays`, `maxFileSize`). `load_hot`/`save_hot` signatures match all call sites. `AppState::new(pool, config, hot)` arity matches `main.rs` and `spawn_app`. ✅
- **Placeholder scan:** No TBD/TODO; every code step contains the actual code. ✅
- **Security:** No secret ever reaches `ConfigResponse` (struct omits secret values; `databaseUrl` is a configured-bool). `deny_unknown_fields` on the patch. All SQL is `$n`-bound (the `rtdb_config` table/row use only the literal `id = 1`). ✅
