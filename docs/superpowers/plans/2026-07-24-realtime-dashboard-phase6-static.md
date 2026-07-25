# Realtime Dashboard — Phase 6: Static hosting (ServeDir) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the dashboard SPA from a configurable on-disk directory (`RTDB_STATIC_DIR`) as a same-origin fallback mounted after all API/admin/WS/auth routes, with correct cache headers — so dropping new build artifacts into the folder updates the frontend with no recompile, no restart, and no `allowed_origins` entry.

**Architecture:** `tower-http::services::ServeDir::new(dir).fallback(ServeFile::new(dir/index.html))` is mounted as the router's `fallback_service` (runs only for paths no registered route matches — so it can never shadow `/healthz`, `/api/*`, `/admin/*`, `/sync`, `/auth/*`). A small `axum::middleware::from_fn` layer wraps the ServeDir and sets `Cache-Control` from the **response** `Content-Type`: `text/html` (the index + SPA-fallback) → `no-cache`; everything else (hashed JS/CSS/assets) → `public, max-age=31536000, immutable`. `RTDB_STATIC_DIR` is a boot `Config` field (`Option<String>`); unset, empty, or a non-existent dir ⇒ the server runs API-only (today's behavior).

**Tech Stack:** Rust (axum 0.8, tower-http 0.6 `ServeDir`/`ServeFile`, `axum::middleware::from_fn`).

## Global Constraints

From the spec (`docs/superpowers/specs/2026-07-24-realtime-dashboard-design.md` §6) and repo invariants:

- **Static serving never shadows a real route.** `ServeDir` is the router's `fallback_service`, added LAST (after `/healthz`, `admin_routes`, `http_api_routes`, `ws_routes`, `auth_routes`). Registered routes keep returning their own JSON/404s.
- **SPA fallback.** Unknown `GET` paths fall through to `index.html` (so client-side routes like `/dbs/foo` deep-link/refresh correctly). Use `ServeDir::fallback(ServeFile::new(index))` (200), NOT `not_found_service` (which sets 404). `append_index_html_on_directories` is the default `true` (so `/` → `index.html`).
- **Cache headers, keyed off response Content-Type** (robust to the SPA fallback serving index.html at arbitrary paths): `text/html` → `no-cache` (so a new deploy's index.html is always fetched, which then references the newest hashed assets); any other Content-Type → `public, max-age=31536000, immutable` (safe because Vite-style hashed assets never change content for a given path). The cache layer wraps ONLY the static `ServeDir`, never the API/admin/WS/auth routes.
- **`RTDB_STATIC_DIR` is a boot `Config` field**, `Option<String>`, env `RTDB_STATIC_DIR` (empty/unset → `None`). The fallback is added only when `Some(dir)` AND the dir exists (`Path::is_dir`); otherwise API-only (no change to today). Same-origin ⇒ the dashboard's API/WS calls need no `allowed_origins` entry.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`. No new second writer, no auth change — this phase touches only `config.rs`, `lib.rs`, and tests.
- `make checkall` is the gate (dev Postgres must be up). Tests share one Postgres and isolate by uniquely-named databases; never drop a db/schema you didn't create. Temp files for the static test go in `std::env::temp_dir()` under a unique name and are removed at the end (no new dev-dependency — use `std::fs`).

---

## File Structure

- **Modify `server/src/config.rs`** — add `static_dir: Option<String>` (+ `RTDB_STATIC_DIR` env parse).
- **Modify `server/src/lib.rs`** — `build_router` adds the cached `ServeDir` fallback when `static_dir` is set + exists; add the `set_cache_headers` middleware + imports.
- **Modify the 4 `Config {}` test builders** (`tests/common/mod.rs`, `tests/healthz_test.rs`, `auth/google.rs`, `auth/github.rs`) — add `static_dir: None`.
- **Modify `server/tests/dashboard_test.rs`** — Phase 6 tests (static serving + no-static API-only).

---

### Task 1: `ServeDir` fallback + `RTDB_STATIC_DIR` + cache headers

**Files:** `server/src/config.rs`, `server/src/lib.rs`, the 4 `Config {}` test builders, `server/tests/dashboard_test.rs`.

**Interfaces:**
- Produces: `Config.static_dir: Option<String>`; the router gains a cached `ServeDir` `fallback_service` when configured.
- Consumes: `tower_http::services::{ServeDir, ServeFile}`, `axum::middleware::{from_fn, Next}`, `axum::extract::Request`, `axum::response::Response`, `axum::http::{HeaderValue, header}`.

- [ ] **Step 1: Add `static_dir` to `Config`**

In `server/src/config.rs`, add the field after `max_affected_docs`:

```rust
    pub max_affected_docs: usize, // RTDB_MAX_AFFECTED_DOCS, default 100 (admin data-browser guardrail)
    pub static_dir: Option<String>, // RTDB_STATIC_DIR — unset/empty ⇒ API-only (no SPA served)
```

and parse it in `from_env` (add near the other env reads, and to the `Ok(Self { … })` literal):

```rust
        let static_dir = std::env::var("RTDB_STATIC_DIR")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
```

```rust
            max_affected_docs,
            static_dir,
```

- [ ] **Step 2: Add `static_dir: None` to the 4 test `Config {}` builders**

Add `static_dir: None,` to the `Config { … }` literal in: `tests/common/mod.rs` (`test_config`), `tests/healthz_test.rs` (`test_config`), `auth/google.rs` (`from_config_returns_none_without_credentials`), `auth/github.rs` (same).

- [ ] **Step 3: Write the failing tests**

Append to `server/tests/dashboard_test.rs`. These build a custom `AppState` with `static_dir` set to a temp dir (mirroring how the Phase 5 cap test builds its own state), and write `index.html` + an asset with `std::fs`:

```rust
// --- Phase 6: static hosting -------------------------------------------------

// ServeDir serves the SPA same-origin: index.html (no-cache), hashed assets
// (immutable), and index.html again for unknown GET paths (SPA fallback).
#[tokio::test]
async fn static_dir_serves_index_and_assets() -> anyhow::Result<()> {
    let dir = std::env::temp_dir().join(format!("rtdb-static-{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(dir.join("assets"))?;
    std::fs::write(dir.join("index.html"), "<html><body>dashboard</body></html>")?;
    std::fs::write(dir.join("assets").join("app.js"), "console.log(1)")?;

    let mut cfg = common::test_config();
    cfg.static_dir = Some(dir.to_string_lossy().to_string());
    let pool = sqlx::PgPool::connect(&cfg.database_url).await?;
    rtdb_server::db::bootstrap(&pool).await?;
    let state = rtdb_server::AppState::new(pool, cfg, common::test_hot());
    let addr = common::spawn_app(state).await;

    // GET / -> index.html, no-cache.
    let resp = reqwest::get(format!("http://{addr}/")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("cache-control").unwrap().to_str().unwrap(),
        "no-cache, no-store, must-revalidate"
    );
    assert!(resp.text().await?.contains("dashboard"));

    // GET /assets/app.js -> immutable.
    let resp = reqwest::get(format!("http://{addr}/assets/app.js")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("cache-control").unwrap().to_str().unwrap(),
        "public, max-age=31536000, immutable"
    );

    // SPA fallback: an unknown GET path returns index.html (no-cache), not a 404.
    let resp = reqwest::get(format!("http://{addr}/dbs/some-db/tables")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.headers().get("cache-control").unwrap().to_str().unwrap(),
        "no-cache, no-store, must-revalidate"
    );
    assert!(resp.text().await?.contains("dashboard"));

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}

// With no RTDB_STATIC_DIR, the server is API-only: GET / is a 404 (not index.html)
// and the API routes are unaffected.
#[tokio::test]
async fn no_static_dir_is_api_only() -> anyhow::Result<()> {
    let state = common::test_state().await; // static_dir = None
    let addr = common::spawn_app(state.clone()).await;

    let resp = reqwest::get(format!("http://{addr}/")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // API still works.
    let resp = common::admin_get(addr, "/admin/dbs").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
```

- [ ] **Step 4: Run the tests to verify they fail**

`cd server && cargo test --test dashboard_test static_dir_serves no_static_dir` → the first FAILs (no static serving; GET / is 404), the second PASSes already (no static_dir → 404 is today's behavior).

- [ ] **Step 5: Add the cache-header middleware and the cached ServeDir fallback to `build_router`**

In `server/src/lib.rs`, add imports (alongside the existing `axum`/`tower_http` uses):

```rust
use std::path::Path;

use axum::extract::Request;
use axum::middleware::{Next, from_fn};
use axum::response::Response;
use axum::http::{HeaderValue, header};
use tower_http::services::{ServeDir, ServeFile};
```

(`Method`, `AllowOrigin`, `ArcSwap`, `Config`, `HotConfig` etc. are already imported — keep them.)

Add the cache middleware (module-level fn):

```rust
/// Sets `Cache-Control` on static responses from their Content-Type: the SPA
/// shell (text/html — including the index served for unknown paths by the SPA
/// fallback) is `no-cache` so a new deploy's index.html is always fetched; all
/// other static assets (hashed JS/CSS/etc.) are `immutable`. Wraps only the
/// static `ServeDir`, never the API/admin/WS/auth routes.
async fn set_static_cache_headers(req: Request, next: Next) -> Response {
    let resp = next.run(req).await;
    let no_cache = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("text/html"));
    let cc = if no_cache {
        "no-cache, no-store, must-revalidate"
    } else {
        "public, max-age=31536000, immutable"
    };
    if let Ok(value) = HeaderValue::from_str(cc) {
        resp.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    resp
}

/// The static SPA service: files from `dir`, with `index.html` as the fallback
/// for unknown GET paths (client-side routing). Layered with the Content-Type
/// cache policy above.
fn static_service(dir: &str) -> tower::ServiceBuilder<
    tower::layer::util::Stack<axum::middleware::FromFnLayer<fn(Request, Next) -> Response>, ServeDir<ServeFile>>,
> {
    let _ = dir; // (see note below — use the real builder form)
    unimplemented!("placeholder — use the concrete form in Step 5b")
}
```

> **Implementer note — write the REAL builder, not the placeholder above.** The return-type annotation for a layered `ServeDir` is unwieldy; do NOT try to name it. Instead, build and attach the service inline inside `build_router` (no separate `static_service` fn), so the type is inferred:

In `build_router`, after the existing `.merge(...)` calls and BEFORE `.layer(TraceLayer)` / `.layer(cors)` / `.with_state(state)`, add the fallback conditionally:

```rust
pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = cors_layer(state.hot.clone());

    let mut router = Router::new()
        .route("/healthz", get(health::handler))
        .merge(admin::admin_routes())
        .merge(http_api::http_api_routes())
        .merge(ws::ws_routes())
        .merge(auth::provider::auth_routes());

    // Static SPA hosting, mounted LAST as the fallback so it can never shadow a
    // real route. Only when RTDB_STATIC_DIR is set and the directory exists;
    // otherwise the server is API-only.
    if let Some(dir) = state.config.static_dir.as_deref()
        && Path::new(dir).is_dir()
    {
        let serve_dir = ServeDir::new(dir).fallback(ServeFile::new(format!("{dir}/index.html")));
        router = router.fallback_service(
            tower::ServiceBuilder::new()
                .layer(from_fn(set_static_cache_headers))
                .service(serve_dir),
        );
    }

    router
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
```

(Delete the placeholder `static_service` fn entirely — the inline form in `build_router` is the implementation. `tower` must be a dependency — it is, transitively via axum/tower-http; if `tower::ServiceBuilder` isn't in scope, `use tower::ServiceBuilder;` or call `tower::ServiceBuilder::new()`.)

- [ ] **Step 6: Run the tests + gate**

`cd server && cargo test --test dashboard_test static_dir_serves no_static_dir` → both PASS. Then `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test` → all green.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(server): static SPA hosting via ServeDir + RTDB_STATIC_DIR + cache headers (#18 phase 6)"
```

---

## Self-Review (controller, before final review)

- **Spec coverage (§6):** `ServeDir::new(dir).fallback(ServeFile index.html)` + `append_index_html_on_directories` (default) → Task 1. Mounted last as `fallback_service` (never shadows routes) → Task 1 Step 5. `RTDB_STATIC_DIR` boot Config, None/non-existent ⇒ API-only → Task 1 Steps 1+5. Cache headers (html no-cache, assets immutable) → Task 1 Step 5. Same-origin ⇒ no CORS entry needed (unchanged CORS). ✅
- **Invariants:** No second writer; no auth change; only `config.rs` + `lib.rs` + tests touched. API routes unaffected (fallback only hits unmatched paths). ✅
- **Cache correctness:** Keying off response `Content-Type` (not request path) correctly marks the SPA-fallback index.html (served at arbitrary paths) as no-cache, and hashed assets as immutable. ✅
- **No core signature changes:** `build_router` signature unchanged; `Config` gains one field (4 test builders updated). ✅
