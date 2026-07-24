# Realtime Dashboard — Phase 3a: Metrics + Gauges Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Expose live operational metrics (cumulative query/mutation/upload counters, active WS-connection + subscription gauges, DB-pool size/idle, uptime) behind the admin gate via `GET /admin/metrics`, so the dashboard can render a live ops panel by polling.

**Architecture:** A new lock-free `Metrics` struct of atomics on `AppState`, incremented at the **transport handlers** (which already hold `AppState`) — HTTP `query_handler`/`mutate_handler`/`upload_handler` and WS `handle_socket`/`handle_text_frame`. A new `SubscriptionManager::count()` sums active subscriptions. `GET /admin/metrics` returns a JSON snapshot. **No change to the committer or the correctness core** — counters live at the transport boundary. (Throughput rates are derived client-side from successive snapshots; the realtime `/admin/stream` WS + op feed + scheduled-fire counting are Phase 3b.)

**Tech Stack:** Rust, axum, sqlx, Postgres 17, `std::sync::atomic`.

## Global Constraints

- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- Every failure is an `RtDbError {code, message}`; 500s generic. `GET /admin/metrics` is gated on `require_admin(&state, &headers).await`.
- `make checkall` is the definition of done; `make dev-db-up` required for tests (if it reports the known port-held condition, run legs directly: `cd server && cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings && cargo test`).
- Tests share one Postgres, isolate by uniquely-named dbs; `dashboard_test.rs` uses only `mod common;` + fully-qualified paths.
- Atomic ordering is `Relaxed` (these are best-effort counters, no synchronization dependence).
- Do NOT thread `Metrics` into the committer or scheduler (that's Phase 3b). Increment only where `AppState` is already in scope.

## File Structure

- `server/src/metrics.rs` — **create**: `Metrics` (atomics) + `record_*`/`ws_connect`/`ws_disconnect` + async `snapshot()` → `MetricsSnapshot` (Serialize).
- `server/src/subs.rs` — add `pub async fn count(&self) -> usize`.
- `server/src/lib.rs` — add `pub metrics: Arc<Metrics>` to `AppState` + wire in `AppState::new`; `pub mod metrics;`.
- `server/src/admin.rs` — add `metrics_handler` + route `GET /admin/metrics`.
- `server/src/http_api.rs` — instrument `query_handler`/`mutate_handler`/`upload_handler`.
- `server/src/ws.rs` — instrument `handle_socket` (ws_connect/disconnect) + `handle_text_frame` Subscribe/Mutate arms.
- `server/tests/dashboard_test.rs` — append tests.

---

## Task 1: `Metrics` struct + `SubscriptionManager::count()` + AppState wiring

**Files:**
- Create: `server/src/metrics.rs`
- Modify: `server/src/subs.rs`, `server/src/lib.rs`.
- Test: `server/tests/dashboard_test.rs` (append a unit-style test via the integration binary).

**Interfaces:**
- Produces: `pub struct Metrics` + `Metrics::new() -> Arc<Self>`, `record_query/record_mutation/record_upload/ws_connect/ws_disconnect`, `async snapshot(&PgPool, &SubscriptionManager, SystemTime) -> MetricsSnapshot`; `SubscriptionManager::count() -> usize`; `AppState.metrics: Arc<Metrics>`.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/dashboard_test.rs`:

```rust

// Metrics counters increment and snapshot reflects them; count() reports active subs.
#[tokio::test]
async fn metrics_counters_and_subs_count() -> anyhow::Result<()> {
    let m = rtdb_server::metrics::Metrics::new();
    m.record_query();
    m.record_query();
    m.record_mutation();

    let state = common::test_state().await;
    // count() is 0 before any subscribe on a fresh manager.
    assert_eq!(state.subs.count().await, 0);

    let snap = m.snapshot(&state.pool, &state.subs, state.started_at).await;
    assert_eq!(snap.queries_total, 2);
    assert_eq!(snap.mutations_total, 1);
    assert_eq!(snap.uploads_total, 0);
    assert_eq!(snap.ws_connections, 0);
    assert!(snap.pool_size >= 0);
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

```
cd server && cargo test --test dashboard_test metrics_counters_and_subs_count
```
Expected: compile error — `rtdb_server::metrics` does not exist.

- [ ] **Step 3: Create `server/src/metrics.rs`**

```rust
//! Process-wide operational metrics for the dashboard: lock-free atomic
//! counters incremented at the transport boundary (HTTP + WS handlers), snapshotted
//! on demand by `GET /admin/metrics`. Rates are derived client-side from successive
//! snapshots; the realtime push stream + op feed live in Phase 3b.
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::SystemTime;

use serde::Serialize;
use sqlx::PgPool;

use crate::subs::SubscriptionManager;

#[derive(Default)]
pub struct Metrics {
    queries_total: AtomicU64,
    mutations_total: AtomicU64,
    uploads_total: AtomicU64,
    /// Current open `/sync` WebSocket connections (inc on auth, dec on close).
    ws_connections: AtomicI64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_query(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_mutation(&self) {
        self.mutations_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_upload(&self) {
        self.uploads_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ws_connect(&self) {
        self.ws_connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ws_disconnect(&self) {
        self.ws_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub async fn snapshot(
        &self,
        pool: &PgPool,
        subs: &SubscriptionManager,
        started_at: SystemTime,
    ) -> MetricsSnapshot {
        MetricsSnapshot {
            queries_total: self.queries_total.load(Ordering::Relaxed),
            mutations_total: self.mutations_total.load(Ordering::Relaxed),
            uploads_total: self.uploads_total.load(Ordering::Relaxed),
            ws_connections: self.ws_connections.load(Ordering::Relaxed),
            active_subscriptions: subs.count().await,
            pool_size: pool.size() as i64,
            pool_idle: pool.num_idle() as i64,
            uptime_seconds: started_at.elapsed().map(|d| d.as_secs()).unwrap_or(0),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsSnapshot {
    pub queries_total: u64,
    pub mutations_total: u64,
    pub uploads_total: u64,
    pub ws_connections: i64,
    pub active_subscriptions: usize,
    pub pool_size: i64,
    pub pool_idle: i64,
    pub uptime_seconds: u64,
}
```

- [ ] **Step 4: Add `count()` to `SubscriptionManager`**

In `server/src/subs.rs`, inside `impl SubscriptionManager` (e.g. after `remove_conn`), add:

```rust
    /// Total active subscriptions across all databases (a dashboard gauge).
    pub async fn count(&self) -> usize {
        let guard = self.subs.lock().await;
        guard.values().map(|db_subs| db_subs.len()).sum()
    }
```

- [ ] **Step 5: Wire `Metrics` into `AppState`**

In `server/src/lib.rs`: add `pub mod metrics;` to the module list (alphabetical, near `mutation_log`/`pagination`), add a `use metrics::Metrics;` (or refer via `crate::metrics::Metrics`), add the field, and construct it. Concretely:

- In the module declarations block, add:
  ```rust
  pub mod metrics;
  ```
- In `AppState`, add the field:
  ```rust
      pub metrics: Arc<metrics::Metrics>,
  ```
- In `AppState::new`, construct it alongside the others and include it in the struct literal:
  ```rust
          let metrics = metrics::Metrics::new();
          // ... existing fields ...
          Arc::new(Self {
              pool, config, schemas, subs, committers, oauth_states, started_at,
              metrics,
          })
  ```
  (`Arc` is already imported in lib.rs; `started_at` is already constructed there — keep its existing line, just add `metrics` to the literal.)

- [ ] **Step 6: Run test to verify it passes**

```
cd server && cargo test --test dashboard_test metrics_counters_and_subs_count
```
Expected: PASS.

- [ ] **Step 7: Full gate + commit**

```
cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test
```
Expected: green. Commit:

```bash
git add server/src/metrics.rs server/src/subs.rs server/src/lib.rs server/tests/dashboard_test.rs
git commit -m "feat(server): Metrics struct + SubscriptionManager::count() (#18 phase 3a)"
```

---

## Task 2: Instrument transport handlers + `GET /admin/metrics`

**Files:**
- Modify: `server/src/admin.rs` (handler + route), `server/src/http_api.rs` (3 sites), `server/src/ws.rs` (connect/disconnect + 2 arms).
- Test: `server/tests/dashboard_test.rs`.

**Interfaces:**
- Consumes: `state.metrics` (Task 1), `require_admin`.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/dashboard_test.rs`:

```rust

// GET /admin/metrics returns the snapshot; a real mutation bumps mutationsTotal.
#[tokio::test]
async fn metrics_endpoint_reflects_a_mutation() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let before: serde_json::Value =
        common::admin_get(addr, "/admin/metrics").await.json().await?;
    assert_eq!(before["mutationsTotal"], 0, "fresh state should have 0 mutations: {before}");

    // Mint a token + run one insert via /api/mutate.
    let mint: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "t"}),
    )
    .await
    .json()
    .await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "db": db,
            "txn": {"steps": [{"type": "insert", "table": "projects", "doc": {"name": "p", "status": "active"}}]}
        }))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let after: serde_json::Value =
        common::admin_get(addr, "/admin/metrics").await.json().await?;
    assert_eq!(after["mutationsTotal"], 1, "one mutation should be counted: {after}");
    assert!(after["queriesTotal"].as_i64().unwrap_or(-1) >= 0);
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

```
cd server && cargo test --test dashboard_test metrics_endpoint_reflects_a_mutation
```
Expected: FAIL — `GET /admin/metrics` 404 (route not registered; and counters not yet instrumented).

- [ ] **Step 3: Add the metrics endpoint**

In `server/src/admin.rs`, before `pub fn admin_routes()`, add:

```rust
async fn metrics_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<crate::metrics::MetricsSnapshot>, RtDbError> {
    require_admin(&state, &headers).await?;
    Ok(Json(
        state
            .metrics
            .snapshot(&state.pool, &state.subs, state.started_at)
            .await,
    ))
}
```

Register the route in `admin_routes()` (e.g. after `/admin/dbs/{db}/stats`):

```rust
        .route("/admin/metrics", get(metrics_handler))
```

- [ ] **Step 4: Instrument the HTTP handlers (`server/src/http_api.rs`)**

In `query_handler`, after the successful `execute_query(...).await?` (right before `Ok(Json(QueryResponse { result }))`), add:

```rust
    state.metrics.record_query();
```

In `mutate_handler`, after the successful `state.committers.mutate(...).await?` (right before `Ok(Json(MutateResponse { results: outcome.results }))`), add:

```rust
    state.metrics.record_mutation();
```

In `upload_handler`, after the successful `storage::put(...).await?` and `id` is bound (right before `Ok(Json(UploadResponse { ... }))`), add:

```rust
    state.metrics.record_upload();
```

- [ ] **Step 5: Instrument the WS handlers (`server/src/ws.rs`)**

In `handle_socket`, immediately after the successful authenticate `let Some((principal, db)) = authenticate(&mut socket, &state).await else { return };` (so only authenticated connections count), add:

```rust
    state.metrics.ws_connect();
```

At the very end of `handle_socket`, after the existing `state.subs.remove_conn(&db, conn_id).await;` line, add:

```rust
    state.metrics.ws_disconnect();
```

In `handle_text_frame`'s `ClientMessage::Subscribe` arm, on the success branch — inside the `Ok(()) => { ... }` after the `state.committers.subscribe(...).await` call succeeds (i.e. in the `if let Err(error) = ...` block's `else`/fall-through, or simply after the match when no `SubscribeErr` was sent) — call `state.metrics.record_query();`. Simplest correct placement: right after the inner `match authorize(...)` block resolves to a successful subscribe (no error sent). Concretely, add the increment as the first statement inside the `Ok(()) => { ... }` arm of the inner `authorize` match.

In `handle_text_frame`'s `ClientMessage::Mutate` arm, on success — inside `Ok(outcome) => { ... }` (where `MutateOk` is sent) — add `state.metrics.record_mutation();` as the first statement of that arm.

- [ ] **Step 6: Run test to verify it passes**

```
cd server && cargo test --test dashboard_test metrics_endpoint_reflects_a_mutation
```
Expected: PASS (mutationsTotal goes 0 → 1).

- [ ] **Step 7: Full gate + commit**

```
cd server && cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test
```
Expected: green. Commit:

```bash
git add server/src/admin.rs server/src/http_api.rs server/src/ws.rs server/tests/dashboard_test.rs
git commit -m "feat(server): instrument metrics + GET /admin/metrics (#18 phase 3a)"
```

---

## Phase 3a Done — Definition of Done

- `GET /admin/metrics` (admin-gated) returns `{queriesTotal, mutationsTotal, uploadsTotal, wsConnections, activeSubscriptions, poolSize, poolIdle, uptimeSeconds}`.
- Counters increment at the transport boundary (HTTP query/mutate/upload, WS subscribe/mutate); `ws_connections` tracks authenticated `/sync` connections; `active_subscriptions` via `SubscriptionManager::count()`.
- `make checkall` green; all pre-existing tests pass.
- No change to the committer or correctness core.

## Next: Phase 3b (op feed + `/admin/stream` WS) — separate plan
