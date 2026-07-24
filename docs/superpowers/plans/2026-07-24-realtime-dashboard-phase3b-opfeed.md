# Realtime Dashboard — Phase 3b: Op Feed + `/admin/stream` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** A realtime document-activity feed: every committed write publishes per-document events tapped at the committer's two commit sites, replayed from an in-memory ring on (re)connect and streamed live to an admin-gated WebSocket, with periodic gauge snapshots multiplexed in.

**Architecture:** A new `OpFeed` (`tokio::sync::broadcast` + a bounded `Mutex<VecDeque>` ring) on `AppState`. The `Committers`/`CommitterCtx`/`run_committer` threading gains an `Arc<OpFeed>` so `handle_mutate` and `handle_scheduled` can publish — once each, after a successful `execute_txn` — one `OpEvent` per `(table, id)` in `outcome.write_set.docs`. `GET /admin/ops/recent` reads the filtered ring; `WS /admin/stream` (admin-gated at upgrade) replays the filtered ring, then streams live broadcast events plus a ~1s gauge snapshot.

**Two deliberate simplifications vs spec §3 (flagged for the user):**
1. **Op kind deferred.** `write_set.docs` carries `(table, id)` but not insert/patch/replace/delete. Adding the kind would require enriching `WriteSet` (a txn-core change that also risks `subs.rs`'s `docs.contains(...)` point-read check). 3b emits doc-change events without the kind; a fast-follow can add `WriteSet.ops` cleanly.
2. **`principal` = the `owner` string.** The committer only sees `owner: Option<String>` (Some = a user's id, None = machine/scheduled), not a `Principal`, so `OpEvent.principal` is `Option<String>` (the owner id) rather than a `PrincipalKind`. Admin writes (Phase 5) will carry `None`.

**Tech Stack:** Rust, axum (`WebSocketUpgrade`), tokio (`broadcast`, `select`), sqlx, Postgres 17.

## Global Constraints

- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- `require_admin(&state, &headers).await` gates `/admin/ops/recent` AND the `/admin/stream` WS upgrade (bearer read from the HTTP headers at upgrade).
- `broadcast::send` errors when there are no receivers — ignore it (`.ok()`); the tap must never fail a commit. Ring push is best-effort.
- The tap publishes AFTER `execute_txn` success and `fan_out` — never changes commit ordering or correctness. One publish per commit site (`handle_mutate`, `handle_scheduled`).
- `make checkall` is the gate; `make dev-db-up` required (if port-held, run legs directly). Tests share one Postgres; `dashboard_test.rs` uses only `mod common;` + fully-qualified paths.

## File Structure

- `server/src/op_feed.rs` — **create**: `OpEvent`, `OpFeed` (broadcast + ring), `publish`/`recent`/`subscribe`, `new`.
- `server/src/committer.rs` — thread `Arc<OpFeed>` through `Committers`/`CommitterCtx`/`run_committer`/`channel_for`; publish in `handle_mutate` + `handle_scheduled`.
- `server/src/lib.rs` — `pub mod op_feed;`, `pub op_feed: Arc<OpFeed>` on `AppState`, construct + pass to `Committers::new`.
- `server/src/admin.rs` — `GET /admin/ops/recent` + `WS /admin/stream` (+ routes).
- `server/tests/dashboard_test.rs` — tests.

---

## Task 1: `OpFeed` module + wire through `AppState`/`Committers`

**Files:** create `server/src/op_feed.rs`; modify `server/src/lib.rs`, `server/src/committer.rs`. Test: `dashboard_test.rs`.

**Interfaces:**
- Produces: `OpFeed::new(cap, ring_cap) -> Arc<Self>`, `publish(&self, db: &str, owner: Option<&str>, docs: &BTreeSet<(String,String)>)`, `recent(&self, db: Option<&str>, table: Option<&str>, n: usize) -> Vec<OpEvent>`, `subscribe(&self) -> broadcast::Receiver<OpEvent>`; `OpEvent { db, table, docId, ts, owner }` (camelCase Serialize); `Committers::new` gains an `op_feed` param threaded to `run_committer`/`CommitterCtx`.

- [ ] **Step 1: Write the failing test** — append to `dashboard_test.rs`:

```rust

// OpFeed publishes per-(table,id) events, replays them from the ring, and broadcasts live.
#[tokio::test]
async fn op_feed_publishes_and_replays() -> anyhow::Result<()> {
    let feed = rtdb_server::op_feed::OpFeed::new(64, 32);
    let mut docs = std::collections::BTreeSet::new();
    docs.insert(("projects".to_string(), "id1".to_string()));
    docs.insert(("projects".to_string(), "id2".to_string()));

    // Subscribe BEFORE publish to also exercise the live receiver.
    let mut rx = feed.subscribe();
    feed.publish("dbA", Some("user-1"), &docs).await;

    // Ring replay returns both docs (newest first or stable order — assert membership).
    let recent = feed.recent(Some("dbA"), None, 10);
    let ids: Vec<&str> = recent.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(ids.contains(&"id1") && ids.contains(&"id2"), "ring missing events: {recent:?}");
    // A different db filter excludes them.
    assert!(feed.recent(Some("dbB"), None, 10).is_empty());

    // Live receiver got both events.
    let mut got = 0;
    while let Ok(ev) = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await {
        let _ = ev; got += 1;
    }
    assert_eq!(got, 2);
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** — `cd server && cargo test --test dashboard_test op_feed_publishes_and_replays` → compile error (`op_feed` missing).

- [ ] **Step 3: Create `server/src/op_feed.rs`**:

```rust
//! Realtime document-activity feed for the dashboard. The committer publishes one
//! `OpEvent` per written `(table, id)` after each successful commit (handle_mutate,
//! handle_scheduled). A bounded ring keeps recent events for reconnect/initial replay;
//! a `broadcast` channel fans live events to open `/admin/stream` sockets. Non-durable.
use std::collections::VecDeque;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{Mutex, broadcast};

use crate::db::now_ms;

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub ts: i64,
    /// The committing principal's owner id when known (Some = a user; None = machine/scheduled/admin).
    pub owner: Option<String>,
}

pub struct OpFeed {
    tx: broadcast::Sender<OpEvent>,
    ring: Mutex<VecDeque<OpEvent>>,
    ring_cap: usize,
}

impl OpFeed {
    pub fn new(broadcast_cap: usize, ring_cap: usize) -> Arc<Self> {
        let (tx, _rx) = broadcast::channel(broadcast_cap);
        Arc::new(Self {
            tx,
            ring: Mutex::new(VecDeque::with_capacity(ring_cap)),
            ring_cap,
        })
    }

    /// Publishes one `OpEvent` per `(table, id)` in `docs`. Ring push is bounded (evicts
    /// oldest); `broadcast::send` is a no-op when there are no subscribers (error ignored).
    /// `owner` is the committer's `owner` (Some user_id / None). Never fails the commit.
    pub async fn publish(
        &self,
        db: &str,
        owner: Option<&str>,
        docs: &std::collections::BTreeSet<(String, String)>,
    ) {
        let ts = now_ms();
        let owner = owner.map(|s| s.to_string());
        let mut ring = self.ring.lock().await;
        for (table, doc_id) in docs {
            let event = OpEvent {
                db: db.to_string(),
                table: table.clone(),
                doc_id: doc_id.clone(),
                ts,
                owner: owner.clone(),
            };
            if ring.len() >= self.ring_cap {
                ring.pop_front();
            }
            ring.push_back(event.clone());
            let _ = self.tx.send(event); // no subscribers → Err, ignored
        }
    }

    /// Recent events (newest-last), filtered by optional db/table, capped at `n`.
    pub async fn recent(
        &self,
        db: Option<&str>,
        table: Option<&str>,
        n: usize,
    ) -> Vec<OpEvent> {
        let ring = self.ring.lock().await;
        ring.iter()
            .rev()
            .filter(|e| db.map_or(true, |d| e.db == d))
            .filter(|e| table.map_or(true, |t| e.table == t))
            .take(n)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// A fresh live receiver for `/admin/stream`.
    pub fn subscribe(&self) -> broadcast::Receiver<OpEvent> {
        self.tx.subscribe()
    }
}
```

- [ ] **Step 4: Wire into `AppState`** — in `server/src/lib.rs`: add `pub mod op_feed;`; in `AppState` add `pub op_feed: Arc<op_feed::OpFeed>,`; in `AppState::new` add `let op_feed = op_feed::OpFeed::new(256, 500);` and pass `op_feed.clone()` to `Committers::new(...)`, and add `op_feed` to the struct literal.

- [ ] **Step 5: Thread through `Committers`** — in `server/src/committer.rs`:
  - Add `op_feed: Arc<crate::op_feed::OpFeed>,` to the `Committers` struct + a param to `Committers::new`; store it.
  - Add `op_feed: Arc<crate::op_feed::OpFeed>,` to `CommitterCtx`.
  - Add an `op_feed: Arc<crate::op_feed::OpFeed>,` param to `run_committer` (after `schemas`) and include it in the `CommitterCtx { ... }` literal.
  - In `channel_for`, pass `self.op_feed.clone()` into the `run_committer(...)` spawn.
  - In `lib.rs`, update the `Committers::new(...)` call to pass `op_feed.clone()`.

- [ ] **Step 6: Verify Task 1 test passes** — `cd server && cargo test --test dashboard_test op_feed_publishes_and_replays` → PASS. Then full gate + commit:

```bash
git add server/src/op_feed.rs server/src/lib.rs server/src/committer.rs server/tests/dashboard_test.rs
git commit -m "feat(server): OpFeed (broadcast + ring) + AppState/Committers wiring (#18 phase 3b)"
```

---

## Task 2: Tap at the two commit sites + `GET /admin/ops/recent`

**Files:** modify `server/src/committer.rs` (tap), `server/src/admin.rs` (endpoint + route). Test: `dashboard_test.rs`.

- [ ] **Step 1: Failing test** — append to `dashboard_test.rs`:

```rust

// A committed mutation publishes an op event; /admin/ops/recent returns it (admin-gated).
#[tokio::test]
async fn op_feed_tapped_on_commit() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    // Mint + insert one project doc via /api/mutate.
    let mint: serde_json::Value =
        common::admin_post(addr, "/admin/mint-token", serde_json::json!({"db": db, "name": "t"}))
            .await.json().await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let resp = reqwest::Client::new().post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"db": db, "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}}))
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // recent lists the event for this db.
    let body: serde_json::Value =
        common::admin_get(addr, &format!("/admin/ops/recent?db={db}&n=10")).await.json().await?;
    let ops = body["ops"].as_array().expect("ops array");
    assert!(ops.iter().any(|o| o["table"] == "projects"), "no projects op event: {body}");

    // A different db filter excludes it.
    let body: serde_json::Value =
        common::admin_get(addr, "/admin/ops/recent?db=other&n=10").await.json().await?;
    assert!(body["ops"].as_array().unwrap().iter().all(|o| o["table"] != "projects"));
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** — route 404 / no events.

- [ ] **Step 3: Add the tap** — in `handle_mutate` (`committer.rs`), immediately after the `ctx.subs.fan_out(...).await;` line (after `execute_txn` success), add:

```rust
    ctx.op_feed.publish(&ctx.db, owner.as_deref(), &outcome.write_set.docs).await;
```

In `handle_scheduled` (`committer.rs`), immediately after its `ctx.subs.fan_out(...).await;` (inside the `Ok(outcome) =>` arm), add:

```rust
            ctx.op_feed.publish(&ctx.db, None, &outcome.write_set.docs).await;
```

- [ ] **Step 4: Add `GET /admin/ops/recent`** — in `server/src/admin.rs`:

```rust
#[derive(Deserialize)]
struct OpsRecentParams {
    db: Option<String>,
    table: Option<String>,
    #[serde(default = "default_ops_n")]
    n: usize,
}
fn default_ops_n() -> usize { 100 }

#[derive(Serialize)]
struct OpsRecentResponse { ops: Vec<crate::op_feed::OpEvent> }

async fn ops_recent(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<OpsRecentParams>,
) -> Result<Json<OpsRecentResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let ops = state
        .op_feed
        .recent(params.db.as_deref(), params.table.as_deref(), params.n.min(500))
        .await;
    Ok(Json(OpsRecentResponse { ops }))
}
```

Register in `admin_routes()`: `.route("/admin/ops/recent", get(ops_recent))`.

- [ ] **Step 5: Verify it passes** → full gate + commit:

```bash
git add server/src/committer.rs server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): op-feed tap at commit sites + GET /admin/ops/recent (#18 phase 3b)"
```

---

## Task 3: `WS /admin/stream` (replay + live + periodic gauges)

**Files:** modify `server/src/admin.rs` (upgrade handler + route), possibly a small helper. Test: `dashboard_test.rs`.

- [ ] **Step 1: Failing test** — append a WS round-trip test that connects to `/admin/stream` with the admin-key bearer, sends nothing, mutates via HTTP, and asserts it receives a `{kind:"op",...}` frame. (Use `tokio-tungstenite` if already a dev-dep; otherwise assert via the HTTP `/admin/ops/recent` path already covered in Task 2 and make this test focus on the upgrade's admin gate: a missing bearer → 403/401 at upgrade, a valid admin key → 101 switching protocols.) Implement the upgrade-gate version first (no extra dep):

```rust

// /admin/stream rejects a missing bearer at the WS upgrade (no 101).
#[tokio::test]
async fn admin_stream_requires_admin() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;
    // Missing bearer → rejected at the admin gate before any WS negotiation.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/stream"))
        .send().await?;
    let status = resp.status();
    assert!(status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN,
        "missing bearer must be rejected, got {status}");
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** — route 404.

- [ ] **Step 3: Add the upgrade handler** — in `server/src/admin.rs`:

```rust
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::response::Response;
use std::time::Duration;

#[derive(Deserialize)]
struct StreamParams {
    db: Option<String>,
    table: Option<String>,
}

async fn admin_stream(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<StreamParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, RtDbError> {
    require_admin(&state, &headers).await?; // admin-gate at the HTTP upgrade
    Ok(ws.on_upgrade(move |socket| run_admin_stream(socket, state, params.db, params.table)))
}

async fn run_admin_stream(mut socket: WebSocket, state: Arc<AppState>, db: Option<String>, table: Option<String>) {
    // 1. Replay recent ring (filtered) as initial op messages.
    for ev in state.op_feed.recent(db.as_deref(), table.as_deref(), 200).await {
        let _ = send_json(&mut socket, &serde_json::json!({"kind":"op","event":ev})).await;
    }
    // 2. Live broadcast + ~1s gauge snapshots.
    let mut rx = state.op_feed.subscribe();
    let mut gauge_tick = tokio::time::interval(Duration::from_secs(1));
    gauge_tick.tick().await; // skip immediate
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Ok(ev) = ev else { break };
                if db.as_deref().map_or(true, |d| ev.db == d)
                    && table.as_deref().map_or(true, |t| ev.table == t)
                {
                    if send_json(&mut socket, &serde_json::json!({"kind":"op","event":ev})).await.is_err() { break; }
                }
            }
            _ = gauge_tick.tick() => {
                let snap = state.metrics.snapshot(&state.pool, &state.subs, state.started_at).await;
                if send_json(&mut socket, &serde_json::json!({"kind":"gauges","gauges":snap})).await.is_err() { break; }
            }
        }
    }
}

async fn send_json(socket: &mut WebSocket, value: &serde_json::Value) -> Result<(), axum::Error> {
    use axum::extract::ws::Message;
    let text = serde_json::to_string(value).unwrap_or_else(|_| "{}".into());
    socket.send(Message::Text(text.into())).await
}
```

Register in `admin_routes()`: `.route("/admin/stream", get(admin_stream))`.

- [ ] **Step 4: Verify it passes + full gate + commit**:

```bash
git add server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): WS /admin/stream — op-feed replay + live + gauge snapshots (#18 phase 3b)"
```

---

## Phase 3b Done — Definition of Done
- Commits publish one `OpEvent` per written doc (handle_mutate + handle_scheduled); ring replays on connect; `/admin/stream` streams live events + 1s gauges, admin-gated; `GET /admin/ops/recent` reads the ring.
- `make checkall` green; existing tests pass; the single-writer + commit ordering invariants intact (publish is after commit, non-fatal).
- Op kind + `PrincipalKind` deferred (see simplifications).

## Next: Phase 4 (config + dynamic CORS) — separate plan
