# Realtime Dashboard — Phase 3b: Op Feed (with op kind) + `/admin/stream` Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** A realtime document-activity feed carrying the full op kind — every committed write publishes per-document `OpEvent{table, docId, kind, owner, ts}` tapped at the committer's two commit sites, replayed from an in-memory ring on (re)connect and streamed live over an admin-gated WebSocket with periodic gauges.

**Architecture:** Enrich `WriteSet` with an `ops: Vec<DocOp{table,id,kind}>` field (alongside the existing `docs`, which `subs.rs` keeps using) by adding a `kind` arg to `touch()` and updating its 6 call sites in the step-dispatch. A new `OpFeed` (broadcast + ring) on `AppState`, threaded into `Committers`/`CommitterCtx`/`run_committer`, publishes one `OpEvent` per `DocOp` after each successful `execute_txn` (in `handle_mutate` + `handle_scheduled`). `GET /admin/ops/recent` reads the filtered ring; `WS /admin/stream` (admin-gated at upgrade) replays it, then streams live events + ~1s gauge snapshots.

**One simplification vs spec §3:** `principal` = the `owner` string (`Option<String>`), since the committer only has `owner`, never a `Principal`.

**Tech Stack:** Rust, axum (`WebSocketUpgrade`), tokio (`broadcast`, `select`), sqlx, Postgres 17.

## Global Constraints
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- `require_admin(&state, &headers).await` gates `/admin/ops/recent` and the `/admin/stream` upgrade.
- `broadcast::send` errors when there are no receivers — ignore (`.ok()`); the tap must never fail a commit. Publish is AFTER `execute_txn` + `fan_out`.
- `subs.rs` reads only `write_set.docs` — do NOT change `docs`; ADD `ops`. Keep `touch` populating `tables` + `docs` exactly as today, plus `ops`.
- `make checkall` is the gate; `make dev-db-up` required (if port-held, run legs directly). Tests share one Postgres; `dashboard_test.rs` uses only `mod common;` + fully-qualified paths.

## File Structure
- `server/src/txn.rs` — `OpKind`, `DocOp`, `WriteSet.ops`, `touch(table,id,kind)`, 6 call-site updates.
- `server/src/op_feed.rs` — **create**: `OpEvent{db,table,docId,kind,owner,ts}`, `OpFeed` (broadcast + ring), `publish`/`recent`/`subscribe`.
- `server/src/committer.rs` — thread `Arc<OpFeed>` through `Committers`/`CommitterCtx`/`run_committer`/`channel_for`; publish in `handle_mutate` + `handle_scheduled`.
- `server/src/lib.rs` — `pub mod op_feed;`, `pub op_feed: Arc<OpFeed>` on `AppState`, construct + pass to `Committers::new`.
- `server/src/admin.rs` — `GET /admin/ops/recent` + `WS /admin/stream`.
- `server/tests/dashboard_test.rs` — tests.

---

## Task 1: Enrich `WriteSet` with op kind

**Files:** `server/src/txn.rs`. Test: a unit test inside `txn.rs`'s `#[cfg(test)]` module (asserts `touch` records kinds into `ops`).

- [ ] **Step 1: Failing unit test** — add to `txn.rs`'s test module:

```rust
    #[test]
    fn write_set_ops_records_kind() {
        let mut ws = WriteSet::default();
        ws.touch("projects", "id1", OpKind::Insert);
        ws.touch("projects", "id2", OpKind::Patch);
        ws.touch("tasks", "id3", OpKind::Delete);
        assert_eq!(ws.docs.len(), 3);
        assert_eq!(ws.ops.len(), 3);
        assert!(ws.ops.iter().any(|o| o.id == "id1" && o.kind == OpKind::Insert));
        assert!(ws.ops.iter().any(|o| o.id == "id2" && o.kind == OpKind::Patch));
        assert!(ws.ops.iter().any(|o| o.table == "tasks" && o.kind == OpKind::Delete));
    }
```

- [ ] **Step 2: Verify it fails** — `cd server && cargo test -p rtdb-server write_set_ops_records_kind` → compile error (`OpKind`/`ops`/`touch` arity missing).

- [ ] **Step 3: Add `OpKind`, `DocOp`, `ops`, and update `touch`** — in `txn.rs` near the `WriteSet` definition:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    Insert,
    Patch,
    Replace,
    Delete,
    Upsert,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DocOp {
    pub table: String,
    pub id: String,
    pub kind: OpKind,
}
```

Change `WriteSet` to add the `ops` field:

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct WriteSet {
    pub tables: BTreeSet<String>,
    pub docs: BTreeSet<(String, String)>,
    pub ops: Vec<DocOp>,
}
```

Change `touch` to record the kind:

```rust
    /// Records that the transaction wrote document `id` in `table` as `kind`.
    fn touch(&mut self, table: &str, id: &str, kind: OpKind) {
        self.tables.insert(table.to_string());
        self.docs.insert((table.to_string(), id.to_string()));
        self.ops.push(DocOp {
            table: table.to_string(),
            id: id.to_string(),
            kind,
        });
    }
```

- [ ] **Step 4: Update the 6 call sites** in `execute_txn`'s step-dispatch (each `write_set.touch(table, id)` gains its kind):

```rust
    // Insert (line ~781):
    write_set.touch(table, &id, OpKind::Insert);
    // Patch (line ~789):
    write_set.touch(table, id, OpKind::Patch);
    // Replace (line ~797):
    write_set.touch(table, id, OpKind::Replace);
    // Delete (line ~804):
    write_set.touch(table, id, OpKind::Delete);
    // Upsert insert path (line ~840) and patch path (line ~855):
    write_set.touch(table, &id, OpKind::Upsert);
```

- [ ] **Step 5: Verify it passes** — `cd server && cargo test -p rtdb-server write_set_ops_records_kind && cargo clippy --all-targets -- -D warnings` → PASS. (`subs.rs` still compiles — it uses `docs`, unchanged.) Then full gate + commit:

```bash
git add server/src/txn.rs
git commit -m "feat(server): record op kind in WriteSet.ops at each write site (#18 phase 3b)"
```

---

## Task 2: `OpFeed` module + wire through `AppState`/`Committers`

**Files:** create `server/src/op_feed.rs`; modify `server/src/lib.rs`, `server/src/committer.rs`. Test: `dashboard_test.rs`.

- [ ] **Step 1: Failing test** — append to `dashboard_test.rs`:

```rust

// OpFeed publishes one event per DocOp (with kind), replays from the ring, broadcasts live.
#[tokio::test]
async fn op_feed_publishes_and_replays() -> anyhow::Result<()> {
    use rtdb_server::txn::{DocOp, OpKind};
    let feed = rtdb_server::op_feed::OpFeed::new(64, 32);
    let ops = vec![
        DocOp { table: "projects".into(), id: "id1".into(), kind: OpKind::Insert },
        DocOp { table: "projects".into(), id: "id2".into(), kind: OpKind::Patch },
    ];
    let mut rx = feed.subscribe();
    feed.publish("dbA", Some("user-1"), &ops).await;

    let recent = feed.recent(Some("dbA"), None, 10);
    let ids: Vec<&str> = recent.iter().map(|e| e.doc_id.as_str()).collect();
    assert!(ids.contains(&"id1") && ids.contains(&"id2"), "ring missing events: {recent:?}");
    assert_eq!(recent.iter().find(|e| e.doc_id == "id1").unwrap().kind, OpKind::Insert);
    assert!(feed.recent(Some("dbB"), None, 10).is_empty());

    let mut got = 0;
    while let Ok(_ev) = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv()).await { got += 1; }
    assert_eq!(got, 2);
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** → compile error (`op_feed` missing).

- [ ] **Step 3: Create `server/src/op_feed.rs`**:

```rust
//! Realtime document-activity feed. The committer publishes one `OpEvent` per
//! `DocOp` after each successful commit. A bounded ring replays recent events on
//! (re)connect; a `broadcast` channel fans live events to `/admin/stream`. Non-durable.
use std::collections::VecDeque;
use std::sync::Arc;

use serde::Serialize;
use tokio::sync::{Mutex, broadcast};

use crate::db::now_ms;
use crate::txn::{DocOp, OpKind};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OpEvent {
    pub db: String,
    pub table: String,
    pub doc_id: String,
    pub kind: OpKind,
    pub ts: i64,
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
        Arc::new(Self { tx, ring: Mutex::new(VecDeque::with_capacity(ring_cap)), ring_cap })
    }

    /// One `OpEvent` per `DocOp`. Ring is bounded (evicts oldest); `broadcast::send`
    /// is a no-op with no subscribers. Never fails the commit.
    pub async fn publish(&self, db: &str, owner: Option<&str>, ops: &[DocOp]) {
        let ts = now_ms();
        let owner = owner.map(|s| s.to_string());
        let mut ring = self.ring.lock().await;
        for op in ops {
            let event = OpEvent {
                db: db.to_string(),
                table: op.table.clone(),
                doc_id: op.id.clone(),
                kind: op.kind,
                ts,
                owner: owner.clone(),
            };
            if ring.len() >= self.ring_cap { ring.pop_front(); }
            ring.push_back(event.clone());
            let _ = self.tx.send(event);
        }
    }

    /// Recent events (oldest-first), filtered by optional db/table, capped at `n`.
    pub async fn recent(&self, db: Option<&str>, table: Option<&str>, n: usize) -> Vec<OpEvent> {
        let ring = self.ring.lock().await;
        ring.iter().rev()
            .filter(|e| db.map_or(true, |d| e.db == d))
            .filter(|e| table.map_or(true, |t| e.table == t))
            .take(n).cloned().collect::<Vec<_>>().into_iter().rev().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<OpEvent> { self.tx.subscribe() }
}
```

- [ ] **Step 4: Wire into `AppState`** — `lib.rs`: add `pub mod op_feed;`; `pub op_feed: Arc<op_feed::OpFeed>,` field; in `AppState::new` add `let op_feed = op_feed::OpFeed::new(256, 500);`, pass `op_feed.clone()` to `Committers::new`, add `op_feed` to the literal.

- [ ] **Step 5: Thread through `Committers`** — `committer.rs`: add `op_feed: Arc<crate::op_feed::OpFeed>` to `Committers` + `Committers::new` param; same field on `CommitterCtx`; add `op_feed` param to `run_committer` (after `schemas`) and to its `CommitterCtx { ... }` literal; in `channel_for` pass `self.op_feed.clone()` into the `run_committer(...)` spawn; update the `Committers::new(...)` call in `lib.rs`.

- [ ] **Step 6: Verify it passes + gate + commit**:

```bash
git add server/src/op_feed.rs server/src/lib.rs server/src/committer.rs server/tests/dashboard_test.rs
git commit -m "feat(server): OpFeed (broadcast + ring, with kind) + AppState/Committers wiring (#18 phase 3b)"
```

---

## Task 3: Tap at the two commit sites + `GET /admin/ops/recent`

**Files:** `server/src/committer.rs` (tap), `server/src/admin.rs` (endpoint + route). Test: `dashboard_test.rs`.

- [ ] **Step 1: Failing test** — append to `dashboard_test.rs`:

```rust

// A committed insert publishes an op event WITH its kind; /admin/ops/recent returns it.
#[tokio::test]
async fn op_feed_tapped_on_commit() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let db = common::fresh_db(&state).await;

    let mint: serde_json::Value =
        common::admin_post(addr, "/admin/mint-token", serde_json::json!({"db": db, "name": "t"})).await.json().await?;
    let token = mint["token"].as_str().unwrap().to_string();
    let resp = reqwest::Client::new().post(format!("http://{addr}/api/mutate"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"db": db, "txn": {"steps":[{"op":"insert","table":"projects","doc":{"name":"p","status":"active","tags":[],"updatedAt":0}}]}}))
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value =
        common::admin_get(addr, &format!("/admin/ops/recent?db={db}&n=10")).await.json().await?;
    let ops = body["ops"].as_array().expect("ops array");
    let ours = ops.iter().find(|o| o["table"] == "projects").expect("projects op event missing");
    assert_eq!(ours["kind"], "insert", "kind should be 'insert': {ours}");
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** → route 404 / no events.

- [ ] **Step 3: Add the tap** — in `handle_mutate` (`committer.rs`), after the `ctx.subs.fan_out(...).await;` line:

```rust
    ctx.op_feed.publish(&ctx.db, owner.as_deref(), &outcome.write_set.ops).await;
```

In `handle_scheduled` (`committer.rs`), after its `ctx.subs.fan_out(...).await;` (inside `Ok(outcome) =>`):

```rust
            ctx.op_feed.publish(&ctx.db, None, &outcome.write_set.ops).await;
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
    let ops = state.op_feed.recent(params.db.as_deref(), params.table.as_deref(), params.n.min(500)).await;
    Ok(Json(OpsRecentResponse { ops }))
}
```

Register in `admin_routes()`: `.route("/admin/ops/recent", get(ops_recent))`.

- [ ] **Step 5: Verify it passes + gate + commit**:

```bash
git add server/src/committer.rs server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): op-feed tap at commit sites + GET /admin/ops/recent (#18 phase 3b)"
```

---

## Task 4: `WS /admin/stream` (replay + live + periodic gauges)

**Files:** `server/src/admin.rs`. Test: `dashboard_test.rs`.

- [ ] **Step 1: Failing test** — append to `dashboard_test.rs` (the admin-gate-at-upgrade check):

```rust

// /admin/stream rejects a missing bearer at the upgrade (no 101).
#[tokio::test]
async fn admin_stream_requires_admin() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;
    let resp = reqwest::Client::new().get(format!("http://{addr}/admin/stream")).send().await?;
    let status = resp.status();
    assert!(status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN,
        "missing bearer must be rejected, got {status}");
    Ok(())
}
```

- [ ] **Step 2: Verify it fails** → route 404.

- [ ] **Step 3: Add the upgrade handler** — in `server/src/admin.rs` (add imports `use axum::extract::ws::{WebSocket, WebSocketUpgrade};` and `use std::time::Duration;` — `Response` is already imported):

```rust
#[derive(Deserialize)]
struct StreamParams { db: Option<String>, table: Option<String> }

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
    for ev in state.op_feed.recent(db.as_deref(), table.as_deref(), 200).await {
        if send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev})).await.is_err() { return; }
    }
    let mut rx = state.op_feed.subscribe();
    let mut gauge_tick = tokio::time::interval(Duration::from_secs(1));
    gauge_tick.tick().await; // skip immediate
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Ok(ev) = ev else { break };
                if db.as_deref().map_or(true, |d| ev.db == d) && table.as_deref().map_or(true, |t| ev.table == t) {
                    if send_stream_json(&mut socket, &serde_json::json!({"kind":"op","event":ev})).await.is_err() { break; }
                }
            }
            _ = gauge_tick.tick() => {
                let snap = state.metrics.snapshot(&state.pool, &state.subs, state.started_at).await;
                if send_stream_json(&mut socket, &serde_json::json!({"kind":"gauges","gauges":snap})).await.is_err() { break; }
            }
        }
    }
}

async fn send_stream_json(socket: &mut WebSocket, value: &serde_json::Value) -> Result<(), axum::Error> {
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
- `WriteSet.ops` records `OpKind` per write; commits publish one `OpEvent{table,docId,kind,owner,ts}` per doc (handle_mutate + handle_scheduled); ring replays on connect; `/admin/stream` streams live + 1s gauges, admin-gated; `GET /admin/ops/recent` reads the ring.
- `make checkall` green; `subs.rs` point-read still works (docs unchanged); single-writer + commit ordering intact (publish after commit, non-fatal).

## Next: Phase 4 (config + dynamic CORS) — separate plan
