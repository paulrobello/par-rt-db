# Per-State Presence TTL — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional per-state TTL to the realtime presence layer so a connection's `state` in a room auto-clears to `null` a bounded duration after its last `presenceState` refresh, and the change broadcasts — without explicit client clears.

**Architecture:** `ttlMs` rides on the `presenceState` (update) frame only. Each `Session` gains `expires_at: Option<i64>`; `update_state` arms/clears it. A new `expire_once()` step at the top of the existing `run_flush_task` loop clears expired sessions' state to `null`, marks their rooms dirty, and the existing `flush_once()` broadcasts the snapshot — no new task, no per-session timer. Expiry clears the *state blob*; the member stays (disconnect/leave still own membership removal). Backward compatible: ttl omitted = today's behavior.

**Tech Stack:** Rust (axum/tokio/sqlx), TypeScript (ts-client + React, bun/vitest), Rust (rust-client), Python (python-client, uv/pytest/pyright). Server tests use the shared harness over a real Postgres on `127.0.0.1:55434`.

**Spec:** `docs/superpowers/specs/2026-08-06-presence-ttl-design.md` (read it first; every task implicitly includes its requirements).

## Global Constraints

- **Wire contract is four-implementations-identical.** `ttlMs` (`Option<u64>`, camelCase) is added to the `presenceState` frame only in `server/src/protocol.rs:63`, `ts-client/src/protocol.ts:300`, `rust-client/src/wire.rs:63`, `python-client/src/par_rt_db/wire.py:406`. Omitted = absent on the wire (`skip_serializing_if = "Option::is_none"` / drop-when-None). Match casing exactly.
- **`presenceState` only — NOT the join `presence` frame.** ttl is a refresh concept; joins stay permanent until an update arms a ttl. This avoids reconnect-replay cache changes in all three reactive clients.
- **Clear-state-to-null, not remove-member.** Expiry sets `state = null`; membership persists until `leavePresence`/disconnect (acceptance criteria 1+2).
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`. No SQL in presence code.
- **Single-writer invariant preserved** — presence still routes through no committer; `expire_once` is a sibling-reactive path like `flush_once`.
- **Definition of done:** `make checkall` green across all five packages (fmt-check + clippy `-D warnings` + typecheck + tests). `make dev-db-up` required (integration tests hit a real DB).
- **Commits:** one atomic, well-described commit per task after its gate is green. No pushes unless asked.
- **Docs in sync:** annotate the FEATURE_MATRIX presence row + the parent ENH-015 spec's shipped note as part of Task 8.

## File Structure

**Server:**
- `server/src/protocol.rs` (modify) — `ttl_ms` on `PresenceState`.
- `server/src/config.rs` (modify) — `RTDB_PRESENCE_MAX_TTL_MS` knob.
- `server/src/presence.rs` (modify) — `Session.expires_at`, `update_state` ttl, `expire_once`, flush-loop wiring, `PresenceConfig.max_ttl_ms`.
- `server/src/ws.rs` (modify) — thread `ttl_ms` in the `PresenceState` arm.
- `server/src/metrics.rs` (modify) — `rtdb_presence_ttl_expiries_total` counter.
- `server/tests/common/mod.rs` (modify) — `test_config()` + `from_env` test var cleanup.

**Clients (parallel, disjoint dirs):**
- `ts-client/src/{protocol.ts,client.ts,react.tsx,in_memory.ts}` + tests.
- `rust-client/src/{wire.rs,ws.rs,in_memory.rs}` + tests.
- `python-client/src/par_rt_db/{wire.py,ws_client.py,in_memory.py}` + tests.

**Docs:** `FEATURE_MATRIX.md`, `docs/superpowers/specs/2026-08-06-presence-design.md`.

**Task dependency:** Tasks 1→4 (server) are sequential. Tasks 5–7 (clients) run **in parallel** after Task 4 (server green confirms the wire shape) — disjoint packages. Task 8 (docs) lands last.

---

### Task 1: Wire field — `ttl_ms` on `PresenceState`

**Files:**
- Modify: `server/src/protocol.rs:63-66` (the `PresenceState` variant).
- Test: `server/src/protocol.rs` `#[cfg(test)] mod tests` (extend the presence wire-tag tests).

**Interfaces:**
- Consumes: nothing new.
- Produces: `ClientMessage::PresenceState { room, state, ttl_ms: Option<u64> }` — consumed by Task 3 (`update_state`) and Task 4 (`ws.rs`), and mirrored by Tasks 5–7.

- [ ] **Step 1: Write the failing wire-tag test**

Add to the `tests` module in `server/src/protocol.rs` (next to the existing `presence_*` wire-tag tests):

```rust
#[test]
fn presence_state_ttl_ms_wire_tag() {
    // ttlMs omitted when None (backward compatible — unchanged shape)
    assert_eq!(
        serde_json::to_value(ClientMessage::PresenceState {
            room: "doc:1".to_string(),
            state: serde_json::json!({"typing": true}),
            ttl_ms: None,
        })
        .unwrap(),
        serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
    );
    // ttlMs present when Some
    assert_eq!(
        serde_json::to_value(ClientMessage::PresenceState {
            room: "doc:1".to_string(),
            state: serde_json::json!({"typing": true}),
            ttl_ms: Some(3000),
        })
        .unwrap(),
        serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 3000})
    );
    // and it deserializes back
    let parsed: ClientMessage =
        serde_json::from_str(r#"{"type":"presenceState","room":"doc:1","state":{},"ttlMs":500}"#).unwrap();
    match parsed {
        ClientMessage::PresenceState { ttl_ms, .. } => assert_eq!(ttl_ms, Some(500)),
        _ => panic!("expected PresenceState"),
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --manifest-path server/Cargo.toml presence_state_ttl_ms_wire_tag`
Expected: FAIL — `PresenceState` has no `ttl_ms` field (the struct literal won't compile).

- [ ] **Step 3: Add the field**

In `server/src/protocol.rs`, change the `PresenceState` variant (currently lines 63-66) from:

```rust
    PresenceState {
        room: String,
        state: serde_json::Value,
    },
```

to:

```rust
    PresenceState {
        room: String,
        state: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
```

(The enum already carries `rename_all = "camelCase"` at `protocol.rs:10`, so `ttl_ms` serializes as `ttlMs`. `skip_serializing_if` keeps omitted ttl off the wire. The `Presence` join variant is deliberately unchanged.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --manifest-path server/Cargo.toml presence_` then `cargo build --manifest-path server/Cargo.toml`
Expected: PASS (the new test + all existing presence wire-tag tests still pass — omitted ttl round-trips identically). Note: `ws.rs` and `presence.rs` will fail to compile until Tasks 3–4 thread the new field; that is expected — this task's scope is the protocol unit test + the type. If the workspace won't build, run `cargo test --manifest-path server/Cargo.toml --lib protocol::tests::presence_state_ttl_ms_wire_tag` (lib-only, the test is self-contained).

- [ ] **Step 5: Commit**

```bash
git add server/src/protocol.rs
git commit -m "feat(protocol): ENH-015 ttl — ttlMs field on presenceState frame"
```

---

### Task 2: Config knob + metrics counter

**Files:**
- Modify: `server/src/config.rs` (struct ~line 118; `from_env` ~line 332; struct tail ~line 380; the `from_env` test ~line 724).
- Modify: `server/src/presence.rs` (`PresenceConfig` ~line 17, `from_config` ~line 29).
- Modify: `server/src/metrics.rs` (counter field ~line 133; recorder ~line 202; snapshot field ~line 419; populate ~line 371; Prometheus render ~line 545; test literal ~line 625/679; metrics test ~line 817).
- Modify: `server/tests/common/mod.rs` (`test_config()` literal).

**Interfaces:**
- Consumes: nothing.
- Produces: `Config.presence_max_ttl_ms: u64`, `PresenceConfig.max_ttl_ms: u64`, `Metrics::record_presence_ttl_expiry()` + `rtdb_presence_ttl_expiries_total`. Consumed by Task 3.

- [ ] **Step 1: Add the config field and parse**

In `server/src/config.rs`, after `presence_update_limit_per_sec` (struct line ~118):

```rust
    /// RTDB_PRESENCE_MAX_TTL_MS (default 300000 = 5 min). Upper bound on a
    /// client-supplied presenceState ttlMs; over-cap is rejected (no clamping).
    pub presence_max_ttl_ms: u64,
```

In `Config::from_env`, after the `presence_update_limit_per_sec` parse (line ~332):

```rust
        let presence_max_ttl_ms = std::env::var("RTDB_PRESENCE_MAX_TTL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300_000)
            .max(1000);
```

(`.max(1000)` floors at 1 s so a misconfigured cap can't make ttl unusable; the default 5 min is generous for typing/cursor refresh patterns.)

Add `presence_max_ttl_ms,` to the `Config { ... }` literal tail (after `presence_update_limit_per_sec,` at line ~380).

In the `from_env` test `presence_env_defaults_and_overrides` (~line 724), add `std::env::remove_var("RTDB_PRESENCE_MAX_TTL_MS");` to the before/after cleanup block (it already removes every other `RTDB_PRESENCE_*` var), and add an assertion `assert_eq!(c.presence_max_ttl_ms, 300_000);` to the defaults section.

- [ ] **Step 2: Mirror into `PresenceConfig`**

In `server/src/presence.rs`, add to the `PresenceConfig` struct (line ~17, after `update_limit_per_sec`):

```rust
    pub max_ttl_ms: u64,
```

In `PresenceConfig::from_config` (line ~29), add:

```rust
            max_ttl_ms: c.presence_max_ttl_ms,
```

- [ ] **Step 3: Add the metrics counter**

In `server/src/metrics.rs`, add the field to `Metrics` (after `presence_broadcasts_total` at line ~133):

```rust
    /// Total presence sessions whose per-state TTL expired (state cleared to null).
    presence_ttl_expiries_total: AtomicU64,
```

(`Metrics` derives `Default` at line 117, so the new `AtomicU64` auto-zeros — no manual `Default`.)

Add recorder + accessor (after `record_presence_broadcast`/`presence_broadcasts_total` at lines 199-208):

```rust
    pub fn record_presence_ttl_expiry(&self) {
        self.presence_ttl_expiries_total
            .fetch_add(1, Ordering::Relaxed);
    }
    pub fn presence_ttl_expiries_total(&self) -> u64 {
        self.presence_ttl_expiries_total.load(Ordering::Relaxed)
    }
```

Add to `MetricsSnapshot` (after `presence_broadcasts_total` at line ~419):

```rust
    /// Presence sessions whose per-state TTL expired (ENH-015 follow-up).
    pub presence_ttl_expiries_total: u64,
```

Populate it in `snapshot()` (after `presence_broadcasts_total` at line ~370):

```rust
            presence_ttl_expiries_total: self.presence_ttl_expiries_total.load(Ordering::Relaxed),
```

Render it in the Prometheus export (after the `rtdb_presence_broadcasts_total` block, ~line 545):

```rust
    s.push_str("# HELP rtdb_presence_ttl_expiries_total Presence sessions whose per-state TTL expired.\n");
    s.push_str("# TYPE rtdb_presence_ttl_expiries_total counter\n");
    s.push_str(&format!(
        "rtdb_presence_ttl_expiries_total {}\n",
        snap.presence_ttl_expiries_total
    ));
```

Update the two `MetricsSnapshot` test literals (lines ~625 and ~679) to include `presence_ttl_expiries_total: 0,`. Extend the `presence_counters_record` test (~line 817) with `m.record_presence_ttl_expiry();` and `assert_eq!(snap.presence_ttl_expiries_total, 1);`.

- [ ] **Step 4: Update `test_config()`**

In `server/tests/common/mod.rs`, add `presence_max_ttl_ms: 300_000,` to the `test_config()` literal (next to the other `presence_*` fields).

- [ ] **Step 5: Build + test + commit**

Run: `cargo test --manifest-path server/Cargo.toml --lib config::tests::presence_env_defaults_and_overrides metrics::tests::presence_counters_record` then `cargo build --manifest-path server/Cargo.toml` then `cargo clippy --manifest-path server/Cargo.toml -- -D warnings`
Expected: PASS + clean. (`presence.rs`/`ws.rs` may still not compile until Task 3/4 — if the workspace build fails on those, scope the build to `cargo build --manifest-path server/Cargo.toml --lib` once Task 3 lands; the config/metrics lib units compile independently.)

```bash
git add server/src/config.rs server/src/presence.rs server/src/metrics.rs server/tests/common/mod.rs
git commit -m "feat(presence): ENH-015 ttl — RTDB_PRESENCE_MAX_TTL_MS knob + expiry counter"
```

---

### Task 3: Presence expiry core — `expires_at`, `update_state` ttl, `expire_once`, flush-loop wiring

**Files:**
- Modify: `server/src/presence.rs` (`Session` line 42; `update_state` line 184; new `expire_once`; `run_flush_task` loop line 409; `cfg()` test helper line 432).

**Interfaces:**
- Consumes: Task 1 (`ttl_ms`), Task 2 (`max_ttl_ms`, `record_presence_ttl_expiry`).
- Produces: `update_state(..., ttl_ms: Option<u64>)`, `pub async fn expire_once(&self) -> bool`. Consumed by Task 4 (`ws.rs` + integration test).

- [ ] **Step 1: Write the failing unit tests**

Add to the `tests` module in `server/src/presence.rs`. These drive `expire_once()` + `flush_once()` directly (deterministic — no wall-clock wait beyond a short ttl sleep). First extend the `cfg()` helper to include `max_ttl_ms`:

```rust
    fn cfg() -> PresenceConfig {
        PresenceConfig {
            enabled: true,
            max_state_bytes: 1024,
            max_room_size: 100,
            max_rooms_per_conn: 32,
            max_room_bytes: 256,
            broadcast_interval_ms: 0,
            update_limit_per_sec: 20,
            max_ttl_ms: 300_000,
        }
    }
```

Then add these tests:

```rust
    #[tokio::test]
    async fn update_state_arms_ttl_that_expires_to_null_not_remove() {
        let m = mgr();
        let (t1, _r1) = tx();
        let (t2, mut r2) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t1).await.unwrap();
        m.join("db", 2, "room", None, user("b@b.com"), t2).await.unwrap();
        // conn 1 arms a 60ms ttl on its typing state.
        m.update_state("db", 1, "room", serde_json::json!({"typing": true}), Some(60))
            .await.unwrap();
        // member is still present, state is the blob.
        let members = m.snapshot("db", "room").await;
        assert_eq!(members.iter().find(|x| x.connection_id == "1").unwrap().state,
                   serde_json::json!({"typing": true}));
        assert_eq!(members.len(), 2);
        // wait out the ttl, then expire.
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let expired = m.expire_once().await;
        assert!(expired);
        // state cleared to null, but the MEMBER is still present.
        let members = m.snapshot("db", room()).await; // BUG-BAIT: see note
        let conn1 = members.iter().find(|x| x.connection_id == "1").unwrap();
        assert_eq!(conn1.state, serde_json::Value::Null);
        assert_eq!(members.len(), 2, "expiry clears state, not membership");
        // and the dirty room broadcast a snapshot with null state.
        m.flush_once().await;
        let msg = r2.try_recv().expect("snapshot after expiry");
        let ServerMessage::PresenceSnapshot { members, .. } = msg else { panic!("snapshot") };
        let conn1 = members.iter().find(|x| x.connection_id == "1").unwrap();
        assert_eq!(conn1.state, serde_json::Value::Null);
    }

    #[tokio::test]
    async fn omitted_ttl_is_permanent_and_does_not_expire() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t).await.unwrap();
        // update with NO ttl -> permanent.
        m.update_state("db", 1, "room", serde_json::json!({"x": 1}), None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let expired = m.expire_once().await;
        assert!(!expired, "no ttl armed -> nothing expires");
        let members = m.snapshot("db", "room").await;
        assert_eq!(members[0].state, serde_json::json!({"x": 1}));
    }

    #[tokio::test]
    async fn ttl_refresh_re_arms_and_a_non_ttl_update_clears_it() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t).await.unwrap();
        m.update_state("db", 1, "room", serde_json::json!({"t": true}), Some(200)).await.unwrap();
        // a later update with NO ttl clears the expiry -> permanent.
        m.update_state("db", 1, "room", serde_json::json!({"t": false}), None).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(220)).await;
        let expired = m.expire_once().await;
        assert!(!expired, "ttl was cleared by the non-ttl update");
        assert_eq!(m.snapshot("db", "room").await[0].state, serde_json::json!({"t": false}));
    }

    #[tokio::test]
    async fn ttl_validation_rejects_zero_and_over_cap() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t).await.unwrap();
        let err = m.update_state("db", 1, "room", serde_json::json!({}), Some(0)).await.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
        let err = m.update_state("db", 1, "room", serde_json::json!({}), Some(300_001)).await.unwrap_err();
        assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn expire_once_with_nothing_expired_is_idle() {
        let m = mgr();
        let (t, _r) = tx();
        m.join("db", 1, "room", None, user("a@b.com"), t).await.unwrap();
        assert!(!m.expire_once().await);
    }
```

**BUG-BAIT note for the implementer:** in `update_state_arms_ttl_that_expires_to_null_not_remove` I wrote `m.snapshot("db", room())` by mistake — it must be `m.snapshot("db", "room")` (the literal room key). Fix that typo; there is no `room()` helper.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --manifest-path server/Cargo.toml --lib presence::tests`
Expected: FAIL — `update_state` takes no `ttl_ms`, `expire_once` missing, `Session` has no `expires_at`, `cfg()` missing `max_ttl_ms`.

- [ ] **Step 3: Add `expires_at` to `Session`**

In `server/src/presence.rs`, change the `Session` struct (lines 42-49) to add the field:

```rust
struct Session {
    user: AuthedUser,
    state: serde_json::Value,
    tx: UnboundedSender<ServerMessage>,
    #[allow(dead_code)] // read in Task 4's broadcast path
    joined_at: i64,
    updated_at: i64,
    /// Absolute epoch-ms at which `state` should be cleared to null by
    /// `expire_once`. `None` = permanent (no ttl armed). Joins never arm it
    /// (ttl rides on `presenceState` only); only `update_state` sets/clears it.
    expires_at: Option<i64>,
}
```

Update the `Session { ... }` literal inside `join` (lines 164-173) to add `expires_at: None,` (joins are permanent).

- [ ] **Step 4: Thread `ttl_ms` through `update_state`**

Change the `update_state` signature (line 184) to add the param, validate early, and arm/clear `expires_at` at the mutation site. The full new method:

```rust
    pub async fn update_state(
        &self,
        db: &str,
        conn: ConnId,
        room: &str,
        state: serde_json::Value,
        ttl_ms: Option<u64>,
    ) -> Result<(), RtDbError> {
        if !self.config.enabled {
            return Err(RtDbError::forbidden("presence not enabled"));
        }
        Self::validate_state(&state, &self.config)?;
        // ttl validation (early reject, alongside the state-size check, before
        // the rate-limit window is touched): 0 is nonsensical, over-cap is
        // rejected — never clamped (presence's "no silent clamping" rule).
        if let Some(t) = ttl_ms {
            if t == 0 {
                return Err(RtDbError::bad_request("ttl must be positive"));
            }
            if t > self.config.max_ttl_ms {
                return Err(RtDbError::bad_request("ttl exceeds maximum"));
            }
        }
        let shard = self.shard(db).await;
        let mut p = shard.lock().await;
        // per-conn update rate limit (membership required)
        if !p.rooms.get(room).is_some_and(|m| m.contains_key(&conn)) {
            return Err(RtDbError::bad_request("not present in room"));
        }
        let limit = self.config.update_limit_per_sec;
        let now = crate::db::now_ms();
        let win = p.update_window.entry(conn).or_insert((now, 0));
        if now - win.0 >= 1000 {
            *win = (now, 0);
        }
        win.1 += 1;
        if win.1 > limit {
            let retry = (((win.0 + 1000) - now) / 1000).max(0) as u32;
            return Err(RtDbError::rate_limited(retry.max(1)));
        }
        let Some(room_map) = p.rooms.get_mut(room) else {
            return Err(RtDbError::bad_request("not present in room"));
        };
        let Some(sess) = room_map.get_mut(&conn) else {
            return Err(RtDbError::bad_request("not present in room"));
        };
        sess.state = state;
        sess.updated_at = now;
        sess.expires_at = ttl_ms.map(|t| now + t as i64); // arm or clear
        drop(p);
        self.mark_dirty(db, room).await;
        Ok(())
    }
```

- [ ] **Step 5: Add `expire_once`**

Add this method to `impl PresenceManager` (place it just before `flush_once`, ~line 341):

```rust
    /// Clear every session whose armed TTL has elapsed: set its `state` to
    /// `null`, clear its expiry, mark each affected room dirty, and count one
    /// expiry per session. Called at the top of every flush-tick (before
    /// `flush_once`) so the coalesced broadcast picks up the null states.
    /// Returns `true` if any session expired (so the immediate-mode flush loop
    /// can yield instead of sleeping). Membership is never removed here — only
    /// `leavePresence`/disconnect do that.
    pub async fn expire_once(&self) -> bool {
        let now = crate::db::now_ms();
        let shards: Vec<(String, Arc<Mutex<DbPresence>>)> = {
            let dbs = self.dbs.lock().await;
            dbs.iter().map(|(db, s)| (db.clone(), s.clone())).collect()
        };
        let mut expired_any = false;
        for (db, shard) in shards {
            let mut dirty_rooms: Vec<String> = Vec::new();
            {
                let mut p = shard.lock().await;
                for (room, members) in p.rooms.iter_mut() {
                    let mut room_expired = false;
                    for sess in members.values_mut() {
                        if let Some(exp) = sess.expires_at {
                            if exp <= now {
                                sess.state = serde_json::Value::Null;
                                sess.expires_at = None;
                                room_expired = true;
                                expired_any = true;
                                if let Some(m) = &self.metrics {
                                    m.record_presence_ttl_expiry();
                                }
                            }
                        }
                    }
                    if room_expired {
                        dirty_rooms.push(room.clone());
                    }
                }
            }
            // mark_dirty takes only the dirty lock — never hold the shard lock across it.
            for room in &dirty_rooms {
                self.mark_dirty(&db, room).await;
            }
        }
        expired_any
    }
```

- [ ] **Step 6: Wire `expire_once` into the flush loop**

In `run_flush_task` (lines 409-420), change the loop body to call `expire_once()` before `flush_once()` in both branches:

```rust
            loop {
                if let Some(t) = ticker.as_mut() {
                    t.tick().await;
                    let _ = this.expire_once().await;
                    let _ = this.flush_once().await;
                } else {
                    // interval == 0: cooperative when busy, 1ms sleep when idle
                    let expired = this.expire_once().await;
                    if this.flush_once().await || expired {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                }
            }
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `cargo test --manifest-path server/Cargo.toml --lib presence::tests` then `cargo clippy --manifest-path server/Cargo.toml -- -D warnings`
Expected: all PASS (existing tests still pass — `update_state` callers in tests must now pass the `ttl_ms` arg; update the existing `update_state(...)` call sites in the `tests` module to add `, None`). No warnings.

**Implementer note:** the existing tests in this file call `m.update_state("db", 1, "room", json!(...))` (4 args). After the signature change they must be `m.update_state("db", 1, "room", json!(...), None)` (5 args). Fix every existing call site in the `#[cfg(test)]` module (grep for `update_state`).

- [ ] **Step 8: Commit**

```bash
git add server/src/presence.rs
git commit -m "feat(presence): ENH-015 ttl — per-state expiry (expire_once) wired into flush loop"
```

---

### Task 4: WS threading + integration test

**Files:**
- Modify: `server/src/ws.rs:614-630` (the `PresenceState` arm).
- Modify: `server/tests/presence_test.rs` (add a TTL integration test).

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: end-to-end ttl over `/sync`; confirms the wire shape clients mirror.

- [ ] **Step 1: Thread `ttl_ms` in the `PresenceState` arm**

In `server/src/ws.rs`, change the `PresenceState` arm (lines 614-630) to destructure and pass `ttl_ms`:

```rust
        ClientMessage::PresenceState {
            room,
            state: presence_state,
            ttl_ms,
        } => {
            match state
                .realtime
                .presence
                .update_state(db, conn_id, &room, presence_state, ttl_ms)
                .await
            {
                Ok(()) => state.runtime.metrics.record_presence_update(),
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::PresenceErr { room, error });
                }
            }
            false
        }
```

(The `Presence` join arm at 586-613 is unchanged — joins carry no ttl.)

- [ ] **Step 2: Write the integration test**

Add to `server/tests/presence_test.rs` (using the existing helpers `test_state_with_presence`, `spawn_app`, `fresh_db`, `mint_user_for_db`, `allowlist`, `ws_connect`, `auth`, `send_json`, `drain_until_snapshot` defined at lines 1-95). `test_state_with_presence` sets `broadcast_interval_ms = 0`; `AppState::new` spawns the flush task, so `expire_once`+`flush_once` run continuously — but for determinism the test drives them explicitly after a short real-time wait:

```rust
/// A presenceState ttl clears the sender's state to null (member still present),
/// observed by the peer via a presenceSnapshot.
#[tokio::test]
async fn ttl_expires_state_to_null_member_remains() {
    let state = test_state_with_presence().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let (token_a, email_a) = mint_user_for_db(&state, &db, "a").await;
    let (token_b, email_b) = mint_user_for_db(&state, &db, "b").await;
    allowlist(addr, &db, &[&email_a, &email_b]).await;
    let mut wa = ws_connect(addr).await;
    let mut wb = ws_connect(addr).await;
    assert_eq!(auth(&mut wa, &token_a, &db)["type"], json!("authOk"));
    assert_eq!(auth(&mut wb, &token_b, &db)["type"], json!("authOk"));

    // both join -> 2 members
    send_json(&mut wa, json!({"type": "presence", "room": "doc:1"})).await;
    send_json(&mut wb, json!({"type": "presence", "room": "doc:1"})).await;
    state.realtime.presence.flush_once().await;
    let _ = drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;

    // conn A arms a 100ms ttl on its typing state.
    send_json(&mut wa, json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 100})).await;
    state.realtime.presence.flush_once().await;
    // peer observes typing:true
    let snap = drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;
    let a = snap["members"].as_array().unwrap().iter()
        .find(|m| m["connectionId"] != json!(<String as std::str::FromStr>::from_str /* unused */ ).to_string()).unwrap();
    // (assert conn A's state has typing:true — find by counting, both are present)
    let typing = snap["members"].as_array().unwrap()
        .iter().any(|m| m["state"]["typing"] == json!(true));
    assert!(typing, "peer saw typing:true before ttl");

    // wait out the ttl, then drive expiry + flush explicitly.
    tokio::time::sleep(std::time::Duration::from_millis(140)).await;
    state.realtime.presence.expire_once().await;
    state.realtime.presence.flush_once().await;

    // peer now sees conn A's state as null, but A is still a member (2 members).
    let snap = drain_until_snapshot(&mut wb, "doc:1", |n| n == 2).await;
    let members = snap["members"].as_array().unwrap();
    assert_eq!(members.len(), 2, "expiry clears state, not membership");
    // exactly one member has state null (the expired typing one); the typing flag is gone.
    assert!(members.iter().any(|m| m["state"].is_null()));
    assert!(members.iter().all(|m| m["state"]["typing"] != json!(true)));
}
```

**Implementer note (BUG-BAIT):** I left a broken `FromStr` expression in the `let a = ...` line as a placeholder — **delete that line entirely**; the real assertion is the `typing` bool computed just below it (`let typing = snap["members"]...any(|m| m["state"]["typing"] == json!(true))`). The `drain_until_snapshot` helper returns the matching snapshot as a `serde_json::Value`; confirm its exact return shape by reading `presence_test.rs:67` and assert against that shape (it returns the full `presenceSnapshot` `Value`). Both members are present and indistinguishable by identity here, so assert on the *aggregate* (one null state, no typing flag) rather than per-connectionId.

- [ ] **Step 3: Run the integration suite**

Run: `make dev-db-up && cargo test --manifest-path server/Cargo.toml --test presence_test ttl_expires_state_to_null_member_remains`
Expected: PASS. (Real-time 100 ms ttl + 140 ms sleep — generous slack.)

- [ ] **Step 4: Full server gate**

Run: `cargo test --manifest-path server/Cargo.toml` then `cargo clippy --manifest-path server/Cargo.toml -- -D warnings` then `cargo fmt --manifest-path server/Cargo.toml -- --check`
Expected: green. The server half of the TTL follow-up is done.

- [ ] **Step 5: Commit**

```bash
git add server/src/ws.rs server/tests/presence_test.rs
git commit -m "feat(ws): ENH-015 ttl — thread ttlMs + /sync expiry integration test"
```

---

> **Parallelization point:** Tasks 5, 6, 7 touch three disjoint client packages and run as parallel subagents after Task 4 lands (server wire shape confirmed green). Each mirrors `ttlMs` on `presenceState` only, exposes it on the update method, models expiry in the in-memory harness, and adds a test. Each is independent.

### Task 5: ts-client mirror

**Files:**
- Modify: `ts-client/src/protocol.ts:300` (the `presenceState` union member).
- Modify: `ts-client/src/client.ts:471-478` (`updatePresence`).
- Modify: `ts-client/src/react.tsx:122-150` (`usePresence` returned `updatePresence`).
- Modify: `ts-client/src/in_memory.ts` (`PresenceRooms.update` 136-144 + new `expire`; client `updatePresence` 1265-1270).
- Test: `ts-client/tests/presence.test.ts` (wire-type assertions ~67-132), `ts-client/tests/in_memory.test.ts` (presence ~415-492).

**Interfaces:**
- Consumes: Task 1 wire shape.
- Produces: parity ts-client ttl surface.

**Read first:** `ts-client/src/protocol.ts`, `src/client.ts` (the `updatePresence`/`presence` methods + the wire-type round-trip tests), `src/in_memory.ts` (`PresenceRooms`).

- [ ] **Step 1: Wire type** — in `src/protocol.ts:300` change:

```typescript
  | { type: "presenceState"; room: string; state: unknown }
```
to:
```typescript
  | { type: "presenceState"; room: string; state: unknown; ttlMs?: number }
```

Update the wire-type round-trip assertions in `tests/presence.test.ts` (~lines 67-132, the `satisfies ClientMessage` literals) so a `presenceState` with `ttlMs` type-checks and one without still omits it. Add an explicit assertion that the serialized `presenceState` omits `ttlMs` when unset and includes it when set (mirror the server test).

- [ ] **Step 2: Reactive `updatePresence`** — in `src/client.ts:471-478`, add the trailing optional param and put it on the wire:

```typescript
  updatePresence(room: string, state: unknown, ttlMs?: number): void {
    if (this.joinedRooms.has(room)) {
      this.joinedRooms.set(room, state);
    }
    if (this.authState === "authenticated") {
      const frame: ClientMessage =
        ttlMs == null
          ? { type: "presenceState", room, state }
          : { type: "presenceState", room, state, ttlMs };
      this.send(frame);
    }
  }
```

(`presence()` join is unchanged — ttl is refresh-only.)

- [ ] **Step 3: `usePresence` hook** — in `src/react.tsx:122-150`, widen the returned `updatePresence` to forward ttl:

```typescript
  const updatePresence = useCallback(
    (state: unknown, ttlMs?: number) => client.updatePresence(room, state, ttlMs),
    [client, room],
  );
```
and update the hook's return type signature (`updatePresence: (state: unknown, ttlMs?: number) => void`).

- [ ] **Step 4: In-memory harness expiry** — in `src/in_memory.ts`, give `PresenceRooms` (lines 112-192) a parallel expiry map + an `expire(now)` sweep. Add a field and a `now` source (default `Date.now`, overridable for tests), and extend `update`:

```typescript
export class PresenceRooms {
  private readonly members = new Map<string, Map<string, PresenceMember>>();
  private readonly subs = new Map<string, Set<(members: PresenceMember[]) => void>>();
  private readonly expiry = new Map<string, Map<string, number>>(); // room -> connId -> expiresAtMs

  // existing snapshot/join/leave/subscribe/fanOut unchanged...

  update(room: string, connectionId: string, state: unknown, ttlMs?: number, now: number = Date.now()): void {
    const roomMap = this.members.get(room);
    if (!roomMap || !roomMap.has(connectionId)) return;
    roomMap.get(connectionId)!.state = state ?? null;
    let exp = this.expiry.get(room);
    if (!exp) { exp = new Map(); this.expiry.set(room, exp); }
    if (ttlMs && ttlMs > 0) exp.set(connectionId, now + ttlMs);
    else exp.delete(connectionId);
    this.fanOut(room);
  }

  /** Clear expired members' state to null + fan out. Returns true if anything expired. */
  expire(now: number = Date.now()): boolean {
    let any = false;
    const touched: string[] = [];
    for (const [room, exp] of this.expiry) {
      const roomMap = this.members.get(room);
      if (!roomMap) { this.expiry.delete(room); continue; }
      for (const [connId, at] of exp) {
        if (at <= now) {
          const m = roomMap.get(connId);
          if (m) { m.state = null; any = true; }
          exp.delete(connId);
        }
      }
      if (any) touched.push(room);
    }
    for (const room of touched) this.fanOut(room);
    return any;
  }
}
```

Also: in `leave` (147-157) delete `this.expiry.get(room)?.delete(connectionId)` so a re-join doesn't inherit a stale expiry. In the in-memory client's `updatePresence` (1265-1270), pass ttl through:

```typescript
  updatePresence(room: string, state: unknown, ttlMs?: number): void {
    if (!this.joinedRooms.has(room)) return;
    this.presenceRooms.update(room, this.connectionId, state, ttlMs);
  }
```

- [ ] **Step 5: Tests + gate** — add a `presence.test.ts` case asserting `updatePresence("r", {typing:true}, 3000)` sends `{type:"presenceState", room:"r", state:{typing:true}, ttlMs:3000}` (via the `FakeSocket`), and an `in_memory.test.ts` case: two clients share a `PresenceRooms`; one `updatePresence(room, {typing:true}, 50)`; advance the harness clock / call `rooms.expire(now+60)`; assert the peer's snapshot shows that member's `state === null` while the member is still listed. Run: `cd ts-client && bunx vitest run` then `bunx tsc --noEmit`. Commit:

```bash
git add ts-client/src ts-client/tests
git commit -m "feat(ts-client): ENH-015 ttl — updatePresence ttlMs + in-memory expiry"
```

---

### Task 6: rust-client mirror

**Files:**
- Modify: `rust-client/src/wire.rs:63-66` (the `PresenceState` variant) + wire-tag tests 1272-1357.
- Modify: `rust-client/src/ws.rs` (`Cmd::PresenceUpdate` ~267-282; `update_presence` ~672; the `Cmd::PresenceUpdate` handling ~1205-1239).
- Modify: `rust-client/src/in_memory.rs` (`PresenceRooms::update` ~250; new `expire`; client `update_presence` ~2178).
- Test: `rust-client/src/wire.rs` tests + `in_memory.rs` presence tests (~7679).

**Interfaces:**
- Consumes: Task 1 wire shape.
- Produces: parity rust-client ttl surface.

**Read first:** `rust-client/src/wire.rs`, the `Cmd` enum + driver `PresenceUpdate` handling in `ws.rs`, `src/in_memory.rs` `PresenceRooms`.

- [ ] **Step 1: Wire type** — in `src/wire.rs:63-66` change `PresenceState` to:

```rust
    PresenceState {
        room: String,
        state: serde_json::Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },
```

Add wire-tag test cases (next to the existing presence assertions at 1272-1357): `PresenceState { ttl_ms: None }` omits `ttlMs`; `ttl_ms: Some(3000)` emits `"ttlMs":3000`; and a deserialize round-trip.

- [ ] **Step 2: `Cmd` + driver** — in `ws.rs`, add `ttl_ms: Option<u64>` to `Cmd::PresenceUpdate` (~line 275). In the driver's `Cmd::PresenceUpdate` handling (~1205-1239), build `ClientMessage::PresenceState { room, state, ttl_ms }`. (The driver replay path for *joins* at 1104-1134 is unchanged — ttl rides on updates, which are not replayed.)

- [ ] **Step 3: `update_presence` method** — at `ws.rs:672`, add the trailing param and thread it into the `Cmd`:

```rust
    pub fn update_presence(&self, room: &str, state: serde_json::Value, ttl_ms: Option<u64>) {
        let joined = {
            let maps = self.inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = maps.by_room.get(room) {
                *existing.state.lock().unwrap_or_else(|p| p.into_inner()) = Some(state.clone());
                true
            } else {
                false
            }
        };
        if joined {
            let _ = self.inner.cmd_tx.send(Cmd::PresenceUpdate {
                room: room.to_string(),
                state,
                ttl_ms,
            });
        }
    }
```

(`presence()` join at 629 is unchanged.)

- [ ] **Step 4: In-memory harness expiry** — in `src/in_memory.rs`, add an expiry field to `PresenceRooms` (185-195) mirroring the ts design: `expiry: HashMap<String, HashMap<String, i64>>` (room -> connId -> expiresAt). Extend `update` (250) to accept `ttl_ms: Option<u64>` + `now: i64` and set/clear the expiry entry; add `pub fn expire(&mut self, now: i64) -> bool` that nulls expired members' `state`, drops their expiry entries, and `fan_out`s affected rooms. In `leave` (262), delete the expiry entry. Update the client's `update_presence` (2178) to pass `ttl_ms` through to `PresenceRooms::update`. (Use the harness's existing time source for `now`; mirror whatever the document-TTL reaper in this file uses. If none, take `now` as a param so tests control it.)

- [ ] **Step 5: Tests + gate** — add a `wire.rs` round-trip and an `in_memory.rs` test (two clients share `PresenceRooms`; one updates with a short ttl; `rooms.expire(now+delta)`; assert peer sees `state == Value::Null` and the member remains). Run: `cd rust-client && cargo test && cargo clippy -- -D warnings`. Commit:

```bash
git add rust-client/src rust-client/tests
git commit -m "feat(rust-client): ENH-015 ttl — update_presence ttlMs + in-memory expiry"
```

---

### Task 7: python-client mirror

**Files:**
- Modify: `python-client/src/par_rt_db/wire.py:406-411` (`_ClientPresenceState`) + its drop-None serializer story.
- Modify: `python-client/src/par_rt_db/ws_client.py:660` (`update_presence`) + the frame construction.
- Modify: `python-client/src/par_rt_db/in_memory.py:275` (`PresenceRooms.update`) + new `expire`; client `update_presence` 1542.
- Test: `python-client/tests/test_wire.py` (or wherever presence wire round-trips live) + the in-memory presence tests.

**Interfaces:**
- Consumes: Task 1 wire shape.
- Produces: parity python-client ttl surface.

**Read first:** `wire.py` (`_Camel` base 33-40, `_ClientPresence`'s `model_serializer` 390-403, `_ClientPresenceState` 406-411), `ws_client.py` (`update_presence` 660), `in_memory.py` (`PresenceRooms` 236, `InMemoryRtDbClientOptions.now` 352).

- [ ] **Step 1: Wire type** — in `wire.py`, add `ttl_ms` to `_ClientPresenceState` (406-411) with drop-when-None treatment. The existing `_ClientPresence` drops only `state` via a custom `model_serializer` (390-403); mirror that for `_ClientPresenceState` dropping `ttl_ms` when None:

```python
class _ClientPresenceState(_Camel):
    """ENH-015 broadcast updated presence state. ``ttlMs`` (ENH-015 follow-up)
    arms a per-state expiry; omitted on the wire when ``None``."""

    type: Literal["presenceState"] = "presenceState"
    room: str
    state: Any
    ttl_ms: int | None = None

    @model_serializer(mode="wrap")
    def _drop_none_ttl(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        out = handler(self)
        if out.get("ttlMs") is None:
            out.pop("ttlMs", None)
        return out
```

(The `_Camel` base's `alias_generator=to_camel` serializes `ttl_ms` as `ttlMs`; `populate_by_name=True` lets tests construct with either spelling.)

- [ ] **Step 2: `update_presence` method** — in `ws_client.py:660`, add the trailing param and put `ttlMs` on the frame:

```python
    def update_presence(self, room: str, state: Any, ttl_ms: int | None = None) -> None:
        rm = self._presence_by_room.get(room)
        if rm is None:
            return
        rm.join_state = state
        if self._state is ConnectionState.CONNECTED:
            frame = _ClientPresenceState(room=room, state=state, ttl_ms=ttl_ms).model_dump_json(by_alias=True)
            asyncio.get_running_loop().create_task(self._send(frame))
```

(`presence()` join at 635 is unchanged; the reconnect-replay `_presence_join_frame` at 935 is unchanged — ttl is not on the join frame.)

- [ ] **Step 3: In-memory harness expiry** — in `in_memory.py`, add an expiry dict to `PresenceRooms` (236-255): `self._expiry: dict[str, dict[str, int]] = {}` (room -> connectionId -> expiresAt). Extend `update` (275) to accept `ttl_ms: int | None` and a `now` (use `self._now()` — the harness already takes `opts.now`; if `PresenceRooms` has no clock, accept `now` as a param). Set/clear the expiry entry; clear it in `leave` (291). Add:

```python
    def expire(self, now: int | None = None) -> bool:
        now = now if now is not None else ...  # use the harness clock; see note
        any_expired = False
        touched: list[str] = []
        for room, by_conn in list(self._expiry.items()):
            members = self._members.get(room)
            if members is None:
                self._expiry.pop(room, None)
                continue
            for conn_id, at in list(by_conn.items()):
                if at <= now:
                    for i, (cid, member) in enumerate(members):
                        if cid == conn_id:
                            member.state = None
                            any_expired = True
                            break
                    by_conn.pop(conn_id, None)
            if any_expired:
                touched.append(room)
        for room in touched:
            self._fan_out(room)
        return any_expired
```

**Implementer note:** `PresenceRooms._members` is `dict[str, list[tuple[str, PresenceMember]]]` (room -> list of (connId, member)) per the agent; iterate the list to find the member by connId (as above). For `now`, the `InMemoryRtDbClient` holds `opts.now`; thread the client's clock into `PresenceRooms` (store it on construction) or pass `now` explicitly from the client's `update_presence`/`expire`. Match how the python harness already advances time for document TTL.

Update the client's `update_presence` (1542) to pass `ttl_ms` through to `PresenceRooms.update`.

- [ ] **Step 4: Tests + gate** — add a wire round-trip (`_ClientPresenceState(room="r", state={}, ttl_ms=3000).model_dump_json(by_alias=True)` contains `"ttlMs":3000`; with `ttl_ms=None` it omits it) and an in-memory test (two clients share `PresenceRooms`; one `update_presence(room, {"typing": True}, ttl_ms=50)`; advance the harness clock / `rooms.expire(now+60)`; assert the peer's snapshot has that member's `state is None` and the member is still listed). Run: `cd python-client && uv run pytest -q` then `uv run pyright`. Commit:

```bash
git add python-client/src python-client/tests
git commit -m "feat(python-client): ENH-015 ttl — update_presence ttlMs + in-memory expiry"
```

---

### Task 8: Docs

**Files:**
- Modify: `FEATURE_MATRIX.md:79` (presence row 25 — append the TTL note).
- Modify: `docs/superpowers/specs/2026-08-06-presence-design.md` (note per-state TTL shipped; it's in the parent spec's out-of-scope-adjacent context).

- [ ] **Step 1: FEATURE_MATRIX** — append to the presence row (line 79) implementation note, after the existing config sentence and before "Wire:":

> Per-state TTL (ENH-015 follow-up): `presenceState` accepts an optional `ttlMs`; the server clears that connection's `state` to `null` `ttlMs` after the last refresh and broadcasts it (the member stays — disconnect/leave still own membership removal). ttl omitted = permanent (today's behavior). Bounded by `RTDB_PRESENCE_MAX_TTL_MS` (default 300000 = 5 min; over-cap rejected, never clamped). Mirrored across all four clients (`updatePresence`/`update_presence` `ttlMs` + in-memory harness expiry). Spec: `docs/superpowers/specs/2026-08-06-presence-ttl-design.md`.

- [ ] **Step 2: Parent spec** — in `docs/superpowers/specs/2026-08-06-presence-design.md`, add a one-line shipped-note under the wire-protocol or behavior section: "Per-state TTL shipped as a follow-up (`presenceState` `ttlMs` → state clears to null after the refresh window; see `2026-08-06-presence-ttl-design.md`)."

- [ ] **Step 3: Gate + commit** — `make checkall` (docs-only; also rebuilds clients). Commit:

```bash
git add FEATURE_MATRIX.md docs/superpowers/specs/2026-08-06-presence-design.md
git commit -m "docs: ENH-015 ttl — feature matrix + parent spec shipped note"
```

---

## Final gate

- [ ] `make checkall` green across all five packages. Requires `make dev-db-up`.
- [ ] Spot-check: server running with `RTDB_PRESENCE_ENABLED=true`; two ts-client `usePresence` instances — one calls `updatePresence(room, {typing:true}, 3000)` on each keystroke and stops; the other sees "typing" clear ~3 s after the last keystroke while both remain "online".
- [ ] No `unwrap()`/`expect()` outside `#[cfg(test)]`. No new SQL in presence code.

## Self-Review (completed during authoring)

- **Spec coverage:** wire (`presenceState` `ttlMs`) → T1; config knob + metric → T2; expiry semantics + `expire_once` + flush-loop → T3; `/sync` threading + integration → T4; four-client parity (wire + update method + in-memory expiry + tests) → T5–T7; docs → T8. Acceptance criteria 1 (ttl clears state + broadcasts) → T3/T4; 2 (disconnect eviction unchanged) → T3 leaves `remove_conn` untouched; 3 (omitted = permanent) → T3; 4 (wire + harness + integration across four clients) → T1, T4–T7.
- **Type consistency:** `ttl_ms: Option<u64>` (Rust) / `ttlMs?: number` (ts) / `ttlMs: int | None` (python) named consistently; `expires_at: Option<i64>` on `Session`; `expire_once()`/`expire()` across server + harnesses; `update_state(..., ttl_ms)` / `updatePresence(room, state, ttlMs?)` / `update_presence(room, state, ttl_ms)` / `update_presence(room, state, ttl_ms=None)` consistent across the four.
- **Known soft spots flagged to implementers:** the `room()` typo + stray `FromStr` placeholder line in T3/T4 tests (delete them); the `MetricsSnapshot` test literals (T2) must add `presence_ttl_expiries_total: 0`; the `cfg()` helper (T3) must add `max_ttl_ms`; existing `update_state` test call sites gain `, None`; the harness `PresenceMember` type must NOT gain an `expiresAt` field (use a parallel expiry map) so the server→client snapshot shape stays byte-identical.
