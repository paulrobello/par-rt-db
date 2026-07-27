# Client Sweep — Item D: Rust Optimistic Updates (#12) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port client-side optimistic updates from the ts-client to the rust-client (#12): a new `rust-client/src/optimistic.rs` projection module + apply/reconcile/rollback hooks in `ws.rs`, opt-in via `Config.optimistic_updates`.

**Architecture:** Mirrors the ts reference (`ts-client/src/optimistic.ts` + `client.ts` integration) exactly in semantics, adapted to Rust. (1) A pure `project_optimistic_update(query, last, txn, now) -> OptimisticProjection` function over the existing `Query`/`Transaction`/`Value` types — conservative projection (only unambiguous cases overlay; everything else `Skip`). (2) `ws.rs` wiring: per-sub `Mutex<OptimisticState>` (server_last + optimistic flag), a `mut_id → query_ids` reverse index in `SubMaps`, a caller-side apply in `RtDbClient::mutate`, reconcile on `QueryUpdate`/`MutateOk`, rollback on `MutateErr` + the reject paths. Opt-in via `Config.optimistic_updates` (default `false` ⇒ bit-for-bit current behavior).

**Tech Stack:** Rust (edition 2024), tokio, serde_json, `#![deny(warnings)]` + clippy `-D warnings`. Run cargo from `rust-client/`.

## Global Constraints

- **No wire/protocol changes** — reuse `Query` (`query.rs:23`), `Step`/`Transaction` (`mutation.rs:7,13`), `Value`. `protocol.rs`/server untouched.
- **Mirror ts semantics** (`ts-client/src/optimistic.ts`): conservative projection — overlay only when unambiguous; `Skip` otherwise. Synthetic id `__optimistic__<n>`. Canonical no-op detection (an overlay reproducing `last` ⇒ `Skip`).
- **Fix the latent ts classification gap** (the port must not replicate it): `search`/`vector_search` are terminals ⇒ excluded from `is_array_query` (they `Skip`); `filter` is a predicate like `index`/`eq` ⇒ included in `has_filter` (filtered-by-predicate arrays get delete-only projection). (ts's `optimistic.ts:34-48` predates these terminals.)
- **Backward compatibility is load-bearing**: `Config.optimistic_updates` defaults to `false`; with it off, behavior is byte-for-byte the current client. The `Snapshot` type, the `request`/send paths, and existing tests are unchanged when off.
- `#![deny(warnings)]` + clippy `-D warnings`; no `unwrap`/`expect` outside `#[cfg(test)]`.
- Verification: each task runs `cd rust-client && cargo test` (+ `cargo clippy -- -D warnings` + `cargo fmt --check` before commit). Full `make checkall` runs at branch finish.
- **Re-Read before editing** (R9): `ws.rs` line numbers below are from the exploration; re-Read each region before applying an edit since earlier edits in the same task shift lines.

## Reference: ts projection model (the contract to port)

From `ts-client/src/optimistic.ts`:
- `OptimisticProjection = {overlaid: true, value} | {overlaid: false}` → rust `enum OptimisticProjection { Skip, Overlaid(Value) }`.
- Synthetic id: module `AtomicU64`, `__optimistic__<n>` (`optimistic.ts:64-68`).
- Canonical no-op: an overlay whose value equals `last` (canonicalized) ⇒ `Skip` (`optimistic.ts:51-62`). In Rust, `serde_json::Value` object equality is canonical **if `preserve_order` is off** (default `BTreeMap` sorts keys). Confirm rust-client's `serde_json` features; if `preserve_order` is on, add a key-sorting canonicalizer.
- Classification (`optimistic.ts:34-48`, corrected for new terminals):
  - `is_array_query`: `get.is_none() && !unique && !first && !count && paginate.is_none() && search.is_none() && vector_search.is_none()`.
  - `has_filter`: `index.is_some() || !eq.is_empty() || gt/gte/lt/lte.is_some() || filter.is_some()`.
- Dispatch (`optimistic.ts:92-100`): `get.is_some()` → project_get; `!is_array_query` → Skip; `has_filter` → project_filtered_array (delete-only); else project_unfiltered_array.
- **Unfiltered array** (`optimistic.ts:105-157`): `last` is `Value::Array`; clone; per step where `step.table == query.table`:
  - `Insert`: Skip if `take.is_some() && len >= take` (window full); else push `{...doc, _id: synthetic, _creationTime: now, _version: 1}`.
  - `Patch`: find by `id`; shallow-merge `fields` over the doc.
  - `Replace`: find by `id`; keep old `_id`+`_creationTime`, drop `_version`, use `doc`.
  - `Delete`: find by `id`; remove.
  - `Upsert`: Skip (insert-vs-update ambiguous). `ExpectVersion`/`ExpectAbsent`: no-op.
  - Final: canonical(working) == canonical(last) ⇒ Skip else Overlaid(working).
- **Filtered array** (`optimistic.ts:161-192`): `Delete`-only (find by id, remove); all other data-ops Skip; same canonical no-op check.
- **`get(id)`** (`optimistic.ts:196-229`): `last` is `Value::Null` or a doc; target = `query.get`; `Delete`/`Patch`/`Replace` apply only if `step.id == target`; `Insert` breaks (never matches a pre-existing get); `Upsert` Skip; canonical no-op check.

---

## File Structure

- `rust-client/src/optimistic.rs` — new module: `OptimisticProjection`, `project_optimistic_update`, classification + per-shape projection + synthetic id + canonical no-op.
- `rust-client/src/ws.rs` — `Config.optimistic_updates`; `SubState` gains `Mutex<OptimisticState>`; `SubMaps.overlays` reverse index; caller-side apply in `RtDbClient::mutate`; reconcile in `apply_server_message` (QueryUpdate/MutateOk); rollback (MutateErr + `reject_inflight`/`reject_all` signature change); test-rig updates.
- `rust-client/src/lib.rs` — `pub mod optimistic;` (+ optional re-export).
- `FEATURE_MATRIX.md` — flip row #12 to ✅ rust.
- `rust-client/README.md` — only if it documents the reactive client's options.

---

## Task 1: `optimistic.rs` — pure projection + unit tests

**Files:**
- Create: `rust-client/src/optimistic.rs`
- Modify: `rust-client/src/lib.rs` (add `pub mod optimistic;` in the module block ~`:38-49`)
- Test: `rust-client/src/optimistic.rs` (`#[cfg(test)] mod tests`) — mirror `ts-client/tests/optimistic.test.ts` categories + the rust-only classification-gap test.

**Interfaces:**
- Consumes: `Query` (`query.rs:23`), `Step`/`Transaction` (`mutation.rs:7,13`), `serde_json::Value`/`Map`.
- Produces: `pub enum OptimisticProjection { Skip, Overlaid(Value) }`, `pub fn project_optimistic_update(query: &Query, last: &Value, txn: &Transaction, now: i64) -> OptimisticProjection`.

- [ ] **Step 1: Write the failing tests** in `rust-client/src/optimistic.rs` (a `#[cfg(test)] mod tests` block). Build queries via `TableQuery` (`query.rs`) and txns via `Mutation` (`mutation.rs`). Port `optimistic.test.ts` categories:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use serde_json::json;

    fn collect_query() -> Query { TableQuery::new("items").collect() }

    #[test]
    fn insert_overlays_on_unfiltered_collect() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "title": "x" }]);
        let txn = Mutation::new().insert("items", json!({ "title": "y" })).build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => {
                let arr = v.as_array().unwrap();
                assert_eq!(arr.len(), 2);
                assert!(arr[1]["_id"].as_str().unwrap().starts_with("__optimistic__"));
                assert_eq!(arr[1]["_creationTime"], 99);
                assert_eq!(arr[1]["_version"], 1);
                assert_eq!(arr[1]["title"], "y");
            }
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn patch_overlays_by_id() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 }]);
        let txn = Mutation::new().patch("items", "a", json!({ "n": 2 })).build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => assert_eq!(v[0]["n"], 2),
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn delete_overlays_by_id() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }, { "_id": "b", "_creationTime": 2, "_version": 1 }]);
        let txn = Mutation::new().delete("items", "a").build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => assert_eq!(v.as_array().unwrap().len(), 1),
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn noop_patch_returns_skip() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 }]);
        // patch to the same value → canonical-equal → Skip
        let txn = Mutation::new().patch("items", "a", json!({ "n": 1 })).build();
        assert!(matches!(project_optimistic_update(&q, &last, &txn, 99), OptimisticProjection::Skip));
    }

    #[test]
    fn insert_skips_when_take_window_full() {
        let q = TableQuery::new("items").take(1);
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }]);
        let txn = Mutation::new().insert("items", json!({ "title": "y" })).build();
        assert!(matches!(project_optimistic_update(&q, &last, &txn, 99), OptimisticProjection::Skip));
    }

    #[test]
    fn filtered_array_delete_only() {
        // index/eq filtered array: only delete projects
        let q = TableQuery::new("items").index("by_status").eq(["active"]).collect();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }]);
        let del = Mutation::new().delete("items", "a").build();
        assert!(matches!(project_optimistic_update(&q, &last, &del, 99), OptimisticProjection::Overlaid(_)));
        let ins = Mutation::new().insert("items", json!({ "title": "y" })).build();
        assert!(matches!(project_optimistic_update(&q, &last, &ins, 99), OptimisticProjection::Skip));
    }

    #[test]
    fn filter_predicate_treated_as_filtered_array() {
        // gap-fix: a collect with a `filter` predicate routes to delete-only projection, not unfiltered
        // (Confirm the exact TableQuery.filter() builder name in query.rs and use it here.)
        // delete overlays; insert skips.
    }

    #[test]
    fn get_point_read_patch() {
        let mut q = TableQuery::new("items").build_query(); // confirm the get(id) builder
        // q.get = Some("a".into()) via the builder if available, else construct Query{get: Some("a"), table: "items", ..}
        let last = json!({ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 });
        let txn = Mutation::new().patch("items", "a", json!({ "n": 2 })).build();
        // expect Overlaid with n:2
    }

    #[test]
    fn always_skip_terminals() {
        // unique, first, count, paginate, search, vector_search all → Skip regardless of txn
        // (Confirm builder names in query.rs; construct one Query per terminal and assert Skip.)
    }

    #[test]
    fn synthetic_id_format() {
        // two inserts in two calls produce __optimistic__N with incrementing N (assert starts_with + that two differ)
    }
}
```

(Confirm the exact `TableQuery` builder method names — `.collect()`, `.take(n)`, `.index(..).eq(..)`, `.filter(..)`, the `get`/`unique`/`first`/`count`/`paginate`/`search`/`vector_search` terminals, and the `Mutation::new().insert/patch/delete(..).build()` shape — against `query.rs` and `mutation.rs` + their `#[cfg(test)]` examples. Replace any pseudocode builder call above with the real one. For terminals without a clean builder (e.g. `get`), construct the `Query` struct literally.)

- [ ] **Step 2: Run to verify RED** — `cd rust-client && cargo test --lib optimistic` (compile errors: module/functions don't exist yet).

- [ ] **Step 3: Implement `optimistic.rs`**

```rust
//! Client-side optimistic-update projection. Pure: given a query, its last
//! authoritative result, and a transaction, produce the projected result to
//! overlay immediately (before the server round-trip), or `Skip` when the
//! effect is ambiguous. Ports `ts-client/src/optimistic.ts` (conservative:
//! only unambiguous cases overlay).
use std::sync::atomic::{AtomicU64, Ordering};
use serde_json::{Map, Value};
use crate::mutation::{Step, Transaction};
use crate::query::Query;

static COUNTER: AtomicU64 = AtomicU64::new(0);
fn synthetic_id() -> String {
    format!("__optimistic__{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Debug)]
pub enum OptimisticProjection {
    Skip,
    Overlaid(Value),
}

/// `now` is epoch-millis for the synthetic `_creationTime` (passed in so the
/// function stays pure and testable without a clock).
pub fn project_optimistic_update(
    query: &Query,
    last: &Value,
    txn: &Transaction,
    now: i64,
) -> OptimisticProjection {
    if query.get.is_some() {
        return project_get(query, last, txn);
    }
    if !is_array_query(query) {
        return OptimisticProjection::Skip;
    }
    if has_filter(query) {
        project_filtered_array(query, last, txn)
    } else {
        project_unfiltered_array(query, last, txn, now)
    }
}

fn is_array_query(q: &Query) -> bool {
    q.get.is_none()
        && !q.unique
        && !q.first
        && !q.count
        && q.paginate.is_none()
        && q.search.is_none()
        && q.vector_search.is_none()
}

fn has_filter(q: &Query) -> bool {
    q.index.is_some()
        || !q.eq.is_empty()
        || q.gt.is_some()
        || q.gte.is_some()
        || q.lt.is_some()
        || q.lte.is_some()
        || q.filter.is_some()
}

fn project_unfiltered_array(query: &Query, last: &Value, txn: &Transaction, now: i64) -> OptimisticProjection {
    let Some(working) = last.as_array().map(|a| a.clone()) else { return OptimisticProjection::Skip; };
    let mut working = working;
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) { // see Step::table() helper note below
            continue;
        }
        match step {
            Step::Insert { doc, .. } => {
                if let Some(take) = query.take {
                    if (working.len() as u32) >= take {
                        return OptimisticProjection::Skip;
                    }
                }
                let mut d = doc.clone();
                d.insert("_id".into(), Value::String(synthetic_id()));
                d.insert("_creationTime".into(), Value::Number(now.into()));
                d.insert("_version".into(), Value::Number(1.into()));
                working.push(Value::Object(d));
            }
            Step::Patch { id, fields, .. } => merge_by_id(&mut working, id, fields),
            Step::Replace { id, doc, .. } => replace_by_id(&mut working, id, doc),
            Step::Delete { id, .. } => remove_by_id(&mut working, id),
            Step::Upsert { .. } => return OptimisticProjection::Skip,
            Step::ExpectVersion | Step::ExpectAbsent => {}
        }
    }
    finalize(working, last)
}

fn project_filtered_array(query: &Query, last: &Value, txn: &Transaction) -> OptimisticProjection {
    let Some(working) = last.as_array().map(|a| a.clone()) else { return OptimisticProjection::Skip; };
    let mut working = working;
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) { continue; }
        match step {
            Step::Delete { id, .. } => remove_by_id(&mut working, id),
            // insert/patch/replace/upsert are membership-ambiguous under a filter → Skip
            Step::Insert { .. } | Step::Patch { .. } | Step::Replace { .. } | Step::Upsert { .. } => {
                return OptimisticProjection::Skip;
            }
            Step::ExpectVersion | Step::ExpectAbsent => {}
        }
    }
    finalize(working, last)
}

fn project_get(query: &Query, last: &Value, txn: &Transaction) -> OptimisticProjection {
    let target = query.get.as_deref().unwrap_or("");
    let mut working = last.clone(); // a doc or Null
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) { continue; }
        match step {
            Step::Delete { id, .. } if id == target => { working = Value::Null; }
            Step::Patch { id, fields, .. } if id == target => {
                if let (Some(d), Some(f)) = (working.as_object().cloned(), Some(fields.clone())) {
                    let mut d = d;
                    for (k, v) in f { d.insert(k.clone(), v.clone()); }
                    working = Value::Object(d);
                }
            }
            Step::Replace { id, doc, .. } if id == target => {
                if let Some(old) = last.as_object() {
                    let mut d = doc.clone();
                    if let Some(oid) = old.get("_id") { d.insert("_id".into(), oid.clone()); }
                    if let Some(ct) = old.get("_creationTime") { d.insert("_creationTime".into(), ct.clone()); }
                    d.remove("_version");
                    working = Value::Object(d);
                }
            }
            Step::Insert { .. } => break, // a fresh insert never matches a pre-existing get(target)
            Step::Upsert { .. } => return OptimisticProjection::Skip,
            _ => {}
        }
    }
    if value_eq(&working, last) { OptimisticProjection::Skip } else { OptimisticProjection::Overlaid(working) }
}

// --- helpers ---

/// `Step` has no `table()` method today (the field is per-variant); add a small
/// inherent helper on `Step` returning `Option<&str>` (Insert/Patch/Replace/Delete/Upsert
/// → Some(self.table), ExpectVersion → Some, ExpectAbsent → None). Put it in optimistic.rs
/// as `impl Step { fn table(&self) -> Option<&str> { ... } }` to avoid editing mutation.rs.

fn finalize(working: Vec<Value>, last: &Value) -> OptimisticProjection {
    let next = Value::Array(working);
    if value_eq(&next, last) { OptimisticProjection::Skip } else { OptimisticProjection::Overlaid(next) }
}

/// Canonical equality. serde_json::Value objects are BTreeMap-backed (sorted) when
/// the `preserve_order` feature is OFF (the default) — so `Value::eq` is canonical.
/// If rust-client enables preserve_order, replace this with a key-sorting canonicalizer.
fn value_eq(a: &Value, b: &Value) -> bool { a == b }

fn merge_by_id(working: &mut Vec<Value>, id: &str, fields: &Map<String, Value>) {
    for v in working.iter_mut() {
        if v.get("_id").and_then(Value::as_str) == Some(id) {
            if let Some(d) = v.as_object_mut() {
                for (k, val) in fields { d.insert(k.clone(), val.clone()); }
            }
        }
    }
}
fn replace_by_id(working: &mut Vec<Value>, id: &str, doc: &Map<String, Value>) {
    for v in working.iter_mut() {
        if v.get("_id").and_then(Value::as_str) == Some(id) {
            let mut d = doc.clone();
            if let Some(old) = v.get("_id").cloned() { d.insert("_id".into(), old); }
            if let Some(ct) = v.get("_creationTime").cloned() { d.insert("_creationTime".into(), ct); }
            d.remove("_version");
            *v = Value::Object(d);
        }
    }
}
fn remove_by_id(working: &mut Vec<Value>, id: &str) {
    working.retain(|v| v.get("_id").and_then(Value::as_str) != Some(id));
}
```

Notes for the implementer:
- Add `impl Step { fn table(&self) -> Option<&str> }` in `optimistic.rs` (matches on the variants; `ExpectAbsent` returns `None`). Do NOT edit `mutation.rs`.
- Confirm the `Query` field names (`vector_search`, `eq`, etc.) and `Step` variant/field names against `query.rs:23` / `mutation.rs:13` — the code above assumes them; fix any mismatch.
- If `cargo test` shows serde_json `preserve_order` is enabled, replace `value_eq` with a recursive key-sorting canonicalizer (mirror `optimistic.ts:51-62`).

- [ ] **Step 4: Run GREEN** — `cd rust-client && cargo test --lib optimistic` (all pass) + `cargo clippy --lib -- -D warnings` (clean).

- [ ] **Step 5: Format + commit**
```
cd rust-client && cargo fmt
git add rust-client/src/optimistic.rs rust-client/src/lib.rs
git commit -m "feat(rust-client): optimistic-update projection module (#12)"
```

---

## Task 2: `ws.rs` wiring (apply / reconcile / rollback)

**Files:**
- Modify: `rust-client/src/ws.rs` (state additions + 4 hook sites + reject signature change + test-rig)
- Test: `rust-client/src/ws.rs` `#[cfg(test)] mod tests` (extend `rig_with_sub` + new wiring tests)

**Interfaces:**
- Consumes: `project_optimistic_update`, `OptimisticProjection` from Task 1; `Config`, `SubState`, `SubMaps`, `ClientInner`, `apply_server_message`, `reject_inflight`/`reject_all` (all in `ws.rs`).

Re-Read each `ws.rs` region before editing (line numbers from the exploration). The hook sites:

- [ ] **Step 1: Add the state**

  (a) `Config` (`ws.rs:90-110`): add `pub optimistic_updates: bool`, default `false` in `Config::default`.
  (b) `SubState` (`ws.rs:117-124`): add `optimistic: Mutex<OptimisticState>` where `struct OptimisticState { active: bool, server_last: Option<Value> }` (sub-module-internal). (`SubState` is `Arc`-shared; the `Mutex` gives interior mutability — today only the atomic `refcount` is mutated through the shared arc.)
  (c) `SubMaps` (`ws.rs:129-133`): add `overlays: HashMap<String, HashSet<String>>` (`mut_id → query_id`s) — shares the single `subs` mutex (`ws.rs:245`). `#[derive(Default)]` propagates.

- [ ] **Step 2: Apply hook (caller-side, in `RtDbClient::mutate`, `ws.rs:478-507`)**

  Right after `let mut_id = format!("mut-{}", ...)` and BEFORE `cmd_tx.send(Cmd::Mutate(...))`: if `self.inner.config.optimistic_updates`, lock `inner.subs`, and for each `SubState` with a non-pending last value, call `project_optimistic_update(&sub.query, &last_value, txn, now)`; for each `Overlaid(v)`, set the sub's `optimistic.active = true`, `sub.tx.send(Snapshot::Value(Box::new(v.clone())))`, and collect its `query_id` under `mut_id` in `maps.overlays`. This mirrors ts's `applyOptimistic` (`client.ts:430-446`) — subscribers in other tasks see the overlay before the caller awaits.

  (The "last value" for a sub is what its `watch::Receiver` would currently show. Since `SubState` holds the `watch::Sender` but not the latest `Snapshot` directly, either: track the last pushed `Value` in `OptimisticState.server_last` as it's pushed, or `borrow()` a stored receiver. Simplest: when you push a `Snapshot::Value` in the existing `QueryUpdate` arm, also store it as `server_last` — then the apply hook reads `server_last` as the projection base. Confirm against the watch semantics; the reconcile hook below already sets `server_last` on `QueryUpdate`.)

- [ ] **Step 3: Reconcile hook (`apply_server_message`, `ws.rs:1048-1120`)**

  (a) `QueryUpdate` arm (`ws.rs:1055-1064`): before `sub.tx.send(...)`, set the sub's `optimistic.active = false` and `optimistic.server_last = Some(result.clone())` (authoritative base updated; overlay superseded). Then send.
  (b) `MutateOk` arm (`ws.rs:1073-1077`): after resolving the reply, `maps.overlays.remove(&mut_id)` (no revert — the reconciling `QueryUpdate` arrives, same as ts `client.ts:605-613`).

- [ ] **Step 4: Rollback hook**

  (a) `MutateErr` arm (`ws.rs:1078-1082`): `if let Some(qids) = maps.overlays.remove(&mut_id) { for qid in qids { /* look up sub; if optimistic.active && server_last.is_some() → active=false, send server_last */ } }`.
  (b) `reject_inflight` / `reject_all` (`ws.rs:1141-1182`): add an `inner: &Arc<ClientInner>` (or `subs: &Mutex<SubMaps>`) parameter; for each removed `mut_id`, run the same revert as MutateErr. Update the 3 call sites (`ws.rs:672`, `:684`, `:701`).

- [ ] **Step 5: Test-rig + wiring tests** (`ws.rs::tests`, `ws.rs:1241-1599`)

  Update `rig_with_sub` (`ws.rs:1379-1410`) to construct the new `Config`, `SubState`, and `SubMaps` fields (every added field must be mirrored or it won't compile). Update `subscribe_err_routes_error_and_removes` (`ws.rs:1430-1446`) to also assert `maps.overlays.is_empty()`. Add:
  - A reconcile test: deliver `QueryUpdate` → assert the sub's `optimistic.active == false` and `server_last` set.
  - A rollback test: seed a sub with `optimistic.active = true`, `server_last = Some(base)`, and `maps.overlays["mut-1"] = {"sub-1"}`; deliver `MutateErr { mut_id: "mut-1", .. }` → assert the watch receiver reverted to `base` and `maps.overlays` is empty.

- [ ] **Step 6: Run + verify**
```
cd rust-client && cargo test
cargo clippy -- -D warnings
cargo fmt --check
```
All green. Critically: with `optimistic_updates: false` (the default), the existing tests are byte-for-bit unchanged.

- [ ] **Step 7: Commit**
```
cd rust-client && cargo fmt
git add rust-client/src/ws.rs
git commit -m "feat(rust-client): wire optimistic updates into the reactive client (#12)"
```

---

## Task 3: lib.rs re-export + docs + gate

**Files:**
- Modify: `rust-client/src/lib.rs` (re-export), `FEATURE_MATRIX.md` (row #12), `rust-client/README.md` (only if it documents reactive options)

- [ ] **Step 1**: In `rust-client/src/lib.rs`, ensure `pub mod optimistic;` is in the module block (added in Task 1) and add `pub use optimistic::{OptimisticProjection, project_optimistic_update};` if the crate surfaces such helpers (check the existing re-export style at `lib.rs:52`).

- [ ] **Step 2**: `FEATURE_MATRIX.md` row #12 — flip the rust-client status to ✅ (it currently says "Rust client pending"). Update the note to describe the `Config.optimistic_updates` opt-in.

- [ ] **Step 3**: `rust-client/README.md` — only if it lists the reactive client's `Config` options, add `optimistic_updates`; otherwise no change.

- [ ] **Step 4 — gate**:
```
cd rust-client && cargo test && cargo clippy -- -D warnings && cargo fmt --check
```
(Full `make checkall` runs at branch finish.)

- [ ] **Step 5 — commit**:
```
git add rust-client/src/lib.rs FEATURE_MATRIX.md rust-client/README.md   # drop README if unchanged
git commit -m "docs: rust-client optimistic updates shipped (#12)"
```

---

## Self-Review (completed during authoring)

- **Spec coverage:** D = projection module (Task 1) + ws.rs wiring (Task 2) + re-export/docs (Task 3). ✅
- **Latent-gap fix:** the corrected `is_array_query` (excludes search/vector terminals only) + `has_filter` (includes the `filter` predicate) is specified, with a dedicated unit test. (The exploration's wording slightly mis-stated excluding `filter` from `is_array_query`; this plan corrects it — `filter` is a predicate, belongs in `has_filter`.) ✅
- **Backward compat:** `Config.optimistic_updates` defaults false; off ⇒ unchanged. ✅
- **Placeholders:** optimistic.rs is real Rust (classification, dispatch, all three shapes, helpers); the ws.rs hook steps cite exact line numbers + verbatim current code from the exploration and specify the precise edit semantics (the implementer re-Reads per R9). The test builder calls are flagged "confirm against query.rs/mutation.rs" where the exact method name must be verified — verify-points, not placeholders. ✅
- **Risks flagged:** SubState interior mutability (Mutex); reverse-index placement in SubMaps; caller-side apply (avoids plumbing `inner` into `deliver_mutate`); reject-path signature change (3 call sites); serde_json `preserve_order` canonical-equality; test-rig churn. ✅
