# Fine-Grained Subscription Invalidation (v1: Point Reads) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Skip needless subscription re-runs for `get(id)` point reads when the write didn't touch their document, while preserving today's table-level behavior for every other query shape.

**Architecture:** Enrich the server-internal `write_set` (today a set of table names) with the `(table, id)` of every written document; derive a per-subscription `ReadSet` from its `Query` at registration (`Point` for `get`, `Table` otherwise); in `fan_out`, skip the re-run only for a `Point` subscription whose id is not in the written set. No protocol or wire change.

**Tech Stack:** Rust (axum/tokio + sqlx/Postgres 17). Tests are integration tests against the real dev Postgres plus one `#[cfg(test)]` unit module.

**Spec:** `docs/superpowers/specs/2026-07-24-fine-grained-subscription-invalidation-design.md`

## Global Constraints

- **Definition of done:** `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests). Requires the dev Postgres: run `make dev-db-up` first.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Server-only, no protocol change:** `TxnOutcome.write_set` is server-internal (transports send only `outcome.results`); do not touch `protocol.rs`, `ws.rs`, `http_api.rs`, or either client SDK.
- **Surgical:** match existing style (snake_case, 4-space indent, the file's existing import grouping). Every changed line traces to the spec.
- **Single-writer invariant (do not break):** this work adds no new writer and does not call `execute_txn` outside the committer. It only changes what `write_set` *contains* and how `fan_out` *reads* it.

## File Structure

- `server/src/txn.rs` — owns the new `WriteSet` type and `TxnOutcome`; `execute_txn` populates it.
- `server/src/subs.rs` — owns `ReadSet`; `SubEntry` stores it; `register` derives it; `fan_out` consumes `WriteSet` + `ReadSet` to decide re-run vs. skip.
- `server/src/committer.rs` — only the idempotent-replay construction of `TxnOutcome` changes (`WriteSet::default()`).
- `server/tests/subs_test.rs` — new integration tests.
- `FEATURE_MATRIX.md` — row #21 status flip + note.

---

## Task 1: Introduce `WriteSet` and thread it through txn → subs → committer

No behavior change in this task: `fan_out` still re-runs every subscription whose table was written (it reads `write_set.tables`). This task only changes the *shape* of `write_set` and proves the new `(table, id)` data is collected.

**Files:**
- Modify: `server/src/txn.rs` (add `WriteSet`; change `TxnOutcome.write_set`; populate in `execute_txn`).
- Modify: `server/src/subs.rs` (imports; `fan_out` signature + one body line).
- Modify: `server/src/committer.rs` (imports; idempotent-replay line).
- Test: `server/tests/subs_test.rs` (append one test).

**Interfaces:**
- Produces: `pub struct WriteSet { pub tables: BTreeSet<String>, pub docs: BTreeSet<(String, String)> }` with `fn touch(&mut self, table: &str, id: &str)`, in `txn.rs`. `TxnOutcome.write_set: WriteSet`. `fan_out` now takes `write_set: &WriteSet`.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/subs_test.rs`:

```rust
// Fine-grained invalidation plumbing: a committed txn's write_set records the
// specific (table, id) of every written document — not just the table name —
// so point-read subscriptions can later skip re-runs that don't touch their doc.
#[tokio::test]
async fn write_set_records_written_document_ids() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert_a = state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;
    let id_a = insert_a.results[0]["id"]
        .as_str()
        .expect("insert returns id")
        .to_string();

    let patch_a = state
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "workItems".to_string(),
                    id: id_a.clone(),
                    fields: serde_json::json!({ "status": "in_progress" })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
        )
        .await?;

    assert!(insert_a.write_set.tables.contains("workItems"));
    assert!(insert_a
        .write_set
        .docs
        .contains(&("workItems".to_string(), id_a.clone())));
    assert!(patch_a
        .write_set
        .docs
        .contains(&("workItems".to_string(), id_a)));
    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails (compile error)**

Run (dev Postgres must be up — `make dev-db-up`):
```bash
cd server && cargo test --test subs_test write_set_records_written_document_ids
```
Expected: FAIL to compile — `no field 'docs' on type BTreeSet<String>` (or similar), because `write_set` is still `BTreeSet<String>`.

- [ ] **Step 3: Add `WriteSet` and change `TxnOutcome` in `txn.rs`**

In `server/src/txn.rs`, insert this **immediately before** the `TxnOutcome` definition (currently at the line `pub struct TxnOutcome {`):

```rust
/// The tables and documents a committed transaction wrote. `tables` drives
/// table-level subscription invalidation; `docs` — the `(table, id)` of every
/// written document — lets point-read subscriptions skip re-runs that don't
/// touch their document (see `subs::ReadSet`). Server-internal: the wire
/// transports send only `TxnOutcome.results`, never `write_set`.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct WriteSet {
    pub tables: BTreeSet<String>,
    pub docs: BTreeSet<(String, String)>,
}

impl WriteSet {
    /// Records that the transaction wrote document `id` in `table`.
    fn touch(&mut self, table: &str, id: &str) {
        self.tables.insert(table.to_string());
        self.docs.insert((table.to_string(), id.to_string()));
    }
}
```

Then change the `write_set` field type inside `TxnOutcome`:

```rust
#[derive(Debug, Clone, serde::Serialize)]
pub struct TxnOutcome {
    pub results: Vec<serde_json::Value>,
    pub write_set: WriteSet,
}
```

- [ ] **Step 4: Populate `docs` in `execute_txn`**

In `server/src/txn.rs`, inside `execute_txn`, change the local from:
```rust
    let mut write_set = BTreeSet::new();
```
to:
```rust
    let mut write_set = WriteSet::default();
```

Then replace each `write_set.insert(table.clone());` with a `touch` call that also records the document id. Each arm already holds the id at that point — use the exact surrounding line to disambiguate (the `insert` lines are otherwise identical):

**Insert arm** (the line after `let id = do_insert(&mut tx, &pg_schema_name, table_def, table, doc).await?;`):
```rust
                write_set.touch(table, &id);
```

**Patch arm** (the line after `do_patch(&mut tx, &pg_schema_name, table_def, table, id, fields).await?;`):
```rust
                write_set.touch(table, id);
```

**Replace arm** (the line after `do_replace(&mut tx, &pg_schema_name, table_def, table, id, doc).await?;`):
```rust
                write_set.touch(table, id);
```

**Delete arm** (the line after `do_delete(&mut tx, &pg_schema_name, table, id).await?;`):
```rust
                write_set.touch(table, id);
```

**Upsert insert branch** (the line after `let id = do_insert(&mut tx, &pg_schema_name, table_def, table, insert).await?;`):
```rust
                        write_set.touch(table, &id);
```

**Upsert patch branch** (the line after `apply_update(&mut tx, &pg_schema_name, table_def, table, &id, merged).await?;`):
```rust
                        write_set.touch(table, &id);
```

(`Patch`/`Replace`/`Delete` bind `id: &String`, which deref-coerces to `&str`. The `Insert`/`Upsert` arms own `id: String`, hence `&id`.)

- [ ] **Step 5: Update `subs.rs` signature + body + imports**

In `server/src/subs.rs`:

Change the import (line 1):
```rust
use std::collections::{BTreeSet, HashMap};
```
to:
```rust
use std::collections::HashMap;
```

Add to the `crate::` import group (alongside the existing `use crate::query::...`):
```rust
use crate::txn::WriteSet;
```

Change the `fan_out` parameter (currently `write_set: &BTreeSet<String>,`):
```rust
        write_set: &WriteSet,
```

Change the table-membership check inside the loop (currently `if !write_set.contains(&entry.query.table) {`):
```rust
            if !write_set.tables.contains(&entry.query.table) {
```

- [ ] **Step 6: Update `committer.rs` replay + imports**

In `server/src/committer.rs`:

Change the import (line 1):
```rust
use std::collections::{BTreeSet, HashMap};
```
to:
```rust
use std::collections::HashMap;
```

Add `WriteSet` to the txn import (currently `use crate::txn::{Transaction, TxnOutcome, execute_txn};`):
```rust
use crate::txn::{Transaction, TxnOutcome, WriteSet, execute_txn};
```

Change the idempotent-replay construction (currently `write_set: BTreeSet::new(),`):
```rust
            write_set: WriteSet::default(),
```

- [ ] **Step 7: Run the test and the full suite; check clippy/fmt**

```bash
cd server && cargo test --test subs_test write_set_records_written_document_ids
cd server && cargo fmt --check && cargo clippy --all-targets -- -D warnings
cd server && cargo test --test subs_test
```
Expected: the new test PASSES; all existing `subs_test` tests still PASS (behavior unchanged); fmt clean; clippy clean. (If clippy flags an unused import, the import edits in Steps 5–6 were meant to prevent exactly that — re-check that `BTreeSet` was removed wherever it is now unused.)

- [ ] **Step 8: Commit**

```bash
git add server/src/txn.rs server/src/subs.rs server/src/committer.rs server/tests/subs_test.rs
git commit -m "refactor(server): WriteSet carries (table, id) per written doc

Thread a richer server-internal write_set through txn -> committer -> subs.
Behavior unchanged: fan_out still re-runs at table granularity (reads
write_set.tables). The new docs field is the plumbing for point-read
skipping in the next change."
```

---

## Task 2: Add `ReadSet` and skip re-runs for `get(id)` point reads

This is the behavior change, scoped to `subs.rs`. A `get(id)` subscription's result depends on exactly one document, so a write that doesn't touch `(table, id)` cannot change it — skip the re-run. Every other shape stays table-level.

> **Testing note:** the skip is *push-invisible* — whether we skip or re-run, a `get(id)` whose doc wasn't written produces no push (re-run → unchanged → diff suppresses). So the skip **decision** is verified by a unit test on `ReadSet::from_query`, and **correctness** (no missed updates, table-level preserved) is verified by integration tests.

**Files:**
- Modify: `server/src/subs.rs` (`ReadSet` enum + `from_query`; `SubEntry` field; `register`; `fan_out` skip; `#[cfg(test)]` unit module).
- Test: `server/tests/subs_test.rs` (append three tests).

**Interfaces:**
- Produces (private to `subs.rs`): `enum ReadSet { Point { id: String }, Table }` with `fn from_query(&Query) -> ReadSet`. `SubEntry` gains `read_set: ReadSet`.

- [ ] **Step 1: Write the failing unit test for the skip decision**

Append a `#[cfg(test)]` module at the **end** of `server/src/subs.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn q(value: serde_json::Value) -> Query {
        serde_json::from_value(value).expect("parse query")
    }

    #[test]
    fn get_query_is_a_point_read() {
        let query = q(serde_json::json!({ "table": "t", "get": "abc" }));
        assert!(matches!(
            ReadSet::from_query(&query),
            ReadSet::Point { id } if id == "abc"
        ));
    }

    #[test]
    fn non_get_queries_are_table_level() {
        let cases = [
            serde_json::json!({ "table": "t" }),                                               // collect
            serde_json::json!({ "table": "t", "take": 5 }),                                    // take
            serde_json::json!({ "table": "t", "index": "by_x", "eq": ["v"] }),                 // eq
            serde_json::json!({ "table": "t", "index": "by_x", "eq": ["v"], "unique": true }), // unique
            serde_json::json!({ "table": "t", "first": true }),                                // first
            serde_json::json!({ "table": "t", "count": true }),                                // count
        ];
        for case in cases {
            let query = q(case);
            assert!(
                matches!(ReadSet::from_query(&query), ReadSet::Table),
                "non-get query must be Table-level"
            );
        }
    }
}
```

- [ ] **Step 2: Run the unit test to verify it fails**

```bash
cd server && cargo test --lib subs
```
Expected: FAIL to compile — `cannot find type ReadSet` / `cannot find function from_query`.

- [ ] **Step 3: Add `ReadSet` + `from_query`, store on `SubEntry`, derive in `register`**

In `server/src/subs.rs`, add the enum (place it just above `struct SubEntry`):

```rust
/// What a subscription's result depends on, used to skip needless re-runs.
/// Derived once from the (immutable) `Query` at registration.
#[derive(Debug, Clone)]
enum ReadSet {
    /// A `get(id)` point read: the result is exactly this one document, so a
    /// write to any other document cannot change it.
    Point { id: String },
    /// Every other shape (take / collect / count / paginate / unique / first /
    /// search / vector): another document can enter the result, so re-run on
    /// any write to the table (today's behavior).
    Table,
}

impl ReadSet {
    fn from_query(query: &Query) -> Self {
        match &query.get {
            Some(id) => ReadSet::Point { id: id.clone() },
            None => ReadSet::Table,
        }
    }
}
```

Add the field to `SubEntry`:
```rust
struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    read_set: ReadSet,
}
```

In `register`, derive and store it. Replace the `SubEntry { query, tx, last }` construction with:
```rust
        let read_set = ReadSet::from_query(&query);
        let mut guard = self.subs.lock().await;
        guard
            .entry(db.to_string())
            .or_default()
            .insert(
                (conn, query_id),
                SubEntry {
                    query,
                    tx,
                    last,
                    read_set,
                },
            );
```

- [ ] **Step 4: Add the skip to `fan_out`**

In `server/src/subs.rs`, inside `fan_out`'s loop, immediately **after** the existing table-membership `continue`, add:

```rust
            // A `get(id)` point read depends only on its one document, so a
            // write that didn't touch it cannot change the result — skip the
            // re-run. Every other shape stays table-level (re-runs below).
            if let ReadSet::Point { id } = &entry.read_set
                && !write_set
                    .docs
                    .contains(&(entry.query.table.clone(), id.clone()))
            {
                continue;
            }
```

- [ ] **Step 5: Run the unit test to verify it passes**

```bash
cd server && cargo test --lib subs
```
Expected: PASS.

- [ ] **Step 6: Write the integration tests (correctness: no missed updates; table-level preserved)**

Append to `server/tests/subs_test.rs`:

```rust
// A get(id) subscription still receives an update when its own document is
// written — the point-read skip must never drop a relevant update.
#[tokio::test]
async fn get_subscription_updates_when_its_doc_is_written() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert = state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;
    let id = insert.results[0]["id"]
        .as_str()
        .expect("insert id")
        .to_string();

    let get_query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "get": id,
    }))
    .expect("parse get query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), get_query, tx)
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "workItems".to_string(),
                    id: id.clone(),
                    fields: serde_json::json!({ "status": "in_progress" })
                        .as_object()
                        .expect("object")
                        .clone(),
                }],
            },
        )
        .await?;

    let msg = rx.try_recv().expect("update after patching the subscribed doc");
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            assert_eq!(query_id, "q1");
            assert_eq!(result["status"], "in_progress");
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    Ok(())
}

// A get(id) subscription does NOT receive an update when an unrelated document
// on the same table is written. (Regression guard; today's canonical diff would
// also suppress this — the skip additionally avoids the re-run entirely.)
#[tokio::test]
async fn get_subscription_skips_update_for_unrelated_doc() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let insert_a = state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;
    let id_a = insert_a.results[0]["id"]
        .as_str()
        .expect("insert id")
        .to_string();

    let get_query: Query = serde_json::from_value(serde_json::json!({
        "table": "workItems",
        "get": id_a,
    }))
    .expect("parse get query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), get_query, tx)
        .await?;
    rx.try_recv().expect("initial query update");

    // Write a different document on the same table.
    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 2.0))
        .await?;

    assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
    Ok(())
}

// A non-get subscription (collect) still re-runs on any write to its table —
// the fine-grained skip is scoped to point reads only.
#[tokio::test]
async fn collect_subscription_still_reruns_on_table_write() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let conn = next_conn_id();
    state
        .committers
        .subscribe(&db, conn, "q1".to_string(), collect_work_items(), tx)
        .await?;
    rx.try_recv().expect("initial query update");

    state
        .committers
        .mutate(&db, None, insert_work_item("backlog", 1.0))
        .await?;

    let msg = rx.try_recv().expect("collect sub re-ran on table write");
    match msg {
        ServerMessage::QueryUpdate { result, .. } => {
            assert_eq!(docs_len(&result), 1);
        }
        other => panic!("expected QueryUpdate, got {other:?}"),
    }
    Ok(())
}
```

- [ ] **Step 7: Run the integration tests**

```bash
cd server && cargo test --test subs_test subscription
```
Expected: all three PASS (`get_subscription_updates_when_its_doc_is_written`, `get_subscription_skips_update_for_unrelated_doc`, `collect_subscription_still_reruns_on_table_write`).

- [ ] **Step 8: Commit**

```bash
git add server/src/subs.rs server/tests/subs_test.rs
git commit -m "feat(server): skip subscription re-runs for get(id) point reads (#21)

A get(id) subscription's result depends on exactly one document, so a
write that doesn't touch (table, id) cannot change it — skip the re-run.
Every other shape stays table-level. Sound: the only new skip is the
provably-independent case; under-invalidation is impossible."
```

---

## Task 3: Update FEATURE_MATRIX.md (#21)

**Files:**
- Modify: `FEATURE_MATRIX.md` (row #21).

- [ ] **Step 1: Read the current row**

`FEATURE_MATRIX.md`, row #21 (the line beginning `| 21 | 3 | **Fine-grained subscription invalidation**`).

- [ ] **Step 2: Flip status + update the note**

Replace that row with:

```markdown
| 21 | 3 | **Fine-grained subscription invalidation** | ✅ read-set tracking | ✅ point reads (`get`) | Low (now) | L | v1: `get(id)` point reads skip re-runs when the write didn't touch their document (sound — a point read depends on exactly one doc); every other shape (take/collect/count/paginate/unique/first/search/vector) stays table-level. Server-only, no protocol change. Range/boundary tracking for ordered/eq/set shapes deferred — see `docs/superpowers/specs/2026-07-24-fine-grained-subscription-invalidation-design.md`. |
```

- [ ] **Step 3: Commit**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs: flip FEATURE_MATRIX #21 to point-read invalidation (v1)"
```

---

## Final verification

- [ ] **Run the full gate** from the repo root:
```bash
make dev-db-up && make checkall
```
Expected: green (fmt-check + clippy `-D warnings` + typecheck + all tests, including the new ones).

- [ ] **Reference sweep (sanity)** — confirm nothing else reads `write_set`/`fan_out`/`SubEntry` in a way this change missed:
```bash
grep -rnE "write_set|fan_out|SubEntry|\.register\(" server/src
```
Expected: only the sites touched by Tasks 1–2 (txn.rs, subs.rs, committer.rs). No surprises.

## Out of scope (deferred — future "B")

Range/boundary tracking for `take(N)` ordered queries; eq-predicate evaluation for `unique`/`eq`; ranking-aware handling for `search`/`vectorSearch`. The `WriteSet.docs` and `ReadSet` shapes are designed so these extend additively.
