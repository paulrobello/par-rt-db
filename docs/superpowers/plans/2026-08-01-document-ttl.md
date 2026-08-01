# Document TTL / Auto-Expiry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add declarative per-document TTL (auto-expiry) to par-rt-db: a table declares a numeric field holding each document's absolute epoch-ms expiry, and a per-db reaper deletes expired rows through the committer so the four tap sites fire on every TTL delete.

**Architecture:** Mongo-style field + optional `defaultDurationMs`. The `ttl.field` is a normal declared field that must carry a single-field btree index (so it already has a typed `f_<field>` column). A new `reaper::run_reaper` task (third per-db task, mirrors `mutation_log::run_cleanup`) periodically enqueues a fire-and-forget `CommitterRequest::RunReaper`; `handle_reaper` batch-deletes expired rows inside the committer's serialized turn and publishes via `fan_out → op_feed → audit → webhook` with `source = "ttl"`, `owner = None`. Single-writer invariant preserved; reaper self-terminates on db deletion + channel close.

**Tech Stack:** Rust (axum/tokio/sqlx/Postgres 17), TypeScript (bun), Python (uv/pyright). Spec: `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.

## Global Constraints

- **Server field-type enum uses `FieldType::Number` (→ `double precision`) and `FieldType::Int64` (→ `bigint`).** TTL field must be one of these.
- **Physical names** are lowercased/prefixed: table → `t_<table>` (`ddl::pg_table`), field column → `f_<field>` (`ddl::pg_col`), schema → `db_<db>` (`ddl::pg_schema`). Never interpolate unvalidated identifiers; bind values via `$n`.
- **Single-writer invariant:** the only path that writes document tables is the committer turn. The reaper never writes directly — it enqueues `RunReaper`. Never call `execute_txn` outside the committer.
- **Tap-site contract:** every durable document write (including TTL deletes) publishes through `fan_out` + `op_feed.publish` + `audit::write_audit_rows` + `webhook::enqueue_for_ops`. TTL uses `source = "ttl"`, `owner = None`.
- **Clients mirror the server byte-for-byte:** `ttl` / `field` / `defaultDurationMs` casing is load-bearing. Adding/removing `ttl` is non-destructive.
- **Gate:** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) must pass before commit. Integration tests need the dev Postgres: `make dev-db-up`.
- **Metric refinement vs spec:** the dashboard `Metrics` struct uses global `AtomicU64` counters (no `db`/`table` labels — `subs_skips_*` etc. are all global). The TTL counter is therefore a global `ttl_expired_total`, surfaced in `MetricsSnapshot` + the Prometheus render, consistent with the other counters (not a labeled `{db,table}` series).

---

## Task 1: Server — `TtlDef`, `TableDef.ttl`, and validation

**Files:**
- Modify: `server/src/schema.rs` (add `TtlDef`; add `ttl` field to `TableDef`; add TTL block to `validate_structure`)
- Test: `server/src/schema.rs` (`mod tests`, unit — no DB)

**Interfaces:**
- Produces: `pub struct TtlDef { pub field: String, pub default_duration_ms: Option<i64> }`; `TableDef.ttl: Option<TtlDef>` (serde `rename = "ttl"`). Consumed by Tasks 2–4 (insert stamp, DDL backfill, reaper) and mirrored by Tasks 5–7.

- [ ] **Step 1: Write the failing tests**

Add to the `#[cfg(test)] mod tests` block in `server/src/schema.rs` (it already has `BTreeMap`, `FieldType`, `IndexDef`, `TableDef` imported — reuse them):

```rust
fn table_with_ttl(ttl: Option<TtlDef>) -> TableDef {
    let mut fields = BTreeMap::new();
    fields.insert("expiresAt".to_string(), FieldType::Number);
    TableDef {
        fields,
        indexes: vec![IndexDef {
            name: "by_expiresAt".to_string(),
            fields: vec!["expiresAt".to_string()],
            unique: false,
            search: false,
            vector: None,
            r#where: None,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl,
    }
}

#[test]
fn ttl_accepts_numeric_field_with_single_btree_index() {
    let mut schema = SchemaDef::default();
    schema.tables.insert(
        "t".to_string(),
        table_with_ttl(Some(TtlDef { field: "expiresAt".to_string(), default_duration_ms: Some(86_400_000) })),
    );
    assert!(schema.validate().is_ok());
}

#[test]
fn ttl_rejects_missing_index() {
    let mut table = table_with_ttl(Some(TtlDef { field: "expiresAt".to_string(), default_duration_ms: None }));
    table.indexes.clear();
    let mut schema = SchemaDef::default();
    schema.tables.insert("t".to_string(), table);
    let err = schema.validate().unwrap_err();
    assert!(err.message.contains("requires a single-field btree index"), "{}", err.message);
}

#[test]
fn ttl_rejects_non_numeric_field() {
    let mut table = table_with_ttl(Some(TtlDef { field: "name".to_string(), default_duration_ms: None }));
    table.fields.insert("name".to_string(), FieldType::String);
    let mut schema = SchemaDef::default();
    schema.tables.insert("t".to_string(), table);
    assert!(schema.validate().is_err());
}

#[test]
fn ttl_rejects_unique_or_partial_or_multifield_index() {
    for bad in [
        IndexDef { name: "x".to_string(), fields: vec!["expiresAt".to_string()], unique: true,  search: false, vector: None, r#where: None },
        IndexDef { name: "x".to_string(), fields: vec!["expiresAt".to_string()], unique: false, search: false, vector: None, r#where: Some("expiresAt > 0".to_string()) },
        IndexDef { name: "x".to_string(), fields: vec!["expiresAt".to_string(), "expiresAt".to_string()], unique: false, search: false, vector: None, r#where: None },
    ] {
        let mut table = table_with_ttl(Some(TtlDef { field: "expiresAt".to_string(), default_duration_ms: None }));
        table.indexes = vec![bad];
        let mut schema = SchemaDef::default();
        schema.tables.insert("t".to_string(), table);
        assert!(schema.validate().is_err(), "should reject this index variant");
    }
}

#[test]
fn ttl_rejects_non_positive_default_duration() {
    let mut schema = SchemaDef::default();
    schema.tables.insert(
        "t".to_string(),
        table_with_ttl(Some(TtlDef { field: "expiresAt".to_string(), default_duration_ms: Some(0) })),
    );
    assert!(schema.validate().is_err());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd server && cargo test --lib schema::tests::ttl`
Expected: compile error — `TtlDef` / `ttl` field do not exist yet.

- [ ] **Step 3: Add `TtlDef` and the `ttl` field**

In `server/src/schema.rs`, add near the other type defs (before `TableDef`):

```rust
/// Declarative document TTL (auto-expiry). `field` names a declared numeric
/// field whose value is each document's absolute epoch-ms expiry; a per-db
/// reaper deletes rows whose value is in the past. `default_duration_ms`
/// stamps the field at insert time when the client omits it. See
/// `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TtlDef {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}
```

Add the field to `pub struct TableDef` (after `collaborators_field`):

```rust
    /// Declarative document TTL. When `Some`, a per-db reaper deletes rows
    /// whose `ttl.field` value is in the past. Additive — schemas without it
    /// deserialize unchanged. See `TtlDef`.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ttl")]
    pub ttl: Option<TtlDef>,
```

- [ ] **Step 4: Add the TTL validation block**

At the end of `fn validate_structure(&self, table_name: &str)`, just before its final `Ok(())` (after the `for index in &self.indexes` loop):

```rust
        if let Some(ttl) = &self.ttl {
            if !is_valid_identifier(&ttl.field, MAX_FIELD_NAME_LEN) {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' is not a valid identifier",
                    ttl.field
                )));
            }
            let fty = self.fields.get(&ttl.field).ok_or_else(|| {
                RtDbError::schema(format!("ttl.field '{}' is not a declared field", ttl.field))
            })?;
            if !matches!(fty, FieldType::Number | FieldType::Int64) {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' must be a number or bigint field",
                    ttl.field
                )));
            }
            let has_ttl_index = self.indexes.iter().any(|idx| {
                !idx.search
                    && idx.vector.is_none()
                    && !idx.unique
                    && idx.r#where.is_none()
                    && idx.fields.len() == 1
                    && idx.fields[0] == ttl.field
            });
            if !has_ttl_index {
                return Err(RtDbError::schema(format!(
                    "ttl.field '{}' requires a single-field, non-unique, non-partial btree index on it",
                    ttl.field
                )));
            }
            if let Some(d) = ttl.default_duration_ms
                && d <= 0
            {
                return Err(RtDbError::schema(
                    "ttl.defaultDurationMs must be greater than 0".to_string(),
                ));
            }
        }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd server && cargo test --lib schema::tests::ttl`
Expected: PASS (5 tests).

- [ ] **Step 6: Commit**

```bash
git add server/src/schema.rs
git commit -m "feat(server): TtlDef + TableDef.ttl + validation"
```

---

## Task 2: Server — stamp TTL default at insert

**Files:**
- Modify: `server/src/txn.rs` (add `stamp_ttl_default`; call it in the `Step::Insert` arm, `execute_txn` ~line 1005)
- Test: `server/tests/ttl_test.rs` (new integration test binary)

**Interfaces:**
- Consumes: `TableDef.ttl` (Task 1), `crate::db::now_ms()`.
- Produces: documents inserted into a TTL-with-default table carry the `expiresAt` field even when the client omitted it.

- [ ] **Step 1: Write the failing integration test**

Create `server/tests/ttl_test.rs`. Reuse the shared test harness in `server/tests/common` (look at `server/tests/scheduled_test.rs` for the exact helpers — `setup`/`create_db`/`push_schema`/`mutate`/`query` patterns). Seed the file:

```rust
mod common;

use common::*;
use serde_json::json;

#[tokio::test]
async fn insert_stamps_ttl_default_when_field_absent() {
    let (pool, db) = setup("ttl_insert_stamp").await; // unique db name; see common::setup
    push_schema(
        &db,
        json!({
            "sessions": {
                "fields": { "expiresAt": "number", "userId": "string" },
                "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
                "ttl": { "field": "expiresAt", "defaultDurationMs": 86_400_000 }
            }
        }),
    )
    .await;

    let before = now_ms_approx();
    let res = mutate(&db, json!([{ "insert": { "table": "sessions", "doc": { "userId": "u1" } } }])).await;
    let id = res[0]["id"].as_str().unwrap().to_string();
    let docs = query(&db, json!({ "get": { "table": "sessions", "id": id } })).await;
    let after = now_ms_approx();

    let expires = docs["_expiresAt"].as_i64().or_else(|| docs["expiresAt"].as_i64()).unwrap();
    // default = 1 day; stamped within the insert window.
    assert!(expires >= before + 86_400_000 && expires <= after + 86_400_000, "expiresAt={expires}");
    drop_db(&db).await;
}
```

> **Note for the implementer:** open `server/tests/scheduled_test.rs` and copy the exact `setup`/`push_schema`/`mutate`/`query`/`drop_db` helper names and the schema JSON shape it uses — match that file's conventions for the db-name suffix, admin auth, and the query/mutate wire envelopes. If `scheduled_test.rs` names a helper differently (e.g. `setup_db`), use that name. Add a `now_ms_approx()` helper to `common` returning `chrono::Utc::now().timestamp_millis()` if one does not already exist. The point of the test is the assertion, not inventing new harness conventions.

- [ ] **Step 2: Run the test to verify it fails**

Run: `make dev-db-up && cd server && cargo test --test ttl_test insert_stamps_ttl_default_when_field_absent`
Expected: FAIL — `expiresAt` is null (field not stamped).

- [ ] **Step 3: Add `stamp_ttl_default` and call it**

In `server/src/txn.rs`, add the helper near `stamp_owner`:

```rust
/// Stamps the TTL field at insert time when the table declares a
/// `default_duration_ms` and the document omits the field. After this, TTL is
/// just the field's value (patch/replace manipulate it normally). See the TTL
/// spec.
fn stamp_ttl_default(table_def: &TableDef, mut doc: serde_json::Value, now: i64) -> serde_json::Value {
    if let Some(ttl) = &table_def.ttl
        && let Some(d) = ttl.default_duration_ms
        && let Some(obj) = doc.as_object_mut()
        && !obj.contains_key(&ttl.field)
    {
        obj.insert(ttl.field.clone(), serde_json::Value::from(now + d));
    }
    doc
}
```

In the `Step::Insert { table, doc } => {` arm of `execute_txn` (~line 1005), insert one line after `let table_def = schema.table(table)?;` and before the `stamp_owner` line:

```rust
                let table_def = schema.table(table)?;
                let doc = stamp_ttl_default(table_def, doc.clone(), crate::db::now_ms());
                let doc = stamp_owner(table_def, doc, owner);
```

(Ensure `TableDef` is in scope in `txn.rs`; it already is — `do_insert` takes a `&TableDef`.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd server && cargo test --test ttl_test insert_stamps_ttl_default_when_field_absent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/txn.rs server/tests/ttl_test.rs server/tests/common
git commit -m "feat(server): stamp TTL default field at insert"
```

---

## Task 3: Server — backfill `expiresAt` when TTL is added to an existing table

**Files:**
- Modify: `server/src/ddl.rs` (in `push_schema`, per-table body — stamp existing rows when a default is declared)
- Test: `server/tests/ttl_test.rs` (append one test)

**Interfaces:**
- Consumes: `TableDef.ttl` (Task 1), `ddl::pg_col` / `pg_table` / `pg_schema`.
- Produces: adding `ttl` with a default to a table that already has rows stamps `f_<field> = created_at + default` on rows lacking the field.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/ttl_test.rs`:

```rust
#[tokio::test]
async fn adding_ttl_backfills_existing_rows() {
    let (pool, db) = setup("ttl_backfill").await;
    // 1. Push a table with NO ttl, insert a row (no expiresAt).
    push_schema(&db, json!({
        "sessions": {
            "fields": { "expiresAt": "number", "userId": "string" },
            "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }]
        }
    })).await;
    let before = now_ms_approx();
    let res = mutate(&db, json!([{ "insert": { "table": "sessions", "doc": { "userId": "u1" } } }])).await;
    let id = res[0]["id"].as_str().unwrap().to_string();
    let after = now_ms_approx();

    // 2. Add ttl with a default. Existing row should be backfilled.
    push_schema(&db, json!({
        "sessions": {
            "fields": { "expiresAt": "number", "userId": "string" },
            "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
            "ttl": { "field": "expiresAt", "defaultDurationMs": 3_600_000 }
        }
    })).await;

    let docs = query(&db, json!({ "get": { "table": "sessions", "id": id } })).await;
    let expires = docs["expiresAt"].as_i64().unwrap();
    // created_at ∈ [before, after]; backfill = created_at + 1h.
    assert!(expires >= before + 3_600_000 && expires <= after + 3_600_000, "expiresAt={expires}");
    drop_db(&db).await;
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd server && cargo test --test ttl_test adding_ttl_backfills_existing_rows`
Expected: FAIL — `expiresAt` is null (no backfill).

- [ ] **Step 3: Add the backfill UPDATE**

In `server/src/ddl.rs::push_schema`, at the end of the `for (table_name, new_table) in &schema.tables` loop body (after the index-creation block, before the loop's closing brace — so it runs for both new and existing tables; a no-op when there are no NULL rows):

```rust
        if let Some(ttl) = &new_table.ttl
            && let Some(d) = ttl.default_duration_ms
        {
            let col = pg_col(&ttl.field);
            sqlx::query(&format!(
                "UPDATE \"{pg_schema_name}\".\"{table_ident}\" \
                 SET \"{col}\" = created_at + $1 \
                 WHERE \"{col}\" IS NULL"
            ))
            .bind(d)
            .execute(&mut *tx)
            .await?;
        }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd server && cargo test --test ttl_test adding_ttl_backfills_existing_rows`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/ddl.rs server/tests/ttl_test.rs
git commit -m "feat(server): backfill expiresAt when ttl is added to a table"
```

---

## Task 4: Server — the reaper (config, metric, task, committer arm)

**Files:**
- Modify: `server/src/config.rs` (two `Config` fields + env parse)
- Modify: `server/src/metrics.rs` (`ttl_expired_total` counter + record + snapshot + render)
- Modify: `server/src/committer.rs` (`RunReaper` arm, `handle_reaper`, `CommitterCtx`/`Committers` plumbing, spawn in `channel_for`)
- Create: `server/src/reaper.rs` (`run_reaper`)
- Modify: `server/src/lib.rs` (declare `mod reaper;`, pass new config + metrics to `Committers::new`)
- Test: `server/tests/ttl_test.rs` (append reaper tests)

**Interfaces:**
- Consumes: `TableDef.ttl` (Task 1), `ddl::pg_col`/`pg_table`/`pg_schema`, `WriteSet`/`DocOp`/`OpKind` (`txn`), `now_ms`, `database_exists`, the four tap-site fns.
- Produces: expired documents are deleted and published through all four tap sites with `source = "ttl"`; `RunReaper` is a new `CommitterRequest` arm; `reaper::run_reaper` is the per-db task.

- [ ] **Step 1: Add config fields + env parse**

In `server/src/config.rs` `pub struct Config`, add (e.g. after `subs_verify_skip_every`):

```rust
    // Document TTL reaper. RTDB_TTL_SWEEP_INTERVAL_SECS (default 60) is the
    // per-db cadence; RTDB_TTL_BATCH (default 5000) bounds rows deleted per
    // table per sweep. TTL is best-effort, so these are boot-only (not hot).
    pub ttl_sweep_interval_secs: u64,
    pub ttl_batch: i64,
```

In `Config::from_env`, mirror the `rate_limit_per_token_rpm` parse style:

```rust
        let ttl_sweep_interval_secs = match std::env::var("RTDB_TTL_SWEEP_INTERVAL_SECS") {
            Ok(v) => v.parse::<u64>().unwrap_or(60),
            Err(_) => 60,
        };
        let ttl_batch = match std::env::var("RTDB_TTL_BATCH") {
            Ok(v) => v.parse::<i64>().unwrap_or(5000),
            Err(_) => 5000,
        };
```

Add both fields to the `Self { … }` literal returned at the end of `from_env`.

- [ ] **Step 2: Add the metric**

In `server/src/metrics.rs`, add to `struct Metrics` (near the subs counters):

```rust
    /// TTL reaper: total expired documents deleted across all dbs/tables.
    ttl_expired_total: AtomicU64,
```

Add a recorder method next to the other `record_*`:

```rust
    /// A TTL reaper sweep deleted an expired document.
    pub fn record_ttl_expired(&self) {
        self.ttl_expired_total.fetch_add(1, Ordering::Relaxed);
    }
```

Add `ttl_expired_total` to the `MetricsSnapshot` struct definition, to the `snapshot()` method (read it with `.load(Ordering::Relaxed)` exactly like the neighboring `subs_*_total` counters), and emit a `ttlExpiredTotal` line in `render_prometheus` (`metrics.rs` ~line 272) next to the existing `subsSkipsTotal`-style lines. Match the exact snapshot-field naming + Prometheus text format the neighboring counters use.

- [ ] **Step 3: Write the failing reaper integration test**

Append to `server/tests/ttl_test.rs`. Set the sweep interval to 1s for the test via the env the test process already runs under (set `RTDB_TTL_SWEEP_INTERVAL_SECS=1` when running, below). The test inserts a doc with an explicit past `expiresAt` and polls until it is gone:

```rust
#[tokio::test]
async fn reaper_deletes_expired_document() {
    let (pool, db) = setup("ttl_reap").await;
    push_schema(&db, json!({
        "sessions": {
            "fields": { "expiresAt": "number" },
            "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
            "ttl": { "field": "expiresAt" }
        }
    })).await;

    // Expired: expiresAt well in the past.
    let past = now_ms_approx() - 1_000_000;
    let res = mutate(&db, json!([{ "insert": { "table": "sessions", "doc": { "expiresAt": past } } }])).await;
    let expired_id = res[0]["id"].as_str().unwrap().to_string();
    // Not expired: far future.
    let future = now_ms_approx() + 1_000_000;
    let res = mutate(&db, json!([{ "insert": { "table": "sessions", "doc": { "expiresAt": future } } }])).await;
    let live_id = res[0]["id"].as_str().unwrap().to_string();

    // Poll until the reaper sweeps (interval=1s in test). Bound to ~10s.
    let mut gone = false;
    for _ in 0..100 {
        let docs = query(&db, json!({ "get": { "table": "sessions", "id": expired_id.clone() } })).await;
        if docs.is_null() { gone = true; break; }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(gone, "expired doc was not reaped");

    // The live doc is untouched.
    let docs = query(&db, json!({ "get": { "table": "sessions", "id": live_id } })).await;
    assert_eq!(docs["_id"].as_str().unwrap(), live_id);
    drop_db(&db).await;
}
```

- [ ] **Step 4: Run the test to verify it fails**

Run: `make dev-db-up && cd server && RTDB_TTL_SWEEP_INTERVAL_SECS=1 cargo test --test ttl_test reaper_deletes_expired_document`
Expected: FAIL / timeout — `gone` stays false (no reaper running).

- [ ] **Step 5: Create `server/src/reaper.rs`**

```rust
//! Per-database document TTL reaper. A periodic timer (mirroring
//! `mutation_log::run_cleanup`) that enqueues a fire-and-forget `RunReaper` on
//! the committer channel; `committer::handle_reaper` performs the batch delete
//! inside the serialized committer turn so the four tap sites fire. See
//! `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc::Sender;

use crate::committer::CommitterRequest;
use crate::db::database_exists;

/// The per-db TTL reaper loop. Every `sweep_interval`, enqueues one
/// `RunReaper` on the committer channel. Exits when the committer channel
/// closes (its task died) or when the database is dropped (the
/// `database_exists` check mirrors `scheduler::run_scheduler` and
/// `mutation_log::run_cleanup`, so `delete-db` retires this task cleanly).
pub async fn run_reaper(
    pool: PgPool,
    db: String,
    committer_tx: Sender<CommitterRequest>,
    sweep_interval: Duration,
) {
    let mut tick = tokio::time::interval(sweep_interval);
    tick.tick().await; // skip the immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if matches!(database_exists(&pool, &db).await, Ok(false)) {
                    tracing::info!(db = %db, "ttl reaper: database removed, exiting");
                    return;
                }
                if committer_tx.send(CommitterRequest::RunReaper).await.is_err() {
                    tracing::debug!(db = %db, "ttl reaper: committer channel closed, exiting");
                    return;
                }
            }
            _ = committer_tx.closed() => {
                tracing::debug!(db = %db, "ttl reaper: committer channel closed, exiting");
                return;
            }
        }
    }
}
```

- [ ] **Step 6: Wire the committer arm, `handle_reaper`, and the spawn**

In `server/src/committer.rs`:

(a) Add the arm to `pub enum CommitterRequest`:

```rust
    /// A TTL reaper sweep is due. Fire-and-forget like `RunScheduled`: the
    /// reaper task does not wait for a reply. The committer runs the batch
    /// delete inside its serialized turn and publishes through the four tap
    /// sites with `source = "ttl"`.
    RunReaper,
```

(b) Add fields to `struct Committers`:

```rust
    ttl_sweep_interval: std::time::Duration,
    ttl_batch: i64,
    metrics: Arc<crate::metrics::Metrics>,
```

Update `Committers::new` to accept `ttl_sweep_interval_secs: u64, ttl_batch: i64, metrics: Arc<crate::metrics::Metrics>`, store `ttl_sweep_interval: Duration::from_secs(ttl_sweep_interval_secs)`, `ttl_batch`, `metrics`.

(c) Add fields to `struct CommitterCtx`:

```rust
    ttl_batch: i64,
    metrics: Arc<crate::metrics::Metrics>,
```

Thread them through `run_committer`'s params (it already has `#[allow(clippy::too_many_arguments)]`) and into the `CommitterCtx { … }` literal.

(d) Match the arm in `run_committer`'s `while let Some(req) = rx.recv().await` match:

```rust
            CommitterRequest::RunReaper => {
                if let Err(err) = handle_reaper(&ctx).await {
                    tracing::error!(db = %ctx.db, error = %err, "ttl reaper handling failed");
                }
            }
```

(e) Spawn the reaper in `channel_for`, right after the `mutation_log::run_cleanup` spawn:

```rust
        tokio::spawn(crate::reaper::run_reaper(
            self.pool.clone(),
            db.to_string(),
            tx.clone(),
            self.ttl_sweep_interval,
        ));
```

(f) Add `handle_reaper`. At the top of the file ensure these imports: `std::collections::{BTreeMap, BTreeSet}` (BTreeSet already imported via `HashMap`? — add `BTreeSet`/`BTreeMap` explicitly), and `use crate::txn::{DocOp, OpKind};` alongside the existing `use crate::txn::{Transaction, TxnOutcome, WriteSet, execute_txn};`:

```rust
/// Runs one TTL reaper sweep. For each table with `ttl`, batch-deletes expired
/// rows and publishes through the four tap sites with `source = "ttl"`. TTL
/// deletes are system-initiated (`owner = None`), bypassing per-row auth like
/// scheduled jobs. Fire-and-forget — errors are logged, not surfaced; a failed
/// delete retries on the next sweep. Each table's delete is an independent
/// statement so one table's failure does not abort the others.
async fn handle_reaper(ctx: &CommitterCtx) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let now = now_ms();
    let mut tables: BTreeSet<String> = BTreeSet::new();
    let mut docs: BTreeSet<(String, String)> = BTreeSet::new();
    let mut ops: Vec<DocOp> = Vec::new();
    for (table_name, table_def) in &schema.tables {
        let Some(ttl) = &table_def.ttl else { continue };
        let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
        let table_ident = crate::ddl::pg_table(table_name);
        let col = crate::ddl::pg_col(&ttl.field);
        let rows: Vec<(String,)> = match sqlx::query_as(&format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE id IN (
                 SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\"
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1
                 ORDER BY \"{col}\" LIMIT $2
             ) RETURNING id"
        ))
        .bind(now)
        .bind(ctx.ttl_batch)
        .fetch_all(&ctx.pool)
        .await {
            Ok(rows) => rows,
            Err(e) => {
                if matches!(crate::db::database_exists(&ctx.pool, &ctx.db).await, Ok(false)) {
                    return Ok(()); // db dropped mid-sweep
                }
                tracing::warn!(db = %ctx.db, table = %table_name, error = %e, "ttl reaper delete failed");
                continue;
            }
        };
        if rows.is_empty() { continue; }
        tables.insert(table_name.clone());
        for (id,) in rows {
            docs.insert((table_name.clone(), id.clone()));
            ops.push(DocOp { table: table_name.clone(), id, kind: OpKind::Delete });
        }
    }
    if ops.is_empty() { return Ok(()); }
    let write_set = WriteSet { tables, docs, ops: ops.clone(), doc_values: BTreeMap::new() };
    ctx.subs.fan_out(&ctx.pool, &ctx.db, &schema, &write_set).await;
    ctx.op_feed.publish(&ctx.db, None, &write_set.ops).await;
    if ctx.audit_log_enabled
        && let Err(e) = crate::audit::write_audit_rows(&ctx.pool, &ctx.db, None, "ttl", &write_set.ops).await
    {
        tracing::warn!(db = %ctx.db, error = %e, "audit log write failed (ttl)");
    }
    if ctx.webhooks_enabled
        && let Err(e) = crate::webhook::enqueue_for_ops(&ctx.pool, &ctx.db, None, "ttl", &write_set.ops).await
    {
        tracing::warn!(db = %ctx.db, error = %e, "webhook enqueue failed (ttl)");
    }
    for _ in 0..ops.len() { ctx.metrics.record_ttl_expired(); }
    Ok(())
}
```

In `server/src/lib.rs`: add `mod reaper;` (next to `mod scheduler;`), and at the `Committers::new(` call site (~line 100) add the three new args: `config.ttl_sweep_interval_secs, config.ttl_batch, metrics.clone()` (use the same `metrics` `Arc<Metrics>` `AppState` already holds — confirm the exact binding name in `lib.rs`).

- [ ] **Step 7: Run the reaper test to verify it passes**

Run: `make dev-db-up && cd server && RTDB_TTL_SWEEP_INTERVAL_SECS=1 cargo test --test ttl_test reaper_deletes_expired_document`
Expected: PASS.

- [ ] **Step 8: Add the remaining reaper coverage tests**

Append to `server/tests/ttl_test.rs` (mirror the audit/webhook assertion style in `server/tests/audit_test.rs` and `webhook_test.rs` for the source-string check; mirror `per_row_auth_test.rs` for the per-row bypass shape):

```rust
#[tokio::test]
async fn reaper_delete_publishes_to_audit_and_webhooks() {
    // Requires RTDB_AUDIT_LOG_ENABLED + RTDB_WEBHOOKS_ENABLED in the test env.
    // Assert a row reaped from a ttl table appears in GET /admin/audit with
    // op="delete", source="ttl", and a webhook delivery with source="ttl".
    // Mirror audit_test.rs / webhook_test.rs helper calls exactly.
}

#[tokio::test]
async fn reaper_bypasses_per_row_owner_auth() {
    // Insert (as owner A) a row owned by A with a past expiresAt on a table
    // with ownerField + ttl. The reaper (system principal) still deletes it.
    // Mirror per_row_auth_test.rs's owner-scoped insert helper.
}

#[tokio::test]
async fn reaper_ignores_tables_without_ttl() {
    // Two tables in one db: one ttl, one not. Insert expired-shaped docs in
    // both (the non-ttl table has an "expiresAt" field but no ttl). Only the
    // ttl table's row is reaped.
}
```

 Flesh each out by copying the concrete helper calls from the referenced sibling test files (do not leave the bodies as comments — copy the real `audit_rows(&db).await` / webhook-list / owner-insert calls and assert on `source == "ttl"`). Run with `RTDB_TTL_SWEEP_INTERVAL_SECS=1 RTDB_AUDIT_LOG_ENABLED=true RTDB_WEBHOOKS_ENABLED=true`.

- [ ] **Step 9: Run the full server suite + gate for this task**

Run: `make dev-db-up && cd server && cargo test --test ttl_test`
Expected: PASS (all ttl tests).

- [ ] **Step 10: Commit**

```bash
git add server/src/config.rs server/src/metrics.rs server/src/committer.rs server/src/reaper.rs server/src/lib.rs server/tests/ttl_test.rs
git commit -m "feat(server): per-db TTL reaper (RunReaper arm + handle_reaper + task)"
```

---

## Task 5: ts-client — `ttl` schema + in-memory `tick()`

**Files:**
- Modify: `ts-client/src/schema.ts` (add `ttl?` to the `TableDef` interface)
- Modify: `ts-client/src/in_memory.ts` (stamp default in `doInsert` ~line 1264; add `tick(now?)`)
- Test: `ts-client/tests/ttl.test.ts` (new)

**Interfaces:**
- Consumes: the server `TableDef.ttl` wire shape (Task 1).
- Produces: `TableDef["ttl"]` type; `client.tick(now?)` expires docs in the in-memory harness.

- [ ] **Step 1: Write the failing test**

Create `ts-client/tests/ttl.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { defineSchema, InMemoryRtDbClient } from "../src";

describe("ttl", () => {
  it("stamps the default at insert and expires on tick", async () => {
    const schema = defineSchema({
      sessions: { fields: { expiresAt: "number" }, indexes: [{ name: "by_expiresAt", fields: ["expiresAt"] }], ttl: { field: "expiresAt", defaultDurationMs: 1000 } },
    } as const);
    const db = new InMemoryRtDbClient(schema);
    const { id } = await db.mutate([{ insert: { table: "sessions", doc: {} } }]);
    let doc = await db.query({ get: { table: "sessions", id } });
    expect(doc.expiresAt).toBe(10_000 + 1000); // tick anchored at 10000 in the harness, or use real now — see note
    db.tick(12_000); // past the stamped expiry
    doc = await db.query({ get: { table: "sessions", id } });
    expect(doc).toBeNull();
  });
});
```

> **Note for the implementer:** confirm the in-memory harness's clock anchor — if `doInsert` already uses `Date.now()`-based `_creationTime`, anchor `tick()`'s default to the same source (pass an explicit `now` to both, or read the inserted doc's `_creationTime` and tick past `_creationTime + defaultDurationMs`). Adjust the test's literals to whatever clock the harness uses; the assertions (stamped present, then gone after tick) are what matter. Match the exact `defineSchema` / `InMemoryRtDbClient` / `mutate` / `query` signatures already used in `ts-client/tests/` (open one existing test file and copy its imports).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd ts-client && bunx vitest run tests/ttl.test.ts`
Expected: FAIL — `ttl` type / `tick` do not exist.

- [ ] **Step 3: Add the `ttl` type + stamp + tick**

In `ts-client/src/schema.ts`, add to the `TableDef` interface:

```ts
  ttl?: { field: string; defaultDurationMs?: number };
```

In `ts-client/src/in_memory.ts` `doInsert` (~line 1264), after the doc is built and before it is stored, mirror the server stamp:

```ts
    const ttl = this.tables.get(table)?.ttl;
    if (ttl?.defaultDurationMs != null) {
      const obj = doc as Record<string, unknown>;
      if (obj[ttl.field] == null) obj[ttl.field] = this.now() + ttl.defaultDurationMs;
    }
```

(Use the same `this.now()` / clock the harness already uses for `_creationTime` — confirm the method name in `in_memory.ts` and use it; if it inlines `Date.now()`, factor a `now()` helper or reuse the existing one.)

Add the `tick` method to the `InMemoryRtDbClient` class:

```ts
  /** Remove documents whose ttl field value is in the past. Returns the count removed. */
  tick(now: number = this.now()): number {
    let removed = 0;
    for (const [table, docs] of this.docs) {        // this.docs: Map<string, Map<string, Doc>> — confirm the field name
      const ttl = this.tables.get(table)?.ttl;
      if (!ttl) continue;
      for (const [id, doc] of docs) {
        const v = (doc as Record<string, unknown>)[ttl.field];
        if (typeof v === "number" && v < now) { docs.delete(id); removed++; }
      }
    }
    return removed;
  }
```

(Confirm the harness's doc-store field name — `this.docs`/`this._docs`/`this.tables` — by reading `in_memory.ts`'s insert path; use the real one. A tick removing a doc need not fire subscription fan-out in the in-memory harness unless the harness already models subscriptions — if it does, mirror its delete notification.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd ts-client && bunx vitest run tests/ttl.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/schema.ts ts-client/src/in_memory.ts ts-client/tests/ttl.test.ts
git commit -m "feat(ts-client): ttl schema + in-memory tick()"
```

---

## Task 6: rust-client — `TtlDef` + in-memory `tick()`

**Files:**
- Modify: `rust-client/src/schema.rs` (add `TtlDef` + `ttl` field to `TableDef`, ~line 153)
- Modify: `rust-client/src/in_memory.rs` (stamp in `do_insert` ~line 1550; add `tick`)
- Test: `rust-client/tests/ttl.rs` (new)

**Interfaces:**
- Consumes: the server wire shape (Task 1).
- Produces: `TtlDef`; `client.tick(now)`.

- [ ] **Step 1: Write the failing test**

Create `rust-client/tests/ttl.rs` (mirror an existing `rust-client/tests/*.rs` for the client construction + mutate + query pattern):

```rust
use par_rt_db_client::*; // adjust to the real crate re-exports used by sibling tests

#[tokio::test]
async fn stamps_default_and_expires_on_tick() {
    // Build a schema with a sessions table: field expiresAt:Number, a single
    // btree index on it, ttl { field expiresAt, default_duration_ms 1000 }.
    // Insert with an empty doc; assert the returned/stored doc has expiresAt.
    // tick(now past the expiry); assert get() is now NotFound/None.
}
```

> **Note for the implementer:** open `rust-client/tests/` and copy the exact client-builder, schema-construction, `mutate`, and `query` calls an existing test uses; fill the body with the stamp + tick assertions. The harness's clock helper (`now()`/`creation_time`) determines the tick literal — read `do_insert` to see how `_creationTime` is set and tick past it.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd rust-client && cargo test --test ttl`
Expected: FAIL — `TtlDef` / `tick` missing.

- [ ] **Step 3: Add `TtlDef` + stamp + tick**

In `rust-client/src/schema.rs` add (mirror the server struct's derives + serde casing):

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TtlDef {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}
```

Add to `pub struct TableDef` (after `collaborators_field`):

```rust
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ttl")]
    pub ttl: Option<TtlDef>,
```

In `rust-client/src/in_memory.rs` `do_insert` (~line 1550), stamp the default when absent (mirror the server `stamp_ttl_default` logic using the harness's `now`). Add a `pub fn tick(&mut self, now: i64) -> usize` that iterates each table with `ttl` and removes docs whose `ttl.field` value `< now`, returning the count removed. Confirm the harness's doc-store field (e.g. `self.tables`/`self.docs`) by reading the insert path.

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd rust-client && cargo test --test ttl`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/schema.rs rust-client/src/in_memory.rs rust-client/tests/ttl.rs
git commit -m "feat(rust-client): TtlDef + in-memory tick()"
```

---

## Task 7: python-client — `TtlDef` + in-memory `tick()`

**Files:**
- Modify: `python-client/src/par_rt_db/schema.py` (add `TtlDef`; add `ttl` to `TableDef`, ~line 191)
- Modify: `python-client/src/par_rt_db/in_memory.py` (stamp in `_do_insert` ~line 1138; add `tick`)
- Test: `python-client/tests/test_ttl.py` (new)

**Interfaces:**
- Consumes: the server wire shape (Task 1).
- Produces: `TtlDef`; `client.tick(now_ms=None)`.

- [ ] **Step 1: Write the failing test**

Create `python-client/tests/test_ttl.py` (mirror an existing `python-client/tests/test_in_memory*.py`):

```python
from par_rt_db import define_schema, InMemoryRtDbClient

def test_stamps_default_and_expires_on_tick():
    schema = define_schema({"sessions": {
        "fields": {"expiresAt": "number"},
        "indexes": [{"name": "by_expiresAt", "fields": ["expiresAt"]}],
        "ttl": {"field": "expiresAt", "defaultDurationMs": 1000},
    }})
    db = InMemoryRtDbClient(schema)
    res = db.mutate([{"insert": {"table": "sessions", "doc": {}}}])
    doc = db.query({"get": {"table": "sessions", "id": res[0]["id"]}})
    assert doc["expiresAt"] is not None
    db.tick(now_ms=doc["_creationTime"] + 2000)
    assert db.query({"get": {"table": "sessions", "id": res[0]["id"]}}) is None
```

> **Note for the implementer:** confirm the exact `define_schema` / `InMemoryRtDbClient` / `mutate` / `query` signatures from an existing `python-client/tests/test_in_memory*.py` and match them.

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd python-client && uv run pytest -q tests/test_ttl.py`
Expected: FAIL — `ttl`/`tick` missing.

- [ ] **Step 3: Add `TtlDef` + stamp + tick**

In `python-client/src/par_rt_db/schema.py`, add (mirror the existing pydantic `_S` base + serializer-drop-None convention `TableDef` uses):

```python
class TtlDef(_S):
    field: str
    default_duration_ms: int | None = None
```

Add to `class TableDef`:

```python
    ttl: TtlDef | None = None
```

(Extend the existing `model_serializer` wrapper to also drop `ttl` when `None`, matching how it drops `ownerField`/`collaboratorsField`.) In `python-client/src/par_rt_db/in_memory.py` `_do_insert` (~line 1138), stamp the default when the doc omits the field (use the harness's `_now_ms()`). Add:

```python
    def tick(self, now_ms: int | None = None) -> int:
        now = now_ms if now_ms is not None else self._now_ms()
        removed = 0
        for name, tdef in self._tables.items():
            if not tdef.ttl:
                continue
            field = tdef.ttl.field
            for doc_id in [d for d, doc in self._docs_of(name).items()
                           if isinstance(doc.get(field), (int, float)) and doc[field] < now]:
                self._delete(name, doc_id)  # use the harness's existing delete helper
                removed += 1
        return removed
```

(Confirm the harness's doc-store + delete helper names — `self._docs_of`/`self._delete`/`self._tables` — by reading `in_memory.py`'s insert/delete paths; use the real names.)

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd python-client && uv run pytest -q tests/test_ttl.py`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add python-client/src/par_rt_db/schema.py python-client/src/par_rt_db/in_memory.py python-client/tests/test_ttl.py
git commit -m "feat(python-client): TtlDef + in-memory tick()"
```

---

## Task 8: Docs + full gate + kanban

**Files:**
- Modify: `FEATURE_MATRIX.md` (new TTL row)
- Modify: `CLAUDE.md` (Architecture + Invariants bullets)

**Interfaces:** none (documentation only).

- [ ] **Step 1: Update `FEATURE_MATRIX.md`**

Add a row for Document TTL / auto-expiry. Note par-rt-db ✅ and Convex ❌ (no built-in document TTL) — this is a par-rt-db advantage. Note client-mirror status (all four clients ship `ttl` + in-memory `tick()`).

- [ ] **Step 2: Update `CLAUDE.md`**

In the **Architecture — what spans files** section, add a bullet describing the reaper as the third per-db task (alongside the committer + scheduler), enqueuing `RunReaper`, with `handle_reaper` publishing at the tap sites `source = "ttl"`, single-writer preserved, self-terminates on db deletion + channel close. In the **Invariants you must preserve** section, add that TTL deletes are durable writes that publish at the tap sites, and that the reaper never writes outside the committer.

- [ ] **Step 3: Run the full gate**

Run: `make checkall`
Expected: PASS (fmt-check + clippy `-D warnings` + typecheck across all five packages + the full test suite). If the dashboard typecheck fails on a stale `ts-client/dist`, run `make ts-client-build` first and re-run.

- [ ] **Step 4: Commit + close the kanban item**

```bash
git add FEATURE_MATRIX.md CLAUDE.md
git commit -m "docs: document TTL / auto-expiry (FEATURE_MATRIX + CLAUDE.md)"
```

Mark the kanban item done:
```bash
kanban item done --id 019fbe205cf57302af1f72f8c1e9ea8b
```

---

## Self-Review (completed)

**Spec coverage:**
- Expiry model (field + default) → Task 1 (schema/validate), Task 2 (insert stamp).
- Required declared btree index → Task 1 validation.
- Backfill on add → Task 3.
- Reaper task + `RunReaper` arm + `handle_reaper` + 4 taps + bypass + lifecycle → Task 4.
- Config (`RTDB_TTL_SWEEP_INTERVAL_SECS`, `RTDB_TTL_BATCH`) → Task 4 Step 1.
- Metric → Task 4 Step 2 (global counter, per the Global Constraints refinement).
- Clients mirror → Tasks 5/6/7.
- Docs → Task 8.
- Testing plan (unit + integration: stamp, reap, sub removal via fan_out, audit/webhook source, bypass, non-ttl untouched, backfill) → Tasks 1–4.
- Gap: an explicit *subscription receives the removal update* integration test. `fan_out` table-level re-runs on a delete (sound over-approximation) so subscribers see it, but there is no dedicated assertion. **Mitigation:** fold a subscription assertion into Task 4 Step 8's `reaper_delete_publishes_to_audit_and_webhooks` or add a small `reaper_notifies_subscription` test mirroring `subs_test.rs`'s subscribe-then-mutate-then-assert-update pattern. Flagged for the implementer.

**Placeholder scan:** The three client tasks and the Task-4 Step-8 audit/webhook/bypass tests reference sibling test files to copy concrete helper calls from, with the exact assertion (`source == "ttl"`, stamped-present → gone-after-tick) stated. This is intentional mirroring of existing harness conventions (not TBD) — the implementer copies real calls, not invents them. All server steps contain full code.

**Type consistency:** `TtlDef { field, default_duration_ms }` and `TableDef.ttl: Option<TtlDef>` are identical across the server (Task 1) and the three clients (Tasks 5–7). `RunReaper` (unit arm), `handle_reaper(ctx)`, `run_reaper(pool, db, tx, interval)`, `record_ttl_expired()` are used consistently. `stamp_ttl_default` is named the same in server (Task 2); clients inline equivalent stamp logic in their `do_insert` (different per-language harness, intentionally).
