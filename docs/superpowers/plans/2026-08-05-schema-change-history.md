# Schema Change History (ENH-013) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture a full schema snapshot on every push/migrate/restore, expose a dashboard history view with client-side diff, and allow restoring a prior schema version via an in-place destructive reconcile.

**Architecture:** A new per-db `schema_history` table (lazy, always-on) stores `SchemaDef` snapshots. A shared `schema_history::capture` function is called at the two sites that overwrite the live schema (`ddl::push_schema` via the admin handler, and `handle_migrate` in the committer). Restore is a new committer arm `RunRestoreSchema` that snapshots the outgoing schema, runs a destructive DDL reconcile to match the target snapshot, then snapshots the incoming schema — making restore itself versioned and undoable.

**Tech Stack:** Rust (axum/sqlx/tokio, server), TypeScript (ts-client SDK + Vite/React dashboard).

## Global Constraints

- Authoritative schema = one `SchemaDef` JSON blob at `key='schema'` in the per-db `meta` table (`server/src/db.rs:311`, read by `load_schema` `db.rs:474`). Two overwrite sites: `ddl.rs:461` (push, NOT in committer) and `committer.rs:778` (migrate, in committer).
- **Always-on, no Config flag.** Do NOT add a field to `Config` / `test_config()` (`tests/common/mod.rs`). The feature needs no boot env var, no `Committers`/`CommitterCtx` flag threading. This keeps `tests/common/mod.rs::test_config()` untouched.
- SQL safety: double-quote every identifier; the per-db schema identifier comes from the existing `pg_schema(db)` helper (lowercased + validated — used unqualified in `db.rs`, as `crate::ddl::pg_schema` in `committer.rs:777`; use the form matching the file you edit). Use `fetch_optional` for lookups that can miss. No value interpolation.
- Best-effort capture: a capture failure is `tracing::warn!`-ed, never propagated — the schema change has already committed (mirrors the audit tap contract, `committer.rs:807`).
- Errors: `RtDbError` envelope; client-facing 500s carry a generic message.
- Definition of done: `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests). Tests need the dev Postgres (`make dev-db-up`).
- Commits land on `main` (trunk-based repo). Use conventional prefixes (`feat`, `feat(dashboard)`, `test`, `docs`).

---

## File Structure

**Server (create):**
- `server/src/schema_history.rs` — capture/list/get + `ensure_table`. One responsibility: snapshot persistence.
- `server/tests/schema_history_test.rs` — integration tests (mirrors `tests/audit_test.rs`).

**Server (modify):**
- `server/src/lib.rs` — `pub mod schema_history;` in the module list.
- `server/src/admin.rs` — push-capture call, `GET history` + `GET history/{version}` + `POST restore` handlers + routes.
- `server/src/committer.rs` — migrate-capture call; `RunRestoreSchema` variant + dispatch + `handle_restore_schema` + `Committers::restore_schema`.
- `server/src/ddl.rs` — extract `apply_schema_additive`; add `reconcile_schema_destructive` + `reconcile_diff`.

**Clients:**
- `ts-client/src/protocol.ts` — wire types `SchemaHistoryEntrySummary`, `SchemaHistoryEntry`.
- `ts-client/src/admin.ts` — `getSchemaHistory`, `getSchemaVersion`, `restoreSchema`.
- `dashboard/src/lib/admin.tsx` — `AdminClient` methods for the three endpoints.
- `dashboard/src/pages/SchemaHistoryPage.tsx` + `.module.css` — the history UI + client-side diff.
- `dashboard/src/App.tsx` — route; `dashboard/src/pages/DbPage.tsx` — History link.

**Docs:**
- `FEATURE_MATRIX.md`, `server/src/audit.rs` + `server/src/webhook.rs` (stale "two tap sites" comments).

---

## Task 1: schema_history module + push capture + read endpoints

**Files:**
- Create: `server/src/schema_history.rs`
- Create: `server/tests/schema_history_test.rs`
- Modify: `server/src/lib.rs` (module list, ~line 1-31)
- Modify: `server/src/admin.rs` (`push_schema` handler ~line 206; new handlers + routes near `admin_routes()` ~line 1875)

**Interfaces:**
- Consumes: `crate::db::{now_ms, validate_db_name}` and the `pg_schema(db)` helper; `crate::schema::SchemaDef`; `crate::error::RtDbError`.
- Produces:
  - `schema_history::capture(pool, db, source: &str, principal: Option<&str>, schema: &SchemaDef) -> Result<(), RtDbError>`
  - `schema_history::list(pool, db, limit: i64, offset: i64) -> Result<Vec<HistorySummary>, RtDbError>`
  - `schema_history::get(pool, db, version: i64) -> Result<Option<HistoryEntry>, RtDbError>`
  - `HistorySummary { version, captured_at, source, principal }` (camelCase serialize, no blob)
  - `HistoryEntry { version, captured_at, source, principal, schema: serde_json::Value }`

- [ ] **Step 1: Write `schema_history.rs`**

```rust
//! Schema change history — captures a full `SchemaDef` snapshot on every push,
//! migrate, and restore, in a per-db `schema_history` table co-located with
//! `meta`. Always on (no config flag): low-volume, and its value is being
//! present when a revert is needed. Best-effort like the audit tap — a capture
//! failure is warned, never propagated (the schema change already committed).

use sqlx::PgPool;

use crate::db::{now_ms, validate_db_name};
use crate::error::RtDbError;
use crate::schema::SchemaDef;

/// Newest N snapshots kept per database. Cheap insurance against a schema
/// pushed in a loop; well above any realistic revert depth.
const MAX_VERSIONS: i64 = 100;

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistorySummary {
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    pub principal: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryEntry {
    pub version: i64,
    pub captured_at: i64,
    pub source: String,
    pub principal: Option<String>,
    pub schema: serde_json::Value,
}

/// Idempotent. Self-heals databases created before this feature shipped.
/// Mirrors the per-db side tables created in `db::create_database`.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema_name = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema_name}\".schema_history (\
            version     BIGSERIAL PRIMARY KEY,\
            captured_at BIGINT NOT NULL,\
            source      TEXT NOT NULL,\
            principal   TEXT,\
            schema      JSONB NOT NULL\
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a snapshot, then prune to the retention cap. Best-effort (callers
/// warn on `Err`). `source` is "push" | "migrate" | "restore".
pub async fn capture(
    pool: &PgPool,
    db: &str,
    source: &str,
    principal: Option<&str>,
    schema: &SchemaDef,
) -> Result<(), RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let value = serde_json::to_value(schema)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".schema_history (captured_at, source, principal, schema) \
         VALUES ($1, $2, $3, $4)"
    ))
    .bind(now_ms())
    .bind(source)
    .bind(principal)
    .bind(value)
    .execute(pool)
    .await?;
    sqlx::query(&format!(
        "DELETE FROM \"{schema_name}\".schema_history WHERE version NOT IN \
         (SELECT version FROM \"{schema_name}\".schema_history ORDER BY version DESC LIMIT $1)"
    ))
    .bind(MAX_VERSIONS)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list(
    pool: &PgPool,
    db: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<HistorySummary>, RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let rows: Vec<(i64, i64, String, Option<String>)> = sqlx::query_as(&format!(
        "SELECT version, captured_at, source, principal \
         FROM \"{schema_name}\".schema_history ORDER BY version DESC LIMIT $1 OFFSET $2"
    ))
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|(version, captured_at, source, principal)| HistorySummary {
            version,
            captured_at,
            source,
            principal,
        })
        .collect())
}

pub async fn get(pool: &PgPool, db: &str, version: i64) -> Result<Option<HistoryEntry>, RtDbError> {
    ensure_table(pool, db).await?;
    let schema_name = pg_schema(db);
    let row: Option<(i64, i64, String, Option<String>, serde_json::Value)> = sqlx::query_as(
        &format!(
            "SELECT version, captured_at, source, principal, schema \
             FROM \"{schema_name}\".schema_history WHERE version = $1"
        ),
    )
    .bind(version)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(version, captured_at, source, principal, schema)| HistoryEntry {
        version,
        captured_at,
        source,
        principal,
        schema,
    }))
}
```

Note on `pg_schema`: there is a `pg_schema(db)` physical-name helper used across `db.rs`/`ddl.rs`/`committer.rs`. Import it from the module the file you are editing uses it from — `db.rs` references it unqualified, `committer.rs:777` uses `crate::ddl::pg_schema`. Pick the path that matches the surrounding code; both lowercase and validate identically.

- [ ] **Step 2: Register the module**

In `server/src/lib.rs`, add `pub mod schema_history;` to the module list (alongside `pub mod audit;`, `pub mod webhook;`, etc.).

- [ ] **Step 3: Wire capture into the push handler**

In `server/src/admin.rs` `push_schema` (line ~206), after `state.schemas.put(&body.db, applied).await;`, add a best-effort capture. The handler already has the admin principal from `require_admin` — capture its identifier if `require_admin` returns one; otherwise pass `None`. Minimal change:

```rust
let applied = ddl::push_schema(&state.pool, &body.db, body.schema).await?;
state.schemas.put(&body.db, applied.clone()).await;
if let Err(err) = schema_history::capture(&state.pool, &body.db, "push", None, &applied).await {
    tracing::warn!(db = %body.db, error = %err, "schema history capture failed");
}
Ok(Json(OkResponse { ok: true }))
```

(If `require_admin` yields an `&AuthedUser`/email, thread it as the principal; otherwise `None` is acceptable for v1. Match what `push_schema` currently receives — do not change `require_admin`'s signature.)

- [ ] **Step 4: Add the two read handlers + routes**

In `admin.rs`, add (mirroring `get_schema` at line 549 and `audit_recent`/`AuditParams` at 1622/1657):

```rust
#[derive(Deserialize)]
struct HistoryParams {
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    offset: Option<i64>,
}

async fn schema_history_list(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    Query(params): Query<HistoryParams>,
) -> Result<Json<serde_json::Value>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let offset = params.offset.unwrap_or(0).max(0);
    let entries = schema_history::list(&state.pool, &db, limit, offset).await?;
    Ok(Json(serde_json::json!({ "entries": entries })))
}

async fn schema_history_get(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, version)): Path<(String, i64)>,
) -> Result<Json<schema_history::HistoryEntry>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    schema_history::get(&state.pool, &db, version)
        .await?
        .map(Json)
        .ok_or_else(|| RtDbError::not_found("schema version not found"))
}
```

Register in `admin_routes()` (line ~1875), near the other schema routes:

```rust
.route("/admin/db/{db}/schema/history", get(schema_history_list))
.route("/admin/db/{db}/schema/history/{version}", get(schema_history_get))
```

- [ ] **Step 5: Write the integration tests**

Create `server/tests/schema_history_test.rs` (mirror `tests/audit_test.rs` — it uses `common::{admin_get, admin_post, fresh_db, spawn_app, test_state}`). `schema_history` is always-on, so plain `test_state()` is enough (no `test_state_with_*` variant needed).

```rust
//! Integration tests for schema change history (ENH-013).

mod common;

use common::{admin_get, admin_post, fresh_db, spawn_app, test_state};
use serde_json::json;

/// POST `/admin/push-schema` and assert success.
async fn push(addr: std::net::SocketAddr, db: &str, schema: serde_json::Value) {
    let resp = admin_post(addr, "/admin/push-schema", json!({ "db": db, "schema": schema })).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "push failed: {:?}", resp.text().await);
}

async fn history(addr: std::net::SocketAddr, db: &str) -> serde_json::Value {
    let resp = admin_get(addr, &format!("/admin/db/{db}/schema/history")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse history");
    body["entries"].clone()
}

#[tokio::test]
async fn push_captures_a_version_and_latest_matches_live() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let db = fresh_db().await;

    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } })).await;
    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" }, "qty": { "type": "number" } } } } })).await;

    let entries = history(addr, &db).await;
    let arr = entries.as_array().expect("entries array");
    assert_eq!(arr.len(), 2, "two pushes -> two versions");
    assert_eq!(arr[0]["source"], "push");           // newest first
    assert!(arr[0]["version"].as_i64() > arr[1]["version"].as_i64());

    // Latest snapshot's schema equals the live schema.
    let newest_version = arr[0]["version"].as_i64().unwrap();
    let resp = admin_get(addr, &format!("/admin/db/{db}/schema/history/{newest_version}")).await;
    let entry: serde_json::Value = resp.json().await?;
    assert!(entry["schema"]["tables"]["items"]["fields"]["qty"].is_object());
    Ok(())
}

#[tokio::test]
async fn history_isolated_per_db_and_missing_version_404s() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let a = fresh_db().await;
    let b = fresh_db().await;
    push(addr, &a, json!({ "tables": { "t": { "fields": { "x": { "type": "string" } } } } })).await;
    // db b never had a schema pushed — its history is empty.
    let entries_b = history(addr, &b).await;
    assert_eq!(entries_b.as_array().unwrap().len(), 0);
    // Missing version on a -> 404.
    let resp = admin_get(addr, &format!("/admin/db/{a}/schema/history/999999")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn lazy_table_self_heals_for_preexisting_db() -> anyhow::Result<()> {
    // A db created directly (no push) has no schema_history table until the first
    // capture/ensure. GET history must still succeed (empty) rather than error.
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let db = fresh_db().await; // creates the db, no schema push
    let entries = history(addr, &db).await;
    assert_eq!(entries.as_array().unwrap().len(), 0);
    Ok(())
}
```

- [ ] **Step 6: Run the tests**

```bash
cd /Users/probello/Repos/par-rt-db && make dev-db-up && cd server && cargo test --test schema_history_test
```
Expected: 3 tests PASS.

- [ ] **Step 7: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add server/src/schema_history.rs server/src/lib.rs server/src/admin.rs server/tests/schema_history_test.rs
git commit -m "feat(schema): ENH-013 schema history capture + history endpoints"
```
Expected: `make checkall` green (fmt-check + clippy -D warnings + typecheck + tests).

---

## Task 2: Capture on migrate

**Files:**
- Modify: `server/src/committer.rs` (`handle_migrate`, after line 786 `ctx.schemas.put(...)`)
- Modify: `server/tests/schema_history_test.rs` (add a migrate test)

**Interfaces:**
- Consumes: `schema_history::capture` from Task 1; `CommitterCtx { pool, db }` already in scope in `handle_migrate`; the post-migration `derived: SchemaDef`.
- Produces: migrate writes one `schema_history` row, `source = "migrate"`, `principal = None`.

- [ ] **Step 1: Write the failing test**

Append to `server/tests/schema_history_test.rs`:

```rust
use common::admin_post;

/// Apply a migrate (non-dry-run) and assert it captured a "migrate" row.
#[tokio::test]
async fn migrate_captures_a_version_and_dry_run_does_not() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let db = fresh_db().await;
    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } })).await;

    // Dry-run must NOT capture.
    let dry = admin_post(addr, &format!("/admin/db/{db}/migrate"),
        json!({ "directives": [{ "op": "renameField", "table": "items", "from": "name", "to": "title" }], "dryRun": true })).await;
    assert_eq!(dry.status(), reqwest::StatusCode::OK);
    assert_eq!(history(addr, &db).await.as_array().unwrap().len(), 1, "dry-run captured nothing");

    // Real migrate captures a second row tagged "migrate".
    let real = admin_post(addr, &format!("/admin/db/{db}/migrate"),
        json!({ "directives": [{ "op": "renameField", "table": "items", "from": "name", "to": "title" }], "dryRun": false })).await;
    assert_eq!(real.status(), reqwest::StatusCode::OK);
    let entries = history(addr, &db).await;
    let arr = entries.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["source"], "migrate");
    Ok(())
}
```

- [ ] **Step 2: Run it to verify it fails**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test --test schema_history_test migrate_captures
```
Expected: FAIL — only 1 entry after migrate (the migrate did not capture).

- [ ] **Step 3: Wire capture into `handle_migrate`**

In `server/src/committer.rs` `handle_migrate`, immediately after line 786 (`ctx.schemas.put(&ctx.db, derived.clone()).await;`), add the best-effort capture (before the tap-site block):

```rust
// Schema history capture — best-effort, like the audit/webhook taps below.
// `derived` is the post-migration schema; principal is None (migrate carries no
// interactive principal — matches the audit `owner = None` for migrate).
if let Err(err) =
    crate::schema_history::capture(&ctx.pool, &ctx.db, "migrate", None, &derived).await
{
    tracing::warn!(db = %ctx.db, error = %err, "schema history capture failed");
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cargo test --test schema_history_test migrate_captures
```
Expected: PASS.

- [ ] **Step 5: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add server/src/committer.rs server/tests/schema_history_test.rs
git commit -m "feat(schema): ENH-013 capture schema history on migrate"
```

---

## Task 3: Destructive reconcile + restore committer arm + endpoint

**Files:**
- Modify: `server/src/ddl.rs` (extract `apply_schema_additive` from `push_schema` 234–467; add `reconcile_diff` + `reconcile_schema_destructive`)
- Modify: `server/src/committer.rs` (`CommitterRequest::RunRestoreSchema` variant ~line 24; dispatch arm; `handle_restore_schema`; `Committers::restore_schema` near `migrate` line 264)
- Modify: `server/src/admin.rs` (`POST restore` handler + route)
- Modify: `server/tests/schema_history_test.rs` (restore tests)

**Interfaces:**
- Consumes: `db::load_schema`, `schema_history::{capture, get}`, `SchemaDef`; the existing `pg_schema`/`pg_table`/`pg_col`/`indexed_fields`/`field_type`/`indexed_column_type`/`backfill_expr` helpers in `ddl.rs`; `WriteSet`/`subs.fan_out` from the committer (for post-restore sub re-evaluation).
- Produces:
  - `ddl::reconcile_schema_destructive(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, db: &str, current: &SchemaDef, target: &SchemaDef) -> Result<Vec<String>, RtDbError>` — executes the destructive + additive DDL to make `current`'s shape match `target`; returns the set of touched table names (for `fan_out`).
  - `Committers::restore_schema(&self, db, target_version: i64) -> Result<RestoreResult, RtDbError>` where `RestoreResult { restoredTo: i64 }`.
  - `POST /admin/db/{db}/schema/restore` body `{ version: i64, confirm: String }` → `{ ok: true, restoredTo: version }`.

- [ ] **Step 1: Refactor — extract `apply_schema_additive`**

In `server/src/ddl.rs`, extract the body of `push_schema` from the `for (table_name, new_table) in &schema.tables` loop (line 234) through the end of index creation (before the `meta` upsert at line ~457) into a reusable helper. `push_schema` keeps its own `meta` upsert + commit; the helper does only the additive table/index DDL:

```rust
/// Additive table + index DDL shared by `push_schema` and the destructive
/// reconcile. `previous` is the currently-applied schema (None = fresh); only
/// NEW tables/columns/indexes (in `schema` but not `previous`) are created.
/// Runs inside the caller's transaction. No `meta` upsert — the caller owns that.
async fn apply_schema_additive(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pg_schema_name: &str,
    previous: Option<&SchemaDef>,
    schema: &SchemaDef,
) -> Result<(), RtDbError> {
    // ... the existing for-loop body from push_schema (234..end-of-index-creation),
    // verbatim, parameterized on `previous` and `schema` instead of the locals.
}
```

`push_schema` becomes: validate → detect_destructive → begin tx → CREATE EXTENSION → `apply_schema_additive(&mut tx, &pg_schema_name, previous.as_ref(), &schema)` → meta upsert → commit. Behavior identical (this is a pure refactor — the existing schema tests must still pass).

- [ ] **Step 2: Add `reconcile_diff` + `reconcile_schema_destructive`**

```rust
/// Pure enumeration of the DDL needed to make `current`'s shape match `target`.
/// The inverse of `detect_destructive_changes`: instead of rejecting the first
/// difference, it lists everything to drop and (via apply_schema_additive) add.
pub(crate) struct ReconcileDiff {
    pub drop_tables: Vec<String>,
    /// (table, index_name) — drop these indexes (by their physical ident).
    pub drop_indexes: Vec<(String, String)>,
    /// (table, field_name) — drop these typed index columns (doc jsonb is preserved).
    pub drop_columns: Vec<(String, String)>,
    /// search indexes to drop also need their generated tsvector column removed.
    pub drop_search_cols: Vec<(String, String)>,
}

pub(crate) fn reconcile_diff(current: &SchemaDef, target: &SchemaDef) -> ReconcileDiff {
    use std::collections::HashSet;
    let mut drop_tables = Vec::new();
    let mut drop_indexes = Vec::new();
    let mut drop_columns = Vec::new();
    let mut drop_search_cols = Vec::new();

    for (table_name, cur_table) in &current.tables {
        match target.tables.get(table_name) {
            None => drop_tables.push(table_name.clone()),
            Some(tgt_table) => {
                let cur_indexed: HashSet<&str> = indexed_fields(cur_table).into_iter().collect();
                let tgt_indexed: HashSet<&str> = indexed_fields(tgt_table).into_iter().collect();
                for field in cur_indexed.difference(&tgt_indexed) {
                    drop_columns.push((table_name.clone(), field.to_string()));
                }
                let tgt_index_names: HashSet<&str> =
                    tgt_table.indexes.iter().map(|i| i.name.as_str()).collect();
                for idx in &cur_table.indexes {
                    if !tgt_index_names.contains(idx.name.as_str()) {
                        drop_indexes.push((table_name.clone(), idx.name.clone()));
                        if idx.search {
                            drop_search_cols.push((table_name.clone(), idx.name.clone()));
                        }
                    }
                }
            }
        }
    }
    ReconcileDiff { drop_tables, drop_indexes, drop_columns, drop_search_cols }
}

/// Destructive reconcile: drop tables/columns/indexes in `current` but not
/// `target`, add those in `target` but not `current`, inside the caller's tx.
/// Returns the set of touched table names (for subscription fan-out). Does NOT
/// touch `meta` — the caller upserts the target blob.
pub async fn reconcile_schema_destructive(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    db: &str,
    current: &SchemaDef,
    target: &SchemaDef,
) -> Result<Vec<String>, RtDbError> {
    let pg_schema_name = pg_schema(db);
    let diff = reconcile_diff(current, target);
    let mut touched: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Drop indexes first (they reference columns/tables), then columns, then tables.
    for (table, index_name) in &diff.drop_indexes {
        let table_ident = pg_table(table);
        let index_ident = format!("i_{}_{}", table.to_lowercase(), index_name.to_lowercase());
        sqlx::query(&format!(
            "DROP INDEX IF EXISTS \"{pg_schema_name}\".\"{index_ident}\""
        ))
        .execute(&mut **tx).await?;
        touched.insert(table.clone());
    }
    for (table, index_name) in &diff.drop_search_cols {
        let table_ident = pg_table(table);
        let sv_col = pg_search_col(index_name);
        sqlx::query(&format!(
            "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" DROP COLUMN IF EXISTS \"{sv_col}\""
        ))
        .execute(&mut **tx).await?;
        touched.insert(table.clone());
    }
    for (table, field) in &diff.drop_columns {
        let table_ident = pg_table(table);
        let col = pg_col(field);
        sqlx::query(&format!(
            "ALTER TABLE \"{pg_schema_name}\".\"{table_ident}\" DROP COLUMN IF EXISTS \"{col}\""
        ))
        .execute(&mut **tx).await?;
        touched.insert(table.clone());
    }
    for table in &diff.drop_tables {
        let table_ident = pg_table(table);
        sqlx::query(&format!(
            "DROP TABLE IF EXISTS \"{pg_schema_name}\".\"{table_ident}\""
        ))
        .execute(&mut **tx).await?;
        touched.insert(table.clone());
    }

    // Additive side: anything in target not in (post-drop) current.
    apply_schema_additive(tx, &pg_schema_name, Some(current), target).await?;
    for table_name in target.tables.keys() {
        touched.insert(table_name.clone());
    }
    Ok(touched.into_iter().collect())
}
```

Note: `pg_search_col`, `pg_table`, `pg_col`, `indexed_fields` are existing private helpers in `ddl.rs` — confirm their visibility/names while editing (they are used in `push_schema` lines 234–355). The reconcile lives in `ddl.rs` so it sees them.

- [ ] **Step 3: Add the `RunRestoreSchema` committer arm**

In `server/src/committer.rs`:

(a) Add the variant to `CommitterRequest` (after `RunReaper`, ~line 60):

```rust
/// Restore the database's schema shape to a captured `schema_history` snapshot.
/// Serialized through the committer like `RunMigrate`: the destructive DDL
/// reconcile runs inside the serialized turn, the outgoing schema is captured
/// first (so the restore is itself undoable), and the incoming schema is
/// captured after. `reply` carries the restored version.
RunRestoreSchema {
    target_version: i64,
    reply: oneshot::Sender<Result<i64, RtDbError>>,
},
```

(b) Add `Committers::restore_schema` next to `migrate` (line ~264):

```rust
pub async fn restore_schema(&self, db: &str, target_version: i64) -> Result<i64, RtDbError> {
    let (reply, reply_rx) = oneshot::channel();
    self.submit(db, CommitterRequest::RunRestoreSchema { target_version, reply }).await?;
    reply_rx.await.map_err(|_| RtDbError::internal("committer task dropped the reply"))?
}
```

(c) Add the dispatch arm in `run_committer`'s `match req` (alongside `RunMigrate`):

```rust
CommitterRequest::RunRestoreSchema { target_version, reply } => {
    let outcome = handle_restore_schema(&ctx, target_version).await;
    let _ = reply.send(outcome);
}
```

(d) Implement `handle_restore_schema` (mirror `handle_migrate`'s structure):

```rust
async fn handle_restore_schema(ctx: &CommitterCtx, target_version: i64) -> Result<i64, RtDbError> {
    let current = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?
        .ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let entry = crate::schema_history::get(&ctx.pool, &ctx.db, target_version)
        .await?
        .ok_or_else(|| RtDbError::not_found("schema version not found"))?;
    let target: crate::schema::SchemaDef = serde_json::from_value(entry.schema)
        .map_err(|e| RtDbError::internal(format!("failed to decode snapshot: {e}")))?;
    target.validate()?;

    // Safety net: capture the outgoing schema first so the restore is undoable.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &current).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (outgoing) failed");
    }

    let mut tx = ctx.pool.begin().await?;
    let touched = crate::ddl::reconcile_schema_destructive(&mut tx, &ctx.db, &current, &target).await?;
    // Persist the target blob (same shape as push/migrate tails).
    let schema_json = serde_json::to_value(&target)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    ctx.schemas.put(&ctx.db, target.clone()).await;

    // Capture the incoming (target) state so the latest history row == live schema.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &target).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (incoming) failed");
    }

    // Re-evaluate subscriptions: dropped tables/columns invalidate their subs.
    let write_set = WriteSet { tables: touched, ..Default::default() };
    ctx.subs.fan_out(&ctx.pool, &ctx.db, &target, &write_set).await;

    Ok(target_version)
}
```

- [ ] **Step 4: Add the `POST restore` handler + route**

In `server/src/admin.rs`:

```rust
#[derive(Deserialize)]
struct RestoreSchemaRequest {
    #[serde(rename = "version")]
    version: i64,
    confirm: String,
}

#[derive(Serialize)]
struct RestoreSchemaResponse {
    ok: bool,
    #[serde(rename = "restoredTo")]
    restored_to: i64,
}

async fn restore_schema(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<RestoreSchemaRequest>,
) -> Result<Json<RestoreSchemaResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    // Typed guard: confirm must equal the db name (mirrors delete-db).
    if body.confirm != db {
        return Err(RtDbError::bad_request("confirm must equal the database name"));
    }
    let restored_to = state.realtime.committers.restore_schema(&db, body.version).await?;
    Ok(Json(RestoreSchemaResponse { ok: true, restored_to }))
}
```

Register: `.route("/admin/db/{db}/schema/restore", post(restore_schema))` in `admin_routes()`.

Confirm the path to the committers handle on `AppState` (`state.realtime.committers` is used by `admin_migrate` at line ~685 — match that exactly).

- [ ] **Step 5: Write the restore tests**

Append to `server/tests/schema_history_test.rs`:

```rust
/// Restore reverts schema shape to a prior snapshot and writes two rows
/// (outgoing + incoming). Round-trip back to the outgoing version works.
#[tokio::test]
async fn restore_reverts_shape_and_is_undoable() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let db = fresh_db().await;

    // v1: items with name.
    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } })).await;
    let v1 = history(addr, &db).await.as_array().unwrap()[0]["version"].as_i64().unwrap();
    // v2: add a second table.
    push(addr, &db, json!({ "tables": {
        "items": { "fields": { "name": { "type": "string" } } },
        "orders": { "fields": { "amt": { "type": "number" } } }
    } })).await;

    // Restore to v1 (drops `orders` table). confirm == db name.
    let resp = admin_post(addr, &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": db })).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "restore: {:?}", resp.text().await);

    // Live schema no longer has `orders`.
    let live: serde_json::Value = admin_get(addr, &format!("/admin/dbs/{db}/schema")).await.json().await?;
    assert!(live["tables"]["items"].is_object());
    assert!(live["tables"].get("orders").is_none(), "orders table should be dropped");

    // Restore writes two new rows (outgoing v2-state + incoming v1-state), source "restore".
    let entries = history(addr, &db).await;
    let arr = entries.as_array().unwrap();
    let restores = arr.iter().filter(|e| e["source"] == "restore").count();
    assert_eq!(restores, 2);

    // Guard: wrong confirm is rejected.
    let bad = admin_post(addr, &format!("/admin/db/{db}/schema/restore"),
        json!({ "version": v1, "confirm": "nope" })).await;
    assert_eq!(bad.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

/// Removing an index column preserves the doc jsonb (data is not lost).
#[tokio::test]
async fn restore_dropping_index_column_preserves_doc_data() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let db = fresh_db().await;
    push(addr, &db, json!({ "tables": { "items": {
        "fields": { "name": { "type": "string" } },
        "indexes": [{ "name": "by_name", "fields": ["name"] }]
    } } })).await;
    let v_indexed = history(addr, &db).await.as_array().unwrap()[0]["version"].as_i64().unwrap();

    // Insert a doc, then drop the index via a push that removes the index column
    // shape by restoring to a snapshot without the index. First push the no-index
    // schema to capture a snapshot of it:
    push(addr, &db, json!({ "tables": { "items": { "fields": { "name": { "type": "string" } } } } })).await;
    // (push is additive — it will NOT drop the existing index; so capture the
    // target by restoring to a hand-built snapshot is not possible via push.
    // Instead this test restores to v_indexed's exact shape after mutating docs,
    // verifying round-trip shape fidelity. See note below.)
    let _ = v_indexed;
    Ok(())
}
```

Note: the additive-only `push` cannot create a "fewer-index" snapshot to restore to. The meaningful, reliable assertion here is the round-trip in `restore_reverts_shape_and_is_undoable` (table drop). If the implementer can construct a snapshot with fewer indexes another way (e.g., capture a snapshot, then `migrate` to drop the index, then restore back), add that as a stronger assertion; otherwise keep the table-drop round-trip as the core coverage and drop the weaker test. Do not leave a test that asserts nothing.

- [ ] **Step 6: Run the tests**

```bash
cd /Users/probello/Repos/par-rt-db/server && cargo test --test schema_history_test
```
Expected: all PASS, including the refactor not breaking existing schema tests:

```bash
cargo test --test schema_evolution_test --test migration_test
```

- [ ] **Step 7: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add server/src/ddl.rs server/src/committer.rs server/src/admin.rs server/tests/schema_history_test.rs
git commit -m "feat(schema): ENH-013 in-place destructive schema restore"
```

---

## Task 4: ts-client mirror

**Files:**
- Modify: `ts-client/src/protocol.ts` (wire types, near the migrate types ~line 175)
- Modify: `ts-client/src/admin.ts` (methods, near `getSchema` line 442 / `migrate` line 536)
- Modify: `ts-client/tests/admin.test.ts` (or the existing admin test file — find with `ls ts-client/tests`)

**Interfaces:**
- Consumes: `SchemaDef`/`SchemaJson` already in `protocol.ts`.
- Produces: `SchemaHistoryEntrySummary`, `SchemaHistoryEntry` types; `AdminClient.getSchemaHistory(db, opts?)`, `getSchemaVersion(db, version)`, `restoreSchema(db, version, confirm)`.

- [ ] **Step 1: Add wire types to `protocol.ts`**

```ts
/** Mirrors server `schema_history::HistorySummary` (camelCase). */
export interface SchemaHistoryEntrySummary {
  version: number;
  capturedAt: number;
  source: "push" | "migrate" | "restore";
  principal: string | null;
}

/** Mirrors server `schema_history::HistoryEntry` (camelCase). */
export interface SchemaHistoryEntry extends SchemaHistoryEntrySummary {
  schema: SchemaJson;
}
```

- [ ] **Step 2: Add methods to `admin.ts`**

Mirror `getSchema` (line 442) / `migrate` (line 536) style:

```ts
/** Schema snapshot history, newest-first (GET /admin/db/{db}/schema/history). */
async getSchemaHistory(
  db: string,
  opts: { limit?: number; offset?: number } = {},
): Promise<SchemaHistoryEntrySummary[]> {
  const params = new URLSearchParams();
  if (opts.limit !== undefined) params.set("limit", String(opts.limit));
  if (opts.offset !== undefined) params.set("offset", String(opts.offset));
  const qs = params.toString();
  const body = await this.request(
    "GET",
    `/admin/db/${encodeURIComponent(db)}/schema/history${qs ? `?${qs}` : ""}`,
  );
  return (body as { entries: SchemaHistoryEntrySummary[] }).entries;
}

/** One full schema snapshot (GET /admin/db/{db}/schema/history/{version}). */
async getSchemaVersion(db: string, version: number): Promise<SchemaHistoryEntry> {
  return this.request(
    "GET",
    `/admin/db/${encodeURIComponent(db)}/schema/history/${version}`,
  );
}

/** Restore the live schema shape to a prior snapshot
 *  (POST /admin/db/{db}/schema/restore). `confirm` must equal the db name. */
async restoreSchema(db: string, version: number, confirm: string): Promise<{ ok: boolean; restoredTo: number }> {
  return this.request("POST", `/admin/db/${encodeURIComponent(db)}/schema/restore`, {
    version,
    confirm,
  });
}
```

Import `SchemaHistoryEntrySummary` / `SchemaHistoryEntry` at the top of `admin.ts`.

- [ ] **Step 3: Write a unit test**

In the existing admin test file, add cases that assert the methods hit the right paths with the right bodies (mirror how `getSchema`/`migrate` are tested there — typically against the in-memory harness or a mocked transport). If the file uses a live-server pattern, gate it like the other live tests.

- [ ] **Step 4: Build + test**

```bash
cd /Users/probello/Repos/par-rt-db/ts-client && bun install && bun run build && bunx vitest run tests/admin.test.ts
```
Expected: PASS.

- [ ] **Step 5: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add ts-client/src/protocol.ts ts-client/src/admin.ts ts-client/tests/
git commit -m "feat(client): ENH-013 schema history + restore admin methods (ts-client)"
```

---

## Task 5: dashboard AdminClient methods + types

**Files:**
- Modify: `dashboard/src/lib/admin.tsx` (`AdminClient` methods, near `getSchema` line 139 / `migrate` line 162)
- Modify: `dashboard/src/lib/types.ts` (add types, near `AuditEntry` line 285)

**Interfaces:**
- Consumes: `SchemaJson` from `@par-rt-db/client`.
- Produces: `AdminClient.getSchemaHistory`, `getSchemaVersion`, `restoreSchema`; types `SchemaHistoryEntrySummary`, `SchemaHistoryEntry`.

- [ ] **Step 1: Add types to `types.ts`**

```ts
// Schema change history — mirrors server `schema_history::*` (camelCase wire).
export interface SchemaHistoryEntrySummary {
  version: number;
  capturedAt: number;
  source: "push" | "migrate" | "restore";
  principal: string | null;
}
export interface SchemaHistoryEntry extends SchemaHistoryEntrySummary {
  schema: SchemaJson;
}
```
Import `SchemaJson` from `@par-rt-db/client` at the top (it is already imported elsewhere in the dashboard — confirm and reuse).

- [ ] **Step 2: Add `AdminClient` methods to `admin.tsx`**

Mirror `getSchema`/`migrate` (which use `this.req<T>(path, init)`):

```ts
getSchemaHistory(db: string, opts: { limit?: number; offset?: number } = {}): Promise<SchemaHistoryEntrySummary[]> {
  const params = new URLSearchParams();
  if (opts.limit !== undefined) params.set("limit", String(opts.limit));
  if (opts.offset !== undefined) params.set("offset", String(opts.offset));
  const qs = params.toString();
  return this.req<{ entries: SchemaHistoryEntrySummary[] }>(
    `/admin/db/${enc(db)}/schema/history${qs ? `?${qs}` : ""}`,
  ).then((r) => r.entries);
}
getSchemaVersion(db: string, version: number): Promise<SchemaHistoryEntry> {
  return this.req<SchemaHistoryEntry>(`/admin/db/${enc(db)}/schema/history/${version}`);
}
restoreSchema(db: string, version: number, confirm: string): Promise<{ ok: boolean; restoredTo: number }> {
  return this.req(`/admin/db/${enc(db)}/schema/restore`, {
    method: "POST",
    body: JSON.stringify({ version, confirm }),
  });
}
```
Import `SchemaHistoryEntrySummary` / `SchemaHistoryEntry` from `./types` in the existing type-import block.

- [ ] **Step 3: Verify typecheck, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make ts-client-build && cd dashboard && bunx tsc --noEmit
git add dashboard/src/lib/admin.tsx dashboard/src/lib/types.ts
git commit -m "feat(dashboard): ENH-013 schema history client methods"
```

---

## Task 6: SchemaHistoryPage + route + nav

**Files:**
- Create: `dashboard/src/pages/SchemaHistoryPage.tsx`
- Create: `dashboard/src/pages/SchemaHistoryPage.module.css`
- Modify: `dashboard/src/App.tsx` (route, ~line 50)
- Modify: `dashboard/src/pages/DbPage.tsx` (History link)
- Modify: `dashboard/src/pages/SchemaHistoryPage.test.tsx` (create) — component test

**Interfaces:**
- Consumes: `useAdmin()` → `client` (`AdminClient` from Task 5); `useParams()` `db`; `Placard`/`Button`/`Spinner`/`RtDbRequestError` from `../components/ui` and `../lib/admin`; `formatFieldType` from `../lib/format`; `SchemaJson`/table rendering pattern from `SchemaPage.tsx`.
- Produces: the history UI (list, snapshot view, client-side diff, restore-with-confirm), routed at `/dbs/:db/schema/history`.

- [ ] **Step 1: Write a small client-side diff util (inside the page)**

A pure helper computing tables/fields/indexes added & removed between two `SchemaJson` snapshots (for the diff view):

```ts
function diffSchemas(prev: SchemaJson, next: SchemaJson) {
  const removedTables = Object.keys(prev.tables).filter((t) => !next.tables[t]);
  const addedTables = Object.keys(next.tables).filter((t) => !prev.tables[t]);
  const removedIndexes: { table: string; index: string }[] = [];
  const addedIndexes: { table: string; index: string }[] = [];
  for (const t of Object.keys(prev.tables)) {
    if (!next.tables[t]) continue;
    const pi = new Set(prev.tables[t].indexes?.map((i) => i.name) ?? []);
    const ni = new Set(next.tables[t].indexes?.map((i) => i.name) ?? []);
    pi.forEach((i) => !ni.has(i) && removedIndexes.push({ table: t, index: i }));
    ni.forEach((i) => !pi.has(i) && addedIndexes.push({ table: t, index: i }));
  }
  return { removedTables, addedTables, removedIndexes, addedIndexes };
}
```

- [ ] **Step 2: Write `SchemaHistoryPage.tsx`**

Structure (mirror `SchemaPage.tsx` + `AuditPage.tsx`): fetch `getSchemaHistory(db)` on mount; render a list of version/capturedAt/source/principal; clicking a version fetches `getSchemaVersion` + the current `getSchema` and shows the snapshot tables + the client-side diff; a "Restore to this version" button opens a confirm prompt requiring the db name, then calls `restoreSchema(db, version, db)` and refreshes. Use `RtDbRequestError` for error display (mirror `SchemaPage`'s `preview` error handling). Wire up the CSS module classes as you write the markup.

- [ ] **Step 3: Write the CSS module**

`SchemaHistoryPage.module.css` — mirror the class shape of `SchemaPage.module.css` (page, title, back, list rows, diff sections with added/removed coloring using the dashboard's existing dark palette).

- [ ] **Step 4: Register the route**

In `dashboard/src/App.tsx`, add the import and route next to the schema route (~line 50):

```tsx
import { SchemaHistoryPage } from "./pages/SchemaHistoryPage";
// ...
<Route path="dbs/:db/schema/history" element={<SchemaHistoryPage />} />
```

- [ ] **Step 5: Add a History link in `DbPage.tsx`**

Mirror the existing Schema/Migrate links in `DbPage.tsx` (a `<Link to={`/dbs/${db}/schema/history`}>history</Link>` alongside them).

- [ ] **Step 6: Write a component test**

`SchemaHistoryPage.test.tsx` — mock `useAdmin` to return a fake `client` with `getSchemaHistory`/`getSchemaVersion`/`getSchema`/`restoreSchema` stubs (mirror how `SchemaPage.test.tsx` mocks the admin client). Assert: the list renders versions; clicking a version shows the diff; the restore flow calls `restoreSchema` with `confirm = db`. Assert the confirm guard rejects a wrong name.

- [ ] **Step 7: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add dashboard/src/pages/SchemaHistoryPage.tsx dashboard/src/pages/SchemaHistoryPage.module.css dashboard/src/pages/SchemaHistoryPage.test.tsx dashboard/src/App.tsx dashboard/src/pages/DbPage.tsx
git commit -m "feat(dashboard): ENH-013 schema history page with diff + restore"
```

---

## Task 7: Docs + stale-comment fix

**Files:**
- Modify: `FEATURE_MATRIX.md` (flip/add the schema-history row; note client-mirror status)
- Modify: `server/src/audit.rs` (module doc, line ~1-8)
- Modify: `server/src/webhook.rs` (module doc, line ~1-11)
- Modify: relevant README/docs (e.g. `server/README.md` or `docs/`) — add a short "Schema change history" section if a schema/admin section exists.

- [ ] **Step 1: Fix the stale "two tap sites" doc comments**

Both `audit.rs` and `webhook.rs` module docs say the committer calls the writer "at its two op-feed tap sites (`handle_mutate` and `handle_scheduled`)." The code wires **four** (`handle_mutate`, `handle_scheduled`, `handle_migrate`, `handle_reaper`). Correct each to name all four.

- [ ] **Step 2: Update `FEATURE_MATRIX.md`**

Add/flip the schema-change-history row (✅), noting: server + dashboard + ts-client shipped; rust-client + python-client admin mirrors are follow-ups. Follow the file's existing row format.

- [ ] **Step 3: Add a short docs section**

Where the admin/schema surface is documented, add: snapshots captured on push/migrate/restore, `GET /admin/db/{db}/schema/history[/{version}]`, `POST /admin/db/{db}/schema/restore` (confirm = db name), and the data-loss caveat (restore reconciles shape; only `DROP TABLE` loses data; migrate data-transforms are not rewound).

- [ ] **Step 4: Verify the gate, then commit**

```bash
cd /Users/probello/Repos/par-rt-db && make checkall
git add FEATURE_MATRIX.md server/src/audit.rs server/src/webhook.rs
git commit -m "docs: ENH-013 schema history + fix stale tap-site doc comments"
```

---

## Self-Review (completed)

**Spec coverage:** Storage (per-db table, lazy, always-on, retention) → Task 1. Capture at push → Task 1; at migrate → Task 2; at restore (outgoing+incoming) → Task 3. Restore destructive reconcile + committer arm + endpoint → Task 3. Read endpoints → Task 1. ts-client → Task 4. dashboard client → Task 5; page/route/nav/diff → Task 6. Docs + stale-comment fix → Task 7. All spec sections mapped.

**Placeholder scan:** No "TBD"/"TODO"/"add validation". The two spots that defer to the implementer's reading of live code are explicit and bounded: (a) which crate path `pg_schema` is imported from (a trivial import choice, not undefined logic), (b) the exact `pg_*` helper names in `ddl.rs` (confirmed they exist; the implementer confirms spelling while editing), (c) the stronger "drop index column preserves jsonb" assertion is flagged honestly with a fallback rather than left fake.

**Type consistency:** `capture(pool, db, source, principal, schema)`, `list(...) -> Vec<HistorySummary>`, `get(...) -> Option<HistoryEntry>` are used identically in Tasks 1–3. `HistorySummary`/`HistoryEntry` field names (`version`, `capturedAt`, `source`, `principal`, `schema`) match across server, ts-client (Task 4), and dashboard (Task 5). `restore_schema -> i64` / `restoredTo` consistent across committer, admin handler, and clients.
