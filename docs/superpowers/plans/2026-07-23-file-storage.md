# File storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Postgres-native blob storage (upload / public+authed serve / delete / metadata) to par-rt-db, mirrored across the TS and Rust clients.

**Architecture:** Bytes live in a per-db `bytea` table (`db_<db>.storage`, TOAST-managed); a global `rtdb.storage_index(id → db_name)` resolves the unauthenticated public serve URL. HTTP-only surface (storage is not reactive, so no WS variants). Authed routes carry `{db}` in the path (raw bodies can't carry it; session principals aren't db-scoped) and reuse the existing `bearer → resolve_bearer → authorize` triple; the public serve route is the one unauthenticated route.

**Tech Stack:** Rust (axum 0.8, sqlx 0.8, sha2, tokio), Postgres 17 `bytea`, TypeScript (bun/vitest), reqwest (rust-client). No new dependencies.

## Global Constraints

(Copied from `docs/superpowers/specs/2026-07-23-file-storage-design.md`; every task implicitly includes these.)

- **Postgres-native only.** Bytes in `bytea`; no disk/S3/object-store; no pluggable storage trait.
- **SQL safety.** Double-quote every identifier; bind every value via `$n`; reuse `db::validate_db_name` and `ddl::pg_schema(db)`; physical side-table name is bare `storage` (no `t_` prefix).
- **IDs / time.** `db::new_id()` (uuid v7 `simple`), `db::now_ms()` (epoch ms). Content hash is lowercase-hex sha256 over the raw bytes.
- **Auth.** Authed routes use `bearer_token → resolve_bearer → authorize(pool, principal, db)`; db comes from the `{db}` path segment. `GET /storage/{id}` is unauthenticated. No new auth mechanism.
- **Errors.** Every failure is the `RtDbError { code, message }` envelope (`error.rs`); unknown id → `NotFound`, oversized upload → `BadRequest` (the `RtDbError` envelope, not axum's 413). No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Wire.** HTTP-only. JSON fields are camelCase on the wire (`contentType`, `creationTime`, `sha256`); `contentType` is omitted when null. Binary responses use `Result<Response, RtDbError>` with `Body::from(bytes)` (mirror `admin::export_db`).
- **Body limit.** axum 0.8's default is 2 MiB and there is no `DefaultBodyLimit` today. The upload route disables the default and enforces `RTDB_MAX_FILE_SIZE` (default 50 MiB) via `axum::body::to_bytes(body, limit)`, mapping overflow to `BadRequest`.
- **Gate.** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests). Integration tests need `make dev-db-up`. Tests isolate by uniquely-named databases; never drop a db you didn't create.
- **Model routing.** Implementers = Sonnet, reviewers = Fable (per `model-routing-subagents.md`).

## File Structure

- **Create** `server/src/storage.rs` — per-db `storage` table `ensure_table`, the `rtdb.storage_index` bootstrap, and the accessors `put` / `get` / `get_meta` / `delete` / `resolve_db`, plus the `FileMeta` wire struct.
- **Modify** `server/src/db.rs` — add the `storage` table to `create_database`; add `CREATE SCHEMA rtdb` + `rtdb.storage_index` to `bootstrap_ddl`.
- **Modify** `server/src/committer.rs` — call `storage::ensure_table` at committer startup alongside `mutation_log::ensure_table`.
- **Modify** `server/src/config.rs` — add `max_file_size: usize` (`RTDB_MAX_FILE_SIZE`, default 50 MiB).
- **Modify** `server/src/http_api.rs` — the five route handlers + route registrations + the upload route's body-limit layer.
- **Create** `server/tests/storage_test.rs` — integration tests (mirrors module→binary convention).
- **Modify** `ts-client/src/http.ts` — `upload` / `deleteFile` / `getFileMetadata` / `getUrl`.
- **Modify** `ts-client/src/in_memory.ts` — same surface backed by a `Map`.
- **Modify** `ts-client/src/client.ts` — add the surface to the shared interface; reactive `RtDbClient` delegates to an `RtDbHttpClient`.
- **Create** `ts-client/tests/storage.test.ts`.
- **Modify** `rust-client/src/http.rs` — `upload` / `delete_file` / `get_file_metadata` / `get_url` + wiremock tests.
- **Modify** `FEATURE_MATRIX.md`, `ts-client/README.md`, `rust-client/README.md`, `server/README.md`, `CLAUDE.md`.

---

### Task 1: Per-db `storage` table + global `rtdb.storage_index`

**Files:**
- Create: `server/src/storage.rs`
- Modify: `server/src/db.rs` (`bootstrap_ddl` after the `machine_tokens` block ~line 110; `create_database` after the `scheduled_txns` index ~line 173)
- Modify: `server/src/committer.rs` (~line 219, next to `mutation_log::ensure_table`)
- Test: `server/tests/storage_test.rs`

**Interfaces:**
- Produces: `storage::ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError>`; the `db_<db>.storage` table in every new database; the global `rtdb.storage_index` table after `db::bootstrap`.

- [ ] **Step 1: Write the failing test**

Create `server/tests/storage_test.rs`. Reuse the existing test helpers (`test_state`, `spawn_app`, `fresh_db`, `mint_token`) the way `server/tests/scheduled_test.rs` does — read that file first to copy the imports exactly.

```rust
//! Integration tests for file storage (FEATURE_MATRIX #16).
mod common;
use common::{fresh_db, mint_token, spawn_app, test_state};

#[tokio::test]
async fn storage_table_created_with_database() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    // create_database ran inside fresh_db; the per-db storage table must exist.
    let schema = format!("db_{db}");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage'
        )",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "storage table should exist for a fresh database");

    // And the global index exists after bootstrap (test_state bootstraps).
    let idx: bool = sqlx::query_scalar(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'rtdb' AND table_name = 'storage_index'
        )",
    )
    .fetch_one(&state.pool)
    .await?;
    assert!(idx, "rtdb.storage_index should exist after bootstrap");
    Ok(())
}

#[tokio::test]
async fn ensure_table_is_idempotent_and_revives_dropped_table() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema = format!("db_{db}");

    // Simulate a database that predates the feature: drop storage, then ensure.
    sqlx::query(&format!("DROP TABLE \"{schema}\".storage"))
        .execute(&state.pool)
        .await?;
    par_rt_db::storage::ensure_table(&state.pool, &db).await?;

    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = 'storage')",
    )
    .bind(&schema)
    .fetch_one(&state.pool)
    .await?;
    assert!(exists, "ensure_table should recreate the storage table");
    Ok(())
}
```

If `common::test_state` does not expose `state.pool` publicly, read `server/tests/common/mod.rs` and use whatever accessor the other tests use to run raw SQL (some suites obtain the pool via `test_state().pool`; if it's private, assert through the HTTP surface instead and drop the raw-SQL assertions).

- [ ] **Step 2: Run the test to verify it fails**

```
cd server && cargo test --test storage_test
```
Expected: compile error — `par_rt_db::storage` does not exist.

- [ ] **Step 3: Create `server/src/storage.rs` with `ensure_table`**

```rust
//! Per-database blob storage (file storage, FEATURE_MATRIX #16). Bytes live in
//! Postgres `bytea` (TOAST-managed) in a per-db `storage` table; a global
//! `rtdb.storage_index(id -> db_name)` resolves the unauthenticated public serve
//! URL to the owning database. See
//! docs/superpowers/specs/2026-07-23-file-storage-design.md.

use sqlx::PgPool;

use crate::db::{validate_db_name};
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// `CREATE TABLE IF NOT EXISTS` for the per-db `storage` table, for databases
/// that predate this feature. Mirrors `mutation_log::ensure_table`; called once
/// at committer startup.
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".storage (
            id           text PRIMARY KEY,
            sha256       text NOT NULL,
            size         bigint NOT NULL,
            content_type text,
            bytes        bytea NOT NULL,
            created_at   bigint NOT NULL
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}
```

Add `pub mod storage;` to `server/src/lib.rs` (next to `pub mod scheduler;`).

- [ ] **Step 4: Add the `storage` table to `db::create_database`**

In `server/src/db.rs`, inside `create_database`, immediately after the `scheduled_due_idx` `CREATE INDEX` and before the `INSERT INTO rtdb_auth.databases`:

```rust
    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".storage (
            id           text PRIMARY KEY,
            sha256       text NOT NULL,
            size         bigint NOT NULL,
            content_type text,
            bytes        bytea NOT NULL,
            created_at   bigint NOT NULL
        )"
    ))
    .execute(&mut *tx)
    .await?;
```

- [ ] **Step 5: Add the global `rtdb.storage_index` to `bootstrap_ddl`**

In `server/src/db.rs`, inside `bootstrap_ddl`, after the `rtdb_auth.machine_tokens` block:

```rust
    sqlx::query("CREATE SCHEMA IF NOT EXISTS rtdb")
        .execute(&mut *conn)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb.storage_index (
            id      text PRIMARY KEY,
            db_name text NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;
```

- [ ] **Step 6: Call `ensure_table` at committer startup**

In `server/src/committer.rs`, next to the existing `mutation_log::ensure_table(&pool, &db).await` call (~line 219), add:

```rust
    if let Err(err) = crate::storage::ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "committer: storage::ensure_table failed");
    }
```

(Match the exact error-handling style of the adjacent `mutation_log::ensure_table` call — read it first.)

- [ ] **Step 7: Run the test to verify it passes**

```
make dev-db-up && cd server && cargo test --test storage_test
```
Expected: PASS (2 tests).

- [ ] **Step 8: Commit**

```bash
git add server/src/storage.rs server/src/lib.rs server/src/db.rs server/src/committer.rs server/tests/storage_test.rs
git commit -m "feat(server): per-db storage table + global storage_index (#16)"
```

---

### Task 2: `storage.rs` accessors

**Files:**
- Modify: `server/src/storage.rs`
- Test: `server/tests/storage_test.rs`

**Interfaces:**
- Consumes: `db::new_id`, `db::now_ms` (from Task 1's module).
- Produces:
  - `storage::FileMeta { id, sha256, size, content_type: Option<String>, creation_time }` (serde camelCase, `content_type` skipped when `None`)
  - `storage::put(pool, db, sha256, size, content_type: Option<&str>, bytes: &[u8]) -> Result<String, RtDbError>` (returns new id)
  - `storage::get(pool, db, id) -> Result<Option<(Vec<u8>, Option<String>)>, RtDbError>`
  - `storage::get_meta(pool, db, id) -> Result<Option<FileMeta>, RtDbError>`
  - `storage::delete(pool, db, id) -> Result<bool, RtDbError>` (also removes the index row)
  - `storage::resolve_db(pool, id) -> Result<Option<String>, RtDbError>` (global index lookup)

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/storage_test.rs`:

```rust
use par_rt_db::storage;

#[tokio::test]
async fn put_get_meta_delete_round_trip() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let bytes = b"hello file storage";
    let sha = storage::sha256_hex_bytes(bytes);
    let id = storage::put(&state.pool, &db, &sha, bytes.len() as i64, Some("text/plain"), bytes).await?;

    let fetched = storage::get(&state.pool, &db, &id).await?.expect("row present");
    assert_eq!(fetched.0, bytes);
    assert_eq!(fetched.1.as_deref(), Some("text/plain"));

    let meta = storage::get_meta(&state.pool, &db, &id).await?.expect("meta present");
    assert_eq!(meta.id, id);
    assert_eq!(meta.sha256, sha);
    assert_eq!(meta.size, bytes.len() as i64);
    assert_eq!(meta.content_type.as_deref(), Some("text/plain"));

    assert!(storage::delete(&state.pool, &db, &id).await?);
    assert!(storage::get(&state.pool, &db, &id).await?.is_none());
    assert_eq!(storage::resolve_db(&state.pool, &id).await?, None, "index row removed on delete");
    Ok(())
}

#[tokio::test]
async fn resolve_db_maps_id_to_owner() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let id = storage::put(&state.pool, &db, "deadbeef", 1, None, b"x").await?;
    assert_eq!(storage::resolve_db(&state.pool, &id).await?, Some(db));
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
cd server && cargo test --test storage_test
```
Expected: compile error — `put`/`get`/`sha256_hex_bytes` etc. not defined.

- [ ] **Step 3: Implement the accessors**

Append to `server/src/storage.rs`:

```rust
use crate::db::{new_id, now_ms};
use sha2::{Digest, Sha256};

/// Lowercase-hex sha256 over raw bytes (for content hashing on upload).
pub fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Wire metadata for a stored file. `contentType` is omitted on the wire when
/// `None` (the upload supplied no Content-Type header).
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMeta {
    pub id: String,
    pub sha256: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub creation_time: i64,
}

/// Inserts a blob + metadata and records the global index row. Returns the id.
pub async fn put(
    pool: &PgPool,
    db: &str,
    sha256: &str,
    size: i64,
    content_type: Option<&str>,
    bytes: &[u8],
) -> Result<String, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let id = new_id();
    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".storage (id, sha256, size, content_type, bytes, created_at)
         VALUES ($1, $2, $3, $4, $5, $6)"
    ))
    .bind(&id)
    .bind(sha256)
    .bind(size)
    .bind(content_type)
    .bind(bytes)
    .bind(now_ms())
    .execute(pool)
    .await?;
    sqlx::query("INSERT INTO rtdb.storage_index (id, db_name) VALUES ($1, $2)")
        .bind(&id)
        .bind(db)
        .execute(pool)
        .await?;
    Ok(id)
}

/// Reads a blob and its content type for serving. `None` if the id is absent.
pub async fn get(
    pool: &PgPool,
    db: &str,
    id: &str,
) -> Result<Option<(Vec<u8>, Option<String>)>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(Vec<u8>, Option<String>)> = sqlx::query_as(&format!(
        "SELECT bytes, content_type FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row)
}

/// Reads just the metadata. `None` if absent.
pub async fn get_meta(pool: &PgPool, db: &str, id: &str) -> Result<Option<FileMeta>, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let row: Option<(String, String, i64, Option<String>, i64)> = sqlx::query_as(&format!(
        "SELECT id, sha256, size, content_type, created_at
         FROM \"{schema}\".storage WHERE id = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|(id, sha256, size, content_type, creation_time)| FileMeta {
        id,
        sha256,
        size,
        content_type,
        creation_time,
    }))
}

/// Deletes a blob and its index row. Returns true if a blob row was removed.
pub async fn delete(pool: &PgPool, db: &str, id: &str) -> Result<bool, RtDbError> {
    validate_db_name(db)?;
    let schema = pg_schema(db);
    let res = sqlx::query(&format!("DELETE FROM \"{schema}\".storage WHERE id = $1"))
        .bind(id)
        .execute(pool)
        .await?;
    if res.rows_affected() > 0 {
        sqlx::query("DELETE FROM rtdb.storage_index WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Resolves an opaque public id to its owning database. `None` if unknown.
pub async fn resolve_db(pool: &PgPool, id: &str) -> Result<Option<String>, RtDbError> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT db_name FROM rtdb.storage_index WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(db,)| db))
}
```

`hex` and `sha2` are already in `server/Cargo.toml`; add `sha2::{Digest, Sha256}` to the top-of-file `use` (merge with the existing module `use` block).

- [ ] **Step 4: Run the tests to verify they pass**

```
cd server && cargo test --test storage_test
```
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add server/src/storage.rs server/tests/storage_test.rs
git commit -m "feat(server): storage accessors put/get/get_meta/delete/resolve_db"
```

---

### Task 3: Upload route + `RTDB_MAX_FILE_SIZE` config + body-limit layer

**Files:**
- Modify: `server/src/config.rs`
- Modify: `server/src/http_api.rs` (routes fn ~line 227; new `upload_handler`; imports)
- Test: `server/tests/storage_test.rs`

**Interfaces:**
- Consumes: `storage::put`, `storage::sha256_hex_bytes`, `storage::ensure_table`, `auth::{resolve_bearer, authorize}`, `bearer_token`, `state.config.max_file_size`.
- Produces: `POST /api/storage/{db}` → `UploadResponse { id, sha256, size, contentType }` (camelCase).

- [ ] **Step 1: Write the failing test**

Append to `server/tests/storage_test.rs`. Upload via the live HTTP endpoint (read `server/tests/ws_test.rs` or `http_api_test.rs` to copy the exact `spawn_app` + `mint_token` + http-POST helper; the test below assumes a `reqwest`-based POST is available — if the suite uses a different helper, adapt to it):

```rust
use axum::http::StatusCode;

#[tokio::test]
async fn upload_returns_id_sha_size_and_content_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let bytes = b"upload payload body";
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "text/plain")
        .body(bytes.to_vec())
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert!(body["id"].is_string());
    assert_eq!(body["size"], json!(bytes.len() as i64));
    assert_eq!(body["contentType"], json!("text/plain"));
    // sha256 of the body, lowercase hex
    let mut h = sha2::Sha256::new();
    sha2::Digest::update(&mut h, bytes);
    assert_eq!(body["sha256"], json!(hex::encode(sha2::Digest::finalize(h))));
    Ok(())
}

#[tokio::test]
async fn upload_rejects_oversized_body() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let too_big = vec![0u8; state.config.max_file_size + 1];
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .body(too_big)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
make dev-db-up && cd server && cargo test --test storage_test
```
Expected: 404 / no such route (uploads not wired yet).

- [ ] **Step 3: Add `max_file_size` to `Config`**

In `server/src/config.rs`, add the field to the struct (with the others):

```rust
    pub max_file_size: usize,       // RTDB_MAX_FILE_SIZE, default 50 MiB
```

Parse it in `from_env` (next to `session_ttl_days`):

```rust
        let max_file_size = match std::env::var("RTDB_MAX_FILE_SIZE") {
            Ok(v) => v
                .parse::<usize>()
                .map_err(|_| "RTDB_MAX_FILE_SIZE must be a valid usize".to_string())?,
            Err(_) => 50 * 1024 * 1024,
        };
```

And add `max_file_size,` to the `Ok(Self { … })` block.

- [ ] **Step 4: Implement `upload_handler` and register the route**

In `server/src/http_api.rs`, add the imports:

```rust
use axum::body::Body;
use axum::extract::Path;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use crate::storage;
```

Add the response struct and handler:

```rust
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct UploadResponse {
    id: String,
    sha256: String,
    size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<String>,
}

async fn upload_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    request: axum::extract::Request,
) -> Result<Json<UploadResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    storage::ensure_table(&state.pool, &db).await?; // revive storage for old dbs

    let limit = state.config.max_file_size;
    let bytes = axum::body::to_bytes(request.into_body(), limit)
        .await
        .map_err(|_| RtDbError::bad_request("upload exceeds max file size"))?;

    let size = bytes.len() as i64;
    let sha256 = storage::sha256_hex_bytes(&bytes);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let id = storage::put(
        &state.pool,
        &db,
        &sha256,
        size,
        content_type.as_deref(),
        &bytes,
    )
    .await?;
    Ok(Json(UploadResponse {
        id,
        sha256,
        size,
        content_type,
    }))
}
```

Register the route in `http_api_routes()`. The upload route must **disable** axum's
default 2 MiB limit so `to_bytes` (with our `RTDB_MAX_FILE_SIZE` limit) is the
sole enforcer — apply `DefaultBodyLimit::disable()` to that one route's method
router (this keeps every other `/api/*` JSON route under the 2 MiB default):

```rust
use axum::extract::DefaultBodyLimit;
// ...
pub fn http_api_routes() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/query", post(query_handler))
        .route("/api/mutate", post(mutate_handler))
        // ... existing schedule routes ...
        .route(
            "/api/storage/{db}",
            post(upload_handler).layer(DefaultBodyLimit::disable()),
        )
        // ... remaining storage routes added in later tasks ...
}
```

Confirm via `cargo check` that this route no longer rejects at 2 MiB.

- [ ] **Step 5: Run the tests to verify they pass**

```
cd server && cargo test --test storage_test
```
Expected: PASS (now 6 tests).

- [ ] **Step 6: Commit**

```bash
git add server/src/config.rs server/src/http_api.rs server/tests/storage_test.rs
git commit -m "feat(server): POST /api/storage/{db} upload with size limit (#16)"
```

---

### Task 4: Serve routes — public `GET /storage/{id}` + authed `GET /api/storage/{db}/{id}`

**Files:**
- Modify: `server/src/http_api.rs`
- Test: `server/tests/storage_test.rs`

**Interfaces:**
- Consumes: `storage::get`, `storage::resolve_db`, the auth triple.
- Produces: `GET /storage/{id}` (unauthenticated, bytes) and `GET /api/storage/{db}/{id}` (bearer, bytes).

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/storage_test.rs`:

```rust
#[tokio::test]
async fn public_and_authed_serve_return_bytes() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let payload = b"serve me";
    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .header("content-type", "image/png")
        .body(payload.to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();

    // Public serve — no bearer.
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.status(), StatusCode::OK);
    assert_eq!(public.headers().get("content-type").unwrap(), "image/png");
    assert_eq!(public.bytes().await?, payload);

    // Authed serve — bearer + db in path.
    let authed = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(authed.status(), StatusCode::OK);
    assert_eq!(authed.bytes().await?, payload);
    Ok(())
}

#[tokio::test]
async fn content_type_defaults_to_octet_stream() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(&token)
        .body(b"no content type".to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();
    let public = reqwest::get(format!("http://{addr}/storage/{id}")).await?;
    assert_eq!(public.headers().get("content-type").unwrap(), "application/octet-stream");
    Ok(())
}

#[tokio::test]
async fn cross_db_authed_serve_is_404() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db_a = fresh_db(&state).await;
    let db_b = fresh_db(&state).await;
    let tok_a = mint_token(addr, &db_a).await;

    let up = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db_a}"))
        .bearer_auth(&tok_a)
        .body(b"a's file".to_vec())
        .send()
        .await?;
    let id = up.json::<serde_json::Value>().await?["id"].as_str().unwrap().to_string();

    // Public serve still resolves (id is global); authed serve from db_b 404s.
    assert_eq!(reqwest::get(format!("http://{addr}/storage/{id}")).await?.status(), StatusCode::OK);
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db_b}/{id}"))
        .bearer_auth(mint_token(addr, &db_b).await)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    Ok(())
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
cd server && cargo test --test storage_test
```
Expected: 404 — serve routes not registered.

- [ ] **Step 3: Implement the serve handlers**

In `server/src/http_api.rs`:

```rust
/// Public, unauthenticated serve: anyone with the URL fetches the bytes. The
/// opaque id resolves to its owning db via the global index.
async fn serve_public_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Response, RtDbError> {
    let db = storage::resolve_db(&state.pool, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    serve_bytes(&state, &db, &id).await
}

/// Authed serve: the caller's principal must be authorized for `{db}`; the id
/// must live in that db's table (404 otherwise — cross-db isolation).
async fn serve_authed_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Response, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    serve_bytes(&state, &db, &id).await
}

async fn serve_bytes(state: &Arc<AppState>, db: &str, id: &str) -> Result<Response, RtDbError> {
    let (bytes, content_type) = storage::get(&state.pool, db, id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    let ct = content_type
        .unwrap_or_else(|| "application/octet-stream".to_string());
    Response::builder()
        .header(header::CONTENT_TYPE, ct)
        .body(Body::from(bytes))
        .map_err(|err| RtDbError::internal(format!("failed to build serve response: {err}")))
}
```

Register both routes in `http_api_routes()`:

```rust
        .route("/api/storage/{db}/{id}", get(serve_authed_handler))
        // public serve is unauthenticated — register on the same router:
        .route("/storage/{id}", get(serve_public_handler))
```

- [ ] **Step 4: Run the tests to verify they pass**

```
cd server && cargo test --test storage_test
```
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add server/src/http_api.rs server/tests/storage_test.rs
git commit -m "feat(server): public + authed file serve routes (#16)"
```

---

### Task 5: Delete + metadata routes

**Files:**
- Modify: `server/src/http_api.rs`
- Test: `server/tests/storage_test.rs`

**Interfaces:**
- Consumes: `storage::delete`, `storage::get_meta`.
- Produces: `DELETE /api/storage/{db}/{id}` → `{ ok: true }`; `GET /api/storage/{db}/{id}/metadata` → `FileMeta`.

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/storage_test.rs`:

```rust
#[tokio::test]
async fn delete_revokes_public_url() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let id = upload(&addr, &db, &token, b"to delete").await;
    assert_eq!(reqwest::get(format!("http://{addr}/storage/{id}")).await?.status(), StatusCode::OK);

    let del = reqwest::Client::new()
        .delete(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(del.status(), StatusCode::OK);
    assert_eq!(del.json::<serde_json::Value>().await?["ok"], json!(true));

    // Public URL now 404s — the index row is gone.
    assert_eq!(reqwest::get(format!("http://{addr}/storage/{id}")).await?.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn metadata_returns_fields_and_omits_null_content_type() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    // Upload with no content-type.
    let id = upload_no_ct(&addr, &db, &token, b"meta").await;
    let body: serde_json::Value = reqwest::Client::new()
        .get(format!("http://{addr}/api/storage/{db}/{id}/metadata"))
        .bearer_auth(&token)
        .send()
        .await?
        .json()
        .await?;
    assert_eq!(body["id"], json!(id));
    assert_eq!(body["size"], json!(4));
    assert!(body["sha256"].is_string());
    assert_eq!(body.get("contentType"), None, "contentType omitted when null");
    assert!(body["creationTime"].is_i64());
    Ok(())
}

#[tokio::test]
async fn revoked_token_cannot_delete() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;
    let id = upload(&addr, &db, &token, b"x").await;

    // Revoke via the admin surface, then retry the delete.
    revoke_token(&addr, &state, &db, &token).await;
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/api/storage/{db}/{id}"))
        .bearer_auth(&token)
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    Ok(())
}
```

Add the small `upload` / `upload_no_ct` / `revoke_token` helpers at the top of the test file (POST raw bytes; revoke via `POST /admin/revoke-token` — read `server/tests/admin_test.rs` to copy the exact admin-revoke helper signature and admin-key accessor).

- [ ] **Step 2: Run the tests to verify they fail**

```
cd server && cargo test --test storage_test
```
Expected: 404 — delete/metadata routes not registered.

- [ ] **Step 3: Implement the delete + metadata handlers**

In `server/src/http_api.rs`:

```rust
#[derive(serde::Serialize)]
struct OkResponse {
    ok: bool,
}

async fn delete_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<OkResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    storage::delete(&state.pool, &db, &id).await?;
    Ok(Json(OkResponse { ok: true }))
}

async fn metadata_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((db, id)): Path<(String, String)>,
) -> Result<Json<storage::FileMeta>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &db).await?;
    let meta = storage::get_meta(&state.pool, &db, &id)
        .await?
        .ok_or_else(|| RtDbError::not_found("unknown file"))?;
    Ok(Json(meta))
}
```

Register in `http_api_routes()`:

```rust
        .route("/api/storage/{db}/{id}/metadata", get(metadata_handler))
        .route("/api/storage/{db}/{id}", delete(delete_handler))
```

- [ ] **Step 4: Run the full server suite to verify it passes**

```
cd server && cargo test
```
Expected: PASS — `storage_test` now 12 tests, all other suites green.

- [ ] **Step 5: Commit**

```bash
git add server/src/http_api.rs server/tests/storage_test.rs
git commit -m "feat(server): delete + metadata storage routes (#16)"
```

---

### Task 6: TS client storage surface

**Files:**
- Modify: `ts-client/src/http.ts`
- Modify: `ts-client/src/in_memory.ts`
- Modify: `ts-client/src/client.ts` (shared interface + reactive delegation)
- Test: `ts-client/tests/storage.test.ts`

**Interfaces:**
- Consumes: the server routes from Tasks 3–5.
- Produces: `upload(bytes, contentType?)`, `deleteFile(id)`, `getFileMetadata(id)`, `getUrl(id)` on `RtDbHttpClient`, `InMemoryRtDbClient`, and the reactive `RtDbClient` interface.

- [ ] **Step 1: Write the failing test**

Create `ts-client/tests/storage.test.ts`:

```ts
import { describe, it, expect } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory.js";

describe("in-memory storage", () => {
  it("uploads, serves-via-url-shape, deletes, and reports metadata", async () => {
    const c = new InMemoryRtDbClient();
    const bytes = new Uint8Array([1, 2, 3, 4]);
    const up = await c.upload(bytes, "image/png");
    expect(up.id).toBeTypeOf("string");
    expect(up.size).toBe(4);
    expect(up.contentType).toBe("image/png");
    expect(up.sha256).toBeTypeOf("string");

    expect(c.getUrl(up.id)).toBe(`memory://${up.id}`);

    const meta = await c.getFileMetadata(up.id);
    expect(meta.size).toBe(4);
    expect(meta.contentType).toBe("image/png");

    await c.deleteFile(up.id);
    await expect(c.getFileMetadata(up.id)).rejects.toBeTruthy();
  });

  it("getUrl against the http client builds the public URL", async () => {
    // Wiremock-free shape check: the http client constructs the public URL
    // without a fetch. Full HTTP round trip is covered by the live-server E2E.
    const { RtDbHttpClient } = await import("../src/http.js");
    const http = new RtDbHttpClient({ url: "https://rtdb.example.com/", db: "kanban", token: "t" });
    expect(http.getUrl("abc")).toBe("https://rtdb.example.com/storage/abc");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```
cd ts-client && bunx vitest run tests/storage.test.ts
```
Expected: FAIL — `upload`/`getUrl`/`getFileMetadata`/`deleteFile` not defined.

- [ ] **Step 3: Add the surface to `RtDbHttpClient`**

In `ts-client/src/http.ts`, export a result type and add the methods:

```ts
export interface UploadResult {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
}
export interface FileMetadata {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
  creationTime: number;
}
```

```ts
  /** Upload raw bytes; the db is this client's db (injected into the path). */
  async upload(bytes: Uint8Array, contentType?: string): Promise<UploadResult> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.token}` };
    if (contentType) headers["content-type"] = contentType;
    const response = await this.fetchImpl(
      `${this.url}/api/storage/${encodeURIComponent(this.db)}`,
      { method: "POST", headers, body: bytes },
    );
    return (await this.parse(response)) as UploadResult;
  }

  async deleteFile(id: string): Promise<void> {
    await this.fetchImpl(
      `${this.url}/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}`,
      { method: "DELETE", headers: { Authorization: `Bearer ${this.token}` } },
    ).then((r) => this.requireOk(r));
  }

  async getFileMetadata(id: string): Promise<FileMetadata> {
    const body = await this.get(
      `/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}/metadata`,
      this.token,
    );
    return body as FileMetadata;
  }

  /** The public serve URL for `id` — no fetch, the browser consumes it. */
  getUrl(id: string): string {
    return `${this.url}/storage/${encodeURIComponent(id)}`;
  }
```

Factor the shared response-parsing out of `post` into two private helpers so upload/delete reuse them — add:

```ts
  private async requireOk(response: Response): Promise<void> {
    if (response.ok) return;
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) throw RtDbError.fromEnvelope(parsed);
    throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
  }

  private async parse(response: Response): Promise<unknown> {
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) throw RtDbError.fromEnvelope(parsed);
      throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
    }
    return parsed;
  }
```

Then refactor `post` and `get` to call `parse`/`requireOk` (delete their duplicated bodies) — keeps the file DRY.

- [ ] **Step 4: Add the surface to `InMemoryRtDbClient`**

In `ts-client/src/in_memory.ts`, add a `private files = new Map<string, { bytes: Uint8Array; contentType?: string; createdAt: number }>();` field and:

```ts
  async upload(bytes: Uint8Array, contentType?: string): Promise<UploadResult> {
    const id = `f${(++this.idCounter).toString(36)}`;
    // sha256 via the Web Crypto subtle digest (available in bun/node).
    const digest = await crypto.subtle.digest("SHA-256", bytes);
    const sha256 = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
    this.files.set(id, { bytes, contentType, createdAt: Date.now() });
    return { id, sha256, size: bytes.length, contentType };
  }

  async deleteFile(id: string): Promise<void> {
    if (!this.files.delete(id)) throw new RtDbError("NOT_FOUND", "unknown file");
  }

  async getFileMetadata(id: string): Promise<FileMetadata> {
    const f = this.files.get(id);
    if (!f) throw new RtDbError("NOT_FOUND", "unknown file");
    return {
      id,
      sha256: "", // not tracked in-memory; only the http client computes it
      size: f.bytes.length,
      contentType: f.contentType,
      creationTime: f.createdAt,
    };
  }

  getUrl(id: string): string {
    return `memory://${id}`;
  }
```

Add `private idCounter = 0;` if no counter exists, and import `UploadResult`, `FileMetadata` from `./http.js`, plus `RtDbError` from `./errors.js`.

- [ ] **Step 5: Add the surface to the reactive `RtDbClient` interface + delegation**

In `ts-client/src/client.ts`, add the four methods to the public `RtDbClient` (and its interface if separate). The reactive client delegates to an `RtDbHttpClient` built from its connection params — read the existing constructor to find the `url`/`db`/`token`/`fetch` fields and construct the http client lazily:

```ts
  private httpForStorage(): RtDbHttpClient {
    // Construct (and cache) an http client from this reactive client's params.
    // Read the existing constructor to use the real field names.
    return this.storageHttp ??= new RtDbHttpClient({ url: this.url, db: this.db, token: this.token });
  }
  private storageHttp?: RtDbHttpClient;

  upload(bytes: Uint8Array, contentType?: string) { return this.httpForStorage().upload(bytes, contentType); }
  deleteFile(id: string) { return this.httpForStorage().deleteFile(id); }
  getFileMetadata(id: string) { return this.httpForStorage().getFileMetadata(id); }
  getUrl(id: string) { return this.httpForStorage().getUrl(id); }
```

(If the reactive client already holds an `RtDbHttpClient`, reuse it instead of constructing one. Match the file's real fields.)

- [ ] **Step 6: Run the tests to verify they pass**

```
cd ts-client && bunx vitest run tests/storage.test.ts
```
Expected: PASS (2 tests). Then run the whole client suite to confirm no regressions: `bunx vitest run`.

- [ ] **Step 7: Commit**

```bash
git add ts-client/src/http.ts ts-client/src/in_memory.ts ts-client/src/client.ts ts-client/tests/storage.test.ts
git commit -m "feat(ts-client): upload/deleteFile/getFileMetadata/getUrl storage surface (#16)"
```

---

### Task 7: Rust client storage surface

**Files:**
- Modify: `rust-client/src/http.rs` (methods + wiremock tests in the existing `mod tests`)
- Test: inline `#[tokio::test]`s in `rust-client/src/http.rs`

**Interfaces:**
- Consumes: the server routes from Tasks 3–5.
- Produces: `RtDbHttpClient::upload`, `delete_file`, `get_file_metadata`, `get_url`.

- [ ] **Step 1: Write the failing tests**

In `rust-client/src/http.rs` `mod tests`, add (the existing `setup()` wiremock helper gives `(server, client)`):

```rust
    #[tokio::test]
    async fn upload_posts_raw_bytes_and_returns_metadata() {
        let (server, client) = setup().await;
        Mock::given(method("POST"))
            .and(path("/api/storage/t<uuid>"))
            .and(header("authorization", "Bearer machine-token"))
            .and(header("content-type", "image/png"))
            .and(body_bytes!("raw-bytes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "f1", "sha256": "abc", "size": 9, "contentType": "image/png"
            })))
            .mount(&server)
            .await;
        let up = client
            .upload(b"raw-bytes", Some("image/png"))
            .await
            .unwrap();
        assert_eq!(up.id, "f1");
        assert_eq!(up.size, 9);
        assert_eq!(up.content_type.as_deref(), Some("image/png"));
    }

    #[tokio::test]
    async fn delete_file_and_metadata_and_get_url() {
        let (server, client) = setup().await;
        Mock::given(method("DELETE"))
            .and(path("/api/storage/t<uuid>/f1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/storage/t<uuid>/f1/metadata"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "f1", "sha256": "abc", "size": 9, "creationTime": 5
            })))
            .mount(&server)
            .await;
        client.delete_file("f1").await.unwrap();
        let meta = client.get_file_metadata("f1").await.unwrap();
        assert_eq!(meta.size, 9);
        assert_eq!(client.get_url("f1"), format!("{}/storage/f1", /* client.url */ "stub"));
    }
```

(For the `get_url` assertion, expose the url or assert against the wiremock server's uri; match how other tests reference the base URL. `body_bytes!` and `header`/`path`/`method` come from the existing `wiremock::matchers` import — confirm the exact matcher name for raw bytes by reading the file's current imports.)

- [ ] **Step 2: Run the tests to verify they fail**

```
cd rust-client && cargo test --features http upload_
```
Expected: compile error — `upload`/`delete_file`/`get_file_metadata`/`get_url` not defined.

- [ ] **Step 3: Implement the methods**

In `rust-client/src/http.rs`, add the result structs and methods (next to `schedule`/`cancel_schedule`):

```rust
    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct UploadResult {
        pub id: String,
        pub sha256: String,
        pub size: i64,
        #[serde(default)]
        pub content_type: Option<String>,
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct FileMetadata {
        pub id: String,
        pub sha256: String,
        pub size: i64,
        #[serde(default)]
        pub content_type: Option<String>,
        pub creation_time: i64,
    }

    /// Upload raw bytes; `content_type` sets the Content-Type header and is
    /// stored as the file's type. Returns the server-computed metadata.
    pub async fn upload(
        &self,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<UploadResult, RtDbError> {
        let mut req = self
            .client
            .post(format!("{}/api/storage/{}", self.url, self.db))
            .bearer_auth(&self.token)
            .body(bytes.to_vec());
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("upload request failed: {e}")))?;
        self.deserialize::<UploadResult>(resp).await
    }

    pub async fn delete_file(&self, id: &str) -> Result<(), RtDbError> {
        let resp = self
            .client
            .delete(format!("{}/api/storage/{}/{id}", self.url, self.db))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("delete file request failed: {e}")))?;
        #[derive(serde::Deserialize)]
        struct OkResp {
            ok: bool,
        }
        let parsed = self.deserialize::<OkResp>(resp).await?;
        if !parsed.ok {
            return Err(RtDbError::internal("delete file returned ok=false"));
        }
        Ok(())
    }

    pub async fn get_file_metadata(&self, id: &str) -> Result<FileMetadata, RtDbError> {
        let resp = self
            .client
            .get(format!("{}/api/storage/{}/{id}/metadata", self.url, self.db))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| RtDbError::internal(format!("file metadata request failed: {e}")))?;
        self.deserialize::<FileMetadata>(resp).await
    }

    /// The public serve URL — no request is made.
    pub fn get_url(&self, id: &str) -> String {
        format!("{}/storage/{id}", self.url)
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

```
cd rust-client && cargo test --features http
```
Expected: PASS (new tests + all existing).

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/http.rs
git commit -m "feat(rust-client): upload/delete_file/get_file_metadata/get_url (#16)"
```

---

### Task 8: Docs sync — FEATURE_MATRIX, READMEs, CLAUDE.md

**Files:**
- Modify: `FEATURE_MATRIX.md` (row #16)
- Modify: `server/README.md`, `ts-client/README.md`, `rust-client/README.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Flip FEATURE_MATRIX row #16 ❌ → ✅**

In `FEATURE_MATRIX.md`, update row #16's par-rt-db cell from ❌ to ✅ and rewrite the Implementation sketch to describe what shipped (per-db `bytea` `storage` table + global `storage_index`; `POST /api/storage/{db}` upload with `RTDB_MAX_FILE_SIZE`; public `GET /storage/{id}` + authed `GET /api/storage/{db}/{id}`; delete + metadata; HTTP-only; mirrored on ts-client + rust-client). Update the "Recommended order" section: move #16 from "Remaining gaps" to "done", leaving #17/#18/#20 as remaining.

- [ ] **Step 2: Document the storage surface in the READMEs**

- `server/README.md`: add a "File storage" section with the five routes and `RTDB_MAX_FILE_SIZE`.
- `ts-client/README.md`: add `upload` / `deleteFile` / `getFileMetadata` / `getUrl` to the API list.
- `rust-client/README.md`: add `upload` / `delete_file` / `get_file_metadata` / `get_url`.

- [ ] **Step 3: Update `CLAUDE.md`**

Add file storage to the "Architecture — what spans files" list (the `storage.rs` module + the public serve route being the one unauthenticated route), and note the `{db}`-in-path convention for authed storage routes.

- [ ] **Step 4: Run the full gate**

```
make checkall
```
Expected: green (fmt-check + clippy `-D warnings` + typecheck + all tests across server, ts-client, rust-client).

- [ ] **Step 5: Commit**

```bash
git add FEATURE_MATRIX.md server/README.md ts-client/README.md rust-client/README.md CLAUDE.md
git commit -m "docs: file storage (#16) — FEATURE_MATRIX, READMEs, CLAUDE.md"
```

---

## Definition of done

- `make checkall` green.
- All five storage routes live and covered by `server/tests/storage_test.rs`.
- ts-client and rust-client ship the storage surface with tests.
- `FEATURE_MATRIX.md` #16 flipped to ✅; READMEs and `CLAUDE.md` updated.
- Kanban item `019f8a8ae11a7c60994c92f51a0a6d37` marked done.
