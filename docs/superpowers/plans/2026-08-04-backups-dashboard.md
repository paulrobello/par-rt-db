# Backups Dashboard (ENH-002) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the operator console the full backup lifecycle — list, manual async trigger, download, delete, and restore-to-new-DB — across the server and all four client surfaces.

**Architecture:** Server adds four admin endpoints over the existing `backup.rs` (`perform_backup`, `list_backups`) plus a new `restore_to_new_db` that shells `createdb` + `pg_restore` into a fresh `rtdb_restored_<stamp>` Postgres DB, never touching the live `rtdb` DB (single-writer invariant preserved). The dashboard renders a `BackupsPage`; `ts-client`, `rust-client`, `python-client` each gain typed admin methods for external automation.

**Tech Stack:** Rust (axum 0.8 / tokio / sqlx / tokio-util), TypeScript (React + Vite for the dashboard; the `@par-rt-db/client` SDK), Rust client crate, Python client (`httpx`).

**Spec:** `docs/superpowers/specs/2026-08-04-backups-dashboard-design.md`

## Global Constraints

- Dump filename grammar is exactly `rtdb-<YYYYmmddTHHMMSSZ>.dump` (the shape `format_timestamp_utc` produces). Every route that takes a client-supplied dump name MUST pass it through `backup::validate_dump_name` first — path-traversal guard.
- Restore target DB name is `rtdb_restored_<stamp>`. Restore NEVER writes to the live `rtdb` DB; it writes only to the fresh target.
- Connection credentials go via `PG*` env vars on `pg_dump`/`pg_restore`/`createdb`/`dropdb` — never argv (so `ps`/`/proc` don't leak the password). Reuse `backup::parse_pg_env`.
- Client-facing trigger/restore/download errors are generic (`RtDbError`); tool stderr is logged via `tracing`, never stringified into a response body.
- Wire contract must stay byte-identical across `server/src/admin.rs` responses and the four client surfaces: `{ running: boolean, backups: BackupFile[] }` where `BackupFile = { name, sizeBytes, createdMs }`; restore returns `{ target, instructions }`.
- The DB role needs `CREATEDB` (new requirement — the server only ever ran `CREATE SCHEMA`/`CREATE EXTENSION` before).
- Definition of done for the whole plan: `make checkall` green (fmt-check + clippy `-D warnings` + typecheck + tests) AND the `#[ignore]` restore integration test passes manually against the dev Postgres.

---

## File Structure

**Server (`server/src/`):**
- `backup.rs` — add `validate_dump_name`, `restore_target_name`, `restore_to_new_db`, `apply_pg_env`, `drop_restore_target`; lift `perform_backup` to `pub(crate)`. Add unit + `#[ignore]` tests.
- `admin.rs` — add `create_backup`, `download_backup`, `delete_backup`, `restore_backup` handlers + `RestoreRequest`/`RestoreResponse`; extend `BackupsResponse` (`running`) and `list_backups`. Register routes in `admin_routes()`.
- `lib.rs` — add `backup_running: Arc<AtomicBool>` to `AppState` + initialize in `AppState::new`.
- `tests/admin_test.rs` — HTTP-layer tests for the new endpoints.

**Clients:**
- `ts-client/src/admin.ts` — add backup methods to `RtDbAdminClient`; `tests/admin.test.ts` (or existing admin test file).
- `rust-client/src/http.rs` — add backup methods to the admin client (find existing `create_db`/`delete_db`); `tests/` companion.
- `python-client/src/par_rt_db/http_client.py` — add backup methods; `tests/test_http_client.py`.

**Dashboard (`dashboard/src/`):**
- `lib/admin.tsx` — add `BackupFile` type + backup methods to `AdminClient`.
- `pages/BackupsPage.tsx` + `pages/BackupsPage.module.css` + `pages/BackupsPage.test.tsx` — the page.
- `App.tsx` — add the `<Route path="backups" ...>`; add a nav entry in the `AppShell` nav component.

**Docs:**
- `CLAUDE.md` — backups invariant bullet.
- `deploy/README.md` — `CREATEDB` requirement + restore/cutover runbook.
- `FEATURE_MATRIX.md` — note the backups-lifecycle row + client-mirror status.

---

## Task 1: backup.rs — name validation + restore-target helper (pure)

**Files:**
- Modify: `server/src/backup.rs` (add `validate_dump_name`, `restore_target_name`; tests in the existing `#[cfg(test)] mod tests`)

**Interfaces:**
- Consumes: existing `parse_timestamp_utc` (validates the ISO stamp grammar).
- Produces: `pub(crate) fn validate_dump_name(name: &str) -> Result<(), RtDbError>` and `pub(crate) fn restore_target_name(name: &str) -> String` — used by Tasks 2 and 3.

- [ ] **Step 1: Write the failing tests** (append to `mod tests` in `backup.rs`)

```rust
#[test]
fn validate_dump_name_accepts_well_formed() {
    assert!(validate_dump_name("rtdb-20260728T143045Z.dump").is_ok());
}

#[test]
fn validate_dump_name_rejects_traversal_and_garbage() {
    // path traversal attempts
    assert!(validate_dump_name("../etc/passwd").is_err());
    assert!(validate_dump_name("rtdb-../../x.dump").is_err());
    assert!(validate_dump_name("rtdb-20260728T143045Z.dump/../../x").is_err());
    assert!(validate_dump_name("rtdb-2026/0728.dump").is_err());
    // wrong shape
    assert!(validate_dump_name("rtdb-20260728143045.dump").is_err()); // missing T/Z
    assert!(validate_dump_name("other-20260728T143045Z.dump").is_err()); // wrong prefix
    assert!(validate_dump_name("rtdb-20260728T143045Z").is_err()); // missing .dump
    assert!(validate_dump_name("").is_err());
}

#[test]
fn restore_target_name_is_scoped_stamp() {
    assert_eq!(
        restore_target_name("rtdb-20260728T143045Z.dump"),
        "rtdb_restored_20260728T143045Z"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib backup::tests::validate_dump_name backup::tests::restore_target_name`
Expected: FAIL — `cannot find function validate_dump_name`.

- [ ] **Step 3: Implement** (add near `parse_timestamp_utc` in `backup.rs`)

```rust
/// Validates a dump filename accepted by the download/delete/restore routes.
/// Accepts only `rtdb-<stamp>.dump` where `<stamp>` is the ISO-8601 basic stamp
/// `format_timestamp_utc` produces. This is the path-traversal guard for routes
/// that resolve a file under `backup_dir` from a client-supplied name.
pub(crate) fn validate_dump_name(name: &str) -> Result<(), RtDbError> {
    if !name.starts_with("rtdb-") || !name.ends_with(".dump") {
        return Err(RtDbError::bad_request("invalid backup filename"));
    }
    let stem = &name["rtdb-".len()..name.len() - ".dump".len()];
    // Defense in depth — the stamp grammar below already forbids these.
    if stem.contains('/') || stem.contains('\\') || stem.contains("..") {
        return Err(RtDbError::bad_request("invalid backup filename"));
    }
    // Reuses the same parser `list_backups` uses, so the grammar is identical.
    if parse_timestamp_utc(stem).is_none() {
        return Err(RtDbError::bad_request("invalid backup filename"));
    }
    Ok(())
}

/// Derives the restore target DB name `rtdb_restored_<stamp>` from a
/// `validate_dump_name`-valid filename.
pub(crate) fn restore_target_name(name: &str) -> String {
    let stem = &name["rtdb-".len()..name.len() - ".dump".len()];
    format!("rtdb_restored_{stem}")
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib backup`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/backup.rs
git commit -m "feat(backup): add validate_dump_name + restore_target_name helpers"
```

---

## Task 2: backup.rs — restore_to_new_db + lift perform_backup

**Files:**
- Modify: `server/src/backup.rs` (add `apply_pg_env`, `restore_to_new_db`, `drop_restore_target`; lift `perform_backup` to `pub(crate)`; add `#[ignore]` integration test)

**Interfaces:**
- Consumes: `validate_dump_name`, `restore_target_name` (Task 1), `parse_pg_env` (existing).
- Produces: `pub(crate) async fn perform_backup(...)` (lifted) and `pub(crate) async fn restore_to_new_db(database_url: &str, dir: &str, name: &str) -> Result<String, RtDbError>` returning the target DB name — used by Task 3.

**Top risk (validate here):** the dump archive records `CREATE EXTENSION vector`; `pg_restore` into a fresh target replays it and may fail under a non-superuser role. The `#[ignore]` test below is the gate. If it fails on `CREATE EXTENSION`, mitigation: in `restore_to_new_db`, after `createdb` and before `pg_restore`, run `psql -c "CREATE EXTENSION IF NOT EXISTS vector"` against the target (same `PG*` env, `PGDATABASE=target`) so the archive's `CREATE EXTENSION` becomes a no-op.

- [ ] **Step 1: Write the failing `#[ignore]` integration test** (append to `mod tests`)

```rust
/// End-to-end restore into a fresh DB. Self-skips when `pg_restore`/`createdb`
/// are absent or there is no live dev Postgres. Run with
/// `cargo test --lib restore -- --ignored` against 127.0.0.1:55434.
#[tokio::test]
#[ignore = "requires pg_restore + createdb on PATH + a live Postgres; self-skips otherwise"]
async fn restore_to_new_db_against_dev_postgres() {
    for tool in ["pg_dump", "pg_restore", "createdb", "dropdb"] {
        let probe = tokio::process::Command::new(tool)
            .arg("--version").stderr(Stdio::null()).stdout(Stdio::null()).status().await;
        if !matches!(probe, Ok(s) if s.success()) {
            eprintln!("skipping: {tool} not found on PATH");
            return;
        }
    }
    let url = std::env::var("RTDB_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://rtdb:rtdb@127.0.0.1:55434/rtdb".into());
    let dir = tempfile::tempdir().unwrap();
    let dir_path = dir.path().to_str().unwrap().to_string();

    // Make a real dump first (reuse the existing, now pub(crate), perform_backup).
    let dumped = perform_backup(&url, &dir_path).await.expect("backup ok");
    let name = dumped.file_name().unwrap().to_string_lossy().into_owned();

    // Restore into a fresh target DB.
    let target = restore_to_new_db(&url, &dir_path, &name).await.expect("restore ok");
    assert!(target.starts_with("rtdb_restored_"));

    // The restored DB must contain the rtdb_auth schema (system tables) — proves
    // the archive restored into the new DB. Connect via a fresh pool.
    let target_url = url_with_db(&url, &target);
    let pool = sqlx::PgPoolOptions::new().max_connections(1).connect(&target_url)
        .await.expect("connect target");
    let row: (i64,) = sqlx::query_as("SELECT count(*) FROM rtdb_auth.databases")
        .fetch_one(&pool).await.expect("query restored db");
    assert!(row.0 >= 0, "rtdb_auth.databases must exist in the restored db");
    pool.close().await;

    // Clean up the restored DB so the test is repeatable.
    let pg = parse_pg_env(&url).unwrap();
    let _ = drop_restore_target(&pg, &target).await;
}

/// Helper for the test: swap the dbname in a postgres:// URL.
fn url_with_db(url: &str, db: &str) -> String {
    let mut u = url::Url::parse(url).unwrap();
    u.set_path(db);
    u.to_string()
}
```

- [ ] **Step 2: Run the test to verify it fails** (compile error first)

Run: `cargo test --lib restore -- --ignored`
Expected: FAIL — `cannot find function restore_to_new_db` / `perform_backup` is private.

- [ ] **Step 3: Lift `perform_backup` to `pub(crate)`**

In `backup.rs`, change:
```rust
async fn perform_backup(database_url: &str, dir: &str) -> Result<PathBuf, RtDbError> {
```
to:
```rust
pub(crate) async fn perform_backup(database_url: &str, dir: &str) -> Result<PathBuf, RtDbError> {
```

- [ ] **Step 4: Implement the restore helpers** (add near `perform_backup`)

```rust
use std::process::Stdio;
use tokio::process::Command;

/// Sets the `PG*` env vars on `cmd` from the parsed connection URL, overriding
/// the database to `db_override`. Mirrors `perform_backup`'s env discipline:
/// credentials travel via env, never argv. `db_override = None` means "do not
/// set PGDATABASE" (used by `createdb`, which connects to the maintenance db).
fn apply_pg_env(cmd: &mut Command, pg: &PgEnv, db_override: Option<&str>) {
    if let Some(user) = pg.user.as_deref() { cmd.env("PGUSER", user); }
    if let Some(pw) = pg.password.as_deref() { cmd.env("PGPASSWORD", pw); }
    if let Some(host) = pg.host.as_deref() { cmd.env("PGHOST", host); }
    if let Some(port) = pg.port { cmd.env("PGPORT", port.to_string()); }
    if let Some(db) = db_override { cmd.env("PGDATABASE", db); }
}

/// `dropdb <target>` — used to clean up a half-populated restore target so a
/// retry starts clean. Best-effort: errors are mapped and logged by the caller.
async fn drop_restore_target(pg: &PgEnv, target: &str) -> Result<(), RtDbError> {
    let mut cmd = Command::new("dropdb");
    apply_pg_env(&mut cmd, pg, pg.database.as_deref());
    cmd.arg(target).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    let out = cmd.output().await
        .map_err(|e| RtDbError::internal(format!("failed to spawn dropdb: {e}")))?;
    if !out.status.success() {
        tracing::warn!(stderr = %String::from_utf8_lossy(&out.stderr), target, "dropdb failed");
    }
    Ok(())
}

/// Restores a dump into a freshly-created `rtdb_restored_<stamp>` Postgres DB.
/// The live `rtdb` DB is never touched — the committer and all live connections
/// keep running undisturbed. Returns the target DB name.
pub(crate) async fn restore_to_new_db(
    database_url: &str,
    dir: &str,
    name: &str,
) -> Result<String, RtDbError> {
    validate_dump_name(name)?;
    let target = restore_target_name(name);

    let mut path = PathBuf::from(dir);
    path.push(name);
    if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
        return Err(RtDbError::not_found("backup file not found"));
    }
    let pg = parse_pg_env(database_url)?;

    // 1) createdb <target> — connects to the original db (PGDATABASE = original).
    let mut createdb = Command::new("createdb");
    apply_pg_env(&mut createdb, &pg, pg.database.as_deref());
    createdb.arg(&target).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    let out = createdb.output().await
        .map_err(|e| RtDbError::internal(format!("failed to spawn createdb: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        tracing::error!(stderr = %stderr, target = %target, "createdb failed");
        if stderr.contains("already exists") {
            return Err(RtDbError::conflict("restore target database already exists"));
        }
        return Err(RtDbError::internal("createdb failed; see server logs"));
    }

    // 2) pg_restore --no-owner --no-privileges <path> — into the target DB.
    //    No --create (that would recreate the archived db name `rtdb`, colliding
    //    with the live one).
    let mut restore = Command::new("pg_restore");
    restore.arg("--no-owner").arg("--no-privileges").arg(&path);
    apply_pg_env(&mut restore, &pg, Some(&target));
    restore.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    let out = restore.output().await
        .map_err(|e| RtDbError::internal(format!("failed to spawn pg_restore: {e}")))?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        tracing::error!(stderr = %stderr, target = %target, "pg_restore failed");
        let _ = drop_restore_target(&pg, &target).await; // clean up the partial target
        return Err(RtDbError::internal("pg_restore failed; see server logs"));
    }
    Ok(target)
}
```

> **Verify before assuming green:** if Step 6 below fails on `CREATE EXTENSION vector`, add a `CREATE EXTENSION IF NOT EXISTS vector` step (via `psql` with `apply_pg_env(&mut cmd, &pg, Some(&target))`) between createdb and pg_restore, per the risk note above.

- [ ] **Step 5: Verify pure unit tests still pass** (no DB)

Run: `cargo test --lib backup`
Expected: PASS (the `#[ignore]` test is skipped).

- [ ] **Step 6: Run the `#[ignore]` restore test manually against the dev DB** (risk gate)

```bash
make dev-db-up
cargo test --lib restore -- --ignored --nocapture
```
Expected: PASS — a `rtdb_restored_<stamp>` DB is created with `rtdb_auth.databases` present, then cleaned up. If it fails on `CREATE EXTENSION vector`, apply the mitigation in the note above and re-run.

- [ ] **Step 7: Commit**

```bash
git add server/src/backup.rs
git commit -m "feat(backup): restore_to_new_db — pg_restore into fresh rtdb_restored_<stamp>"
```

---

## Task 3: server endpoints + AppState flag + routing

**Files:**
- Modify: `server/src/lib.rs` (add `backup_running` to `AppState` + `AppState::new`)
- Modify: `server/src/admin.rs` (handlers, request/response structs, route registration, extend `list_backups`/`BackupsResponse`)
- Test: `server/tests/admin_test.rs`

**Interfaces:**
- Consumes: `backup::perform_backup`, `backup::list_backups`, `backup::validate_dump_name`, `backup::restore_to_new_db` (Tasks 1–2); existing `require_admin`, `ApiJson`, `BackupsResponse`, `OkResponse`, `RtDbError`.
- Produces: HTTP endpoints `POST /admin/backup`, `GET /admin/backups` (extended), `GET|DELETE /admin/backups/{name}`, `POST /admin/restore`, with the wire shapes in Global Constraints.

- [ ] **Step 1: Write the failing HTTP-layer tests** (add to `server/tests/admin_test.rs`, mirroring how existing admin tests build the app + call routes via the `common` harness — read that file first)

```rust
use crate::common::*; // mirror the existing imports/assist in admin_test.rs

#[tokio::test]
async fn backup_trigger_returns_accepted_then_conflict() {
    let app = build_admin_app().await; // mirror existing helper name in common/admin_test
    let r1 = admin_post(&app, "/admin/backup", &serde_json::Value::Null).await;
    assert_eq!(r1.0, 202);                       // 202 Accepted
    // The spawned pg_dump sets the in-progress flag; a second call conflicts.
    let r2 = admin_post(&app, "/admin/backup", &serde_json::value::Null).await;
    assert_eq!(r2.0, 409);                       // 409 Conflict
}

#[tokio::test]
async fn list_backups_reports_running_flag() {
    let app = build_admin_app().await;
    let (status, body) = admin_get_json::<serde_json::Value>(&app, "/admin/backups").await;
    assert_eq!(status, 200);
    assert!(body.get("running").is_some(), "response must include running");
    assert!(body["backups"].is_array());
}

#[tokio::test]
async fn restore_requires_matching_confirm() {
    let app = build_admin_app().await;
    let body = serde_json::json!({ "name": "rtdb-20260728T143045Z.dump", "confirm": "wrong" });
    let (status, _) = admin_post(&app, "/admin/restore", &body).await;
    assert_eq!(status, 400);                     // confirm guard short-circuits
}

#[tokio::test]
async fn download_rejects_bad_dump_name() {
    let app = build_admin_app().await;
    let (status, _) = admin_get(&app, "/admin/backups/..%2Fetc%2Fpasswd").await;
    assert!(status == 400 || status == 404, "traversal name must be rejected, got {status}");
}
```

> The exact helper names (`build_admin_app`, `admin_post`, `admin_get_json`) come from `server/tests/common` / the top of `admin_test.rs` — read them and rename to match. The confirm-guard and traversal-rejection tests need no live dump; the trigger-conflict test relies only on the in-memory flag (the spawned pg_dump may fail harmlessly if no DB — that's fine, the flag still gets set then cleared).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test --test admin_test backup_trigger list_backups_reports restore_requires download_rejects`
Expected: FAIL — unknown routes / no `running` field.

- [ ] **Step 3: Add `backup_running` to `AppState`** (`server/src/lib.rs`)

```rust
use std::sync::atomic::AtomicBool;
// inside pub struct AppState { ... }
pub backup_running: Arc<AtomicBool>,
```
In `AppState::new`, add to the struct literal:
```rust
backup_running: Arc::new(AtomicBool::new(false)),
```

- [ ] **Step 4: Add the handlers + structs** (`server/src/admin.rs`)

Add imports at the top:
```rust
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use axum::body::Body;
use axum::extract::Path;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use tokio_util::codec::{BytesCodec, FramedRead};
```

Extend the existing `BackupsResponse` and `list_backups`:
```rust
#[derive(Serialize)]
struct BackupsResponse {
    running: bool,
    backups: Vec<crate::backup::BackupFile>,
}

async fn list_backups(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<BackupsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let backups = crate::backup::list_backups(&state.config.backup_dir).await?;
    let running = state.backup_running.load(Ordering::Acquire);
    Ok(Json(BackupsResponse { running, backups }))
}
```

Add the new handlers + request/response types (place near `delete_db`):
```rust
/// `POST /admin/backup` — trigger one `pg_dump` now. Returns 202 immediately;
/// the dump runs in a detached task and the in-progress flag is cleared on
/// completion. A second call while one is running → 409. Runs outside the
/// committer (pg_dump is a read), exactly like the cron backup task.
async fn create_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<OkResponse>), RtDbError> {
    require_admin(&state, &headers).await?;
    if state.backup_running.swap(true, Ordering::AcqRel) {
        return Err(RtDbError::conflict("backup already running"));
    }
    let url = state.config.database_url.clone();
    let dir = state.config.backup_dir.clone();
    let flag = state.backup_running.clone();
    tokio::spawn(async move {
        match crate::backup::perform_backup(&url, &dir).await {
            Ok(p) => tracing::info!(path = %p.display(), "manual backup completed"),
            Err(e) => tracing::error!(error = %e, "manual backup failed"),
        }
        flag.store(false, Ordering::Release);
    });
    Ok((StatusCode::ACCEPTED, Json(OkResponse { ok: true })))
}

/// `GET /admin/backups/{name}` — stream a dump file (admin-gated).
async fn download_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<Response, RtDbError> {
    require_admin(&state, &headers).await?;
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    let file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(RtDbError::not_found("backup file not found"));
        }
        Err(_) => return Err(RtDbError::internal("failed to open backup")),
    };
    let body = Body::from_stream(FramedRead::new(file, BytesCodec::new()));
    let mut resp = Response::new(body);
    resp.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")).unwrap(),
    );
    Ok(resp)
}

/// `DELETE /admin/backups/{name}` — remove one dump (admin-gated).
async fn delete_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> Result<StatusCode, RtDbError> {
    require_admin(&state, &headers).await?;
    crate::backup::validate_dump_name(&name)?;
    let mut path = PathBuf::from(&state.config.backup_dir);
    path.push(&name);
    match tokio::fs::remove_file(&path).await {
        Ok(_) => Ok(StatusCode::NO_CONTENT),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            Err(RtDbError::not_found("backup file not found"))
        }
        Err(_) => Err(RtDbError::internal("failed to delete backup")),
    }
}

#[derive(Deserialize)]
struct RestoreRequest {
    name: String,
    confirm: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RestoreResponse {
    target: String,
    instructions: String,
}

/// `POST /admin/restore` — restore a dump into a fresh `rtdb_restored_<stamp>`
/// DB. `confirm` must equal `name` (typed guard). The live DB is never touched.
async fn restore_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RestoreRequest>,
) -> Result<Json<RestoreResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if body.confirm != body.name {
        return Err(RtDbError::bad_request("confirmation does not match backup filename"));
    }
    let target =
        crate::backup::restore_to_new_db(&state.config.database_url, &state.config.backup_dir, &body.name).await?;
    Ok(Json(RestoreResponse {
        instructions: format!(
            "Restore complete into database '{target}'. To cut over: set RTDB_DATABASE_URL to connect to '{target}', then restart the server."
        ),
        target,
    }))
}
```

> If `RtDbError::conflict` / `RtDbError::not_found` do not exist with those names, check `server/src/error.rs` and use the matching constructors (codes `CONFLICT`/`NOT_FOUND` exist per `error.rs`).

- [ ] **Step 5: Register the routes** in `admin_routes()` (`server/src/admin.rs`, near the existing `/admin/backups` line)

```rust
.route("/admin/backup", post(create_backup))
.route("/admin/backups", get(list_backups))
.route("/admin/backups/{name}", get(download_backup).delete(delete_backup))
.route("/admin/restore", post(restore_backup))
```
(Replace the existing single `.route("/admin/backups", get(list_backups))` with these four lines.)

- [ ] **Step 6: Run the new tests**

Run: `cargo test --test admin_test`
Expected: PASS.

- [ ] **Step 7: Run the full server gate**

Run: `make dev-db-up && (cd server && cargo test)` then `make fmt-check && make lint`
Expected: green (clippy `-D warnings` clean, no `unwrap()`/`expect()` outside tests).

- [ ] **Step 8: Commit**

```bash
git add server/src/lib.rs server/src/admin.rs server/tests/admin_test.rs
git commit -m "feat(server): backup trigger/download/delete/restore admin endpoints"
```

---

## Task 4: ts-client — admin backup methods

**Files:**
- Modify: `ts-client/src/admin.ts` (add methods to `RtDbAdminClient`)
- Test: `ts-client/tests/admin.test.ts` (or the existing admin test file — find `deleteDb` tests and mirror)

**Interfaces:**
- Consumes: the `request` helper in `admin.ts` (existing).
- Produces: `backupNow()`, `listBackups()`, `downloadBackup(name)`, `deleteBackup(name)`, `restoreBackup(name)` and exported `BackupFile`/`BackupsListResponse`/`RestoreResult` types.

- [ ] **Step 1: Write the failing tests** (mirror the existing admin test setup — find how `deleteDb` is tested, including how the mock server / in-memory harness is built)

```ts
import { describe, it, expect } from "vitest";
// import the existing test harness used by deleteDb/createDb tests in this file

describe("RtDbAdminClient backups", () => {
  it("backupNow POSTs /admin/backup", async () => {
    const c = makeClient(); // existing helper
    fetchMock.once("202", ""); // or the harness's pattern
    await c.backupNow();
    expect(lastCall()).toMatchObject({ method: "POST", path: "/admin/backup" });
  });

  it("listBackups returns running + backups", async () => {
    const c = makeClient();
    fetchMock.once(JSON.stringify({ running: false, backups: [
      { name: "rtdb-20260728T143045Z.dump", sizeBytes: 10, createdMs: 123 },
    ]}));
    const res = await c.listBackups();
    expect(res.running).toBe(false);
    expect(res.backups[0].name).toBe("rtdb-20260728T143045Z.dump");
  });

  it("restoreBackup sends confirm === name", async () => {
    const c = makeClient();
    fetchMock.once(JSON.stringify({ target: "rtdb_restored_x", instructions: "..." }));
    const res = await c.restoreBackup("rtdb-20260728T143045Z.dump");
    expect(lastCall().body).toEqual({ name: "rtdb-20260728T143045Z.dump", confirm: "rtdb-20260728T143045Z.dump" });
    expect(res.target).toBe("rtdb_restored_x");
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts`
Expected: FAIL — `backupNow is not a function`.

- [ ] **Step 3: Implement** (in `ts-client/src/admin.ts`)

Add exported types near the other admin types:
```ts
export interface BackupFile {
  name: string;
  sizeBytes: number;
  createdMs: number;
}
export interface BackupsListResponse {
  running: boolean;
  backups: BackupFile[];
}
export interface RestoreResult {
  target: string;
  instructions: string;
}
```
Add methods to `RtDbAdminClient` (the `request` helper is `private async request(method, path, body?)`):
```ts
  /** Trigger one pg_dump now (POST /admin/backup, 202 Accepted). */
  async backupNow(): Promise<void> {
    await this.request("POST", "/admin/backup", {});
  }

  async listBackups(): Promise<BackupsListResponse> {
    return (await this.request("GET", "/admin/backups")) as BackupsListResponse;
  }

  /** Download a dump as a raw Response (caller streams to a file). Uses the
   *  fetch impl directly because the result is binary, not JSON. */
  async downloadBackup(name: string): Promise<Response> {
    const resp = await this.fetchImpl(`${this.url}/admin/backups/${encodeURIComponent(name)}`, {
      method: "GET",
      headers: { authorization: `Bearer ${this.adminKey}` },
    });
    if (!resp.ok) throw new Error(`download failed (${resp.status})`);
    return resp;
  }

  async deleteBackup(name: string): Promise<void> {
    await this.request("DELETE", `/admin/backups/${encodeURIComponent(name)}`);
  }

  /** Restore a dump into a fresh rtdb_restored_<stamp> DB. `confirm` is sent
   *  equal to `name` (the server's typed guard). */
  async restoreBackup(name: string): Promise<RestoreResult> {
    return (await this.request("POST", "/admin/restore", { name, confirm: name })) as RestoreResult;
  }
```
> Confirm `this.request` accepts a `"DELETE"` method with no body (check the helper's signature and adjust, e.g. pass `null`/`undefined` if it requires a third arg). Export the new types from `index.ts` alongside the existing admin exports.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/admin.ts ts-client/src/index.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): admin backup methods (trigger/list/download/delete/restore)"
```

---

## Task 5: rust-client — admin backup methods

**Files:**
- Modify: `rust-client/src/http.rs` (the admin client lives here behind the `admin` feature — locate the existing `create_db`/`delete_db`/`mint_token` methods and mirror them)
- Test: the rust-client admin test file (find where `delete_db` is tested)

**Interfaces:**
- Consumes: the existing HTTP admin request helper in `http.rs`.
- Produces: `backup_now()`, `list_backups()`, `download_backup(name) -> Vec<u8>`, `delete_backup(name)`, `restore_backup(name) -> RestoreResult` (+ `BackupFile`, `BackupsListResponse`, `RestoreResult` types).

- [ ] **Step 1: Locate the existing admin surface**

Run: `grep -n "delete_db\|create_db\|mint_token\|fn .*admin\|/admin/" rust-client/src/http.rs`
Read the helper that POSTs to `/admin/*` and the existing struct/type definitions. Note its error type and how it returns JSON.

- [ ] **Step 2: Write the failing test** (mirror the existing admin test's app/mock setup)

```rust
#[tokio::test]
async fn backup_admin_methods_wire_correctly() {
    let client = test_admin_client(); // existing helper used by delete_db test
    // list_backups returns the wire shape
    // restore_backup sends confirm == name and parses target
    // (mirror the assertions from the ts-client task, in Rust)
    let res = client.list_backups().await.unwrap();
    // assert res.backups / res.running shape
    let r = client.restore_backup("rtdb-20260728T143045Z.dump").await.unwrap();
    assert!(r.target.starts_with("rtdb_restored_"));
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd rust-client && cargo test --features admin backup_admin`
Expected: FAIL — no method `list_backups`.

- [ ] **Step 4: Implement** (in `rust-client/src/http.rs`, alongside the existing admin methods)

```rust
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupFile {
    pub name: String,
    pub size_bytes: u64,
    pub created_ms: i64,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupsListResponse {
    pub running: bool,
    pub backups: Vec<BackupFile>,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreResult {
    pub target: String,
    pub instructions: String,
}
```
Methods (use the existing admin POST/GET helper — match its exact name/signature):
```rust
pub async fn backup_now(&self) -> Result<(), Error> { /* POST /admin/backup {} */ }
pub async fn list_backups(&self) -> Result<BackupsListResponse, Error> { /* GET /admin/backups */ }
pub async fn download_backup(&self, name: &str) -> Result<Vec<u8>, Error> { /* GET /admin/backups/{name} -> bytes */ }
pub async fn delete_backup(&self, name: &str) -> Result<(), Error> { /* DELETE /admin/backups/{name} */ }
pub async fn restore_backup(&self, name: &str) -> Result<RestoreResult, Error> {
    // POST /admin/restore { name, confirm: name } -> RestoreResult
}
```
> Mirror the existing helper exactly for URL building, auth header, and error mapping. For `download_backup`, the response is binary — use the raw bytes path, not the JSON deserializer.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd rust-client && cargo test --features admin`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add rust-client/src/http.rs rust-client/tests/   # or wherever the admin test lives
git commit -m "feat(rust-client): admin backup methods (trigger/list/download/delete/restore)"
```

---

## Task 6: python-client — admin backup methods

**Files:**
- Modify: `python-client/src/par_rt_db/http_client.py` (find the existing admin methods `delete_db`/`create_db`/`mint_token` and mirror)
- Test: `python-client/tests/test_http_client.py`

**Interfaces:**
- Consumes: the existing `_admin_request`/`_post`/`_get` helper in `http_client.py`.
- Produces: `backup_now()`, `list_backups() -> dict`, `download_backup(name) -> bytes`, `delete_backup(name)`, `restore_backup(name) -> dict`.

- [ ] **Step 1: Locate the existing admin surface**

Run: `grep -n "delete_db\|create_db\|mint_token\|/admin/" python-client/src/par_rt_db/http_client.py`
Read the helper methods and the class they belong to.

- [ ] **Step 2: Write the failing test** (mirror existing admin tests; find how `delete_db` is tested — likely with a mock/recorded HTTP layer or the in-memory harness)

```python
def test_backup_admin_methods(client):  # mirror the fixture used by delete_db
    res = client.list_backups()
    assert "running" in res and "backups" in res
    r = client.restore_backup("rtdb-20260728T143045Z.dump")
    assert r["target"].startswith("rtdb_restored_")
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd python-client && uv run pytest -q tests/test_http_client.py -k backup`
Expected: FAIL — `AttributeError: list_backups`.

- [ ] **Step 4: Implement** (in `http_client.py`, alongside the existing admin methods)

```python
def backup_now(self) -> None:
    """Trigger one pg_dump now (POST /admin/backup)."""
    self._admin_post("/admin/backup", body={})

def list_backups(self) -> dict:
    """Return {running: bool, backups: [{name, sizeBytes, createdMs}]}."""
    return self._admin_get("/admin/backups")

def download_backup(self, name: str) -> bytes:
    """Download a dump file's raw bytes."""
    resp = self._admin_get_raw(f"/admin/backups/{name}")
    return resp.content

def delete_backup(self, name: str) -> None:
    self._admin_delete(f"/admin/backups/{name}")

def restore_backup(self, name: str) -> dict:
    """Restore into a fresh rtdb_restored_<stamp> DB. confirm is sent == name."""
    return self._admin_post("/admin/restore", body={"name": name, "confirm": name})
```
> Match the exact helper names (`_admin_post`/`_admin_get`/etc.) from the file. Add a raw-bytes GET path for `download_backup` if one doesn't exist (don't JSON-decode the response).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd python-client && uv run pytest -q tests/test_http_client.py`
Expected: PASS.

- [ ] **Step 6: Lint/typecheck**

Run: `cd python-client && uv run ruff check . && uv run pyright`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add python-client/src/par_rt_db/http_client.py python-client/tests/test_http_client.py
git commit -m "feat(python-client): admin backup methods (trigger/list/download/delete/restore)"
```

---

## Task 7: Dashboard — BackupsPage

**Files:**
- Modify: `dashboard/src/lib/admin.tsx` (add `BackupFile` type + methods to `AdminClient`)
- Create: `dashboard/src/pages/BackupsPage.tsx`, `dashboard/src/pages/BackupsPage.module.css`, `dashboard/src/pages/BackupsPage.test.tsx`
- Modify: `dashboard/src/App.tsx` (add `<Route path="backups" element={<BackupsPage />} />`) and the `AppShell` nav component (add a "Backups" nav entry — find it via the `<AppShell />` usage)

**Interfaces:**
- Consumes: the `AdminClient.req<T>` helper (existing); the server wire shapes.
- Produces: the Backups page on the `/backups` route.

- [ ] **Step 1: Add types + methods to `AdminClient`** (`dashboard/src/lib/admin.tsx`)

```ts
export interface BackupFile {
  name: string;
  sizeBytes: number;
  createdMs: number;
}
export interface BackupsListResponse {
  running: boolean;
  backups: BackupFile[];
}
export interface RestoreResult {
  target: string;
  instructions: string;
}
// inside class AdminClient {
  backupNow(): Promise<void> {
    return this.req("/admin/backup", { method: "POST", body: "{}" });
  }
  listBackups(): Promise<BackupsListResponse> {
    return this.req("/admin/backups");
  }
  async downloadBackup(name: string): Promise<void> {
    const resp = await fetch(`/admin/backups/${encodeURIComponent(name)}`, {
      headers: this.authHeader(),
    });
    if (!resp.ok) throw new Error(`download failed (${resp.status})`);
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  }
  deleteBackup(name: string): Promise<void> {
    return this.req(`/admin/backups/${encodeURIComponent(name)}`, { method: "DELETE" });
  }
  restoreBackup(name: string): Promise<RestoreResult> {
    return this.req("/admin/restore", {
      method: "POST",
      body: JSON.stringify({ name, confirm: name }),
    });
  }
// }
```
> `req` already sets `content-type: json` + the bearer token from `getToken()`. For `downloadBackup`, fetch needs the same auth header — extract a small `private authHeader()` helper returning `{ authorization: ... }` (or inline `this.getToken()`). Match how `session.tsx` does admin fetches.

- [ ] **Step 2: Write the failing page test** (`dashboard/src/pages/BackupsPage.test.tsx`, mirroring an existing page test like `MetricsPage.test.tsx` for the mock-admin-client pattern)

```tsx
import { describe, it, expect, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { BackupsPage } from "./BackupsPage";

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: {
      listBackups: vi.fn().mockResolvedValue({ running: false, backups: [
        { name: "rtdb-20260728T143045Z.dump", sizeBytes: 1024, createdMs: 1788019245000 },
      ]}),
      backupNow: vi.fn().mockResolvedValue(undefined),
    },
  }),
}));

describe("BackupsPage", () => {
  it("lists backups newest-first", async () => {
    render(<BackupsPage />);
    await waitFor(() => expect(screen.getByText(/rtdb-20260728T143045Z\.dump/)).toBeInTheDocument());
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd dashboard && bunx vitest run pages/BackupsPage.test.tsx`
Expected: FAIL — module not found.

- [ ] **Step 4: Implement the page** (`BackupsPage.tsx`) — newest-first table; "Back up now" button (polls `listBackups` every 2s while `running`); per-row Download / Restore (modal requiring exact-name confirm) / Delete. Mirror the styling conventions of an existing page (read `MetricsPage.tsx` + its `.module.css`). Use `lib/format` for humanized size/time.

- [ ] **Step 5: Add the route + nav**

In `dashboard/src/App.tsx` add inside the `<Route element={<AppShell />}>` group:
```tsx
<Route path="backups" element={<BackupsPage />} />
```
(import `BackupsPage` from `./pages/BackupsPage`). Add a "Backups" entry to the `AppShell` nav alongside the others (find the nav list in the AppShell component).

- [ ] **Step 6: Run the page test + typecheck**

Run: `cd dashboard && bunx vitest run pages/BackupsPage.test.tsx && bunx tsc --noEmit`
Expected: PASS + clean typecheck.

- [ ] **Step 7: Commit**

```bash
git add dashboard/src/lib/admin.tsx dashboard/src/pages/BackupsPage.* dashboard/src/App.tsx dashboard/src/AppShell* 
git commit -m "feat(dashboard): BackupsPage — view/trigger/download/delete/restore"
```

---

## Task 8: Documentation

**Files:**
- Modify: `CLAUDE.md` (add a backups invariant bullet)
- Modify: `deploy/README.md` (`CREATEDB` requirement + restore/cutover runbook)
- Modify: `FEATURE_MATRIX.md` (backups-lifecycle row + client-mirror status)

- [ ] **Step 1: `CLAUDE.md`** — under "Invariants you must preserve", add a bullet alongside the TTL/op-feed ones:

```markdown
- **Backups lifecycle** (`backup.rs`, `admin.rs`): the manual `POST /admin/backup` trigger spawns one `pg_dump` **outside the committer** (a read — same as the cron task) and is gated by an `AppState` `backup_running` flag (a second call while running → 409). `POST /admin/restore` restores a dump into a **fresh `rtdb_restored_<stamp>` Postgres DB** via `createdb` + `pg_restore --no-owner --no-privileges` — the live `rtdb` DB is never touched, so the single-writer invariant is preserved and there are no locks/races. `GET|DELETE /admin/backups/{name}` download/delete a dump; all name-bearing routes pass `backup::validate_dump_name` (path-traversal guard). Restore requires a typed `confirm == name`. **`CREATEDB` privilege** is required on the DB role (new — the server previously only ran `CREATE SCHEMA`/`CREATE EXTENSION`). Credentials travel via `PG*` env, never argv.
```

- [ ] **Step 2: `deploy/README.md`** — add a "Backups & restore" section: the `CREATEDB` requirement for the DB role, how to enable scheduled backups (`RTDB_BACKUP_ENABLED` etc. — already documented; cross-link), and the restore runbook: trigger from the console (or `POST /admin/restore`), then cut over by pointing `RTDB_DATABASE_URL` at the `rtdb_restored_<stamp>` DB and restarting the server.

- [ ] **Step 3: `FEATURE_MATRIX.md`** — flip/note the backups row to reflect view+trigger+download+delete+restore, and note client-mirror status (ts/rust/python admin clients + dashboard).

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md deploy/README.md FEATURE_MATRIX.md
git commit -m "docs(enh-002): backups lifecycle invariant, CREATEDB requirement, restore runbook"
```

---

## Task 9: Full gate + close-out

- [ ] **Step 1: Build the ts-client dist the dashboard typecheck needs**

Run: `make ts-client-build`
Expected: success (the dashboard resolves `@par-rt-db/client` from `ts-client/dist`).

- [ ] **Step 2: Run the full repo gate**

Run: `make checkall`
Expected: green (fmt-check + clippy `-D warnings` + typecheck across all five packages + tests).

- [ ] **Step 3: Re-run the `#[ignore]` restore test to confirm the risk is still clear**

Run: `make dev-db-up && (cd server && cargo test --lib restore -- --ignored --nocapture)`
Expected: PASS.

- [ ] **Step 4: Mark ENH-002 done**

In `ENHANCEMENTS.md`, change `- [ ] **ENH-002` to `- [x] **ENH-002`.

- [ ] **Step 5: Commit + board**

```bash
git add ENHANCEMENTS.md
git commit -m "docs(enh-002): mark ENH-002 done — backups dashboard shipped"
```
Move the kanban card to `done`: `kanban item done --id 019fcb66d144735097f46055be58a6ea`.

---

## Self-Review (run after writing)

- **Spec coverage:** ✅ list (Task 3 `list_backups`), trigger (Task 3 `create_backup`), download (Task 3), delete (Task 3), restore-to-new-DB (Tasks 2+3), dashboard page (Task 7), client parity (Tasks 4–6), docs (Task 8), CREATEDB requirement (Task 8 + flagged in Task 2), single-writer invariant (Task 3 comment + docs). The `CREATE EXTENSION vector` risk is the Task 2 gate.
- **Placeholder scan:** Client/server helper names that depend on reading the existing file (e.g. `request` signature, `_admin_post`, the admin-test `common` helpers) are explicit "read the file and match" instructions, not TODOs — the exact wire shapes and method bodies are given.
- **Type consistency:** `BackupsListResponse { running, backups: BackupFile[] }` and `RestoreResult { target, instructions }` are identical across server, ts-client, rust-client, python-client, and dashboard. `validate_dump_name` / `restore_target_name` / `restore_to_new_db` signatures match between Task 1, Task 2, and Task 3.
