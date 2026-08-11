# Backups Dashboard — View, Trigger, Download, Delete, Restore (ENH-002)

- **Date:** 2026-08-04
- **Enhancement:** ENH-002 (kanban board; `ENHANCEMENTS.md` retired)
- **Status:** Implemented (2026-08-10)
- **Depends on:** existing `backup.rs` (scheduled `pg_dump`, `list_backups`), `GET /admin/backups`, `delete-db` typed-confirm pattern.

## Problem

`backup.rs` runs a scheduled `pg_dump` and exposes `GET /admin/backups` (a list), but there is no dashboard page, no manual trigger, no download/delete, and no restore path. An operator who needs to recover must drop to the shell. This spec closes the full backup lifecycle in the console.

## Load-bearing fact

The entire server state — every tenant "database" (a Postgres **schema**) plus all `rtdb_auth.*` / `rtdb.*` system tables and `storage` blobs — lives in a **single Postgres database**: the dbname in `RTDB_DATABASE_URL` (see `db.rs:279-281`, `create_database`). Therefore one `pg_dump --format=custom` is a **full server snapshot**, and "restore" is inherently *over that one live database*. The restore model chosen here restores into a **new** Postgres database and never touches the live one.

## Scope

**In:**
- Dashboard page `BackupsPage.tsx`: list dumps newest-first (time, size) with row actions.
- Manual **async** "Back up now" trigger.
- Download a dump, delete a dump.
- **Restore-to-new-DB** with a typed `confirm` guard, returning the target DB name + cutover instructions.
- Client-parity methods across the dashboard admin client, `ts-client` `RtDbAdminClient`, `rust-client` admin, `python-client` admin.
- Docs: `CLAUDE.md` invariant, `deploy/README.md` `CREATEDB` requirement + restore runbook, `FEATURE_MATRIX.md`.

**Out (YAGNI):**
- Backup config editing (cron/retention via UI) — env / hot config only.
- Per-tenant or selective restore.
- In-place restore over the live DB (rejected: racy with live pool connections, partial-failure risk, self-restart-from-handler is fragile across deploy types).
- Restored-DB auto-registration or auto-cutover.

## Server design

### `AppState`

Add `backup_running: Arc<std::sync::atomic::AtomicBool>`. Set by `POST /admin/backup`; read by `GET /admin/backups`.

### `backup.rs`

- **Lift `perform_backup` to `pub(crate)`** (currently private) so the manual trigger can call the same code path the cron loop uses.
- **`pub(crate) fn validate_dump_name(name: &str) -> Result<(), RtDbError>`** — the path-traversal guard shared by download/delete/restore. Accepts only `rtdb-<stem>.dump` where `<stem>` matches `^\d{8}T\d{6}Z$` (the exact shape `format_timestamp_utc` produces). The regex already forbids `/`, `\`, and `..`; reject those explicitly as defense in depth.
- **`pub(crate) async fn restore_to_new_db(database_url: &str, dir: &str, name: &str) -> Result<String, RtDbError>`** — returns the target DB name:
  1. `validate_dump_name(name)`; resolve `dir/name`; confirm the file exists.
  2. `stem` = name with `rtdb-` prefix and `.dump` suffix removed; `target = format!("rtdb_restored_{stem}")`.
  3. `pg = parse_pg_env(database_url)` (existing).
  4. `createdb <target>` — spawn with `PG*` env (`PGUSER`/`PGPASSWORD`/`PGHOST`/`PGPORT` from `pg`; `PGDATABASE` = original db so `createdb` connects there and creates `target`). On non-zero exit: log stderr, return `Err(RtDbError::internal("createdb failed; see server logs"))`. Map the "already exists" case to a `CONFLICT`-style error.
  5. `pg_restore --no-owner --no-privileges <path>` — spawn with `PG*` env but `PGDATABASE = target`. **Must not pass `--create`** (that would recreate the archived db name `rtdb`, colliding with the live one). On non-zero exit: log stderr, return generic `internal` error.
  6. Return `target`.

No new boot config fields are required; trigger and restore reuse `config.database_url` and `config.backup_dir`.

### `admin/backups.rs` endpoints

All admin-gated through the same `authenticate_admin` middleware as the existing `GET /admin/backups`.

- **`POST /admin/backup`** (`create_backup`) — if `state.backup_running.swap(true, AcqRel)` was already `true` → `409 CONFLICT`. Otherwise clone `database_url`, `backup_dir`, and the `Arc<AtomicBool>`; `tokio::spawn` a detached task that calls `perform_backup(...)`, clears the flag on completion (success or failure), and logs the result. Return `202 {}` immediately.
  - Runs **outside the committer** — `pg_dump` is a read of the DB and does not touch document tables or subscriptions. This matches the existing cron backup task, which also runs `pg_dump` outside the committer.
- **`GET /admin/backups`** — extend the response: `{ running: bool, backups: Vec<BackupFile> }` (read the flag).
- **`GET /admin/backups/{name}`** (`download_backup`) — `validate_dump_name`; `path = backup_dir/name`; `404` if absent; stream via `axum::body::Body::from_stream(tokio::fs::File)` with `Content-Type: application/octet-stream` and `Content-Disposition: attachment; filename="<name>"`. Stream (do not buffer) so large dumps don't blow memory.
- **`DELETE /admin/backups/{name}`** (`delete_backup`) — `validate_dump_name`; `remove_file` (`404` if missing); `204`. No confirm body: backups are reproducible and cron retention already deletes them.
- **`POST /admin/restore`** (`restore_backup`) — body `RestoreRequest { name: String, confirm: String }`; require `confirm == name` (mirrors `delete-db`'s guard → `Forbidden`/`400` on mismatch); call `backup::restore_to_new_db(database_url, backup_dir, name)`; on `Ok` → `200 { target, instructions }` where `instructions = "Restore complete into database '<target>'. To cut over: set RTDB_DATABASE_URL to connect to '<target>', then restart the server."`. On `Err` → propagate as `RtDbError` (generic message; stderr already logged).

### Routing (`lib.rs`)

Alongside the existing `/admin/backups` `get`:

```
.route("/admin/backup", post(create_backup))
.route("/admin/backups", get(list_backups))            // existing, response extended
.route("/admin/backups/{name}", get(download_backup).delete(delete_backup))
.route("/admin/restore", post(restore_backup))
```

### Invariants preserved

- **Single-writer invariant** — restore writes only to the fresh `rtdb_restored_*` DB; the live `rtdb` DB and the committer are untouched. The manual trigger spawns `pg_dump` (a read) outside the committer, exactly as the cron task does.
- **Path traversal** — `validate_dump_name` gates download/delete/restore.
- **Irreversibility** — restore requires a typed confirm; delete is unguarded but non-destructive at the system level (backups are reproducible and cron already prunes them).
- **Credentials** — `pg_dump`/`pg_restore`/`createdb` all take connection params via `PG*` env, never argv, so `ps`/`/proc` don't leak the password. Restore reuses `backup.rs`'s `parse_pg_env`.
- **Errors** — client-facing trigger/restore/download errors are generic (`RtDbError`); tool stderr is logged via `tracing`, never stringified into a response body.

### Requirement: `CREATEDB` privilege

The DB role in `RTDB_DATABASE_URL` must have `CREATEDB` to create the `rtdb_restored_*` target. The server only ever runs `CREATE SCHEMA`/`CREATE EXTENSION` today, so this is a **new** requirement. It is documented in `deploy/README.md`; a missing privilege surfaces as a clear error (`createdb` exits non-zero → "restore failed; see server logs" with stderr in the logs).

## Dashboard design

### `dashboard/src/lib/admin.tsx`

Add to the `AdminClient` class (the dashboard's own fetch wrapper, separate from `ts-client`):

- `backupNow(): Promise<void>` → `POST /admin/backup` (202).
- `listBackups(): Promise<{ running: boolean; backups: BackupFile[] }>` → `GET /admin/backups`.
- `downloadBackup(name: string): Promise<void>` → `fetch` (credentials) → blob → browser download via object URL.
- `deleteBackup(name: string): Promise<void>` → `DELETE /admin/backups/{name}`.
- `restoreBackup(name: string): Promise<{ target: string; instructions: string }>` → `POST /admin/restore { name, confirm: name }`.

Add `BackupFile` type `{ name: string; sizeBytes: number; createdMs: number }`.

### `BackupsPage.tsx` (+ `.module.css`, `.test.tsx`)

- On mount: `listBackups`. Render a table newest-first: **Created** (absolute + relative), **Size** (humanized via `lib/format`), **Actions**.
- **Back up now** button → `backupNow()`; poll `listBackups` every ~2s while `running`; show a spinner and disable the button while running; stop polling once `!running`.
- Row actions:
  - **Download** → `downloadBackup(name)`.
  - **Restore** → modal requiring the operator to type the exact dump name; on submit `restoreBackup(name)`; success banner shows the target DB + cutover steps; errors surfaced inline.
  - **Delete** → confirm dialog; `deleteBackup(name)`; refresh the list.
- Empty state ("No backups yet. Click *Back up now*, or enable `RTDB_BACKUP_ENABLED` for scheduled backups.").
- Wire into nav + router (`App.tsx` / shell), mirroring existing page conventions.

### Dashboard testing

`BackupsPage.test.tsx`: mock the admin client; assert list render, trigger calls `backupNow` and polls, restore confirm gating, delete calls.

## Client parity (`ts-client`, `rust-client`, `python-client`)

These admin methods are for **external automation**; the dashboard uses its own `lib/admin.tsx`. Per the "clients mirror the core" invariant, add the same surface to each (typed `confirm = name` on restore):

- `ts-client` `RtDbAdminClient` (`admin.ts`): `backupNow`, `listBackups`, `downloadBackup` (returns `Blob`/`Response`), `deleteBackup`, `restoreBackup`. + tests.
- `rust-client` admin surface: same set (`downloadBackup` returns `Vec<u8>`). + tests.
- `python-client` admin surface: same set (`download_backup` returns `bytes`). + tests.

Run the three client mirrors as **parallel subagents** (disjoint files).

## Documentation updates

- `CLAUDE.md` — add a backups bullet under "Invariants you must preserve": manual trigger spawns `pg_dump` outside the committer; restore-to-new-DB writes only to `rtdb_restored_*`; `CREATEDB` required; single-writer preserved.
- `deploy/README.md` — `CREATEDB` requirement + restore/cutover runbook (point `RTDB_DATABASE_URL` at the restored DB, restart).
- `FEATURE_MATRIX.md` — flip/note the backups-lifecycle row and client-mirror status.

## Testing summary

- **Pure unit (no DB):** `validate_dump_name` (valid stamp; traversal attempts; bad stem); restore confirm guard; target-name derivation; restore `PG*`-env construction (assert `PGDATABASE = target`, password present in env map but never on argv).
- **`#[ignore]` integration** (self-skip if `pg_dump`/`pg_restore`/`createdb` absent or no dev Postgres, mirroring the existing `perform_backup_against_dev_postgres`): trigger creates `rtdb-*.dump`; restore creates `rtdb_restored_*` with the dumped schemas present; download streams the correct bytes; delete removes the file. Reuse the dev Postgres on `127.0.0.1:55434` / `RTDB_TEST_DATABASE_URL`.
- **Dashboard:** `BackupsPage.test.tsx`.
- **Clients:** per-package tests for the new admin methods.

## Risks / open questions

1. **`CREATE EXTENSION vector` on restore (top risk).** A `pg_dump --format=custom` archive records the `vector` extension; `pg_restore` into a fresh `rtdb_restored_*` DB will replay `CREATE EXTENSION vector`. The extension's shared library is installed cluster-wide (the docker image / `postgresql-client`), but the role may lack superuser. If `CREATE EXTENSION` fails under a non-superuser role, restore aborts. The `#[ignore]` integration test against the dev Postgres will surface this; mitigations if needed: pre-`CREATE EXTENSION vector` in the target before `pg_restore`, or document a superuser requirement. Validate during implementation before assuming green.
2. **`CREATEDB` privilege** on the deploy/dev role (documented; surfaces as a clear error).
3. **`downloadBackup` streaming semantics** differ across the three SDKs (blob vs bytes); keep each idiomatic to its language rather than forcing one shape.
