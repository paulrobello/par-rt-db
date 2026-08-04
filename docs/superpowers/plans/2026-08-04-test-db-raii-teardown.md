# Test-database RAII teardown — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the dev-DB test schema leak at the source — each test's database is dropped automatically when its `TestDb` name goes out of scope, via a dedicated cleanup worker on its own runtime.

**Architecture:** `fresh_db` returns a `TestDb` newtype (behaves like a string) whose `Drop` enqueues the db name to a process-global worker thread (own `tokio::runtime` + own `PgPool`) that runs `db::drop_database`. Independent of the test's current-thread runtime, so cleanup proceeds after the test returns.

**Tech Stack:** Rust (tokio current-thread test runtimes, sqlx, OnceLock, mpsc).

**Spec:** `docs/superpowers/specs/2026-08-04-test-db-raii-teardown-design.md`

## Global Constraints

- **Test-only.** Touch only `server/tests/`. No `server/src/` production behavior change. (Exception: if `db::drop_database` / `db::database_exists` / `pg_schema` need to be `pub(crate)`→`pub` for test access, that visibility bump is allowed and is the only `src/` change.)
- **Reuse `db::drop_database`** for cleanup (DRY — identical to real deletion: schema + registry + tokens + allowlist + storage_index).
- **The worker uses its OWN `tokio::runtime::Runtime` and its OWN `PgPool`** built from `database_url` — never share the test's pool across runtimes (sqlx pins connections to their runtime).
- **No test behavior change** — same db names (`t<uuid-v7>`), same kanban-schema fixture; only the return type + added cleanup differ.
- `fresh_db` still creates the db + pushes the schema; it just also calls `ensure_cleanup_worker` and returns `TestDb`.
- **Definition of done:** `make checkall` green AND a no-accumulation check (clean dev DB → run a test binary → leaked-schema count is ~0, not one-per-test).
- The `make dev-db-clean` valve stays (safety net for the bounded process-exit tail).

---

## File Structure

- `server/tests/common/mod.rs` — add `TestDb` newtype + `CLEANUP_TX` + `ensure_cleanup_worker`; change `fresh_db` to return `TestDb`.
- `server/tests/db_cleanup_test.rs` — NEW: live verification that `TestDb::drop` cleans up via the worker (Task 1).
- `server/tests/**/*.rs` — call-site migration (Task 2): `let db = fresh_db(...)` now binds a `TestDb`; fix compile errors.

---

## Task 1: `TestDb` newtype + cleanup worker (de-risk the mechanism)

**Files:**
- Modify: `server/tests/common/mod.rs` (add `TestDb` + worker; `fresh_db` UNCHANGED here — still returns `String`)
- Create: `server/tests/db_cleanup_test.rs`

**Interfaces:**
- Produces: `pub struct TestDb(String)` with `Deref<Target=str>`/`AsRef<str>`/`Display`/`Clone`/`From<TestDb> for String`/`Drop`; `pub fn ensure_cleanup_worker(database_url: &str)`; `static CLEANUP_TX: OnceLock<UnboundedSender<String>>`. Consumes `db::drop_database` + `db::database_exists` (verify visibility).

**Note on `db` import:** mirror the exact import path `common/mod.rs` already uses for `db::create_database` (read the top of `common/mod.rs`); use the same for `db::drop_database` / `db::database_exists`. If `drop_database`/`database_exists` are not `pub`, bump them to `pub` in `server/src/db.rs` (the only `src/` change).

- [ ] **Step 1: Write the failing verification test** (`server/tests/db_cleanup_test.rs`)

```rust
mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// Mirror the `db` import path used by common/mod.rs (e.g. `use rtdb_server::db;` — confirm in common/mod.rs).
use rtdb_server::db;
use common::{ensure_cleanup_worker, test_state, TestDb};

/// Proves the novel part: dropping a TestDb cleans up its database via the
/// dedicated worker thread (own runtime + pool), even though the test runs on
/// a current-thread runtime that would cancel a plain tokio::spawn.
#[tokio::test]
async fn testdb_drop_cleans_up_via_worker() {
    let state = test_state().await;
    ensure_cleanup_worker(&state.config.database_url);

    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await.expect("create db");
    assert!(db::database_exists(&state.pool, &name).await.expect("exists check"));

    // Drop enqueues the name on the worker (separate thread/runtime).
    drop(TestDb(name.clone()));

    // Poll until the worker has dropped it (it's async + on another thread).
    let mut gone = false;
    for _ in 0..100 {
        if !db::database_exists(&state.pool, &name).await.expect("exists check") {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(gone, "TestDb::drop did not clean up the database via the worker within ~5s");
    // keep `state` alive for the duration of the polling above
    let _ = state;
    let _ = AtomicBool::new(false); // placeholder use to avoid unused-import if AtomicBool unused
}
```
(Remove the `AtomicBool` lines if unused — only there to avoid an unused-import error if you import it; prefer not to import it.)

- [ ] **Step 2: Run the test to verify it fails**

Run: `make dev-db-up && cd server && cargo test --test db_cleanup_test`
Expected: FAIL — `cannot find type TestDb` / `cannot find function ensure_cleanup_worker`.

- [ ] **Step 3: Implement `TestDb` + worker** (add to `server/tests/common/mod.rs`, near `fresh_db`)

```rust
use std::sync::OnceLock;
use tokio::sync::mpsc::{self, UnboundedSender};

/// A par-rt-db database created for a test. `Drop` schedules best-effort
/// teardown (DROP SCHEMA + registry deletes via `db::drop_database`) on a
/// dedicated worker thread that owns its own runtime + pool — independent of
/// the test's current-thread runtime, so cleanup runs after the test returns.
/// Behaves like a string via `Deref`/`AsRef`/`Display`.
pub struct TestDb(pub String);

impl TestDb {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::ops::Deref for TestDb {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}
impl AsRef<str> for TestDb {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for TestDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl Clone for TestDb {
    fn clone(&self) -> Self {
        TestDb(self.0.clone())
    }
}
impl From<TestDb> for String {
    fn from(t: TestDb) -> String {
        t.0
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        if let Some(tx) = CLEANUP_TX.get() {
            let name = std::mem::take(&mut self.0);
            let _ = tx.send(name);
        }
    }
}

static CLEANUP_TX: OnceLock<UnboundedSender<String>> = OnceLock::new();

/// Lazily spawn the cleanup worker once per process on its own OS thread, with
/// its own runtime + pool built from `database_url`. Idempotent.
pub fn ensure_cleanup_worker(database_url: &str) {
    CLEANUP_TX.get_or_init(|| {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let url = database_url.to_string();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("cleanup worker runtime");
            rt.block_on(async move {
                let pool = match sqlx::PgPoolOptions::new()
                    .max_connections(4)
                    .connect(&url)
                    .await
                {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("test-db cleanup worker: connect failed: {e}");
                        return;
                    }
                };
                while let Some(name) = rx.recv().await {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        if let Err(e) = db::drop_database(&pool, &name).await {
                            eprintln!("test-db cleanup: drop {name} failed: {e}");
                        }
                    });
                }
            });
        });
        tx
    });
}
```

> Use the same `db::` import path `fresh_db` uses for `db::create_database`. If `db::drop_database` / `db::database_exists` aren't `pub`, bump them in `server/src/db.rs` (only `src/` change allowed). `sqlx::PgPoolOptions` may need `use sqlx::PgPoolOptions;` — add it. `tokio::sync::mpsc` and `OnceLock` imports as shown. Mark `TestDb` / `ensure_cleanup_worker` with `#[allow(dead_code)]` only if clippy flags them unused this task (Task 2 removes the allows once `fresh_db` returns `TestDb`).

- [ ] **Step 4: Run the verification test to confirm it passes**

Run: `cd server && cargo test --test db_cleanup_test -- --nocapture`
Expected: PASS — the schema/registry row is gone within ~5s of `drop(TestDb(..))`, proving the separate-runtime worker mechanism.

- [ ] **Step 5: Confirm the rest of the suite still compiles + passes (fresh_db unchanged)**

Run: `cd server && cargo test --no-run` (compiles all binaries) — then `make checkall`
Expected: green. `TestDb` is unused by the suite yet (only by the new test); gate stays green.

- [ ] **Step 6: Commit**

```bash
git add server/tests/common/mod.rs server/tests/db_cleanup_test.rs server/src/db.rs
git commit -m "test: TestDb newtype + dedicated cleanup worker (RAII teardown scaffolding)"
```

---

## Task 2: point `fresh_db` at `TestDb` + migrate all call sites

**Files:**
- Modify: `server/tests/common/mod.rs` (`fresh_db` returns `TestDb`; remove `#[allow(dead_code)]` from Task 1)
- Modify: every `server/tests/*.rs` with a `fresh_db` call site (~236 sites) — compiler-guided
- Audit: any other `create_database(` call site in `server/tests/` (route through `TestDb` too)

**Interfaces:**
- Consumes: `TestDb`, `ensure_cleanup_worker` (Task 1).
- Produces: `fresh_db(state) -> TestDb`; migrated call sites; no-accumulation behavior.

- [ ] **Step 1: Change `fresh_db` to return `TestDb`** (`server/tests/common/mod.rs`)

```rust
pub async fn fresh_db(state: &Arc<AppState>) -> TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name)
        .await
        .expect("create fresh database");

    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema fixture");
    ddl::push_schema(&state.pool, &name, schema)
        .await
        .expect("push kanban schema");

    ensure_cleanup_worker(&state.config.database_url);
    TestDb(name)
}
```
Remove any `#[allow(dead_code)]` added in Task 1.

- [ ] **Step 2: Build the test suite and fix every compile error (compiler-guided)**

Run: `cd server && cargo test --no-run 2>&1 | tee /tmp/build.log`
Most sites are `let db = fresh_db(&state).await;` then `&db` / `format!("{db}")` — these compile unchanged via `Deref`/`Display`. Fix the rest per pattern:
- Needs owned `String`: `db.to_string()` / `String::from(db)` / `db.into()`.
- `name.clone()` expecting `String`: `name.to_string()` (since `TestDb::clone` yields `TestDb`).
- `db` passed by value where `String` is expected: `.into()` / `.to_string()`.
- Needs `&str` explicitly: `&*db` or `db.as_ref()`.

Iterate `cargo test --no-run` until it compiles. Per-binary (`cargo test --test <name> --no-run`) localizes errors if the full build is noisy. Variants `db_a`, `db_b`, `db_name`, `name`, `name_a/b`, `source_db`, `target_db` are all `TestDb` now.

- [ ] **Step 3: Audit for other leak sources**

Run: `grep -rn "create_database(" server/tests/`
Any call site NOT going through `fresh_db` (e.g. a local helper in `scheduled_test.rs`) that creates a `t<uuid>` db must also return a `TestDb` (or wrap its result in `TestDb`) so it's cleaned up. Route them.

- [ ] **Step 4: Run the full server test suite**

Run: `cd server && cargo test`
Expected: green. If a test now fails, it's almost certainly a `String`/`TestDb` conversion that changed a db name mid-test — re-check Step 2 fixes.

- [ ] **Step 5: The no-accumulation check (the real gate)**

```bash
make dev-db-clean
# baseline
PGPASSWORD=rtdb psql -h 127.0.0.1 -p 55434 -U rtdb -d rtdb -tAc \
  "SELECT count(*) FROM information_schema.schemata WHERE schema_name ~ '^db_t[0-9a-f]{32}$'"
# run one binary
cd server && cargo test --test admin_test >/dev/null 2>&1 ; cd ..
# after — must be ~0, NOT one-per-test (admin_test has ~40 tests)
PGPASSWORD=rtdb psql -h 127.0.0.1 -p 55434 -U rtdb -d rtdb -tAc \
  "SELECT count(*) FROM information_schema.schemata WHERE schema_name ~ '^db_t[0-9a-f]{32}$'"
```
Expected: the "after" count is a small bounded tail (≤ a handful), NOT ~40. Then run the full `cargo test` and re-count — still bounded. This proves the leak stopped. (A handful may remain from process-exit cuts; that's the valve's job.)

- [ ] **Step 6: Full repo gate**

Run: `make checkall`
Expected: green.

- [ ] **Step 7: Commit**

```bash
git add server/tests/ server/src/db.rs
git commit -m "test: fresh_db returns TestDb — auto-cleanup stops the dev-db leak"
```

---

## Self-Review

- **Spec coverage:** ✅ `TestDb` newtype (Task 1), dedicated worker with own runtime+pool (Task 1, verification test), `fresh_db` change (Task 2), compiler-guided migration (Task 2), audit other `create_database` (Task 2 Step 3), no-accumulation gate (Task 2 Step 5), valve retained (Global Constraints).
- **Placeholder scan:** the migration step enumerates concrete fix patterns and delegates site enumeration to the compiler (not a TODO — the compiler is the authoritative list). Verification code is real.
- **Type consistency:** `TestDb` signature identical between Task 1 and Task 2; `ensure_cleanup_worker(&state.config.database_url)` used in both `fresh_db` and the verification test.
