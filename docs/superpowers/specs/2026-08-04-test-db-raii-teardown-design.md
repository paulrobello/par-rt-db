# Test-database RAII teardown — stop the dev-DB leak at the source (backlog 019fce5b)

- **Date:** 2026-08-04
- **Backlog card:** `019fce5b` (kanban)
- **Status:** Design approved (Approach A) → implementation plan pending
- **Related:** ENH-002 follow-up; `make dev-db-clean` valve (`scripts/dev-db-clean.sql`) stays as the safety net for the residual tail.

## Problem

`server/tests/common::fresh_db` creates a par-rt-db database (a Postgres schema `db_t<uuid-v7>`) per test and pushes the kanban schema fixture, then returns the name as a `String`. **Nothing drops it.** ~236 call sites do `let db = fresh_db(&state).await;`. Over time the dev `rtdb` DB accumulates leaked schemas (51k observed 2026-08-04, enough to OOM `pg_dump`). The interim `make dev-db-clean` valve clears them, but the durable fix is to stop the leak at the source.

## Goal

Each test's database is dropped automatically when its name goes out of scope, so a full `cargo test` run leaves no (or only a tiny, bounded) accumulation of leaked schemas — without requiring test authors to remember any teardown call.

## The crux

`db::drop_database(pool, name)` is `async` (a transaction: validate, `DROP SCHEMA … CASCADE`, delete registry/token/allowlist/storage_index rows). Rust's `Drop::drop` is **sync**. All 474 `#[tokio::test]` in this repo run on **current-thread** runtimes (0 multi-thread), which shut down the instant the test fn returns. Therefore:

- `Drop` cannot `.await` `drop_database`.
- `tokio::spawn(cleanup)` from `Drop` is cancelled when the test's runtime stops — unreliable.

## Design (Approach A) — `TestDb` newtype + a dedicated cleanup worker

### `TestDb` newtype (`server/tests/common/mod.rs`)

`fresh_db` returns `TestDb` instead of `String`:

```rust
pub struct TestDb(String);
```

Trait impls so existing call sites compile with minimal change (the compiler guides the rest):
- `Deref<Target = str>` — `&db` deref-coerces to `&str` (the dominant usage).
- `AsRef<str>` and a `Display` impl — `format!("{db}")`, `db.as_ref()`.
- `Clone` (derived) — `db.clone()` yields a `TestDb`.
- `From<TestDb> for String` — `String::from(db)` / `db.into()` for the sites that need an owned `String`.
- `impl Drop for TestDb` — schedules cleanup (below).

### Dedicated cleanup worker (one per process)

A process-global lazy worker, independent of any test's runtime:

```rust
static CLEANUP_TX: std::sync::OnceLock<tokio::sync::mpsc::UnboundedSender<String>> = OnceLock::new();

fn ensure_cleanup_worker(database_url: &str) {
    CLEANUP_TX.get_or_init(|| {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let url = database_url.to_string();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("cleanup worker runtime");
            rt.block_on(async move {
                let pool = sqlx::PgPoolOptions::new()
                    .max_connections(4)
                    .connect(&url)
                    .await
                    .expect("cleanup worker pool");
                while let Some(name) = rx.recv().await {
                    // Concurrent, best-effort; the pool caps parallelism to 4.
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        if let Err(e) = crate::db::drop_database(&pool, &name).await {
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

- `fresh_db` calls `ensure_cleanup_worker(&state.config.database_url)` (no-op after the first call) before returning.
- `TestDb::drop` does `if let Some(tx) = CLEANUP_TX.get() { let _ = tx.send(std::mem::take(&mut self.0)); }`.
- The worker thread owns its **own** `tokio::runtime::Runtime` and its **own** `PgPool` built from `database_url` — fully independent of the test's current-thread runtime, so cleanup proceeds even after the test/runtime shuts down. Reusing `db::drop_database` keeps teardown identical to real deletion (schema + registry + tokens + allowlist + storage_index) — DRY.

### Why this solves the crux

The worker lives on a separate OS thread driving its own runtime + pool. When a test fn returns and its `TestDb` locals drop, each `Drop` enqueues the name on an unbounded channel (non-blocking, never awaits). The worker drains the channel and runs `drop_database` concurrently (pool cap 4) regardless of the test runtime's lifetime. No `await` in `Drop`; no dependence on the test runtime surviving.

### Bounded residual tail

A detached OS thread can be cut short when the test **binary** exits. In practice tests finish staggered and the worker drains continuously (≈4 concurrent drops), so the queue stays near-empty. At most a handful of in-flight/queued drops may be interrupted by process exit. That bounded tail is exactly what the existing `make dev-db-clean` valve (and `scripts/dev-db-clean.sql`) remains for — it is **not** removed by this work.

## `fresh_db` change

```rust
pub async fn fresh_db(state: &Arc<AppState>) -> TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &name).await.expect("create fresh database");
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema fixture");
    ddl::push_schema(&state.pool, &name, schema).await.expect("push kanban schema");
    ensure_cleanup_worker(&state.config.database_url);
    TestDb(name)
}
```

## Call-site migration (~236 sites, compiler-guided)

Most sites are `let db = fresh_db(&state).await;` then `&db` / `format!("{db}")` — these compile unchanged via `Deref`/`Display`. The compiler flags the rest; each fix is one of:

- Needs an owned `String`: `db.to_string()` / `String::from(db)` / `db.into()`.
- `name.clone()` expecting `String`: `name.to_string()` (since `TestDb::clone` yields `TestDb`).
- `db` passed by value where `String` is expected: `.into()` / `.to_string()`.
- Direct `&str` method on the value: use `&*db` or `db.as_ref()`.

Variants `db_a`, `db_b`, `db_name`, `name`, `name_a`, `name_b`, `source_db`, `target_db` all become `TestDb` the same way. Bindings shadowed or reassigned to a `String` later are fixed at the reassignment.

## Audit for other leak sources

Grep `server/tests/` for any **other** `db::create_database(` / `create_database(` call sites (e.g. local helpers like the one referenced in `scheduled_test.rs`) that bypass `fresh_db`, and route them through `TestDb` + the worker too (or confirm they're negligible). Only `common::fresh_db`-routed creation should remain.

## Testing / verification

1. **Newtype unit tests** (pure): `Deref`/`AsRef`/`Display`/`From<TestDb> for String`/`Clone` behave as expected; `Drop` enqueues the name when a worker is initialized (assert via a test-only hook or by checking the schema is gone after a `tokio::task::yield_now` + short sleep).
2. **No-accumulation check (the real gate):** with the dev DB clean (`make dev-db-clean`), run a focused test binary, then count `SELECT count(*) … WHERE schema_name ~ '^db_t[0-9a-f]{32}$'`. Before this work the count grows by ~one-per-test; after, it must be **0 (or a small bounded tail)** — not the per-test leak. Then run the full `cargo test` and re-count; assert the leaked count is bounded (e.g. ≤ a small constant), proving accumulation stopped.
3. **Full gate:** `make checkall` green (the migration must not change any test's behavior — only add cleanup).

## Out of scope

- Production (`server/src/`) behavior — untouched; this is test-only (`server/tests/`).
- Removing the `make dev-db-clean` valve — it stays as the safety net for the residual tail.
- Other dev-DB hygiene (orphan `rtdb_auth` sessions/users from manual testing) — not addressed.

## Risks

- **sqlx pool/runtime pinning:** the worker uses its OWN pool on its OWN runtime, so no connection crosses runtimes (the failure mode that would panic). Verified by design; the no-accumulation test confirms end-to-end.
- **Migration regressions:** a botched `String`/`TestDb` conversion could change a test's db name mid-test. Mitigated by compiler-guided edits + the full `cargo test` gate.
- **Worker starvation under heavy parallel test load:** pool cap 4 + concurrent drain is sized for dev; if the tail grows under `cargo test` (parallel binaries), raise the cap. The no-accumulation test catches a regression.
