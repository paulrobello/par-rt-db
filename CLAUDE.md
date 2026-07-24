# CLAUDE.md

Guidance for Claude Code (claude.ai/code) working in this repository.

## What this is

par-rt-db is a self-hosted, Convex-inspired realtime document database in Rust (axum/tokio + Postgres 17). Clients send a **declarative JSON DSL** — typed queries and atomic multi-step transactions — over WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query updates on change. One instance hosts many named databases. There is **no embedded JS runtime** and **no per-app server code** — one generic server serves every app.

Authoritative design: the spec (`docs/superpowers/specs/2026-07-21-par-rt-db-design.md`) and server plan (`docs/superpowers/plans/2026-07-21-server.md`). Read the spec before changing protocol or semantics.

## Workspace & commands

Three packages run from the root `Makefile`: Rust **server** (`server/`, cargo), **TypeScript client SDK** (`ts-client/`, npm `@par-rt-db/client`, bun), and **Rust client crate** (`rust-client/`, `par-rt-db-client`, cargo). Run `cargo` from `server/` or `rust-client/`, `bun` from `ts-client/`.

- `make checkall` — the full gate (fmt-check + clippy `-D warnings` + typecheck + tests). **Definition of done; must pass before commit.**
- `make dev-db-up` / `dev-db-down` — start/stop the dev Postgres on `127.0.0.1:55434`. **Required for any test run** — integration tests hit a real DB.
- `make test` — `dev-db-up` then the whole suite. First-time TS setup: `make ts-client-install`.
- Single test: `cargo test --test txn_test upsert_multiple_matches` (one integration binary), `cargo test upsert` (by name across binaries; binaries mirror the modules — `txn_test`, `query_test`, `subs_test`, …), or `cd ts-client && bunx vitest run tests/<file>.test.ts`.
- Live-server tests are opt-in: `ts-client/tests/integration/**` and `rust-client/tests/http_integration.rs` (`#[ignore]`, needs `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`, run with `--ignored`). The rust-client one needs no dev Postgres.

Tests share one Postgres, isolating by creating uniquely-named databases per test (`t<uuid>`). Never assume exclusive access, and never drop a database or schema you didn't create.

## Architecture — what spans files

- **Committer is the correctness core** (`committer.rs`, `subs.rs`): each database has one task that serializes all writes, then — before dequeuing the next message — re-runs affected subscriptions, diffs against the last pushed value, and pushes only on change. Subscription registration rides the same queue. **This serialization is load-bearing**: `execute_txn`/`execute_query` run READ COMMITTED with no row locking. Never call `execute_txn` outside the committer; never add a second writer.
- **Scheduler is a second per-db task, not a second writer** (`scheduler.rs`): each database also gets a timer task (`run_scheduler`, spawned alongside the committer in `Committers::channel_for`) that drains a `scheduled_txns` side table of `(due_at, txn)` rows. It writes ONLY that side table (claim/reset) and enqueues each due job as a `CommitterRequest::RunScheduled`; the committer's `RunScheduled` arm (`handle_scheduled`) executes it via the normal `execute_txn` + `subs.fan_out` path and finalizes the row. The single-writer invariant is intact — never execute scheduled txns from the scheduler task directly. Delivery is at-least-once; one-shot catches up if past due, cron skips missed windows.
- **Data pipeline** (`schema.rs` → `ddl.rs` → `txn.rs`/`query.rs`): a pushed schema compiles to Postgres DDL — one typed column per indexed field, documents stored as `doc` jsonb with system fields merged in at read time, schema changes additive-only. The read and write paths share index-value typing; keep them aligned.
- **Two transports, one vocabulary** (`protocol.rs`, `ws.rs`, `http_api.rs`): `ws.rs` is the reactive WebSocket handler, `http_api.rs` is one-shot query/mutate. **Both route mutations through `Committers::mutate`** so subscriptions fire regardless of which transport wrote.
- **Auth** (`auth/`): per-database machine tokens and GitHub OAuth sessions. The WS handler **re-runs `authorize` on every Subscribe and Mutate** — not just at connect — so revocation, allowlist changes, and session expiry take effect on open connections. On top of the db-level gate, a table may declare an opt-in `ownerField` for **per-row authorization** (`schema.rs`, enforced in `query.rs`/`txn.rs`/`subs.rs`): an authenticated user reads/mutates only rows they own (inserts are server-stamped; `patch`/`replace`/`delete`/`upsert`-update pre-check ownership inside the serialized txn → `Forbidden`/403; subscriptions re-filter to the subscriber's owner on every `fan_out`). Machine tokens and scheduled jobs (no interactive principal) bypass per-row rules; the db-level allowlist/token/session gate still runs first. See FEATURE_MATRIX #20.
- **File storage is HTTP-only and bypasses the committer** (`storage.rs`, `http_api.rs`): per-db `bytea` blobs in a `storage` side table + a global `rtdb.storage_index(id → db)` for opaque public-serve resolution. Upload (`POST /api/storage/{db}`) and the authed routes carry `{db}` in the path (raw bodies can't carry it; session principals aren't db-scoped) and reuse the `bearer → resolve_bearer → authorize` triple. **`GET /storage/{id}` is the one unauthenticated route** (public bearer URL, Convex parity). Storage writes via `storage::put` directly — never the committer — because blobs don't touch document tables or subscriptions.
- **Three clients, one wire contract**: `server/src/protocol.rs`, `ts-client/src/protocol.ts`, and `rust-client/src/wire.rs` are three implementations of the same protocol and must stay byte-identical (serde tags and field names). The casing is deliberately non-uniform and load-bearing — match the protocol files exactly (see the spec). Both SDKs are no-codegen: a schema object is both pushed to the server and the source of inferred types. The Rust client ports the TS SDK (design at `docs/superpowers/specs/2026-07-22-rust-client-design.md`); its `http`, reactive `ws`, and `admin` features all ship, plus index/`mutate_with_retry` helpers and `.filter()`/`.search()` builders. `FEATURE_MATRIX.md` tracks parity vs. Convex.

## Invariants you must preserve

- **SQL construction**: validate and double-quote every identifier; bind every value via `$n`. Never interpolate an unvalidated value. Physical names are lowercased and length-capped to fit Postgres's 63-byte limit (see `ddl.rs`) — don't raise the caps.
- **Errors**: every failure is the `RtDbError` envelope `{code, message}` (codes/statuses in `error.rs`). Client-facing 500s carry a **generic** message — never stringify a sqlx/serde error into the body (log it via `tracing`). Use `fetch_optional` for any lookup that can legitimately miss.
- **Clients mirror the core**: the server is the source of truth for the protocol, DSL, step-result shapes, and behavior. Any server change must be mirrored in **both** clients — wire types, DSL builders, and their tests. If the Rust client doesn't yet cover a changed surface, file the gap explicitly rather than letting it drift.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- **Keep docs in sync**: when a feature lands or changes, update `FEATURE_MATRIX.md` (the Convex-parity contract — flip rows ❌→✅ and note client-mirror status), the relevant README(s)/docs, and any skill that documents par-rt-db's surface. A stale doc that contradicts the code is a bug.

## Deployment

Deployed live at `rtdb.pardev.net` on host lenny2 (plain `docker compose`, via a Cloudflare tunnel). **Build on the x86_64 host, not from an arm64 Mac.** Secrets come from a mode-600 `.env` (`.env.example` is the template). Full runbook: `deploy/README.md`.
