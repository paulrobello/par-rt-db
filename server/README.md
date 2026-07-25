# rtdb-server

The **par-rt-db** realtime document database server — a self-hosted,
Convex-inspired document DB in Rust (axum/tokio + Postgres 17). Clients send a
declarative JSON DSL (typed queries and atomic multi-step transactions) over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live
query updates on change. One instance hosts many named databases. There is no
embedded JS runtime and no per-app server code — this one generic binary serves
every app.

This directory holds the `rtdb-server` binary. The three client SDKs live
alongside it: [`../ts-client/`](../ts-client) (browser/Node),
[`../rust-client/`](../rust-client) (Rust), and
[`../python-client/`](../python-client) (Python — wire + DSL today; HTTP/WS/admin
pending). An operator dashboard SPA ([`../dashboard/`](../dashboard)) is served
same-origin by the server when `RTDB_STATIC_DIR` is set. See the
[root README](../README.md) for the project overview and
[`../CLAUDE.md`](../CLAUDE.md) for contributor guidance. Authoritative design:
[`../docs/superpowers/specs/2026-07-21-par-rt-db-design.md`](../docs/superpowers/specs/2026-07-21-par-rt-db-design.md).

## Stack

- **axum 0.8** + **tokio** — HTTP and WebSocket transports, graceful shutdown.
- **sqlx 0.8** + **Postgres 17** — storage: one typed column per indexed field,
  documents stored as `doc jsonb` (system fields merged in at read time).
- **tracing** — structured logs.
- Auth: multi-provider OAuth trait (`auth/provider.rs`) with GitHub
  (`auth/github.rs`) and Google (`auth/google.rs`) providers — cross-provider
  same-email logins link to one user by email — plus hashed per-database machine
  tokens (`auth/tokens.rs`); the admin key is compared constant-time.
- Per-row authorization: a table may declare an opt-in `ownerField`, after which
  an authenticated user reads/mutates only rows they own (enforced server-side
  on query, mutate, and subscription re-run; machine tokens and scheduled jobs
  bypass it). See FEATURE_MATRIX #20.

## Layout

| Area | Files |
| --- | --- |
| Correctness core (serialized writes + subscription fan-out) | `src/committer.rs`, `src/subs.rs` |
| Scheduled / cron transactions | `src/scheduler.rs` (+ the `RunScheduled` arm in `src/committer.rs`) |
| File storage (blobs) | `src/storage.rs` (+ the storage routes in `src/http_api.rs`) |
| Schema model + validation | `src/schema.rs` |
| Schema → Postgres DDL | `src/ddl.rs` |
| Write / read paths | `src/txn.rs`, `src/query.rs` |
| Wire messages | `src/protocol.rs` |
| Transports | `src/ws.rs` (reactive), `src/http_api.rs` (one-shot) |
| Auth | `src/auth/` (`tokens.rs`, `provider.rs`, `github.rs`, `google.rs`, `session.rs`) |

The read path compiles a db-side `filter()` predicate DSL to SQL, a full-text
`search` query terminal backed by a generated tsvector column + GIN index,
ranked by `ts_rank`, and a `vectorSearch` terminal backed by pgvector — a
write-maintained `vector(N)` column + HNSW `vector_cosine_ops` index per vector
index, ranked by cosine distance (`<=>`) with an optional eq-`filter` over
declared `filterFields`. Embeddings are client-supplied (no server-side
generation); the Postgres image is `pgvector/pgvector:pg17`.

## Scheduling

Per-database scheduled/cron transactions live in `src/scheduler.rs`. Each
database gets a `scheduled_txns` side table (sibling of `mutations`) holding
`(due_at, txn)` rows of declarative `Transaction`s — not code — created in
`db::create_database` and lazily by `scheduler::ensure_table`. A per-db timer
task (`scheduler::run_scheduler`, spawned alongside the committer in
`Committers::channel_for`) claims due rows and enqueues each as a
`CommitterRequest::RunScheduled`; the committer's `RunScheduled` arm
(`handle_scheduled`) executes it via the normal `execute_txn` + `subs.fan_out`
path and finalizes the row, so the single-writer invariant is intact. The
scheduler task only writes the side table (claim/reset) — it never executes
transactions itself.

`when` is one-shot (`afterMs`/`runAt`) or a 5-field cron expression (UTC,
min-first, via the `croner` crate). Delivery is **at-least-once**: a crash
between commit and finalize is recovered by `reset_running` on startup, so apps
should write idempotent scheduled txns. A past-due one-shot fires immediately
(catch-up); cron **skips** missed windows with no backfill. WS surface:
`Schedule` / `CancelSchedule` / `PauseSchedule` / `ResumeSchedule` /
`ListSchedules`. HTTP surface: `POST /api/schedule`,
`POST /api/schedule/{id}/{cancel,pause,resume}`, `POST /api/schedules`.

## File storage

Per-database blob storage lives in `src/storage.rs`. Each database gets a
`storage` side table (`bytea`, TOAST-managed) holding each file's bytes plus
`{sha256, size, content_type, created_at}`; a global `rtdb.storage_index(id →
db_name)` resolves the unauthenticated public serve URL to the owning database.

Bytes are written via `storage::put` **directly, not through the committer** —
blobs don't touch document tables or subscriptions, so the single-writer
invariant is unaffected. Storage is not reactive, so there are **no WS
variants** — the surface is HTTP only:

- `POST /api/storage/{db}` — bearer, raw body, `Content-Type` → `{ id, sha256, size, contentType }`. The route disables axum's 2 MiB default and enforces `RTDB_MAX_FILE_SIZE` (default 50 MiB); overflow is `BadRequest`.
- `GET /storage/{id}` — **unauthenticated** public serve (anyone with the opaque uuid-v7 URL; revoke by delete). Convex parity.
- `GET /api/storage/{db}/{id}` — authed serve (caller-db-scoped).
- `DELETE /api/storage/{db}/{id}` — bearer → `{ ok: true }` (idempotent; revokes the public URL).
- `GET /api/storage/{db}/{id}/metadata` — bearer → `{ id, sha256, size, contentType?, creationTime }`.

`{db}` is in the path because the raw upload/serve bodies can't carry it and
session principals aren't db-scoped. The table is created eagerly in
`db::create_database` and lazily via `storage::ensure_table` at committer
startup (mirrors `mutations`/`scheduled_txns`).

## Develop

```sh
make dev-db-up        # start dev Postgres on 127.0.0.1:55434 (required for tests)
make test             # dev-db-up, then cargo test
make checkall         # fmt-check + clippy -D warnings + typecheck + test
```

Run cargo directly from this directory: `cargo build`, `cargo clippy --all-targets
--all-features -- -D warnings`, or a single test by name
(`cargo test --test txn_test upsert_multiple_matches`). Integration test
binaries mirror the modules: `txn_test`, `query_test`, `subs_test`, `ws_test`,
`http_api_test`, `oauth_test`, `admin_test`, `healthz_test`.

Tests share one Postgres instance and isolate by creating uniquely-named
databases (`t<uuid>`) — never assume exclusive access, and never drop a database
or schema you didn't create.

## Invariants

The committer is the correctness core: every write for a database flows through
one serialized committer task, and subscriptions are re-evaluated under that same
serialization. `execute_txn`/`execute_query` run under READ COMMITTED with no row
locking — never call `execute_txn` outside the committer, and never add a second
concurrent writer. Every SQL identifier is validated and double-quoted, every
value goes through a `$n` bind, and every failure is the `RtDbError` envelope
`{code, message}` (500s carry a generic message). No `unwrap()`/`expect()`
outside `#[cfg(test)]`. Full list in [`../CLAUDE.md`](../CLAUDE.md).

## Deploy

Deployed live at `rtdb.pardev.net` on host lenny2 (plain `docker compose`)
reached through a Cloudflare tunnel. Production stack and runbook:
[`../Dockerfile`](../Dockerfile), [`../docker-compose.yml`](../docker-compose.yml),
[`../deploy/README.md`](../deploy/README.md). Build on the x86_64 host, not from
an arm64 Mac.
