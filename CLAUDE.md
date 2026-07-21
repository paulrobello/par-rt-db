# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

par-rt-db is a self-hosted, Convex-inspired realtime document database in Rust (axum/tokio + Postgres 17). Clients send a **declarative JSON DSL** — typed queries and atomic multi-step transactions — over WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query updates on change. One instance hosts many named databases. There is **no embedded JS runtime** and **no per-app server code** — one generic server serves every app.

The authoritative design lives in `docs/superpowers/specs/2026-07-21-par-rt-db-design.md` (spec) and `docs/superpowers/plans/2026-07-21-server.md` (implementation plan). Read the spec before changing protocol or semantics.

## Commands

The Rust crate lives in `server/`, not the repo root. The Makefile `cd`s there for you; if you run `cargo` directly, do it from `server/`.

- `make checkall` — the full gate (fmt-check + clippy `-D warnings` + typecheck + tests). Must pass before any commit; this is the project's definition of done.
- `make dev-db-up` — start the dev/test Postgres (loopback `127.0.0.1:55434`). **Required before any test run** — the integration tests hit a real database. `make dev-db-down` to stop it.
- `make test` — runs `dev-db-up` then the whole suite.
- Single test: `cd server && cargo test --test txn_test upsert_multiple_matches` (a test in a specific integration binary), or `cargo test upsert` to match by name across binaries. Integration binaries mirror the modules: `txn_test`, `query_test`, `subs_test`, `ws_test`, `http_api_test`, `oauth_test`, `admin_test`, `healthz_test`.
- `make lint` / `make fmt` — clippy / rustfmt.

Tests connect via `RTDB_TEST_DATABASE_URL` (defaults to the dev-db URL). They **share one Postgres instance and isolate by creating uniquely-named databases per test** (`t<uuid>`) — never assume exclusive access, and never drop a database or schema you didn't create.

## Architecture — the parts that span files

### The committer is the correctness core (`committer.rs`, `subs.rs`)
Every database has one **committer task** that serializes all of its writes. A mutation runs `execute_txn`, then — before the committer dequeues the next message — re-runs every active subscription whose query touches a written table, diffs the result against the last pushed value (via `query::canonical`), and pushes `queryUpdate` only on change. Subscription registration is also serialized through this task, so no update can slip between a subscribe's initial query and its registration.

**This serialization is load-bearing:** `execute_txn` and `execute_query` run under READ COMMITTED with **no row locking**. Their correctness depends on all writes for a database flowing through the committer. Never call `execute_txn` from a non-committer path, and never add a second concurrent writer.

### The data pipeline: schema → DDL → txn/query
- `schema.rs` — the `SchemaDef`/`FieldType` type model, structural validation, and document/value validation. The largest and most central file.
- `ddl.rs` — compiles a pushed schema to Postgres DDL. Each user table becomes `"db_<name>"."t_<table>"(id text pk, doc jsonb, created_at bigint, version bigint, f_<field> <typed column> …)` — **one real typed column per indexed field**, and a btree index per declared index. Schema changes are additive-only; destructive pushes are rejected.
- `txn.rs` (write path) and `query.rs` (read path) execute against that layout. **Documents are stored as `doc` jsonb without system fields**; `_id`, `_creationTime`, and `_version` are merged in at read time. Indexed columns are recomputed from the doc on every write. `query.rs` reuses `txn.rs`'s `eq_binds`/`EqBind` for index-value typing — the read and write paths must not diverge on how eq values map to SQL types.

### Two transports, one vocabulary (`protocol.rs`, `ws.rs`, `http_api.rs`)
`protocol.rs` defines the wire messages (serde `camelCase` tags and field names — e.g. `queryUpdate`, `mutId`; changing these is a breaking wire change). `ws.rs` is the reactive WebSocket handler; `http_api.rs` is one-shot query/mutate. **Both route mutations through `Committers::mutate`** so subscriptions fire regardless of which transport wrote.

### Auth (`auth/`)
Two schemes resolved by `resolve_bearer`: hashed per-database **machine tokens** (`auth/tokens.rs`) and **GitHub OAuth sessions** (`auth/github.rs`, `auth/session.rs`). `authorize` enforces a per-database email allowlist for users and db-match + live revocation for machines. The WS handler **re-runs `authorize` on every Subscribe and Mutate**, not just at connect, so revocation/allowlist changes take effect on open connections (session *expiry* mid-connection is a known deferred gap). Tokens are stored only as SHA-256 digests; the admin key is compared constant-time.

## Invariants you must preserve

- **SQL construction**: every identifier reaching SQL is validated (identifier/db-name regex) **and** double-quoted; every value goes through a `$n` bind. Never interpolate an unvalidated value into SQL. Physical names are always lowercased: schema `db_<name>`, table `t_<table>`, column `f_<field>`, index `i_<table>_<index>`. Identifier length caps (tables/indexes ≤30, fields ≤60 chars) exist to keep prefixed names within Postgres's 63-byte limit — don't raise them.
- **Errors**: every failure is the `RtDbError` envelope `{code, message}` with codes `UNAUTHORIZED, FORBIDDEN, NOT_FOUND, SCHEMA_VIOLATION, PRECONDITION_FAILED, BAD_REQUEST, INTERNAL` (statuses 401/403/404/422/409/400/500). Client-facing 500s must carry a **generic** message — never stringify a sqlx/serde error into the body (log it via `tracing` instead).
- `fetch_optional` for any lookup that can legitimately miss; a bare sqlx `RowNotFound` must not surface as a 500.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.

## Deployment

Deployed live at `rtdb.pardev.net` on host lenny2 (plain `docker compose`, not Swarm) reached through a Cloudflare tunnel. Production stack is `Dockerfile` + `docker-compose.yml`; **build on the x86_64 host, not from an arm64 Mac**. Secrets come from a mode-600 `.env` (never committed; `.env.example` is the template). Full runbook: `deploy/README.md`.
