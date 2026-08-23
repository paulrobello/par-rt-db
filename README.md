# PAR RT DB

## Table of Contents

* [About](#about)
* [Features](#features)
   * [Core Capabilities](#core-capabilities)
   * [Advanced Features](#advanced-features)
   * [Technical Excellence](#technical-excellence)
* [Packages](#packages)
* [How it works](#how-it-works)
* [Prerequisites for running](#prerequisites-for-running)
* [Prerequisites for dev](#prerequisites-for-dev)
* [Quickstart](#quickstart)
* [Endpoints](#endpoints)
   * [Health, realtime, queries, mutations, scheduling](#health-realtime-queries-mutations-scheduling)
   * [File storage (HTTP-only, bypasses the committer)](#file-storage-http-only-bypasses-the-committer)
   * [Admin: databases, schema, tokens, allowlist](#admin-databases-schema-tokens-allowlist)
   * [Admin: dashboard operator surface](#admin-dashboard-operator-surface)
   * [Auth (OAuth + sessions)](#auth-oauth--sessions)
* [Configuration](#configuration)
* [Error envelope](#error-envelope)
* [Wire protocol](#wire-protocol)
* [Pagination](#pagination)
* [Scheduling](#scheduling)
* [Durable workflows](#durable-workflows)
* [Computed fields](#computed-fields)
* [Realtime presence](#realtime-presence)
* [Make targets](#make-targets)
* [Graceful shutdown](#graceful-shutdown)
* [Known MVP limitations](#known-mvp-limitations)
* [Clients](#clients)
* [FAQ](#faq)
* [Roadmap](#roadmap)
   * [Where we are](#where-we-are)
   * [Where we're going](#where-were-going)
* [What's new](#whats-new)
* [Contributing](#contributing)
* [License](#license)

[![CI](https://github.com/paulrobello/par-rt-db/actions/workflows/ci.yml/badge.svg)](https://github.com/paulrobello/par-rt-db/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)  
![Runs on Linux | MacOS](https://img.shields.io/badge/runs%20on-Linux%20%7C%20MacOS-blue)
![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange)
![Postgres 17](https://img.shields.io/badge/Postgres-17-blue)
![Deploy: docker compose](https://img.shields.io/badge/deploy-docker%20compose-blue)

## About
PAR RT DB is a self-hosted, Convex-inspired realtime document database. Clients send a
**declarative JSON DSL** — typed queries and atomic multi-step transactions — over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query
updates on change. There is no embedded JS runtime and no per-app server code — one
generic server hosts many named databases for every app. Built in Rust on axum/tokio
with Postgres 17 storage. Authoritative design:
[`docs/superpowers/specs`](docs/superpowers/specs).

Related documentation: [`CHANGELOG.md`](CHANGELOG.md), [`DESIGN.md`](DESIGN.md),
[`PRODUCT.md`](PRODUCT.md), [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md),
[`deploy/README.md`](deploy/README.md) (production runbook),
[`docs/README.md`](docs/README.md) (docs index),
[`CONTRIBUTING.md`](CONTRIBUTING.md).

[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://buymeacoffee.com/probello3)

## Features

### Core Capabilities
- **Declarative JSON DSL**: typed queries and atomic multi-step transactions — no embedded JS runtime, no per-app server code
- **Realtime live queries**: every subscribed query is re-evaluated after each committed transaction and pushed only when its serialized result changes
- **Two transports, one vocabulary**: mutations route through the same committer path over WebSocket (`/sync`) or one-shot HTTP, so subscriptions fire regardless of which transport wrote
- **Many databases per instance**: one generic server hosts many named databases for every app
- **Typed schemas**: pushed schemas compile to additive Postgres DDL — one typed column per indexed field plus the `doc` jsonb body
- **First-class SDKs**: TypeScript (with React bindings), Rust, Python, and Swift (iOS/macOS) clients that mirror the wire contract directly, plus the `rtdb` CLI and an operator dashboard

### Advanced Features
- **Full-text search**: websearch-syntax `search` ranked by `ts_rank` with optional snippets, plus a `trgm` mode for substring/autocomplete matching
- **Vector + hybrid search**: write-maintained pgvector columns ranked by the index's declared metric; `hybridSearch` fuses full-text and vector rankings via Reciprocal Rank Fusion
- **Scheduling**: one-shot (`afterMs`/`runAt`), 5-field UTC `cron`, and fixed-interval (`everyMs`) transactions with cancel/pause/resume — scheduled work is data, not server code
- **Durable workflows**: multi-step specs with per-step retry, backoff, and sleep, plus `awaitSignal` approval gates that park a run until an out-of-band signal (or timeout); at-least-once per step with crash-resume
- **Server-stamped `updatedAt`**: a table may declare `updatedAtField` naming a `number`/`int64` field the server stamps with epoch-ms on every version-bumping write (insert/patch/replace/upsert/patchByQuery/cascade setNull), overwriting client-supplied values — no more hand-rolled timestamps in every mutation, and orderable with a declared index
- **Auto-increment counters**: a table may declare `autoIncrementField` naming an `int64` field the server assigns from a per-table Postgres sequence on insert (overwriting client-supplied values, immutable afterward, unique-indexable) — ticket/issue numbers with zero races; snapshot import continues numbering past the imported max, and gaps from rolled-back transactions are documented behavior
- **Computed fields**: a table may declare `computed`, a map of field name → typed expression (`concat`/arithmetic/`coalesce`/`lower`/`upper`/`trim`/casts/`case`/`now`), re-derived by the server on every write and stored in the doc body **and** the typed column — client-supplied values never survive, `null` results leave the key absent, the field stays indexable, and pushes backfill existing rows (see [Computed fields](#computed-fields))
- **Realtime presence**: transient room membership and state ("who is online", cursors, typing) over the existing `/sync` socket — no extra infrastructure
- **File storage**: opaque unauthenticated public URLs, signed time-limited URLs, HTTP `Range` support, and read-time image transforms
- **Auth**: six optional OAuth providers (GitHub, Google, GitLab, Microsoft, Apple, generic OIDC), per-database machine tokens, optional anonymous access with anon→real account merge, and per-row `ownerField`/`authorize` rules
- **Operator surfaces**: op feed, audit log, webhooks, Prometheus `/metrics` with optional OTLP tracing, backup/restore, schema migration with snapshot history and restore, hot-reloaded config, per-database quotas, slow-query ring, and query explain

### Technical Excellence
- **Single serialized committer per database**: all writes flow through one committer task per database and reads run under READ COMMITTED — realtime correctness without distributed coordination
- **Rust on axum/tokio with Postgres 17 storage**: graceful shutdown waits for in-flight requests and open WebSockets before exiting
- **One wire contract, five implementations**: the server and the ts/rust/python clients stay byte-identical, enforced by a shared semantics corpus ([`wire-corpus/`](wire-corpus/README.md)); the Swift client mirrors the same wire types, pinned by the wire-parity corpus
- **Security defaults**: constant-time key comparison, generic client-facing 500 messages (detail only in logs), typed `confirm` guards on destructive operations, path-traversal-guarded downloads

## Packages

| Package | Path | Stack | What it is |
| --- | --- | --- | --- |
| **Server** | [`server/`](server) | Rust (axum/tokio + Postgres 17) | The realtime database binary |
| **TypeScript client** | [`ts-client/`](ts-client) | TS (`@par-rt-db/client`, bun) | Browser/Node/React Native SDK + React bindings + in-memory test harness |
| **Rust client** | [`rust-client/`](rust-client) | Rust (`par-rt-db-client`) | Rust SDK: http + reactive ws + admin + `.filter()`/`.search()`/`.vector_search()` builders |
| **Python client** | [`python-client/`](python-client) | Python (`par-rt-db`, uv) | Python SDK: wire + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS |
| **Swift client** | [`swift-client/`](swift-client) | Swift 6 (`ParRtDbClient`/`ParRtDbUI`, SPM) | iOS 17+/macOS 14 SDK: wire + query/mutation/schema DSL + HTTP + reactive WS + SwiftUI `LiveQuery` (Darwin only) |
| **Dashboard** | [`dashboard/`](dashboard) | Vite + React 19 + TS (bun) | Operator console SPA served same-origin at `RTDB_STATIC_DIR` |
| **`rtdb` CLI** | [`cli/`](cli) | Rust (`rtdb` binary, cargo) | Operator/CI wrapper around `par-rt-db-client`: list/create dbs, push schema, query/mutate, mint/revoke tokens |

The server is the source of truth; the four SDKs (ts/rust/python/swift) each mirror
its wire contract directly, the CLI wraps `rust-client`, and the dashboard SPA
consumes `ts-client`. See [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) for the
Convex-parity contract and [`CLAUDE.md`](CLAUDE.md) for contributor guidance.

## How it works

par-rt-db is built around one load-bearing invariant: **a single serialized committer
task per database**. All writes for a database flow through one committer, which also
re-runs affected subscriptions under the same serialization and pushes only on change.
This keeps realtime correct without distributed coordination.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (WS / HTTP)
    participant H as HTTP / WS handler
    participant Co as Committer (per-db, single writer)
    participant S as Subscription fan-out
    participant DB as Postgres

    rect rgb(30, 30, 30)
    Note over C,DB: Mutate → commit → live push
    C->>H: POST /api/mutate (or WS `mutate`)
    H->>Co: CommitterRequest::Mutate
    Co->>DB: BEGIN + execute_txn (insert/patch/...)
    DB-->>Co: step results
    Co->>S: fan_out(affected tables)
    S->>DB: re-run affected subscriptions (READ COMMITTED)
    S-->>C: queryUpdate (only if result changed)
    Co-->>H: mutateOk + results
    H-->>C: 200 / mutateOk
    end
```

The same path runs for scheduled/cron jobs and durable workflow steps (a per-db
scheduler enqueues due work as `CommitterRequest::RunScheduled` /
`RunWorkflowAdvance`). Reads that don't write go straight to Postgres
through `execute_query` under READ COMMITTED. Auth re-runs `authorize` on every
Subscribe/Mutate over an open WebSocket. File storage is HTTP-only and bypasses the
committer (blobs don't touch document tables). See
[`server/README.md`](server/README.md) for the server layout and
[`CLAUDE.md`](CLAUDE.md) for the full invariant list.

## Prerequisites for running
* Postgres 17 — the repo's `docker-compose.dev.yml` starts one on `127.0.0.1:55434` via `make dev-db-up`, or point `RTDB_DATABASE_URL` at your own
* [Docker](https://www.docker.com/) — for the dev Postgres and for the production `docker compose` deploy path (see [`deploy/README.md`](deploy/README.md))
* A Rust `stable` toolchain to build the server binary from source ([`rust-toolchain.toml`](rust-toolchain.toml) is the single source of truth)
* `jq` for the Quickstart walkthrough
* Per client SDK: bun/node for TypeScript, cargo for Rust, Python 3.12+ (uv) for Python

## Prerequisites for dev
* See [CONTRIBUTING's development setup](CONTRIBUTING.md#development-setup) for the full tool list and first-time setup
* A GNU-compatible `make` — every target spans all seven packages (swift-client's lines are Darwin-guarded and skip loudly on Linux); first-time installs are `make ts-client-install`, `make dashboard-install`, and `make python-client-install` (see [Make targets](#make-targets))

## Quickstart

This walkthrough brings up a local server, creates a database, mints a machine token, and
runs one mutate + one query. It assumes `docker` is available and that the repo is
checked out at the root; it also uses `cargo` and `jq` — see
[CONTRIBUTING's development setup](CONTRIBUTING.md#development-setup) for the full
tool list and first-time setup.

```bash
# 1. Start the dev Postgres (127.0.0.1:55434) used by both the server and the tests.
make dev-db-up

# 2. Configure the minimum env the binary needs.
export RTDB_DATABASE_URL='postgres://rtdb:rtdb@127.0.0.1:55434/rtdb'
export RTDB_ADMIN_KEY="$(openssl rand -hex 32)"
export RTDB_PUBLIC_URL='http://localhost:8300'

# 3. Run the server (listens on RTDB_PORT, default 8300).
cd server && cargo run
```

In another shell, bootstrap a database and exercise the API:

```bash
# 4. Create a database.
curl -s -X POST http://localhost:8300/admin/create-db \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name":"myapp"}'

# 5. Push a minimal schema (additive DDL: typed columns + the table).
curl -s -X POST http://localhost:8300/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","schema":{"tables":{"tasks":{"fields":{"title":{"type":"string"},"done":{"type":"boolean"}}}}}}'

# 6. Mint a machine token scoped to the new database.
TOKEN=$(curl -s -X POST http://localhost:8300/admin/mint-token \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","name":"cli"}' | jq -r .token)

# 7. Insert one task, then read it back.
curl -s -X POST http://localhost:8300/api/mutate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","txn":{"steps":[{"op":"insert","table":"tasks","doc":{"title":"Buy milk","done":false}}]}}'

curl -s -X POST http://localhost:8300/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","query":{"table":"tasks"}}'
```

To run the full test suite instead (dev Postgres must be up):

```bash
make test   # dev-db-up + test suites across all six packages (fmt/clippy/typecheck live in make checkall)
```

## Endpoints

Auth is one of: `none`, `Bearer token` (machine token or session via
`Authorization: Bearer <token>`), `Bearer admin key` (the server-wide
`RTDB_ADMIN_KEY`, or an OAuth session for a user listed in `rtdb_auth.admins`),
`Bearer session` (OAuth session token only), or `first WS frame` (the WS `auth`
message). The dashboard's WS to `/admin/stream` is a browser special-case: the
admin bearer rides in the `Sec-WebSocket-Protocol: rtdb-admin.<token>` subprotocol
since browsers cannot set headers on a WS handshake.

### Health, realtime, queries, mutations, scheduling

| Method & path | Auth | Description |
| --- | --- | --- |
| `GET /healthz` | none | Liveness: `{status:"ok"\|"degraded", version, git_commit, build_timestamp, started_at, uptime_seconds, postgres}`. 503 when Postgres is unreachable. |
| `GET /privacy` | none | par-rt-db's own privacy policy as static HTML (compile-time-embedded from `static/privacy.html`), so an OAuth consent screen's required privacy URL can point at the deployment itself. Public and stateless; works in API-only mode. |
| `GET /metrics` | none | Prometheus text-exposition scrape endpoint. Content-negotiated on `Accept`: a browser (`text/html`) is served the SPA's `index.html` when `RTDB_STATIC_DIR` is set; everything else (Prometheus sends `application/openmetrics-text`, curl, API-only deploys) gets Prometheus text. Aggregate-only (no per-db, no principal data), same posture as `/healthz`. The admin JSON snapshot stays at `GET /admin/metrics`. |
| `GET /sync` | first WS frame | WebSocket upgrade. Speaks the realtime protocol (auth, subscribe, mutate, schedule, ping). |
| `POST /api/query` | Bearer token | One-shot query against a database; see [Query shape](#query-shape). |
| `POST /api/query-batch` | Bearer token | Fans out N queries in one round trip (per-query error isolation); each slot returns `{ok, result}` or `{ok:false, error}`. |
| `POST /api/mutate` | Bearer token | One-shot transaction (`insert`/`patch`/`replace`/`delete`/`undelete`/`expectVersion`/`expectAbsent`/`upsert` + `patchByQuery`/`deleteByQuery` + `schedule`/`cancelSchedule` + `startWorkflow`/`cancelWorkflow` steps). |
| `POST /api/schedule` | Bearer token | Schedules a transaction: `afterMs`/`runAt` one-shot, `cron` (5-field, UTC, min-first), or `interval` (fixed `everyMs`); returns `{id}`. |
| `POST /api/schedule/{id}/{cancel,pause,resume}` | Bearer token | Cancels, pauses, or resumes a scheduled job. |
| `POST /api/schedules` | Bearer token | Lists scheduled jobs for a database (`ScheduleInfo[]`). |
| `POST /api/workflows` | Bearer token | Starts a durable workflow run from a `WorkflowSpec` (FM-29); returns `{id}`. See [Durable workflows](#durable-workflows). |
| `POST /api/workflows/list` | Bearer token | Lists workflow runs for a database (`WorkflowInfo[]`, newest first; optional `status` filter). |
| `POST /api/workflows/{id}/cancel` | Bearer token | Cancels a non-terminal run (`{ok:false}` = unknown/terminal — a no-op, not an error). |
| `POST /api/workflows/{id}/signal` | Bearer token | Delivers a named signal to a `waiting` run (`{db, name, payload?}` → `{"delivered": true}`; releases an `awaitSignal` step). 404 unknown id; 409 not waiting or name mismatch; payload capped at 64 KiB serialized. |

### File storage (HTTP-only, bypasses the committer)

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /api/storage/{db}` | Bearer token | Upload: raw body + `Content-Type` → `{ id, sha256, size, contentType }`. Enforces `RTDB_MAX_FILE_SIZE`. |
| `GET /storage/{id}` | **none** | **Unauthenticated public serve** — anyone with the opaque uuid-v7 URL fetches it (Convex parity; revoke by delete). The single unauthenticated route. |
| `GET /api/storage/{db}/{id}` | Bearer token | Authed, caller-db-scoped serve. |
| `DELETE /api/storage/{db}/{id}` | Bearer token | Deletes a blob (idempotent; revokes the public URL). |
| `GET /api/storage/{db}/{id}/metadata` | Bearer token | `{ id, sha256, size, contentType?, creationTime }`. |
| `GET /api/storage/{db}/{id}/signed-url` | Bearer token | Mints a signed, time-limited public URL (`?exp=` + HMAC `?sig=`) for the blob. The mint is db-scoped: a caller authorized for db A cannot mint for a blob in db B (SEC-113). `?ttlSeconds=` clamps to `[1, MAX_SIGNED_URL_TTL_SECS]`; default `DEFAULT_SIGNED_URL_TTL_SECS`. The unauthenticated `GET /storage/{id}` route verifies `?exp=&sig=` when present (403 on failure) and behaves unchanged when absent (ENH-017). |

Both serve routes honor HTTP `Range` requests: `Range: bytes=...` → `206 Partial Content` with `Content-Range`/`Content-Length` (and `Accept-Ranges: bytes` advertised), `416` for an out-of-bounds range, and a `200` full body when no range is requested. Single-range only; on-the-fly image transforms (`?w=` etc.) skip range handling.

### Admin: databases, schema, tokens, allowlist

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /admin/login` | Bearer admin key | Validates the admin key and mints a short-lived admin session token (used by the dashboard's login form). |
| `POST /admin/logout` | Bearer admin key | Invalidates the admin session token. |
| `POST /admin/create-db` | Bearer admin key | Creates a new database. |
| `POST /admin/delete-db` | Bearer admin key | Deletes a database. Body `{ name, confirm }` where `confirm` must equal the db name (typed guard). Retires the per-db committer/scheduler/reaper tasks cleanly. |
| `POST /admin/clone-db` | Bearer admin key | Clones an existing database's schema into a new db name (no data). |
| `POST /admin/push-schema` | Bearer admin key | Applies additive schema DDL to a database. |
| `POST /admin/db/{db}/schema/preview` | Bearer admin key | Previews the DDL a `push-schema` would run, without applying. |
| `POST /admin/db/{db}/migrate` | Bearer admin key | Runs an ordered `Directive` list (rename/coerce/remove/default-backfill) transactionally inside the committer; live queries, op feed, audit, and webhooks all fire. See [Schema migration](#schema-migration). |
| `GET /admin/dbs` | Bearer admin key | Lists all databases. |
| `GET /admin/dbs/{db}/schema` | Bearer admin key | Returns the pushed schema for a database. |
| `GET /admin/dbs/{db}/stats` | Bearer admin key | Per-table row counts and storage sizes; includes quota usage and caps (ENH-011). |
| `GET /admin/db/{db}/schema/history` | Bearer admin key | Lists schema-change snapshots newest-first (`?limit=`/`?offset=`). See [Schema change history](#schema-change-history). |
| `GET /admin/db/{db}/schema/history/{version}` | Bearer admin key | Returns one full schema snapshot by version. |
| `POST /admin/db/{db}/schema/restore` | Bearer admin key | Reconciles the db to a prior snapshot's shape. Body `{ version, confirm }`, `confirm` = db name. |
| `POST /admin/mint-token` | Bearer admin key | Mints a machine token scoped to one database. |
| `POST /admin/revoke-token` | Bearer admin key | Revokes a machine token by its id. |
| `GET /admin/tokens?db=` | Bearer admin key | Lists machine tokens for a database (no secrets). |
| `GET /admin/allowlist?db=` | Bearer admin key | Lists the emails allowlisted for a database. |
| `POST /admin/allowlist` | Bearer admin key | Adds or removes an email from a database's allowlist. |
| `GET /admin/admins` | Bearer admin key | Lists the server-wide OAuth admin allowlist (`rtdb_auth.admins`). |
| `POST /admin/admins` | Bearer admin key | Adds an email to the admin allowlist. |
| `DELETE /admin/admins` | Bearer admin key | Removes an email from the admin allowlist. |

#### Schema migration

Destructive/type-changing schema transformations (rename, type coercion, field
removal, default backfill) are a deliberate admin operation separate from the
additive `POST /admin/push-schema`. Build a `Migration` and apply it (or dry-run
first) via `POST /admin/db/{db}/migrate` — the directives run transactionally
inside the committer's serialized turn (`handle_migrate`), so live queries,
the op feed, audit, and webhooks all fire under the same single-writer
invariant as `handle_mutate`. `changeType` takes a closed `cast`
(`toString`/`toNumber`/`toInt64`/`toBoolean`); the optional `default`
substitutes for un-coercible rows (without it one bad value rolls the whole
migrate back atomically). Spec:
[`docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

#### Schema change history

Every `push-schema`, `migrate`, and `restore` captures a schema snapshot into a per-db
`schema_history` table (always-on, lazy-created on first capture, retention-capped at the
most recent 100 versions). The history list (`GET /admin/db/{db}/schema/history`) returns
summaries newest-first; `GET /admin/db/{db}/schema/history/{version}` returns one full
snapshot. `POST /admin/db/{db}/schema/restore` body `{ version, confirm }` — where `confirm`
must equal the db name (the same typed guard as `delete-db`) — runs an **in-place destructive
shape reconcile** inside the committer: it captures the outgoing schema first (so the restore
itself is undoable), then applies the target shape. Data-loss caveat: restore reconciles
schema SHAPE — dropping an index column preserves the `doc` jsonb data, so the only real data
loss is `DROP TABLE` for tables absent from the target snapshot, and migrate data-transforms
(`renameField`/`changeType`/…) applied after the captured snapshot are not rewound.

### Admin: dashboard operator surface

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /admin/db/{db}/query` | Bearer admin key | Admin reads documents across any database (`owner=None`, bypassing per-row `ownerField`). Accepts `includeDeleted: true` (admin-route param, not a wire `Query` field) to surface soft-deleted rows. |
| `POST /admin/db/{db}/mutate` | Bearer admin key | Admin writes documents across any database. Capped by `RTDB_MAX_AFFECTED_DOCS`. |
| `GET /admin/db/{db}/schedules` | Bearer admin key | Lists scheduled jobs for a database (admin-scoped). |
| `POST /admin/db/{db}/schedules` | Bearer admin key | Creates a scheduled job for a database (admin-scoped). |
| `POST /admin/db/{db}/schedules/{id}/{cancel,pause,resume}` | Bearer admin key | Cancels, pauses, or resumes a scheduled job (admin-scoped). |
| `GET|POST /admin/db/{db}/workflows` | Bearer admin key | Lists workflow runs for a database / starts one (admin-scoped; `POST` body is a `WorkflowSpec`). |
| `GET|DELETE /admin/db/{db}/workflows/{id}` | Bearer admin key | Fetches one run's `WorkflowInfo` / deletes a terminal run's row. |
| `POST /admin/db/{db}/workflows/{id}/cancel` | Bearer admin key | Cancels a non-terminal run (admin-scoped; `{ok:false}` = unknown/terminal). |
| `POST /admin/db/{db}/workflows/{id}/signal` | Bearer admin key | Delivers a named signal to a `waiting` run (admin-scoped; `{name, payload?}` → `{ok}`) — the route the dashboard's send-signal form and `rtdb workflows signal` ride. |
| `GET|PATCH /admin/db/{db}/anonymous-access` | Bearer admin key | Per-database anonymous-auth toggle (SEC-103), on top of the instance-wide `RTDB_AUTH_ANONYMOUS_ENABLED` boot gate. |
| `GET /admin/db/{db}/storage` | Bearer admin key | Lists blobs stored in a database (id, sha256, size, contentType, createdAt). |
| `POST /admin/db/{db}/storage` | Bearer admin key | Uploads a blob (admin-scoped; same shape as `POST /api/storage/{db}`). |
| `DELETE /admin/db/{db}/storage/{id}` | Bearer admin key | Deletes a blob (admin-scoped). |
| `GET /admin/db/{db}/webhooks` | Bearer admin key | Lists webhooks configured for a database. |
| `POST /admin/db/{db}/webhooks` | Bearer admin key | Creates a webhook (target URL + event filter). Enabled when `RTDB_WEBHOOKS_ENABLED=true`. |
| `PUT /admin/db/{db}/webhooks/{id}` | Bearer admin key | Edits a webhook. |
| `DELETE /admin/db/{db}/webhooks/{id}` | Bearer admin key | Deletes a webhook. |
| `GET /admin/db/{db}/webhooks/{id}/deliveries` | Bearer admin key | Lists recent delivery attempts (outbox drain) for a webhook. |
| `GET /admin/metrics` | Bearer admin key | Live gauges and throughput. |
| `GET /admin/ops/recent` | Bearer admin key | Recent document-mutation op feed (durable). |
| `GET /admin/audit?db=&limit=&offset=` | Bearer admin key | Audit log entries (`ts_ms, db, table, op, doc_id, principal, source`) — only when `RTDB_AUDIT_LOG_ENABLED=true`; 404 otherwise. |
| `GET /admin/subscriptions` | Bearer admin key | Lists active subscriptions across all dbs (live query inspector). |
| `POST /admin/db/{db}/explain` | Bearer admin key | Query introspection (ENH-019): re-compiles a `Query` through the real path and returns `{sql, params, terminal, warnings}` — a plan, no rows. |
| `GET /admin/slow-queries` | Bearer admin key | Bounded in-memory ring of queries slower than `RTDB_SLOW_QUERY_MS` (default 0 = disabled; `RTDB_SLOW_QUERY_CAPACITY` default 200; `RTDB_SLOW_QUERY_LOG_PARAMS=false` keeps doc content out). |
| `GET /admin/sessions?user=&limit=` | Bearer admin key | Lists active interactive sessions (OAuth/anonymous/admin-key). `?user=` filters by `user_id` or email (omitted ⇒ all, server-wide); `?limit=` clamped to `[1, 1000]`, default 200. `token_hash` is a non-reversible sha256 digest, safe to surface. |
| `DELETE /admin/sessions/{token_hash}` | Bearer admin key | Revokes a single session by its `token_hash` (takes effect on the next op over an already-open connection — `session_still_valid` re-queries on every interactive Subscribe/Mutate/Presence). |
| `DELETE /admin/sessions?user=` | Bearer admin key | Revokes every session for a user. Requires exactly one scope — `?user=` or `?expired=true`; a bare `DELETE` (or both params) is a 400 (refuses to revoke every session instance-wide from one unscoped or ambiguous call). |
| `DELETE /admin/sessions?expired=true` | Bearer admin key | Revokes every EXPIRED session instance-wide (OAuth/anonymous and admin-key login rows alike — expired rows otherwise linger until each is used or individually revoked). Returns `{ok, revoked}` with the count swept; the dashboard's Sessions page exposes this as "remove all expired". |
| `WS /admin/stream` | Bearer admin key | Live op-feed stream over WebSocket (subprotocol auth for browsers). |
| `GET /admin/config` | Bearer admin key | Hot config, redacted (`admin_key`/OAuth secrets/`database_url` → configured-bools only). |
| `PATCH /admin/config` | Bearer admin key | Mutates `allowed_origins`/`session_ttl_days`/`max_file_size`/`idempotency_ttl_ms`/`max_tables_per_db`/`max_storage_bytes_per_db`/`max_subs_per_db`; validates, persists, swaps live (no restart). |
| `POST /admin/backup` | Bearer admin key | Spawns a manual `pg_dump` of the live DB (409 if one is already running). Enabled when `RTDB_BACKUP_ENABLED=true`. |
| `GET /admin/backups` | Bearer admin key | Lists existing dump files. |
| `GET /admin/backups/{name}` | Bearer admin key | Downloads a dump file (path-traversal-guarded). |
| `DELETE /admin/backups/{name}` | Bearer admin key | Deletes a dump file. |
| `POST /admin/restore` | Bearer admin key | Restores a dump into a fresh `rtdb_restored_<stamp>` Postgres DB via `pg_restore --no-owner --no-privileges`. Body `{ name, confirm }`, `confirm` = name. The live `rtdb` DB is never touched (single-writer invariant intact). |
| `GET /admin/export-db?db=` | Bearer admin key | Snapshot export: schema line + one JSONL doc line per document. |
| `POST /admin/import-db?db=` | Bearer admin key | Snapshot import: applies the schema line, replays each doc with original id/timestamp/version. |
| `POST /admin/merge-users` | Bearer admin key | Runs the anon→real account merge synchronously and returns the full report. Body `{ anonUserId, realUserId, confirm }`, `confirm` must equal `realUserId`; 404 when the anon row does not exist. Operator escape hatch (crash-window cleanup, manual consolidation) — the OAuth callback merges automatically on sign-in. |

### Auth (OAuth + sessions)

Six optional OAuth providers ship behind the shared `OAuthProvider` trait —
GitHub, Google, GitLab, Microsoft, Apple, and a generic OIDC provider — plus
hashed per-database machine tokens and a server-wide admin key. Each provider
is independently optional: leave its env vars blank and its routes return
`503`. See [`docs/OAUTH_SETUP.md`](docs/OAUTH_SETUP.md) for the per-provider
setup (callback URLs, scopes, env-var pairs).

| Method & path | Auth | Description |
| --- | --- | --- |
| `GET /auth/{provider}/begin?origin=` | none | Starts the OAuth flow for one of `github`/`google`/`gitlab`/`microsoft`/`apple`/`oidc`. Validates `origin` against the **live** `RTDB_ALLOWED_ORIGINS` (403 on miss), mints a single-use `state` token, sets the `SameSite=None;HttpOnly` `rtdb-oauth-csrf` nonce cookie, and returns JSON `{ authorizeUrl, state }`. The caller opens `authorizeUrl` in a `noopener,noreferrer` popup and polls `/auth/state` for completion. Optional `&mode=cookie` (SEC-207) marks a cookie-mode login: the `/auth/state` completion omits the session token entirely — the HttpOnly cookie set by the callback is the only credential carrier, so no script-readable copy ever exists. Any other `mode` value is a `400`. |
| `GET /auth/callback` | none (state token) | GitHub OAuth callback (the canonical path; Google/GitLab/Microsoft/OIDC use `GET /auth/{provider}/callback`). Constant-time-verifies the CSRF nonce cookie against `state`, claims the pending entry (replay → 400), exchanges the code, fetches the verified email, upserts `rtdb_auth.users`, sets the `HttpOnly` session cookie, and returns popup-closing HTML. |
| `POST /auth/apple/callback` | none (state token) | Apple-specific callback — Apple POSTs `code`+`state` via `response_mode=form_post`. Same CSRF/session handling as the GET path (the `SameSite=None` cookie survives the cross-site POST). |
| `GET /auth/state?state=` | none (state token + cookie) | Provider-agnostic poll endpoint keyed on the single-use `state` token. Returns `{ status: "pending" }` until the callback completes, then `{ status: "complete", token, user }` (one-shot; for a `mode=cookie` begin the completion is `{ status: "complete", user }` — no `token` field, SEC-207); `{ status: "expired" }` for a timed-out/already-claimed flow, `{ status: "error" }` for a failed login. SEC-121: the poll must also carry the `rtdb-oauth-state` cookie set at `/begin` with the same value (constant-time compared; a miss returns `expired`), so a leaked state URL alone cannot poll — cross-origin SDK login sends `credentials: "include"` on the `/begin` and poll fetches (that cookie is `SameSite=None`, unlike the `SameSite=Lax` session cookie). |
| `POST /auth/logout` | Bearer session | Deletes the session for the given bearer token. Idempotent: always 200 unless the delete query itself fails. |
| `GET /auth/me` | Bearer session | Returns the authenticated user. 401 for a machine token (session only). |
| `GET /auth/validate` | Bearer token | Validates a presented session or machine token; returns the `AuthedUser`. Used by backends to check a player-supplied token. |
| `POST /auth/anonymous` | none | Mints an ephemeral anonymous user + session for a credential-less guest (gated by `RTDB_AUTH_ANONYMOUS_ENABLED`, default off ⇒ `403 FORBIDDEN`; a per-db toggle — `GET|PATCH /admin/db/{db}/anonymous-access`, SEC-103 — must also allow it). Sets the `HttpOnly` session cookie (browser path) **and** returns the plaintext session token in the body (SDK/bearer path — pass it as the WS/HTTP bearer, exactly like a machine token). Per-IP rate-limited; an anonymous user is authorized for any database via that boot gate (no allowlist entry) and owns its own documents via per-row `ownerField` (the anon `user_id`). On a later OAuth sign-in with that session's bearer presented at `/begin`, the anon footprint is merged into the real account — owned docs across all databases restamped, storage blob ownership swapped, the live session re-pointed — via `merge::merge_users` (`rtdb_merge_docs_total` counts restamped docs); `POST /admin/merge-users` is the operator escape hatch. |

The live login flow (SEC-012): the browser hits `GET /auth/{provider}/begin` →
opens the provider authorize URL in a `noopener,noreferrer` popup (reverse-tabnabbing
defense — `window.opener` is severed) → the parent polls
`GET /auth/state?state=<token>` → the provider redirects to the callback →
the callback sets the `HttpOnly` session cookie and returns popup-closing HTML
→ the parent's next poll receives the session token and closes the popup
(in cookie mode the begin carries `&mode=cookie` and that poll carries **no**
token — the cookie is the credential, SEC-207).
Login-CSRF is defended by the `rtdb-oauth-csrf` double-submit cookie set at
`/begin` and constant-time-verified at `/callback`
(`RTDB_OAUTH_LOGIN_CSRF=true` by default; cross-origin SDK consumers must send
`credentials: "include"` on the `/begin` and `/auth/state` fetches — the poll
also requires the `rtdb-oauth-state` cookie, SEC-121). Identity is email-keyed with
cross-provider linking (Apple additionally keys on its stable `sub`).

Bearer tokens are either a per-database **machine token** (minted via `/admin/mint-token`)
or a **session token** (minted by completing an OAuth flow). Both resolve through the same
`Authorization: Bearer <token>` header on `/api/*`, `/auth/*`, and the WS `auth` frame. The
WS handler and admin re-auth paths re-run on every op (revocation and expiry take effect on
open connections — see [Known MVP limitations](#known-mvp-limitations)).

## Configuration

The server reads its configuration from environment variables (prefix `RTDB_`).
The full, comment-annotated list lives in [`.env.example`](.env.example) — copy
it to `.env` and edit. The subset below is what most operators tune on first
boot:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `RTDB_DATABASE_URL` | yes | — | Postgres connection string. |
| `RTDB_ADMIN_KEY` | yes | — | Server-wide admin bearer (constant-time compared). Generate with `openssl rand -hex 32`. |
| `RTDB_PORT` | no | `8300` | HTTP/WS listen port. |
| `RTDB_PUBLIC_URL` | no | `http://localhost:8300` | Public origin (OAuth callback base, external links). |
| `RTDB_ALLOWED_ORIGINS` | no | empty | Comma-separated browser origins; also the exact-match CORS allowlist for `/api/*` and `/auth/*`. **Hot-reloadable.** |
| `RTDB_STATIC_DIR` | no | unset | Directory of static SPA build artifacts. Set/unset existing dir ⇒ dashboard served same-origin at the catch-all route fallback; unset/empty/missing ⇒ API-only. |
| `RTDB_GITHUB_CLIENT_ID` + `RTDB_GITHUB_CLIENT_SECRET` | no | none | GitHub OAuth app credentials — both required to enable GitHub login. The other five providers (Google, GitLab, Microsoft, Apple, OIDC) follow the same `RTDB_<PROVIDER>_CLIENT_ID` + `_CLIENT_SECRET` pair pattern; see `.env.example` and [`docs/OAUTH_SETUP.md`](docs/OAUTH_SETUP.md). |
| `RTDB_OTEL_ENABLED` | no | `false` | OpenTelemetry/OTLP tracing master switch (ENH-018). Requires the server built with the `otel` cargo feature; `false` (default) produces zero OTLP calls. Paired with `RTDB_OTEL_ENDPOINT` (default `http://127.0.0.1:4317`), `RTDB_OTEL_SERVICE_NAME` (default `par-rt-db`), and `RTDB_OTEL_SAMPLE_RATIO` (default `0.05`). See [Tracing](deploy/README.md#tracing-opentelemetry--otlp-enh-018). |

The hot-reloadable settings (live on `AppState` as `Arc<ArcSwap<HotConfig>>`,
seeded from env at first boot, persisted in a single-row `rtdb_config` table,
and swapable via `PATCH /admin/config` without a restart): `allowed_origins`,
`session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`, and the three
per-database quota caps `max_tables_per_db` / `max_storage_bytes_per_db` /
`max_subs_per_db` (`0` = unlimited, ENH-011). The full set of boot-time vars —
OAuth, rate limits, scheduling, presence, image transforms, backups, audit,
webhooks, OTLP tracing, and more — is annotated in [`.env.example`](.env.example). `RTDB_ALLOWED_ORIGINS` is also the
exact-match CORS allowlist for `/api/*` and `/auth/*` (GET, POST, OPTIONS;
`authorization` and `content-type` headers). Each OAuth provider is only active
when both its client id and secret are set; a half-configured pair is treated
the same as neither, and `GET /auth/<provider>/begin` returns `503` with an
`INTERNAL` error envelope.

## Error envelope

Every error response — HTTP and WebSocket alike — is a JSON object:

```json
{"code": "NOT_FOUND", "message": "document 'abc' not found"}
```

A `RATE_LIMITED` denial additionally carries a `retryAfter` field (seconds,
mirrored as the HTTP `Retry-After` header); every other code omits it:

```json
{"code": "RATE_LIMITED", "message": "rate limit exceeded", "retryAfter": 42}
```

| `code`                | HTTP status | Notes |
| --------------------- | ----------- | --- |
| `BAD_REQUEST`         | 400         | Validation or step/budget rejection (e.g. over `MAX_STEPS` / `MAX_AFFECTED_ROWS_PER_TXN`). |
| `UNAUTHORIZED`        | 401         | Missing/invalid/expired token or session. |
| `FORBIDDEN`           | 403         | Per-row `ownerField`/`authorize` denial; anonymous auth disabled; signed-URL verification failure. |
| `NOT_FOUND`           | 404         | Unknown document/file/route; cross-db storage mismatch is reported as 404 (no existence disclosure). |
| `CONFLICT`            | 409         | `UNIQUE` index violation (`unique_violation`, SQLSTATE 23505) at `CREATE UNIQUE INDEX` time or on a colliding write. |
| `PRECONDITION_FAILED` | 409         | `expectVersion` / `expectAbsent` guard failed. |
| `SCHEMA_VIOLATION`    | 422         | Schema push / type-coercion rejection. |
| `RATE_LIMITED`        | 429         | Per-token or per-db rate limit hit (HTTP and WS frames). Carries `retryAfter`; the WS connection stays open. |
| `INTERNAL`            | 500         | Unexpected server error (generic message; detail logged via `tracing`). |
| `QUOTA_EXCEEDED`      | 507         | Per-database resource cap hit (tables / storage bytes / concurrent subscriptions — ENH-011). |

## Wire protocol

### Query shape

`{"table": "<name>", "get"?, "index"?, "eq"?, "order"?, "take"?, "unique"?, "first"?,
"count"?, "filter"?, "search"?, "vectorSearch"?, "hybridSearch"?, "paginate"?, "distinct"?, "aggregate"?, "fields"?}`
— exactly one terminal per query (terminals are mutually exclusive). See
`server/src/query/` for full semantics: index prefix binds, range predicates
(`gt`/`gte`/`lt`/`lte`) follow the `eq` prefix, `order: "asc"|"desc"`, `take`
capped at 4096, `unique` de-duplicates on the indexed fields, `count` is an
uncapped `SELECT COUNT(*)`, `first` is sugar over `take(1)`, `filter` carries
an `eq`/`neq`/`gt`/`gte`/`lt`/`lte`/`in`/`not`/`contains`/`exists` + `and`/`or`
predicate compiled to SQL, `search` ranks by `ts_rank` over a generated
tsvector (and accepts an optional `filter` to narrow the `WHERE` in the same
SQL pass — the same `FilterExpr` `.filter()` accepts; query text is parsed as
websearch syntax — quoted phrases require adjacency, a bare `or` unions,
`-term` excludes; an optional `snippet: true` attaches a `_searchSnippet`
`ts_headline` fragment per hit with matched terms wrapped in `<mark>`
(server-fixed word bounds, tsquery mode only); an optional
`mode: "trgm"` switches to substring/autocomplete matching — case-insensitive
`ILIKE` ranked by `pg_trgm` `similarity()`, backed by a GIN trigram index),
`vectorSearch` ranks by
the index's declared metric distance over a write-maintained pgvector column
(also accepting an optional full `filter`), `hybridSearch` fuses the full-text
(`ts_rank`) and `vectorSearch` rankings for the same table into one list via
Reciprocal Rank Fusion, `paginate` is opaque-cursor keyset pagination,
`distinct` returns the unique values of the index field after the `eq` prefix
(ascending; NULLs included, sorted last), and `aggregate` runs a scalar
`sum`/`min`/`max`/`avg`/`count`, optionally grouped (`groupBy`) by the next
index field (rows missing the group value form one `key: null` group,
sorted last), and an optional `fields` array projects each result doc to the
listed user fields — the system fields (`_id`/`_creationTime`/`_version`) and
synthetic fields (`_searchSnippet`) are always kept, `fields: []` is an
ids-only view, names are validated against the schema (`BAD_REQUEST` on an
unknown field), it composes with every doc-bearing terminal (`count`/
`distinct`/`aggregate` return no docs, so it is a no-op there), and a
projected subscription does not push when a write changes only non-projected
fields (the pushed payloads still carry `_version`).

### Transaction shape

`{"steps": [...]}` where each step is tagged by `"op"`: `insert`, `patch`,
`replace`, `delete`, `undelete` (softDelete tables only — clears the
`deleted_at` stamp), `expectVersion`, `expectAbsent`, `upsert` (per-id, one
document each), the predicate-driven bulk steps `patchByQuery` and
`deleteByQuery` (each finds rows matching a `filter` and acts on up to
`MAX_BY_QUERY_ROWS` of them in one serialized committer turn), the
scheduler control-flow steps `schedule` (enqueues a nested txn by inserting
the `scheduled_txns` row on the open sqlx transaction — atomic with the
enclosing writes; step result `{"scheduleId": "<id>"}`) and `cancelSchedule`
(`{"cancelled": <bool>}`, `false` on a missing/already-fired/already-cancelled job), and the
workflow control-flow steps `startWorkflow` (inserts the per-db `workflows`
run row on the open sqlx transaction — atomic with the enclosing writes; step
result `{"workflowId": "<id>"}`; the spec's tables are allowlist-checked at
submit time) and `cancelWorkflow` (`{"cancelled": <bool>}`, `false` on a
missing/terminal run — a no-op, not an error) — see `server/src/txn.rs`.

A by-query `filter` additionally accepts the execution-time-relative
`olderThan` op — `{"op": "olderThan", "field": "completedAt", "ms": 604800000}` —
matching rows whose epoch-ms field is strictly older than `now − ms`, with the
cutoff derived from the server clock **at each execution**. A scheduled
one-shot/cron/interval txn carrying it stays fresh on every fire with no
client re-scheduling (server-side sweeps: archive done rows older than 7 days,
expire claim leases). The op is by-query-only — read/query filters, `authorize`
predicates, partial-index `where` predicates, and computed `case` whens reject
it — and requires a declared `number`/`int64` field with `ms ≥ 0`.

### WebSocket example: subscribe, then mutate

Connect to `ws://localhost:8300/sync`. The first frame must be `auth`; every message
after is JSON text, camelCase, tagged by `"type"`.

```jsonc
// -> client: authenticate with a machine token scoped to db "myapp"
{"type": "auth", "token": "<machine-token>", "db": "myapp"}

// <- server
{"type": "authOk", "user": {"kind": "machine", "email": null, "name": null}}

// -> client: subscribe to all not-done tasks via the "by_done" index
{"type": "subscribe", "queryId": "q1", "query": {"table": "tasks", "index": "by_done", "eq": [false]}}

// <- server: initial result (empty — no rows yet)
{"type": "queryUpdate", "queryId": "q1", "result": []}

// -> client: insert one task
{"type": "mutate", "mutId": "m1", "txn": {"steps": [{"op": "insert", "table": "tasks", "doc": {"title": "Buy milk", "done": false}}]}}

// <- server: the mutation's own result
{"type": "mutateOk", "mutId": "m1", "results": [{"id": "018f9a2b3c4d75e6a8b1c2d3e4f5a6b7"}]}

// <- server: pushed to q1 because the insert matches its filter
{"type": "queryUpdate", "queryId": "q1", "result": [{"_id": "018f9a2b3c4d75e6a8b1c2d3e4f5a6b7", "_creationTime": 1732000000000, "_version": 1, "title": "Buy milk", "done": false}]}
```

Every subscribed query is re-evaluated and pushed (only if its serialized result
changed) after every committed transaction that touches the query's table — there is
no separate diffing of individual documents.

### HTTP one-shot example: mutate, then query

Using a machine token minted via `POST /admin/mint-token`:

```bash
curl -s -X POST http://localhost:8300/api/mutate \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp", "txn": {"steps": [{"op": "insert", "table": "tasks", "doc": {"title": "Buy milk", "done": false}}]}}'
# {"results":[{"id":"018f9a2b3c4d75e6a8b1c2d3e4f5a6b7"}]}

curl -s -X POST http://localhost:8300/api/query \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp", "query": {"table": "tasks"}}'
# {"result":[{"_id":"018f9a2b3c4d75e6a8b1c2d3e4f5a6b7","_creationTime":1732000000000,"_version":1,"title":"Buy milk","done":false}]}
```

## Pagination

Keyset pagination over an index is supported via the `paginate` query terminal. A
page request carries an opaque cursor (omitted for the first page) and a page
size; the response is `{docs, nextCursor}`, where `nextCursor` is omitted when
there is no next page. The cursor encodes the sort-column values of the last row on the
page, so the server resumes strictly *after* it — stable under concurrent
inserts/deletes unlike offset pagination.

The `paginate` terminal composes with `index`, `eq`, range bounds (`gt`/`gte`/
`lt`/`lte`), and `order`, and is mutually exclusive with `get`, `take`, `unique`,
`first`, and `count`. `numItems` is capped at 4096 (`MAX_TAKE`).

### Pagination query shape

```jsonc
// First page — cursor omitted
{"table": "items", "index": "by_priority", "order": "asc", "paginate": {"numItems": 20}}

// Subsequent page — pass the previously returned nextCursor
{"table": "items", "index": "by_priority", "order": "asc",
 "paginate": {"cursor": "<opaque-cursor>", "numItems": 20}}
```

```jsonc
// Response (HTTP `/api/query` wraps it as `{"result": {...}}`; WS `queryUpdate`
// delivers the inner object directly)
{"docs": [{"_id": "...", "_creationTime": 1732000000000, "_version": 1, "name": "item 1", "priority": 1}, /* ... */],
 "nextCursor": "<opaque-cursor>"}  // omitted on the last page
```

### HTTP example: page through an index

```bash
# Page 1
curl -s -X POST http://localhost:8300/api/query \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp", "query": {"table": "items", "index": "by_priority", "order": "asc", "paginate": {"numItems": 10}}}'
# {"result":{"docs":[/* 10 docs */],"nextCursor":"<opaque-cursor>"}}

# Page 2 — feed nextCursor back into the request
curl -s -X POST http://localhost:8300/api/query \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp", "query": {"table": "items", "index": "by_priority", "order": "asc", "paginate": {"cursor": "<opaque-cursor>", "numItems": 10}}}'
```

### TypeScript client

The `TableQuery.paginate(cursor, numItems)` builder terminal produces a paginated
query, and the `usePaginatedQuery` hook (from `@par-rt-db/client/react`) manages
page state and live subscriptions across pages. See
[`ts-client/README.md`](ts-client/README.md) for the full SDK surface.

```ts
import { createApi } from "@par-rt-db/client";
import { schema } from "./schema";

const api = createApi(schema);
// Returns {docs, nextCursor} — the wire page shape.
const page = await http.query(
  api.items.query().withIndex("by_priority").order("asc").paginate(undefined, 20),
);
```

```tsx
import { usePaginatedQuery } from "@par-rt-db/client/react";

function ItemList() {
  // The factory returns the base query JSON (no `paginate`); the hook injects
  // `paginate` per page. Each loaded page is a live subscription, docs stitch
  // across pages, and `loadMore` advances the cursor from the last page.
  const { data, loading, hasNextPage, loadMore } = usePaginatedQuery<{
    _id: string;
    name: string;
  }>(() => ({ table: "items", index: "by_priority", order: "asc" }), { pageSize: 20 });

  return (
    <div>
      {data.map((item) => (
        <div key={item._id}>{item.name}</div>
      ))}
      {hasNextPage && (
        <button type="button" onClick={() => void loadMore()} disabled={loading}>
          Load more
        </button>
      )}
    </div>
  );
}
```

### Rust client

`TableQuery::paginate(cursor: Option<&str>, num_items: u32)` mirrors the TS builder.
The HTTP client maps the result to `Paginated<T>`. See
[`rust-client/README.md`](rust-client/README.md).

```rust
use par_rt_db_client::{Order, Paginated, RtDbHttpClient, TableQuery};

let page: Paginated<Item> = http
    .query(
        TableQuery::new("items")
            .with_index("by_priority", &[])
            .order(Order::Asc)
            .paginate(None, 20),
    )
    .await?;
for item in &page.docs { /* ... */ }
if let Some(cursor) = page.next_cursor.as_deref() {
    // pass `cursor` into the next paginate(...) call
}
```

### Python client

`TableQuery.paginate(*, cursor=None, num_items)` mirrors the same shape; combine
it with `encode_cursor` / `decode_cursor` if you crack cursors client-side. The
HTTP client (`pip install par-rt-db[http]`) runs the built query against
`POST /api/query` and returns the `Paginated` result. See
[`python-client/`](python-client).

```python
from par_rt_db import TableQuery
from par_rt_db.http_client import RtDbHttpClient

client = RtDbHttpClient("https://rtdb.example.com", db="myapp", token=TOKEN)

cursor = None
while True:
    q = (
        TableQuery("items")
        .with_index("by_priority")
        .order("asc")
        .paginate(cursor=cursor, num_items=20)
    )
    page = client.query(q)           # -> Paginated(docs=[...], next_cursor="..." | None)
    for doc in page.docs:
        ...
    cursor = page.next_cursor
    if cursor is None:
        break
```

### Cursor format

Cursors are opaque base64-encoded JSON arrays of the index field values (plus the
tie-breaker columns `created_at` and `id`) for the last row on the previous page.
Clients should treat them as opaque and pass them back verbatim. A cursor is tied
to the query shape — changing the `index`, `order`, or `eq` prefix invalidates it;
restart from the first page (no cursor). Compound (multi-column) indexes are
fully supported: the cursor carries every sort-column value and the server resumes
via a row-value comparison across all of them, with `id` as the globally unique
final tie-breaker, so pages never skip or duplicate rows.

## Scheduling

Transactions can be scheduled for later or recurring execution — declarative
`Transaction`s stored as data in a per-database `scheduled_txns` side table, not
server-side code. `when` is one of:

- `{type: "afterMs", ms}` — fire `ms` milliseconds from now (one-shot).
- `{type: "runAt", ms}` — fire at this UTC epoch-ms instant (one-shot; in the past
  fires immediately).
- `{type: "cron", expr}` — fire on a 5-field standard cron expression (UTC,
  min-first, e.g. `"*/5 * * * *"` = every 5 minutes). The server validates `expr`.
- `{type: "interval", everyMs}` — fire every `everyMs` milliseconds, first fire
  one interval from now (e.g. `300000` = every 5 minutes, no cron expression
  needed). `everyMs` must be positive and at most 31,536,000,000 (one year).

A per-database scheduler timer claims due rows and enqueues each through the
single-writer committer, which executes it via the normal transaction path (so
subscriptions fire on schedule-driven writes too). Delivery is **at-least-once**:
apps should write idempotent scheduled transactions. A one-shot catches up if past
due; a cron or interval job **skips** missed windows with no backfill (each fire
re-arms from its actual fire time, and `resume` shifts the next fire one full
interval/expression step from the resume). Each job's lifecycle is
managed with `cancel` / `pause` / `resume` and listed via `listSchedules`.

### HTTP example: schedule a one-shot, then list

```bash
curl -s -X POST http://localhost:8300/api/schedule \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp", "when": {"type": "afterMs", "ms": 60000},
       "txn": {"steps": [{"op": "insert", "table": "tasks", "doc": {"title": "deferred", "done": false}}]}}'
# {"id":"<schedule-id>"}

curl -s -X POST http://localhost:8300/api/schedules \
  -H "Authorization: Bearer <machine-token>" \
  -H "Content-Type: application/json" \
  -d '{"db": "myapp"}'
# {"schedules":[{"id":"<schedule-id>","kind":"oneshot","dueAt":...,"status":"pending","createdAt":...,"firedCount":0}]}
```

### WebSocket

The WS surface adds `schedule` / `cancelSchedule` / `pauseSchedule` /
`resumeSchedule` / `listSchedules` messages (tagged by `"type"`), with replies
`scheduleOk` / `scheduleErr` / `scheduleAck` / `listSchedulesOk`. Authorization is
re-run on every op, not just at connect.

## Durable workflows

A workflow is a named spec of steps — each either an ordinary declarative
`Transaction` or an `awaitSignal` wait (`{"name": string, "timeoutMs"?: number}`,
exactly one of the two per step), plus an optional `StepRetry` (`maxAttempts`,
default 3; 1s initial backoff doubling to a 60s cap) and an optional
`sleepBeforeMs` — that the server advances durably (FM-29). A run is a row in a
per-db `workflows` side table; the scheduler timer enqueues each due step as
`CommitterRequest::RunWorkflowAdvance` and the committer executes it through
the normal `execute_txn` + `fan_out` path, so the single-writer invariant and
the op-feed/audit/webhook taps hold for every step. Delivery is
**at-least-once per step** with crash-resume (a row left `running` by a crash
is re-armed at startup); a step that exhausts its retries fails the run.
Steps fire as the system (bypass) principal — a scoped machine token is
confined at submit time — so write idempotent step txns. A run's status is
one of `pending` / `running` / `success` / `failed` / `cancelled` /
`waiting`.

An `awaitSignal` step is an approval gate: it parks the run in the
non-terminal `waiting` state until an out-of-band signal with a matching name
is delivered (or an optional timeout fires). While waiting, `WorkflowInfo`
carries `waitingFor` (the signal name) and `waitedSince` (ms epoch the wait
started); both are omitted otherwise. The delivered payload is recorded
verbatim on the step outcome (`signal`) and is **latest-wins** — a second
delivery while the run is still waiting overwrites the first (every delivery
still acks). A `timeoutMs` expiry counts as a failed attempt routed into the
step's `retry` policy, and each re-parked attempt waits the full `timeoutMs`
again (no backoff); omit `timeoutMs` to park forever — cancel is the escape.

Send the signal on any of three surfaces — `POST /api/workflows/{id}/signal`
(Bearer token, body `{db, name, payload?}` → `{"delivered": true}`), the WS
`signalWorkflow` frame (reply reuses `workflowAck`, with the typed
`{code, message}` error envelope on failure), or admin
`POST /admin/db/{db}/workflows/{id}/signal` (the dashboard and `rtdb`
CLI ride this one). Unknown id is 404; a non-waiting run or a name mismatch
is 409 (the message names both signals). Delivery is one conditional UPDATE
on the side table row (like cancel) — `awaitSignal` steps write no documents.

Start runs via the HTTP `POST /api/workflows` routes, the WS
`startWorkflow` / `cancelWorkflow` / `listWorkflows` frames, the admin CRUD
routes, or the `startWorkflow` / `cancelWorkflow` txn steps (insertion atomic
with the enclosing writes). All four clients mirror the surface:
`startWorkflow` / `cancelWorkflow` / `listWorkflows` / `signalWorkflow`
(ts, rust, python, swift — reactive + HTTP) plus admin variants, the
`rtdb workflows` CLI (incl. `rtdb workflows signal`), and the dashboard
Workflows page (which shows waiting runs and can send the signal). Spec:
[`docs/superpowers/specs/2026-08-15-workflows-design.md`](docs/superpowers/specs/2026-08-15-workflows-design.md)
+ [`docs/superpowers/specs/2026-08-21-workflow-await-signal-design.md`](docs/superpowers/specs/2026-08-21-workflow-await-signal-design.md).

### TypeScript client

```ts
import { WorkflowSpec, awaitSignal } from "@par-rt-db/client";

const spec: WorkflowSpec = {
  name: "onboard",
  steps: [
    { txn: { steps: [{ op: "insert", table: "work_items", doc: { title: "welcome" } }] } },
    // Approval gate: parks the run as `waiting` until a matching signal.
    { ...awaitSignal("approve", 86_400_000), retry: { maxAttempts: 1 } },
    {
      txn: { steps: [{ op: "insert", table: "work_items", doc: { title: "follow-up" } }] },
      retry: { maxAttempts: 5 },
      sleepBeforeMs: 60_000,
    },
  ],
};
const { id } = await client.startWorkflow(spec);
await client.signalWorkflow(id, "approve", { approvedBy: "u1" }); // releases the gate
await client.cancelWorkflow(id); // false for a missing/terminal run
const runs = await client.listWorkflows("running"); // newest first
```

## Computed fields

A table may declare `computed`, a map of field name → typed expression, and the
server re-derives every entry on **every write** (insert, patch, replace, upsert
both branches, `patchByQuery`, cascade setNull) — storing the result in both the document body and
the typed column, which is what makes a computed field **indexable** (`order`,
`filter`, and `count` work over its declared index like any field). The grammar
is the closed `ValueExpr` set the migrate `evalExpr` path already uses — `field`,
`literal`, `concat`, arithmetic (`add`/`sub`/`mul`/`div`), `coalesce`,
`lower`/`upper`/`trim`, casts, `now`, and `case` — with no subqueries, no
function calls by name, and no raw SQL.

Semantics:

- **Server authority**: a client-supplied value for a computed field never
  survives — it is dropped before validation (so even a wrong-typed one cannot
  fail the write) and the stamp overwrites it.
- **Null removes the key**: an expression evaluating `null` (e.g. `coalesce`
  over a missing optional input) leaves the key absent — the unset-optional
  convention, never a stored `null`.
- **`now` is epoch-ms** — a JSON number, the same value `updatedAtField`
  stamping uses.
- **A runtime error fails the write**: e.g. division by zero fails the whole
  transaction atomically with `BAD_REQUEST` naming the field.
- **Push validates the map**: keys must name declared fields (not
  `ownerField`/`collaboratorsField`/`autoIncrementField`), every referenced
  field must be declared and not itself computed (no chaining), `case` `when`
  filters may not use `$user`/`$email` markers, and a statically-known result
  kind must be acceptable to the field's type.
- **Push backfills**: adding or changing an entry re-derives every existing row
  (no `_version` bump — a backfill is not a write). **Removing** an entry
  leaves the stored values in place; the field becomes an ordinary
  client-writable field again.

```bash
# Declare computed fields: fullName = first + " " + last, handle = lower(trim(email)).
curl -s -X POST http://localhost:8300/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","schema":{"tables":{"users":{"fields":{"first":{"type":"string"},"last":{"type":"string"},"email":{"type":"string"},"fullName":{"type":"string"},"handle":{"type":"string"}},"indexes":[{"name":"by_fullName","fields":["fullName"]}],"computed":{"fullName":{"op":"concat","parts":[{"op":"field","field":"first"},{"op":"literal","value":" "},{"op":"field","field":"last"}]},"handle":{"op":"lower","value":{"op":"trim","value":{"op":"field","field":"email"}}}}}}}}'

# Insert — client-supplied fullName is ignored; fullName and handle are stamped.
curl -s -X POST http://localhost:8300/api/mutate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","txn":{"steps":[{"op":"insert","table":"users","doc":{"first":"Grace","last":"Hopper","email":"  Grace@Example.COM ","fullName":"WRONG"}}]}}'

# Read it back — fullName and handle are server-derived; email is untouched.
curl -s -X POST http://localhost:8300/api/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","query":{"table":"users","index":"by_fullName","order":"desc"}}'
# {"result":[{"_id":"…","_creationTime":…,"_version":1,"first":"Grace","last":"Hopper",
#             "email":"  Grace@Example.COM ","fullName":"Grace Hopper","handle":"grace@example.com"}]}
```

A patch that changes `first` re-derives `fullName` from the merged document
within that same write. Schema migration stays consistent: `renameField` rewrites the
expression and re-stamps, dropping a referenced field is rejected naming the
computed field, and `evalExpr`/`setDefault`/`changeType` rewrites that feed a
computed input re-stamp dependents in the same migrate. See
[FEATURE_MATRIX #39](FEATURE_MATRIX.md) and
[`docs/superpowers/specs/2026-07-21-par-rt-db-design.md`](docs/superpowers/specs/2026-07-21-par-rt-db-design.md).

## Realtime presence

For ephemeral "who is online right now" data — online indicators, cursors,
typing — that doesn't fit durable document queries, par-rt-db ships a transient,
in-memory presence layer over the existing `/sync` WebSocket. It is **on by
default** (`RTDB_PRESENCE_ENABLED`, default on; set `=false` to disable), **not committer-bound, not durable,
not persisted** (no document tables, no Postgres write — a sibling reactive
surface to live queries), and **connection-bound**: each joined `/sync`
connection is one presence session, and the open WS is itself the liveness
signal (disconnect evicts; no app-level heartbeat). Joining a room makes a
connection present *and* subscribes it to the member list in one act; broadcasts
are coalesced by a process-wide flush task (`RTDB_PRESENCE_BROADCAST_INTERVAL_MS`,
default 50 ms), and per-connection safeguards bound state size, room size, room
count, and update rate. See FEATURE_MATRIX #25 and
[`docs/superpowers/specs/2026-08-06-presence-design.md`](docs/superpowers/specs/2026-08-06-presence-design.md).

```ts
import { usePresence } from "@par-rt-db/client/react";

function Cursors({ roomId }: { roomId: string }) {
  // Joins on mount, re-renders on every presenceSnapshot, leaves on unmount.
  const { members, updatePresence } = usePresence(roomId);
  return (
    <ul>
      {members.map((m) => (
        <li key={m.connectionId}>{m.user.email ?? m.user.kind}</li>
      ))}
    </ul>
  );
}
// Broadcast local cursor/typing state from an event handler:
//   updatePresence({ x: 120, y: 40, typing: true });
```

The TS client also exposes `RtDbClient.presence(room, state?, onSnapshot?)` /
`updatePresence(room, state)` / `leavePresence(room)` for non-React callers; the
Rust and Python reactive clients mirror them as `presence` / `update_presence` /
`leave_presence`. The wire frames are `presence` / `updatePresence` /
`leavePresence` (client) and `presenceSnapshot` / `presenceOk` / `presenceErr`
(server). When presence is disabled, the WS frames reply with a
`PRESENCE_DISABLED` `presenceErr`.

## Make targets

Each `make` target spans **all seven packages** (server, ts-client, rust-client,
python-client, swift-client, dashboard, cli). The swift-client lines in every
sweep are Darwin-guarded — on Linux they print `Skipping swift-client (non-Darwin
host)` and the macOS CI job runs `make swift-client-checkall` instead. The
composition is summarized below; see the
[`Makefile`](Makefile) for the canonical commands.

### First-time install (per package)

| Target | Packages | Purpose |
| --- | --- | --- |
| `make ts-client-install` | ts-client | `bun install` in `ts-client/`. |
| `make dashboard-install` | dashboard | `bun install` at repo root + `dashboard/`. |
| `make python-client-install` | python-client | `uv sync` in `python-client/`. |

Cargo workspaces (`server/`, `rust-client/`) have no install target — `cargo`
fetches on first build.

### Build / format / lint / typecheck / test (each runs across all six packages)

| Target | What runs in each package |
| --- | --- |
| `make build` | server `cargo build` · rust-client `cargo build --all-features` · dashboard `bun run build` · (also runs `ts-client-build` first because the dashboard resolves `@par-rt-db/client` from `ts-client/dist`, which is gitignored) |
| `make fmt` | server/rust-client `cargo fmt --all` · ts-client/dashboard `bun run fmt` · python-client `uv run ruff format .` |
| `make fmt-check` | same as `fmt` but in `--check` mode |
| `make lint` | server/rust-client `cargo clippy --all-targets --all-features -- -D warnings` · ts-client/dashboard `bun run lint` · python-client `uv run ruff check .` |
| `make typecheck` | server/rust-client `cargo check` · ts-client/dashboard `bun run typecheck` · python-client `uv run pyright` (also runs `ts-client-build` first) |
| `make test` | server `cargo test` · ts-client `bun run test` · rust-client `cargo test --all-features` · dashboard `bun run test` · python-client `uv run pytest -q` |

`make dev-db-up` (a prerequisite of `make test`) starts the dev Postgres on
`127.0.0.1:55434` via `docker-compose.dev.yml` and waits for it to be healthy.
`make dev-db-down` stops it. `make dev-db-clean` drops leaked test artifacts
from the dev `rtdb` DB — per-test schemas (`db_t<uuid-v7>`; tests self-clean
via RAII but a bounded tail leaks per binary) and the semantics-corpus
runner's per-case databases (`sc_<case>_<hex>`, ENH-023) — each `DROP` in
psql autocommit so a large sweep doesn't accumulate catalog locks. Run it
periodically; it is scoped to those test patterns and never touches
`rtdb`/`rtdb_auth`/real databases.

### Other workflow targets

| Target | Purpose |
| --- | --- |
| `make ts-client-build` | Builds `ts-client/dist` (gitignored). Run on a fresh or stale checkout before `typecheck`/`build` — the dashboard resolves `@par-rt-db/client` from `dist`. `build` and `typecheck` pull this first. |
| `make env-drift-check` | First stage of `checkall`. Fails when a `RTDB_*` var documented in `.env.example` or read by the server is not forwarded to the container by `docker-compose.yml`'s `environment:` block (an explicit allowlist, so a `.env`-only key silently does nothing). |
| `make rtdb-cli` | Builds the `rtdb` CLI release binary (`cli/`, wraps `par-rt-db-client`). |
| `make dev-db-clean` | Drops leaked test schemas from the dev Postgres (see above). |

### The gate

| Target | Purpose |
| --- | --- |
| `make checkall` | `fmt-check` + `lint` + `typecheck` + `test` across all six packages. **Definition of done; must pass before commit.** |
| `make pre-commit` | `pre-commit run --all-files` (runs `gitleaks`, `detect-private-key`, etc.). |
| `make pre-commit-update` | `pre-commit autoupdate`. |
| `make deploy` | `checkall` → rsync to the Docker host → `docker compose up -d --build` → healthz probe. |

### Granular python-client targets

`make python-client-test | python-client-lint | python-client-fmt | python-client-typecheck | python-client-checkall` run that one stage in `python-client/` only. Equivalent granular targets for the other packages are run directly with the package's tool (`cargo test` in `server/`/`rust-client/`, `bunx vitest run` in `ts-client/`, `bun run test` in `dashboard/`).

> The dashboard ships a Vitest + React Testing Library suite covering its pages and shared lib/hooks; run it standalone with `make dashboard-test`, or as part of `make checkall`/`make test`.

## Graceful shutdown

The server exits cleanly on `SIGINT` or `SIGTERM`: in-flight requests are allowed to
finish (via `axum::serve(...).with_graceful_shutdown(...)`) before the process stops. This
includes open WebSocket connections — shutdown waits for them to close rather than forcibly
dropping them, with no timeout of its own; Docker's SIGTERM→SIGKILL window is the backstop
that ultimately terminates a connection that never closes on its own.

## Known MVP limitations

- **Multi-instance is safe for reads + one writer per database; non-owner
  writes are forwarded to the owner.** With `RTDB_MULTI_INSTANCE=true`
  (ENH-022 Stages 1–4c), OAuth login state, the op-feed, presence,
  rate-limit budgets (`rtdb_auth.rate_counters` — one shared ceiling per
  token/db/ip), and per-database write ownership all coordinate across
  replicas. Ownership: the first replica to touch a database takes a Postgres
  advisory-lock lease on a dedicated connection and runs that database's
  committer (and pollers) on it — no other replica can write the database
  concurrently, and an owner's death (kill -9, container stop) releases the
  lease to the next taker. A non-owner replica serves reads and live
  subscriptions for the database and FORWARDS writes to the owner over
  `pg_notify` (Stage 4c): the owner executes the write inside its committer
  turn, returns the outcome to the non-owner's client, and the non-owner
  re-runs its local subscriptions against the returned write set — the
  caller's principal travels with the request, so per-row authz evaluates
  against the identity that authorized the write at the edge. If no owner
  answers within `RTDB_FORWARD_TIMEOUT_MS` (default 5s), the non-owner
  attempts the lease takeover itself — that path is the failover. Note the
  standard timeout ambiguity: a reply racing the timeout is dropped, so the
  write may have committed even though the client saw `CONFLICT`; exactly-once
  retries should carry an idempotency key. Design + as-built notes:
  `docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md`.
- **Session expiry, machine-token revocation, allowlist removal, and admin-role
  revocation all take effect on open WebSocket connections.** The WS handler re-runs
  `authorize` on every `subscribe` and `mutate` (not just at connect) and re-runs
  `is_admin` on every admin op: an expired session or revoked admin is rejected on the
  next op with `UNAUTHORIZED`/`FORBIDDEN` while the connection stays open, so the client
  can refresh its token and retry without reconnecting. See FEATURE_MATRIX #8.
- Graceful shutdown waits for open WebSocket connections to close on their own rather
  than forcibly dropping them, with no timeout of its own — Docker's SIGTERM→SIGKILL
  window is the backstop that ultimately terminates a connection that never closes (see
  [Graceful shutdown](#graceful-shutdown) above).
- `AuthedUser.name` is always `null`: the `rtdb_auth.users` table has no `name` column, so
  users are identified on the wire by `kind`, `email`, and (for GitHub-linked accounts)
  `githubLogin` / `githubId` — never by a free-form display name.

## Clients

par-rt-db ships four client SDKs that each mirror the server's wire contract,
plus an operator SPA and a CLI built on top of them:

- [`ts-client/`](ts-client/README.md) — `@par-rt-db/client` (browser/Node/React Native): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`rust-client/`](rust-client/README.md) — `par-rt-db-client` (Rust): http + reactive ws + admin, `.filter()`/`.search()`/`.vector_search()` builders.
- [`python-client/`](python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS.
- [`swift-client/`](swift-client/README.md) — `ParRtDbClient`/`ParRtDbUI` (Swift 6, iOS 17+/macOS 14): wire + query/mutation/schema DSL + HTTP + reactive WS + SwiftUI `LiveQuery`; admin/presence/optimistic/in-memory-engine surfaces deferred (see its README's coverage table).
- [`dashboard/`](dashboard/README.md) — the operator console SPA (admin/operator UI served same-origin at `RTDB_STATIC_DIR`; consumes `ts-client`).
- [`cli/`](cli/README.md) — `rtdb` operator/CI binary (wraps `par-rt-db-client`).

[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) tracks parity vs. Convex, with per-row notes on which clients mirror each feature.

## FAQ
* Q: Do I need a Convex account?
  * A: No. par-rt-db is Convex-*inspired* but fully self-hosted — storage is your own Postgres 17.
* Q: Do I write server-side functions?
  * A: No. Queries and transactions are a declarative JSON DSL; there is no embedded JS runtime and no per-app server code — one generic server hosts many databases.
* Q: Can I use it over plain HTTP?
  * A: Yes — one-shot `POST /api/query` / `POST /api/mutate` cover reads and writes. The WebSocket (`/sync`) is only needed for live subscriptions and presence.
* Q: Which languages have client SDKs?
  * A: TypeScript (`@par-rt-db/client`, with React bindings), Rust (`par-rt-db-client`), Python (`par-rt-db`), and Swift (`ParRtDbClient`, iOS 17+/macOS 14), plus the `rtdb` CLI and the operator dashboard.
* Q: Is auth required?
  * A: Machine tokens are the baseline. Each of the six OAuth providers is independently optional (blank env ⇒ its routes return 503), and anonymous access is opt-in (off by default).
* Q: Can I run multiple replicas behind a load balancer?
  * A: Yes — set `RTDB_MULTI_INSTANCE=true`. Login state, op-feed, presence, rate budgets, and per-database write ownership (advisory-lock lease with kill-failover) coordinate via Postgres; a non-owner replica serves reads/subscriptions and forwards writes to the owning replica automatically (taking the lease itself if the owner dies within `RTDB_FORWARD_TIMEOUT_MS`). See [Known MVP limitations](#known-mvp-limitations) (ENH-022).

## Roadmap

### Where we are
* **Core**: realtime live queries, atomic multi-step transactions, typed schemas compiled to Postgres DDL, WebSocket + HTTP transports, many databases per instance
* **Query terminals**: `get`/`index`/`count`/`unique`/`first`/`take`, `filter` predicate DSL, full-text `search` (`ts_rank` + `trgm` mode), `vectorSearch` (pgvector), `hybridSearch` (RRF), `paginate` keyset pagination, `distinct`, `aggregate`
* **Beyond documents**: scheduling (`afterMs`/`runAt`/`cron`/`interval`), durable workflows, realtime presence, file storage with signed URLs and image transforms
* **Auth**: six optional OAuth providers, per-db machine tokens, optional anonymous with anon→real merge, per-row `ownerField`/`authorize` rules
* **Operations**: operator dashboard, `rtdb` CLI, op feed, audit log, webhooks, Prometheus metrics + optional OTLP tracing, backup/restore, schema migration with snapshot history, hot config, per-db quotas
* [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) is the authoritative Convex-parity contract

### Where we're going
* Multi-instance rate limiting and cross-process write funnelling — the remaining ENH-022 stages that make a multi-replica deploy safe
* The first tagged release (`v0.1.0`, ENH-026): lockstep client versions and the release process

## What's new

No release has been tagged yet — everything currently lives under `[Unreleased]` in
[`CHANGELOG.md`](CHANGELOG.md) (the eventual `0.1.0` cut is tracked as ENH-026). Recent
highlights from the unreleased section: grouped aggregates include the null group key,
wire-corpus semantic alignment across the server and the three in-memory client engines,
durable workflows, realtime presence, hybrid search, and schema migration with snapshot
history and restore.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, the `make checkall`
gate, Conventional Commits, the four-client wire-mirror rule, and the PR checklist.
[`CLAUDE.md`](CLAUDE.md) is the agent-facing companion with the full invariant list.

## License

MIT — see [`LICENSE`](LICENSE). Each package manifest (`server/Cargo.toml`,
`rust-client/Cargo.toml`, `ts-client/package.json`, `dashboard/package.json`,
`python-client/pyproject.toml`) declares the same MIT license.
