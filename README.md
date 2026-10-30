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
   * [Per-row public share grants](#per-row-public-share-grants)
   * [Admin: databases, schema, tokens, allowlist](#admin-databases-schema-tokens-allowlist)
   * [Admin: dashboard operator surface](#admin-dashboard-operator-surface)
   * [Auth (OAuth + sessions)](#auth-oauth--sessions)
* [Configuration](#configuration)
   * [Topology and security](#topology-and-security)
* [Error envelope](#error-envelope)
* [Wire protocol](#wire-protocol)
* [Pagination](#pagination)
* [Scheduling](#scheduling)
* [Durable workflows](#durable-workflows)
* [Change feed](#change-feed)
* [Computed fields](#computed-fields)
* [Append-only tables](#append-only-tables-insertonly)
* [Field constraints](#field-constraints)
* [Realtime presence](#realtime-presence)
* [Make targets](#make-targets)
   * [Verification gates](#verification-gates)
* [Graceful shutdown](#graceful-shutdown)
* [Multi-instance](#multi-instance)
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
with Postgres 17 storage. Internals:
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); the original design specs in
[`docs/superpowers/specs`](docs/superpowers/specs) are a historical record.

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
- **First-class SDKs**: TypeScript (with React bindings), Rust, Python, Swift (iOS/macOS), and Go clients that mirror the wire contract directly, plus the `rtdb` CLI and an operator dashboard

### Advanced Features
- **Full-text search**: websearch-syntax `search` ranked by `ts_rank` with optional snippets, plus a `trgm` mode for substring/autocomplete matching
- **Vector + hybrid search**: write-maintained pgvector columns ranked by the index's declared metric; `hybridSearch` fuses full-text and vector rankings via Reciprocal Rank Fusion
- **Scheduling**: one-shot (`afterMs`/`runAt`), 5-field `cron` (UTC by default, optional IANA `tz`), and fixed-interval (`everyMs`) transactions with cancel/pause/resume — scheduled work is data, not server code
- **Durable workflows**: multi-step specs with per-step retry, backoff, and sleep, plus `awaitSignal` approval gates that park a run until an out-of-band signal (or timeout); at-least-once per step with crash-resume
- **Server-stamped `updatedAt`**: a table may declare `updatedAtField` naming a `number`/`int64` field the server stamps with epoch-ms on every version-bumping write (insert/patch/replace/upsert/patchByQuery/cascade setNull), overwriting client-supplied values — no more hand-rolled timestamps in every mutation, and orderable with a declared index
- **Auto-increment counters**: a table may declare `autoIncrementField` naming an `int64` field the server assigns from a per-table Postgres sequence on insert (overwriting client-supplied values, immutable afterward, unique-indexable) — ticket/issue numbers with zero races; snapshot import continues numbering past the imported max, and gaps from rolled-back transactions are documented behavior
- **Computed fields**: a table may declare `computed`, a map of field name → typed expression (`concat`/arithmetic/`coalesce`/`lower`/`upper`/`trim`/casts/`case`/`now`), re-derived by the server on every write and stored in the doc body **and** the typed column — client-supplied values never survive, `null` results leave the key absent, the field stays indexable, and pushes backfill existing rows (see [Computed fields](#computed-fields))
- **Declarative field constraints**: a table may declare `constraints`, a map of field name → `min`/`max` (number/int64, inclusive) and/or `minLength`/`maxLength`/`pattern` (string; regex validated at push time), enforced on every write — a violating insert/patch/replace/upsert/patchByQuery fails with `SCHEMA_VIOLATION` and rolls back (see [Field constraints](#field-constraints))
- **Schema descriptions**: a table may declare `description` and `fieldDescriptions` (a map of declared field name → free text) — pure documentation metadata persisted with the schema and round-tripped through schema read-back, surfaced by the dashboard (schema page + data-browser column tooltips); no validation, no DDL, and description edits are ordinary additive pushes
- **Realtime presence**: transient room membership and state ("who is online", cursors, typing) over the existing `/sync` socket — no extra infrastructure
- **File storage**: opaque unauthenticated public URLs, signed time-limited URLs, HTTP `Range` support, and read-time image transforms
- **Auth**: six optional OAuth providers (GitHub, Google, GitLab, Microsoft, Apple, generic OIDC), per-database machine tokens plus optional host-scoped machine credentials with enrollment grants, optional anonymous access with anon→real account merge, and per-row `ownerField`/`authorize` rules
- **Operator surfaces**: op feed, audit log, webhooks, Prometheus `/metrics` with optional OTLP tracing, backup/restore, schema migration with snapshot history and restore, hot-reloaded config, per-database quotas, slow-query ring, and query explain

### Technical Excellence
- **Round-trip JSON float parsing**: the shared Rust JSON parser preserves finite `f64` values when decoding their serialized representation. Exact-bit tests guard fractional values that otherwise move by one floating-point unit, including values nested in document payloads.
- **Single serialized committer per database**: all writes flow through one committer task per database and reads run under READ COMMITTED (except `adjustCounter`, which row-locks its counter) — realtime correctness without distributed coordination
- **Rust on axum/tokio with Postgres 17 storage**: graceful shutdown drains in-flight requests before exiting, bounded by `RTDB_SHUTDOWN_DRAIN_MS`
- **One wire contract, six implementations**: the server and the ts/rust/python/swift/go clients all run the shared semantics corpus, staying byte-identical ([`wire-corpus/`](wire-corpus/README.md) — every top-level section is consumed by all six runners)
- **Security defaults**: constant-time key comparison, generic client-facing 500 messages (detail only in logs), typed `confirm` guards on destructive operations, path-traversal-guarded downloads

## Packages

| Package | Path | Stack | What it is |
| --- | --- | --- | --- |
| **Server** | [`server/`](server) | Rust (axum/tokio + Postgres 17) | The realtime database binary |
| **TypeScript client** | [`ts-client/`](ts-client) | TS (`@par-rt-db/client`, bun) | Browser/Node/React Native SDK + React bindings + in-memory test harness |
| **Rust client** | [`rust-client/`](rust-client) | Rust (`par-rt-db-client`) | Rust SDK: http + reactive ws + admin + `.filter()`/`.search()`/`.vector_search()` builders |
| **Python client** | [`python-client/`](python-client) | Python (`par-rt-db`, uv) | Python SDK: wire + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS |
| **Swift client** | [`swift-client/`](swift-client) | Swift 6 (`ParRtDbClient`/`ParRtDbUI`, SPM) | iOS 17+/macOS 14 SDK: wire + query/mutation/schema DSL + HTTP + reactive WS + SwiftUI `LiveQuery` (Darwin only) |
| **Go client** | [`go-client/`](go-client) | Go 1.23+ (`github.com/paulrobello/par-rt-db/go-client`) | Go SDK: wire + schema DSL + typed HTTP queries + reactive WS + admin client + in-memory engine |
| **Kotlin client** | [`kotlin-client/`](kotlin-client) | Kotlin 2.4 (JVM 17+ bytecode, Gradle) | JVM + Android (minSdk 26, via the OkHttp transport) SDK: wire + query/mutation/schema DSL + HTTP + reactive WS (`Flow`) + admin client + in-memory engine |
| **Dashboard** | [`dashboard/`](dashboard) | Vite + React 19 + TS (bun) | Operator console SPA served same-origin at `RTDB_STATIC_DIR` |
| **`rtdb` CLI** | [`cli/`](cli) | Rust (`rtdb` binary, cargo) | Operator/CI wrapper around `par-rt-db-client`: list/create dbs, push schema, query/mutate, mint/revoke tokens |

The server is the source of truth; the five SDKs (ts/rust/python/swift/go) each mirror
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
Subscribe/Mutate over an open WebSocket. Revocation liveness extends to pushes:
the subscription fan-out re-checks credential liveness per write batch — host
tokens via `authorize`, user sessions via an `EXISTS` on the session row — and
drops revoked/expired-session subscriptions before publication. The guarantee is
batch-scoped, not continuous: a revocation that commits while a batch is already
re-running can still deliver that batch's already-computed update (the same
one-op window the per-op checks accept); every subsequent batch excludes the
session. A transient liveness-lookup failure suppresses that batch's pushes for
session-backed subscriptions (fail-closed, like host tokens) without severing
the subscriptions. File storage is HTTP-only and bypasses the
committer (blobs don't touch document tables). See
[`server/README.md`](server/README.md) for the server layout and
[`CLAUDE.md`](CLAUDE.md) for the full invariant list.

## Prerequisites for running
* Postgres 17 — the repo's `docker-compose.dev.yml` starts one on `127.0.0.1:55434` via `make dev-db-up`, or point `RTDB_DATABASE_URL` at your own
* [Docker](https://www.docker.com/) — for the dev Postgres and for the production `docker compose` deploy path (see [`deploy/README.md`](deploy/README.md))
* A Rust `stable` toolchain to build the server binary from source ([`rust-toolchain.toml`](rust-toolchain.toml) is the single source of truth)
* `jq` for the Quickstart walkthrough
* Per client SDK: bun/node for TypeScript, cargo for Rust, Python 3.12+ (uv) for Python, Xcode with the Swift 6 toolchain for Swift (Darwin only), Go 1.23+ for Go

## Prerequisites for dev
* See [CONTRIBUTING's development setup](CONTRIBUTING.md#development-setup) for the full tool list and first-time setup
* A GNU-compatible `make` — every target spans all nine packages (swift-client's lines are Darwin-guarded and skip loudly on Linux); first-time installs are `make ts-client-install`, `make dashboard-install`, `make python-client-install`, and `make go-client-install` (see [Make targets](#make-targets))

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
  -d '{"db":"myapp","schema":{"tables":{"tasks":{"fields":{"title":{"type":"string"},"done":{"type":"boolean"}},"indexes":[{"name":"by_done","fields":["done"]}]}}}}'

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
make test   # dev-db-up + test suites across all nine packages (fmt/clippy/typecheck live in make checkall)
```

## Endpoints

Auth is one of: `none`, `Bearer token` (machine token or session via
`Authorization: Bearer <token>`), `Bearer admin key` (the server-wide
`RTDB_ADMIN_KEY`, or an OAuth session for a user listed in `rtdb_auth.admins`),
`Bearer session` (OAuth session token only), or `first WS frame` (the WS `auth`
message). The dashboard's WS to `/admin/stream` is a browser special-case: the
admin bearer rides in the `Sec-WebSocket-Protocol: rtdb-admin.<token>` subprotocol
since browsers cannot set headers on a WS handshake.

The JSON API and admin surface are gzip-compressed when the client sends
`Accept-Encoding: gzip` (responses at or under 1 KiB stay uncompressed; SSE and
`image/*` are skipped). The WebSocket upgrade at `/sync` is untouched by
compression.

### Health, realtime, queries, mutations, scheduling

| Method & path | Auth | Description |
| --- | --- | --- |
| `GET /openapi.json` | none | Machine-readable OpenAPI 3.1 description of the whole HTTP surface (`/api/*`, `/admin/*`, `/share/*`, `/storage/{id}`, `/auth/*`, `/healthz`/`/metrics`/`/sync`). Hand-maintained in `server/src/openapi.rs`; full field/required detail on the core data-plane request/response shapes, summary-level operations on the admin surface. Unauthenticated (shapes, not secrets) and served even in API-only mode. A drift test (`server/tests/openapi_drift.rs`) fails the build when a registered route is missing from the document (or a documented path is not registered), so the doc cannot go stale silently. |
| `GET /healthz` | none | Liveness: `{status:"ok"\|"degraded", started_at, uptime_seconds, postgres}`. 503 when Postgres is unreachable. The build fingerprint fields (`version`, `git_commit`, `build_timestamp`) are admin-only — included just when the request carries a valid admin bearer (SEC-129). |
| `GET /privacy` | none | par-rt-db's own privacy policy as static HTML (compile-time-embedded from `server/src/static/privacy.html`), so an OAuth consent screen's required privacy URL can point at the deployment itself. Public and stateless; works in API-only mode. |
| `GET /metrics` | none | Prometheus text-exposition scrape endpoint. Content-negotiated on `Accept`: a browser (`text/html`) is served the SPA's `index.html` when `RTDB_STATIC_DIR` is set; everything else (Prometheus sends `application/openmetrics-text`, curl, API-only deploys) gets Prometheus text. Aggregate-only (no per-db, no principal data), same posture as `/healthz`. The admin JSON snapshot stays at `GET /admin/metrics`. |
| `GET /sync` | first WS frame | WebSocket upgrade. Speaks the realtime protocol (auth, subscribe, mutate, schedule, presence, change-feed push, ping). |
| `POST /api/query` | Bearer token | One-shot query against a database; see [Query shape](#query-shape). |
| `POST /api/query-batch` | Bearer token | Fans out N queries in one round trip (per-query error isolation); each slot returns `{ok, result}` or `{ok:false, error}`. |
| `POST /api/mutate` | Bearer token | One-shot transaction (`insert`/`patch`/`replace`/`delete`/`undelete`/`expectVersion`/`expectFields`/`expectAbsent`/`expectNoMatch`/`expectBefore`/`upsert`/`adjustCounter` + bulk `insertMany`/`upsertMany` + `patchByQuery`/`deleteByQuery` + `schedule`/`cancelSchedule` + `startWorkflow`/`cancelWorkflow` steps). |
| `POST /api/mutate-batch` | Bearer token | Fans out N independent transactions in one round trip (per-entry error isolation, deliberately not atomic); each positional slot returns `{ok, results}` or `{ok:false, error}`. Entries may carry their own `idempotencyKey` (per-entry dedup/replay). |
| `POST /api/schedule` | Bearer token | Schedules a transaction: `afterMs`/`runAt` one-shot, `cron` (5-field, min-first, UTC unless `tz` is given), or `interval` (fixed `everyMs`); returns `{id}`. Pass `"external": true` to create an external-claim job (never internally executed; claimed by an app worker via `/api/schedule/claim` with a fencing token). |
| `POST /api/schedule/claim` | Bearer token | Atomically claims up to `limit` due external jobs (`leaseMs` lease each), returning `ClaimedSchedule[]` with per-job monotonic `leaseGeneration` fencing tokens. |
| `POST /api/schedule/{id}/{complete,retry,fail}` | Bearer token | Worker-owned finalize transitions for a claimed external job, each guarded by the claim's `lease` token — a stale token (job re-claimed after lease expiry) is `409 CONFLICT`. `complete` deletes a one-shot or advances a recurring job to its next due instant; `retry` re-arms at `now + delayMs` (default 60s) recording `error`; `fail` (requires `error`) parks the job in a terminal `error` state. |
| `POST /api/schedule/{id}/{cancel,pause,resume}` | Bearer token | Cancels, pauses, or resumes a scheduled job. |
| `POST /api/schedules` | Bearer token | Lists scheduled jobs for a database (`ScheduleInfo[]`). |
| `POST /api/workflows` | Bearer token | Starts a durable workflow run from a `WorkflowSpec` (FM-29); returns `{id}`. See [Durable workflows](#durable-workflows). |
| `POST /api/workflows/list` | Bearer token | Lists workflow runs for a database (`WorkflowInfo[]`, newest first; optional `status` filter). |
| `POST /api/workflows/{id}/cancel` | Bearer token | Cancels a non-terminal run (`{ok:false}` = unknown/terminal — a no-op, not an error). |
| `POST /api/workflows/{id}/signal` | Bearer token | Delivers a named signal to a `waiting` run (`{db, name, payload?}` → `{"delivered": true}`; releases an `awaitSignal` step). 404 unknown id; 409 not waiting or name mismatch; payload capped at 64 KiB serialized. |
| `GET /api/db/{db}/changes` | Bearer token | **Durable change feed** — machine tokens only. `?since=<seq>&table=<name>&limit=<n>` returns every committed document op strictly after the cursor (oldest first, post-images included, `doc:null` on deletes) plus `{nextSeq, head, logId}`. An impossible cursor is `410 CURSOR_EXPIRED` (resync from 0) — never silent data loss. See [Change feed](#change-feed). |
| `GET /api/db/{db}/schema` | Bearer token | The database's pushed `SchemaDef` — the machine-token twin of the admin schema read-back (serves tooling holding only a data-plane credential, e.g. `rtdb import --dry-run`). |

### File storage (HTTP-only, bypasses the committer)

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /api/storage/{db}` | Bearer token | Upload: raw body + `Content-Type` → `{ id, sha256, size, contentType }`. Enforces `RTDB_MAX_FILE_SIZE`. |
| `GET /storage/{id}` | **none** | **Unauthenticated public serve** — anyone with the opaque uuid-v7 URL fetches it (Convex parity; revoke by delete). The single unauthenticated route. |
| `GET /api/storage/{db}/{id}` | Bearer token | Authed, caller-db-scoped serve. |
| `DELETE /api/storage/{db}/{id}` | Bearer token | Deletes a blob (idempotent; revokes the public URL). **Principal-scoped**: a machine token deletes only blobs it uploaded (`machine:{token_id}` owner stamp) and an OAuth user only its own; reads stay db-open for machine-owned blobs (consumer references survive token churn), and `None`-owner legacy rows stay deletable by any db-scoped principal. Consumer rule: never split uploader and sweeper across tokens — run uploads and cleanup under one token, or use the admin `DELETE /admin/db/{db}/storage/{id}` route (admin bypasses per-row ownership). |
| `GET /api/storage/{db}/{id}/metadata` | Bearer token | `{ id, sha256, size, contentType?, creationTime }`. |
| `GET /api/storage/{db}/{id}/signed-url` | Bearer token | Mints a signed, time-limited public URL (`?exp=` + HMAC `?sig=`) for the blob. The mint is db-scoped: a caller authorized for db A cannot mint for a blob in db B (SEC-113). `?ttlSeconds=` clamps to `[1, MAX_SIGNED_URL_TTL_SECS]`; default `DEFAULT_SIGNED_URL_TTL_SECS`. The unauthenticated `GET /storage/{id}` route verifies `?exp=&sig=` when present (403 on failure) and behaves unchanged when absent (ENH-017). |

**Signed-only serving (SEC-113).** `RTDB_STORAGE_REQUIRE_SIGNED_URLS` (boot-only) makes `GET /storage/{id}` reject any request without a valid `?exp=&sig=` pair (403). `false`/unset (default) keeps opaque public URLs, `true` requires signatures for every database, and a comma-separated list of database names (case-insensitive, whitespace and empty entries ignored, e.g. `RTDB_STORAGE_REQUIRE_SIGNED_URLS=secure_docs,private`) requires them only for blobs in those databases while every other database stays public. A malformed value (anything that is neither a boolean nor valid database names) aborts startup. The scope is read back via `GET /admin/config` as `storageSignedUrls: { mode: "off" | "all" | "databases", databases: [...] }`. Share-minted signed URLs serve normally under every mode.

Both serve routes honor HTTP `Range` requests: `Range: bytes=...` → `206 Partial Content` with `Content-Range`/`Content-Length` (and `Accept-Ranges: bytes` advertised), `416` for an out-of-bounds range, and a `200` full body when no range is requested. Single-range only; on-the-fly image transforms (`?w=` etc.) skip range handling.

### Per-row public share grants

A share grant is an unguessable token that lets an **unauthenticated** visitor read **exactly one** granted document row — and, when the grant names a blob, mint a signed URL for **exactly that blob** — never anything db-wide. This is what backs a public share link (e.g. `/s/{token}` on an app backend) without that backend having to hold a machine token: the nearest prior primitive, `POST /auth/anonymous`, grants a full db-authorized principal (every ownerless table becomes readable, signed-url minting is db-scoped), so it can never scope a share to one row.

**Disabled by default.** Enable with `RTDB_SHARE_GRANTS_ENABLED=true`. Enabling exposes grant-targeted content to the public internet: mint grants only for content meant for public sharing. The token **is** the credential — the server mints 256 bits from the OS CSPRNG and stores only its SHA-256 hash (anything at 128+ bits of entropy is acceptable; never share a token across grants, and treat a minted token as you would a password). Revocation = `DELETE` of the grant (idempotent) or expiry (default 7 days, cap 30 days); expired grants are cleaned up lazily. Every unauthenticated failure — unknown, expired, or revoked token, deleted row or blob, flag off — returns the identical `401 {"code":"UNAUTHORIZED","message":"invalid share token"}` envelope, so the public surface is enumeration-free. The public routes share the per-IP `RTDB_STORAGE_RATE_LIMIT_PER_IP_RPM` budget.

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /api/db/{db}/share-grants` | Bearer token | Mint a grant: `{ table, id, blobId?, ttlSeconds? }` → `{ token, url, expiresAt }`. The table must be schema-declared and the row must exist; a named blob must live in the same db. Flag-gated (403 when off). The token is returned once — only its hash is stored. |
| `DELETE /api/db/{db}/share-grants/{token}` | Bearer token | Revoke (idempotent; db-scoped). |
| `GET /share/{token}` | **none** | Read the granted row (the doc). |
| `GET /share/{token}/signed-url` | **none** | Mint a signed URL for exactly the grant's `blobId` (`?ttlSeconds=` as usual). Grants without a blob get the generic 401. Composes with `RTDB_STORAGE_REQUIRE_SIGNED_URLS=true`: the share-minted signed URL is then the only way a visitor can fetch the granted blob. |

Worked example:

```bash
export RTDB_URL=http://localhost:8300
export MACH=<machine-token-for-db-mydb>
export ADMIN=<admin-key>

# 1. Operator turns the feature on: RTDB_SHARE_GRANTS_ENABLED=true (server restart).

# 2. Your backend (holding the machine token) inserts the row and mints a grant:
curl -s -X POST "$RTDB_URL/api/mutate" -H "Authorization: Bearer $MACH" \
  -H 'Content-Type: application/json' \
  -d '{"db":"mydb","txn":{"steps":[{"op":"insert","table":"pages","doc":{"title":"hello","body":"public copy"}}]}}'
# → {"results":[{"id":"01J...","table":"pages",...}]}

curl -s -X POST "$RTDB_URL/api/db/mydb/share-grants" -H "Authorization: Bearer $MACH" \
  -H 'Content-Type: application/json' \
  -d '{"table":"pages","id":"01J...","ttlSeconds":86400}'
# → {"token":"<64-hex>","url":"https://rtdb.example/share/<64-hex>","expiresAt":1766...}

# 3. Any visitor (no credentials) reads exactly that row:
curl -s "$RTDB_URL/share/<64-hex>"
# → {"_id":"01J...","title":"hello","body":"public copy"}

# 4. For a blob-backed share (grant minted with "blobId":"<storage-id>"):
curl -s "$RTDB_URL/share/<64-hex>/signed-url"
# → {"url":"https://rtdb.example/storage/<id>?exp=...&sig=...","expiresAt":...}

# 5. Revoke:
curl -s -X DELETE "$RTDB_URL/api/db/mydb/share-grants/<64-hex>" -H "Authorization: Bearer $MACH"
```

### Admin: databases, schema, tokens, allowlist

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /admin/login` | Bearer admin key | Validates the admin key and mints a short-lived admin session token (used by the dashboard's login form). |
| `POST /admin/logout` | Bearer admin key | Invalidates the admin session token. |
| `POST /admin/create-db` | Bearer admin key | Creates a new database. Returns `{ ok, creationId }` — a stable identity minted at creation and never reused for a recreated name (also exposed as `creationId` on `GET /admin/dbs/{db}/stats`). |
| `POST /admin/delete-db` | Bearer admin key | Deletes a database. Body `{ name, confirm, expectedCreationId? }` where `confirm` must equal the db name (typed guard). When `expectedCreationId` is given and differs from the db's current `creationId` (it was deleted and recreated), fails `CONFLICT` and deletes nothing; the check runs under the same row lock as the drop, so it is race-free. Retires the per-db committer/scheduler/reaper tasks cleanly. |
| `POST /admin/clone-db` | Bearer admin key | Clones an existing database's schema into a new db name (no data). |
| `POST /admin/push-schema` | Bearer admin key | Applies additive schema DDL to a database. |
| `POST /admin/db/{db}/schema/preview` | Bearer admin key | Previews the DDL a `push-schema` would run, without applying. |
| `POST /admin/db/{db}/migrate` | Bearer admin key | Runs an ordered `Directive` list (rename/coerce/remove/default-backfill) transactionally inside the committer; live queries, op feed, audit, and webhooks all fire. See [Schema migration](#schema-migration). |
| `GET /admin/dbs` | Bearer admin key | Lists all databases. |
| `GET /admin/dbs/{db}/schema` | Bearer admin key | Returns the pushed schema for a database. |
| `GET /admin/dbs/{db}/stats` | Bearer admin key | Per-table row counts and storage sizes; includes quota usage and caps (ENH-011). |
| `GET /admin/db/{db}/schema/json-schema` | Bearer admin key | Compiles the pushed schema to a JSON Schema (draft 2020-12) document, one `$defs` entry per table, for external codegen/validation. Plain JSON, no envelope. See [JSON Schema export](#json-schema-export). |
| `GET /admin/db/{db}/schema/history` | Bearer admin key | Lists schema-change snapshots newest-first (`?limit=`/`?offset=`). See [Schema change history](#schema-change-history). |
| `GET /admin/db/{db}/schema/history/{version}` | Bearer admin key | Returns one full schema snapshot by version. |
| `POST /admin/db/{db}/schema/restore` | Bearer admin key | Reconciles the db to a prior snapshot's shape. Body `{ version, confirm }`, `confirm` = db name. |
| `POST /admin/mint-token` | Bearer admin key | Mints a machine token scoped to one database. An optional `hostCapability` (e.g. `{"type":"parMeshV1","host":"macdaddy"}`) mints a host-restricted credential instead of an unrestricted one; omitting it keeps legacy semantics. See [Host machine authorization](#host-machine-authorization). |
| `POST /admin/revoke-token` | Bearer admin key | Revokes a machine token by its id. |
| `GET /admin/tokens?db=` | Bearer admin key | Lists machine tokens for a database (no secrets); each row carries `hostCapability` when the token has one. |
| `POST /admin/enrollment-grants` | Bearer admin key | Creates a host machine authorization enrollment grant. Body `{db, name, capability, expiresAt, tokenExpiresAt?}` — `expiresAt` must be in the future and at most 15 minutes out. The response `{grantId, grant, db, capability, expiresAt}` returns the plaintext grant **exactly once**; only its SHA-256 digest is stored. Redeemed by the host via the public `POST /auth/enroll`. |
| `POST /admin/revoke-enrollment-grant` | Bearer admin key | Revokes a pending enrollment grant by id (`{grantId}`); 404 for an unknown id. |
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

#### JSON Schema export

`GET /admin/db/{db}/schema/json-schema` returns `{ "$schema": "https://json-schema.org/draft/2020-12/schema", "$defs": { "<table>": {...} } }`
with one definition per table (reference one with `#/$defs/<table>`). Each definition describes a document
as read back from the server: the system fields `_id` (32-char lowercase hex), `_creationTime` and `_version`
(integers) plus the declared fields, `additionalProperties: false`, and `required` listing every field that
is not `optional`. Table `description` and `fieldDescriptions` become the JSON Schema `description` keyword;
`defaults` become `default`; `constraints` become `minimum`/`maximum`/`minLength`/`maxLength`/`pattern`.

| Schema type | JSON Schema |
| --- | --- |
| `string` | `{"type":"string"}` |
| `number` | `{"type":"number"}` |
| `boolean`, `null` | `{"type":"boolean"}`, `{"type":"null"}` |
| `int64` | `{"type":"string","pattern":"^-?[0-9]+$","format":"int64"}` — an exact decimal STRING, never a JSON number (the int64 wire convention, so values past 2^53 keep precision). A generated client must parse it into a 64-bit integer type. |
| `id` (table `T`) | `{"type":"string","pattern":"^[0-9a-f]{32}$","x-rtdb-table":"T"}` — JSON Schema cannot express referential integrity, so the target table is carried in the `x-rtdb-table` extension keyword and the description. |
| `bytes` | `{"type":"string","contentEncoding":"base64","pattern":...}` — base64 text. |
| `literal` | `{"const": value}` |
| `union` | `{"anyOf":[...members]}` |
| `optional` (`T`) | `{"anyOf":[T, {"type":"null"}]}` and the field is omitted from the parent's `required`. The server accepts both an absent key and an explicit `null`. |
| `array` | `{"type":"array","items":...}` |
| `object` | `{"type":"object","properties":...,"required":[non-optional keys],"additionalProperties":false}` |
| `record` | `{"type":"object","additionalProperties":<value type>}` |
| `vector` | `{"type":"array","items":{"type":"number"},"minItems":N,"maxItems":N}` |
| `any` | `{}` |

Unknown database returns the standard `NOT_FOUND` error envelope. The admin clients expose it as
`getJsonSchema` (TS, Swift, Kotlin), `get_json_schema` (Rust, Python sync + async), and `GetJSONSchema` (Go),
returning the document as untyped JSON.

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
| `GET|PATCH /admin/db/{db}/readonly` | Bearer admin key | Per-database read-only freeze. While frozen, every client-plane document write — WS, HTTP, mutate-batch, admin direct mutate, any principal — is rejected with `409 READ_ONLY`; reads, subscriptions, admin surfaces, and system writes (scheduled fires, workflow advances, TTL reaping) are unaffected. New scheduled jobs, workflow starts, and signal delivery are also rejected (they are future writes); an already-admitted transaction may still finish. An idempotent retry of an already-committed write still replays its cached result. |
| `GET|POST|DELETE /admin/db/{db}/quiesce` | Bearer admin key | Per-database cutover barrier (see [Per-database quiescence](#per-database-quiescence-cutover)). `GET` reads state `{quiesced, generation}`; `POST` closes client-write admission and drains already-queued committer work; `DELETE` reopens, requiring the `x-rtdb-quiesce-generation` header. Stale generations are `409`; state and generation persist across restart. Multi-instance deployments get `409` until replica coordination exists. |
| `GET /admin/db/{db}/storage` | Bearer admin key | Lists blobs stored in a database (id, sha256, size, contentType, createdAt). |
| `POST /admin/db/{db}/storage` | Bearer admin key | Uploads a blob (admin-scoped; same shape as `POST /api/storage/{db}`). |
| `DELETE /admin/db/{db}/storage/{id}` | Bearer admin key | Deletes a blob (admin-scoped). |
| `GET /admin/db/{db}/webhooks` | Bearer admin key | Lists webhooks configured for a database. |
| `POST /admin/db/{db}/webhooks` | Bearer admin key | Creates a webhook (target URL + event filter). Enabled when `RTDB_WEBHOOKS_ENABLED=true`. |
| `PUT /admin/db/{db}/webhooks/{id}` | Bearer admin key | Edits a webhook. |
| `DELETE /admin/db/{db}/webhooks/{id}` | Bearer admin key | Deletes a webhook. |
| `GET /admin/db/{db}/webhooks/{id}/deliveries` | Bearer admin key | Lists recent delivery attempts (outbox drain) for a webhook. |
| `POST /admin/db/{db}/webhooks/{id}/deliveries/{delivery_id}/redeliver` | Bearer admin key | Re-enqueues a failed (attempts-exhausted) delivery: resets attempts and re-pends it for the outbox drain. A pending/retrying/delivered delivery is `409 CONFLICT` (never double-fired); unknown id `404`. Returns the updated delivery row. |
| `POST /admin/db/{db}/webhooks/{id}/deliveries/redeliver-failed` | Bearer admin key | Bulk form: re-enqueues every failed delivery of the webhook, returning the reset rows in delivery-id order (empty array when none). |
| `GET /admin/metrics` | Bearer admin key | Live gauges and throughput. |
| `GET /admin/ops/recent` | Bearer admin key | Recent document-mutation op feed (durable). |
| `GET /admin/audit?db=&limit=&offset=` | Bearer admin key | Audit log entries (`ts_ms, db, table, op, doc_id, principal, source`) — only when `RTDB_AUDIT_LOG_ENABLED=true`; 404 otherwise. |
| `GET /admin/subscriptions` | Bearer admin key | Lists active subscriptions across all dbs (live query inspector). |
| `GET /admin/presence` | Bearer admin key | Live per-room presence inspector: one row per room with member count, serialized-state bytes, and the oldest member's join age. In multi-instance mode, `memberCount`/`stateBytes` merge in gossiped peer membership (best-effort, never authoritative — `mergedWithPeers` discloses it); `localMemberCount` is always this replica's own count. Also surfaced as `presenceDetail` on `GET /admin/metrics`. |
| `POST /admin/db/{db}/explain` | Bearer admin key | Query introspection (ENH-019): re-compiles a `Query` through the real path and returns `{sql, params, terminal, warnings}` — a plan, no rows. |
| `GET /admin/db/{db}/changes` | Bearer admin key or dashboard cookie | The retained change feed under the admin gate — identical cursor/`logId`/`410 CURSOR_EXPIRED` semantics as the data-plane feed, no machine token and no table allowlist (the admin gate grants the whole database). Backs the dashboard's document-history viewer. Mirrored in all five admin clients (`adminChanges` TS/Swift, `admin_changes` Rust/Python, `AdminChanges` Go). |
| `GET /admin/slow-queries` | Bearer admin key | Bounded in-memory ring of queries slower than `RTDB_SLOW_QUERY_MS` (default 0 = disabled; `RTDB_SLOW_QUERY_CAPACITY` default 200; `RTDB_SLOW_QUERY_LOG_PARAMS=false` keeps doc content out). |
| `GET /admin/sessions?user=&limit=` | Bearer admin key | Lists active interactive sessions (OAuth/anonymous/admin-key). `?user=` filters by `user_id` or email (omitted ⇒ all, server-wide); `?limit=` clamped to `[1, 1000]`, default 200. `token_hash` is a non-reversible sha256 digest, safe to surface. |
| `DELETE /admin/sessions/{token_hash}` | Bearer admin key | Revokes a single session by its `token_hash` (takes effect on the next op over an already-open connection — `session_still_valid` re-queries on every interactive Subscribe/Mutate/Presence). |
| `DELETE /admin/sessions?user=` | Bearer admin key | Revokes every session for a user. Requires exactly one scope — `?user=` or `?expired=true`; a bare `DELETE` (or both params) is a 400 (refuses to revoke every session instance-wide from one unscoped or ambiguous call). |
| `DELETE /admin/sessions?expired=true` | Bearer admin key | Revokes every EXPIRED session instance-wide (OAuth/anonymous and admin-key login rows alike — expired rows otherwise linger until each is used or individually revoked). Returns `{ok, revoked}` with the count swept; the dashboard's Sessions page exposes this as "remove all expired". |
| `WS /admin/stream` | Bearer admin key | Live op-feed stream over WebSocket (subprotocol auth for browsers). |
| `GET /admin/config` | Bearer admin key | Hot config, redacted (`admin_key`/OAuth secrets/`database_url` → configured-bools only). |
| `PATCH /admin/config` | Bearer admin key | Mutates the ten hot settings — `allowedOrigins`/`sessionTtlDays`/`maxFileSize`/`idempotencyTtlMs`/`maxTablesPerDb`/`maxStorageBytesPerDb`/`maxSubsPerDb`/`changeLogMaxRows`/`perTokenRpm`/`perDbRpm` (camelCase wire names; unknown fields are rejected); validates, persists to the `rtdb_config` row, swaps live (no restart). |
| `POST /admin/backup` | Bearer admin key | Spawns a manual `pg_dump` of the live DB (409 if one is already running). Always available — `RTDB_BACKUP_ENABLED` gates only the scheduled cron backups. |
| `GET /admin/backups` | Bearer admin key | Lists existing dump files. |
| `GET /admin/backups/{name}` | Bearer admin key | Downloads a dump file (path-traversal-guarded). |
| `DELETE /admin/backups/{name}` | Bearer admin key | Deletes a dump file. |
| `POST /admin/restore` | Bearer admin key | Restores a dump into a fresh `rtdb_restored_<stamp>` Postgres DB via `pg_restore --no-owner --no-privileges`. Body `{ name, confirm }`, `confirm` = name. The live `rtdb` DB is never touched (single-writer invariant intact). |
| `GET /admin/export-db?db=` | Bearer admin key | Snapshot export: schema line + one JSONL doc line per document. Streams (per-table server-side cursor; bounded server memory). Pages are independent queries — under concurrent writes the export can mix per-table time points; quiesce the database for a consistent point-in-time copy. |
| `POST /admin/import-db?db=` | Bearer admin key | Snapshot import: applies the schema line, replays each doc with original id/timestamp/version. The request body is consumed line-by-line — bounded server memory, no request size cap. A transport-interrupted body (declared-length/chunked framing violation) aborts and rolls back; a cleanly terminated body at a record boundary is indistinguishable from a complete snapshot without an out-of-band length/hash contract, so validate against one for authoritative restores. The schema push commits before the document transaction, so a failed import leaves rows rolled back but any schema change in place. Requires an empty target — importing into a database that already holds documents is `409 CONFLICT` (clone-db and restore always target fresh databases). |
| `POST /admin/merge-users` | Bearer admin key | Runs the anon→real account merge synchronously and returns the full report. Body `{ anonUserId, realUserId, confirm }`, `confirm` must equal `realUserId`; 404 when the anon row does not exist. Operator escape hatch (crash-window cleanup, manual consolidation) — the OAuth callback merges automatically on sign-in. |

#### Per-database quiescence (cutover)

`POST /admin/db/{db}/quiesce` closes one database for client-plane writes and
drains it for a safe cutover; `DELETE` with the `x-rtdb-quiesce-generation`
header reopens it. The operational contract:

1. **Fence.** Quiescing bumps a global, persisted admission epoch and stamps
   the database's `admission_fence_epoch` with it. Every write path — committer
   mutations (WS, HTTP, admin, forwarded), storage put/stream/delete,
   scheduled and workflow claims, TTL reaping — captures the epoch at
   admission and validates it against the fence inside its write transaction,
   after acquiring the per-db quiesce lock. A write admitted before the
   quiesce that has not finished by then is rejected with `409 READ_ONLY`
   even after release; the fence never rolls back.
2. **Drain.** After the fence is set, `POST` places a barrier behind every
   already-queued committer request for that database. When the call returns,
   every write that was mid-flight has committed and everything else already
   queued has been rejected — that is drained: the database's last accepted
   write is durable and no new admission can succeed. Reads, subscriptions,
   admin surfaces, and unrelated databases stay fully served.
3. **Release.** `DELETE` validates the generation while still holding the
   quiesce lease, so a stale header is `409` and can never release a newer
   session. State and generation persist across restart; an instance that
   comes back after the quiescing process died recovers the lease via
   `pg_try_advisory_lock` (the old owner's Postgres session ended with the
   process), so the operator can always release using the generation from
   `GET /admin/db/{db}/quiesce`.
4. **Scope.** Single-instance only for now: under `RTDB_MULTI_INSTANCE` both
   lifecycle endpoints return `409` until the quiesce generation is
   coordinated across replicas. The admission-fence machinery itself is
   instance-agnostic — a fence persisted out-of-band still excludes stale
   epochs on every replica, including forwarded writes executed by the owner.

### Auth (OAuth + sessions)

Six optional OAuth providers ship behind the shared `OAuthProvider` trait —
GitHub, Google, GitLab, Microsoft, Apple, and a generic OIDC provider — plus
hashed per-database machine tokens and a server-wide admin key. Each provider
is independently optional: leave its env vars blank and its routes return
`503`. See [`docs/OAUTH_SETUP.md`](docs/OAUTH_SETUP.md) for the per-provider
setup (callback URLs, scopes, env-var pairs).

| Method & path | Auth | Description |
| --- | --- | --- |
| `GET /auth/{provider}/begin?origin=` | none | Starts the OAuth flow for one of `github`/`google`/`gitlab`/`microsoft`/`apple`/`oidc`. Validates `origin` against the **live** `RTDB_ALLOWED_ORIGINS` (403 on miss), mints a single-use `state` token, sets the `SameSite=None;HttpOnly` `rtdb-oauth-csrf` nonce cookie, and returns JSON `{ authorizeUrl, state }`. The caller opens `authorizeUrl` in a `noopener,noreferrer` popup and polls `/auth/state` for completion. Optional `&mode=cookie` (SEC-207) marks a cookie-mode login: the `/auth/state` completion omits the session token entirely — the HttpOnly cookie set by the callback is the only credential carrier, so no script-readable copy ever exists. Optional `&mode=bearer` marks a bearer-delivery login (opt-in cross-tab hardening): the callback sets NO `rtdb_session` cookie and mints NO admin-CSRF nonce — the one-shot poll is the only credential carrier, so other same-origin tabs cannot adopt the session via the shared cookie, and revoking this login cannot disturb a pre-existing cookie-backed session. A bearer begin additionally returns `revokeSecret` in the response body — the client-only revocation capability (only its sha256 verifier is stored server-side; `state` alone transits the provider redirect URL and is not a capability). Any other `mode` value is a `400`. |
| `GET /auth/callback` | none (state token) | GitHub OAuth callback (the canonical path; Google/GitLab/Microsoft/OIDC use `GET /auth/{provider}/callback`). Constant-time-verifies the CSRF nonce cookie against `state`, claims the pending entry (replay → 400), exchanges the code, fetches the verified email, upserts `rtdb_auth.users`, sets the `HttpOnly` session cookie, and returns popup-closing HTML. |
| `POST /auth/apple/callback` | none (state token) | Apple-specific callback — Apple POSTs `code`+`state` via `response_mode=form_post`. Same CSRF/session handling as the GET path (the `SameSite=None` cookie survives the cross-site POST). |
| `GET /auth/state?state=` | none (state token + cookie) | Provider-agnostic poll endpoint keyed on the single-use `state` token. Returns `{ status: "pending" }` until the callback completes, then `{ status: "complete", token, user, sessionId?, expiresAt? }` (one-shot; for a `mode=cookie` begin the completion is `{ status: "complete", user }` — no `token` field, SEC-207; `sessionId` and `expiresAt` appear only for a `mode=bearer` begin — see `/auth/session/revoke`; `expiresAt` is the session's absolute TTL on server time, the instant the capability tombstone is swept — the bound for a client's retry loop, not a client-side proof (a client clock ahead of the server reaches it early; see `/auth/session/revoke` for the sound resolution rule)); `{ status: "expired" }` for a timed-out/already-claimed flow, `{ status: "error" }` for a failed login. SEC-121: the poll must also carry the `rtdb-oauth-state` cookie set at `/begin` with the same value (constant-time compared; a miss returns `expired`), so a leaked state URL alone cannot poll — cross-origin SDK login sends `credentials: "include"` on the `/begin` and poll fetches (that cookie is `SameSite=None`, unlike the `SameSite=Lax` session cookie). |
| `POST /auth/state/revoke` | none (state + revokeSecret) | Fenced cleanup for a login the caller gave up on. Requires `{state, revokeSecret}` — `state` transits the provider redirect URL, so state alone is not a revocation capability; the secret was delivered once in the bearer begin response. Scoped to **bearer-delivery** rows — a `default`/`cookie` state, a wrong or missing secret, or an unknown state all answer the same `unknown`-shaped body and are untouched. One Postgres transaction fences the row and deletes its tracked session; a crash between them rolls the fence back so a retry re-runs the cleanup. Response mapping (durable settlement, never inferred): `pending` → `{status:"revoked"}` (cancelled before claim); `claiming` → `{status:"unconfirmed"}` until the callback durably settles (`callback_settled_at`); `completed` → `revoked` with its tracked session verifiably deleted; already-`revoked` → `revoked` once settled (deleting any owed token first), `unconfirmed` before that — forever after a crash between the session INSERT and the settlement marker; `failed` → `unconfirmed` (ambiguous-insert is not provably clean). Any non-`revoked` outcome is treated as unconfirmed by clients. The route carries the same SEC-003-style per-IP fixed-window guard as `/begin` (30 rpm per IP) and a strict body-shape gate (4 KiB cap, object-with-string-fields) — shape-invalid bodies, oversize payloads, and limiter denials all answer the same `unknown`-shaped 200 without touching the fence. Sets no cookies, so it never disturbs a pre-existing `rtdb_session`. |
| `POST /auth/session/revoke` | none (sessionId + revokeSecret) | **Durable sign-out for one session** — the capability the 10-minute `/auth/state/revoke` window cannot provide. `{sessionId, revokeSecret}` where `sessionId` is the non-secret identifier the bearer-mode poll delivered alongside the token and `revokeSecret` is the same begin-time client-only secret (`state/revoke` uses); the server stores only its sha256 hash on the session row. Works for the session's actual lifetime: no 10-minute bound, no bearer required, no admin credential. Unauthenticated (capability = the pair), same SEC-003-style per-IP guard (30 rpm) and 4 KiB body gate as `/auth/state/revoke`, constant-time hash compare under a `FOR UPDATE` lock. `revoked` is a CONFIRMED fence: the matching live session was marked revoked (`revoked_at` — every resolver excludes it, so the token dies everywhere immediately) in the same transaction, or was already a tombstone (retry re-confirms), or was past expiry and was pruned in the same transaction. `unknown` = no match: never-stamped session (cookie/default logins, pre-upgrade rows), wrong or missing secret, never-minted identifier, or a row already removed by background cleanup (the 60s tombstone sweep or lazy expiry delete — a retry after a confirmed revoke can therefore turn `unknown` once the row is swept past TTL; safe, since expiry was already enforced by every resolver, but not evidentiary) — all indistinguishable (no existence leak). Any non-`revoked` outcome is treated as unconfirmed by clients. Tombstones are bounded by the session TTL and pruned on the background sweep; revoked rows keep their dead `token_hash` (PRIMARY KEY) and never reappear in `/admin/sessions` listings. Sets no cookies. **Resolution rule (skew-safe):** the bearer poll delivers `expiresAt` with `sessionId` — the tombstone-sweep instant on SERVER time. `revoked` is the only confirmation, and the server answers it even for an already-expired row it still holds (proving and pruning it), so a client retries until `revoked`; only after `expiresAt` plus a clock-skew margin AND continued `unknown` may it stop — resolved-unverifiable, never proven. (Local time alone is not proof: a client clock ahead of the server can pass `expiresAt` while the server still enforces its own expiry. `unknown` well before the bound is a genuine anomaly — wrong server, stamp failure, feature-less server.) |
| `POST /auth/logout` | Bearer session | Deletes the session for the given bearer token — unless it carries a durable revoke capability (`revoke_secret_hash` set, bearer-mode logins), in which case the row is tombstoned (`revoked_at` set) instead, preserving the anchor so a later `/auth/session/revoke` retry answers a confirmed `revoked` rather than `unknown`. The token dies identically either way; a repeat logout is a no-op on the tombstone. Idempotent: always 200 unless the delete query itself fails. The same anchor-preserving rule covers the admin `DELETE /admin/sessions/{hash}` and revoke-all paths. |
| `GET /auth/me` | Bearer session | Returns the authenticated user. 401 for a machine token (session only). |
| `GET /auth/validate` | Bearer token | Validates a presented session or machine token; returns the `AuthedUser`. Used by backends to check a player-supplied token. |
| `POST /auth/anonymous` | none | Mints an ephemeral anonymous user + session for a credential-less guest (gated by `RTDB_AUTH_ANONYMOUS_ENABLED`, default off ⇒ `403 FORBIDDEN`; a per-db toggle — `GET|PATCH /admin/db/{db}/anonymous-access`, SEC-103 — must also allow it). Sets the `HttpOnly` session cookie (browser path) **and** returns the plaintext session token in the body (SDK/bearer path — pass it as the WS/HTTP bearer, exactly like a machine token). Per-IP rate-limited; an anonymous user is authorized for any database via that boot gate (no allowlist entry) and owns its own documents via per-row `ownerField` (the anon `user_id`). On a later OAuth sign-in with that session's bearer presented at `/begin`, the anon footprint is merged into the real account — owned docs across all databases restamped, storage blob ownership swapped, the live session re-pointed — via `merge::merge_users` (`rtdb_merge_docs_total` counts restamped docs); `POST /admin/merge-users` is the operator escape hatch. |
| `POST /auth/enroll` | none (grant) | Redeems a one-time enrollment grant for a host-scoped machine credential — see [Host machine authorization](#host-machine-authorization). |

The live login flow (SEC-012): the browser hits `GET /auth/{provider}/begin` →
opens the provider authorize URL in a `noopener,noreferrer` popup (reverse-tabnabbing
defense — `window.opener` is severed) → the parent polls
`GET /auth/state?state=<token>` → the provider redirects to the callback →
the callback sets the `HttpOnly` session cookie and returns popup-closing HTML
→ the parent's next poll receives the session token and closes the popup
(in cookie mode the begin carries `&mode=cookie` and that poll carries **no**
token — the cookie is the credential, SEC-207; in bearer mode the begin carries
`&mode=bearer`, the callback sets **no** cookies, and that poll is the only
carrier).
Login-CSRF is defended by the `rtdb-oauth-csrf` double-submit cookie set at
`/begin` and constant-time-verified at `/callback`
(`RTDB_OAUTH_LOGIN_CSRF=true` by default; cross-origin SDK consumers must send
`credentials: "include"` on the `/begin` and `/auth/state` fetches — the poll
also requires the `rtdb-oauth-state` cookie, SEC-121). Identity is keyed on each
provider's stable subject (`github_id`, `apple_sub`, `microsoft_sub`,
`google_sub`, `gitlab_id`, `oidc_sub`), with the verified email linking an
account across providers on a first login.

Bearer tokens are either a per-database **machine token** (minted via `/admin/mint-token`,
optionally carrying a host capability) or a **session token** (minted by completing an OAuth
flow). Both resolve through the same
`Authorization: Bearer <token>` header on `/api/*`, `/auth/*`, and the WS `auth` frame. The
WS handler and admin re-auth paths re-run on every op (revocation and expiry take effect on
open connections — see [Known MVP limitations](#known-mvp-limitations)).

#### Host machine authorization

An optional, versioned **host capability** on a machine token —
`hostCapability: {"type":"parMeshV1","host":"macdaddy"}` — turns the credential into a
representation of one authenticated host: agents on the same host intentionally trust each
other, and the token's document access is restricted to the par-mesh `agents`/`messages`
contract below. The host follows par-mesh's component grammar
(`^[a-z0-9][a-z0-9-_]{0,31}$` — lowercase ASCII letters/digits, then lowercase
letters/digits/hyphens/underscores, at most 32 characters); unknown capability types and
extra capability properties fail validation. The database binding stays the token's existing
per-database scope: a restricted token cannot become an admin, and `readOnly`/`tables`
scoping still composes on top without broadening the capability. Legacy tokens (no
capability) keep their exact existing semantics. Spec:
[`docs/superpowers/specs/2026-10-21-host-machine-auth-design.md`](docs/superpowers/specs/2026-10-21-host-machine-auth-design.md).

**Enrollment.** `POST /admin/enrollment-grants` creates a grant fixed to a database, token
name, and capability, with an explicit `expiresAt` no more than 15 minutes out; the response
returns the plaintext `grant` exactly once and only its SHA-256 digest is stored — the same
one-time-plaintext contract as machine tokens. The host redeems it with the public,
unauthenticated `POST /auth/enroll`, whose body accepts **only** `{"grant": "..."}` (no
host, database, capability, or scope override). In one SQL transaction the server claims one
unexpired/unrevoked/unredeemed grant and inserts exactly one restricted machine credential,
returning `{tokenId, token, db, capability}` after commit. Concurrent redemptions of one
grant yield at most one success; an unknown, expired, revoked, or already-redeemed grant all
get the same generic 401. The grant plaintext and the minted token are never logged. If the
successful response is lost, the plaintext is unrecoverable by design: the admin issues
another grant and revokes the orphaned token via the existing `/admin/revoke-token` API.

**Restricted surface — `agents` and `messages` only, failing closed** when the pushed
schema lacks the expected fields:

- **Directory (`agents`)**: every scoped host can read the whole directory — scans,
  aggregates, direct-ID queries, and subscriptions. Insert and patch are own-host only: both
  the previous and the resulting row must belong to the authenticated host, `addr` must
  parse as `name@host:project` whose host exactly matches both the credential and the row's
  `host` (with `name`/`project` matching the row's declared values), and wildcard agent
  names are rejected. A patch cannot transfer an agent between hosts. All other agent
  mutation forms — replace, delete, undelete, upsert, counters, bulk operations — are
  denied.
- **Messages**: a message is readable by the recipient host (`host`) or the host parsed from
  `from_addr`; invalid or inconsistent routing rows are invisible to restricted readers.
  Insert (cross-host sends are explicitly permitted) requires a concrete, valid `from_addr`
  belonging to the authenticated host; a concrete, valid `target` whose parsed host equals
  the row's `host`; a valid `to_addr` (concrete or wildcard) whose host and project match
  the target; `state = "queued"`, `delivered_ms = 0`, `acked_ms = 0`, `ack_note = ""`; and a
  valid priority (`next-turn` or `interrupt`). Routing, content, sender, message identity,
  trace, and `_creationTime` become immutable to restricted tokens after insertion. Only the
  recipient host may patch a message, with exactly one of two receipt shapes: queued →
  delivered (`state = "delivered"` plus a finite positive `delivered_ms`) or queued/delivered
  → acked (`state = "acked"` plus a finite positive `acked_ms` and string `ack_note`).
  Rewriting a completed receipt, extra fields, arbitrary state transitions, and sender-side
  forged receipts are rejected. Every other message mutation variant is denied.
- **Everything else is denied** for a `parMeshV1` credential: schedules and workflows (nested
  and standalone), presence, storage/blob operations, change feeds, and all other
  non-document surfaces. `expectFields`/`expectVersion`/`expectAbsent` preconditions enforce
  the same read visibility (an invisible row is indistinguishable from absent) without
  conferring mutation rights. The existing live machine-token revocation/expiry gate covers
  every REST and WS operation **including pushes into already-open subscriptions** — a
  restricted subscription whose token is revoked or expired stops receiving data, not just
  losing the ability to issue new requests.

**Wildcard grammar (contractual under `parMeshV1`).** In `to_addr`, `*` is valid only in the
name position (`*@host:<project>`). A wildcard project (`*@host:*`) is rejected by design:
routing requires `to_addr` to carry the same concrete project as the message's `target`, and
the read-visibility lane applies the same rule. Clients expand a broadcast across projects
into one message per concrete `*@host:<project>` scope.

Deployment rollout constraints (legacy-token bypass, the all-replicas-upgraded
prerequisite, and the mutation replay-cache generation bump): see
[deploy/README.md](deploy/README.md#host-machine-authorization-rollout).

## Configuration

The server reads its configuration from environment variables (prefix `RTDB_`).
[`.env.example`](.env.example) is the comment-annotated template for a
container deploy — copy it to `.env` and edit. It covers the knobs the compose
file forwards, so a few that the process reads directly (`RTDB_DATABASE_URL`,
`RTDB_PORT`) are set elsewhere in `docker-compose.yml` and do not appear there;
`server/src/config/` is the complete list. The subset below is what most
operators tune on first boot:

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `RTDB_DATABASE_URL` | yes | — | Postgres connection string. |
| `RTDB_ADMIN_KEY` | yes | — | Server-wide admin bearer (constant-time compared). Generate with `openssl rand -hex 32`. |
| `RTDB_PORT` | no | `8300` | HTTP/WS listen port. |
| `RTDB_PUBLIC_URL` | no | `http://localhost:8300` | Public origin (OAuth callback base, external links). |
| `RTDB_ALLOWED_ORIGINS` | no | empty | Comma-separated browser origins; also the exact-match CORS allowlist for `/api/*` and `/auth/*`. **Hot-reloadable.** |
| `RTDB_STATIC_DIR` | no | unset | Directory of static SPA build artifacts. Set/unset existing dir ⇒ dashboard served same-origin at the catch-all route fallback; unset/empty/missing ⇒ API-only. |
| `RTDB_GITHUB_CLIENT_ID` + `RTDB_GITHUB_CLIENT_SECRET` | no | none | GitHub OAuth app credentials — both required to enable GitHub login. The other five providers (Google, GitLab, Microsoft, Apple, OIDC) follow the same `RTDB_<PROVIDER>_CLIENT_ID` + `_CLIENT_SECRET` pair pattern; see `.env.example` and [`docs/OAUTH_SETUP.md`](docs/OAUTH_SETUP.md). |
| `RTDB_OTEL_ENABLED` | no | `false` | OpenTelemetry/OTLP tracing master switch (ENH-018). Requires the `otel` cargo feature — the shipped Docker image builds with it, source builds must pass `--features otel`. `false` (default) produces zero OTLP calls. Paired with `RTDB_OTEL_ENDPOINT` (default `http://127.0.0.1:4317`), `RTDB_OTEL_SERVICE_NAME` (default `par-rt-db`), and `RTDB_OTEL_SAMPLE_RATIO` (default `0.05`). A bearer-gated collector also needs the standard `OTEL_EXPORTER_OTLP_HEADERS` var (crate-native; the deploy-compose forwards it). See [Tracing](deploy/README.md#tracing-opentelemetry--otlp-enh-018). |
| `RTDB_LOG_FORMAT` | no | `text` | Stdout log output format. `text` (default) keeps the historical human formatter; `json` makes every stdout log line a single JSON object so container log pipelines (Loki/Datadog) ingest stdout without regex parsing. Boot-only; an unrecognized value fails boot. |
| `RTDB_FORWARD_CONCURRENCY` | no | `64` | ENH-022 Stage 4c / ARC-008: caps how many forwarded-write executions a replica runs concurrently. A forwarded write that arrives once the cap is saturated gets an immediate retryable `RATE_LIMITED` reply instead of executing. Only meaningful with `RTDB_MULTI_INSTANCE=true`. |
| `RTDB_RATE_LIMIT_EXACT` | no | `false` | ARC-007: multi-instance-only (`RTDB_MULTI_INSTANCE=true`). `false` (default) has each replica count `RTDB_RATE_LIMIT_PER_TOKEN_RPM`/`RTDB_RATE_LIMIT_PER_DB_RPM` locally and reconcile with `rtdb_auth.rate_counters` every `RTDB_RATE_LIMIT_SYNC_MS` instead of a synchronous Postgres round trip per request. Overshoot is bounded by roughly (number of replicas × one sync window) of extra allowance — every replica can admit up to its own local budget before its next flush reconciles the shared count — and a request is never denied below the configured limit, only possibly over-admitted within that bound. `true` keeps the pre-ARC-007 exact path: one synchronous UPSERT per checked request, no overshoot. |
| `RTDB_RATE_LIMIT_SYNC_MS` | no | `1000` | ARC-007: how often the approximate limiter (`RTDB_RATE_LIMIT_EXACT=false`) flushes local deltas into Postgres and refreshes its shared-count view; clamped to >= 50. Unused when `RTDB_RATE_LIMIT_EXACT=true` or in single-instance mode. |
| `RTDB_RATE_LIMIT_EXEMPT_DBS` | no | empty | Boot-pinned capacity exemption: every request against a listed database (comma-separated names) skips the per-token and per-db ceilings entirely — the limiter checks it before charging either bucket. The per-IP abuse guards and the WS per-connection frame cap are never exempted. Changing the list takes a restart; the ceilings themselves stay hot via `PATCH /admin/config` (`perTokenRpm`/`perDbRpm`). |
| `RTDB_RATE_LIMIT_OAUTH_BEGIN_PER_IP_RPM` | no | `0` (disabled; shipped `.env.example`/compose default `30`) | Per-IP rate limit on the unauthenticated `GET /auth/{provider}/begin` login route (SEC-003). Without it a single IP can mint OAuth state rows in a loop. `0` disables. |
| `RTDB_SHARE_GRANTS_ENABLED` | no | `false` | Per-row public share grants: when `true`, a db-authorized caller can mint an unguessable single-row share token and an unauthenticated holder reads exactly the granted row or mints a signed URL for exactly the granted blob. **Risk:** enabling exposes grant-targeted content to the public internet — mint grants only for content meant for public sharing; the token is the credential (256-bit, stored only as a SHA-256 hash; revocation = `DELETE` or expiry). See [Per-row public share grants](#per-row-public-share-grants). |

### Topology and security

These are boot-only (not hot-reloadable) and matter as soon as the server sits
behind a proxy or runs as more than one replica.

| Variable | Default | Description |
| --- | --- | --- |
| `RTDB_TRUSTED_PROXY` | `false` | When true, the server trusts `CF-Connecting-IP` / `X-Forwarded-For` for per-IP rate-limit keys and `X-Forwarded-Proto` for cookie `Secure` and HSTS. Leave it `false` on a directly reachable deploy, or a caller can rotate its own rate-limit bucket with a spoofed header. **Set it `true` behind the Cloudflare tunnel**, where every request arrives from the tunnel's address. |
| `RTDB_COOKIE_SECURE` | `true` | Sets the `Secure` attribute on the session cookie. Only turn it off for plain-HTTP local development. |
| `RTDB_MULTI_INSTANCE` | `false` | Master switch for cross-replica coordination — see [Multi-instance](#multi-instance). |
| `RTDB_INSTANCE_ID` | auto-generated | Stable replica id. Set it when you want log lines and notify payloads to name a replica you recognize. |
| `RTDB_FORWARD_TIMEOUT_MS` | `5000` | How long a non-owner waits for the lease owner's reply before attempting takeover. Clamped to a 100 ms floor. |
| `RTDB_POOL_MAX_CONNECTIONS` | `75` | Postgres pool ceiling. One committer task plus N subscription re-runs per database, so a many-database instance needs headroom. |
| `RTDB_ADMIN_EMAILS` | empty | Comma-separated emails seeded into `rtdb_auth.admins` at startup, so an OAuth login can reach the dashboard before any admin exists. |
| `RTDB_BACKUP_ENABLED` | `false` | Enables scheduled `pg_dump` backups. Paired with `RTDB_BACKUP_CRON` (default daily at 03:00 UTC), `RTDB_BACKUP_DIR`, and `RTDB_BACKUP_RETENTION` (default `7` dumps kept). Defaults and semantics live in `server/src/config/`. |
| `RTDB_SHUTDOWN_DRAIN_MS` | `30000` | Upper bound in milliseconds on the graceful-shutdown drain; `0` waits forever (the behavior before this knob existed). See [Graceful shutdown](#graceful-shutdown). |


The hot-reloadable settings (live on `AppState` as `Arc<ArcSwap<HotConfig>>`,
seeded from env at first boot, persisted in a single-row `rtdb_config` table,
and swapable via `PATCH /admin/config` without a restart): `allowed_origins`,
`session_ttl_days`, `max_file_size`, `idempotency_ttl_ms`, the three
per-database quota caps `max_tables_per_db` / `max_storage_bytes_per_db` /
`max_subs_per_db` (`0` = unlimited, ENH-011), `change_log_max_rows`
(durable change-feed retention), and the rate-limit ceilings
`per_token_rpm` / `per_db_rpm` (`0` = unlimited). **Once any `PATCH /admin/config` has been
applied, the persisted row is overlaid onto env on every boot — env edits to
these ten settings stop taking effect; change them via `PATCH /admin/config`
or the dashboard Settings page.** The full set of boot-time vars —
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
| `UNSUPPORTED_PROTOCOL`| 400         | Requested `protocolVersion` (WS `auth` frame or `X-Rtdb-Protocol` HTTP header) is newer than the server's `PROTOCOL_VERSION` (ARC-013). |
| `UNAUTHORIZED`        | 401         | Missing/invalid/expired token or session. |
| `FORBIDDEN`           | 403         | Per-row `ownerField`/`authorize` denial; anonymous auth disabled; signed-URL verification failure. |
| `NOT_FOUND`           | 404         | Unknown document/file/route; cross-db storage mismatch is reported as 404 (no existence disclosure). |
| `CONFLICT`            | 409         | `UNIQUE` index violation (`unique_violation`, SQLSTATE 23505) at `CREATE UNIQUE INDEX` time or on a colliding write. |
| `PRECONDITION_FAILED` | 409         | `expectVersion` / `expectFields` / `expectAbsent` / `expectNoMatch` / `expectBefore` guard failed. |
| `SCHEMA_VIOLATION`    | 422         | Schema push / type-coercion rejection. |
| `RATE_LIMITED`        | 429         | Per-token or per-db rate limit hit (HTTP and WS frames). Carries `retryAfter`. The WS connection stays open for this error; the separate per-connection frame-rate limit below closes it instead. |
| `INTERNAL`            | 500         | Unexpected server error (generic message; detail logged via `tracing`). |
| `CURSOR_EXPIRED`      | 410         | Change-feed cursor older than the retention window or ahead of the log; resync from `since=0` (see [Change feed](#change-feed)). |
| `QUOTA_EXCEEDED`      | 507         | Per-database resource cap hit (tables / storage bytes / concurrent subscriptions — ENH-011). |
| `READ_ONLY`           | 409         | The database is frozen read-only (`PATCH /admin/db/{db}/readonly`); client-plane document writes are rejected until it is unfrozen. |

## Wire protocol

### Query shape

`{"table": "<name>", "get"?, "index"?, "eq"?, "order"?, "take"?, "unique"?, "first"?,
"count"?, "filter"?, "search"?, "vectorSearch"?, "hybridSearch"?, "paginate"?, "distinct"?, "aggregate"?, "fields"?, "lookups"?}`
— exactly one terminal per query (terminals are mutually exclusive), except
that `paginate` pages a ranked terminal rather than replacing it, so it may
accompany `search`/`vectorSearch`/`hybridSearch` (see
[Pagination](#pagination)). See
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
`ILIKE` ranked by `pg_trgm` `similarity()`, backed by a GIN trigram index; the
index **must** declare `trgm: true` at push time — a search index without the
flag is rejected with `BAD_REQUEST` ("does not declare trgm: true");
two optional ranking knobs, both omitted from the wire when unset so existing
requests stay byte-identical: `rankMode: "ts_rank" | "ts_rank_cd"` (tsquery
mode only; `ts_rank_cd` is cover-density ranking and rewards tighter term
proximity, `ts_rank` is the default) and `minSimilarity` (trgm mode only, a
number in `0..=1`; hits whose `similarity()` is below it are dropped). A knob
used in the wrong mode, or `minSimilarity` outside `0..=1`, is `BAD_REQUEST`),
`vectorSearch` ranks by
the index's declared metric distance over a write-maintained pgvector column
(also accepting an optional full `filter`), `hybridSearch` fuses the full-text
(`ts_rank`) and `vectorSearch` rankings for the same table into one list via
Reciprocal Rank Fusion, `paginate` is opaque-cursor keyset pagination,
`distinct` returns the unique values of the index field after the `eq` prefix
(ascending; NULLs included, sorted last), and `aggregate` runs a scalar
`sum`/`min`/`max`/`avg`/`count`, optionally grouped (`groupBy`) by the next
index field (rows missing the group value form one `key: null` group,
sorted last) — or, wire-v2, several named ops in one pass via an
`aggregates: {alias: op, …}` map, and `groupBy` widened to an explicit list of
declared index fields for composite grouping (`count per (status, priority)`);
the map form returns `{alias: value, …}` ungrouped or `[{keys, values}]` rows
grouped, and composes with the legacy single-`op` + field-list combination the
same way. Aggregate result forms: `sum`/`min`/`max` over an `int64` index
field return the exact decimal string — the int64 wire convention, exact past
2^53 where a JSON number is an IEEE-754 double (a `sum` of satoshis or byte
counts is not silently rounded); `avg` over any numeric field stays a JSON
number (documented f64 form — a fractional mean has no int64 representation);
`count` is an integer; a `number` field's aggregates are JSON numbers. A grouped
aggregate may carry `having: {name: {eq?, ne?, gt?, gte?, lt?, lte?}}`, a
post-group filter over aggregate values (`count >= 2`, `sum > 100`): `name` is an
`aggregates` alias or, for a single-`op` spec, the lowercase op name; bounds on
one name and across names AND together; it compiles to SQL `HAVING` on the
server and post-filters identically in every in-memory engine. `having` needs a
grouped spec, a numeric aggregate (`count`, or `sum`/`avg`/`min`/`max` over a
numeric field), at least one bound per name, and a known name — otherwise
`BAD_REQUEST`. Failing groups are dropped before the `MAX_TAKE` group cap, and
a NULL aggregate never satisfies a bound. An optional `fields` array projects each result doc to the
listed user fields — the system fields (`_id`/`_creationTime`/`_version`) and
synthetic fields (`_searchSnippet`) are always kept, `fields: []` is an
ids-only view, names are validated against the schema (`BAD_REQUEST` on an
unknown field), it composes with every doc-bearing terminal (`count`/
`distinct`/`aggregate` return no docs, so it is a no-op there), and a
projected subscription does not push when a write changes only non-projected
fields (the pushed payloads still carry `_version`).

An optional `lookups` array performs server-side lookup expansion — an
N+1-free id join on the read DSL. Each `{"field", "table", "as", "cap"?}`
spec resolves a declared `Id`/`Optional<Id>`/`Array<Id>` field (whose
`Id.table` must equal `table`) on every result doc into the referenced
document(s) of `table`, exposed under the `as` key on the same doc: `null`
for the single form (field null/absent, dangling id, or an unauthorized row)
and an array in the id-array's declared order for the `Array<Id>` form
(dangling/unauthorized ids dropped, truncated to `cap` — default 100, hard
max 1000, above is `BAD_REQUEST`; at most 4 lookups per query). Depth-1 only,
id-equality only. Every joined row passes the exact per-row authorization the
parent read applies (`ownerField`/`collaboratorsField`/`authorize`), so an
invisible row reads as absent rather than erroring (an authorization failure
on one joined child must never freeze a live subscription). Joined docs carry
the child table's system fields, so a client can patch/subscribe against a
joined doc's identity. Legal with every doc-returning terminal (`get`,
`unique`, `first`, `take`/collect, `paginate`, `search`, `vectorSearch`,
`hybridSearch`) and rejected `BAD_REQUEST` with the doc-less ones (`count`,
`distinct`, `aggregate`); the `fields` projection may name lookup aliases
(omitting an alias drops the joined docs; full docs remain the default).
Subscriptions see joined keys as part of the change-detection canonical: a
child-table write that changes joined output re-runs the subscription and
pushes; a re-run where nothing visible changed pushes nothing. Mirrored in
all five clients (ts `QueryJson.lookups` + `.lookup()` builder, rust via the
shared core type); pinned by the `lookup-*` semantics-corpus cases.

### Transaction shape

`{"steps": [...]}` where each step is tagged by `"op"`: `insert`, `patch`,
`replace`, `delete`, `undelete` (softDelete tables only — clears the
`deleted_at` stamp), `expectVersion`, `expectFields`, `expectAbsent`, `expectNoMatch`, `upsert` (per-id, one
document each), the bulk import steps `insertMany` and `upsertMany` (see
below), `adjustCounter` (atomically adds a safe-integer delta to a
declared numeric field, with optional `min`/`max` bounds and `expected` row
fields — see [`docs/atomic-counter.md`](docs/atomic-counter.md)), the
predicate-driven bulk steps `patchByQuery` and
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

`expectFields(table, id, expected)` checks exact values at selected object-key paths without writing the live row or advancing its version. Unasserted fields may change. Missing and null remain distinct, selected objects compare in full, and existing visibility/table scopes apply. See [row-field guards](docs/transaction-field-guards.md) for bounds, ordering, retries and SDK methods.

`expectAbsent` and `upsert` resolve rows through the named index and require **full-index equality**: `eq` carries exactly one value per declared index field, in declaration order. Unlike queries — whose `eq` prefix binds may be followed by range predicates — a prefix is not accepted, and an arity mismatch fails with `BAD_REQUEST` (e.g. "index 'by_room_actor' expects 3 eq value(s), got 2") before any step executes. To guard on a shorter key, declare a second nonunique index over exactly those fields.

The bulk import steps carry many documents in one step, so an import is no longer bounded by one step per document:

- `{"op":"insertMany","table":t,"docs":[...]}` inserts every doc in order, validated and indexed exactly like `insert` (defaults, stamps, computed fields, constraints, unique indexes, per-row auth). The step result is `{"ids":[...]}` in doc order.
- `{"op":"upsertMany","table":t,"index":i,"docs":[...]}` resolves each doc through the named index's **full equality**, reading the eq values from the doc's own indexed fields (a doc missing one fails `BAD_REQUEST`), then inserts the doc or merges it into the single match like `upsert` with `insert == patch == doc`. The step result is `{"ids":[...],"inserted":[bool,...]}` in doc order.
- **Row cap.** At most `MAX_BY_QUERY_ROWS` (1000) docs per step; more fails `BAD_REQUEST` before any doc is written. Each doc counts toward the admin `RTDB_MAX_AFFECTED_DOCS` budget and the 10,000-row per-transaction ceiling.
- **Atomicity.** Any invalid document (schema violation, unique conflict, constraint failure, authorization) aborts the whole transaction with that document's typed error, rolling back the earlier docs of the step and every other step.
- v1 scope: a step carries documents only, with no interleaved preconditions. Mirrored in all five clients (`insertMany`/`upsertMany` builders and in-memory engines) and pinned by the `txn-insert-many-*` and `txn-upsert-many-*` corpus cases.

The `expectNoMatch` step (`{"op":"expectNoMatch","table":...,"filter":{...}}`) is a predicate-driven precondition: if any live row in `table` that the caller can see matches `filter` (the same `FilterExpr` the by-query steps accept, including `olderThan`), the whole transaction aborts with `PRECONDITION_FAILED` and the fixed message "filter matched an existing document". Otherwise it writes nothing and its step result is `null`. Steps run in order, so an `insert` earlier in the same transaction counts. Client builders expose the idiom as `insertIf(table, doc, { expectNoMatch: filter })`, which emits `expectNoMatch` followed by `insert`. Because the committer serializes writes per database, concurrent overlapping inserts (for example room bookings) admit exactly one. Notes:

- **Visibility, not global exclusivity.** Only rows visible to the caller count (soft-deleted rows, rows hidden by `ownerField`/`collaboratorsField`, and rows failing `authorize` are ignored). On an owner-scoped table two users can therefore both pass for the same predicate. For cross-owner exclusivity use an unscoped table or a machine/scheduled principal, which sees every row.
- **Step cap.** `expectNoMatch` counts toward the same 16-step per-transaction cap as `patchByQuery`/`deleteByQuery`; a seventeenth fails with `BAD_REQUEST` before any step runs.
- **Index the predicate fields.** The probe is `SELECT 1 ... LIMIT 1` inside the single-writer turn. An unindexed filter scans the table, bounded only by `LIMIT 1`, the step cap, and the 60s statement timeout.
- Host-capability (par-mesh) principals are denied this step.

The `expectBefore` step adds an exclusive server-clock epoch-ms deadline. The earliest guard is checked after queue wait and at the final commit decision, after asynchronous work. Expiry rolls back the complete transaction with `PRECONDITION_FAILED`; malformed deadlines fail with `BAD_REQUEST`. Combine the stored lease deadline with room-version and ownership guards when fencing a lease. See [transaction deadlines](docs/transaction-deadlines.md) for scheduling, replay and SDK semantics.

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

**Connection limits.** The socket is closed, not just error-replied, when a
connection crosses one of these:

| Limit | Value | Close code |
| --- | --- | --- |
| Frame size | 64 KB per frame | `4400` |
| Frame rate | 200 frames per rolling 10 s window | `4400` |
| Malformed or out-of-order frame | — | `4400` |
| Auth failure (bad, missing, or expired credentials) | — | `4401` |
| First frame is not `auth` | 10 s handshake timeout | `4401` |

`4400` and `4401` are in the WebSocket private-use range. `4401` is kept
distinct so a client knows not to blind-retry with the same credentials. A
connection whose outbound backlog exceeds 1024 unsent frames, or that stalls a
single send for 30 s, is also dropped.

`auth` and the HTTP API (`X-Rtdb-Protocol` request header, honored on both the
per-db `/api/*` routes and the `/admin/*` control plane) both accept an
optional `protocolVersion` (ARC-013) so a client can declare the wire version it
speaks. Omitting it is backward-compatible (treated as version `1`); a client
requesting a version *newer* than the server's is
rejected with `UNSUPPORTED_PROTOCOL` (400) instead of a generic `deny_unknown_fields`
400, so a version skew is diagnosable. All five SDKs send the header on every
HTTP call — data plane and admin alike. When the `auth` frame carries
`protocolVersion`, `authOk` echoes the server's `PROTOCOL_VERSION` back (also
omitted otherwise, so an older client's `authOk` bytes never change). Version
3 also gates the presence `presenceDelta` frame server-side: the presence
flush emits incremental deltas only to connections that negotiated at least
version 3 (`PRESENCE_DELTA_MIN_VERSION`); every other connection keeps
receiving full `presenceSnapshot` frames, so a pre-v3 SDK is never sent a
frame type it cannot parse:

```jsonc
// -> client: authenticate with a machine token scoped to db "myapp"
{"type": "auth", "token": "<machine-token>", "db": "myapp", "protocolVersion": 3}

// <- server
{"type": "authOk", "user": {"kind": "machine", "email": null, "name": null}, "protocolVersion": 3}

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

Keyset pagination is supported via the `paginate` query terminal, over an index
scan or over any of the ranked terminals. A page request carries an opaque
cursor (omitted for the first page) and a page size; the response is
`{docs, nextCursor}`, where `nextCursor` is omitted when there is no next page.
The cursor encodes the sort-column values of the last row on the page, so the
server resumes strictly *after* it — stable under concurrent inserts/deletes
unlike offset pagination.

`paginate` composes with `index`, `eq`, range bounds (`gt`/`gte`/`lt`/`lte`),
and `order`, and is mutually exclusive with `get`, `take`, `unique`, `first`,
and `count`. `numItems` is capped at 4096 (`MAX_TAKE`).

### Paginating a ranked terminal

`paginate` also composes with `search`, `vectorSearch`, and `hybridSearch`.
(Before ENH-030 the combination was rejected outright: the ranked terminals
return early from query compilation and never reached the paginated scan, so
there was no way to page a relevance-ranked result at all.) It composes the way
`take` already composes with `search` — it does not change which terminal runs,
only how many rows come back and whether the result is a page envelope. The
keyset runs over the terminal's own ranking key plus the `created_at`/`id`
tie-breakers that make the order total, so pages are non-overlapping and their
concatenation is exactly the unpaginated ranking:

| Terminal | Ranking key | Page order |
| --- | --- | --- |
| `search` | `ts_rank` (or `similarity` in `trgm` mode) | relevance desc |
| `vectorSearch` | the index's metric distance | nearest first |
| `hybridSearch` | the RRF fused score | score desc |

For `vectorSearch` and `hybridSearch` the terminal's own `limit` keeps its
meaning — the size of the ranked candidate pool — and `numItems` slices that
pool into pages, so paging delivers the same top-`limit` results the
unpaginated call returns, in instalments. `search` has no `limit` of its own
(`take` is mutually exclusive with `paginate`), so a paged search walks the
whole match set.

```jsonc
// First page of a relevance-ranked search
{"table": "notes", "search": {"index": "search_body", "query": "postgres"},
 "paginate": {"numItems": 20}}

// Next page — feed the returned nextCursor back in
{"table": "notes", "search": {"index": "search_body", "query": "postgres"},
 "paginate": {"cursor": "<opaque-cursor>", "numItems": 20}}

// Top 100 nearest neighbours, 20 at a time
{"table": "docs", "vectorSearch": {"index": "by_embedding", "vector": [/* … */], "limit": 100},
 "paginate": {"numItems": 20}}
```

A paginated `vectorSearch` adds `created_at`/`id` tie-breakers to the page
ordering, which the unpaginated form does not have — without a total order a
distance tie makes "resume after this row" ambiguous. They are applied only to
the already-materialized candidate pool, so the unpaginated query's plan (and
its use of the HNSW index) is unchanged.

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
    .run(
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
HTTP client runs the built query against `POST /api/query` and returns the
`Paginated` result. Install with the extras quoted so zsh doesn't glob the
brackets:

```bash
uv add "par-rt-db[http] @ git+https://github.com/paulrobello/par-rt-db#subdirectory=python-client"
# or: pip install "par-rt-db[http] @ git+https://github.com/paulrobello/par-rt-db#subdirectory=python-client"
```

See [`python-client/`](python-client).

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
- `{type: "cron", expr, tz?}` — fire on a 5-field standard cron expression (min-first,
  e.g. `"*/5 * * * *"` = every 5 minutes). Fields are evaluated in UTC unless the
  optional IANA `tz` (e.g. `"America/New_York"`) is given; an unknown zone is
  `BAD_REQUEST`. The server validates `expr`.
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

#### External jobs (app-worker claims with fencing tokens)

Pass `"external": true` on any schedule create (HTTP, WS, or the `schedule`
txn step) to create a job the server **never executes itself**. An application
worker claims due external jobs over HTTP and executes them externally — the
pattern for actions the server cannot run (third-party HTTP calls, native
push, anything imperative):

```bash
# Create an external job.
curl -s -X POST http://localhost:8300/api/schedule \
  -H "Authorization: Bearer <machine-token>" -H "Content-Type: application/json" \
  -d '{"db": "myapp", "external": true,
       "when": {"type": "afterMs", "ms": 0},
       "txn": {"steps": [{"op": "patch", "table": "orders", "id": "o1",
                          "doc": {"status": "notify-customer"}}]}}'

# A worker claims due jobs (limit/leaseMs optional; lease 1s–24h, default 5min).
curl -s -X POST http://localhost:8300/api/schedule/claim \
  -H "Authorization: Bearer <machine-token>" -H "Content-Type: application/json" \
  -d '{"db": "myapp", "limit": 8, "leaseMs": 300000}'
# {"jobs":[{"id":"<id>","kind":"oneshot","dueAt":...,"txn":{...},
#           "leaseGeneration":1,"leaseDeadlineMs":...}]}

# Finalize with the claim's fencing token (stale token → 409 CONFLICT).
curl -s -X POST http://localhost:8300/api/schedule/<id>/complete \
  -H "Authorization: Bearer <machine-token>" -H "Content-Type: application/json" \
  -d '{"db": "myapp", "lease": 1}'
```

The claim atomically assigns the per-job monotonic `leaseGeneration` (starts at
1, +1 per claim) and stamps `leaseDeadlineMs`; the job is re-claimable only
once the lease expires. Every finalize transition
(`complete`/`retry`/`fail`) is rejected `409 CONFLICT` once a newer generation
exists, so a worker that stalls past its lease cannot finalize over its
replacement — and a restart's recovery (`reset_running`) never resets external
rows or generations, so a live lease survives a server restart while the
former owner's token stays stale after any re-claim. Recurring external jobs
advance to their next due instant on `complete`; `fail` is terminal (revive via
`cancel` + re-create; `resume` only lifts `paused`).

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

Aggregate queries (`aggregate`, including the `aggregates` map and `groupBy`
forms) subscribe live over `/sync` like row queries: the subscription pushes a
`queryUpdate` with the new scalar, `{alias: value}` object, or grouped rows only
when a write moves the result. Grouped results are capped at `MAX_TAKE` (4096)
groups, ordered by group key ascending, and a capped result says so: it is
`{"groups": [...], "truncated": true}` instead of the bare row array (the
marker is omitted under the cap, so under-cap bytes are unchanged). A client
can therefore tell a live subscription is missing groups past the cap. With
`having`, the cap applies after the filter, so `truncated` reflects only the
groups that pass it.

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
with the enclosing writes). All five clients mirror the surface:
`startWorkflow` / `cancelWorkflow` / `listWorkflows` / `signalWorkflow`
(ts, rust, python, swift, go — reactive + HTTP) plus admin variants, the
`rtdb workflows` CLI (incl. `rtdb workflows signal`), and the dashboard
Workflows page (which shows waiting runs and can send the signal). Spec:
[`docs/superpowers/specs/2026-08-15-workflows-design.md`](docs/superpowers/specs/2026-08-15-workflows-design.md)
+ [`docs/superpowers/specs/2026-08-21-workflow-await-signal-design.md`](docs/superpowers/specs/2026-08-21-workflow-await-signal-design.md).

### TypeScript client

```ts
import type { WorkflowSpec } from "@par-rt-db/client";
import { awaitSignal } from "@par-rt-db/client";

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

## Change feed

HTTP-only clients (machines, agents, cron jobs) can stay current without a
WebSocket by polling the durable per-db change feed. Every committed document
write — interactive mutations, scheduled txns, workflow steps, TTL expiry,
anon→real merge, and admin snapshot imports — stamps one row per written
document inside the same Postgres transaction as the write, under a gap-free
per-db sequence.

```bash
# First poll (or full resync): the oldest retained ops.
curl -s "http://localhost:8300/api/db/myapp/changes?since=0" \
  -H "Authorization: Bearer $TOKEN"

# Subsequent polls resume from the last acknowledged cursor.
curl -s "http://localhost:8300/api/db/myapp/changes?since=$NEXT_SEQ&limit=500" \
  -H "Authorization: Bearer $TOKEN"
```

```json
{
  "ops": [
    { "seq": 41, "table": "tasks", "docId": "d41", "kind": "patch",
      "doc": { "title": "Buy milk", "done": true }, "ts": 1758300000000 },
    { "seq": 42, "table": "tasks", "docId": "d42", "kind": "delete", "doc": null,
      "ts": 1758300000001 }
  ],
  "nextSeq": 42, "head": 57, "logId": "0a1b2c3d4e5f6071"
}
```

The consumer contract:

- **Exactly once per cursor position.** Re-reading a cursor returns the same
  page (idempotent retries); every op has exactly one seq in commit order.
  Loop `while nextSeq < head`, then poll again later.
- **`nextSeq` is the last returned seq only on a full page**; a short page
  means the log is exhausted up to `head` and `nextSeq = head` — a filtered
  (`table=`) page therefore still advances the consumer.
- **`doc: null`** means no visible end state (delete, txn-local
  insert+delete, or a payload-less migrate backfill): re-fetch the id; a 404
  (or an invisible soft-deleted row) resolves it as deleted.
- **`logId`** changes if a database was dropped and recreated under the same
  name — seqs restart at 0 and the old cursor is meaningless; resync.
- **Expired cursors are typed**: a cursor older than the retention window
  (newest `RTDB_CHANGE_LOG_MAX_ROWS` rows kept, default 100 000, PATCHable
  via `/admin/config` `changeLogMaxRows`) or ahead of the log returns
  `410 CURSOR_EXPIRED` — an empty success would be silent data loss.
- **Auth**: machine tokens only, filtered by the token's table allowlist the
  same way every other executor surface is. Read-only tokens are fine.
- **Admin twin**: `GET /admin/db/{db}/changes` serves the same retained log
  under the admin gate (admin key or dashboard cookie session) — identical
  cursor/`logId`/`CURSOR_EXPIRED` semantics, no machine token and no table
  allowlist (the admin gate grants the whole database). It backs the
  dashboard's document-history viewer.

All five client SDKs expose the feed (`listChanges` in TS, `changes()` in
Rust/Python/Swift/Go) and the admin twin (`adminChanges` in TS/Swift,
`admin_changes` in Rust/Python, `AdminChanges` in Go).

### Pushed change feed over WebSocket

A machine-token consumer that already holds a `/sync` socket can receive the
same durable feed as pushed frames instead of polling. The channel reads the
durable `changes` log directly (the identical rows `GET /api/db/{db}/changes`
serves), so a resume cursor is honored across reconnects and server restarts.

```jsonc
// -> open (or resume) a feed. since = last PROCESSED seq; omitted = current head.
{"type": "feedSubscribe", "feedId": "f1", "since": 41,
 "feedEpoch": "0a1b2c3d4e5f6071", "tables": ["tasks"]}

// <- one frame per batch (oldest first). The first frame always arrives, with
//    zero ops if nothing is pending, to confirm the feed and carry feedEpoch/head.
{"type": "feedEvents", "feedId": "f1", "feedEpoch": "0a1b2c3d4e5f6071",
 "ops": [{"seq": 42, "table": "tasks", "docId": "d42", "kind": "patch",
          "doc": {"title": "Buy milk"}, "ts": 1758300000001}],
 "head": 57}

// -> flow control: everything with seq <= 42 is processed
{"type": "feedAck", "feedId": "f1", "seq": 42}

// <- terminal failure for this feed (the socket stays open)
{"type": "feedErr", "feedId": "f1",
 "error": {"code": "CURSOR_EXPIRED", "message": "cursor 3 predates the retention window ..."}}

// -> {"type": "feedUnsubscribe", "feedId": "f1"}
```

- **At-least-once, dedupable.** A consumer that reconnects with its last
  *processed* seq receives exactly the ops after it, so ops it received but
  never processed are redelivered. `(feedEpoch, op.seq)` is the stable identity:
  `feedEpoch` is the log's `logId`. The SDKs drop already-seen seqs for you.
- **Server-side table filter.** `tables` (omitted when empty = all) is applied
  in the log query, so non-matching ops are never sent. It is intersected with
  the token's table allowlist; a request naming only tables the token cannot
  see yields no ops, never a widened feed.
- **Replay bounds.** Replay is bounded by the change log's retention window
  (`RTDB_CHANGE_LOG_MAX_ROWS`, default 100 000), not by the in-memory op-feed
  ring. A cursor older than the window, ahead of the log, or paired with a
  `feedEpoch` that does not match (database recreated) answers `feedErr` with
  `CURSOR_EXPIRED` (the code the HTTP feed uses as `410`). Resync over
  `GET /api/db/{db}/changes?since=0`, then open a new feed from its `nextSeq`.
- **Backpressure.** The server keeps at most 2 000 unacknowledged ops in
  flight per feed and pauses until a `feedAck` arrives.
- **Authorization.** Machine tokens only (a user or admin session gets
  `FORBIDDEN`), re-checked per batch so revocation stops the stream; the
  per-token RPM bucket is charged once per `feedSubscribe`. Host-capability
  tokens cannot open feeds, matching the HTTP route.
- **Read side only.** No new write path; the committer is untouched.

Client APIs: TS `subscribeChanges()` / `changes()` (async iterable), Rust
`watch_changes()` (`ChangeFeed::next`), Python `watch_changes()` (async
iterator), Swift `watchChanges()` (`AsyncThrowingStream`), Go `WatchChanges()`
(channel), Kotlin `watchChanges()` (`Flow`). Each re-sends `feedSubscribe` with
its last delivered `(feedEpoch, seq)` on reconnect, acks each batch, and treats
`feedErr` as terminal (no auto-retry).

## Append-only tables (`insertOnly`)

A table may declare `"insertOnly": true` (omitted on the wire when false). Such a
table is append-only for transaction steps: `insert` and `insertMany` pass, while
`patch`, `adjustCounter`, `replace`, `delete`, `undelete`, `upsert`, `upsertMany`,
`patchByQuery`, and `deleteByQuery` targeting it fail with the existing
`SCHEMA_VIOLATION` code (no new error code) and roll the transaction back. Reads,
indexes and queries are unchanged, and the flag is not compared by
destructive-change detection (adding or removing it is an additive push).

Schema-driven and operator paths stay sanctioned:

- **TTL expiry** still reaps rows from an `insertOnly` table.
- **`onDelete` cascade is exempt (decision).** Deleting a parent row on a regular
  table still removes (`cascade`) or nulls (`setNull`) its `insertOnly` children,
  because the cascade is declared in the schema and walks outside the per-step
  gate. Only a *direct* modifying step on the `insertOnly` table is rejected, so
  declare `onDelete: restrict` on the reference if children must never be removed
  by a parent delete.
- **The admin owner-bypass mutate route** (`POST /admin/db/{db}/mutate`, which the
  dashboard uses, and the admin WS bypass) is exempt, so operators can still
  correct or purge rows. Machine tokens, users, and scheduled jobs are gated.

`insertOnly` and `softDelete` are redundant together (every delete is rejected, so
nothing can be stamped); `undelete` is rejected like any other modifying step.
Mirrored in all seven implementations (server plus the ts/rust/python/swift/go/
kotlin clients' schema DSLs and in-memory engines); see the `insert-only-*` /
`error-insert-only-*` cases in [`wire-corpus/semantics/`](wire-corpus/semantics/)
and [FEATURE_MATRIX #48](FEATURE_MATRIX.md).

## Field constraints

A table may declare `constraints`, a map of field name → constraint set, and
the server enforces it on the **write path only** — every write that stores or
merges a constrained field (insert, patch, replace, upsert both branches,
`patchByQuery`, and snapshot import) fails with `SCHEMA_VIOLATION` and rolls
back the transaction when a non-null value violates one. Reads, indexes, and
query results are unchanged: constraints type nothing, they only gate writes
(the read/write index-value typing is untouched).

Per type:

- `number`/`int64` accept `min`/`max` — **inclusive** bounds (`v >= min &&
  v <= max`; a value exactly at either boundary passes). `int64` values are
  decimal strings on the wire; bound compares are exact i64, and the declared
  bounds must be integers (a fractional bound is a push error).
- `string` accepts `minLength`/`maxLength` (inclusive, counted in Unicode
  scalar values) and `pattern` — an **unanchored** regex search (`is_match` /
  `RegExp.test` semantics; declare `^…$` anchors in the pattern itself when
  you want a full match).

`Optional` fields check their non-null values; `null` and absent keys never
violate. Push-time validation rejects a **misapplied** constraint (e.g. `min`
on a string, `maxLength` on a number) and an **invalid regex** with
`SCHEMA_VIOLATION` naming the field, crossed bounds (`min > max`,
`minLength > maxLength`), and a constraint key naming an undeclared field. A
`defaults` entry must also satisfy its field's constraints, so a default can
never invalidate new documents. Migrate stays consistent: `renameField` re-keys
the entry, `dropField` removes it, and `changeType` re-validates — a retype
that would leave the entry misapplied is rejected at plan time, and every
coerced row value is re-checked at apply time.

Constraints are **write-path only**: they change no stored shape, are not
compared by destructive-change detection (adding or removing one is an
additive push), and mirror across all six implementations (enforced in every
client's in-memory engine). See the `constraint-*` / `error-constraint-*`
cases in [`wire-corpus/semantics/`](wire-corpus/semantics/) for the pinned
cross-engine semantics and [FEATURE_MATRIX #43](FEATURE_MATRIX.md).

```bash
# Declare constraints: score 1..10 inclusive, counter 1..100, name 2..5 chars matching ^[a-z]+$.
curl -s -X POST http://localhost:8300/admin/push-schema \
  -H "Authorization: Bearer $RTDB_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","schema":{"tables":{"tasks":{"fields":{"name":{"type":"string"},"score":{"type":"number"},"counter":{"type":"int64"}},"indexes":[{"name":"by_name","fields":["name"]}],"constraints":{"score":{"min":1,"max":10},"counter":{"min":1,"max":100},"name":{"minLength":2,"maxLength":5,"pattern":"^[a-z]+$"}}}}}}'

# A violating write fails atomically with SCHEMA_VIOLATION naming the field.
curl -s -X POST http://localhost:8300/api/mutate \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"db":"myapp","txn":{"steps":[{"op":"insert","table":"tasks","doc":{"name":"ok","score":11,"counter":"1"}}]}}'
# {"error":{"code":"SCHEMA_VIOLATION","message":"field 'score' violates its 'max' constraint"}}
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
[FEATURE_MATRIX #39](FEATURE_MATRIX.md) and the `computed-*` cases in
[`wire-corpus/semantics/`](wire-corpus/semantics/).

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
`leave_presence`.

Those SDK method names are not the wire tags. On the wire the client sends:

| Frame | Fields | Purpose |
| --- | --- | --- |
| `presence` | `room`, optional `state` | Join a room, optionally publishing initial state. |
| `presenceState` | `room`, `state`, optional `ttlMs` | Replace this connection's state in the room. `ttlMs` sets how long the entry survives without a refresh. |
| `leavePresence` | `room` | Leave the room. |

The server answers with `presenceSnapshot` (`room`, `members`) or
`presenceErr`. There is no `presenceOk` frame. When presence is disabled, the
server replies with a `FORBIDDEN` `presenceErr` ("presence not enabled").

A connection that negotiated `protocolVersion` ≥ 3 on its `auth` frame also
receives `presenceDelta` frames (`room`, `seq`, `joined`, `left`,
`stateChanged`) instead of a full snapshot once the server knows it has seen
the room — its first broadcast is always a full snapshot, every later one a
delta. `seq` is a per-room counter: a client that observes a gap knows it
missed a delta and should re-join the room to force a fresh snapshot. Empty
delta arrays are omitted from the wire. Connections that omitted
`protocolVersion` or negotiated below 3 keep receiving full `presenceSnapshot`
frames, so the delta rollout never breaks an older SDK.

## Make targets

Every sweep target spans **all ten packages** — `core` (`par-rt-db-core`,
the shared Rust wire vocabulary), `server`, `ts-client`, `rust-client`,
`python-client`, `swift-client`, `go-client`, `kotlin-client`, `dashboard`, and `cli`. The swift-client lines
in every sweep are Darwin-guarded: on Linux they print `Skipping swift-client
(non-Darwin host)` and a macOS CI job runs `make swift-client-checkall`
instead. The [`Makefile`](Makefile) holds the canonical commands; this section
is the canonical *description* of them, and other documents in this repo link
here rather than restating it.

### First-time install (per package)

| Target | Packages | Purpose |
| --- | --- | --- |
| `make ts-client-install` | ts-client | `bun install` in `ts-client/` and `ts-client/docs-toolchain/`. |
| `make dashboard-install` | dashboard | `bun install` at repo root + `dashboard/`. |
| `make python-client-install` | python-client | `uv sync --all-extras` in `python-client/`, so pyright can resolve the optional `http`/`ws` deps. |
| `make go-client-install` | go-client | `go mod download` in `go-client/`. |

The Cargo workspace members (`core/`, `server/`, `rust-client/`, `cli/`) and
`swift-client/` have no install target — `cargo` and SwiftPM fetch on first
build.

### Sweep targets

| Target | What runs in each package |
| --- | --- |
| `make build` | core/server/rust-client/cli `cargo build` (rust-client and cli with `--all-features`) · go-client `go mod download` · dashboard `bun run build` · swift-client `swift build` (Darwin). Runs `ts-client-build` first, because the dashboard resolves `@par-rt-db/client` from the gitignored `ts-client/dist`. |
| `make fmt` | workspace `cargo fmt --all` (core/server/rust-client/cli) · ts-client/dashboard `bun run fmt` · python-client `uv run ruff format .` · go-client `gofmt -w .` · swift-client `swiftformat .` |
| `make fmt-check` | the same commands in check mode (`--check`, `gofmt -l`, `swiftformat --lint`). |
| `make lint` | workspace `cargo clippy --all-targets --all-features -- -D warnings` (core/server/rust-client/cli) · ts-client/dashboard `bun run lint` · python-client `uv run ruff check .` · go-client `go vet ./...` (also `-tags live`) · swift-client `swiftlint --strict` |
| `make typecheck` | ts-client/dashboard `bun run typecheck` · python-client `uv run pyright` · go-client `go vet ./...` · swift-client `swift build`. The Rust crates are covered by `make lint`'s clippy run, which already fails on everything `cargo check` would. Runs `ts-client-build` first. |
| `make test` | workspace `cargo nextest run --workspace --all-features` when `cargo-nextest` is installed, else `cargo test --workspace --all-features` (core/server/rust-client/cli — the flag is required, or cargo silently skips rust-client's feature-gated test targets) · ts-client/dashboard `bun run test` · python-client `uv run pytest -q` · go-client `go test ./...` · swift-client `swift test`. Runs `dev-db-up` first. |

### Verification gates

`make checkall` is the **definition of done**: it must pass before every
commit. It runs these stages in order, and a failure in any stage stops the
run.

| Stage | What it checks | Standalone target |
| --- | --- | --- |
| 1. `env-drift-check` | Every `RTDB_*` var read by the server or documented in `.env.example` is forwarded to the container by `docker-compose.yml`'s `environment:` block. The block is an explicit allowlist, so a `.env`-only key silently does nothing. | `make env-drift-check` |
| 2. `dockerfile-stub-check` | Every `[[test]]` declared by a non-server workspace member has a matching placeholder in the Dockerfile's dependency-caching layer. Without it `make deploy` fails at cargo's manifest parse — a break the rest of `checkall` cannot see. | `make dockerfile-stub-check` |
| 3. `backup-persistence-check` | The rendered `docker-compose.yml` mounts a persistent volume at `/backups` for the server, so scheduled dumps survive a container restart. | `make backup-persistence-check` |
| 4. `hostname-leak-check` | Runs when a local, gitignored `.hostname-leak-patterns` file exists (skips silently in CI, where it doesn't): no tracked file may match the compiled hostname patterns, so a scrubbed hostname cannot quietly reappear in a doc or commit. | `make hostname-leak-check` |
| 5. `cli-docs-check` | `cli/README.md` matches what `gen-cli-docs` regenerates, so the CLI reference cannot drift from the CLI. | `make cli-docs-check` |
| 6. `docs-api` | API docs regenerate cleanly for every SDK (`rust-client-doc`, `ts-client-doc`, `python-client-doc`, `swift-client-doc`), with warnings denied, so doc comments cannot rot. | `make docs-api` |
| 7. `fmt-check` | Formatting across all nine packages. | `make fmt-check` |
| 8. `lint` | Clippy under `-D warnings`, biome, ruff, `go vet`, swiftlint `--strict`. | `make lint` |
| 9. `typecheck` | `tsc`, pyright, `go vet`, `swift build`. | `make typecheck` |
| 10. `test` | The full suite across all nine packages, against the dev Postgres. | `make test` |
| 11. `rust-client-check-features` | The rust-client library **and** its test targets compile under every meaningful feature combination, not just `--all-features`. | `make rust-client-check-features` |

Never `--no-verify` past the gate. If you do, fix the gate before anything
else.

### Other workflow targets

| Target | Purpose |
| --- | --- |
| `make ts-client-build` | Builds `ts-client/dist` (gitignored). Required on a fresh or stale checkout before `typecheck`/`build`; both pull it first. |
| `make dev-db-up` / `dev-db-down` | Start/stop the dev Postgres on `127.0.0.1:55434` via `docker-compose.dev.yml`, waiting for health. `make test` depends on `dev-db-up`. |
| `make dev-db-clean` | Drops leaked test artifacts from the dev `rtdb` DB — per-test databases (`db_t<uuid-v7>`; tests self-clean via RAII, but a bounded tail leaks per binary) and the semantics-corpus runner's per-case databases (`sc_<case>_<hex>`). Each `DROP` runs in psql autocommit so a large sweep does not accumulate catalog locks. Scoped to those patterns; it never touches `rtdb`, `rtdb_auth`, or real databases. |
| `make cli-docs` | Regenerates the CLI reference in `cli/README.md` in place. |
| `make rtdb-cli` | Builds the `rtdb` CLI release binary (`cli/`, wraps `par-rt-db-client`). |
| `make dashboard-test` | Runs the dashboard's Vitest + React Testing Library suite standalone. |
| `make pre-commit` / `pre-commit-update` | `pre-commit run --all-files` (gitleaks, private-key detection, per-package fmt/lint) and `pre-commit autoupdate`. |
| `make deploy` | `checkall` → rsync to the Docker host → `BUILDX_BUILDER=par-rt-db-builder docker compose up -d --build` → healthz probe. |
| `make docs-api` / `bench-micro` / `bench` / `bench-baseline` | Regenerate the four SDK API references; run the criterion micro-benchmarks and the black-box load benchmark (ENH-033 — deliberately outside `checkall`; see [CONTRIBUTING's Benchmarks section](CONTRIBUTING.md#benchmarks)). |

### Per-package targets

| Package | Targets |
| --- | --- |
| python-client | `python-client-fmt`, `python-client-lint`, `python-client-typecheck`, `python-client-test`, `python-client-checkall` |
| swift-client | `swift-client-fmt`, `swift-client-fmt-check`, `swift-client-lint`, `swift-client-typecheck`, `swift-client-build`, `swift-client-test`, `swift-client-checkall` (Darwin only) |
| rust-client | `rust-client-check-features` |
| go-client | `go-client-install`, `go-client-fmt`, `go-client-fmt-check`, `go-client-lint`, `go-client-typecheck`, `go-client-test`, `go-client-checkall` |
| ts-client | `ts-client-install`, `ts-client-build` |
| dashboard | `dashboard-install`, `dashboard-test` |

Other packages are exercised directly with their own tool: `cargo test` in
`core/`, `server/`, `rust-client/`, and `cli/`, `go test ./...` in `go-client/`, and
`bunx vitest run` in `ts-client/`.

Server integration tests are a single consolidated binary, so run one test by
name with `cargo test --test main <name>` from `server/`, not by test-file
name.

## Graceful shutdown

The server exits cleanly on `SIGINT` or `SIGTERM`: it stops accepting new connections and
lets in-flight requests finish (via `axum::serve(...).with_graceful_shutdown(...)`) before
the process stops.

What the drain waits for is any connection whose response is still in flight — most
realistically a large storage download to a client that has stopped reading, which holds
its connection open indefinitely. **Open WebSockets are not in that set**: `on_upgrade`
hands the socket to a detached task, so the connection future completes at the handshake
and an idle subscriber never holds shutdown open.

`RTDB_SHUTDOWN_DRAIN_MS` (default `30000`) bounds that wait. When it elapses the
remaining connections are closed and shutdown proceeds to the background-task cleanup,
itself bounded at 5 s. `0` restores the previous behavior of waiting forever, where
Docker's SIGTERM→SIGKILL window was the only backstop. The bundled
`docker-compose.yml` sets `stop_grace_period: 45s` so the bound can actually elapse
there; any other deploy recipe needs a container grace period larger than the drain
bound (plus the ~10 s of bounded cleanup) or Docker's 10 s default SIGKILL wins.

## Multi-instance

par-rt-db runs as a single process by default. Set `RTDB_MULTI_INSTANCE=true`
and every replica behind a load balancer coordinates through Postgres — no
extra service, no consensus layer.

**Write ownership is a lease, not a rule.** The first replica to touch a
database opens a dedicated one-connection pool and takes
`pg_try_advisory_lock` on a key derived from the database name. That replica
runs the database's committer and all of its pollers on that same connection,
so the lease and every write share one Postgres backend: no other replica can
acquire it mid-flight, which makes split-brain impossible by construction
rather than by fencing. When an owner dies — `kill -9`, container stop, network
partition that drops the session — Postgres releases the advisory lock and the
next replica to try acquires it. Failover is just the acquire path.

**A non-owner replica is a full participant.** It serves reads, live
subscriptions, and presence for the database, and it forwards writes to the
owner:

1. The non-owner spools the request into `rtdb_auth.forward_queue` and sends
   only that row's id over `pg_notify('rtdb_write_fwd', …)`. Every replica
   reads the row; every replica that does not hold the lease drops it. Exactly
   one execution happens per forwarded write, whatever the fleet size.
2. The owner executes the write inside its own committer turn and spools the
   outcome back, again notifying only a row id on `rtdb_write_replies`. The
   reply row is claimed with an atomic `DELETE … RETURNING`, which doubles as
   the target filter.
3. The origin replica returns the owner's outcome to its client and re-runs its
   local subscriptions against the returned write set.

The spool exists because a `pg_notify` payload is capped at 8000 bytes and
Postgres rejects anything larger. Carrying a 36-byte row id instead of the body
means a write of any size forwards, and it makes forwarded work durable across
a listener reconnect — `run_forward_sweeper` reclaims rows left behind by a
crashed consumer.

The principal that authorized the write at the edge travels with the request,
so per-row `ownerField` / `authorize` rules evaluate against the identity that
actually made the call. Rate limits stay enforced at the origin; quotas stay
enforced inside the owner's committer turn.

**Timeout and takeover.** If no owner answers within
`RTDB_FORWARD_TIMEOUT_MS` (default 5 s, 100 ms floor), the origin attempts the
lease takeover itself. A forwarded mutate that carries no client idempotency
key is given a server-minted one, so if a late reply races the timeout and the
takeover resubmits, the shared dedup table replays the owner's first outcome
instead of writing twice. The ordinary timeout ambiguity still applies to what
the *client* observes: the write may have committed even though the caller saw
`CONFLICT`. A client that must tell the two apart should send its own
idempotency key and retry with it.

**Live queries stay correct on every replica.** Every durable write also
publishes its write set on `rtdb_write_sets`, and each replica re-runs the
subscriptions that write touched. A subscriber on replica B is invalidated by a
write the owner executed on replica A — including scheduled jobs, TTL expiry,
and migrations. Write sets over roughly 7.5 KB travel through the same spool
rather than inline. Document *values* never travel, so `indexed` and `ordered`
subscriptions on the receiving replica fall back to their conservative
"unrankable ⇒ re-run" path: extra re-runs are possible, a missed push is not.

**What else coordinates.** OAuth login state and sessions live in Postgres
already. The op feed fans out over `pg_notify('rtdb_ops', …)` so a dashboard
streaming `/admin/stream` on any replica sees every replica's writes. Presence
gossips per-room member snapshots over `rtdb_presence`. Rate-limit budgets
share `rtdb_auth.rate_counters`, giving one ceiling per route/token/db/ip
across the fleet rather than one per process. Every channel carries its origin
`instance_id` and skips its own echo, because Postgres delivers a session's own
notifications back to it.

Design and as-built notes:
[`docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md`](docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md).

## Known MVP limitations

- **Session expiry, machine-token revocation, allowlist removal, and admin-role
  revocation all take effect on open WebSocket connections.** The WS handler re-runs
  `authorize` on every `subscribe` and `mutate` (not just at connect) and re-runs
  `is_admin` on every admin op: an expired session or revoked admin is rejected on the
  next op with `UNAUTHORIZED`/`FORBIDDEN` while the connection stays open, so the client
  can refresh its token and retry without reconnecting. See FEATURE_MATRIX #8.
- `AuthedUser.name` is display-only and may be `null`. It is filled from the OAuth
  provider's profile at sign-in (GitHub's `name`, falling back to the `login` handle;
  Google/GitLab/Microsoft/OIDC `name`; Apple's rarely-present `name` claim), stored on
  `rtdb_auth.users.name`, and returned on every identity surface — `GET /auth/me`,
  `/auth/validate`, the WS `authOk` frame, and presence members. Identity itself is
  still keyed on the stable per-provider subject columns plus the verified email, never
  on this name; a provider that supplies none leaves any previously stored name intact,
  and anonymous users and machine tokens are always `null`. Existing accounts are not
  backfilled — a name appears on the user's next sign-in.

## Clients

par-rt-db ships five client SDKs that each mirror the server's wire contract,
plus an operator SPA and a CLI built on top of them:

- [`ts-client/`](ts-client/README.md) — `@par-rt-db/client` (browser/Node/React Native): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`rust-client/`](rust-client/README.md) — `par-rt-db-client` (Rust): http + reactive ws + admin, `.filter()`/`.search()`/`.vector_search()` builders.
- [`python-client/`](python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS.
- [`swift-client/`](swift-client/README.md) — `ParRtDbClient`/`ParRtDbUI` (Swift 6, iOS 17+/macOS 14): wire + query/mutation/schema DSL + HTTP + reactive WS + SwiftUI `LiveQuery`, plus the admin client, presence, optimistic updates, and the in-memory engine (see its README's coverage table).
- [`go-client/`](go-client/README.md) — `github.com/paulrobello/par-rt-db/go-client` (Go 1.23+): wire + schema DSL + typed HTTP queries + reactive WS + admin client + the in-memory engine, stdlib-only outside `coder/websocket`.
- [`dashboard/`](dashboard/README.md) — the operator console SPA (admin/operator UI served same-origin at `RTDB_STATIC_DIR`; consumes `ts-client`).
- [`cli/`](cli/README.md) — `rtdb` operator/CI binary (wraps `par-rt-db-client`). `rtdb changes --db <db> [--since <seq>] [--limit <n>] [--follow]` prints one page of durable change ops as compact NDJSON, with the `nextSeq` cursor on stderr. Omit `--since` to start at the current head, or pass `--since 0` for the oldest retained ops. `--follow` polls every two seconds until Ctrl-C; `CURSOR_EXPIRED` exits non-zero without silently resyncing. Supply `RTDB_URL` and `RTDB_TOKEN` (machine token).

[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) tracks parity vs. Convex, with per-row notes on which clients mirror each feature.

## FAQ
* Q: Do I need a Convex account?
  * A: No. par-rt-db is Convex-*inspired* but fully self-hosted — storage is your own Postgres 17.
* Q: Do I write server-side functions?
  * A: No. Queries and transactions are a declarative JSON DSL; there is no embedded JS runtime and no per-app server code — one generic server hosts many databases.
* Q: Can I use it over plain HTTP?
  * A: Yes — one-shot `POST /api/query` / `POST /api/mutate` cover reads and writes. The WebSocket (`/sync`) is only needed for live subscriptions and presence.
* Q: Which languages have client SDKs?
  * A: TypeScript (`@par-rt-db/client`, with React bindings), Rust (`par-rt-db-client`), Python (`par-rt-db`), Swift (`ParRtDbClient`, iOS 17+/macOS 14), and Go (`go-client`), plus the `rtdb` CLI and the operator dashboard.
* Q: Is auth required?
  * A: Machine tokens are the baseline. Each of the six OAuth providers is independently optional (blank env ⇒ its routes return 503), and anonymous access is opt-in (off by default).
* Q: Can I run multiple replicas behind a load balancer?
  * A: Yes — set `RTDB_MULTI_INSTANCE=true`. Login state, op-feed, presence, rate budgets, and per-database write ownership (advisory-lock lease with kill-failover) coordinate via Postgres; a non-owner replica serves reads/subscriptions and forwards writes to the owning replica automatically (taking the lease itself if the owner dies within `RTDB_FORWARD_TIMEOUT_MS`). See [Multi-instance](#multi-instance).

## Roadmap

### Where we are
* **Core**: realtime live queries, atomic multi-step transactions, typed schemas compiled to Postgres DDL, WebSocket + HTTP transports, many databases per instance
* **Query terminals**: `get`/`index`/`count`/`unique`/`first`/`take`, `filter` predicate DSL, full-text `search` (`ts_rank` + `trgm` mode), `vectorSearch` (pgvector), `hybridSearch` (RRF), `paginate` keyset pagination, `distinct`, `aggregate`
* **Beyond documents**: scheduling (`afterMs`/`runAt`/`cron`/`interval`), durable workflows, realtime presence, file storage with signed URLs and image transforms
* **Auth**: six optional OAuth providers, per-db machine tokens plus optional host-scoped machine credentials with enrollment grants, optional anonymous with anon→real merge, per-row `ownerField`/`authorize` rules
* **Operations**: operator dashboard, `rtdb` CLI, op feed, audit log, webhooks, Prometheus metrics + optional OTLP tracing, backup/restore, schema migration with snapshot history, hot config, per-db quotas
* [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) is the authoritative Convex-parity contract

### Where we're going
* The next lockstep release cutting `[Unreleased]` — all packages version together per [`docs/RELEASING.md`](docs/RELEASING.md); publishing the SDKs to crates.io / npm / PyPI stays a separate, owner-approved decision

## What's new

`0.1.0` is tagged; everything since lives under `[Unreleased]` in
[`CHANGELOG.md`](CHANGELOG.md). Highlights: the Swift client (the fourth SDK and
the fifth implementation of the wire contract), multi-instance write forwarding
with cross-replica subscription invalidation, computed fields, auto-increment
counters, `awaitSignal` workflow approval gates, query field projection, and
interval-recurrence scheduling. Read the **Breaking** group first — signed
storage URLs minted before this cycle no longer verify, and missing-schema
database operations now answer `NOT_FOUND` rather than `INTERNAL`.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, the `make checkall`
gate, Conventional Commits, the five-client wire-mirror rule, and the PR checklist.
[`CLAUDE.md`](CLAUDE.md) is the agent-facing companion with the full invariant list.

## License

MIT — see [`LICENSE`](LICENSE). Each package manifest (`server/Cargo.toml`,
`rust-client/Cargo.toml`, `ts-client/package.json`, `dashboard/package.json`,
`python-client/pyproject.toml`) declares the same MIT license.
