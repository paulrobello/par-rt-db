# par-rt-db

[![CI](https://github.com/paulrobello/par-rt-db/actions/workflows/ci.yml/badge.svg)](https://github.com/paulrobello/par-rt-db/actions/workflows/ci.yml)
[![license: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A self-hosted, Convex-inspired realtime document database. Clients send a
**declarative JSON DSL** — typed queries and atomic multi-step transactions — over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query
updates on change. There is no embedded JS runtime and no per-app server code — one
generic server hosts many named databases for every app. Built in Rust on axum/tokio
with Postgres 17 storage. Authoritative design:
[`docs/superpowers/specs`](docs/superpowers/specs).

## Table of contents

- [Packages](#packages)
- [How it works](#how-it-works)
- [Quickstart](#quickstart)
- [Endpoints](#endpoints)
- [Configuration](#configuration)
- [Error envelope](#error-envelope)
- [Wire protocol](#wire-protocol)
- [Pagination](#pagination)
- [Scheduling](#scheduling)
- [Realtime presence](#realtime-presence)
- [Make targets](#make-targets)
- [Graceful shutdown](#graceful-shutdown)
- [Known MVP limitations](#known-mvp-limitations)
- [Clients](#clients)
- [Contributing](#contributing)
- [License](#license)

Related documentation: [`CHANGELOG.md`](CHANGELOG.md), [`DESIGN.md`](DESIGN.md),
[`PRODUCT.md`](PRODUCT.md), [`FEATURE_MATRIX.md`](FEATURE_MATRIX.md),
[`deploy/README.md`](deploy/README.md) (production runbook),
[`docs/README.md`](docs/README.md) (docs index),
[`CONTRIBUTING.md`](CONTRIBUTING.md).

## Packages

| Package | Path | Stack | What it is |
| --- | --- | --- | --- |
| **Server** | [`server/`](server) | Rust (axum/tokio + Postgres 17) | The realtime database binary |
| **TypeScript client** | [`ts-client/`](ts-client) | TS (`@par-rt-db/client`, bun) | Browser/Node SDK + React bindings + in-memory test harness |
| **Rust client** | [`rust-client/`](rust-client) | Rust (`par-rt-db-client`) | Rust SDK: http + reactive ws + admin + `.filter()`/`.search()`/`.vector_search()` builders |
| **Python client** | [`python-client/`](python-client) | Python (`par-rt-db`, uv) | Python SDK: wire + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS |
| **Dashboard** | [`dashboard/`](dashboard) | Vite + React 19 + TS (bun) | Operator console SPA served same-origin at `RTDB_STATIC_DIR` |
| **`rtdb` CLI** | [`cli/`](cli) | Rust (`rtdb` binary, cargo) | Operator/CI wrapper around `par-rt-db-client`: list/create dbs, push schema, query/mutate, mint/revoke tokens |

The server is the source of truth; the three SDKs (ts/rust/python) each mirror
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
    Co->>DB: BEGIN; execute_txn (insert/patch/...)
    DB-->>Co: step results
    Co->>S: fan_out(affected tables)
    S->>DB: re-run affected subscriptions (READ COMMITTED)
    S-->>C: queryUpdate (only if result changed)
    Co-->>H: mutateOk + results
    H-->>C: 200 / mutateOk
    end
```

The same path runs for scheduled/cron jobs (a per-db scheduler enqueues due jobs as
`CommitterRequest::RunScheduled`). Reads that don't write go straight to Postgres
through `execute_query` under READ COMMITTED. Auth re-runs `authorize` on every
Subscribe/Mutate over an open WebSocket. File storage is HTTP-only and bypasses the
committer (blobs don't touch document tables). See
[`server/README.md`](server/README.md) for the server layout and
[`CLAUDE.md`](CLAUDE.md) for the full invariant list.

## Quickstart

This walkthrough brings up a local server, creates a database, mints a machine token, and
runs one mutate + one query. It assumes `docker` is available and that the repo is
checked out at the root.

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
make test   # dev-db-up + fmt/clippy/typecheck/tests across all six packages
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
| `GET /metrics` | none | Prometheus text-exposition scrape endpoint. Content-negotiated on `Accept`: a browser (`text/html`) is served the SPA's `index.html` when `RTDB_STATIC_DIR` is set; everything else (Prometheus sends `application/openmetrics-text`, curl, API-only deploys) gets Prometheus text. Aggregate-only (no per-db, no principal data), same posture as `/healthz`. The admin JSON snapshot stays at `GET /admin/metrics`. |
| `GET /sync` | first WS frame | WebSocket upgrade. Speaks the realtime protocol (auth, subscribe, mutate, schedule, ping). |
| `POST /api/query` | Bearer token | One-shot query against a database; see [Query shape](#query-shape). |
| `POST /api/query-batch` | Bearer token | Fans out N queries in one round trip (per-query error isolation); each slot returns `{ok, result}` or `{ok:false, error}`. |
| `POST /api/mutate` | Bearer token | One-shot transaction (`insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert` + `patchByQuery`/`deleteByQuery` + `schedule`/`cancelSchedule` steps). |
| `POST /api/schedule` | Bearer token | Schedules a transaction: `afterMs`/`runAt` one-shot or `cron` (5-field, UTC, min-first); returns `{id}`. |
| `POST /api/schedule/{id}/{cancel,pause,resume}` | Bearer token | Cancels, pauses, or resumes a scheduled job. |
| `POST /api/schedules` | Bearer token | Lists scheduled jobs for a database (`ScheduleInfo[]`). |

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
| `POST /admin/db/{db}/query` | Bearer admin key | Admin reads documents across any database (`owner=None`, bypassing per-row `ownerField`). |
| `POST /admin/db/{db}/mutate` | Bearer admin key | Admin writes documents across any database. Capped by `RTDB_MAX_AFFECTED_DOCS`. |
| `GET /admin/db/{db}/schedules` | Bearer admin key | Lists scheduled jobs for a database (admin-scoped). |
| `POST /admin/db/{db}/schedules` | Bearer admin key | Creates a scheduled job for a database (admin-scoped). |
| `POST /admin/db/{db}/schedules/{id}/{cancel,pause,resume}` | Bearer admin key | Cancels, pauses, or resumes a scheduled job (admin-scoped). |
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
| `GET /admin/sessions?user=&limit=` | Bearer admin key | Lists active interactive sessions (OAuth/anonymous/admin-key). `?user=` filters by `user_id` or email (omitted ⇒ all, server-wide); `?limit=` clamped to `[1, 1000]`, default 200. `token_hash` is a non-reversible sha256 digest, safe to surface. |
| `DELETE /admin/sessions/{token_hash}` | Bearer admin key | Revokes a single session by its `token_hash` (takes effect on the next op over an already-open connection — `session_still_valid` re-queries on every interactive Subscribe/Mutate/Presence). |
| `DELETE /admin/sessions?user=` | Bearer admin key | Revokes every session for a user. Requires `?user=` — a bare `DELETE` is a 400 (refuses to revoke every session instance-wide from one unscoped call). |
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
| `GET /auth/{provider}/begin?origin=` | none | Starts the OAuth flow for one of `github`/`google`/`gitlab`/`microsoft`/`apple`/`oidc`. Validates `origin` against the **live** `RTDB_ALLOWED_ORIGINS` (403 on miss), mints a single-use `state` token, sets the `SameSite=None;HttpOnly` `rtdb-oauth-csrf` nonce cookie, and returns JSON `{ authorizeUrl, state }`. The caller opens `authorizeUrl` in a `noopener,noreferrer` popup and polls `/auth/state` for completion. |
| `GET /auth/callback` | none (state token) | GitHub OAuth callback (the canonical path; Google/GitLab/Microsoft/OIDC use `GET /auth/{provider}/callback`). Constant-time-verifies the CSRF nonce cookie against `state`, claims the pending entry (replay → 400), exchanges the code, fetches the verified email, upserts `rtdb_auth.users`, sets the `HttpOnly` session cookie, and returns popup-closing HTML. |
| `POST /auth/apple/callback` | none (state token) | Apple-specific callback — Apple POSTs `code`+`state` via `response_mode=form_post`. Same CSRF/session handling as the GET path (the `SameSite=None` cookie survives the cross-site POST). |
| `GET /auth/state?state=` | none (state token) | Provider-agnostic poll endpoint keyed on the single-use `state` token. Returns `{ status: "pending" }` until the callback completes, then `{ status: "ok", token }` (one-shot — the next poll returns `{ status: "gone" }`). The state token, not the session cookie, is the poll capability, which is what makes cross-origin SDK login work where the `SameSite=Lax` session cookie would not be sent. |
| `POST /auth/logout` | Bearer session | Deletes the session for the given bearer token. Idempotent: always 200 unless the delete query itself fails. |
| `GET /auth/me` | Bearer session | Returns the authenticated user. 401 for a machine token (session only). |
| `GET /auth/validate` | Bearer token | Validates a presented session or machine token; returns the `AuthedUser`. Used by backends to check a player-supplied token. |
| `POST /auth/anonymous` | none | Mints an ephemeral anonymous user + session for a credential-less guest (gated by `RTDB_AUTH_ANONYMOUS_ENABLED`, default off ⇒ `403 FORBIDDEN`). Sets the `HttpOnly` session cookie (browser path) **and** returns the plaintext session token in the body (SDK/bearer path — pass it as the WS/HTTP bearer, exactly like a machine token). Per-IP rate-limited; an anonymous user is authorized for any database via that boot gate (no allowlist entry) and owns its own documents via per-row `ownerField` (the anon `user_id`). On a later OAuth sign-in with that session's bearer presented at `/begin`, the anon footprint is merged into the real account — owned docs across all databases restamped, storage blob ownership swapped, the live session re-pointed — via `merge::merge_users` (`rtdb_merge_docs_total` counts restamped docs); `POST /admin/merge-users` is the operator escape hatch. |

The live login flow (SEC-012): the browser hits `GET /auth/{provider}/begin` →
opens the provider authorize URL in a `noopener,noreferrer` popup (reverse-tabnabbing
defense — `window.opener` is severed) → the parent polls
`GET /auth/state?state=<token>` → the provider redirects to the callback →
the callback sets the `HttpOnly` session cookie and returns popup-closing HTML
→ the parent's next poll receives the session token and closes the popup.
Login-CSRF is defended by the `rtdb-oauth-csrf` double-submit cookie set at
`/begin` and constant-time-verified at `/callback`
(`RTDB_OAUTH_LOGIN_CSRF=true` by default; cross-origin SDK consumers must send
`credentials: "include"` on the `/begin` fetch). Identity is email-keyed with
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
"count"?, "filter"?, "search"?, "vectorSearch"?, "hybridSearch"?, "paginate"?, "distinct"?, "aggregate"?}`
— exactly one terminal per query (terminals are mutually exclusive). See
`server/src/query.rs` for full semantics: index prefix binds, range predicates
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
`distinct` collapses duplicates on a field set, and `aggregate` runs a grouped
`sum`/`min`/`max`/`avg`/`count`.

### Transaction shape

`{"steps": [...]}` where each step is tagged by `"op"`: `insert`, `patch`,
`replace`, `delete`, `expectVersion`, `expectAbsent`, `upsert` (per-id, one
document each), the predicate-driven bulk steps `patchByQuery` and
`deleteByQuery` (each finds rows matching a `filter` and acts on up to
`MAX_BY_QUERY_ROWS` of them in one serialized committer turn), and the
scheduler control-flow steps `schedule` (enqueues a nested txn by inserting
the `scheduled_txns` row on the open sqlx transaction — atomic with the
enclosing writes; step result `{"scheduleId": "<id>"}`) and `cancelSchedule`
(`{"cancelled": <bool>}`, `false` on a missing/already-fired/already-cancelled job) — see
`server/src/txn.rs`.

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

client = RtDbHttpClient("https://rtdb.pardev.net", db="myapp", token=TOKEN)

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

A per-database scheduler timer claims due rows and enqueues each through the
single-writer committer, which executes it via the normal transaction path (so
subscriptions fire on schedule-driven writes too). Delivery is **at-least-once**:
apps should write idempotent scheduled transactions. A one-shot catches up if past
due; a cron **skips** missed windows with no backfill. Each job's lifecycle is
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

Each `make` target spans **all six packages** (server, ts-client, rust-client,
python-client, dashboard, cli). The composition is summarized below; see the
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
`make dev-db-down` stops it. `make dev-db-clean` drops leaked test schemas
(`db_t<uuid-v7>`) from the dev `rtdb` DB — tests create a database per test and
don't drop it, so the dev DB bloats over time; run this periodically (it is
scoped to the test pattern and never touches `rtdb`/`rtdb_auth`/real databases).

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
| `make deploy` | `checkall` → rsync to lenny2 → `docker compose up -d --build` → healthz probe. |

### Granular python-client targets

`make python-client-test | python-client-lint | python-client-fmt | python-client-typecheck | python-client-checkall` run that one stage in `python-client/` only. Equivalent granular targets for the other packages are run directly with the package's tool (`cargo test` in `server/`/`rust-client/`, `bunx vitest run` in `ts-client/`, `bun run test` in `dashboard/`).

> The dashboard ships a Vitest + React Testing Library suite (`session`, `useLiveTable`, `ConfigPage`); run it standalone with `make dashboard-test`, or as part of `make checkall`/`make test`.

## Graceful shutdown

The server exits cleanly on `SIGINT` or `SIGTERM`: in-flight requests are allowed to
finish (via `axum::serve(...).with_graceful_shutdown(...)`) before the process stops. This
includes open WebSocket connections — shutdown waits for them to close rather than forcibly
dropping them, with no timeout of its own; Docker's SIGTERM→SIGKILL window is the backstop
that ultimately terminates a connection that never closes on its own.

## Known MVP limitations

- **Deploy as a single instance — running multiple replicas behind a load
  balancer is not yet supported.** Of the four pieces of server state that were
  once in-process, three now coordinate across replicas via Postgres
  LISTEN/NOTIFY when `RTDB_MULTI_INSTANCE=true` (ENH-022 Stages 1–3): OAuth
  login state, the op-feed, and presence. Two remaining constraints still make
  an unscaled deploy **silently** unsafe (no error — it just half-works):
  - **Rate limiting** — in-process counters multiply the effective budget by the
    replica count (a silent weakening of the cap). *(ENH-022 Stage 4.)*
  - **Write funnelling** — writes for a given database are not yet funnelled to
    a single committer owner across processes. Two replicas both writing the
    same database would interleave `execute_txn` (READ COMMITTED, no cross-
    process lock) and break the subscription skip-invalidation logic. A real
    fix needs a Postgres advisory lock or lease; it is a future stage.
  The server logs a `WARN` at boot naming these constraints. The single-writer
  invariant (one committer task per database) is intact and must stay so —
  multi-instance here means multiple *readers/connection-holders*, not a second
  writer onto the same database. Horizontal scaling is tracked as ENH-022.
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

par-rt-db ships three client SDKs that each mirror the server's wire contract,
plus an operator SPA and a CLI built on top of them:

- [`ts-client/`](ts-client/README.md) — `@par-rt-db/client` (browser/Node): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`rust-client/`](rust-client/README.md) — `par-rt-db-client` (Rust): http + reactive ws + admin, `.filter()`/`.search()`/`.vector_search()` builders.
- [`python-client/`](python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL + sync HTTP/admin/storage + reactive WS.
- [`dashboard/`](dashboard/README.md) — the operator console SPA (admin/operator UI served same-origin at `RTDB_STATIC_DIR`; consumes `ts-client`).
- [`cli/`](cli/README.md) — `rtdb` operator/CI binary (wraps `par-rt-db-client`).

[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) tracks parity vs. Convex, with per-row notes on which clients mirror each feature.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, the `make checkall`
gate, Conventional Commits, the four-client wire-mirror rule, and the PR checklist.
[`CLAUDE.md`](CLAUDE.md) is the agent-facing companion with the full invariant list.

## License

MIT — see [`LICENSE`](LICENSE). Each package manifest (`server/Cargo.toml`,
`rust-client/Cargo.toml`, `ts-client/package.json`, `dashboard/package.json`,
`python-client/pyproject.toml`) declares the same MIT license.
