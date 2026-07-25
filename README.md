# par-rt-db

A self-hosted, Convex-inspired realtime document database. Clients send a
**declarative JSON DSL** — typed queries and atomic multi-step transactions — over
WebSocket (`/sync`) or one-shot HTTP; the server executes them and pushes live query
updates on change. There is no embedded JS runtime and no per-app server code — one
generic server hosts many named databases for every app. Built in Rust on axum/tokio
with Postgres 17 storage. Authoritative design:
[`docs/superpowers/specs`](docs/superpowers/specs).

## Packages

| Package | Path | Stack | What it is |
| --- | --- | --- | --- |
| **Server** | [`server/`](server) | Rust (axum/tokio + Postgres 17) | The realtime database binary |
| **TypeScript client** | [`ts-client/`](ts-client) | TS (`@par-rt-db/client`, bun) | Browser/Node SDK + React bindings + in-memory test harness |
| **Rust client** | [`rust-client/`](rust-client) | Rust (`par-rt-db-client`) | Rust SDK: http + reactive ws + admin + `.filter()`/`.search()`/`.vector_search()` builders |
| **Python client** | [`python-client/`](python-client) | Python (`par-rt-db`, uv) | Python SDK: wire + schema/mutation/query DSL (HTTP/WS/admin pending) |
| **Dashboard** | [`dashboard/`](dashboard) | Vite + React 19 + TS (bun) | Operator console SPA served same-origin at `RTDB_STATIC_DIR` |

The server is the source of truth; the four clients mirror its wire contract. See
[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) for the Convex-parity contract and
[`CLAUDE.md`](CLAUDE.md) for contributor guidance.

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
make test   # dev-db-up + fmt/clippy/typecheck/tests across all five packages
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
| `GET /sync` | first WS frame | WebSocket upgrade. Speaks the realtime protocol (auth, subscribe, mutate, schedule, ping). |
| `POST /api/query` | Bearer token | One-shot query against a database; see [Query shape](#query-shape). |
| `POST /api/mutate` | Bearer token | One-shot transaction (`insert`/`patch`/`replace`/`delete`/`expectVersion`/`expectAbsent`/`upsert` steps). |
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

### Admin: databases, schema, tokens, allowlist

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /admin/create-db` | Bearer admin key | Creates a new database. |
| `POST /admin/push-schema` | Bearer admin key | Applies additive schema DDL to a database. |
| `GET /admin/dbs` | Bearer admin key | Lists all databases. |
| `GET /admin/dbs/{db}/schema` | Bearer admin key | Returns the pushed schema for a database. |
| `GET /admin/dbs/{db}/stats` | Bearer admin key | Per-table row counts and storage sizes. |
| `POST /admin/mint-token` | Bearer admin key | Mints a machine token scoped to one database. |
| `POST /admin/revoke-token` | Bearer admin key | Revokes a machine token by its id. |
| `GET /admin/tokens?db=` | Bearer admin key | Lists machine tokens for a database (no secrets). |
| `GET /admin/allowlist?db=` | Bearer admin key | Lists the emails allowlisted for a database. |
| `POST /admin/allowlist` | Bearer admin key | Adds or removes an email from a database's allowlist. |
| `GET /admin/admins` | Bearer admin key | Lists the server-wide OAuth admin allowlist (`rtdb_auth.admins`). |
| `POST /admin/admins` | Bearer admin key | Adds an email to the admin allowlist. |
| `DELETE /admin/admins` | Bearer admin key | Removes an email from the admin allowlist. |

### Admin: dashboard operator surface

| Method & path | Auth | Description |
| --- | --- | --- |
| `POST /admin/db/{db}/query` | Bearer admin key | Admin reads documents across any database (`owner=None`, bypassing per-row `ownerField`). |
| `POST /admin/db/{db}/mutate` | Bearer admin key | Admin writes documents across any database. Capped by `RTDB_MAX_AFFECTED_DOCS`. |
| `GET /admin/metrics` | Bearer admin key | Live gauges and throughput. |
| `GET /admin/ops/recent` | Bearer admin key | Recent document-mutation op feed (durable). |
| `WS /admin/stream` | Bearer admin key | Live op-feed stream over WebSocket (subprotocol auth for browsers). |
| `GET /admin/config` | Bearer admin key | Hot config, redacted (`admin_key`/OAuth secrets/`database_url` → configured-bools only). |
| `PATCH /admin/config` | Bearer admin key | Mutates `allowed_origins`/`session_ttl_days`/`max_file_size`; validates, persists, swaps live (no restart). |
| `GET /admin/export-db?db=` | Bearer admin key | Snapshot export: schema line + one JSONL doc line per document. |
| `POST /admin/import-db?db=` | Bearer admin key | Snapshot import: applies the schema line, replays each doc with original id/timestamp/version. |

### Auth (OAuth + sessions)

| Method & path | Auth | Description |
| --- | --- | --- |
| `GET /auth/github?origin=` | none | Starts the GitHub OAuth flow; 302s to GitHub's authorize page. `origin` must be an exact member of `RTDB_ALLOWED_ORIGINS`. |
| `GET /auth/callback` | none (state token) | GitHub OAuth callback; exchanges the code, mints a session, and returns HTML that `postMessage`s the session token back to the popup opener. |
| `GET /auth/google?origin=` | none | Starts the Google OAuth flow; 302s to Google's authorize page. `origin` must be an exact member of `RTDB_ALLOWED_ORIGINS`. |
| `GET /auth/google/callback` | none (state token) | Google OAuth callback; exchanges the code, mints a session, and returns HTML that `postMessage`s the session token back to the popup opener. |
| `POST /auth/logout` | Bearer session | Deletes the session for the given bearer token. Idempotent: always 200 unless the delete query itself fails. |
| `GET /auth/me` | Bearer session | Returns the authenticated user. 401 for a machine token (session only). |
| `GET /auth/validate` | Bearer token | Validates a presented session or machine token; returns the `AuthedUser`. Used by backends to check a player-supplied token. |

Bearer tokens are either a per-database **machine token** (minted via `/admin/mint-token`)
or a **session token** (minted by completing the GitHub or Google OAuth flow). Both resolve
through the same `Authorization: Bearer <token>` header on `/api/*`, `/auth/*`, and the WS
`auth` frame. The WS handler and admin re-auth paths re-run on every op (revocation and
expiry take effect on open connections — see [Known MVP limitations](#known-mvp-limitations)).

Bearer tokens are either a per-database **machine token** (minted via `/admin/mint-token`)
or a **session token** (minted by completing the GitHub or Google OAuth flow). Both resolve
through the same `Authorization: Bearer <token>` header on `/api/*`, `/auth/*`, and the WS
`auth` frame.

## Configuration

The server reads its configuration from environment variables. Boot-time vars
(prefix `RTDB_`) come from the environment; the three runtime-mutable ones
(`allowed_origins`, `session_ttl_days`, `max_file_size`) are seeded from env at
first boot, persisted in a single-row `rtdb_config` table, and hot-swappable
via `PATCH /admin/config` thereafter.

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `RTDB_DATABASE_URL` | yes | — | Postgres connection string. |
| `RTDB_ADMIN_KEY` | yes | — | Server-wide admin bearer (constant-time compared). Generate with `openssl rand -hex 32`. |
| `RTDB_PORT` | no | `8300` | HTTP/WS listen port. |
| `RTDB_PUBLIC_URL` | no | `http://localhost:8300` | Public origin (OAuth callback base, external links). |
| `RTDB_ALLOWED_ORIGINS` | no | empty | Comma-separated browser origins; also the exact-match CORS allowlist for `/api/*` and `/auth/*`. Hot-reloadable. |
| `RTDB_SESSION_TTL_DAYS` | no | `30` | OAuth session lifetime in days. Hot-reloadable. |
| `RTDB_MAX_FILE_SIZE` | no | `52428800` (50 MiB) | Per-upload byte ceiling enforced inside the upload handler. Hot-reloadable. |
| `RTDB_MAX_AFFECTED_DOCS` | no | `100` | Max steps an admin mutation may carry — rejects over-cap writes before the committer (each DSL step touches ≤1 doc). |
| `RTDB_STATIC_DIR` | no | unset | Directory of static SPA build artifacts. Set/unset existing dir ⇒ dashboard served same-origin at the catch-all route fallback; unset/empty/missing ⇒ API-only. |
| `RTDB_ADMIN_EMAILS` | no | empty | Comma-separated emails seeded into the server-wide OAuth admin allowlist (`rtdb_auth.admins`) at boot. Manageable at runtime via `/admin/admins`. |
| `RTDB_GITHUB_CLIENT_ID` | no | none | GitHub OAuth app client id. |
| `RTDB_GITHUB_CLIENT_SECRET` | no | none | GitHub OAuth app client secret. |
| `RTDB_GITHUB_BASE_URL` | no | `https://github.com` | GitHub authorize/base URL (override for GitHub Enterprise). |
| `RTDB_GITHUB_API_URL` | no | `https://api.github.com` | GitHub API base URL (override for GitHub Enterprise). |
| `RTDB_GOOGLE_CLIENT_ID` | no | none | Google OAuth client id. |
| `RTDB_GOOGLE_CLIENT_SECRET` | no | none | Google OAuth client secret. |
| `RTDB_BUILD_COMMIT` | no | `git rev-parse --short HEAD`, else `unknown` | Short SHA baked into `/healthz` `git_commit` at build time. Set explicitly when building without `.git` (e.g. Docker build-arg). |

`RTDB_ALLOWED_ORIGINS` is also the exact-match CORS allowlist for `/api/*` and `/auth/*`
(GET, POST, OPTIONS; `authorization` and `content-type` headers). Each OAuth provider is
only active when both its client id and secret are set — GitHub needs
`RTDB_GITHUB_CLIENT_ID` + `RTDB_GITHUB_CLIENT_SECRET`, Google needs
`RTDB_GOOGLE_CLIENT_ID` + `RTDB_GOOGLE_CLIENT_SECRET`. A half-configured pair (only one
of the two) is treated the same as neither, and `GET /auth/<provider>` returns `503` with
an `INTERNAL` error envelope.

## Error envelope

Every error response — HTTP and WebSocket alike — is a JSON object:

```json
{"code": "NOT_FOUND", "message": "document 'abc' not found"}
```

| `code`                | HTTP status |
| --------------------- | ----------- |
| `UNAUTHORIZED`        | 401         |
| `FORBIDDEN`           | 403         |
| `NOT_FOUND`           | 404         |
| `SCHEMA_VIOLATION`    | 422         |
| `PRECONDITION_FAILED` | 409         |
| `BAD_REQUEST`         | 400         |
| `INTERNAL`            | 500         |

## Wire protocol

### Query shape

`{"table": "<name>", "get"?, "index"?, "eq"?, "order"?, "take"?, "unique"?}` — see
`server/src/query.rs` for full semantics (index prefix binds, `order: "asc"|"desc"`,
`take` capped at 4096, `unique`, point `get` by id).

### Transaction shape

`{"steps": [...]}` where each step is tagged by `"op"`: `insert`, `patch`, `replace`, `delete`,
`expectVersion`, `expectAbsent`, `upsert` — see `server/src/txn.rs`.

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

### Query shape

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
HTTP client surface is pending; today this is a DSL builder that emits the wire
`paginate` field. See [`python-client/`](python-client).

```python
from par_rt_db import TableQuery

# Wire payload for the first page (cursor omitted). Pass `nextCursor` back as
# `cursor=...` on subsequent pages.
q = (
    TableQuery("items")
    .with_index("by_priority")
    .order("asc")
    .paginate(num_items=20)
)
payload = q.build().model_dump(by_alias=True, mode="json")  # ready to POST as the `query`
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

## Make targets

Each `make` target spans **all five packages** (server, ts-client, rust-client,
dashboard, python-client). The composition is summarized below; see the
[`Makefile`](Makefile) for the canonical commands.

### First-time install (per package)

| Target | Packages | Purpose |
| --- | --- | --- |
| `make ts-client-install` | ts-client | `bun install` in `ts-client/`. |
| `make dashboard-install` | dashboard | `bun install` at repo root + `dashboard/`. |
| `make python-client-install` | python-client | `uv sync` in `python-client/`. |

Cargo workspaces (`server/`, `rust-client/`) have no install target — `cargo`
fetches on first build.

### Build / format / lint / typecheck / test (each runs across all five packages)

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
`make dev-db-down` stops it.

### The gate

| Target | Purpose |
| --- | --- |
| `make checkall` | `fmt-check` + `lint` + `typecheck` + `test` across all five packages. **Definition of done; must pass before commit.** |
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
- OAuth popup login has an accepted CSRF residual: the `state` token is bound to the
  initiating *origin* (so a malicious page can't receive the session token even if it can
  trigger the flow), but not to the initiating *browser* (no PKCE, no state cookie) — see
  the design spec's Auth section for the accepted-risk rationale.

## Clients

par-rt-db ships four clients that mirror one wire contract:

- [`ts-client/`](ts-client/README.md) — `@par-rt-db/client` (browser/Node): schema builder, reactive WebSocket client, React bindings, HTTP/admin clients, in-memory test harness.
- [`rust-client/`](rust-client/README.md) — `par-rt-db-client` (Rust): http + reactive ws + admin, `.filter()`/`.search()`/`.vector_search()` builders.
- [`python-client/`](python-client/README.md) — `par-rt-db` (Python): wire contract + schema/mutation/query DSL (HTTP/WS/admin/storage pending).
- [`dashboard/`](dashboard/README.md) — the operator console SPA (admin/operator UI served same-origin at `RTDB_STATIC_DIR`).

[`FEATURE_MATRIX.md`](FEATURE_MATRIX.md) tracks parity vs. Convex, with per-row notes on which clients mirror each feature.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for development setup, the `make checkall`
gate, Conventional Commits, the four-client wire-mirror rule, and the PR checklist.
[`CLAUDE.md`](CLAUDE.md) is the agent-facing companion with the full invariant list.

## License

MIT — see [`LICENSE`](LICENSE). Each package manifest (`server/Cargo.toml`,
`rust-client/Cargo.toml`, `ts-client/package.json`, `dashboard/package.json`,
`python-client/pyproject.toml`) declares the same MIT license.
