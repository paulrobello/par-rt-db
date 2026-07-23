# par-rt-db

A Convex-inspired realtime document database server, written in Rust on axum/tokio with
Postgres storage and WebSocket-based subscriptions. See
[`docs/superpowers/specs`](docs/superpowers/specs) for the approved design spec.

## Quickstart

```bash
make dev-db-up   # start the dev Postgres container (127.0.0.1:55434)
make test        # run the server test suite
```

The server itself listens on `RTDB_PORT` (default `8300`); run it with `cargo run` from
`server/` once `RTDB_DATABASE_URL` and `RTDB_ADMIN_KEY` are set.

## Endpoints

| Method & path              | Auth               | Description                                                                                                                                  |
| -------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /healthz`             | none               | Liveness check; returns the body `ok`.                                                                                                       |
| `GET /sync`                | first WS frame     | Upgrades to WebSocket; speaks the realtime protocol below (auth, subscribe, mutate, ping).                                                   |
| `POST /api/query`          | Bearer token       | One-shot query against a database; see [Query shape](#query-shape).                                                                          |
| `POST /api/mutate`         | Bearer token       | One-shot transaction (insert/patch/replace/delete/expectVersion/expectAbsent/upsert steps).                                                  |
| `POST /api/schedule`       | Bearer token       | Schedules a transaction: `afterMs`/`runAt` one-shot or `cron` (5-field, UTC, min-first); returns `{id}`.                                      |
| `POST /api/schedule/{id}/{cancel,pause,resume}` | Bearer token | Cancels, pauses, or resumes a scheduled job.                                                                                        |
| `POST /api/schedules`      | Bearer token       | Lists scheduled jobs for a database (`ScheduleInfo[]`).                                                                                      |
| `POST /admin/create-db`    | Bearer admin key   | Creates a new database.                                                                                                                      |
| `POST /admin/push-schema`  | Bearer admin key   | Applies additive schema DDL to a database.                                                                                                   |
| `GET /admin/dbs`           | Bearer admin key   | Lists all databases.                                                                                                                         |
| `POST /admin/mint-token`   | Bearer admin key   | Mints a machine token scoped to one database.                                                                                                |
| `POST /admin/revoke-token` | Bearer admin key   | Revokes a machine token by its id.                                                                                                           |
| `GET /admin/allowlist?db=` | Bearer admin key   | Lists the emails allowlisted for a database.                                                                                                 |
| `POST /admin/allowlist`    | Bearer admin key   | Adds or removes an email from a database's allowlist.                                                                                        |
| `GET /auth/github?origin=` | none               | Starts the GitHub OAuth flow; 302s to GitHub's authorize page. `origin` must be an exact member of `RTDB_ALLOWED_ORIGINS`.                   |
| `GET /auth/callback`       | none (state token) | GitHub OAuth callback; exchanges the code, mints a session, and returns HTML that `postMessage`s the session token back to the popup opener. |
| `GET /auth/google?origin=` | none               | Starts the Google OAuth flow; 302s to Google's authorize page. `origin` must be an exact member of `RTDB_ALLOWED_ORIGINS`.                   |
| `GET /auth/google/callback` | none (state token) | Google OAuth callback; exchanges the code, mints a session, and returns HTML that `postMessage`s the session token back to the popup opener. |
| `POST /auth/logout`        | Bearer session     | Deletes the session for the given bearer token. Idempotent: always 200 unless the delete query itself fails.                                 |
| `GET /auth/me`             | Bearer session     | Returns the authenticated user. 401 for a machine token (session only).                                                                      |
| `GET /auth/validate`       | Bearer token       | Validates a presented session or machine token; returns the `AuthedUser`. Used by backends to check a player-supplied token.                 |

Bearer tokens are either a per-database **machine token** (minted via `/admin/mint-token`)
or a **session token** (minted by completing the GitHub or Google OAuth flow). Both resolve
through the same `Authorization: Bearer <token>` header on `/api/*`, `/auth/*`, and the WS
`auth` frame.

## Configuration

The server reads its configuration from environment variables:

| Variable                    | Required | Default                      |
| --------------------------- | -------- | ---------------------------- |
| `RTDB_PORT`                 | no       | `8300`                       |
| `RTDB_DATABASE_URL`         | yes      | —                            |
| `RTDB_ADMIN_KEY`            | yes      | —                            |
| `RTDB_PUBLIC_URL`           | no       | `http://localhost:8300`      |
| `RTDB_ALLOWED_ORIGINS`      | no       | empty (comma-separated list) |
| `RTDB_GITHUB_CLIENT_ID`     | no       | none                         |
| `RTDB_GITHUB_CLIENT_SECRET` | no       | none                         |
| `RTDB_GITHUB_BASE_URL`      | no       | `https://github.com`         |
| `RTDB_GITHUB_API_URL`       | no       | `https://api.github.com`     |
| `RTDB_GOOGLE_CLIENT_ID`     | no       | none                         |
| `RTDB_GOOGLE_CLIENT_SECRET` | no       | none                         |
| `RTDB_SESSION_TTL_DAYS`     | no       | `30`                         |

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

| Target                   | Purpose                                                                         |
| ------------------------ | ------------------------------------------------------------------------------- |
| `make build`             | `cargo build`                                                                   |
| `make fmt`               | `cargo fmt --all`                                                               |
| `make fmt-check`         | `cargo fmt --all -- --check`                                                    |
| `make lint`              | `cargo clippy --all-targets --all-features -- -D warnings`                      |
| `make typecheck`         | `cargo check --all-targets`                                                     |
| `make dev-db-up`         | Starts the dev Postgres container (`docker-compose.dev.yml`), waits for healthy |
| `make dev-db-down`       | Stops the dev Postgres container                                                |
| `make test`              | `dev-db-up`, then `cargo test`                                                  |
| `make checkall`          | `fmt-check` + `lint` + `typecheck` + `test`                                     |
| `make pre-commit`        | `pre-commit run --all-files`                                                    |
| `make pre-commit-update` | `pre-commit autoupdate`                                                         |

## Graceful shutdown

The server exits cleanly on `SIGINT` or `SIGTERM`: in-flight requests are allowed to
finish (via `axum::serve(...).with_graceful_shutdown(...)`) before the process stops. This
includes open WebSocket connections — shutdown waits for them to close rather than forcibly
dropping them, with no timeout of its own; Docker's SIGTERM→SIGKILL window is the backstop
that ultimately terminates a connection that never closes on its own.

## Known MVP limitations

- A **session** that expires or is logged out mid-connection keeps its open WebSocket
  connection working until the client disconnects. Machine-token revocation and
  allowlist removal ARE enforced live — every `subscribe`/`mutate` re-checks
  authorization on that open connection — but session-token expiry is not; closing that
  gap needs the session token hash retained on `Principal::User`, deferred to Plan 2.
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

## TypeScript client

The `ts-client/` package (`@par-rt-db/client`) is the browser/Node SDK: schema builder,
reactive WebSocket client, React bindings, and HTTP/admin clients. See
[`ts-client/README.md`](ts-client/README.md).
