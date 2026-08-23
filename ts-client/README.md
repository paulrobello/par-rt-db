# @par-rt-db/client

TypeScript client for [par-rt-db](../README.md). Speaks the server's declarative
query/transaction protocol over WebSocket (reactive) and HTTP (one-shot). No codegen —
your schema is a TypeScript object that is both pushed to the server and the source of
inferred types.

## Install

```sh
bun add @par-rt-db/client
```

## Define a schema (once, shared by app + admin)

```ts
import { defineSchema, defineTable, t } from "@par-rt-db/client";

export const schema = defineSchema({
  items: defineTable({
    projectId: t.id("projects"),
    title: t.string(),
    status: t.union(t.literal("backlog"), t.literal("done")),
    order: t.number(),
  }).index("by_project", ["projectId"]),
});
```

A table can declare opt-in per-row authorization (server-enforced on read,
mutate, and subscription re-run; machine tokens bypass):

- `.ownerField("userId")` — names a declared, string-compatible field holding
  the owner's `user_id`.
- `.collaboratorsField("memberIds")` — names an array-of-strings field; a
  principal reads/mutates a row if they own it **or** are listed in it.
- `.authorize(expr)` — a general `FilterExpr` predicate over doc fields plus
  `$user`/`$email` principal markers (e.g. `.authorize({ field: "userId", eq:
  "$user" })`).
- `.ttl("expiresAt")` — declares a document-TTL field; the server's per-db
  reaper deletes rows whose field value is past. The optional second argument
  (`defaultDurationMs`) stamps the field at insert time when a document omits
  it.
- `.defaults({ status: "backlog" })` — declares field-level default values
  (FM-32), stamped onto a **new** document that omits the key (insert / replace
  / upsert-insert only; `patch` never re-applies — clearing a field stays
  cleared).
- `.updatedAtField("updatedAt")` — declares a server-stamped last-write field
  (FM-36): names a declared `number` or `int64` field the server stamps with
  the current epoch-ms on every version-bumping write (insert, patch, replace,
  upsert both branches, patchByQuery, cascade setNull), overwriting any
  client-supplied value — a JSON number on `number`, a decimal string on
  `int64`. The field is optional in the insert/replace input type (omission is
  accepted and stamped; read types keep it required). Must differ from
  `ttl.field`; no index required.
- `.autoIncrementField("num")` — declares a server-assigned per-table
  monotonic counter (FM-37): names a declared `int64` field the server stamps
  with the next sequence value on insert (and upsert's insert branch) as a
  decimal string, overwriting any client-supplied value (a `defaults` entry
  on the field loses to the stamp). Optional in the insert input type —
  omission is accepted and assigned. Immutable after insert — a patch or
  replace that changes the stored value is rejected; round-tripping the
  equal value is allowed, and a replace that omits the field keeps the
  stored one. Must be `int64` exactly and differ from `ttl.field` and
  `updatedAtField`; legal in a unique index (the ticket-number guarantee).
  Gaps are possible on rolled-back transactions.
- `.computed(name, expr)` — declares a server-computed field (ENH-028): the
  server re-evaluates `expr` on every write (insert, patch, replace, upsert
  both branches, patchByQuery, cascade setNull) and stores the result,
  overwriting any client-supplied value. A null result REMOVES the key, so an
  optional computed field is absent when its expression yields null. Build
  `expr` with the `ve` helper namespace (below); the value lands in a typed
  column, so a computed field is indexable and orderable like any other.
  Push-time-validated: the key must be a declared, non-stamped field
  (not `ownerField`/`collaboratorsField`/`autoIncrementField`), referenced
  fields must be declared and non-computed, and a statically-known result
  kind must fit the field's type (wrap arithmetic in `ve.cast(..., "toString")`
  to store into an `int64` field).

The `ve` namespace builds the expression grammar for `.computed(...)`
(and migrate's typed `evalExpr`): `ve.field(name)` reads a declared field as
text (numbers become `"42"`-style strings), `ve.literal(v)` is any JSON
literal, `ve.concat(...parts)` skips null parts, `ve.add/sub/mul/div` do
IEEE-double arithmetic with null propagation, `ve.coalesce(...parts)`,
`ve.lower/upper/trim(value)` (trim strips spaces only), `ve.cast(value, to)`
with `to` one of `"toString" | "toNumber" | "toInt64" | "toBoolean"`,
`ve.now()` (epoch-ms), and `ve.case(whens, otherwise)` where each `when` is
the same `FilterExpr` DSL `.filter()` queries use.

```ts
import { defineSchema, defineTable, t, ve } from "@par-rt-db/client";

export const schema = defineSchema({
  users: defineTable({
    first: t.string(),
    last: t.string(),
    handle: t.string(),
    fullName: t.string(), // computed — never written by the client
    slug: t.string(), // computed — never written by the client
  })
    .index("by_fullName", ["fullName"]) // the computed column is indexable
    .computed("fullName", ve.concat(ve.field("first"), ve.literal(" "), ve.field("last")))
    .computed("slug", ve.lower(ve.trim(ve.field("handle")))),
});
```

Push it with the admin client (admin key required):

```ts
import { RtDbAdminClient } from "@par-rt-db/client";
const admin = new RtDbAdminClient({ url: "https://rtdb.example.com", adminKey: process.env.RTDB_ADMIN_KEY! });
await admin.createDb("kanban");
await admin.pushSchema("kanban", schema);
```

## React

```tsx
import { RtDbClient } from "@par-rt-db/client";
import { RtDbProvider, useQuery, useMutation } from "@par-rt-db/client/react";
import { createApi } from "@par-rt-db/client";
import { schema } from "./schema";

const client = new RtDbClient({ url: "wss://rtdb.example.com", db: "kanban", getToken: () => localStorage.getItem("rtdb-session-token") });
const api = createApi(schema);

// RtDbProvider is required around any component using the hooks. `authBaseUrl`
// is the server's HTTP origin, used for the OAuth sign-in popup and logout
// (any of the server's configured providers: GitHub/Google/GitLab/Microsoft/Apple/OIDC).
function App() {
  return (
    <RtDbProvider client={client} authBaseUrl="https://rtdb.example.com">
      <Board projectId="p1" />
    </RtDbProvider>
  );
}

function Board({ projectId }: { projectId: string }) {
  const items = useQuery(api.items.query().withIndex("by_project", [projectId]).collect());
  const mutate = useMutation();
  // items is Doc<typeof schema, "items">[] | undefined
}
```

### Authentication & token storage

The server's recommended auth mode is **cookie mode**: after OAuth sign-in the
server sets an HttpOnly `rtdb_session` cookie that the browser attaches
automatically, and `useRtDbAuth()` / `RtDbProvider` send `credentials: "include"`
on auth requests. JS can never read the cookie, and the cookie-mode sign-in
begins with `mode=cookie` (SEC-207) so the completion poll's response body
carries no token either — no script-readable copy of the credential exists at
any point, so an XSS or malicious
extension cannot lift the session token — this is the default and the safest
option for browser apps.

The `getToken` option shown above (reading a token out of `localStorage`) is an
**opt-in alternative** for non-browser clients or advanced setups where you
mint and persist a machine/session token yourself. It is a tradeoff: a token in
`localStorage` is readable by any JS running in your origin, so it is
 XSS-exfiltrable. Do not use `getToken` + `localStorage` in a browser app
unless you have a specific reason and have audited your CSP / dependency
surface; prefer cookie mode. Machine tokens minted via the admin API are the
right choice for server-to-server (`Node / CLI`) callers, where `localStorage`
does not apply.

For a credential-less guest, `useRtDbAuth().signInAnonymous()` (or a plain
`POST /auth/anonymous` outside React — see
[React Native / Expo](#react-native--expo)) mints an ephemeral anonymous
session — gated by the server's `RTDB_AUTH_ANONYMOUS_ENABLED` boot flag (default
off ⇒ `403`). It sets the same HttpOnly cookie **and** returns the plaintext
session token for the SDK/bearer path; an anonymous user owns its own documents
via per-row `ownerField`. The admin client (`RtDbAdminClient`) additionally
exposes the active-session management surface — `listSessions({ user?, limit? })`
and `revokeSession(tokenHash)` / `revokeUserSessions(userId)` — mirroring
`GET/DELETE /admin/sessions`, plus `mergeUsers(anonUserId, realUserId)` — the
operator escape hatch that merges an anonymous user into a real one — mirroring
`POST /admin/merge-users` (the typed `confirm == realUserId` guard is applied
for you).

#### The `unreachable` auth state

A cookie-mode app served from an origin the server has not allowlisted gets
its WS upgrade rejected with HTTP 403 — which the browser surfaces only as a
close code 1006, indistinguishable from a server outage. To bound that blind
spot, `getAuthState()` / `onAuthChange` expose a fourth state,
`"unreachable"`: after `authUnreachableAfterAttempts` consecutive socket
closes during the auth handshake (default 5; `authUnreachableAfterAttempts: 0`
disables the signal), the state flips from the eternal `"authenticating"` to
`"unreachable"` so the app can render "sign-in unavailable" instead of a
spinner. The client keeps retrying in the background — the state is a
signal, not a stop — and any completed handshake, a `4401`, or a fresh
`connect()` / `setToken()` clears it.

## Node / CLI

```ts
import { RtDbHttpClient, createApi, mutation } from "@par-rt-db/client";
import { schema } from "./schema";

const db = new RtDbHttpClient({ url: "https://rtdb.example.com", db: "kanban", token: process.env.RTDB_TOKEN! });
const api = createApi(schema);
const rows = await db.query(api.items.query().withIndex("by_project", ["p1"]).collect());
// Many queries in one round trip (`POST /api/query-batch`) — outcomes align with the
// input order; a per-query error is that slot's { ok: false, error } and never throws:
const [top, recent] = await db.batchQuery([
  api.items.query().withIndex("by_project", ["p1"]).take(5).json,
  api.items.query().order("desc").take(5).json,
]);
// An insert step returns `{ id }` (not a bare string); patch/delete/expect* return null.
const [{ id }] = (await db.mutate(
  mutation().insert("items", { projectId: "p1", title: "x", status: "backlog", order: 1 }).build(),
)) as [{ id: string }];
```

Beyond the `collect`/`take` queries above, the builder carries `first()`,
`unique()`, `count()`, `distinct()`, `aggregate(op, groupBy?)`, and
`paginate(cursor, n)` — plus `.filter(expr)`, an `eq`/`neq`/`gt`/`gte`/`lt`/
`lte`/`in`/`contains`/`exists`/`and`/`or`/`not` predicate over declared fields
that narrows the matching set server-side. A filter value whose JSON kind
contradicts the declared field type (a number against a string field) is
rejected with `BAD_REQUEST` — on the server and in `InMemoryRtDbClient` alike —
instead of silently matching nothing. One more op exists but is not on this
list: `patchByQuery`/`deleteByQuery` step filters additionally accept the
execution-time-relative `olderThan` op —
`{ op: "olderThan", field: "completedAt", ms: 604800000 }` — matching rows
whose epoch-ms field is strictly older than `now − ms`, with the cutoff derived
from the clock **at each execution** (a scheduled txn carrying it stays fresh
on every fire). It is by-query-only — `.filter()`, `authorize` predicates,
partial-index `where` predicates, and computed `case` whens reject it — and
requires a declared `number`/`int64` field with `ms >= 0`; `InMemoryRtDbClient`
evaluates it against its injected `now` clock. `.fields(...names)` projects
each result doc to the listed fields (system fields
`_id`/`_creationTime`/`_version` are always kept; `.fields()` with no args is
an ids-only view; unknown names are `BAD_REQUEST`) — it composes with every
doc-bearing terminal, and a projected subscription stays silent when a write
changes only non-projected fields.

```ts
// given .index("by_project_status", ["projectId", "status"]) — distinct and
// aggregate read the index field AFTER the eq prefix:
const statuses = await db.query(
  api.items.query().withIndex("by_project_status", ["p1"]).distinct(),
);
// → ["backlog", "done"] — unique values, ascending, nulls included and sorted last
const perStatus = await db.query(
  api.items.query().withIndex("by_project_status", ["p1"]).aggregate("count", true),
);
// → { key, value }[] ordered by key: rows missing the group field form one
//   key:null group (sorted last); null aggregate values are skipped (SQL
//   semantics), so a group whose values are all null returns value:null.
```

## React Native / Expo

The core clients run on React Native (Hermes) with **no polyfills** — the
reactive `RtDbClient` uses RN's global `WebSocket`, `fetch`, and `setTimeout`
directly, and every browser-only API (`localStorage`, `document`, `window`) sits
behind a `typeof` guard so importing is safe. Smoke-tested end-to-end in an
Expo iOS app (connect → subscribe → mutate → live push) against a local server.

```tsx
import * as SecureStore from "expo-secure-store";
import { RtDbClient } from "@par-rt-db/client";

const client = new RtDbClient({
  url: "wss://rtdb.example.com",
  db: "kanban",
  // There is no localStorage in RN — keep the session token in the keychain.
  getToken: () => SecureStore.getItemAsync("rtdb-token"),
});
```

Per surface:

- **Reactive client (`RtDbClient`)** — works as-is. The `webSocketFactory` /
  `setTimeoutImpl` / `clearTimeoutImpl` options exist for substituting
  platform-specific transports, but RN's globals satisfy the defaults. JS timers
  pause while the app is backgrounded, so the heartbeat stalls; the client's
  existing reconnect-with-backoff recovers the socket when the app foregrounds.
- **HTTP client (`RtDbHttpClient`)** — works as-is. Pass `Uint8Array` or
  `ArrayBuffer` to `upload()` (RN's `fetch` handles both; `Blob` works where
  RN's Blob polyfill is present, `ReadableStream` does not exist in RN).
- **Auth** — anonymous sign-in is a plain `fetch("…/auth/anonymous", { method:
  "POST" })`; store the returned `{ token }` in `expo-secure-store` and feed it
  back via `getToken` (this is exactly what `useRtDbAuth().signInAnonymous()`
  does in the browser). The provider-OAuth helpers in `react` use
  `window.open`, which RN does not have — open
  `${url}/auth/{provider}/begin?origin=` with `openAuthSessionAsync` from
  `expo-web-browser` and poll `GET /auth/state?state=` (the `state` token from
  the begin response) until it returns the session token — the same poll the
  browser SDK performs.
- **Admin client (`RtDbAdminClient`)** — bearer mode (`adminKey`) works; the
  cookie/CSRF mode is browser-only (no `document` in RN). Don't embed the admin
  key in an app — keep admin calls server-side or in the CLI.

The `react` entry (`useQuery` / `useMutation` / `usePresence` /
`usePaginatedQuery` hooks) imports clean under RN (it depends only on `react`),
so the hooks are usable in an Expo app; only the popup-based OAuth helpers
within it are browser-only, per the auth bullet above.

## Scheduling

Both the reactive `RtDbClient` (WS) and the one-shot `RtDbHttpClient` expose
scheduled/cron transactions:

```ts
// Schedule a txn — `when` is afterMs / runAt (one-shot), cron (5-field, UTC, min-first), or interval (everyMs).
const { id } = await db.schedule(
  mutation().insert("tasks", { title: "deferred", done: false }).build(),
  { type: "afterMs", ms: 60_000 },
);
await db.cancelSchedule(id);      // …or pauseSchedule(id) / resumeSchedule(id)
const jobs = await db.listSchedules();   // ScheduleInfo[]
```

The server validates the `cron` expression and resolves `due_at`; the client
does no schedule arithmetic. Delivery is at-least-once, so scheduled txns
should be idempotent. The in-memory test client (`InMemoryRtDbClient`) mirrors
the store and exposes a timer-less `tick(nowMs?)` that fires due jobs
synchronously in unit tests.

## Durable workflows

Durable declarative workflows (FM-29) — multi-step txn pipelines with per-step
retry and sleeps, executed server-side and observable without holding a
connection. Both the reactive `RtDbClient` and the one-shot `RtDbHttpClient`
expose the trio, and a txn can start (or cancel) a run as a step, atomic with
its writes:

```ts
import { RtDbHttpClient, createApi, mutation } from "@par-rt-db/client";
import type { WorkflowSpec } from "@par-rt-db/client";

const db = new RtDbHttpClient({ url: "https://rtdb.example.com", db: "kanban", token: process.env.RTDB_TOKEN! });

// A spec is snapshotted verbatim per run, so step values are literals known
// at build time — there is no prior-step output referencing.
const itemId = "wi_123";

const spec: WorkflowSpec = {
  name: "onboard",
  steps: [
    { txn: mutation().insert("workItems", { title: "welcome", done: false }).build() },
    {
      txn: { steps: [{ op: "patch", table: "workItems", id: itemId, fields: { done: true } }] },
      retry: { maxAttempts: 5, initialRetryMs: 500, maxRetryMs: 30_000 },
      sleepBeforeMs: 60_000,
    },
  ],
};

const { id } = await db.startWorkflow(spec);    // HTTP client → { id }; the reactive RtDbClient resolves WorkflowInfo
await db.cancelWorkflow(id);                    // false for a missing/terminal run (a no-op, not an error)
const runs = await db.listWorkflows("running"); // WorkflowInfo[], newest first

// …or start one atomically inside a txn:
await db.mutate(mutation().insert("users", { name: "a" })
  .startWorkflow(spec).build());
```

Steps are ordinary declarative txns firing as the system principal (a scoped
machine token is confined at submit time, not per step); delivery is
at-least-once per step, so write idempotent step txns. A step that exhausts
its retries fails the run (terminal). `adminStartWorkflow` on the admin client
covers the admin route. The in-memory harness models the engine: it validates
the spec and advances runs on `tick()`, so workflow flows are testable with no
network.

A step can instead be an `awaitSignal` approval gate (exactly one of
`txn`/`awaitSignal` per step): `awaitSignal(name, timeoutMs?)` parks the run
in the non-terminal `waiting` state until `signalWorkflow(id, name, payload?)`
— on both the reactive and HTTP clients, `adminSignalWorkflow` on the admin
client — delivers a matching signal. The payload is latest-wins and is
recorded verbatim on the step outcome (`signal`); while waiting,
`WorkflowInfo` carries `waitingFor`/`waitedSince`. An optional `timeoutMs`
counts as a failed attempt through the step's `retry` (each re-wait is the
full timeout again, no backoff); omit it to wait forever — cancel is the
escape. The harness models the parked state on `tick()` too.

```ts
import { awaitSignal } from "@par-rt-db/client";

const spec: WorkflowSpec = {
  name: "gate",
  steps: [
    { txn: mutation().insert("workItems", { title: "review me" }).build() },
    awaitSignal("approve", 86_400_000), // parks `waiting` for up to 24h
  ],
};
await db.signalWorkflow(id, "approve", { approvedBy: "u1" }); // releases the gate
```

## Cascade delete & soft delete

Referential actions (FM-33) are declared on the CHILD table's id field — no
SQL FK; the server expands the cascade app-level inside the txn so every
cascaded row is a first-class op (op-feed, audit, webhooks, subscriptions):

```ts
const schema = defineSchema({
  projects: defineTable({ title: t.string() }),
  tasks: defineTable({
    title: t.string(),
    projectId: t.id("projects", { onDelete: "cascade" }), // or "restrict" / "setNull"
  })
    .index("by_project", ["projectId"]) // required: single-field, non-unique btree
    .softDelete(), // opt in per table: delete stamps deletedAt instead of removing
});

// setNull needs the optional shape: t.optional(t.id("projects", { onDelete: "setNull" }))

await db.mutate(mutation().delete("projects", projectId).build()); // cascades tasks
await db.mutate(mutation().undelete("tasks", taskId).build()); // restore a soft-deleted row
```

`onDelete` deletes children (`cascade`), blocks while a live child references
the row (`restrict`, Conflict), or clears the child's field (`setNull` — the
key is removed and the child's version bumps). One initiating delete may
cascade through at most 10,000 rows (Conflict + full rollback past that), and
self-reference cycles terminate. On a `.softDelete()` table, `delete` /
`deleteByQuery` stamp an internal `deletedAt` (+version) instead of removing:
the row is invisible to every read terminal, eq-lookup, and unique index,
per-id writes see NotFound, and `undelete` restores it (version+1, idempotent
on a live row, Conflict when a live row now holds its unique key, BadRequest
on a table without `softDelete`). A stamped row never triggers or receives a
cascade, and the TTL reaper always hard-deletes. Adding or changing
`onDelete` — and adding `softDelete` — is an additive schema push.

## Search

`.search(index, query, opts)` full-text queries a declared search index
(composes with `.take(n)`; an optional `filter` narrows results server-side).
The query text honors web-search operators (the server compiles it via
`websearch_to_tsquery`): a quoted phrase (`"exact phrase"`) requires the words
adjacent, a bare `or` unions alternatives, and `-term` excludes — plain terms
stay AND. An optional `mode: "trgm"` switches to case-insensitive
substring/autocomplete matching, and `snippet: true` (tsquery mode only —
rejected with `mode: "trgm"`) adds a `_searchSnippet` string to each hit: a
server-rendered excerpt with matched terms wrapped in `<mark>…</mark>`.

```ts
const hits = await db.query(
  api.notes.query().search("search_text", '"release plan" -draft', { snippet: true }).take(10),
);
// hits[0]._searchSnippet → e.g. "… the <mark>release</mark> <mark>plan</mark> is …"
```

## Realtime presence

Ephemeral "who is online right now" data (online indicators, cursors, typing)
that doesn't fit durable document queries uses the presence layer — on by
default on the server (`RTDB_PRESENCE_ENABLED`, default on), in-memory, connection-bound (the open
`/sync` WS *is* the liveness signal), and not committer-bound or persisted. The
reactive client exposes `presence` / `updatePresence` / `leavePresence`, and the
React hook `usePresence(room)` joins on mount, re-renders on every snapshot, and
leaves on unmount:

```tsx
import { usePresence } from "@par-rt-db/client/react";

function Cursors({ roomId }: { roomId: string }) {
  const { members, updatePresence, leavePresence } = usePresence(roomId);
  // members: PresenceMember[] — each carries { connectionId, user, state }
  return (
    <ul>
      {members.map((m) => (
        <li key={m.connectionId}>{m.user.email ?? m.user.kind}</li>
      ))}
    </ul>
  );
}
// updatePresence({ x: 120, y: 40, typing: true }) broadcasts on the next flush.
```

`InMemoryRtDbClient` mirrors `PresenceRooms` so app-level presence flows are
exercisable in unit tests with no network. See FEATURE_MATRIX #25 and the
top-level [`README.md`](../README.md#realtime-presence).

## Schema migration

Destructive/type-changing schema transformations (rename, type coercion, removal,
default backfill) are a deliberate admin operation separate from the additive
`pushSchema`. Build a `Migration` and apply it (or dry-run first) via the admin
client — `POST /admin/db/{db}/migrate` runs the directives transactionally inside
the committer, so live queries, the op feed, audit, and webhooks all fire.

```ts
import { Migration } from "@par-rt-db/client";

const result = await admin.migrate("kanban", new Migration()
  .renameField("items", "title", "summary")
  .changeType("items", "order", { type: "string" }, "toString", "0")
  .setDefault("items", "status", "backlog")
  .dryRun()                       // preview first — returns the report + derived schema
  .build());
if (result.applied) { /* … */ }   // re-run without .dryRun() to apply
```

`changeType` takes a closed `cast` (`toString`/`toNumber`/`toInt64`/`toBoolean`);
the optional `default` substitutes for un-coercible rows (without it a single bad
value rolls the whole migrate back atomically). `evalExpr` is the scoped raw-SQL
escape hatch (one table's `doc` jsonb, no joins/DDL). See
[`docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md`](../docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md).

## File storage

Both the one-shot `RtDbHttpClient` and the reactive `RtDbClient` (delegating to
HTTP) expose file storage; `InMemoryRtDbClient` mirrors it in memory:

```ts
const { id } = await db.upload(bytes, "image/png");   // → { id, sha256, size, contentType }
db.getUrl(id);                                         // public URL for <img src> — no fetch
db.transformUrl(id, { w: 128, fit: "cover" });         // same URL with image-transform params — no fetch
const meta = await db.getFileMetadata(id);             // { id, sha256, size, contentType?, creationTime }
const { url, expiresAt } = await db.getSignedUrl(id, 3600);  // signed, time-limited public URL
await db.deleteFile(id);                               // revokes the public URL
```

`upload` POSTs raw bytes to `POST /api/storage/{db}` (the client injects its own
db); `getUrl` returns `${url}/storage/${id}` for the browser to fetch with no
token; `getSignedUrl` calls `GET /api/storage/{db}/{id}/signed-url?ttlSeconds=`
to mint an HMAC-signed, time-limited public URL. Storage is HTTP-only (no
reactive updates).

## In-memory test client

`InMemoryRtDbClient` (`src/in_memory/`) is an in-memory implementation of the
client surface for unit tests — no server, no Postgres. It mirrors the schema,
query, and transaction semantics, including cursor pagination, so app code can
exercise the full DSL against it directly.

`RtDbClient` also accepts an opt-in `optimisticUpdates` option that applies
mutations to local state before the server confirms them.

## Development

The full gate runs from the **repo root** and covers all six packages; the
integration suite is opt-in and lives under `ts-client/` (these are separate
Makefiles — `make test-integration` is not a root target):

```sh
# from the repo root — fmt-check + lint + typecheck + test across all six packages
make checkall
make env-drift-check   # confirms RTDB_* keys stay in sync across .env.example, compose, and source

# from ts-client/ — opt-in live-server tests; needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
make test-integration
```
