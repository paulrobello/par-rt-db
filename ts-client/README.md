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
- `.ttl({ field: "expiresAt" })` — declares a document-TTL field; the server's
  per-db reaper deletes rows whose field value is past.

Push it with the admin client (admin key required):

```ts
import { RtDbAdminClient } from "@par-rt-db/client";
const admin = new RtDbAdminClient({ url: "https://rtdb.pardev.net", adminKey: process.env.RTDB_ADMIN_KEY! });
await admin.createDb("kanban");
await admin.pushSchema("kanban", schema);
```

## React

```tsx
import { RtDbClient } from "@par-rt-db/client";
import { RtDbProvider, useQuery, useMutation } from "@par-rt-db/client/react";
import { createApi } from "@par-rt-db/client";
import { schema } from "./schema";

const client = new RtDbClient({ url: "wss://rtdb.pardev.net", db: "kanban", getToken: () => localStorage.getItem("rtdb-session-token") });
const api = createApi(schema);

// RtDbProvider is required around any component using the hooks. `authBaseUrl`
// is the server's HTTP origin, used for the OAuth sign-in popup and logout
// (any of the server's configured providers: GitHub/Google/GitLab/Microsoft/Apple/OIDC).
function App() {
  return (
    <RtDbProvider client={client} authBaseUrl="https://rtdb.pardev.net">
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
on auth requests. JS can never read the cookie, so an XSS or malicious
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
and `revokeSession(tokenHash)` / `revokeSessionsForUser(user)` — mirroring
`GET/DELETE /admin/sessions`, plus `mergeUsers(anonUserId, realUserId)` — the
operator escape hatch that merges an anonymous user into a real one — mirroring
`POST /admin/merge-users` (the typed `confirm == realUserId` guard is applied
for you).

## Node / CLI

```ts
import { RtDbHttpClient, createApi, mutation } from "@par-rt-db/client";
import { schema } from "./schema";

const db = new RtDbHttpClient({ url: "https://rtdb.pardev.net", db: "kanban", token: process.env.RTDB_TOKEN! });
const api = createApi(schema);
const rows = await db.query(api.items.query().withIndex("by_project", ["p1"]).collect());
// An insert step returns `{ id }` (not a bare string); patch/delete/expect* return null.
const [{ id }] = (await db.mutate(
  mutation().insert("items", { projectId: "p1", title: "x", status: "backlog", order: 1 }).build(),
)) as [{ id: string }];
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
  url: "wss://rtdb.pardev.net",
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
// Schedule a txn — `when` is afterMs / runAt (one-shot) or cron (5-field, UTC, min-first).
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
const meta = await db.getFileMetadata(id);             // { id, sha256, size, contentType?, creationTime }
const { url, expiresAt } = await db.getSignedUrl(id, { ttlSeconds: 3600 }); // signed, time-limited public URL
await db.deleteFile(id);                               // revokes the public URL
```

`upload` POSTs raw bytes to `POST /api/storage/{db}` (the client injects its own
db); `getUrl` returns `${url}/storage/${id}` for the browser to fetch with no
token; `getSignedUrl` calls `GET /api/storage/{db}/{id}/signed-url?ttlSeconds=`
to mint an HMAC-signed, time-limited public URL. Storage is HTTP-only (no
reactive updates).

## In-memory test client

`InMemoryRtDbClient` (`src/in_memory.ts`) is an in-memory implementation of the
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
