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

A table can declare an owner field for opt-in per-row authorization
(server-enforced on read, mutate, and subscription re-run; machine tokens
bypass): `.ownerField("userId")` names a declared, string-compatible field
holding the owner's `user_id`.

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
await db.deleteFile(id);                               // revokes the public URL
```

`upload` POSTs raw bytes to `POST /api/storage/{db}` (the client injects its own
db); `getUrl` returns `${url}/storage/${id}` for the browser to fetch with no
token. Storage is HTTP-only (no reactive updates).

## In-memory test client

`InMemoryRtDbClient` (`src/in_memory.ts`) is an in-memory implementation of the
client surface for unit tests — no server, no Postgres. It mirrors the schema,
query, and transaction semantics, including cursor pagination, so app code can
exercise the full DSL against it directly.

`RtDbClient` also accepts an opt-in `optimisticUpdates` option that applies
mutations to local state before the server confirms them.

## Development

```sh
make checkall          # from the repo root: fmt-check + lint + typecheck + test (all 6 packages)
make test-integration  # opt-in; needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
```
