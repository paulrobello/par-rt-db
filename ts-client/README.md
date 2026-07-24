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
// is the server's HTTP origin, used for the GitHub sign-in popup and logout.
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
make checkall          # from the repo root: fmt-check + lint + typecheck + test (all 3 packages)
make test-integration  # opt-in; needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
```
