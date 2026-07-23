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
