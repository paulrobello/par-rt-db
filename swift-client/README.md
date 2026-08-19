# swift-client — ParRtDbClient

Swift client for [par-rt-db](../README.md): the fifth implementation of the wire
contract (after the server, ts-client, rust-client, and python-client). One
UI-free core package (`ParRtDbClient` — wire types, query/mutation/schema DSLs,
one-shot HTTP client, admin client + migrate DSL, reactive WebSocket client)
plus a thin SwiftUI package (`ParRtDbUI` — an `@Observable LiveQuery` wrapper).
Design and scope:
[docs/superpowers/specs/2026-08-18-swift-client-design.md](../docs/superpowers/specs/2026-08-18-swift-client-design.md).

## Requirements

- Swift 6 (Swift concurrency in strict mode; `swift-tools-version:6.0`)
- iOS 17+ / macOS 14+ (the `@Observable` macro requires it)
- Zero third-party dependencies — Foundation (+ `Observation` in `ParRtDbUI`) only
- Building/linting from the repo: `swiftformat` and `swiftlint` on PATH
  (`brew install swiftformat swiftlint`)

## Installation (Swift Package Manager)

The package currently lives in this repo — depend on it by local path:

```swift
// in your Package.swift
dependencies: [
    .package(path: "../par-rt-db/swift-client"),
],
targets: [
    .target(name: "MyApp", dependencies: [
        .product(name: "ParRtDbClient", package: "swift-client"),
        .product(name: "ParRtDbUI", package: "swift-client"),   // if you use LiveQuery
    ]),
],
```

In an Xcode app project: File → Add Package Dependencies… → Add Local… → select
`swift-client/`.

```swift
// Future remote form, once the package is published (SPM resolves packages at
// a repo root, so this lands when swift-client gets its own release tag):
// .package(url: "https://github.com/paulrobello/par-rt-db.git", branch: "main")
```

## Quick start

### One-shot HTTP: query + mutate

`RtDbHttpClient` is an actor — one instance per database, bearer-token
authorized. Queries are built with `TableQuery`, decoded either as the raw
`JSONValue` payload or into your own `Codable` model via `run(_:as:)`.

```swift
import ParRtDbClient

struct TaskDoc: Codable, Sendable, Identifiable {
    var _id: String
    var title: String
    var done: Bool
    var priority: Double?
    var id: String { _id }
}

let http = RtDbHttpClient(url: "https://rtdb.example.com", db: "myapp", token: token)

// Typed query: the query's own terminal picks the decode shape.
let openTasks = try TableQuery("tasks")
    .withIndex("by_done")
    .eq(.bool(false))
    .order(.asc)
    .take(20)
    .build()
let tasks: [TaskDoc] = try await http.run(openTasks, as: [TaskDoc].self)

// Point read, decoded (nil when absent) — or use http.get(_:_) for the raw JSONValue?.
let got = try TableQuery("tasks").get(id).build()
let doc: TaskDoc? = try await http.run(got, as: TaskDoc?.self)

let txn = try MutationBuilder()
    .insert("tasks", ["title": .string("Buy milk"), "done": .bool(false)])
    .build()
let results: [StepResult] = try await http.mutate(txn)   // one result per step
```

### Reactive: `RtDbClient` + `LiveQuery` in SwiftUI

`RtDbClient` (also an actor) owns one `/sync` WebSocket: it authenticates,
heartbeats, reconnects with jittered backoff, replays subscriptions after
reconnect, and deduplicates subscriptions by canonical query shape — multiple
`subscribe` calls for the same shape share one server subscription.

```swift
import ParRtDbClient
import ParRtDbUI
import SwiftUI

let client = RtDbClient(
    url: "https://rtdb.example.com",          // http(s):// is converted to ws(s):// + /sync
    db: "myapp",
    getToken: { await TokenStore.shared.rtdbToken() },   // re-fetched on every (re)connect
    transportFactory: { _ in URLSessionWebSocketTransport() }
)
await client.connect()
```

`LiveQuery<T>` (from `ParRtDbUI`) pumps one subscription's snapshots into
observable state on the main actor:

```swift
struct TaskListView: View {
    @State private var live: LiveQuery<[TaskDoc]>

    init(client: RtDbClient, query: Query) {
        _live = State(initialValue: LiveQuery(client: client, query: query))
    }

    var body: some View {
        switch live.state {
        case .pending:
            ProgressView()
        case .failed(let error):
            ContentUnavailableView("Query failed", systemImage: "exclamationmark.triangle",
                                   description: Text(error.message))
        case .value(let tasks):
            List(tasks) { task in Label(task.title, systemImage: task.done ? "checkmark.circle" : "circle") }
        }
    }
}

// Usage: TaskListView(
//     client: client,
//     query: try TableQuery("tasks").withIndex("by_done").eq(.bool(false)).build()
// )
```

For non-SwiftUI callers, use the subscription handle directly:

```swift
let sub: Subscription<[TaskDoc]> = try await client.subscribe(openTasks, as: [TaskDoc].self)
for await snapshot in sub.stream {
    switch snapshot {
    case .pending: break                       // never yielded by stream, only in `current`
    case .value(let docs): print(docs)
    case .failed(let error): print(error)
    }
}
await sub.cancel()     // refcounted: the server unsubscribes when the last handle drops
```

Mutations, scheduling, and workflows also run over the WS client
(`mutate(_:idempotencyKey:)`, `schedule(_:when:)`, `cancelSchedule(_:)`,
`pauseSchedule(_:)`, `resumeSchedule(_:)`, `listSchedules()`,
`startWorkflow(_:)`, `cancelWorkflow(_:)`, `listWorkflows()`).

Presence rooms (ENH-015): `presence(room:state:)` joins (or attaches to) a
room and returns a `PresenceHandle` whose `current`/`stream` carry
`PresenceSnapshot` — `.pending` until the first `presenceSnapshot`, then
`.members([PresenceMember])`, or `.rejected` if the server refuses the join.
Joined rooms replay with their freshest state on every reconnect;
`updatePresence(room:state:ttlMs:)` broadcasts a new per-connection state
(`ttlMs` heartbeats the state; nil means no expiry) and
`leavePresence(room:)` is the explicit leave — stopping a handle's stream
only stops listening.

```swift
let room = await client.presence(room: "doc:1", state: .object(["mode": .string("edit")]))
if case .members(let members) = room.current { print(members.map(\.connectionId)) }
await client.updatePresence(room: "doc:1", state: .object(["mode": .string("view")]))
await client.leavePresence(room: "doc:1")
```

Optimistic updates (off by default — `RtDbClientConfig.optimisticUpdates`):
with the flag on, every `mutate` overlays its projected effect on each
matching subscription immediately (synthetic `__optimistic__` ids for
inserts), reconciles to the authoritative `queryUpdate`, and rolls back to
the last server value on `mutateErr`, a disconnect-reject, or `close()`. The
projection is conservative — only unambiguous `collect`/`take`/`get`/filter-
delete shapes overlay; everything else waits for server truth.

## DSL examples

Numeric convention: the query and mutation builders take `Int` for numeric
arguments and throw `RtDbError(.badRequest)` from `build()` on values outside
the wire's `UInt32` range, while the schema DSL takes the exact wire types
(`UInt32`/`Int64`) directly.

### Query DSL (`TableQuery`)

```swift
// Index + eq prefix + range bound + order + take
let q1 = try TableQuery("items")
    .withIndex("by_priority").eq(.string("p1")).gte(.int(2)).lt(.int(10))
    .order(.desc).take(50).build()

// Every terminal is supported: get / collect / take / unique / first / count /
// distinct / aggregate (optionally grouped) / paginate / search / vectorSearch / hybridSearch
let page = try TableQuery("items").withIndex("by_priority").order(.asc)
    .paginate(cursor: nil, numItems: 20).build()
let pageResult: Paginated<TaskDoc> = try await http.run(page, as: Paginated<TaskDoc>.self)

// Full-text search (trgm mode + snippet highlighting); vector + hybrid search
let hits = try TableQuery("items")
    .search("by_title", "database notes", mode: .trgm, snippet: true).build()
let near = try TableQuery("items").vectorSearch("embedding", vector, limit: 10).build()
let fused = try TableQuery("items").hybridSearch("database notes", vector, limit: 10).build()

// Db-side filter predicate (eq/neq/gt/gte/lt/lte/in/or/and/not/contains/exists)
let done = try TableQuery("tasks")
    .filter(.and(exprs: [
        .eq(field: "status", value: .string("done")),
        .gt(field: "priority", value: .int(1)),
    ]))
    .build()

// Terminal-combination rules are enforced client-side at build(), with the
// server's verbatim messages (e.g. "first cannot be combined with take").
```

### Mutation DSL (`MutationBuilder`)

All 14 step ops: `insert`, `patch`, `replace`, `delete`, `undelete`,
`expectVersion`, `expectAbsent`, `upsert`, `patchByQuery`, `deleteByQuery`,
`schedule`, `cancelSchedule`, `startWorkflow`, `cancelWorkflow`.

```swift
let txn = try MutationBuilder()
    .expectVersion("tasks", id, 3)
    .patch("tasks", id, ["done": .bool(true)])
    .insert("audit_log", ["what": .string("task_done")])
    .build()
let results = try await http.mutate(txn)

// Bulk by filter, capped at the server's per-step limit (truncated result)
let bulk = try MutationBuilder()
    .patchByQuery("tasks", filter: .eq(field: "status", value: .string("stale")),
                  patch: ["archived": .bool(true)])
    .build()
```

### Schema DSL (`SchemaBuilder`)

```swift
let schema = SchemaBuilder()
    .table("projects") { t in
        t.field("name", .string)
            .field("slug", .string)
            .index("by_slug", on: ["slug"]).unique()
    }
    .table("tasks") { t in
        t.field("title", .string)
            .field("done", .boolean)
            .field("priority", .optional(.number))
            .field("project", .id("projects"))
            .field("assignee", .string)
            .field("expiresAt", .number)
            .index("by_done", on: ["done"])
            .index("by_project", on: ["project"])
            .index("by_expires_at", on: ["expiresAt"])   // ttl requires an index on its field
            .ttl("expiresAt", defaultDurationMs: 3_600_000)
            .ownerField("assignee")
            .softDelete()
    }
    .build()

try await http.pushSchema(schema)          // POST /admin/push-schema (same token)
let diff = try await http.previewSchema(schema)   // advisory diff, applies nothing
```

The full `FieldType` set is supported (15 variants — including `int64`, `bytes`,
`any`, `record`, `vector`, `literal`, `union`, `array`, `object`), plus search
indexes (`searchIndex(_:on:language:)`), vector indexes
(`vectorIndex(_:on:dimensions:filterFields:metric:)`), partial indexes
(`whereClause(_:)`), `collaboratorsField`, `authorize(_:)`, `defaults(_:)`, and
`onDelete` on id fields (`.id("projects").onDelete(.cascade)`).

## Storage

`RtDbHttpClient` carries the full file-storage surface (HTTP-only, like the
server):

```swift
let id = try await http.upload(pdfData, contentType: "application/pdf")  // -> file id
let publicUrl = http.getUrl(id)                          // no request — opaque public URL
let signed = try await http.getSignedUrl(id, ttlSeconds: 300)   // HMAC time-limited URL
let thumb = http.transformUrl(id, width: 256, fit: .cover, format: .jpeg)  // read-time image transform
let meta: JSONValue = try await http.getFileMetadata(id) // {id, sha256, size, contentType?, creationTime}
try await http.deleteFile(id)                            // idempotent; revokes the public URL
```

## Admin client

`RtDbAdminClient` (an actor, like `RtDbHttpClient`) keys the whole `/admin/*`
control plane with the instance admin key — the Swift mirror of the rust
client's admin surface: database lifecycle (create/delete/clone/export/
import), the schema plane (push/preview/get/history/restore), declarative
migrations, tokens + allowlist + admins, metrics/hot-config/op-feed/audit/
sessions/merge-users, owner-bypass admin query/mutate, explain + slow
queries, backups, webhooks + deliveries, and the admin views of workflows,
schedules, and file storage.

```swift
let admin = RtDbAdminClient(url: "https://rtdb.example.com", adminKey: adminKey)

try await admin.createDb("myapp")
try await admin.pushSchema(db: "myapp", schema: schema)

// Declarative migration: preview first, then apply.
let migration = Migration()
    .renameField("tasks", from: "done", to: "isDone")
    .setDefault("tasks", field: "priority", value: .int(0))
let directives = migration.build()
let preview = try await admin.migrateSchema(db: "myapp", directives: directives, dryRun: true)
if preview.schema.tables["tasks"] != nil {   // dry-run derives the shape, commits nothing
    _ = try await admin.migrateSchema(db: "myapp", directives: directives, dryRun: false)
}

let metrics = try await admin.metrics()            // counters, gauges, latency percentiles
let tokens = try await admin.listTokens("myapp")   // minted machine tokens
```

The `Migration` builder queues every directive kind — `renameField`,
`renameTable`, `changeType` (with a `Cast` + optional substitution default),
`dropField`, `dropTable`, `dropIndex`, `setDefault`, and `evalExpr` in both
the legacy raw-SQL form and the typed `ValueExpr` grammar (ENH-020 — field
reads, literals, concat/arithmetic, `coalesce`, text ops, closed casts,
`now`, and `case`/`when` branches; no raw-SQL escape). `build()` yields the
directives for `migrateSchema`; `buildRequest()` wraps them with the dry-run
flag.

## Error handling

Every failure is the standard `RtDbError` envelope — `{code, message}` (plus
`retryAfter` seconds on `.rateLimited`), with all ten `ErrorCode` cases. The
HTTP client decodes the envelope on any non-2xx and never leaks raw response
bodies into thrown errors; the WS client surfaces `authErr`/`subscribeErr`/
op-error frames the same way.

```swift
do {
    let tasks: [TaskDoc] = try await http.run(openTasks, as: [TaskDoc].self)
} catch let error as RtDbError {
    switch error.code {
    case .unauthorized: await TokenStore.shared.refresh()   // then retry
    case .preconditionFailed: break                          // OCC conflict — rebuild and retry
    case .rateLimited: if let wait = error.retryAfter { /* back off `wait` seconds */ }
    default: break
    }
}

// Optimistic-concurrency retry helper (also available as http.mutateWithRetry):
let result = try await retryOnPrecondition {
    try await http.mutate(rebuiltTxn)
}
```

## Testing

- `swift test` from `swift-client/` — 387 tests in 26 suites (Swift Testing
  framework): wire-type round-trips, DSL builder shapes, URLProtocol-mocked
  HTTP + admin tests, fake-transport WS tests, in-memory engine tests, and
  `LiveQuery` main-actor tests.
- The wire layer runs the shared [`wire-corpus/wire-corpus.json`](../wire-corpus/wire-corpus.json)
  parity corpus (ARC-008): every message/user/schedule/query/migrate section
  must round-trip value-identically, and every `rejects_*` section must be
  rejected.
- The in-memory engine runs both behavioral corpora against
  [`wire-corpus/semantics/`](../wire-corpus/semantics) (ENH-023 — all 53
  cases: per-case schema/seed/op/expect with `normalize` projection,
  `unordered` multiset compare, `$idRef` substitution, the `$prev` paginate
  sentinel, and error-code assertions) and
  [`wire-corpus/golden-vector.json`](../wire-corpus/golden-vector.json)
  (QA-001 — all 38 cases over the shared seeded dataset), the same fixtures
  the server and the ts/rust/python engines execute; the server is the source
  of truth for every expected value.
- Live-server integration tests ship in the suite (`LiveIntegrationTests`):
  `httpPushQueryMutateRoundTrip` (schema push, inserts, ordered scan, count
  terminal, blob upload/serve round trip, error envelope against a real
  server) and `wsSubscribeReceivesLiveUpdate` (WS subscription fed by a
  second writer's mutation). They follow the repo convention — opt-in via the
  `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` environment variables pointing
  at a running dev server (`make dev-db-up` + `cd server && cargo run`); unset,
  they are skipped cleanly.
- From the repo root: `make swift-client-checkall` (fmt-check + fmt + lint +
  typecheck + test). The root sweeps include the same lines behind a Darwin
  guard — on Linux they print `Skipping swift-client (non-Darwin host)` and
  the macOS CI job covers the suite.

## Coverage: v1 surfaces vs deferred

| Surface | Status |
| --- | --- |
| Wire types — fifth implementation of the contract | ✅ (+ `wire-corpus.json` parity runner, ARC-008) |
| Query DSL — every terminal incl. `search`/`vectorSearch`/`hybridSearch`/`paginate`/`aggregate`/`distinct` | ✅ |
| Mutation DSL — all 14 step ops, recursive step-cap enforcement | ✅ |
| Schema DSL — 15 field types, btree/search/vector/unique/partial indexes, `ownerField`/`collaboratorsField`/`authorize`, `ttl`, `defaults`, `softDelete`, `onDelete` | ✅ |
| HTTP client — query/query-batch/mutate (+ idempotency key, retry helper), schedule ops, workflow ops, full storage surface, `pushSchema`/`previewSchema`, `authMe` | ✅ |
| WS client — auth/reconnect/heartbeat, shared subscriptions with replay, mutate-over-WS, schedule + workflow ops | ✅ |
| Presence (ENH-015) — `presence(room:state:)` / `updatePresence(room:state:ttlMs:)` / `leavePresence(room:)`, `PresenceSnapshot` fan-out, reconnect replay of joined rooms | ✅ |
| Optimistic updates — `RtDbClientConfig.optimisticUpdates` (default off), overlay on mutate, reconcile on `queryUpdate`, rollback on `mutateErr`/reject/close | ✅ |
| `ParRtDbUI` — `@Observable LiveQuery<T>` | ✅ |
| Admin client — the full `/admin/*` control plane (db CRUD/clone/export/import, schema push/preview/get/history/restore, tokens, allowlist, admins, metrics, hot config, op feed, audit, sessions, merge-users, owner-bypass query/mutate, explain, slow queries, backups, webhooks + deliveries, admin workflow/schedule/file views) | ✅ |
| Migrate DSL — `Migration` builder for every directive kind (incl. the typed `ValueExpr` evalExpr path), `Directive` wire family pinned by the corpus | ✅ |
| In-memory engine — `InMemoryRtDbClient` mirrors the server's DSL/step-result/system-field semantics; fifth runner of `wire-corpus/semantics/` (53 cases) + `wire-corpus/golden-vector.json` (38 cases) | ✅ (2026-08-19) |
| OAuth login-flow helpers | Not in v1 (spec decision — the `getToken` hook is the integration seam) |

## License

MIT — see [../LICENSE](../LICENSE).
