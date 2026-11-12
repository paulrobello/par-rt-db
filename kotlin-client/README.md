# kotlin-client

Kotlin/JVM client for [par-rt-db](../README.md) — the seventh implementation
of the par-rt-db wire contract (after TypeScript, Rust, Python, Swift, and
Go). Speaks the server's declarative query/transaction DSL over one-shot
HTTP and a reactive WebSocket (`Flow`-based subscriptions and presence), with
an admin control-plane client and an in-memory test engine. No codegen: you
build schema values that serialize to the exact server `SchemaDef`.

JVM-first: Kotlin Multiplatform is deferred. The core layers (wire, DSL,
in-memory engine, admin DTOs) are pure Kotlin + kotlinx-serialization. The
network layer (`RtDbHttpClient`, `AdminClient`, `RtDbWsClient`) runs over a
pluggable transport (`dev.parrtdb.transport`): `JdkTransport` (the default,
`java.net.http`, JVM only) or `OkHttpTransport` (OkHttp 5.x).

**Android: supported at minSdk 26.** `java.net.http` does not exist on Android
at any API level, so pass an `OkHttpTransport` to every network client. The
floor is 26 because the cursor codec uses `java.util.Base64`. OkHttp is a
`compileOnly` dependency of this library: add `com.squareup.okhttp3:okhttp:5.5.1`
(or newer 5.x) to your app.

```kotlin
val transport = OkHttpTransport(okHttpClient) // your configured OkHttpClient
val db = RtDbHttpClient(baseUrl, "app", token, transport)
val admin = AdminClient(baseUrl, adminKey, transport)
val ws = RtDbWsClient(wsUrl, "app", { token }, WsOptions(), transport)
```

Custom transports implement `HttpTransport` (one buffered request/response)
and/or `WsTransport`/`WsSession`. The `java.net.http.HttpClient` constructor
overloads remain for JVM callers.

## Using the client

The library is not published to a registry yet (see
[`docs/RELEASING.md`](../docs/RELEASING.md)), so consume it from a checkout.
The project applies no publishing plugin — a Gradle composite build is the
supported path, substituting the coordinate `dev.parrtdb:par-rt-db-kotlin-client`
for the included build:

```kotlin
// settings.gradle.kts of your app
includeBuild("../par-rt-db/kotlin-client")

// build.gradle.kts of your app
dependencies {
    implementation("dev.parrtdb:par-rt-db-kotlin-client")
}
```

No extra repository declarations are needed; `mavenCentral()` covers the
library's own dependencies (kotlinx-serialization, OkHttp when you opt in).

## Surfaces

| Layer | Surface |
| --- | --- |
| `wire` | The wire vocabulary: strict-decoding JSON values, envelope types, `Query`/`FilterExpr`/`ValueExpr`, `Transaction`/`Step`, errors. Unknown fields rejected on decode, mirroring the server's `deny_unknown_fields`. |
| `dsl` | Builders: table queries, filter/value-expr constructors, `MutationBuilder`, schema DSL, cursor codec. |
| `errors` | The `ErrorCode` enum mirroring `server/src/error.rs`, `{code, message}` `RtDbError`. |
| `http` | One-shot HTTP client (queries, mutate, schedules, storage) over a pluggable transport. |
| `transport` | `HttpTransport`/`WsTransport` interfaces, `JdkTransport` (default) and `OkHttpTransport` (Android). |
| `ws` | Reactive WebSocket client over `/sync`: `Flow` live-query subscriptions, presence, mutate-over-WS. |
| `admin` | The `/admin/*` control plane: db lifecycle, schema push, tokens, admin query/mutate, workflows, schedules. |
| `inmemory` | The in-memory engine — server-identical semantics for tests (schema validation, queries, transactions, presence, subscriptions). |
| `corpus` | The wire-corpus runner harness: every semantics case, golden-vector case, and wire section, zero skips (see [`wire-corpus/README.md`](../wire-corpus/README.md) for current counts); the corpus test fails the build on any skip. |

## Examples

All snippets assume these imports (the DSL entry points live in
`dev.parrtdb.dsl`):

```kotlin
import dev.parrtdb.admin.AdminClient
import dev.parrtdb.dsl.*
import dev.parrtdb.http.RtDbHttpClient
import dev.parrtdb.inmemory.InMemoryClient
import dev.parrtdb.ws.RtDbWsClient
```

### Schema, query, mutate (HTTP)

```kotlin
val admin = AdminClient("http://localhost:8300", adminKey)
admin.createDb("myapp")
admin.pushSchema(
    "myapp",
    defineSchema {
        table("tasks") {
            field("title", FieldType.string())
            field("done", FieldType.boolean())
            field("owner", FieldType.optional(FieldType.string()))
            index("by_done", "done")
            index("by_owner_done", "owner", "done")
        }
    },
)
val token = admin.mintToken("myapp", "app").token

val db = RtDbHttpClient("http://localhost:8300", "myapp", token)

// Index-scoped read with a post-filter and a cap.
val openTasks = db.query(
    query("tasks")
        .withIndex("by_done", false)
        .orderAsc()
        .take(10)
        .filter(Filters.eq("owner", "paul"))
        .build(),
)

// Conditional insert: expectNoMatch + insert in one builder call.
val steps = db.mutate(
    mutation()
        .insertIf("bookings", Filters.eq("room", "oval"), obj("room" to "oval", "slot" to 14))
        .build(),
)   // steps[1] is the insert's StepResult, carrying the minted id
```

### Reactive WebSocket (`Flow`)

```kotlin
val ws = RtDbWsClient("ws://localhost:8300/sync", "myapp", { token }, WsOptions())
ws.connect()

val scope = CoroutineScope(Dispatchers.Default)
scope.launch {
    // Re-emits the full result set on every server push that changes it.
    ws.subscribe(query("tasks").withIndex("by_done", false).build())
        .collect { docs -> render(docs) }
}

ws.mutate(mutation().patch("tasks", taskId, obj("done" to true)).build())
ws.close()
```

### File storage

```kotlin
val up = db.upload("image/png", pngBytes)   // UploadResult(id, sha256, size, contentType)
val bytes = db.download(up.id)
val url = db.getSignedUrl(up.id, ttlSeconds = 300) // SignedUrl(url, expiresAt)
db.getFileMetadata(up.id)
db.deleteFile(up.id)
```

### Workflows

Data-plane client and admin twin both expose the workflow surface:

```kotlin
val spec = WorkflowSpec(
    name = "booking-cleanup",
    steps = listOf(
        WorkflowStepSpec(txn = mutation().deleteByQuery("bookings", Filters.olderThan("createdAt", 86_400_000)).build()),
        WorkflowStepSpec(awaitSignal = AwaitSignalSpec("operator-ok", timeoutMs = 60_000)),
    ),
)

val runId = db.startWorkflow(spec)                     // POST /api/workflows
val waiting = db.listWorkflows(status = WorkflowStatus.Waiting)
db.signalWorkflow(runId, "operator-ok")                // returns true when delivered

// The admin twin scopes the same surface per database:
val runs = admin.listWorkflows("myapp")
admin.cancelWorkflow("myapp", runId)
```

### Change feed

```kotlin
// Poll the durable log (data plane, machine token):
val page = db.changes(since = 0, table = "tasks")      // ChangeFeedResponse(ops, nextSeq, ...)

// Admin twin under the admin gate:
val history = admin.adminChanges(db = "myapp", since = 0, table = "tasks")

// Or receive it as pushed frames on an open /sync socket (resumable):
ws.watchChanges(tables = listOf("tasks")).collect { batch -> ingest(batch) }
```

### In-memory engine (tests)

`InMemoryClient` mirrors the HTTP/WS usage surface with server-identical
semantics — no server or database needed:

```kotlin
val mem = InMemoryClient()
mem.pushSchema(defineSchema { table("tasks") { field("title", FieldType.string()); index("by_title", "title") } })
mem.mutate(mutation().insert("tasks", obj("title" to "write tests")).build())
val rows = mem.query(query("tasks").build())
mem.subscribeFlow(query("tasks").build()).collect { docs -> assert(docs == rows) }
mem.tick(System.currentTimeMillis())   // deterministic scheduler/TTL/presence clock
```

## Build

Requires a JDK 17+ (the build targets JVM 17 bytecode; CI builds on Temurin
21). The Gradle wrapper (9.9.1, Kotlin 2.4.21) bootstraps itself:

```bash
./gradlew build          # compile + test, warnings are errors
./gradlew test           # corpus + unit tests only
```

From the repo root, `make kotlin-client-checkall` runs the same legs and is
wired into `make checkall`.

## Tests

`./gradlew test` executes the wire corpus against the in-memory engine —
the same fixtures the server and the five other clients run — plus HTTP/WS
client tests against a JDK `HttpServer` and a hand-rolled RFC 6455 fake
server, and negative controls proving the corpus runner fails on wrong
expectations. No live server or database is needed.
