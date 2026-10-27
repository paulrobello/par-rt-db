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
`java.net.http`, JVM only) or `OkHttpTransport` (OkHttp 4.x).

**Android: supported at minSdk 26.** `java.net.http` does not exist on Android
at any API level, so pass an `OkHttpTransport` to every network client. The
floor is 26 because the cursor codec uses `java.util.Base64`. OkHttp is a
`compileOnly` dependency of this library: add `com.squareup.okhttp3:okhttp:4.12.0`
(or newer 4.x) to your app.

```kotlin
val transport = OkHttpTransport(okHttpClient) // your configured OkHttpClient
val db = RtDbHttpClient(baseUrl, "app", token, transport)
val admin = AdminClient(baseUrl, adminKey, transport)
val ws = RtDbWsClient(wsUrl, "app", { token }, WsOptions(), transport)
```

Custom transports implement `HttpTransport` (one buffered request/response)
and/or `WsTransport`/`WsSession`. The `java.net.http.HttpClient` constructor
overloads remain for JVM callers.

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
| `corpus` | The wire-corpus runner harness: 266 semantics cases, 38 golden-vector cases, 15 wire sections, zero skips; the corpus test fails the build on any skip. |

## Build

Requires a JDK 21+. The Gradle wrapper (9.8.2, Kotlin 2.4.21) bootstraps
itself:

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
