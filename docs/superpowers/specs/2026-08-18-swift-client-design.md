# par-rt-db Swift Client Package — Design Spec

**Date:** 2026-08-18
**Status:** Proposed
**Repo:** `~/Repos/par-rt-db` (package lives in `swift-client/`)
**Kanban:** par-rt-db → "Add Swift client library (swift-client)" (high, `01a016ad676a772082917fd62ac29fb2`)
**End goal:** an Apple-platform client (`ParRtDbClient` + thin `ParRtDbUI`) with v1 parity of the
sibling clients' core surface — wire types, query/mutation/schema DSL, reactive WS client, HTTP
client, file storage, token auth — plus wire-corpus byte-parity from day one.

## Purpose

Swift is the only language in the active stack (TypeScript, Rust, Python all covered) without a
par-rt-db client. iOS apps are a first-class consumer type for a realtime document DB — live
queries map directly onto SwiftUI — and Convex (the reference system) has no official Swift
client, which differentiates the public repo. Go was considered and deliberately deferred (no Go
work in the ecosystem; every client is a permanent mirror obligation — filed as its own backlog
card).

This spec covers the client package only. Server/protocol changes are out of scope — the client
speaks the protocol as it exists today.

## Decisions (settled during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Platform floor | iOS 17+ / macOS 14+ | Earliest floor with the Observation framework; matches the actual device fleet. No Linux/server-side target. |
| Language mode | Swift 6, strict concurrency throughout | House style (`SWIFT_STRICT_CONCURRENCY: complete`); toolchain on this machine is Swift 6.3.3 / Xcode 26.6. |
| v1 scope | Core + transports (wire types, DSL, WS, HTTP, storage, token auth); admin client and in-memory engine deferred | iOS consumer value is the data plane. Admin is operator tooling the dashboard already provides; the in-memory engine is test infrastructure whose only product purpose is the semantics corpus. |
| Wire architecture | Static hand-written Codable wire types; `JSONValue` for user documents | Identical shape to the siblings (static types for protocol, dynamic for data). Codegen rejected on the repo's no-codegen philosophy; a JSONValue-first dynamic layer was rejected as stringly-typed and structurally divergent. |
| WS concurrency | `actor RtDbClient` behind a `WebSocketTransport` protocol | Natural fit for Swift 6 strict concurrency; the transport seam makes reconnect/heartbeat/resubscribe logic testable with an in-process fake, no server. |
| iOS-native extras | Thin `@Observable` layer in a separate `ParRtDbUI` product; **no** OAuth flow code in v1 | Sibling clients ship no OAuth helpers either; `getToken` async closure covers token refresh. UI layer uses the Observation framework only (no SwiftUI import). |
| Testing | Swift Testing framework; four tiers (unit / wire-corpus / mocked transport / env-gated live) | Mirrors the sibling test structure; `swift test` runs everything hermetic. |
| Gate & CI | Root Makefile targets Darwin-guarded; new `macos-latest` CI job runs the swift gate | Local `make checkall` on the Mac includes Swift; Linux `checkall` skips loudly instead of failing on a missing toolchain; Swift still gets a CI lane. |
| Naming | Package dir `swift-client/`; products/modules `ParRtDbClient` and `ParRtDbUI` | Follows the rust crate name (`par-rt-db-client`) in Swift casing. |

## Scope

**In (v1):**
- Wire types — fifth implementation of the contract after `server/src/protocol.rs`,
  `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`.
- `JSONValue` document type (the `serde_json::Value` equivalent).
- Query DSL (`TableQuery` builder + `parseResult`), mutation DSL (all 14 step ops → `Transaction`),
  schema DSL (builder + full `FieldType` set), errors (`RtDbError`/`ErrorCode`), cursor
  encode/decode.
- HTTP client: query/batch-query/mutate (+ retry helper), scheduler ops, workflow ops, `authMe`,
  full file-storage surface, `pushSchema`/`previewSchema` facade.
- WS reactive client: connect/auth/reconnect/heartbeat, subscriptions, mutate-over-WS,
  scheduler ops.
- `ParRtDbUI`: `@Observable LiveQuery<T>` wrapper.
- Wire-corpus (`wire-corpus.json`) parity tests.

**Out (v1) — each filed as an explicit gap card, not a silent omission:**
- In-memory engine → no semantics-corpus or golden-vector runner in v1 (phase 2; requires
  extending the `skip` union in all four existing runners + README).
- Admin client + migrate DSL (phase 2/3; dashboard covers ops today).
- Presence, optimistic updates (phase 2).
- OAuth login flow helpers (declined for v1; `getToken` hook suffices).

## Package layout & tooling

```
swift-client/
  Package.swift            swift-tools 6.x; platforms .iOS(.v17), .macOS(.v14)
  Sources/ParRtDbClient/   Wire.swift, JSONValue.swift, Query.swift, Mutation.swift,
                           Schema.swift, Errors.swift, Cursor.swift, HttpClient.swift,
                           WsClient.swift, Transport.swift
  Sources/ParRtDbUI/       LiveQuery.swift
  Tests/ParRtDbClientTests/
  Tests/ParRtDbUITests/
  README.md  Makefile  .swiftlint.yml  .swiftformat
```

- Two library products: `ParRtDbClient` (UI-free core) and `ParRtDbUI` (depends on core).
- `swiftlint --strict` + `swiftformat` (both installed on this machine); per-package `Makefile`
  with the standard targets (`build test lint fmt typecheck checkall`), following the
  python-client pattern.
- Zero third-party dependencies — Foundation + Observation only.

## Wire layer (fifth implementation)

**`JSONValue`** — `null | bool | int(Int64) | double | string | array | object`. Int64 and Double
stay distinct so int64-indexed fields survive round-trips (the server's int64-indexable support).
Documents flow through this; consumer models decode from it via `Decodable`.

**Protocol types** — every type from `protocol.rs`, hand-written Codable, each carrying a
`Mirrors server/src/protocol.rs::X` doc comment like the rust client. Discriminator and casing
rules per type (load-bearing — see the rust client's `wire.rs` conventions):

| Type | Tag | Casing |
|---|---|---|
| `ClientMessage` / `ServerMessage` | `"type"` (internal) | camelCase tags and fields; unknown fields **rejected** (extra keys close the WS in the server's hands) |
| `Step` (14 ops) | `"op"` (internal) | camelCase (`patchByQuery`, `startWorkflow`, …) |
| `FilterExpr` | `"op"` (internal) | lowercase tags |
| `ScheduleWhen` | `"type"` (internal) | camelCase tags (`afterMs`/`runAt`/`cron`) |
| `StepResult` | untagged | decoded by shape **in the load-bearing variant order** (upsert before insert — greediness is documented in `mutation.rs` of the siblings) |
| `UserKind`, `ScheduleKind`, `ScheduleStatus` | scalar | snake_case (`"user"`, `"oneshot"`, …) |
| `AggregateOp`, `SearchMode`, `OutcomeStatus` | scalar | lowercase |
| `ErrorCode` | scalar | SCREAMING_SNAKE |
| `Query` | plain struct | snake_case fields with the explicit camelCase outliers `vectorSearch`/`hybridSearch` via `CodingKeys` |

- **Omit-vs-null per field**: mirrors every `#[serde(skip_serializing_if = "Option::is_none")]`
  distinction in the server — e.g. `AuthedUser` writes `email`/`name` as explicit `null` but
  omits `githubLogin`/`githubId` when absent. Expressed via `encodeIfPresent` vs unconditional
  `encode` per field.
- **Unknown-field rejection**: a decoding helper validates `KeyedDecodingContainer.allKeys`
  against each type's allowed key set — the `deny_unknown_fields` equivalent, pinned by the
  corpus `rejects_*` sections.
- Tagged-enum decoding: decode the discriminator key first, then switch to a payload-specific
  nested decode. No compiler synthesis for tagged unions — conformances are hand-written, like
  serde's attribute stack is hand-declared.

**Wire-corpus parity test** — loads the shared `wire-corpus/wire-corpus.json` (located via
`#filePath`, CWD-independent), round-trips every v1-relevant section with **parsed-value
equality** (decode → encode → compare as parsed JSON values — the same semantic equality the
rust/python runners use; key order is not load-bearing): `client_messages`, `server_messages`,
`authed_users`, `schedule_whens`, `schedule_infos`, `queries`, plus must-reject assertions for
`rejects_client_message_unknown_field`, `rejects_schedule_when_unknown_field`,
`rejects_authed_user_unknown_kind`, `rejects_schedule_info_unknown_kind`,
`rejects_schedule_info_unknown_status`. Asserts `protocol_constants.max_steps` (1024) against
the client's `MutationBuilder.MAX_STEPS`. The migrate sections are admin-plane — deferred with
the admin client.

## DSL surface

**Query** — fluent builder producing the `Query` wire struct; terminal mutual exclusion enforced
at `build()` (same rules as siblings):

```swift
let q = TableQuery("users").withIndex("by_email").eq("x@y.z").order(.desc).take(10).collect()
let users: [User] = try client.run(q)   // User: consumer's own Codable struct
```

Terminals: `collect/unique/first/count/distinct/aggregate(_:groupBy:)/paginate(cursor:numItems:)`
plus `search(_:filter:mode:snippet:)`, `vectorSearch(_:limit:filter:)`,
`hybridSearch(query:vector:limit:searchIndex:vectorIndex:k:)` with options structs.
`parseResult(_:terminal:)` re-tags the untagged `QueryResult` into `T?`, `[T]`, `Int`,
`Paginated<T>`, or aggregate groups.

**Mutation** — `MutationBuilder()` with all 14 steps:
`insert/patch/replace/delete/undelete/expectVersion/expectAbsent/upsert/patchByQuery/
deleteByQuery/schedule/cancelSchedule/startWorkflow/cancelWorkflow` → `Transaction`.
Client-side `MAX_STEPS = 1024` cap (mirrors `server/src/txn.rs`; asserted against the corpus).

**Schema** — builder with the full field-type set (15 variants incl. `.vector`), indexes,
search/vector indexes, unique, `where` clause, `ownerField`/`collaboratorsField`/`authorize`,
defaults, soft-delete, TTL:

```swift
let schema = SchemaBuilder()
    .table("users") { t in
        t.field("email", .string).index("by_email")
        t.ownerField("owner")
    }
    .build()
try client.pushSchema(schema)
```

**Errors / cursor** — `RtDbError { code: ErrorCode, message: String }` mirroring `error.rs`
(codes and statuses identical); `retryOnPrecondition` helper for optimistic-concurrency retries;
cursor encode/decode, straight port.

## HTTP client

`RtDbHttpClient(url:db:token:)` over URLSession async/await; bearer token on every call;
trailing `/` trimmed (same construction semantics as the rust client). Surface:

- `run(_:)` / `run<T>(_:as:)`, `get`, `findOneByIndex`, `batchQuery`, `upsertByIndex`
- `mutate(_:idempotencyKey:)` → `[StepResult]`, `mutateWithRetry`
- `schedule` / `cancelSchedule` / `pauseSchedule` / `resumeSchedule` / `listSchedules`
- `startWorkflow` / `cancelWorkflow` / `listWorkflows`
- `authMe()`
- Storage: `upload(_:contentType:)` (streaming raw body), `deleteFile`, `getFileMetadata`,
  `getSignedUrl(_:ttlSeconds:)`, `getUrl`, `transformUrl(_:width:height:fit:quality:format:)`
- `pushSchema` / `previewSchema` (facade, like the rust HTTP client's embedded admin impl)

## WS client

`actor RtDbClient` — construction with `url`, `db`, `getToken: @Sendable () async -> String?`
(re-invoked on every (re)connect so credentials can refresh; `nil` pauses reconnects), and a
config: `backoffBase` (500 ms), `backoffMax` (15 s), `heartbeat` (20 s).

Behind the `WebSocketTransport` protocol (send/receive/close event stream). Production impl
wraps `URLSessionWebSocketTask`; tests use an in-process fake. Client logic — auth, heartbeat,
reconnect, resubscribe, mutation queueing — is transport-agnostic and fully testable hermetically.

Lifecycle semantics mirror the siblings exactly:

- `connect()` idempotent; `close()`; `status()` → state (idle/connecting/connected/reconnecting/
  closed) + user.
- Auth frame must land within **15 s** (`AUTH_DEADLINE`) or the connection is torn down and
  retried.
- Close code **4401 is terminal** — no reconnect.
- Liveness: `{type:"ping"}` every heartbeat interval; presumed dead after **2× heartbeat**
  without pong.
- Reconnect: jittered exponential backoff (base → max); a `generation` counter prevents
  duplicate sockets; on reconnect the driver replays all live subscriptions.
- Mutations issued while unauthenticated queue until `authOk`.

**Subscriptions** — `subscribe(_ query:) -> Subscription<T>`:

- Refcounted per canonical query shape; the last holder dropping sends unsubscribe.
- Snapshot states `.pending / .value(QueryResult) / error(RtDbError)`; cached latest value plus
  an `AsyncStream` for iteration; errors surface per-subscription.
- Generic `T: Decodable` re-tagging via `parseResult` for typed access.

Mutations over WS (`mutate(_:idempotencyKey:)`), scheduler and workflow ops ride the same
request/response machinery (`mutId` correlation).

## UI layer (`ParRtDbUI`)

One thin type — `@Observable final class LiveQuery<T: Codable & Sendable>`:

- Wraps a `Subscription<T>`; exposes `state: LiveState<T>` (`.pending / .value(T) /
  .failed(RtDbError)`).
- Starts on an explicit `start()` (or task-driven), cancels on deinit.
- Pure Observation framework — no SwiftUI import; serves SwiftUI views on iOS 17+/macOS 14+ and
  any other Observation consumer.

Deliberately nothing fancier — composition belongs to the app.

## Testing strategy

Swift Testing (`import Testing`; explicit `import Foundation` in test files):

1. **Unit** — wire-type fixtures copied verbatim from `protocol.rs` tests; builder → exact-JSON
   assertions for query/mutation/schema DSL (mirroring the siblings' builder tests);
   `JSONValue` round-trip and Int64/Double discrimination; omit-vs-null per-field assertions.
2. **Wire-corpus parity** — the shared-corpus test described above; drift here means the Swift
   wire types drifted from the contract.
3. **Mocked transport** — WS client against a fake `WebSocketTransport` (auth deadline, 4401
   terminality, heartbeat death, backoff, resubscribe, mutation queueing, refcounted
   unsubscribe); HTTP client against a `URLProtocol` stub.
4. **Live-server, env-gated** — `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` env vars, skipped
   by default (same convention as rust/python); runs against a local dev server during
   development. Tests create a unique `t<uuid>` db per run and never touch a db they didn't
   create.

## Makefile & CI wiring

Root `Makefile` (follows the python-client pattern):

- New targets: `swift-client-test`, `swift-client-lint`, `swift-client-fmt`, `swift-client-typecheck`
  (= `swift build`), `swift-client-checkall`; names added to `.PHONY`.
- The aggregate sweeps (`build`, `fmt`, `fmt-check`, `lint`, `typecheck`, `test`) gain
  `cd swift-client && …` lines, wrapped in an `ifeq ($(shell uname -s),Darwin)` guard with a loud
  ` Skipping swift-client (non-Darwin host)` echo in the else branch, so:
  - local `make checkall` on the Mac runs the full Swift suite (definition of done includes it),
  - Linux `make checkall` (the ubuntu CI job) skips Swift loudly rather than failing.

`ci.yml` gains a second job: `runs-on: macos-latest`, steps: checkout → `make swift-client-checkall`.
The existing ubuntu `checkall` job is untouched.

## Documentation & parity bookkeeping

- `swift-client/README.md` — install (SPM), construction, query/mutate/subscribe examples,
  storage, platform requirements.
- Root `README.md` — client list gains Swift.
- `CLAUDE.md` — workspace table row (`swift-client/`, swift tool); wire-contract paragraph
  four → **five** implementations.
- `FEATURE_MATRIX.md` — Swift client coverage column; v1 surfaces marked, deferred surfaces
  marked as gaps (not absent).
- `wire-corpus/README.md` — note Swift runs `wire-corpus.json` (ARC-008) but not the
  semantics/golden corpora yet. `CONTRIBUTING.md`'s "all four golden-vector suites" stays
  accurate until phase 2 adds the Swift runner.
- Gap cards filed on the board (backlog): in-memory engine + semantics/golden runner (+ skip-union
  extension in the four existing runners), admin client + migrate DSL, presence + optimistic
  updates.

## Build order (implementation plan formalizes)

- **A** Package scaffold + `JSONValue` + wire types + corpus parity test — the load-bearing core.
- **B** DSL builders (query/mutation/schema/errors/cursor) + unit tests.
- **C** HTTP client + `URLProtocol`-mocked tests.
- **D** WS client + fake-transport tests.
- **E** `ParRtDbUI` `LiveQuery` + tests.
- **F** Makefile/CI/docs wiring, full gate green (`make checkall` on the Mac), live smoke test
  against a dev server.

## Future phases

- **Phase 2** — in-memory engine (port of the TS engine, ~5–6K LOC) unlocking the
  semantics-corpus and golden-vector runners + the `skip`-union extension across the four
  existing runners; presence; optimistic updates.
- **Phase 3** — admin client + migrate DSL (+ their wire-corpus sections).
- Go client remains a separate backlog card, gated on external demand.
