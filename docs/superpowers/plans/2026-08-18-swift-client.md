# Swift Client (swift-client) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `swift-client/` — the fifth par-rt-db client (Swift 6, iOS 17+/macOS 14+): wire types, query/mutation/schema DSL, reactive WS client, HTTP client, file storage, and a thin `@Observable` layer, with wire-corpus parity from Task 7 onward.

**Architecture:** Static hand-written Codable wire types (the fifth implementation of `server/src/protocol.rs`), `JSONValue` for user documents, an `actor` WS client behind a `WebSocketTransport` protocol (fake in tests), URLSession HTTP client, and a separate `ParRtDbUI` product wrapping subscriptions in `@Observable`.

**Tech Stack:** Swift 6.3 / SPM, Foundation, Observation. Zero third-party dependencies. Swift Testing (`import Testing`), swiftlint `--strict` + swiftformat.

**Spec:** `docs/superpowers/specs/2026-08-18-swift-client-design.md` — read it first. The plan argues from the spec.

## Global Constraints

- **Mirror source of record:** `server/src/protocol.rs` is the contract; `rust-client/src/wire.rs` (+ `query.rs`, `mutation.rs`, `schema.rs`, `http.rs`, `ws.rs`, `error.rs`, `cursor.rs`) is the structural model for the port. When in doubt about a wire shape, read the server file — never guess.
- Every wire type carries a doc comment `/// Mirrors server/src/protocol.rs::<Type>` (or the owning rust file for non-protocol types).
- Swift 6 language mode, strict concurrency: all public types `Sendable`; no `@unchecked Sendable` unless a comment justifies it.
- Platforms: `.iOS(.v17)`, `.macOS(.v14)`. No `#if os` guards needed — no Linux target.
- No third-party dependencies. Foundation + Observation only.
- Coding conventions: 4-space indent (swiftformat default), `swiftlint --strict` clean (config may disable rules, with a comment why, prefer not to).
- Wire casing is load-bearing and deliberately non-uniform: per-type tag keys and casings are in the tables of Tasks 4–6 and MUST match exactly.
- Omit-vs-null: a field the server declares `skip_serializing_if = "Option::is_none"` on is encoded with `encodeIfPresent`; a plain `Option` field is encoded unconditionally (nil → JSON null). Never blanket-`encodeIfPresent` a struct.
- Commits: conventional style (`feat(swift-client): …`), one per task, verified test-green first. Run commits with a ≥600 s tool timeout — the repo pre-commit runs clippy and takes >2 min.
- Tests use Swift Testing: `import Testing` AND `import Foundation` explicitly in every test file. `swift test` from `swift-client/` is the test command.
- Do NOT modify `Makefile` aggregates, `ci.yml`, or docs outside `swift-client/` until Task 16.
- Never run `make dev-db-clean` or drop databases you didn't create.

## File Structure

```
swift-client/
  Package.swift
  Makefile
  .swiftlint.yml
  .swiftformat
  README.md                                  (Task 16)
  Sources/ParRtDbClient/
    JSONValue.swift        JSONValue enum + Codable            (Task 2)
    Wire.swift             CodingTools (rejectUnknownKeys), ClientMessage, ServerMessage,
                           AuthedUser, UserKind, wire enums     (Tasks 4–5)
    Query.swift            Query wire struct                    (Task 5)
    Mutation.swift         Step, StepResult, Transaction, MutationLimits (Task 6)
    Errors.swift           RtDbError, ErrorCode                 (Task 3)
    Cursor.swift           cursor encode/decode                 (Task 3)
    QueryDsl.swift         TableQuery builder, parseResult, Paginated (Task 8)
    MutationDsl.swift      MutationBuilder                      (Task 9)
    SchemaDsl.swift        FieldType, TableBuilder, SchemaBuilder (Task 10)
    HttpClient.swift       RtDbHttpClient                       (Task 11)
    Transport.swift        WebSocketTransport + URLSession impl (Task 12)
    WsClient.swift         actor RtDbClient, Subscription       (Tasks 13–14)
  Sources/ParRtDbUI/
    LiveQuery.swift        @Observable LiveQuery<T>            (Task 15)
  Tests/ParRtDbClientTests/
    JSONValueTests.swift  WireTests.swift  WireCorpusTests.swift  QueryTests.swift
    MutationTests.swift   SchemaTests.swift ErrorsCursorTests.swift
    HttpClientTests.swift WsClientTests.swift LiveIntegrationTests.swift
  Tests/ParRtDbUITests/
    LiveQueryTests.swift
```

Root-repo files touched (Task 16 only): `Makefile`, `.github/workflows/ci.yml`, `README.md`, `CLAUDE.md`, `FEATURE_MATRIX.md`, `wire-corpus/README.md`.

---

### Task 1: Package scaffold + tooling

**Files:**
- Create: `swift-client/Package.swift`, `swift-client/Makefile`, `swift-client/.swiftlint.yml`, `swift-client/.swiftformat`
- Create: `swift-client/Sources/ParRtDbClient/Placeholder.swift` (deleted in Task 2), `swift-client/Sources/ParRtDbUI/Placeholder.swift` (deleted in Task 15), `swift-client/Tests/ParRtDbClientTests/SmokeTests.swift`, `swift-client/Tests/ParRtDbUITests/SmokeTests.swift`

**Interfaces:**
- Produces: SPM package with two library targets `ParRtDbClient`, `ParRtDbUI` and two test targets, building under Swift 6 strict concurrency; `swift build` and `swift test` green.

- [ ] **Step 1: Write Package.swift**

```swift
// swift-tools-version:6.0
import PackageDescription

let package = Package(
    name: "swift-client",
    platforms: [.iOS(.v17), .macOS(.v14)],
    products: [
        .library(name: "ParRtDbClient", targets: ["ParRtDbClient"]),
        .library(name: "ParRtDbUI", targets: ["ParRtDbUI"]),
    ],
    targets: [
        .target(name: "ParRtDbClient"),
        .target(name: "ParRtDbUI", dependencies: ["ParRtDbClient"]),
        .testTarget(name: "ParRtDbClientTests", dependencies: ["ParRtDbClient"]),
        .testTarget(name: "ParRtDbUITests", dependencies: ["ParRtDbUI", "ParRtDbClient"]),
    ]
)
```

- [ ] **Step 2: Create placeholder sources + smoke tests**

`swift-client/Sources/ParRtDbClient/Placeholder.swift`:
```swift
/// Temporary — removed when real sources land (Task 2).
public enum PackagePlaceholder {}
```
`swift-client/Sources/ParRtDbUI/Placeholder.swift`: same content. SPM requires at least one source file per target.

`swift-client/Tests/ParRtDbClientTests/SmokeTests.swift`:
```swift
import Testing
import Foundation
@testable import ParRtDbClient

@Test func packageBuilds() {
    #expect(PackagePlaceholder.self != Void.self)
}
```
`swift-client/Tests/ParRtDbUITests/SmokeTests.swift`: identical but `@testable import ParRtDbUI`.

- [ ] **Step 3: Write tooling configs**

`swift-client/.swiftlint.yml` (keep permissive until code exists; tighten never — rules disabled here stay disabled):
```yaml
included:
  - Sources
  - Tests
line_length:
  warning: 120
identifier_name:
  min_length: 2
type_body_length: 400
file_length: 1000
```
`swift-client/.swiftformat` (repo has no Swift house style yet — this becomes it):
```swift
--swiftversion 6.0
--indent 4
```
`swift-client/Makefile` (python-client pattern):
```make
.PHONY: build test lint fmt typecheck checkall

build:
	swift build

test:
	swift test

lint:
	swiftlint --strict

fmt:
	swiftformat .

typecheck:
	swift build

checkall: fmt lint typecheck test
```

- [ ] **Step 4: Verify build + tests + lint**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift build 2>&1 | tail -3 && swift test 2>&1 | tail -5 && swiftlint --strict; echo "LINT_EXIT=$?"`
Expected: build succeeds, both smoke tests pass, `LINT_EXIT=0`.

- [ ] **Step 5: Commit**

```bash
git add swift-client/
git commit -m "feat(swift-client): package scaffold — SPM, Swift 6, swiftlint/swiftformat, smoke tests"
```

---

### Task 2: JSONValue

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/JSONValue.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/JSONValueTests.swift`
- Delete: `swift-client/Sources/ParRtDbClient/Placeholder.swift` (remove the `PackagePlaceholder` reference from SmokeTests.swift too)

**Interfaces:**
- Produces:

```swift
public enum JSONValue: Equatable, Hashable, Sendable, Codable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public var stringValue: String?            // non-nil only for .string
    public var objectValue: [String: JSONValue]?  // non-nil only for .object
    public static func from(any: Any) throws -> JSONValue   // JSONSerialization bridge
    public var anyValue: Any                   // JSONSerialization bridge back
}
```

- Documents (user data) flow through `JSONValue` everywhere; consumer models decode out of it (`init(jsonValue:)` below is how `parseResult` bridges).
- Also produces, in the same file:

```swift
extension KeyedDecodingContainer {
    /// serde `deny_unknown_fields` equivalent: throws if the payload carries a key
    /// not declared on K. Wire structs/enums call this first in init(from:).
    public func rejectUnknownKeys<K: CodingKey>(_ typeName: String) throws
        where K: CaseIterable, K.RawValue == String
}
```

- [ ] **Step 1: Write failing tests**

```swift
import Testing
import Foundation
@testable import ParRtDbClient

@Test func int64AndDoubleStayDistinct() throws {
    let roundTripped = try JSONDecoder().decode([JSONValue].self, from: Data(#"[1, 1.5, 9223372036854775807]"#.utf8))
    #expect(roundTripped[0] == .int(1))
    #expect(roundTripped[1] == .double(1.5))
    #expect(roundTripped[2] == .int(9_223_372_036_854_775_807))
    let reencoded = try JSONEncoder().encode(roundTripped)
    let back = try JSONDecoder().decode([JSONValue].self, from: reencoded)
    #expect(back == roundTripped)
}

@Test func objectRoundTripsThroughSerialization() throws {
    let original: [String: JSONValue] = ["a": .int(1), "b": .string("x"), "c": .null, "d": .array([.bool(true), .double(2.5)])]
    let value = JSONValue.object(original)
    let data = try JSONEncoder().encode(value)
    let parsed = try JSONDecoder().decode(JSONValue.self, from: data)
    #expect(parsed == value)
    #expect(parsed.objectValue?["a"] == .int(1))
}

@Test func unknownKeyHelperRejects() throws {
    struct Strict: Codable, Equatable {
        let a: Int
        enum CodingKeys: String, CodingKey, CaseIterable { case a }
        init(from decoder: Decoder) throws {
            let c = try decoder.container(keyedBy: CodingKeys.self)
            try c.rejectUnknownKeys("Strict")
            a = try c.decode(Int.self, forKey: .a)
        }
        func encode(to encoder: Encoder) throws {
            var c = encoder.container(keyedBy: CodingKeys.self)
            try c.encode(a, forKey: .a)
        }
    }
    #expect(throws: DecodingError.self) {
        _ = try JSONDecoder().decode(Strict.self, from: Data(#"{"a":1,"zzz":2}"#.utf8))
    }
    let ok = try JSONDecoder().decode(Strict.self, from: Data(#"{"a":1}"#.utf8))
    #expect(ok.a == 1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test --filter JSONValueTests 2>&1 | tail -5`
Expected: FAIL — `JSONValue` (and `rejectUnknownKeys`) not defined.

- [ ] **Step 3: Implement JSONValue.swift**

```swift
import Foundation

/// The document currency — the `serde_json::Value` equivalent. User documents are
/// schemaless jsonb server-side, so they flow through this enum; consumer models
/// decode out of it. `int`/`double` stay distinct so int64-indexed fields survive
/// round-trips (see int64-indexable support in docs/superpowers/specs/).
public enum JSONValue: Equatable, Hashable, Sendable, Codable {
    case null
    case bool(Bool)
    case int(Int64)
    case double(Double)
    case string(String)
    case array([JSONValue])
    case object([String: JSONValue])

    public var stringValue: String? {
        if case .string(let s) = self { return s }
        return nil
    }

    public var objectValue: [String: JSONValue]? {
        if case .object(let o) = self { return o }
        return nil
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if c.decodeNil() { self = .null }
        else if let b = try? c.decode(Bool.self) { self = .bool(b) }
        else if let i = try? c.decode(Int64.self) { self = .int(i) }       // Int64 before Double
        else if let d = try? c.decode(Double.self) { self = .double(d) }
        else if let s = try? c.decode(String.self) { self = .string(s) }
        else if let a = try? c.decode([JSONValue].self) { self = .array(a) }
        else if let o = try? c.decode([String: JSONValue].self) { self = .object(o) }
        else {
            throw DecodingError.dataCorruptedError(in: c, debugDescription: "unsupported JSON value")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch self {
        case .null: try c.encodeNil()
        case .bool(let b): try c.encode(b)
        case .int(let i): try c.encode(i)
        case .double(let d): try c.encode(d)
        case .string(let s): try c.encode(s)
        case .array(let a): try c.encode(a)
        case .object(let o): try c.encode(o)
        }
    }

    /// Bridge from `JSONSerialization.jsonObject(with:)` output.
    public static func from(any: Any) throws -> JSONValue {
        switch any {
        case is NSNull: return .null
        case let n as NSNumber:
            // CFNumberType discrimination: keep Int64 and Double apart.
            if CFNumberIsFloatType(n) { return .double(n.doubleValue) }
            return .int(n.int64Value)
        case let s as String: return .string(s)
        case let a as [Any]: return .array(try a.map(from(any:)))
        case let o as [String: Any]: return .object(try o.mapValues(from(any:)))
        default:
            throw CocoaError(.propertyListReadCorrupt, userInfo: [NSLocalizedDescriptionKey: "unserializable JSON value"])
        }
    }

    /// Bridge back to a `JSONSerialization`-compatible Any.
    public var anyValue: Any {
        switch self {
        case .null: return NSNull()
        case .bool(let b): return b
        case .int(let i): return i
        case .double(let d): return d
        case .string(let s): return s
        case .array(let a): return a.map(\.anyValue)
        case .object(let o): return o.mapValues(\.anyValue)
        }
    }
}

extension KeyedDecodingContainer {
    /// serde `deny_unknown_fields` equivalent — reject any payload key not declared on K.
    /// Wire types call this FIRST in `init(from:)`. Requires K: CaseIterable.
    public func rejectUnknownKeys<K: CodingKey>(_ typeName: String) throws
        where K: CaseIterable, K.RawValue == String {
        let allowed = Set(K.allCases.map(\.rawValue))
        for key in allKeys where !allowed.contains(key.rawValue) {
            throw DecodingError.dataCorruptedError(
                forKey: key, in: self,
                debugDescription: "\(typeName): unknown field '\(key.rawValue)'")
        }
    }
}
```

Delete `Placeholder.swift` and the `PackagePlaceholder` line from `SmokeTests.swift` (or delete SmokeTests.swift entirely — its purpose ended).

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test --filter JSONValueTests 2>&1 | tail -3 && swiftlint --strict; echo "LINT_EXIT=$?"`
Expected: all pass, `LINT_EXIT=0`.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): JSONValue document type + deny-unknown-fields decoding helper"
```

---

### Task 3: Errors + Cursor

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/Errors.swift`
- Create: `swift-client/Sources/ParRtDbClient/Cursor.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/ErrorsCursorTests.swift`

**Interfaces:**
- Consumes: `JSONValue` (Task 2).
- Produces:

```swift
public enum ErrorCode: String, Codable, Sendable, CaseIterable   // SCREAMING_SNAKE — exact set from server/src/error.rs
public struct RtDbError: Error, Equatable, Codable, Sendable {
    public var code: ErrorCode
    public var message: String
    public init(code: ErrorCode, message: String)
    /// The wire envelope `{code, message}` — decode from a server error body.
    public static func decodeEnvelope(from data: Data) -> RtDbError?  // nil when body isn't the envelope
}
/// Retry `body` while it throws PRECONDITION_FAILED, up to `attempts` times (rust error.rs `retry_on_precondition`).
public func retryOnPrecondition<T: Sendable>(attempts: Int = 8, _ body: () async throws -> T) async throws -> T
public func encodeCursor(_ values: [String: JSONValue]) -> String   // port of rust-client/src/cursor.rs
public func decodeCursor(_ s: String) -> [String: JSONValue]?
```

- [ ] **Step 1: Read the mirror sources**

Read `server/src/error.rs` (the full `ErrorCode` set and status mapping) and `rust-client/src/error.rs` + `rust-client/src/cursor.rs` (retry helper semantics, cursor encoding — base64 of canonical JSON). Port the FULL ErrorCode set — do not sample it.

- [ ] **Step 2: Write failing tests** (exemplars; add one assertion per ErrorCode case in a loop over `ErrorCode.allCases` — the rawValue IS the wire string)

```swift
import Testing
import Foundation
@testable import ParRtDbClient

@Test func errorEnvelopeRoundTrips() throws {
    let err = RtDbError(code: .preconditionFailed, message: "version mismatch")  // case names: lowerCamel of the SCREAMING_SNAKE rawValue
    let data = try JSONEncoder().encode(err)
    let text = String(decoding: data, as: UTF8.self)
    #expect(text.contains(#""PRECONDITION_FAILED""#))
    #expect(text.contains(#""message":"version mismatch""#))
    let decoded = try RtDbError.decodeEnvelope(from: data)
    #expect(decoded == err)
}

@Test func errorCodeCasingIsScreamingSnake() {
    for code in ErrorCode.allCases {
        #expect(code.rawValue == code.rawValue.uppercased())
    }
}

@Test func cursorRoundTrips() throws {
    let cursor: [String: JSONValue] = ["_id": .string("abc"), "n": .int(3)]
    let encoded = encodeCursor(cursor)
    #expect(decodeCursor(encoded) == cursor)
    #expect(decodeCursor("not-a-cursor!!") == nil)
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test --filter ErrorsCursorTests 2>&1 | tail -5`
Expected: FAIL — types not defined.

- [ ] **Step 4: Implement Errors.swift and Cursor.swift**

Errors.swift pattern (complete; fill `allCases` from `error.rs` — every code, exact rawValues):
```swift
import Foundation

/// Mirrors server/src/error.rs::ErrorCode — SCREAMING_SNAKE on the wire.
public enum ErrorCode: String, Codable, Sendable, CaseIterable {
    case badRequest = "BAD_REQUEST"
    case unauthorized = "UNAUTHORIZED"
    case preconditionFailed = "PRECONDITION_FAILED"
    // … FULL set from server/src/error.rs — do not sample.
}

/// Every failure is this envelope: `{code, message}` (server invariant).
public struct RtDbError: Error, Equatable, Codable, Sendable {
    public var code: ErrorCode
    public var message: String
    public init(code: ErrorCode, message: String) { self.code = code; self.message = message }

    private enum CodingKeys: String, CodingKey, CaseIterable { case code, message }
    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        try c.rejectUnknownKeys("RtDbError")
        code = try c.decode(ErrorCode.self, forKey: .code)
        message = try c.decode(String.self, forKey: .message)
    }

    public static func decodeEnvelope(from data: Data) -> RtDbError? {
        try? JSONDecoder().decode(RtDbError.self, from: data)
    }
}

/// Retry helper for optimistic-concurrency conflicts (rust error.rs retry_on_precondition).
public func retryOnPrecondition<T: Sendable>(attempts: Int = 8, _ body: () async throws -> T) async throws -> T {
    var last: Error?
    for _ in 0..<max(1, attempts) {
        do { return try await body() }
        catch let e as RtDbError where e.code == .preconditionFailed { last = e }
    }
    throw last ?? RtDbError(code: .preconditionFailed, message: "retryOnPrecondition: attempts exhausted")
}
```
Cursor.swift: port `rust-client/src/cursor.rs` exactly (canonical-key JSON → base64url; decode is failable). If the rust cursor uses sorted keys / no-whitespace JSON encoding, replicate that — the cursor must be byte-compatible across clients.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test 2>&1 | tail -3 && swiftlint --strict; echo "LINT_EXIT=$?"`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): RtDbError/ErrorCode envelope, precondition retry, cursor codec"
```

---

### Task 4: Wire types — messages

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/Wire.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/WireTests.swift`

**Interfaces:**
- Consumes: `JSONValue`, `rejectUnknownKeys` (Task 2).
- Produces (exact names later tasks and tests use):

```swift
public enum UserKind: String, Codable, Sendable       // "user" | "machine"  (snake_case wire)
public struct AuthedUser: Equatable, Codable, Sendable // kind, email, name, … full field set from protocol.rs
public enum ClientMessage: Equatable, Codable, Sendable
    // .auth(token:db:) .subscribe(queryId:query:) .unsubscribe(queryId:)
    // .mutate(mutId:idempotencyKey:txn:) .ping
public enum ServerMessage: Equatable, Codable, Sendable
    // .authOk(user:) .authErr(error:) .queryUpdate(queryId:result:)
    // .mutateOk(mutId:results:) .mutateErr(mutId:error:) .subscribeErr(queryId:error:) .pong
```

`query:` is `Query` (Task 5) — declare both message enums now with `Query` and `Transaction`/`StepResult` forward-declared as their Task 5/6 types; **build order fix: implement Task 5's `Query` struct and Task 6's `Step`/`StepResult`/`Transaction` wire types in this task as minimal-pass-through, OR (preferred) implement this task's message enums fully but add the message tests after Tasks 5–6.** Preferred sequencing inside the task: write `Wire.swift` message enums + `Query.swift` wire struct + `Mutation.swift` wire types together is too big — instead: **this task declares ClientMessage/ServerMessage payload types as their concrete Task 5/6 types and the build stays red until Task 6 lands. To keep every task green, this task instead defines the message enums against placeholder-free minimal `Query`/`Transaction` structs that Tasks 5–6 EXTEND (builder methods added, wire fields complete from the start).** Concretely: Task 4 creates `Query.swift` with the complete wire `Query` struct (all fields, Codable — no builder yet) and `Mutation.swift` with the complete wire `Step`, `StepResult`, `Transaction` (no builder yet); Tasks 5–6 add FilterExpr/ScheduleWhen/ScheduleInfo enums and the builder layers. Everything builds green at every task boundary.

- [ ] **Step 1: Read the mirror sources**

Read `server/src/protocol.rs` in full (the type inventory, serde attributes, omit-vs-null per field, and its inline test fixtures) and `rust-client/src/wire.rs:1-400` (message section). List every field of `AuthedUser` — the corpus `authed_users` section shows the full field set; match it exactly.

- [ ] **Step 2: Write failing tests** (fixtures copied from protocol.rs inline tests — read them and copy at least: auth round-trip, mutate with idempotencyKey present AND omitted, authOk with explicit-null email, unknown-field rejection, unknown-kind rejection)

```swift
import Testing
import Foundation
@testable import ParRtDbClient

private func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    try JSONDecoder().decode(T.self, from: JSONEncoder().encode(value))
}

@Test func authMessageRoundTrips() throws {
    let msg = ClientMessage.auth(token: "tok", db: "app")
    #expect(try roundTrip(msg) == msg)
    let text = String(decoding: JSONEncoder().encode(msg), as: UTF8.self)
    #expect(text == #"{"db":"app","token":"tok","type":"auth"}"#)   // exact bytes; key order is not asserted by the corpus but OUR encoder emits CodingKeys order — assert as written, adjust if encoder ordering differs
}

@Test func mutateOmitsIdempotencyKeyWhenNil() throws {
    let txn = Transaction(steps: [.insert(table: "t", doc: .object([:]))])
    let with = ClientMessage.mutate(mutId: "m1", idempotencyKey: "k", txn: txn)
    let without = ClientMessage.mutate(mutId: "m1", idempotencyKey: nil, txn: txn)
    let withText = String(decoding: JSONEncoder().encode(with), as: UTF8.self)
    let withoutText = String(decoding: JSONEncoder().encode(without), as: UTF8.self)
    #expect(withText.contains(#""idempotencyKey":"k""#))
    #expect(!withoutText.contains("idempotencyKey"))
}

@Test func clientMessageRejectsUnknownField() {
    #expect(throws: DecodingError.self) {
        _ = try JSONDecoder().decode(ClientMessage.self, from: Data(#"{"type":"ping","zzz":1}"#.utf8))
    }
}

@Test func authedUserEmailIsNullNeverOmitted_githubLoginOmittedWhenNil() throws {
    // Server: email/name are plain Option (null on the wire when absent);
    // githubLogin/githubId carry skip_serializing_if (omitted when absent).
    let json = Data(#"{"kind":"user","email":null,"name":null}"#.utf8)
    let user = try JSONDecoder().decode(AuthedUser.self, from: json)
    let out = String(decoding: JSONEncoder().encode(user), as: UTF8.self)
    #expect(out.contains(#""email":null"#))
    #expect(out.contains(#""name":null"#))
    #expect(!out.contains("githubLogin"))
    #expect(!out.contains("githubId"))
}

@Test func authedUserRejectsUnknownKind() {
    #expect(throws: DecodingError.self) {
        _ = try JSONDecoder().decode(AuthedUser.self, from: Data(#"{"kind":"robot"}"#.utf8))
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test --filter WireTests 2>&1 | tail -5`
Expected: FAIL — types not defined.

- [ ] **Step 4: Implement Wire.swift (messages) + Query.swift (wire struct) + Mutation.swift (wire types)**

Message enum pattern — `ClientMessage` complete (ServerMessage follows the identical pattern; implement every case):

```swift
import Foundation

/// Mirrors server/src/protocol.rs::ClientMessage — internally tagged on "type",
/// camelCase tags and fields, unknown fields rejected (server closes the WS on them).
public enum ClientMessage: Equatable, Codable, Sendable {
    case auth(token: String, db: String)
    case subscribe(queryId: String, query: Query)
    case unsubscribe(queryId: String)
    case mutate(mutId: String, idempotencyKey: String?, txn: Transaction)
    case ping

    private enum CodingKeys: String, CodingKey, CaseIterable {
        case type, token, db, queryId, query, mutId, idempotencyKey, txn
    }
    private static let tag = "type"

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        try c.rejectUnknownKeys("ClientMessage")
        switch try c.decode(String.self, forKey: .type) {
        case "auth":
            self = .auth(token: try c.decode(String.self, forKey: .token),
                         db: try c.decode(String.self, forKey: .db))
        case "subscribe":
            self = .subscribe(queryId: try c.decode(String.self, forKey: .queryId),
                              query: try c.decode(Query.self, forKey: .query))
        case "unsubscribe":
            self = .unsubscribe(queryId: try c.decode(String.self, forKey: .queryId))
        case "mutate":
            self = .mutate(mutId: try c.decode(String.self, forKey: .mutId),
                           idempotencyKey: try c.decodeIfPresent(String.self, forKey: .idempotencyKey),
                           txn: try c.decode(Transaction.self, forKey: .txn))
        case "ping":
            self = .ping
        case let unknown:
            throw DecodingError.dataCorruptedError(forKey: .type, in: c,
                debugDescription: "ClientMessage: unknown type '\(unknown)'")
        }
    }

    public func encode(to encoder: Encoder) throws {
        var c = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .auth(let token, let db):
            try c.encode("auth", forKey: .type)
            try c.encode(token, forKey: .token)
            try c.encode(db, forKey: .db)
        case .subscribe(let queryId, let query):
            try c.encode("subscribe", forKey: .type)
            try c.encode(queryId, forKey: .queryId)
            try c.encode(query, forKey: .query)
        case .unsubscribe(let queryId):
            try c.encode("unsubscribe", forKey: .type)
            try c.encode(queryId, forKey: .queryId)
        case .mutate(let mutId, let idempotencyKey, let txn):
            try c.encode("mutate", forKey: .type)
            try c.encode(mutId, forKey: .mutId)
            try c.encodeIfPresent(idempotencyKey, forKey: .idempotencyKey)   // omit when nil
            try c.encode(txn, forKey: .txn)
        case .ping:
            try c.encode("ping", forKey: .type)
        }
    }
}
```

`Query` wire struct: plain struct, **snake_case** keys, all fields optional except `table`, every optional encoded `encodeIfPresent`, decode with `rejectUnknownKeys`, plus explicit `CodingKeys` for `vectorSearch`/`hybridSearch`. Field inventory from `rust-client/src/query.rs` `Query` struct — port ALL of it (index, eq, gt/gte/lt/lte, order, take, unique, first, count, distinct, aggregate, search, vectorSearch, hybridSearch, paginate, filter, snippet, … exactly what rust has).

`Mutation.swift` wire types: `Step` enum — internally tagged `"op"`, camelCase tags, all 14 cases with exact payload fields from `rust-client/src/mutation.rs`; `StepResult` — untagged, decode by shape **in variant order (upsert before insert, then the rest in rust's declaration order)**:
```swift
/// Mirrors rust-client/src/mutation.rs::StepResult — untagged; order is load-bearing.
public enum StepResult: Equatable, Codable, Sendable {
    case upsert(id: String)          // {"id": …} — decode: object with ONLY "id"
    case insert(id: String)          // {"id": …} — …same shape: FIRST match wins, so upsert precedes insert
    case ok                          // literal true
    case count(n: Int)               // bare integer
    case deleted(ids: [String])      // {"deleted": […]}     — exact key set from mutation.rs
    case scheduleId(id: String)
    case workflowId(id: String)
    // …full variant list + exact shapes from rust mutation.rs StepResult

    public init(from decoder: Decoder) throws {
        // try each shape in the load-bearing order; first match wins.
        // singleValueContainer for true/int; keyed for objects.
        // Follow rust-client/src/mutation.rs StepResult serde variant ORDER exactly.
        …
    }
}
```
and `Transaction { public var steps: [Step] }` (tag `{"steps": […]}`, cap NOT enforced here — the builder enforces it in Task 9).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test 2>&1 | tail -3 && swiftlint --strict; echo "LINT_EXIT=$?"`
Expected: all green. If the exact-bytes assertion on encoder key order fails, drop the byte-order assertion but keep a parsed-equality assertion (`JSONSerialization` compare) — the corpus compares parsed values, not byte order.

- [ ] **Step 6: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): wire messages (ClientMessage/ServerMessage/AuthedUser) + Query/Step/StepResult/Transaction wire structs"
```

---

### Task 5: Wire types — filter, schedule, query enums

**Files:**
- Modify: `swift-client/Sources/ParRtDbClient/Wire.swift` (add `FilterExpr`, `ScheduleWhen`, `ScheduleInfo`, `ScheduleKind`, `ScheduleStatus`, `AggregateOp`, `SearchMode`, and any other scalar enums `protocol.rs` defines — the FULL inventory from the server file)
- Modify: `swift-client/Sources/ParRtDbClient/Query.swift` (nothing if Task 4's field inventory was complete; extend `FilterExpr`-typed `filter` etc.)
- Modify: `swift-client/Tests/ParRtDbClientTests/WireTests.swift`

**Interfaces:**
- Produces:

```swift
public enum FilterExpr: Equatable, Codable, Sendable
    // tagged "op", lowercase tags — full variant set from protocol.rs (eq, ne, in, lt, lte, gt, gte,
    // and, or, not, exists, … exactly what the server defines)
public enum ScheduleWhen: Equatable, Codable, Sendable
    // tagged "type", camelCase: .afterMs(ms: Int) .runAt(ms: Int) .cron(expr: String)
public struct ScheduleInfo: Equatable, Codable, Sendable
public enum ScheduleKind: String, Codable, Sendable { case oneshot, cron }
public enum ScheduleStatus: String, Codable, Sendable { case pending, running, paused, error }  // exact set from protocol.rs
public enum AggregateOp: String, Codable, Sendable   // lowercase — from protocol.rs
public enum SearchMode: String, Codable, Sendable    // lowercase — from protocol.rs
```

- [ ] **Step 1: Read the mirror sources** — `server/src/protocol.rs` filter/schedule sections and `rust-client/src/wire.rs` equivalents. Enumerate ALL `FilterExpr` variants and their payload shapes (in/nin carry arrays; and/or carry nested exprs; etc.).

- [ ] **Step 2: Write failing tests** — at minimum: one round-trip per FilterExpr variant kind (comparison, array, logical nesting), ScheduleWhen all three tags with exact tag bytes, ScheduleInfo round-trip + unknown-kind/status rejection:

```swift
@Test func scheduleWhenTags() throws {
    let cases: [(ScheduleWhen, String)] = [
        (.afterMs(ms: 500), #"{"type":"afterMs","ms":500}"#),
        (.runAt(ms: 1_770_000_000_000), #"{"type":"runAt","ms":1770000000000}"#),
        (.cron(expr: "0 * * * *"), #"{"type":"cron","expr":"0 * * * *"}"#),
    ]
    for (value, json) in cases {
        let text = String(decoding: JSONEncoder().encode(value), as: UTF8.self)
        #expect(try jsonDict(text) == jsonDict(json))   // local helper: JSONSerialization parse → [String: Any]
        #expect(try roundTrip(value) == value)
    }
}

@Test func filterExprNests() throws {
    let f = FilterExpr.or([.and([.eq(["users", "a"]), .gt(["n", .int(1)])]), .not(.exists(["email"]))])
    // exact payload shapes from protocol.rs — the array-carries-[field, value] convention above is a
    // GUESS placeholder pattern; replace with the server's real shapes read in Step 1.
    #expect(try roundTrip(f) == f)
}
```
NOTE: the `FilterExpr` test above intentionally documents that payload shapes MUST come from Step 1's reading — write the real shapes there, do not invent.

- [ ] **Step 3: Run tests to verify they fail** — `swift test --filter WireTests 2>&1 | tail -5`, FAIL expected.

- [ ] **Step 4: Implement** — same tagged-enum pattern as Task 4 (`FilterExpr` uses lowercase tags in its switch; `ScheduleWhen` camelCase tags `afterMs`/`runAt`/`cron`; scalar enums via `String` rawValue). Add `snake_case` rawValues for `ScheduleKind`/`ScheduleStatus` etc. matching the wire strings exactly (`"oneshot"`, `"pending"`…).

- [ ] **Step 5: Run tests to verify they pass** — full suite + lint green.

- [ ] **Step 6: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): FilterExpr, ScheduleWhen/ScheduleInfo, scalar wire enums"
```

---

### Task 6: MutationLimits + wire smoke completion

**Files:**
- Modify: `swift-client/Sources/ParRtDbClient/Mutation.swift` (add `MutationLimits`)
- Modify: `swift-client/Tests/ParRtDbClientTests/MutationTests.swift` (new file)

**Interfaces:**
- Produces: `public enum MutationLimits { public static let maxSteps = 1024 }` — mirrors `protocol_constants.max_steps`; the corpus test (Task 7) asserts equality, so this number is contract.

- [ ] **Step 1: Write failing test**

```swift
import Testing
import Foundation
@testable import ParRtDbClient

@Test func maxStepsMatchesRustAndServer() {
    #expect(MutationLimits.maxSteps == 1024)   // server/src/txn.rs cap; corpus protocol_constants pins it
}

@Test func stepOpTagsAreCamelCase() throws {
    let step = Step.patchByQuery(table: "t", filter: nil, patch: .object([:]), limit: nil)  // exact payload from mutation.rs
    let text = String(decoding: JSONEncoder().encode(step), as: UTF8.self)
    #expect(text.contains(#""op":"patchByQuery""#))
    #expect(try roundTrip(step) == step)
}

@Test func stepResultUntaggedOrder() throws {
    // {"id":"x"} decodes as .upsert (first in declaration order), never .insert —
    // mirror the greediness documented in rust mutation.rs.
    let r = try JSONDecoder().decode(StepResult.self, from: Data(#"{"id":"x"}"#.utf8))
    #expect(r == .upsert(id: "x"))
}
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter MutationTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement** `MutationLimits`; adjust `Step.patchByQuery` payload labels to whatever Task 4 actually defined (they must match `rust-client/src/mutation.rs` exactly).

- [ ] **Step 4: Run tests to verify they pass** — full suite + lint.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): MutationLimits contract const + step/step-result encoding tests"
```

---

### Task 7: Wire-corpus parity test

**Files:**
- Create: `swift-client/Tests/ParRtDbClientTests/WireCorpusTests.swift`

**Interfaces:**
- Consumes: all wire types (Tasks 2–6).
- Produces: the ARC-008 Swift runner — `swift test` now enforces byte-parity with the shared corpus.

- [ ] **Step 1: Write the test** (complete — this IS the deliverable; sections per spec: round-trip `client_messages`, `server_messages`, `authed_users`, `schedule_whens`, `schedule_infos`, `queries`; reject `rejects_client_message_unknown_field`, `rejects_schedule_when_unknown_field`, `rejects_authed_user_unknown_kind`, `rejects_schedule_info_unknown_kind`, `rejects_schedule_info_unknown_status`; assert `protocol_constants.max_steps`):

```swift
import Testing
import Foundation
@testable import ParRtDbClient

/// Cross-client wire-parity corpus (ARC-008) — swift-client view. The server,
/// ts-client, rust-client, and python-client run equivalent tests on the same
/// file; drift here means the Swift wire types drifted from the contract.
struct WireCorpus {
    let json: [String: Any]

    init() throws {
        // .../swift-client/Tests/ParRtDbClientTests/WireCorpusTests.swift → repo root
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()   // ParRtDbClientTests
            .deletingLastPathComponent()   // Tests
            .deletingLastPathComponent()   // swift-client
            .appendingPathComponent("wire-corpus/wire-corpus.json")
        self.json = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
    }

    func section(_ name: String) -> [[String: Any]] {
        json[name] as! [[String: Any]]
    }
}

func corpusRoundTrip<T: Codable>(_ type: T.Type, _ section: String, _ corpus: WireCorpus) throws {
    for (i, raw) in corpus.section(section).enumerated() {
        let input = try JSONSerialization.data(withJSONObject: raw)
        let parsed = try JSONDecoder().decode(T.self, from: input)
        let dumped = try JSONEncoder().encode(parsed)
        let inputObj = try JSONSerialization.jsonObject(with: input)
        let dumpedObj = try JSONSerialization.jsonObject(with: dumped)
        #expect(dumpedObj as NSDictionary == inputObj as NSDictionary,
                 "\(section) #\(i): drift — dumped: \(String(decoding: dumped, as: UTF8.self)) input: \(String(decoding: input, as: UTF8.self))")
    }
}

func corpusRejects<T: Codable>(_ type: T.Type, _ section: String, _ corpus: WireCorpus) throws {
    for (i, raw) in corpus.section(section).enumerated() {
        let input = try JSONSerialization.data(withJSONObject: raw)
        #expect(throws: DecodingError.self, "\(section) #\(i): expected rejection") {
            _ = try JSONDecoder().decode(T.self, from: input)
        }
    }
}

@Test func corpusClientMessages() throws { try corpusRoundTrip(ClientMessage.self, "client_messages", WireCorpus()) }
@Test func corpusServerMessages() throws { try corpusRoundTrip(ServerMessage.self, "server_messages", WireCorpus()) }
@Test func corpusAuthedUsers() throws { try corpusRoundTrip(AuthedUser.self, "authed_users", WireCorpus()) }
@Test func corpusScheduleWhens() throws { try corpusRoundTrip(ScheduleWhen.self, "schedule_whens", WireCorpus()) }
@Test func corpusScheduleInfos() throws { try corpusRoundTrip(ScheduleInfo.self, "schedule_infos", WireCorpus()) }
@Test func corpusQueries() throws { try corpusRoundTrip(Query.self, "queries", WireCorpus()) }
@Test func corpusRejectsUnknownClientMessageField() throws { try corpusRejects(ClientMessage.self, "rejects_client_message_unknown_field", WireCorpus()) }
@Test func corpusRejectsUnknownScheduleWhenField() throws { try corpusRejects(ScheduleWhen.self, "rejects_schedule_when_unknown_field", WireCorpus()) }
@Test func corpusRejectsUnknownUserKind() throws { try corpusRejects(AuthedUser.self, "rejects_authed_user_unknown_kind", WireCorpus()) }
@Test func corpusRejectsUnknownScheduleKind() throws { try corpusRejects(ScheduleInfo.self, "rejects_schedule_info_unknown_kind", WireCorpus()) }
@Test func corpusRejectsUnknownScheduleStatus() throws { try corpusRejects(ScheduleInfo.self, "rejects_schedule_info_unknown_status", WireCorpus()) }
@Test func corpusMaxSteps() throws {
    let consts = WireCorpus().json["protocol_constants"] as! [String: Any]
    #expect((consts["max_steps"] as! Int) == MutationLimits.maxSteps)
}
```

- [ ] **Step 2: Run and fix drift until green**

Run: `cd /Users/probello/Repos/par-rt-db/swift-client && swift test --filter WireCorpusTests 2>&1 | tail -20`
Expected: initial run MAY reveal wire-shape drift (a field the Swift type got wrong in Tasks 4–5). Every failure is a bug in the Swift types — fix the TYPE, never the test. Iterate until green.

- [ ] **Step 3: Run full suite + lint** — green.

- [ ] **Step 4: Commit**

```bash
git add -A swift-client/
git commit -m "test(swift-client): ARC-008 wire-corpus parity runner — shared wire-corpus.json round-trips + rejects"
```

---

### Task 8: Query DSL

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/QueryDsl.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/QueryTests.swift`

**Interfaces:**
- Consumes: `Query` wire struct (Task 4), `FilterExpr` (Task 5), `JSONValue` (Task 2).
- Produces (HTTP/WS clients and LiveQuery consume these):

```swift
public struct TableQuery: Sendable {
    public init(_ table: String)
    public func get(_ id: String) -> TableQuery
    public func withIndex(_ name: String) -> TableQuery
    public func eq(_ values: JSONValue...) -> TableQuery
    public func gt(_ v: JSONValue) -> TableQuery; public func gte/lte/lt … -> TableQuery
    public func order(_ o: Order) -> TableQuery        // public enum Order: String, Sendable { case asc, desc }
    public func take(_ n: Int) -> TableQuery
    public func filter(_ f: FilterExpr) -> TableQuery
    public func collect() -> TableQuery; public func unique() -> TableQuery
    public func first() -> TableQuery; public func count() -> TableQuery
    public func distinct() -> TableQuery
    public func aggregate(_ op: AggregateOp, groupBy: [String]? = nil) -> TableQuery
    public func paginate(cursor: String? = nil, numItems: Int) -> TableQuery
    public func search(_ index: String, _ query: String, filter: FilterExpr? = nil,
                       mode: SearchMode? = nil, snippet: Bool? = nil) -> TableQuery
    public func vectorSearch(_ index: String, _ vector: [Double], limit: Int? = nil,
                             filter: FilterExpr? = nil) -> TableQuery
    public func hybridSearch(_ query: String, _ vector: [Double], limit: Int? = nil,
                             searchIndex: String? = nil, vectorIndex: String? = nil, k: Int? = nil) -> TableQuery
    public func build() throws -> Query                 // enforces terminal mutual exclusion
}
public struct Paginated<T: Codable & Sendable>: Codable, Sendable, Equatable {
    public var items: [T]; public var nextCursor: String?   // exact wire keys from protocol.rs paginate result
}
public func parseResult<T: Codable & Sendable>(_ value: JSONValue, terminal: QueryTerminal) throws -> T
// QueryTerminal: enum mirroring the terminal set (collect/first/count/distinct/aggregate/paginate/…) —
// parseResult re-tags the untagged QueryResult payload: object/null → T?, array → [T], number → Int,
// {items, nextCursor} → Paginated<T>, aggregate groups → [AggregateGroup]. Port rust query.rs parse_result.
```

- [ ] **Step 1: Read the mirror source** — `rust-client/src/query.rs` (`TableQuery` builder + `parse_result` + terminal exclusivity rules) and the server's paginate result shape in `protocol.rs`.

- [ ] **Step 2: Write failing tests** — builder→exact-JSON assertions (mirror `rust-client` builder tests), at least:

```swift
@Test func indexEqOrderTakeCollectBuildsExactShape() throws {
    let q = try TableQuery("users").withIndex("by_email").eq(.string("a@b.c")).order(.desc).take(10).collect().build()
    let obj = q.wireObject()   // helper on Query: JSONValue.object of the wire encoding — add in this task
    #expect(obj["table"] == .string("users"))
    #expect(obj["index"] == .string("by_email"))
    #expect(obj["eq"] == .array([.string("a@b.c")]))
    #expect(obj["order"] == .string("desc"))
    #expect(obj["take"] == .int(10))
    #expect(obj["collect"] != nil)   // terminal key present — exact key name from protocol.rs
}

@Test func terminalsAreMutuallyExclusive() {
    #expect(throws: RtDbError.self) {
        _ = try TableQuery("t").first().count().build()
    }
}

@Test func parseResultDecodesArray() throws {
    let docs: JSONValue = .array([.object(["_id": .string("a")]), .object(["_id": .string("b")])])
    struct Doc: Codable, Equatable, Sendable { var _id: String }
    let parsed: [Doc] = try parseResult(docs, terminal: .collect)
    #expect(parsed.map(\._id) == ["a", "b"])
}

@Test func parseResultDecodesPaginated() throws {
    let payload: JSONValue = .object(["items": .array([.object(["_id": .string("a")])]), "nextCursor": .string("c1")])
    struct Doc: Codable, Equatable, Sendable { var _id: String }
    let page: Paginated<Doc> = try parseResult(payload, terminal: .paginate)
    #expect(page.items.count == 1)
    #expect(page.nextCursor == "c1")
}
```

- [ ] **Step 3: Run to verify fail** — `swift test --filter QueryTests 2>&1 | tail -5`.

- [ ] **Step 4: Implement QueryDsl.swift** — builder mutating a private field accumulator, `build()` validating terminal exclusivity exactly as rust's `build()` does (read it; the rules are: at most one terminal; `get` conflicts with index/range terminals; paginate not combinable with take; etc. — port the exact rule set), then constructing the wire `Query`. `parseResult` ports rust `parse_result`'s dispatch table.

- [ ] **Step 5: Run full suite + lint** — green.

- [ ] **Step 6: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): TableQuery DSL, terminal exclusivity, parseResult, Paginated"
```

---

### Task 9: Mutation DSL

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/MutationDsl.swift`
- Modify: `swift-client/Tests/ParRtDbClientTests/MutationTests.swift`

**Interfaces:**
- Consumes: `Step`, `Transaction`, `MutationLimits` (Tasks 4, 6).
- Produces:

```swift
public final class MutationBuilder: Sendable {                 // class for fluent chaining; internally a value stack
    public init()
    public func insert(_ table: String, _ doc: JSONValue) -> MutationBuilder
    public func patch(_ table: String, _ id: String, _ fields: JSONValue) -> MutationBuilder
    public func replace(_ table: String, _ id: String, _ doc: JSONValue) -> MutationBuilder
    public func delete(_ table: String, _ id: String) -> MutationBuilder
    public func undelete(_ table: String, _ id: String) -> MutationBuilder
    public func expectVersion(_ table: String, _ id: String, _ version: Int) -> MutationBuilder
    public func expectAbsent(_ table: String, _ index: String, _ eq: [JSONValue]) -> MutationBuilder
    public func upsert(_ table: String, index: String, eq: [JSONValue],
                       insert: JSONValue, patch: JSONValue) -> MutationBuilder
    public func patchByQuery(_ table: String, filter: FilterExpr, patch: JSONValue, limit: Int? = nil) -> MutationBuilder
    public func deleteByQuery(_ table: String, filter: FilterExpr, limit: Int? = nil) -> MutationBuilder
    public func schedule(_ when: ScheduleWhen, _ txn: Transaction) -> MutationBuilder
    public func cancelSchedule(_ id: String) -> MutationBuilder
    public func startWorkflow(_ spec: WorkflowSpec) -> MutationBuilder
    public func cancelWorkflow(_ id: String) -> MutationBuilder
    public func build() throws -> Transaction                   // throws when steps.count > MutationLimits.maxSteps
}
```
`WorkflowSpec` — port the wire struct from `rust-client/src/wire.rs` (steps/retry shape; if it lives in mutation.rs there, mirror from there).

- [ ] **Step 1: Write failing tests** — exact-JSON for a 3-step chained builder; the `maxSteps` overflow rejection; schedule-with-nested-txn shape:

```swift
@Test func builderChainsAndEncodes() throws {
    let txn = try MutationBuilder()
        .insert("users", .object(["email": .string("a@b.c")]))
        .patch("counters", "c1", .object(["n": .int(1)]))
        .delete("sessions", "s9")
        .build()
    #expect(txn.steps.count == 3)
    let obj = try txn.wireObject()
    #expect(obj["steps"] == .array([
        .object(["op": .string("insert"), "table": .string("users"), "doc": .object(["email": .string("a@b.c")])]),
        .object(["op": .string("patch"), "table": .string("counters"), "id": .string("c1"), "doc": .object(["n": .int(1)])]),
        .object(["op": .string("delete"), "table": .string("sessions"), "id": .string("s9")]),
    ]))   // exact per-op keys from mutation.rs — verify each against the rust builder tests
}

@Test func buildRejectsOverMaxSteps() {
    let b = MutationBuilder()
    for _ in 0...(MutationLimits.maxSteps + 1) { _ = b.insert("t", .object([:])) }
    #expect(throws: RtDbError.self) { _ = try b.build() }
}
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter MutationTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement MutationDsl.swift.** Add `wireObject()` on `Transaction` (`JSONValue.object` of its encoding — same helper pattern as Task 8's on `Query`; put the shared `wireObject()` in `JSONValue.swift` as a protocol extension: `protocol WireEncodable: Codable { func wireObject() throws -> JSONValue }`).

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): MutationBuilder DSL — 14 step ops, MAX_STEPS cap"
```

---

### Task 10: Schema DSL

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/SchemaDsl.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/SchemaTests.swift`

**Interfaces:**
- Consumes: `JSONValue`, `WireEncodable` (Tasks 2, 9).
- Produces:

```swift
public enum FieldType: Equatable, Codable, Sendable {   // 15 variants — exact set + payload shapes from rust-client/src/schema.rs
    case string, number, boolean, null, id(table: String, onDelete: OnDelete?), literal, optional,
         union, array, `object`, int64, bytes, any, record, vector
    // OnDelete: cascade | restrict | setNull (exact wire strings from schema.rs)
}
public final class TableBuilder: Sendable {
    public func field(_ name: String, _ type: FieldType) -> TableBuilder
    public func index(_ name: String, on fields: [String]) -> TableBuilder          // exact param shape from schema.rs
    public func unique(_ name: String, on fields: [String]) -> TableBuilder
    public func searchIndex(_ name: String, on fields: [String]) -> TableBuilder
    public func vectorIndex(_ name: String, on field: String, dimensions: Int) -> TableBuilder
    public func ownerField(_ name: String) -> TableBuilder
    public func collaboratorsField(_ name: String) -> TableBuilder
    public func authorize(_ predicate: JSONValue) -> TableBuilder   // predicate DSL shape from the per-row-auth spec section of schema.rs
    public func defaults(_ d: [String: JSONValue]) -> TableBuilder
    public func softDelete(_ enabled: Bool = true) -> TableBuilder
    public func ttl(_ seconds: Int) -> TableBuilder
    public func `where`(_ clause: String) -> TableBuilder
}
public final class SchemaBuilder: Sendable {
    public func table(_ name: String, _ build: (TableBuilder) -> Void) -> SchemaBuilder
    public func build() -> SchemaDef
}
public struct SchemaDef: Equatable, Codable, Sendable { /* tables — wire shape from rust schema.rs */ }
```

- [ ] **Step 1: Read the mirror source** — `rust-client/src/schema.rs` in full. FieldType's 15 variants have payload shapes there (id carries table+onDelete; vector carries dimensions; etc.) — port exactly. `python-client/src/par_rt_db/schema.py` is the second reference.

- [ ] **Step 2: Write failing tests** — one exact-wire-object assertion for a schema with: string field + index, id field with onDelete, vector index, ownerField, softDelete, ttl; plus a round-trip of the built `SchemaDef`:

```swift
@Test func schemaBuildsExactWireShape() throws {
    let schema = SchemaBuilder()
        .table("users") { t in
            t.field("email", .string).index("by_email", on: ["email"])
            t.field("org", .id(table: "orgs", onDelete: .cascade))
            t.field("embedding", .vector).vectorIndex("by_embedding", on: "embedding", dimensions: 1536)
            t.ownerField("owner")
            t.softDelete()
        }
        .build()
    let obj = try schema.wireObject()
    // assert exact nested shape against rust schema.rs's builder test fixtures —
    // copy the expected JSON from rust-client's schema tests and compare parsed-equal.
}
```

- [ ] **Step 3: Run to verify fail** — `swift test --filter SchemaTests 2>&1 | tail -5`.

- [ ] **Step 4: Implement SchemaDsl.swift.**

- [ ] **Step 5: Run full suite + lint** — green.

- [ ] **Step 6: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): schema DSL — FieldType set, table/schema builders"
```

---

### Task 11: HTTP client

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/HttpClient.swift`
- Create: `swift-client/Tests/ParRtDbClientTests/HttpClientTests.swift`

**Interfaces:**
- Consumes: `Query`/`TableQuery`, `Transaction`, `StepResult`, `SchemaDef`, `RtDbError`, `encodeCursor`, `JSONValue` (Tasks 2–10).
- Produces:

```swift
public actor RtDbHttpClient: Sendable {
    public init(url: String, db: String, token: String, session: URLSession = .shared)
    // Data plane
    public func run(_ query: Query) async throws -> JSONValue                       // POST /api/query {db, query} → result
    public func run<T: Codable & Sendable>(_ query: Query, as type: T.Type) async throws -> T
    public func get(_ table: String, _ id: String) async throws -> JSONValue?
    public func findOneByIndex(_ table: String, _ index: String, _ value: JSONValue) async throws -> JSONValue?
    public func batchQuery(_ queries: [Query]) async throws -> [JSONValue]          // POST /api/query-batch
    public func mutate(_ txn: Transaction, idempotencyKey: String? = nil) async throws -> [StepResult]  // POST /api/mutate
    public func upsertByIndex(_ table: String, index: String, eq: [JSONValue],
                              insert: JSONValue, patch: JSONValue) async throws -> String
    public func mutateWithRetry(_ txn: Transaction) async throws -> [StepResult]    // retryOnPrecondition wrapper
    // Scheduler / workflows
    public func schedule(_ txn: Transaction, when: ScheduleWhen) async throws -> String   // returns schedule id
    public func cancelSchedule(_ id: String) async throws
    public func pauseSchedule(_ id: String) async throws
    public func resumeSchedule(_ id: String) async throws
    public func listSchedules() async throws -> [ScheduleInfo]
    public func startWorkflow(_ spec: WorkflowSpec) async throws -> String
    public func cancelWorkflow(_ id: String) async throws
    public func listWorkflows() async throws -> [WorkflowInfo]     // port wire shape from rust http.rs
    // Auth + schema facade
    public func authMe() async throws -> AuthedUser                // GET /api/auth/me
    public func pushSchema(_ schema: SchemaDef) async throws       // admin route w/ same token (facade)
    public func previewSchema(_ schema: SchemaDef) async throws -> JSONValue
    // Storage
    public func upload(_ data: Data, contentType: String) async throws -> String   // raw body; returns file id
    public func deleteFile(_ id: String) async throws
    public func getFileMetadata(_ id: String) async throws -> JSONValue            // port FileMetadata from rust http.rs
    public func getSignedUrl(_ id: String, ttlSeconds: Int) async throws -> String
    public func getUrl(_ id: String) -> String                                     // pure local: {url}/storage/{id}
    public func transformUrl(_ id: String, width: Int? = nil, height: Int? = nil,
                             fit: Fit? = nil, quality: Int? = nil, format: OutFormat? = nil) -> String
    // public enum Fit / OutFormat — port from rust http.rs TransformOpts
}
```
Route/method/body shapes: read `rust-client/src/http.rs` for EVERY route — path, verb, JSON body keys, response envelope unwrapping — and mirror. Error handling: any non-2xx decodes via `RtDbError.decodeEnvelope`; if the body isn't the envelope, throw `RtDbError(code: .badRequest, message: "HTTP <status>")` — never leak raw body text into errors.

- [ ] **Step 1: Write failing tests with a URLProtocol stub**

```swift
import Testing
import Foundation
@testable import ParRtDbClient

final class StubProtocol: URLProtocol {
    nonisolated(unsafe) static var handler: ((URLRequest) throws -> (Int, Data))?
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
    override func startLoading() {
        guard let handler = Self.handler else { fatalError("no stub handler installed") }
        do {
            let (status, body) = try handler(request)
            let response = HTTPURLResponse(url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: body)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }
    override func stopLoading() {}
}

func makeClient() -> RtDbHttpClient {
    let cfg = URLSessionConfiguration.ephemeral
    cfg.protocolClasses = [StubProtocol.self]
    return RtDbHttpClient(url: "http://rtdb.test/", db: "app", token: "tok",
                          session: URLSession(configuration: cfg))
}

@Test func runPostsQueryEnvelope() async throws {
    StubProtocol.handler = { req in
        #expect(req.httpMethod == "POST")
        #expect(req.url!.path == "/api/query")
        #expect(req.value(forHTTPHeaderField: "Authorization") == "Bearer tok")
        let body = try JSONSerialization.jsonObject(with: req.httpBody ?? Data()) as! [String: Any]
        #expect(body["db"] as! String == "app")
        return (200, Data(#"{"result":[{"_id":"a"}]}"#.utf8))
    }
    let docs: [JSONValue] = try await makeClient().run(try TableQuery("users").collect().build())
    #expect(docs.count == 1)
}

@Test func errorBodyBecomesRtDbError() async throws {
    StubProtocol.handler = { _ in (409, Data(#"{"code":"PRECONDITION_FAILED","message":"stale"}"#.utf8)) }
    do { _ = try await makeClient().mutate(Transaction(steps: [])); Issue.record("should throw") }
    catch let e as RtDbError { #expect(e.code == .preconditionFailed) }
}

@Test func uploadSendsRawBodyAndContentType() async throws {
    StubProtocol.handler = { req in
        #expect(req.value(forHTTPHeaderField: "Content-Type") == "image/png")
        #expect(req.httpBody?.count == 4)
        return (200, Data(#"{"id":"f1"}"#.utf8))
    }
    let id = try await makeClient().upload(Data([1, 2, 3, 4]), contentType: "image/png")
    #expect(id == "f1")
}
```
Add equivalent tests for `get`, `findOneByIndex`, `batchQuery`, `schedule`, `authMe`, `pushSchema` (route + body shape each — read rust http.rs for the exact paths), and `getUrl`/`transformUrl` (pure string building, no stub).

- [ ] **Step 2: Run to verify fail** — `swift test --filter HttpClientTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement HttpClient.swift** — one private `request(path:method:body:)` helper decoding envelopes; public methods per the interface block. `WorkflowInfo` ported from rust http.rs.

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): RtDbHttpClient — query/mutate/scheduler/workflows/auth/schema/storage"
```

---

### Task 12: WebSocket transport + fake

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/Transport.swift`
- Modify: `swift-client/Tests/ParRtDbClientTests/WsClientTests.swift` (new file)

**Interfaces:**
- Consumes: nothing new.
- Produces:

```swift
/// Transport seam — the WS client's entire I/O. Production: URLSessionWebSocketTask.
/// Tests: an in-process fake (see WsClientTests).
public protocol WebSocketTransport: Sendable {
    /// Errors carry a close code when the peer closed: `TransportCloseError`.
    func connect(to url: URL) async throws
    func send(_ text: String) async throws
    func receive() async throws -> String
    func close(code: UInt16) async
}
public struct TransportCloseError: Error, Sendable { public let code: UInt16? }   // 4401 etc.

public struct URLSessionWebSocketTransport: WebSocketTransport {
    public init(session: URLSession = .shared)
    // wraps URLSessionWebSocketTask; receive() maps .closed events to TransportCloseError
    // with the close code; send/receive are strings (the protocol is text JSON).
}

/// Injectable clock for deterministic backoff/heartbeat tests (python client's now/sleep injection).
public protocol WScheduler: Sendable {
    func now() -> UInt64                                    // ms since epoch
    func sleep(_ ms: UInt64) async
}
public struct SystemScheduler: WScheduler { public init() }
```

- [ ] **Step 1: Write failing tests** — the fake transport (scriptable: enqueue incoming frames, record sent frames, simulate close):

```swift
import Testing
import Foundation
@testable import ParRtDbClient

actor FakeTransport: WebSocketTransport {
    private(set) var sent: [String] = []
    private var incoming: [Result<String, Error>] = []
    private var waiters: [CheckedContinuation<String, Error>] = []
    var closeCode: UInt16?

    func enqueue(_ frame: String) { incoming.append(.success(frame)) }
    func enqueueClose(_ code: UInt16?) { incoming.append(.failure(TransportCloseError(code: code))) }
    func connect(to url: URL) async throws {}
    func send(_ text: String) async throws { sent.append(text) }
    func receive() async throws -> String {
        if !incoming.isEmpty { return try incoming.removeFirst().get() }
        return try await withCheckedThrowingContinuation { c in waiters.append(c) }
    }
    func close(code: UInt16) async { closeCode = code }
    func release(_ frame: String) { … resume first waiter or enqueue … }
}
```
Test: `URLSessionWebSocketTransport` compiles and (skipped-on-CI) round-trips one frame against a throwaway `Network.framework` listener — keep it simple: only assert the fake satisfies the protocol and `TransportCloseError` carries the code:

```swift
@Test func fakeTransportRecordsAndCloses() async throws {
    let fake = FakeTransport()
    await fake.enqueue(#"{"type":"pong"}"#)
    let frame = try await fake.receive()
    #expect(frame.contains("pong"))
    await fake.enqueueClose(4401)
    do { _ = try await fake.receive(); Issue.record("should throw") }
    catch let e as TransportCloseError { #expect(e.code == 4401) }
}
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter WsClientTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement Transport.swift** — `URLSessionWebSocketTransport` maps `URLSessionWebSocketTask.Message.string`, uses `receiveAnonymous`-style continuation bridging or AsyncStream per task; on `.closed` throw `TransportCloseError(code:)` (close code via `withCloseCode` if the SDK exposes it on macOS 14/iOS 17 — if unavailable, `code: nil` and note it; the WS client also reads `CloseReason` from server `authErr` frames).

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): WebSocketTransport seam — URLSession impl + WScheduler clock"
```

---

### Task 13: WS client actor — connection lifecycle

**Files:**
- Create: `swift-client/Sources/ParRtDbClient/WsClient.swift`
- Modify: `swift-client/Tests/ParRtDbClientTests/WsClientTests.swift`

**Interfaces:**
- Consumes: `WebSocketTransport`, `WScheduler`, `ClientMessage`/`ServerMessage`, `RtDbError` (Tasks 2–6, 12).
- Produces:

```swift
public struct RtDbClientConfig: Sendable {
    public var backoffBaseMs: UInt64 = 500
    public var backoffMaxMs: UInt64 = 15_000
    public var heartbeatMs: UInt64 = 20_000
    public init()
}
public enum WsState: Equatable, Sendable { case idle, connecting, connected, reconnecting, closed }
public struct ClientStatus: Equatable, Sendable { public var state: WsState; public var user: AuthedUser? }

public actor RtDbClient: Sendable {
    public init(url: String, db: String, getToken: @Sendable () async -> String?,
                config: RtDbClientConfig = RtDbClientConfig(),
                transportFactory: @escaping @Sendable (URL) -> any WebSocketTransport,
                scheduler: WScheduler = SystemScheduler())
    public func connect() async                       // idempotent
    public func close() async
    public func status() -> ClientStatus
    public var statusStream: AsyncStream<ClientStatus>    // state transitions
}
```
Constants (module-level): `let authDeadlineMs: UInt64 = 15_000`, terminal close code `4401`. Backoff: exponential base→max with jitter (±20% via scheduler-provided randomness — inject `random()` into WScheduler in this task if needed; keep determinism by seeding from an injectable closure).

- [ ] **Step 1: Write failing lifecycle tests** (fake transport + controllable scheduler):

```swift
@Test func connectAuthsAndBecomesConnected() async throws {
    let fake = await FakeTransport()
    let client = RtDbClient(url: "ws://rtdb.test/sync", db: "app",
                            getToken: { "tok" },
                            transportFactory: { _ in fake },
                            scheduler: ManualScheduler())          // fake WScheduler: records sleeps, never advances time
    async let connected: Void = client.awaitConnected()           // helper: suspends until state == .connected
    await fake.release(#"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#)
    _ = try await connected
    let sent = await fake.sent
    #expect(sent.first.map { $0.contains(#""type":"auth"#) } == true)
    let status = await client.status()
    #expect(status.state == .connected)
}

@Test func authDeadlineTearsDownWhenNoAuthOk() async throws {
    // with a manual scheduler whose now() jumps past authDeadlineMs → connect fails over to reconnecting
    …
}

@Test func close4401IsTerminal() async throws {
    … connect, then fake.enqueueClose(4401); client must land in .closed and NOT reconnect …
}

@Test func heartbeatPingsAndDetectsDeath() async throws {
    … advance manual clock past heartbeatMs → a ping frame was sent; past 2× with no pong → state reconnecting …
}

@Test func mutationsQueueWhileUnauthenticated() async throws { … }   // covered fully in Task 14
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter WsClientTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement WsClient.swift** — actor with: state machine (`WsState`), `generation: Int` guard against duplicate receive loops, `connect()` starting `runLoop(generation:)`: connect transport → send auth → race receive-loop vs auth-deadline → on authOk set connected + start heartbeat task → on failure backoff-and-retry (unless 4401 → `.closed`). Receive loop decodes `ServerMessage`, dispatches authOk/authErr/pong to lifecycle, everything else to handlers registered by Task 14 (define an internal `onServerMessage` hook table now, empty).

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): WS client actor — connect/auth deadline/heartbeat/backoff/4401-terminal"
```

---

### Task 14: WS client — subscriptions + mutate + scheduler ops

**Files:**
- Modify: `swift-client/Sources/ParRtDbClient/WsClient.swift`
- Modify: `swift-client/Tests/ParRtDbClientTests/WsClientTests.swift`

**Interfaces:**
- Consumes: Task 13 lifecycle; `Query`, `Transaction`, `StepResult`, `encodeCursor`.
- Produces:

```swift
public struct Subscription<T: Codable & Sendable>: Sendable {
    public var current: QuerySnapshot<T>            // .pending / .value(T) / .failed(RtDbError)
    public var stream: AsyncStream<QuerySnapshot<T>>
    public func cancel() async                       // drops this ref; last one sends unsubscribe
}
public enum QuerySnapshot<T: Codable & Sendable>: Sendable, Equatable where T: Equatable {
    case pending, value(T), failed(RtDbError)
}
extension RtDbClient {
    public func subscribe<T: Codable & Sendable>(_ query: Query, as type: T.Type = T.self) async throws -> Subscription<T>
    // canonical-query refcounting: same canonical JSON shape ⇒ one server queryId, N local handles.
    // On reconnect: replay subscribe frames for all live queries.
    public func mutate(_ txn: Transaction, idempotencyKey: String? = nil) async throws -> [StepResult]
    public func schedule(_ txn: Transaction, when: ScheduleWhen) async throws -> String
    public func cancelSchedule(_ id: String) async throws
    public func pauseSchedule(_ id: String) async throws
    public func resumeSchedule(_ id: String) async throws
    public func listSchedules() async throws -> [ScheduleInfo]
    public func startWorkflow(_ spec: WorkflowSpec) async throws -> String
    public func cancelWorkflow(_ id: String) async throws
    public func listWorkflows() async throws -> [WorkflowInfo]
}
```

- [ ] **Step 1: Write failing tests** — subscribe (queryUpdate → snapshot updates; second subscriber with same query reuses queryId; last cancel sends unsubscribe; subscribeErr → failed snapshot), mutate correlation (mutateOk with matching mutId resolves; mutateErr rejects with RtDbError), mutation queued while disconnected then flushed after authOk, resubscribe-after-reconnect:

```swift
@Test func subscribeDeliversUpdatesAndRefcounts() async throws {
    let fake = await FakeTransport()
    let client = … connect as Task 13 …
    struct Doc: Codable, Equatable, Sendable { var _id: String; var n: Int }
    let sub1 = try await client.subscribe(try TableQuery("t").collect().build(), as: Doc.self)
    let sub2 = try await client.subscribe(try TableQuery("t").collect().build(), as: Doc.self)
    let sent = await fake.sent
    #expect(sent.filter { $0.contains(#""type":"subscribe""#) }.count == 1)   // refcounted — ONE server subscribe
    await fake.release(#"{"type":"queryUpdate","queryId":"\(<firstQueryId(from: sent)>)"result":[{"_id":"a","n":1}]}"#)
    // … assert sub1/sub2 snapshots == [.value([Doc(_id: "a", n: 1)])] via stream or current …
    await sub1.cancel(); await sub2.cancel()
    #expect(sent.contains { $0.contains(#""type":"unsubscribe""#) })          // exactly one unsubscribe
}

@Test func mutateOkResolvesById() async throws { … }
@Test func queuedMutationFlushesAfterAuthOk() async throws { … }
@Test func reconnectReplaysSubscriptions() async throws { … }
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter WsClientTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement** — internal `SubscriptionRegistry` (canonical JSON → queryId + refcount + fan-out continuations), `MutationCorrelations` (mutId → checked continuation), wiring into Task 13's `onServerMessage` hook table. Reconnect path replays `subscribe` frames for all live entries.

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): WS subscriptions (refcounted, replayed) + mutate correlation + scheduler/workflow ops"
```

---

### Task 15: ParRtDbUI — LiveQuery

**Files:**
- Create: `swift-client/Sources/ParRtDbUI/LiveQuery.swift`
- Delete: `swift-client/Sources/ParRtDbUI/Placeholder.swift`
- Create: `swift-client/Tests/ParRtDbUITests/LiveQueryTests.swift`

**Interfaces:**
- Consumes: `RtDbClient`, `Subscription`, `QuerySnapshot` (Tasks 13–14).
- Produces:

```swift
import Observation

@MainActor @Observable
public final class LiveQuery<T: Codable & Sendable> {
    public enum State: Sendable { case pending; case value(T); case failed(RtDbError) }
    public private(set) var state: State = .pending
    public init(client: RtDbClient, query: Query, started: Bool = true)
    public func start() async          // idempotent; subscribes and pumps snapshots into state
    public func stop() async           // cancels the subscription
    // deinit cancels — deinit is nonisolated; hop via Task { await stop() } capturing the subscription.
}
```

- [ ] **Step 1: Write failing tests**

```swift
import Testing
import Foundation
import ParRtDbClient
@testable import ParRtDbUI

@MainActor
@Test func liveQueryPublishesValue() async throws {
    let fake = await FakeTransport()   // FakeTransport lives in ParRtDbClientTests — duplicate a minimal copy in this target's support file
    let client = RtDbClient(… connect, authOk …)
    struct Doc: Codable, Equatable, Sendable { var _id: String }
    let live = LiveQuery<[Doc]>(client: client, query: try TableQuery("t").collect().build())
    #expect(live.state == .pending)
    await fake.release(queryUpdateFrame(docId: "a"))
    try await waitFor { if case .value = live.state { true } else { false } }   // helper: poll with Task.yield, timeout 1s
    guard case .value(let docs) = live.state else { Issue.record("expected value"); return }
    #expect(docs == [Doc(_id: "a")])
}
```

- [ ] **Step 2: Run to verify fail** — `swift test --filter LiveQueryTests 2>&1 | tail -5`.

- [ ] **Step 3: Implement LiveQuery.swift.**

- [ ] **Step 4: Run full suite + lint** — green.

- [ ] **Step 5: Commit**

```bash
git add -A swift-client/
git commit -m "feat(swift-client): ParRtDbUI — @Observable LiveQuery over WS subscriptions"
```

---

### Task 16: Root Makefile + CI + docs

**Files:**
- Modify: `Makefile` (root), `.github/workflows/ci.yml`, `README.md`, `CLAUDE.md`, `FEATURE_MATRIX.md`, `wire-corpus/README.md`
- Create: `swift-client/README.md`

**Interfaces:**
- Consumes: the finished package (Tasks 1–15).
- Produces: `make checkall` on Darwin includes Swift; a macOS CI lane; docs current. Spec sections "Makefile & CI wiring" and "Documentation & parity bookkeeping" are the requirement text.

- [ ] **Step 1: Wire the root Makefile** — add targets + `.PHONY` entries (follow the python-client naming):

```make
SWIFT_OS := $(shell uname -s)
SWIFT_SKIP := @echo "Skipping swift-client (non-Darwin host)"

swift-client-build:    ; $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && swift build,$(SWIFT_SKIP))
swift-client-test:     ; $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && swift test,$(SWIFT_SKIP))
swift-client-lint:     ; $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && swiftlint --strict,$(SWIFT_SKIP))
swift-client-fmt:      ; $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && swiftformat .,$(SWIFT_SKIP))
swift-client-typecheck:; $(if $(filter Darwin,$(SWIFT_OS)),cd swift-client && swift build,$(SWIFT_SKIP))
swift-client-checkall: swift-client-fmt swift-client-lint swift-client-typecheck swift-client-test
```
Then add `cd swift-client && …` lines to the aggregate sweeps `build`, `fmt`, `fmt-check` (use `swiftformat --lint .` — match how the aggregate distinguishes check from apply; if swiftformat lacks a --lint-equivalent for the aggregate, run `swiftformat --dryrun --severity error .`), `lint`, `typecheck`, `test` — same Darwin guard form. Add all new target names to `.PHONY`.

- [ ] **Step 2: Add the CI lane** — in `.github/workflows/ci.yml`, add a job (match the existing job's action-pinning style — read the file first):

```yaml
  swift:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@<same pinned ref style as the existing job>
      - name: Swift gate
        run: make swift-client-checkall
```

- [ ] **Step 3: Write swift-client/README.md** — sections: What/Why (one paragraph, link the spec), Requirements (Swift 6, iOS 17+/macOS 14), Installation (SPM local path; `.package(url:)` placeholder-free — write the future GitHub URL form), Quick Start (construct `RtDbHttpClient` run a query; construct `RtDbClient` + `LiveQuery` in a SwiftUI view), DSL examples (query/mutation/schema), Storage, Error handling, Testing (incl. the env-gated live vars), Coverage table (v1 surfaces vs deferred: admin, presence, optimistic, in-memory engine — link the gap cards by title), License (MIT).

- [ ] **Step 4: Update repo docs** —
- `README.md`: add Swift to the client list/table with one-line description.
- `CLAUDE.md`: workspace table row (`| Swift client — ParRtDbClient | swift-client/ | swift |`); update the wire-contract paragraph: "four implementations" → "five implementations" adding `swift-client/Sources/ParRtDbClient/Wire.swift` to the list; update the "mirrored in all three clients" phrasing to "all four clients".
- `FEATURE_MATRIX.md`: add Swift coverage per the matrix's existing client-coverage convention — mark v1 surfaces covered, deferred surfaces explicitly "deferred (see gap cards)".
- `wire-corpus/README.md`: runners section gains Swift running `wire-corpus.json` only (not semantics/golden yet), linking the in-memory-engine gap card title.

- [ ] **Step 5: Full gate** — Run: `cd /Users/probello/Repos/par-rt-db && make -C /Users/probello/Repos/par-rt-db checkall; echo "EXIT=$?"` (dev Postgres must be up: `make dev-db-up` first if tests need it).
Expected: `EXIT=0` with the Swift suite included.

- [ ] **Step 6: Commit**

```bash
git add Makefile .github/workflows/ci.yml README.md CLAUDE.md FEATURE_MATRIX.md wire-corpus/README.md swift-client/README.md
git commit -m "feat(swift-client): join the gate (Darwin-guarded), macOS CI lane, docs — fifth client"
```

---

### Task 17: Live smoke test + card criteria verification

**Files:**
- Create: `swift-client/Tests/ParRtDbClientTests/LiveIntegrationTests.swift`

**Interfaces:**
- Consumes: everything.
- Produces: env-gated end-to-end proof against a real server (same convention as rust/python live tests).

- [ ] **Step 1: Write the live tests** (skipped unless `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY` set; creates a unique `t<uuid>` db via the admin token mint path used by rust `tests/common/mod.rs` — read it; never touch a db you didn't create):

```swift
import Testing
import Foundation
@testable import ParRtDbClient

@Suite(.skip(if: ProcessInfo.processInfo.environment["RTDB_TEST_SERVER_URL"] == nil
             || ProcessInfo.processInfo.environment["RTDB_TEST_ADMIN_KEY"] == nil,
             "live server not configured (RTDB_TEST_SERVER_URL/RTDB_TEST_ADMIN_KEY)"))
struct LiveIntegrationTests {
    @Test func httpPushQueryMutateRoundTrip() async throws {
        // mint token for fresh db t<uuid>, pushSchema, insert via mutate, run query, assert doc,
        // upload/download a blob, delete db (admin) — teardown in defer.
    }
    @Test func wsSubscribeReceivesLiveUpdate() async throws {
        // connect WS, subscribe collect, mutate via a second HTTP client, assert the
        // subscription delivers the updated snapshot within a generous timeout (10 s).
    }
}
```

- [ ] **Step 2: Run against a dev server** —
Run: `cd /Users/probello/Repos/par-rt-db && make dev-db-up && (cd server && RTDB_DATABASE_URL=postgres://127.0.0.1:55434/postgres RTDB_PORT=8300 RTDB_ADMIN_KEY=dev-admin-key cargo run &>/tmp/rtdb-server.log & echo $! >/tmp/rtdb-server.pid); sleep 5; tail -5 /tmp/rtdb-server.log`
Then: `cd swift-client && RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 RTDB_TEST_ADMIN_KEY=dev-admin-key swift test --filter LiveIntegrationTests 2>&1 | tail -8`
Expected: both live tests pass. Kill the server afterwards (`kill $(cat /tmp/rtdb-server.pid)`); env var names come from the server's README — verify against `server/src/config.rs` before starting it; adjust to the real names (the memory notes say RTDB_DATABASE_URL/PORT/ADMIN_KEY).

- [ ] **Step 3: Card acceptance criteria check** — the kanban card (`01a016ad676a772082917fd62ac29fb2`) carries five criteria. Verify EACH and record evidence, then check them off (`kanban item check --id … --criterion N --note "<evidence>"`):
1. "Swift wire types mirror protocol.rs byte-identically" — evidence: `swift test --filter WireCorpusTests` green (Task 7) + full suite.
2. "WS transport with live subscriptions and HTTP one-shot mutations both work against a live server" — evidence: Task 17 Step 2 output.
3. "swift-client test/lint target wired into make checkall and passing" — evidence: Task 16 Step 5 `EXIT=0`.
4. "wire-corpus runner executes the shared corpus cases with parity" — evidence: same as 1.
5. "Docs updated: FEATURE_MATRIX.md, README, CLAUDE.md workspace table, client README" — evidence: Task 16 commit.
Then `kanban item done --id 01a016ad676a772082917fd62ac29fb2` (and verify the lease didn't expire — re-claim if it did).

- [ ] **Step 4: Commit**

```bash
git add swift-client/Tests/ParRtDbClientTests/LiveIntegrationTests.swift
git commit -m "test(swift-client): env-gated live-server integration tests (HTTP + WS live update)"
```

---

## Self-Review (done at plan time)

- **Spec coverage:** wire layer (T2–6), corpus (T7), DSL (T8–10), HTTP (T11), WS (T12–14), UI (T15), gate/CI/docs (T16), live verification + criteria (T17). Deferred surfaces (admin, presence, optimistic, in-memory engine) are gap cards, per spec. ✓
- **Placeholder scan:** the only intentional "replace-me" text is inside Task 5's FilterExpr test where the plan explicitly says shapes MUST be read from `protocol.rs` in Step 1 — that is a directed read, not a placeholder. ✓
- **Type consistency:** `JSONValue`/`rejectUnknownKeys` (T2) → used T3–10; `Query` (T4) extended T8; `MutationLimits.maxSteps` (T6) asserted T7, enforced T9; `Subscription`/`QuerySnapshot` (T14) consumed T15; transport factory injection (T12) consumed T13–15. `wireObject()` introduced T8, generalized T9 via `WireEncodable`. ✓
