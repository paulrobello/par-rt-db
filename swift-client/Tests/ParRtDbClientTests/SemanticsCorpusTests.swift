import Foundation
@testable import ParRtDbClient
import Testing

// ENH-023: behavioral-semantics corpus runner (swift-client in-memory view) —
// the fifth runner of the corpus.
//
// Enumerates every `*.json` case in `wire-corpus/semantics/` (repo root — the
// single source of truth; one self-contained case per file carrying its own
// schema, seed, operation, and expected result) and executes each against a
// fresh in-memory engine instance, comparing normalized results. The same
// fixture files are consumed by the server (Postgres), ts-client, rust-client,
// and python-client; the server is the source of truth for every expected
// value, so a divergence here is a swift-engine bug (or a stale fixture).
//
// The runner implements `wire-corpus/README.md`'s "How a runner executes a
// case" algorithm exactly, mirroring ts-client/tests/semantics-corpus.test.ts:
// runtime directory enumeration (the directory IS the case count — no
// hardcoded constant), per-case fresh client, seed through the normal `mutate`
// insert path with `$id` label capture, `{"$idRef": ...}` substitution
// throughout `op`/`then.query`, the `"$prev"` paginate-cursor sentinel, error
// cases asserting the error `code` only, `normalize` projection applied
// recursively to both trees, `unordered` multiset comparison via
// canonical-JSON sort, numeric-tolerant equality (JSONValue's .int/.double
// split needs `6 == 6.0`), and structural `expect_next_cursor` presence. The
// injected clock makes id minting and `_creationTime` deterministic; no time
// is advanced and no scheduler/TTL reaper runs between seeding and the op —
// the corpus pins synchronous semantics only.
//
// A `skip: {"swift": "reason"}` case is skipped loudly: the reason rides the
// test-case ID (the argument's `description`). No corpus case carries a swift
// skip today — the engine is a full port.

// MARK: - Failure type

private struct CorpusFailure: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String {
        message
    }
}

// MARK: - Clock

/// Monotonically increasing per-case clock, boxed for `@Sendable` capture —
/// the ts runner's `now: () => ms++` (each insert mints a distinct
/// `_creationTime` even with the pinned RNG; normalize projects it out anyway).
/// Internal: shared with GoldenVectorTests.
final class MonotonicMs: @unchecked Sendable {
    private let lock = NSLock()
    private var ms: Int64

    init(_ start: Int64) {
        ms = start
    }

    func next() -> Int64 {
        lock.lock()
        defer { lock.unlock() }
        let current = ms
        ms += 1
        return current
    }
}

/// Fresh engine per case: deterministic incrementing clock + constant RNG.
private func makeClient() -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    return InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
}

// MARK: - JSONValue helpers

// Internal: shared with GoldenVectorTests.
extension JSONValue {
    var boolValue: Bool? {
        if case let .bool(value) = self {
            return value
        }
        return nil
    }

    var arrayValue: [JSONValue]? {
        if case let .array(value) = self {
            return value
        }
        return nil
    }

    /// Compact JSON with object keys sorted recursively — the canonical form
    /// the unordered multiset sort uses (README determinism ruling 2).
    var canonicalString: String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        guard let data = try? encoder.encode(self) else {
            return "<unencodable>"
        }
        return String(data: data, encoding: .utf8) ?? "<unencodable>"
    }

    /// Readable failure payload (sorted keys, pretty) — never contract.
    var debugString: String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .prettyPrinted]
        guard let data = try? encoder.encode(self) else {
            return "<unencodable>"
        }
        return String(data: data, encoding: .utf8) ?? "<unencodable>"
    }
}

/// Decode a wire struct out of a raw `JSONValue` tree (corpus JSON -> wire
/// types through the standard Codable path, same as the server's serde).
private func decodeWire<T: Decodable>(_: T.Type, _ value: JSONValue, _ what: String) throws -> T {
    let data: Data
    do {
        data = try JSONEncoder().encode(value)
    } catch {
        throw CorpusFailure("\(what): re-encode failure: \(error)")
    }
    do {
        return try JSONDecoder().decode(T.self, from: data)
    } catch {
        throw CorpusFailure("\(what): decode failure: \(error)\n  input: \(value.debugString)")
    }
}

// MARK: - Case loading

/// System fields minted at run time and projected out of both sides unless a
/// case's `normalize` list replaces the default (README "Semantics corpus
/// format"). A txn case adds `"id"` for minted step-result ids — via its own
/// `normalize` list, never a runner-side default.
private let defaultNormalize = ["_id", "_creationTime", "_version"]

/// One loaded corpus case: the file's JSON parsed into the pieces the runner
/// needs, with schema/op/then kept raw (substitution happens per execution).
/// `description` names the parameterized test case; a skip reason rides it.
/// (Internal, not private: it appears in the parameterized test's signature.)
struct SemanticsCase: Sendable, CustomStringConvertible {
    /// A follow-up read after a successful op (write-then-read visibility
    /// cases). Inherits the case-level `normalize` unless it gives its own.
    struct ThenBlock: Sendable {
        let query: JSONValue
        let expect: JSONValue
        let unordered: Bool
        let normalize: [String]?
    }

    let stem: String
    let schema: SchemaDef
    let seed: [JSONValue]
    let opQuery: JSONValue?
    let opTxn: JSONValue?
    let expect: JSONValue
    let unordered: Bool
    let normalize: [String]?
    let expectNextCursor: Bool?
    let thenBlock: ThenBlock?
    let skipReason: String?

    var description: String {
        if let skipReason {
            return "[skip:swift] \(stem) — \(skipReason)"
        }
        return stem
    }
}

/// The corpus directory (repo root), located from this file like
/// WireCorpusTests does.
private func corpusDirectory() -> URL {
    URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // ParRtDbClientTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // swift-client
        .deletingLastPathComponent() // repo root
        .appendingPathComponent("wire-corpus/semantics")
}

/// Enumerate every `*.json` case file, sorted — the directory IS the count.
private func corpusStems() throws -> [String] {
    let contents = try FileManager.default.contentsOfDirectory(
        at: corpusDirectory(), includingPropertiesForKeys: nil
    )
    let stems = contents
        .filter { $0.pathExtension == "json" }
        .map { $0.deletingPathExtension().lastPathComponent }
    guard !stems.isEmpty else {
        throw CorpusFailure("wire-corpus/semantics contains no fixture files")
    }
    return stems.sorted()
}

/// Parse one case file. `name` must equal the filename stem (README format).
private func loadCase(stem: String) throws -> SemanticsCase {
    let url = corpusDirectory().appendingPathComponent("\(stem).json")
    let raw: JSONValue
    do {
        raw = try JSONDecoder().decode(JSONValue.self, from: Data(contentsOf: url))
    } catch {
        throw CorpusFailure("\(stem): parse failure: \(error)")
    }
    guard let object = raw.objectValue else {
        throw CorpusFailure("\(stem): case is not a JSON object")
    }
    guard let name = object["name"]?.stringValue else {
        throw CorpusFailure("\(stem): missing name")
    }
    guard name == stem else {
        throw CorpusFailure(
            "\(stem): case `name` ('\(name)') must equal the filename stem ('\(stem)')"
        )
    }
    let schema = try decodeWire(
        SchemaDef.self, required(object, "schema", stem), "\(stem): schema"
    )
    guard let seed = object["seed"]?.arrayValue else {
        throw CorpusFailure("\(stem): seed must be an array")
    }
    guard let op = object["op"]?.objectValue else {
        throw CorpusFailure("\(stem): missing op object")
    }
    guard op["query"] != nil || op["txn"] != nil else {
        throw CorpusFailure("\(stem): op must carry `query` or `txn`")
    }
    return try SemanticsCase(
        stem: stem, schema: schema, seed: seed,
        opQuery: op["query"], opTxn: op["txn"],
        expect: required(object, "expect", stem),
        unordered: object["unordered"]?.boolValue ?? false,
        normalize: stringList(object["normalize"], "\(stem): normalize"),
        expectNextCursor: object["expect_next_cursor"]?.boolValue,
        thenBlock: parseThen(object, stem),
        skipReason: skipReason(object, stem)
    )
}

/// The required member `key` of a case object, or a loud failure.
private func required(
    _ object: [String: JSONValue], _ key: String, _ stem: String
) throws -> JSONValue {
    guard let value = object[key] else {
        throw CorpusFailure("\(stem): missing \(key)")
    }
    return value
}

/// A case's `then` block (follow-up read), nil when the case has none.
private func parseThen(
    _ object: [String: JSONValue], _ stem: String
) throws -> SemanticsCase.ThenBlock? {
    guard let then = object["then"] else {
        return nil
    }
    guard let block = then.objectValue, let query = block["query"], let expect = block["expect"]
    else {
        throw CorpusFailure("\(stem): then requires query and expect")
    }
    return try SemanticsCase.ThenBlock(
        query: query,
        expect: expect,
        unordered: block["unordered"]?.boolValue ?? false,
        normalize: stringList(block["normalize"], "\(stem): then.normalize")
    )
}

/// A `normalize` list: absent -> nil (caller applies its fallback); present ->
/// the exact strings (a present list REPLACES the default).
private func stringList(_ value: JSONValue?, _ what: String) throws -> [String]? {
    guard let value else {
        return nil
    }
    guard let items = value.arrayValue else {
        throw CorpusFailure("\(what): must be an array when present")
    }
    var out: [String] = []
    for item in items {
        guard let string = item.stringValue else {
            throw CorpusFailure("\(what): entries must be strings")
        }
        out.append(string)
    }
    return out
}

/// The `skip.swift` reason, nil when the case is not swift-skipped.
private func skipReason(_ object: [String: JSONValue], _: String) -> String? {
    guard let skip = object["skip"]?.objectValue else {
        return nil
    }
    return skip["swift"]?.stringValue
}

/// Load every case file. A parse failure here fails ALL tests loudly (the
/// arguments expression throws), so no file can be silently dropped.
private func loadCorpus() throws -> [SemanticsCase] {
    try corpusStems().map { try loadCase(stem: $0) }
}

// MARK: - Seed parsing

/// A resolved `seed` entry: where the doc goes and its optional `$id` label.
private struct SeedEntry {
    let table: String
    let doc: [String: JSONValue]
    let label: String?
}

/// Resolve one `seed` entry into a `SeedEntry`. A wrapped entry is an object
/// with a `doc` key whose value is an object (with optional `table` and `$id`
/// siblings); any other object is a plain doc, legal only when the schema
/// declares exactly one table (the disambiguation rule the corpus README
/// states).
private func parseSeedEntry(
    _ entry: JSONValue, _ singleTable: String?, _ caseName: String, _ index: Int
) throws -> SeedEntry {
    guard let object = entry.objectValue else {
        throw CorpusFailure("\(caseName): seed #\(index) must be a JSON object")
    }
    if let wrapped = object["doc"]?.objectValue {
        let table: String
        if let named = object["table"]?.stringValue {
            table = named
        } else if let singleTable {
            table = singleTable
        } else {
            throw CorpusFailure(
                "\(caseName): seed #\(index): wrapped entry without `table` requires a single-table schema"
            )
        }
        return SeedEntry(table: table, doc: wrapped, label: object["$id"]?.stringValue)
    }
    guard let singleTable else {
        throw CorpusFailure(
            "\(caseName): seed #\(index): plain-doc seed requires a single-table schema"
        )
    }
    return SeedEntry(table: singleTable, doc: object, label: nil)
}

// MARK: - Substitution & projection

/// Replace every `{"$idRef": "<label>"}` object anywhere in the tree with the
/// minted id recorded for that seed label (README "Substitution placeholders").
private func substitute(
    _ node: JSONValue, _ ids: [String: String], _ caseName: String
) throws -> JSONValue {
    switch node {
    case let .array(items):
        return try .array(items.map { try substitute($0, ids, caseName) })
    case let .object(object):
        if object.count == 1, let ref = object["$idRef"] {
            guard case let .string(label) = ref else {
                throw CorpusFailure("\(caseName): $idRef label must be a string")
            }
            guard let id = ids[label] else {
                throw CorpusFailure(
                    "\(caseName): $idRef references unknown seed label '\(label)'"
                )
            }
            return .string(id)
        }
        return try .object(object.mapValues { try substitute($0, ids, caseName) })
    default:
        return node
    }
}

/// Remove every `keys` member from every object in the tree, recursively —
/// the README's `normalize` projection applies to every object in both the
/// actual and expected trees (docs inside `paginate.docs`, step results, ...).
private func projectRecursive(_ node: JSONValue, _ keys: Set<String>) -> JSONValue {
    switch node {
    case let .array(items):
        return .array(items.map { projectRecursive($0, keys) })
    case let .object(object):
        var out: [String: JSONValue] = [:]
        for (key, value) in object where !keys.contains(key) {
            out[key] = projectRecursive(value, keys)
        }
        return .object(out)
    default:
        return node
    }
}

// MARK: - Comparison

/// Numeric-tolerant equality so the SQL-numeric server result and the client
/// number result agree (e.g. `6` == `6.0` across JSONValue's .int/.double
/// split) — the same tolerance golden-vector applies. Recurses into
/// arrays/objects; booleans stay distinct from numbers. Internal: shared with
/// GoldenVectorTests.
func jsonEqNumeric(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case (.null, .null):
        return true
    case (.int, .int), (.int, .double), (.double, .int), (.double, .double):
        guard let lhsNum = lhs.doubleValue, let rhsNum = rhs.doubleValue else {
            return lhs == rhs
        }
        return lhsNum == rhsNum || abs(lhsNum - rhsNum) < 1e-9
    case let (.array(lhsItems), .array(rhsItems)):
        return lhsItems.count == rhsItems.count
            && zip(lhsItems, rhsItems).allSatisfy { jsonEqNumeric($0.0, $0.1) }
    case let (.object(lhsObject), .object(rhsObject)):
        return lhsObject.count == rhsObject.count && lhsObject.allSatisfy { key, value in
            guard let other = rhsObject[key] else {
                return false
            }
            return jsonEqNumeric(value, other)
        }
    default:
        return lhs == rhs
    }
}

/// Assert actual == expected under `normalize` projection already applied:
/// `unordered` compares the two arrays as multisets (each side sorted by
/// canonical JSON, then element-wise numeric-tolerant), otherwise the values
/// compare in place, recursively numeric-tolerant. Mirrors the ts runner's
/// `assertExpected`.
private func assertExpected(
    _ got: JSONValue, _ want: JSONValue, unordered: Bool, _ message: String
) throws {
    if jsonEqNumeric(got, want) {
        return // equal as sequences — also covers every unordered case
    }
    guard unordered else {
        throw CorpusFailure("\(message)\n got \(got.debugString)\nwant \(want.debugString)")
    }
    guard case let .array(gotRows) = got, case let .array(wantRows) = want else {
        throw CorpusFailure(
            "\(message): unordered comparison requires arrays — got \(got.debugString), "
                + "want \(want.debugString)"
        )
    }
    guard gotRows.count == wantRows.count else {
        throw CorpusFailure(
            "\(message): row count mismatch (unordered) — got \(gotRows.count), "
                + "want \(wantRows.count)"
        )
    }
    let gotSorted = gotRows.sorted { $0.canonicalString < $1.canonicalString }
    let wantSorted = wantRows.sorted { $0.canonicalString < $1.canonicalString }
    for (index, pair) in zip(gotSorted, wantSorted).enumerated() {
        let (gotRow, wantRow) = pair
        let matches = jsonEqNumeric(gotRow, wantRow)
        if !matches {
            throw CorpusFailure(
                "\(message): row \(index) mismatch (unordered compare)\n"
                    + " got \(got.debugString)\nwant \(want.debugString)"
            )
        }
    }
    // Lengths equal and every sorted row matched: the multisets agree, so the
    // values differ only in order — exactly what `unordered` forgives.
}

/// The expected error `code` when `expect` is an error object, else nil.
private func errorCodeOf(_ expect: JSONValue) -> String? {
    guard let error = expect.objectValue?["error"]?.objectValue else {
        return nil
    }
    return error["code"]?.stringValue
}

/// Error-case assertion: only the code is compared, never the message.
private func assertErrorCode(
    _ error: RtDbError, _ wantCode: String, _ caseName: String
) throws {
    guard let want = ErrorCode(rawValue: wantCode) else {
        throw CorpusFailure("\(caseName): expected error code does not parse: \(wantCode)")
    }
    guard error.code == want else {
        throw CorpusFailure(
            "\(caseName): error code mismatch — got \(error.code.rawValue), want \(wantCode)"
                + " (engine message: \(error.message))"
        )
    }
}

/// What an assertion compares against: the `expect` tree, the optional
/// `expect_next_cursor` pin (paginate), and the effective `normalize` /
/// `unordered` flags.
private struct Expectation {
    let expect: JSONValue
    let expectNextCursor: Bool?
    let keys: [String]
    let unordered: Bool
}

/// Compare an op/then success result against its `expect` block: apply the
/// `normalize` projection to both trees, structurally assert `nextCursor`
/// presence when the block pins it (paginate), then ordered/unordered compare.
private func assertResult(
    _ caseName: String, _ actual: JSONValue, _ expectation: Expectation
) throws {
    var projected = Set(expectation.keys)
    var got = actual
    var want = expectation.expect
    if let pinned = expectation.expectNextCursor {
        let has = actual.objectValue?["nextCursor"] != nil
        guard has == pinned else {
            throw CorpusFailure(
                "\(caseName): nextCursor presence mismatch (got \(has), want \(pinned))"
            )
        }
        projected.insert("nextCursor")
    }
    got = projectRecursive(got, projected)
    want = projectRecursive(want, projected)
    try assertExpected(got, want, unordered: expectation.unordered, "\(caseName): result mismatch")
}

// MARK: - Execution

/// Serialize typed step results back to their raw wire shapes (the corpus
/// `expect` blocks are raw JSON, not typed StepResults).
private func stepResultsJSON(_ results: [StepResult]) throws -> JSONValue {
    let data = try JSONEncoder().encode(results)
    return try JSONDecoder().decode(JSONValue.self, from: data)
}

/// Execute a query op: substitute placeholders (README step 3), then resolve
/// the `"$prev"` paginate-cursor sentinel when present (README step 4) — run
/// the cursor-less query, take its `nextCursor` (fail loudly if absent), then
/// run the query with it — `expect` describes the SECOND page.
private func executeQueryOp(
    _ client: InMemoryRtDbClient, _ queryJson: JSONValue, _ ids: [String: String], _ caseName: String
) throws -> JSONValue {
    let substituted = try substitute(queryJson, ids, caseName)
    var query = try decodeWire(Query.self, substituted, "\(caseName): op.query")
    if query.paginate?.cursor == "$prev" {
        var firstPage = query
        firstPage.paginate?.cursor = nil
        let first = try client.query(firstPage)
        guard case let .string(cursor) = first.objectValue?["nextCursor"] else {
            throw CorpusFailure("\(caseName): $prev: first page has no nextCursor")
        }
        query.paginate?.cursor = cursor
    }
    return try client.query(query)
}

/// Execute the case's op, capturing an engine error instead of throwing it so
/// the caller can distinguish expected failures (error cases) from surprises.
private func executeOp(
    _ client: InMemoryRtDbClient, _ corpusCase: SemanticsCase, _ ids: [String: String]
) throws -> Result<JSONValue, RtDbError> {
    let caseName = corpusCase.stem
    if let txnJson = corpusCase.opTxn {
        let txn = try decodeWire(
            Transaction.self, substitute(txnJson, ids, caseName), "\(caseName): op.txn"
        )
        do {
            return try .success(stepResultsJSON(client.mutate(txn)))
        } catch let error as RtDbError {
            return .failure(error)
        }
    }
    guard let queryJson = corpusCase.opQuery else {
        throw CorpusFailure("\(caseName): op must carry `query` or `txn`")
    }
    do {
        return try .success(executeQueryOp(client, queryJson, ids, caseName))
    } catch let error as RtDbError {
        return .failure(error)
    }
}

/// Seed every entry through the normal insert path (`mutate` with a single
/// insert step), recording `label -> minted id` for `$id`-labeled entries.
private func seedClient(
    _ client: InMemoryRtDbClient, _ corpusCase: SemanticsCase, _ singleTable: String?
) throws -> [String: String] {
    var ids: [String: String] = [:]
    for (index, entry) in corpusCase.seed.enumerated() {
        let seed = try parseSeedEntry(entry, singleTable, corpusCase.stem, index)
        let results = try client.mutate(Transaction(steps: [
            .insert(table: seed.table, doc: seed.doc)
        ]))
        guard case let .insert(id) = results[0] else {
            throw CorpusFailure("\(corpusCase.stem): seed #\(index): insert result missing id")
        }
        if let label = seed.label {
            ids[label] = id
        }
    }
    return ids
}

/// Execute one corpus case end to end against a fresh in-memory instance.
/// Every failure names the case.
private func runCase(_ corpusCase: SemanticsCase) throws {
    let caseName = corpusCase.stem
    let client = makeClient()
    try client.pushSchema(corpusCase.schema)

    let tableNames = corpusCase.schema.tables.keys.sorted()
    let singleTable = tableNames.count == 1 ? tableNames[0] : nil
    let ids = try seedClient(client, corpusCase, singleTable)

    let expectErr = errorCodeOf(corpusCase.expect)
    let caseKeys = corpusCase.normalize ?? defaultNormalize

    // Execute the op. An expected-error case asserts the code and stops (no
    // `then` follow-up); an unexpected error fails loudly.
    let opResult: JSONValue
    switch try executeOp(client, corpusCase, ids) {
    case let .failure(error):
        guard let want = expectErr else {
            throw CorpusFailure(
                "\(caseName): unexpected op error (\(error.code.rawValue)): \(error.message)"
            )
        }
        try assertErrorCode(error, want, caseName)
        return // a failed op has no `then` follow-up
    case let .success(value):
        opResult = value
    }

    if let want = expectErr {
        throw CorpusFailure(
            "\(caseName): expected error \(want), got success \(opResult.debugString)"
        )
    }
    try assertResult(caseName, opResult, Expectation(
        expect: corpusCase.expect,
        expectNextCursor: corpusCase.expectNextCursor,
        keys: caseKeys,
        unordered: corpusCase.unordered
    ))

    // Follow-up read after a successful op (write-then-read visibility cases).
    guard let then = corpusCase.thenBlock else {
        return
    }
    let substituted = try substitute(then.query, ids, caseName)
    let query = try decodeWire(Query.self, substituted, "\(caseName): then.query")
    let actual = try client.query(query)
    try assertResult(caseName, actual, Expectation(
        expect: then.expect,
        expectNextCursor: nil, // `then` blocks carry no cursor pin (ts ThenBlock)
        keys: then.normalize ?? caseKeys,
        unordered: then.unordered
    ))
}

// MARK: - Suite

struct SemanticsCorpusTests {
    /// One parameterized test case per corpus file — executed unless the case
    /// carries a swift skip (which lands in the test-case ID via `description`).
    @Test("semantics corpus case", arguments: try loadCorpus())
    func semanticsCase(_ corpusCase: SemanticsCase) throws {
        guard let reason = corpusCase.skipReason else {
            try runCase(corpusCase)
            return
        }
        // Loud skip: the reason rides the test-case ID; nothing executes.
        print("skip: \(corpusCase.stem) (\(reason))")
    }

    /// Every corpus file became exactly one executed-or-skipped test case —
    /// the directory IS the count (dynamic, never a hardcoded constant). The
    /// re-enumeration catches any load() path that drops a file; a case that
    /// fails to parse throws at load time (failing every test above). Within
    /// the parameterized test each case takes exactly one of the two body
    /// paths (execute, or skip with the reason in its ID), so
    /// executed + skipped == files.
    @Test func accountsForEveryCorpusFile() throws {
        let stems = try corpusStems()
        let loaded = try loadCorpus()
        let skipped = loaded.compactMap(\.skipReason)
        #expect(loaded.map(\.stem) == stems)
        #expect(loaded.count == stems.count)
        // Report the split loudly for anyone reading the log.
        let executedCount = stems.count - skipped.count
        print("semantics corpus: \(stems.count) files, \(executedCount) executed, \(skipped.count) skipped")
    }
}
