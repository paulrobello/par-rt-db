import Foundation
@testable import ParRtDbClient
import Testing

// QA-001: Golden-vector parity test (swift-client view) — the fifth runner of
// the vector.
//
// Loads `wire-corpus/golden-vector.json` (repo root — the single source of
// truth) and runs each query case through the swift-client in-memory engine,
// comparing canonicalized projected results. The same fixture is consumed by
// the ts-client, rust-client, python-client, and server (against Postgres)
// tests; a divergence in any one implementation surfaces there.
//
// The fixture encodes the dataset (legacy flat `schema_table`/`schema_fields`/
// `schema_indexes` shorthand + `seed` — NOT the semantics SchemaDef shape; the
// SchemaDef is built programmatically the way the ts/rust runners do), the
// per-case wire-shape `Query`, and the expected canonical result. Docs are
// projected to {name, status, order} before comparison so the client's
// id-minting order doesn't cause spurious divergence — the audit point is to
// catch **sort-comparator / boundary / terminal-cascade / filter-semantics**
// divergence, not id-minting drift.
//
// Every case is a pure read, so each parameterized case seeds its own fresh
// engine with the one shared dataset (swift-testing runs cases concurrently;
// the engine is deliberately not Sendable). Reads never mutate, so per-case
// seeding is observationally identical to the ts runner's shared instance.

// MARK: - Fixture

private struct GoldenFailure: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String {
        message
    }
}

/// One case: its id (names the test), the raw wire-shape query, and whichever
/// expected-* fields the case carries (the branch order in `runCase` mirrors
/// the ts runner exactly). Internal, not private: it appears in the
/// parameterized test's signature.
struct GoldenCase: Sendable, CustomStringConvertible {
    let id: String
    let query: JSONValue
    let expected: JSONValue?
    let expectedScalar: JSONValue?
    let expectedValuePresent: Bool
    let expectedValue: JSONValue?
    let expectedGroups: [JSONValue]?
    let expectedDistinct: [JSONValue]?
    let expectedUnordered: Bool
    let expectedHasNextCursor: Bool

    var description: String {
        id
    }
}

private struct GoldenFixture: Sendable {
    let schemaTable: String
    let schemaFields: [String: String]
    let schemaIndexes: [[String: JSONValue]]
    let seed: [[String: JSONValue]]
    let cases: [GoldenCase]
}

private func fixtureURL() -> URL {
    URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // ParRtDbClientTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // swift-client
        .deletingLastPathComponent() // repo root
        .appendingPathComponent("wire-corpus/golden-vector.json")
}

private func loadFixtureObject() throws -> [String: JSONValue] {
    let raw: JSONValue
    do {
        raw = try JSONDecoder().decode(JSONValue.self, from: Data(contentsOf: fixtureURL()))
    } catch {
        throw GoldenFailure("golden-vector.json: parse failure: \(error)")
    }
    guard let object = raw.objectValue else {
        throw GoldenFailure("golden-vector.json: top level is not a JSON object")
    }
    return object
}

/// The fixture's flat schema shorthand, parsed.
private struct FixtureSchema {
    let table: String
    let fields: [String: String]
    let indexes: [[String: JSONValue]]
}

private func parseSchema(_ object: [String: JSONValue]) throws -> FixtureSchema {
    guard let table = object["schema_table"]?.stringValue else {
        throw GoldenFailure("golden-vector.json: missing schema_table")
    }
    guard let fieldsObject = object["schema_fields"]?.objectValue else {
        throw GoldenFailure("golden-vector.json: schema_fields must be an object")
    }
    var fields: [String: String] = [:]
    for (name, shorthand) in fieldsObject {
        guard let type = shorthand.stringValue else {
            throw GoldenFailure("golden-vector.json: field '\(name)' shorthand must be a string")
        }
        fields[name] = type
    }
    guard let indexArray = object["schema_indexes"]?.arrayValue else {
        throw GoldenFailure("golden-vector.json: schema_indexes must be an array")
    }
    var indexes: [[String: JSONValue]] = []
    for index in indexArray {
        guard let spec = index.objectValue else {
            throw GoldenFailure("golden-vector.json: schema_indexes entries must be objects")
        }
        indexes.append(spec)
    }
    return FixtureSchema(table: table, fields: fields, indexes: indexes)
}

private func parseSeed(_ object: [String: JSONValue]) throws -> [[String: JSONValue]] {
    guard let seedArray = object["seed"]?.arrayValue else {
        throw GoldenFailure("golden-vector.json: seed must be an array")
    }
    return try seedArray.map { doc in
        guard let docObject = doc.objectValue else {
            throw GoldenFailure("golden-vector.json: seed entries must be objects")
        }
        return docObject
    }
}

private func parseCase(_ entry: JSONValue) throws -> GoldenCase {
    guard let caseObject = entry.objectValue else {
        throw GoldenFailure("golden-vector.json: case entries must be objects")
    }
    guard let id = caseObject["id"]?.stringValue else {
        throw GoldenFailure("golden-vector.json: case missing id")
    }
    guard let query = caseObject["query"] else {
        throw GoldenFailure("golden-vector.json: case '\(id)' missing query")
    }
    return GoldenCase(
        id: id,
        query: query,
        expected: caseObject["expected"],
        expectedScalar: caseObject["expected_scalar"],
        // A present JSON null (empty-set aggregate) is distinct from an
        // absent field — dictionary membership keeps them apart.
        expectedValuePresent: caseObject["expected_value"] != nil,
        expectedValue: caseObject["expected_value"],
        expectedGroups: caseObject["expected_groups"]?.arrayValue,
        expectedDistinct: caseObject["expected_distinct"]?.arrayValue,
        expectedUnordered: caseObject["expected_unordered"]?.boolValue ?? false,
        expectedHasNextCursor: caseObject["expected_has_next_cursor"]?.boolValue ?? false
    )
}

private func loadFixture() throws -> GoldenFixture {
    let object = try loadFixtureObject()
    let schema = try parseSchema(object)
    let casesArray = object["cases"]?.arrayValue ?? []
    guard !casesArray.isEmpty else {
        throw GoldenFailure("golden-vector.json: cases must be a non-empty array")
    }
    return try GoldenFixture(
        schemaTable: schema.table,
        schemaFields: schema.fields,
        schemaIndexes: schema.indexes,
        seed: parseSeed(object),
        cases: casesArray.map(parseCase)
    )
}

// MARK: - Dataset construction

/// Translate the fixture's field-type shorthand into a `FieldType`. Only the
/// types the fixture uses are implemented; a new shorthand fails loudly.
private func fieldType(fromShorthand shorthand: String) throws -> FieldType {
    switch shorthand {
    case "string": return .string
    case "number": return .number
    case "optional(string)": return .optional(inner: .string)
    case "array(string)": return .array(element: .string)
    default:
        if shorthand.hasPrefix("vector("), shorthand.hasSuffix(")") {
            let digits = shorthand.dropFirst("vector(".count).dropLast()
            guard let dimensions = UInt32(digits) else {
                throw GoldenFailure("fixture field type bad vector dimensions: \(shorthand)")
            }
            return .vector(dimensions: dimensions)
        }
        throw GoldenFailure("fixture field type not implemented: \(shorthand)")
    }
}

/// The index's vector dimensions when it declares a `vector` spec, else nil.
private func vectorDimensions(_ index: [String: JSONValue]) -> UInt32? {
    guard let vector = index["vector"]?.objectValue, let value = vector["dimensions"] else {
        return nil
    }
    return value.doubleValue.flatMap { UInt32(exactly: $0) }
}

/// Declare one fixture index on `builder`: search, vector, or plain btree.
private func declareIndex(
    _ builder: TableBuilder, _ index: [String: JSONValue]
) throws -> TableBuilder {
    guard let name = index["name"]?.stringValue else {
        throw GoldenFailure("golden-vector.json: index missing name")
    }
    guard let fieldNames = index["fields"]?.arrayValue?.compactMap(\.stringValue) else {
        throw GoldenFailure("golden-vector.json: index '\(name)' missing fields")
    }
    if index["search"]?.boolValue == true {
        return builder.searchIndex(name, on: fieldNames)
    }
    if let dimensions = vectorDimensions(index) {
        guard let field = fieldNames.first else {
            throw GoldenFailure("golden-vector.json: vector index '\(name)' names no field")
        }
        return builder.vectorIndex(name, on: field, dimensions: dimensions)
    }
    return builder.index(name, on: fieldNames)
}

/// Build the `SchemaDef` from the legacy flat shorthand (the ts runner's
/// `buildSchema`): fields, then btree / search / vector indexes.
private func buildSchema(_ fixture: GoldenFixture) throws -> SchemaDef {
    var builder = TableBuilder()
    for (name, shorthand) in fixture.schemaFields.sorted(by: { $0.key < $1.key }) {
        builder = try builder.field(name, fieldType(fromShorthand: shorthand))
    }
    for index in fixture.schemaIndexes {
        builder = try declareIndex(builder, index)
    }
    return SchemaBuilder().table(fixture.schemaTable) { _ in builder }.build()
}

/// Fresh engine seeded with the one shared dataset: deterministic incrementing
/// clock + constant RNG so each insert mints a distinct id.
private func seedClient(_ fixture: GoldenFixture) throws -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    let client = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
    try client.pushSchema(buildSchema(fixture))
    for doc in fixture.seed {
        let results = try client.mutate(Transaction(steps: [
            .insert(table: fixture.schemaTable, doc: doc)
        ]))
        guard case .insert = results[0] else {
            throw GoldenFailure("seed insert did not return an insert result")
        }
    }
    return client
}

// MARK: - Comparison

/// Project a doc to {name, status, order} — drops system fields so id-minting
/// order differences don't cause spurious divergence.
private func projectDoc(_ doc: JSONValue) -> JSONValue {
    let object = doc.objectValue ?? [:]
    return .object([
        "name": object["name"] ?? .null,
        "status": object["status"] ?? .null,
        "order": object["order"] ?? .null
    ])
}

private func projectList(_ docs: [JSONValue]) -> [JSONValue] {
    docs.map(projectDoc)
}

private func docName(_ projected: JSONValue) -> String {
    projected.objectValue?["name"]?.stringValue ?? ""
}

/// Lists equal element-wise under numeric tolerance, with a length pre-check.
private func listsMatchNumeric(_ got: [JSONValue], _ want: [JSONValue]) -> Bool {
    got.count == want.count && zip(got, want).allSatisfy { jsonEqNumeric($0.0, $0.1) }
}

// MARK: - Case assertions

/// `expected_scalar`: the count terminal returns a bare integer.
private func assertScalarCount(_ result: JSONValue, _ want: JSONValue, _ id: String) throws {
    guard jsonEqNumeric(result, want) else {
        throw GoldenFailure(
            "\(id): count mismatch — got \(result.debugString), want \(want.debugString)"
        )
    }
}

/// `expected_value`: an aggregate scalar — a bare number, or null for an empty
/// match set. Presence (not non-nullness) selects this branch.
private func assertAggregateValue(_ result: JSONValue, _ want: JSONValue, _ id: String) throws {
    guard jsonEqNumeric(result, want) else {
        throw GoldenFailure(
            "\(id): aggregate scalar mismatch — got \(result.debugString), want \(want.debugString)"
        )
    }
}

/// `expected_groups`: grouped aggregate — {key, value} rows by key ascending;
/// keys compare exactly, values numerically.
private func assertGroups(
    _ result: JSONValue, _ wantGroups: [JSONValue], _ id: String
) throws {
    guard case let .array(got) = result else {
        throw GoldenFailure("\(id): aggregate groupBy must return an array")
    }
    guard got.count == wantGroups.count else {
        throw GoldenFailure(
            "\(id): group count mismatch — got \(got.count), want \(wantGroups.count)"
        )
    }
    for (index, pairs) in zip(got, wantGroups).enumerated() {
        let gotKey = pairs.0.objectValue?["key"]
        let wantKey = pairs.1.objectValue?["key"]
        let gotValue = pairs.0.objectValue?["value"]
        let wantValue = pairs.1.objectValue?["value"]
        guard let gotKey, let wantKey, let gotValue, let wantValue else {
            throw GoldenFailure("\(id): group \(index) missing key/value")
        }
        guard jsonEqNumeric(gotKey, wantKey), jsonEqNumeric(gotValue, wantValue) else {
            throw GoldenFailure(
                "\(id): group \(index) mismatch — got \(pairs.0.debugString), "
                    + "want \(pairs.1.debugString)"
            )
        }
    }
}

/// `expected_distinct`: unique values of the index field, ascending.
private func assertDistinct(
    _ result: JSONValue, _ want: [JSONValue], _ id: String
) throws {
    guard case let .array(got) = result else {
        throw GoldenFailure("\(id): distinct must return an array")
    }
    guard listsMatchNumeric(got, want) else {
        throw GoldenFailure(
            "\(id): distinct mismatch — got \(result.debugString), "
                + "want \(JSONValue.array(want).debugString)"
        )
    }
}

/// `expected_unordered`: docs with no deterministic order — both sides
/// projected, sorted by name, then compared.
private func assertUnorderedDocs(
    _ result: JSONValue, _ wantDocs: [JSONValue], _ id: String
) throws {
    guard case let .array(gotDocs) = result else {
        throw GoldenFailure("\(id): unordered comparison requires an array result")
    }
    let got = projectList(gotDocs).sorted { docName($0) < docName($1) }
    let want = wantDocs.sorted { docName($0) < docName($1) }
    guard listsMatchNumeric(got, want) else {
        throw GoldenFailure(
            "\(id): unordered mismatch — got \(result.debugString), "
                + "want \(JSONValue.array(wantDocs).debugString)"
        )
    }
}

/// `expected_has_next_cursor`: paginate — projected docs match AND a
/// nextCursor is present.
private func assertPaginate(
    _ result: JSONValue, _ expected: JSONValue?, _ id: String
) throws {
    guard let page = result.objectValue, case let .array(docs)? = page["docs"] else {
        throw GoldenFailure("\(id): paginate must return {docs, nextCursor?}")
    }
    guard page["nextCursor"] != nil else {
        throw GoldenFailure("\(id): paginate result missing nextCursor")
    }
    let want = expected?.arrayValue ?? []
    guard listsMatchNumeric(projectList(docs), want) else {
        throw GoldenFailure(
            "\(id): paginate docs mismatch — got \(result.debugString), "
                + "want \(expected?.debugString ?? "nil")"
        )
    }
}

/// `expected` as an ordered array: projected docs compared in sequence.
private func assertOrderedList(
    _ result: JSONValue, _ wantDocs: [JSONValue], _ id: String
) throws {
    guard case let .array(gotDocs) = result else {
        throw GoldenFailure("\(id): ordered list comparison requires an array result")
    }
    guard listsMatchNumeric(projectList(gotDocs), wantDocs) else {
        throw GoldenFailure(
            "\(id): ordered list mismatch — got \(result.debugString), "
                + "want \(JSONValue.array(wantDocs).debugString)"
        )
    }
}

/// Fallback: a single projected doc (get / first / unique terminals).
private func assertSingleDoc(
    _ result: JSONValue, _ want: JSONValue, _ id: String
) throws {
    let got = projectDoc(result)
    guard jsonEqNumeric(got, want) else {
        throw GoldenFailure(
            "\(id): single-doc mismatch — got \(got.debugString), want \(want.debugString)"
        )
    }
}

// MARK: - Case execution

/// Execute one golden case against a freshly seeded engine. Branch order
/// mirrors the ts runner exactly: scalar count, aggregate scalar value,
/// grouped aggregate, distinct, unordered set, paginate-with-cursor, ordered
/// list, single doc.
private func runCase(_ fixture: GoldenFixture, _ golden: GoldenCase) throws {
    let client = try seedClient(fixture)
    let query: Query
    do {
        let data = try JSONEncoder().encode(golden.query)
        query = try JSONDecoder().decode(Query.self, from: data)
    } catch {
        throw GoldenFailure("\(golden.id): query does not decode: \(error)")
    }
    let result = try client.query(query)

    if let wantScalar = golden.expectedScalar {
        return try assertScalarCount(result, wantScalar, golden.id)
    }
    if golden.expectedValuePresent {
        return try assertAggregateValue(result, golden.expectedValue ?? .null, golden.id)
    }
    if let wantGroups = golden.expectedGroups {
        return try assertGroups(result, wantGroups, golden.id)
    }
    if let wantDistinct = golden.expectedDistinct {
        return try assertDistinct(result, wantDistinct, golden.id)
    }
    if golden.expectedUnordered, case let .array(wantDocs)? = golden.expected {
        return try assertUnorderedDocs(result, wantDocs, golden.id)
    }
    if golden.expectedHasNextCursor {
        return try assertPaginate(result, golden.expected, golden.id)
    }
    if case let .array(wantDocs)? = golden.expected {
        return try assertOrderedList(result, wantDocs, golden.id)
    }
    return try assertSingleDoc(result, golden.expected ?? .null, golden.id)
}

// MARK: - Suite

struct GoldenVectorTests {
    /// One parameterized test case per fixture case — the fixture's `cases`
    /// array IS the count (dynamic, never a hardcoded constant).
    @Test("golden vector case", arguments: try loadFixture().cases)
    func goldenCase(_ golden: GoldenCase) throws {
        try runCase(loadFixture(), golden)
    }

    /// The fixture loaded: a non-empty dataset and case list. The case count
    /// rides the parameterized test above; this pins the dataset shape.
    @Test func fixtureLoadsNonEmpty() throws {
        let fixture = try loadFixture()
        #expect(!fixture.seed.isEmpty)
        #expect(!fixture.cases.isEmpty)
        let message = "golden vector: \(fixture.cases.count) cases over \(fixture.seed.count) docs"
        print(message)
    }
}
