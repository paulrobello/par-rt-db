import Foundation
@testable import ParRtDbClient
import Testing

/// ENH-052 family 1 — spec-table corpus runner: the FieldType → Postgres
/// column-type mapping (wire-corpus/spec/index-column-types.json).
///
/// Asserts the engine's `indexColumnType` against every fixture row — decoded
/// through the client's real Codable `FieldType`, the same wire spelling
/// pushSchema parses — and that the engine's FieldType variant set is covered
/// by the fixture. `columnType` carries the server's raw Postgres string; the
/// four-value `PgType` maps back to it.
struct SpecCorpusTests {
    private struct SpecDoc: Decodable {
        let rows: [SpecRow]
    }

    private struct SpecRow: Decodable {
        let fieldType: FieldType
        let columnType: String?
        let nullable: Bool?
        let error: Bool?
    }

    /// The corpus file, located from this file's path (the SemanticsCorpusTests
    /// pattern).
    private func loadCorpus() throws -> SpecDoc {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // ParRtDbClientTests
            .deletingLastPathComponent() // Tests
            .deletingLastPathComponent() // swift-client
            .deletingLastPathComponent() // repo root
            .appendingPathComponent("wire-corpus/spec/index-column-types.json")
        return try JSONDecoder().decode(SpecDoc.self, from: Data(contentsOf: url))
    }

    /// The engine's four-value storage enum mapped back to the server's raw
    /// column strings.
    private func rawPg(_ pg: PgType) -> String {
        switch pg {
        case .text: "text"
        case .number: "double precision"
        case .boolean: "boolean"
        case .int64: "bigint"
        }
    }

    /// One representative per FieldType variant this engine's indexColumnType
    /// distinguishes (the corpus README's own-variant coverage rule: a variant
    /// without a fixture row fails).
    private func ownVariants() -> [FieldType] {
        [
            .string,
            .number,
            .boolean,
            .int64,
            .id(table: "t", onDelete: nil),
            .id(table: "t", onDelete: .cascade),
            .literal(value: .string("x")),
            .literal(value: .int(42)),
            .optional(inner: .string),
            .union(variants: [.literal(value: .string("a")), .literal(value: .string("b"))]),
            .union(variants: []),
            .union(variants: [.literal(value: .string("a")), .number]),
            .null,
            .array(element: .string),
            .object(fields: ["a": .string]),
            .bytes,
            .any,
            .record(value: .string),
            .vector(dimensions: 3)
        ]
    }

    @Test func everyOwnVariantHasAFixtureRow() throws {
        let rows = try loadCorpus().rows
        for own in ownVariants() {
            #expect(
                rows.contains { $0.fieldType == own },
                "no fixture row for \(own)"
            )
        }
    }

    @Test func everyFixtureRowMatchesThisEngine() throws {
        for (rowIndex, row) in try loadCorpus().rows.enumerated() {
            if row.error == true {
                #expect(throws: RtDbError.self) {
                    try indexColumnType(row.fieldType)
                }
                continue
            }
            let resolved = try indexColumnType(row.fieldType)
            #expect(rawPg(resolved.pg) == row.columnType, "row \(rowIndex) columnType")
            #expect(resolved.nullable == row.nullable, "row \(rowIndex) nullable")
        }
    }
}

/// ENH-052 family 2 — spec-table corpus runner: computed-field stamping
/// (wire-corpus/spec/computed-stamping.json).
///
/// Asserts the engine's `stampComputed` port of server `txn::stamp_computed` +
/// `value_expr::eval_value_expr` against every fixture row: each row's wire
/// schema fragment (`fields` + `computed`) is decoded through the client's
/// real Codable `TableDef`, stamped with the engine, and asserted per key —
/// a JSON-null expected value means the key must be ABSENT after stamping;
/// anything else compares numerically (`.int(42)` and `.double(42.0)` are the
/// same JSON number). A semantics change regenerates the fixture in the same
/// commit (`make gen-spec-computed-stamping`).
struct ComputedStampingSpecCorpusTests {
    private struct SpecDoc: Decodable {
        let rows: [SpecRow]
    }

    private struct SpecRow: Decodable {
        let id: String
        let fields: [String: JSONValue]
        let computed: [String: JSONValue]
        let inputDoc: [String: JSONValue]
        let now: Int64
        let expect: Expect
    }

    private struct Expect: Decodable {
        let stampedValues: [String: JSONValue]?
        let error: Bool?
    }

    /// The table JSON object the row's fragment decodes through: the same
    /// shape pushSchema parses.
    private struct Fragment: Encodable {
        let fields: [String: JSONValue]
        let computed: [String: JSONValue]
    }

    /// The corpus file, located from this file's path (the SemanticsCorpusTests
    /// pattern).
    private func loadCorpus() throws -> SpecDoc {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // ParRtDbClientTests
            .deletingLastPathComponent() // Tests
            .deletingLastPathComponent() // swift-client
            .deletingLastPathComponent() // repo root
            .appendingPathComponent("wire-corpus/spec/computed-stamping.json")
        return try JSONDecoder().decode(SpecDoc.self, from: Data(contentsOf: url))
    }

    private func decodeTable(_ row: SpecRow) throws -> TableDef {
        let data = try JSONEncoder().encode(Fragment(fields: row.fields, computed: row.computed))
        return try JSONDecoder().decode(TableDef.self, from: data)
    }

    /// Numeric-tolerant recursive equality: `1` and `1.0` are the same JSON
    /// number, everything else compares by payload (the corpus README's
    /// cross-engine numeric rule).
    private func valuesEqual(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
        switch (lhs, rhs) {
        case (.null, .null):
            true
        case let (.string(left), .string(right)):
            left == right
        case let (.bool(left), .bool(right)):
            left == right
        case let (.int(left), .int(right)):
            left == right
        case let (.double(left), .double(right)):
            left == right
        case let (.int(left), .double(right)), let (.double(right), .int(left)):
            Double(left) == right
        case let (.array(left), .array(right)):
            left.count == right.count && zip(left, right).allSatisfy { valuesEqual($0, $1) }
        case let (.object(left), .object(right)):
            left.count == right.count && left.allSatisfy { key, value in
                right[key].map { valuesEqual(value, $0) } ?? false
            }
        default:
            false
        }
    }

    /// The wire op tags this engine's evalValueExpr distinguishes — the corpus
    /// README's own-op coverage rule: a grammar node without a fixture row
    /// fails.
    private static let ownOps: Set<String> = [
        "field", "literal", "concat", "add", "sub", "mul", "div",
        "coalesce", "lower", "upper", "trim", "cast", "now", "case"
    ]

    @Test func everyOpHasAFixtureRow() throws {
        var seen: Set<String> = []
        for row in try loadCorpus().rows {
            for expr in row.computed.values {
                guard case let .object(obj) = expr, case let .string(op) = obj["op"] else {
                    continue
                }
                seen.insert(op)
            }
        }
        for op in Self.ownOps {
            #expect(seen.contains(op), "no fixture row exercises op '\(op)'")
        }
    }

    @Test func everyFixtureRowMatchesThisEngine() throws {
        for (rowIndex, row) in try loadCorpus().rows.enumerated() {
            let table = try decodeTable(row)
            if row.expect.error == true {
                do {
                    _ = try stampComputed(table, row.inputDoc, row.now)
                    Issue.record("row \(rowIndex) (\(row.id)): expected the stamp to fail")
                } catch let error as RtDbError {
                    #expect(error.code == .badRequest, "row \(rowIndex) error code")
                }
                continue
            }
            let stamped: [String: JSONValue]
            do {
                stamped = try stampComputed(table, row.inputDoc, row.now)
            } catch {
                Issue.record("row \(rowIndex) (\(row.id)): unexpected stamp error \(error)")
                continue
            }
            for (name, expected) in row.expect.stampedValues ?? [:] {
                if expected == .null {
                    #expect(
                        stamped[name] == nil,
                        "row \(rowIndex) (\(row.id)): key '\(name)' must be absent after stamping"
                    )
                    continue
                }
                guard let got = stamped[name] else {
                    Issue.record("row \(rowIndex) (\(row.id)): missing computed key '\(name)'")
                    continue
                }
                #expect(
                    valuesEqual(got, expected),
                    "row \(rowIndex) (\(row.id)): stamped value of '\(name)' is \(got), want \(expected)"
                )
            }
        }
    }
}

/// The schema JSON object the aggregate rows' fragments decode through: the
/// same shape pushSchema parses (file scope keeps swiftlint's nesting rule
/// happy).
private struct AggregateIndexFragment: Codable {
    let name: String
    let fields: [String]
}

private struct AggregateSchemaFragment: Encodable {
    struct Table: Encodable {
        let fields: JSONValue
        let indexes: [AggregateIndexFragment]
    }

    let tables: [String: Table]
}

/// ENH-052 family 3 — aggregate-terminals spec-table runner:
/// wire-corpus/spec/aggregate-terminals.json. Each row pushes its schema
/// fragment, inserts the docs through the real write path, runs the
/// aggregate query through `query`, and asserts the bare wire value
/// (`expect.error` rows assert the failure).
struct AggregateTerminalsSpecCorpusTests {
    private struct SpecDoc: Decodable {
        let rows: [SpecRow]
    }

    private struct SpecRow: Decodable {
        let id: String
        let fields: JSONValue
        let index: IndexFragment
        let docs: [JSONValue]
        let eq: [JSONValue]
        let aggregate: JSONValue
        let expect: JSONValue
    }

    private typealias IndexFragment = AggregateIndexFragment

    /// The schema JSON object the row's fragment decodes through: the same
    /// shape pushSchema parses.
    private typealias SchemaFragment = AggregateSchemaFragment

    private struct QueryWire: Encodable {
        let table: String
        let index: String
        let eq: [JSONValue]
        let aggregate: JSONValue
    }

    private func loadCorpus() throws -> SpecDoc {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // ParRtDbClientTests
            .deletingLastPathComponent() // Tests
            .deletingLastPathComponent() // swift-client
            .deletingLastPathComponent() // repo root
            .appendingPathComponent("wire-corpus/spec/aggregate-terminals.json")
        return try JSONDecoder().decode(SpecDoc.self, from: Data(contentsOf: url))
    }

    private static let ownOps = ["sum", "avg", "min", "max", "count"]

    /// The corpus README's own-op coverage rule.
    @Test func everyAggregateOpHasAFixtureRow() throws {
        let corpus = try loadCorpus()
        var seen = Set<String>()
        for row in corpus.rows {
            if case let .object(agg) = row.aggregate {
                if case let .string(op) = agg["op"] {
                    seen.insert(op)
                }
                if case let .object(map) = agg["aggregates"] {
                    for opValue in map.values {
                        if case let .string(op) = opValue {
                            seen.insert(op)
                        }
                    }
                }
            }
        }
        for op in Self.ownOps {
            #expect(seen.contains(op), "no fixture row exercises aggregate op '\(op)'")
        }
    }

    @Test func everyRowMatchesThisEngine() throws {
        let corpus = try loadCorpus()
        for (rowIndex, row) in corpus.rows.enumerated() {
            let clock = MonotonicMs(1_700_000_000_000)
            let client = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
                now: { clock.next() },
                random: { 0 }
            ))
            let schemaData = try JSONEncoder().encode(SchemaFragment(tables: [
                "items": .init(fields: row.fields, indexes: [row.index])
            ]))
            let schema = try JSONDecoder().decode(SchemaDef.self, from: schemaData)
            try client.pushSchema(schema)

            let docs: [[String: JSONValue]] = try row.docs.map { doc in
                guard case let .object(object) = doc else {
                    struct DocShapeFailure: Error {}
                    throw DocShapeFailure()
                }
                return object
            }
            _ = try client.mutate(Transaction(steps: [.insertMany(table: "items", docs: docs)]))

            let queryData = try JSONEncoder().encode(QueryWire(
                table: "items",
                index: row.index.name,
                eq: row.eq,
                aggregate: row.aggregate
            ))
            let query = try JSONDecoder().decode(Query.self, from: queryData)

            var isErrRow = false
            if case let .object(expect) = row.expect, expect["error"] != nil {
                isErrRow = true
            }
            if isErrRow {
                #expect(throws: (any Error).self, "\(row.id): expected the aggregate to fail") {
                    _ = try client.query(query)
                }
                continue
            }
            let result = try client.query(query)
            #expect(result == row.expect, "\(rowIndex) (\(row.id)): aggregate result")
        }
    }
}

/// File-scope scalar comparison for the pagination rows (int/double cross
/// compare mirrors the stamping corpus's valuesEqual).
private func paginateValuesEqual(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case (.null, .null):
        true
    case let (.string(left), .string(right)):
        left == right
    case let (.bool(left), .bool(right)):
        left == right
    case let (.int(left), .int(right)):
        left == right
    case let (.double(left), .double(right)):
        left == right
    case let (.int(left), .double(right)), let (.double(right), .int(left)):
        Double(left) == right
    case let (.array(left), .array(right)):
        left.count == right.count && zip(left, right).allSatisfy { paginateValuesEqual($0, $1) }
    case let (.object(left), .object(right)):
        left.count == right.count && left.allSatisfy { key, value in
            right[key].map { paginateValuesEqual(value, $0) } ?? false
        }
    default:
        false
    }
}

/// ENH-052 family 4 — pagination spec-table runner:
/// wire-corpus/spec/pagination.json. Each row pushes its schema fragment,
/// inserts the docs through the real write path, encodes the RAW cursor
/// values through this engine's codec, runs the paginate query through
/// `query`, and asserts the page envelope — `nextCursorPrefix` is the
/// unbound-index prefix of the decoded minted cursor (`expect.error` rows
/// assert the failure with its code).
struct PaginationSpecCorpusTests {
    private struct SpecDoc: Decodable {
        let rows: [SpecRow]
    }

    private struct PaginateSpec: Decodable {
        let numItems: Int
        let cursor: [JSONValue]?
    }

    private struct SpecRow: Decodable {
        let id: String
        let fields: JSONValue
        let index: AggregateIndexFragment
        let docs: [JSONValue]
        let eq: [JSONValue]
        let order: String
        let paginate: PaginateSpec
        let expect: JSONValue
    }

    private struct QueryWire: Encodable {
        let table: String
        let index: String
        let eq: [JSONValue]
        let order: String
        let paginate: PaginateWire
    }

    private struct PaginateWire: Encodable {
        let numItems: Int
        let cursor: String?
    }

    private func loadCorpus() throws -> SpecDoc {
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // ParRtDbClientTests
            .deletingLastPathComponent() // Tests
            .deletingLastPathComponent() // swift-client
            .deletingLastPathComponent() // repo root
            .appendingPathComponent("wire-corpus/spec/pagination.json")
        return try JSONDecoder().decode(SpecDoc.self, from: Data(contentsOf: url))
    }

    /// The corpus README's own-coverage rule: every Order spelling and keyset
    /// value kind this engine distinguishes must be exercised by a row.
    @Test func everyOrderAndKindHasAFixtureRow() throws {
        let corpus = try loadCorpus()
        var orders = Set<String>()
        var kinds = Set<String>()
        for row in corpus.rows {
            orders.insert(row.order)
            guard case let .object(fields) = row.fields,
                  case let .object(vType) = fields["v"] ?? .object([:]),
                  case let .string(ty) = vType["type"] ?? .string("string")
            else {
                Issue.record("\(row.id): fields.v does not parse")
                continue
            }
            kinds.insert(ty)
        }
        for order in ["asc", "desc"] {
            #expect(orders.contains(order), "no fixture row exercises order '\(order)'")
        }
        for kind in ["number", "int64", "boolean", "optional"] {
            #expect(kinds.contains(kind), "no fixture row exercises value kind '\(kind)'")
        }
    }

    @Test func everyRowMatchesThisEngine() throws {
        let corpus = try loadCorpus()
        for (rowIndex, row) in corpus.rows.enumerated() {
            let clock = MonotonicMs(1_700_000_000_000)
            let client = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
                now: { clock.next() },
                random: { 0 }
            ))
            let schemaData = try JSONEncoder().encode(AggregateSchemaFragment(tables: [
                "items": .init(fields: row.fields, indexes: [row.index])
            ]))
            let schema = try JSONDecoder().decode(SchemaDef.self, from: schemaData)
            try client.pushSchema(schema)
            let docs: [[String: JSONValue]] = try row.docs.map { doc in
                guard case let .object(object) = doc else {
                    struct DocShapeFailure: Error {}
                    throw DocShapeFailure()
                }
                return object
            }
            _ = try client.mutate(Transaction(steps: [.insertMany(table: "items", docs: docs)]))
            let query = try buildQuery(row)
            if let wantCode = rejectCode(of: row.expect) {
                try assertRowRejects(query, row, wantCode, client)
                continue
            }
            let envelope = try client.query(query)
            try assertPage(envelope, row, rowIndex)
        }
    }

    private func buildQuery(_ row: SpecRow) throws -> Query {
        let queryData = try JSONEncoder().encode(QueryWire(
            table: "items",
            index: row.index.name,
            eq: row.eq,
            order: row.order,
            paginate: PaginateWire(
                numItems: row.paginate.numItems,
                cursor: row.paginate.cursor.map { encodeCursor($0) }
            )
        ))
        return try JSONDecoder().decode(Query.self, from: queryData)
    }

    private func rejectCode(of expect: JSONValue) -> String? {
        guard case let .object(expectObj) = expect,
              case let .object(err) = expectObj["error"] ?? .null,
              case let .string(code) = err["code"] ?? .null
        else {
            return nil
        }
        return code
    }

    private func assertRowRejects(
        _ query: Query,
        _ row: SpecRow,
        _ wantCode: String,
        _ client: InMemoryRtDbClient
    ) throws {
        do {
            _ = try client.query(query)
            Issue.record("\(row.id): expected the query to fail")
        } catch let error as RtDbError {
            #expect(error.code.rawValue == wantCode, "\(row.id): rejection code")
        }
    }

    private func assertPage(
        _ envelope: JSONValue,
        _ row: SpecRow,
        _ rowIndex: Int
    ) throws {
        guard case let .object(env) = envelope,
              case let .array(pageDocs) = env["docs"] ?? .array([])
        else {
            Issue.record("\(rowIndex) (\(row.id)): expected a paginated envelope")
            return
        }
        let projected = pageDocs.map { doc -> JSONValue in
            guard case let .object(map) = doc else { return doc }
            var stripped = map
            for key in ["_id", "_creationTime", "_version"] {
                stripped.removeValue(forKey: key)
            }
            return .object(stripped)
        }
        guard case let .object(expectObj) = row.expect,
              case let .array(wantDocs) = expectObj["docs"] ?? .array([])
        else {
            Issue.record("\(rowIndex) (\(row.id)): expect.docs does not parse")
            return
        }
        #expect(
            projected.count == wantDocs.count
                && zip(projected, wantDocs).allSatisfy { paginateValuesEqual($0, $1) },
            "\(rowIndex) (\(row.id)): page docs"
        )
        if expectObj["nextCursorPrefix"] == nil {
            #expect(env["nextCursor"] == nil, "\(row.id): expected no next cursor")
            return
        }
        guard case let .array(wantPrefix) = expectObj["nextCursorPrefix"] else {
            Issue.record("\(row.id): expect.nextCursorPrefix does not parse")
            return
        }
        assertMintedCursor(env, row, wantPrefix)
    }

    private func assertMintedCursor(
        _ envelope: [String: JSONValue],
        _ row: SpecRow,
        _ wantPrefix: [JSONValue]
    ) {
        guard case let .string(encoded) = envelope["nextCursor"] ?? .null else {
            Issue.record("\(row.id): expected a next cursor")
            return
        }
        guard let decoded = decodeCursor(encoded) else {
            Issue.record("\(row.id): minted cursor does not decode")
            return
        }
        #expect(decoded.count == wantPrefix.count + 2, "\(row.id): minted cursor arity")
        let prefixMatches = wantPrefix.count <= decoded.count
            && zip(decoded.prefix(wantPrefix.count), wantPrefix)
            .allSatisfy { paginateValuesEqual($0, $1) }
        #expect(prefixMatches, "\(row.id): minted cursor prefix")
        let createdAtIsNumber = switch decoded.dropLast(1).last {
        case .int, .double:
            true
        default:
            false
        }
        #expect(createdAtIsNumber, "\(row.id): created_at cursor kind")
        let idIsString = if case .string = decoded.last ?? .null {
            true
        } else {
            false
        }
        #expect(idIsString, "\(row.id): id cursor kind")
    }
}
