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
