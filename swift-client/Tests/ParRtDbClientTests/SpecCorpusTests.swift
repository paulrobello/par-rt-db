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
