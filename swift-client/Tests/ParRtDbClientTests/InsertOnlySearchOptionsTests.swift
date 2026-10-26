import Foundation
@testable import ParRtDbClient
import Testing

// insertOnly table flag + search rankMode / minSimilarity wire and builder tests.

private func encoded(_ value: some Codable) throws -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    let data = try encoder.encode(value)
    return String(bytes: data, encoding: .utf8) ?? ""
}

struct InsertOnlySearchOptionsTests {
    @Test func insertOnlyOmittedWhenFalseAndRoundTripsWhenTrue() throws {
        let plain = SchemaBuilder().table("t") { $0.field("a", .string) }.build()
        #expect(try !encoded(plain).contains("insertOnly"))

        let schema = SchemaBuilder().table("t") { $0.field("a", .string).insertOnly() }.build()
        #expect(schema.tables["t"]?.insertOnly == true)
        let data = try JSONEncoder().encode(schema)
        #expect(try encoded(schema).contains("\"insertOnly\":true"))
        #expect(try JSONDecoder().decode(SchemaDef.self, from: data) == schema)
    }

    @Test func insertOnlyGateRunsBeforeUndeleteSoftDeleteCheck() throws {
        let client = InMemoryRtDbClient()
        try client.pushSchema(SchemaBuilder().table("t") { $0.field("a", .string).insertOnly() }.build())
        let results = try client.mutate(MutationBuilder().insert("t", ["a": .string("x")]).build())
        guard case let .insert(id) = results[0] else {
            Issue.record("expected insert")
            return
        }
        do {
            _ = try client.mutate(MutationBuilder().undelete("t", id).build())
            Issue.record("expected throw")
        } catch let error as RtDbError {
            #expect(error.code == .schemaViolation)
        }
    }

    @Test func searchOptionsEncodeOnlyWhenSetAndValidate() throws {
        let plain = SearchQuery(index: "i", query: "q")
        #expect(try encoded(plain) == "{\"index\":\"i\",\"query\":\"q\"}")
        let cd = SearchQuery(index: "i", query: "q", rankMode: .tsRankCd)
        #expect(try encoded(cd).contains("\"rankMode\":\"ts_rank_cd\""))
        #expect(throws: RtDbError.self) {
            try validateSearchRanking(SearchQuery(index: "i", query: "q", mode: .trgm, rankMode: .tsRank))
        }
        #expect(throws: RtDbError.self) {
            try validateSearchRanking(SearchQuery(index: "i", query: "q", minSimilarity: 0.5))
        }
        #expect(throws: RtDbError.self) {
            try validateSearchRanking(SearchQuery(index: "i", query: "q", mode: .trgm, minSimilarity: 1.5))
        }
    }
}
