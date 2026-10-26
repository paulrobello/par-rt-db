import Foundation
@testable import ParRtDbClient
import Testing

// `insertMany` / `upsertMany` bulk steps — swift mirror of the rust-client
// in-memory bulk tests.

private func makeClient() throws -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    let client = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
    try client.pushSchema(
        SchemaBuilder()
            .table("items") {
                $0.field("sku", .string).field("qty", .number).index("by_sku", on: ["sku"])
            }
            .build()
    )
    return client
}

private func docs(_ range: Range<Int>) -> [[String: JSONValue]] {
    range.map { ["sku": .string("s\($0)"), "qty": .int(Int64($0))] }
}

private func count(_ client: InMemoryRtDbClient) throws -> Int {
    guard case let .int(total) = try client.query(Query(table: "items", index: "by_sku", count: true)) else {
        return -1
    }
    return Int(total)
}

struct InsertManyTests {
    @Test func insertsPastStepCapAndRollsBackOnInvalidDoc() throws {
        let client = try makeClient()
        let results = try client.mutate(MutationBuilder().insertMany("items", docs(0 ..< 40)).build())
        guard case let .insertMany(ids) = results[0] else {
            Issue.record("expected insertMany result")
            return
        }
        #expect(ids.count == 40)

        var bad = docs(100 ..< 120)
        bad[7]["qty"] = .string("oops")
        do {
            _ = try client.mutate(MutationBuilder().insertMany("items", bad).build())
            Issue.record("expected SCHEMA_VIOLATION")
        } catch let error as RtDbError {
            #expect(error.code == .schemaViolation)
        }
        #expect(try count(client) == 40)

        do {
            _ = try client.mutate(MutationBuilder().insertMany("items", docs(0 ..< 1001)).build())
            Issue.record("expected BAD_REQUEST")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
        }
    }

    @Test func upsertManyPatchesExistingAndInsertsNew() throws {
        let client = try makeClient()
        _ = try client.mutate(MutationBuilder().insertMany("items", docs(0 ..< 2)).build())
        let results = try client.mutate(
            MutationBuilder().upsertMany("items", index: "by_sku", [
                ["sku": .string("s1"), "qty": .int(99)],
                ["sku": .string("s9"), "qty": .int(9)]
            ]).build()
        )
        guard case let .upsertMany(ids, inserted) = results[0] else {
            Issue.record("expected upsertMany result")
            return
        }
        #expect(ids.count == 2)
        #expect(inserted == [false, true])
        #expect(try count(client) == 3)

        do {
            _ = try client.mutate(
                MutationBuilder().upsertMany("items", index: "by_sku", [["qty": .int(1)]]).build()
            )
            Issue.record("expected BAD_REQUEST")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
        }
    }

    @Test func wireShapesRoundTrip() throws {
        let txn = try MutationBuilder()
            .insertMany("items", docs(0 ..< 1))
            .upsertMany("items", index: "by_sku", docs(0 ..< 1))
            .build()
        let data = try JSONEncoder().encode(txn)
        #expect(try JSONDecoder().decode(Transaction.self, from: data) == txn)
        let json = String(bytes: data, encoding: .utf8) ?? ""
        #expect(json.contains("\"insertMany\"") && json.contains("\"upsertMany\""))
    }
}
