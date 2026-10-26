import Foundation
@testable import ParRtDbClient
import Testing

/// Aggregate subscriptions over the in-memory engine: the committer-side
/// re-run + canonical diff fires only when an aggregated value changes —
/// mirrors server/tests/ws_test.rs::subscribe_aggregate_pushes_on_change_over_sync.
struct AggregateSubscriptionTests {
    @Test func groupedMultiOpAggregateSubscriptionReRunsAndDiffs() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("orders") {
                    $0.field("status", .string)
                        .field("total", .number)
                        .field("note", .optional(.string))
                        .index("by_status_total", on: ["status", "total"])
                }
                .build()
        )
        let ids = try client.mutate(Transaction(steps: [
            .insert(table: "orders", doc: ["status": .string("open"), "total": .int(10)]),
            .insert(table: "orders", doc: ["status": .string("open"), "total": .int(5)]),
            .insert(table: "orders", doc: ["status": .string("done"), "total": .int(7)])
        ])).map(insertId)
        let box = ExpectBox()
        let query = Query(
            table: "orders", index: "by_status_total",
            aggregate: AggregateSpec(
                aggregates: ["total": .sum, "n": .count], groupBy: .fields(["status"])
            )
        )
        let unsub = try client.subscribe(query) { value in
            box.values.append(value)
        }
        let initial = JSONValue.array([group("done", total: 7, count: 1), group("open", total: 15, count: 2)])
        #expect(box.values == [initial])
        // An in-window insert changes the aggregate and fires with new values.
        try client.mutate(Transaction(steps: [
            .insert(table: "orders", doc: ["status": .string("open"), "total": .int(20)])
        ]))
        let updated = JSONValue.array([group("done", total: 7, count: 1), group("open", total: 35, count: 3)])
        #expect(box.values == [initial, updated])
        // A patch of a non-aggregated field leaves the result equal: no fire.
        try client.mutate(Transaction(steps: [
            .patch(table: "orders", id: ids[0], fields: ["note": .string("hi")])
        ]))
        #expect(box.values == [initial, updated])
        unsub()
    }

    private func deterministicClient() -> InMemoryRtDbClient {
        InMemoryRtDbClient(
            options: InMemoryRtDbClientOptions(now: { 1_700_000_000_000 }, random: { 0 })
        )
    }

    private func group(_ status: String, total: Int64, count: Int64) -> JSONValue {
        .object([
            "keys": .array([.string(status)]),
            "values": .object(["total": .int(total), "n": .int(count)])
        ])
    }

    private func insertId(_ result: StepResult) throws -> String {
        guard case let .insert(id) = result else {
            throw RtDbError(code: .internal, message: "expected insert result")
        }
        return id
    }

    private final class ExpectBox {
        var values: [JSONValue] = []
    }
}
