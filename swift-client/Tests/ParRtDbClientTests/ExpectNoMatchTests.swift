import Foundation
@testable import ParRtDbClient
import Testing

// `expectNoMatch` (filter-absence precondition) and the `insertIf` pair — the
// swift-client mirror of rust-client/src/in_memory/tests/expect_no_match.rs.

private let past = 1
private let future: Int64 = 9_000_000_000_000_000
private let sweepMs: Int64 = 1_000_000_000_000

private func makeClient() -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    return InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
}

private func tasksSchema(softDelete: Bool = false) throws -> SchemaDef {
    try SchemaBuilder()
        .table("tasks") {
            let table = $0.field("title", .string)
                .field("updatedAt", .number)
                .index("by_title", on: ["title"])
            return softDelete ? table.softDelete() : table
        }
        .build()
}

private func titleIs(_ title: String) -> FilterExpr {
    .eq(field: "title", value: .string(title))
}

private func titleCount(_ client: InMemoryRtDbClient) throws -> Int {
    let result = try client.query(Query(table: "tasks", index: "by_title", count: true))
    guard case let .int(count) = result else {
        throw RtDbError(code: .internal, message: "expected count, got \(result)")
    }
    return Int(count)
}

private func insertTask(_ client: InMemoryRtDbClient, _ title: String, updatedAt: Int64 = 1) throws -> String {
    let results = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: ["title": .string(title), "updatedAt": .int(updatedAt)])
    ]))
    guard case let .insert(id) = results[0] else {
        throw RtDbError(code: .internal, message: "expected insert result")
    }
    return id
}

private func expectNoMatchAbort(_ body: () throws -> Void) {
    do {
        try body()
        Issue.record("expected PRECONDITION_FAILED")
    } catch let error as RtDbError {
        #expect(error.code == .preconditionFailed, "got \(error.code): \(error.message)")
        #expect(error.message.contains("filter matched an existing document"), "got: \(error.message)")
    } catch {
        Issue.record("unexpected error \(error)")
    }
}

struct ExpectNoMatchTests {
    @Test func expectNoMatchWireShape() throws {
        let step = Step.expectNoMatch(table: "tasks", filter: titleIs("a"))
        let encoded = try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(step))
        let expected: JSONValue = .object([
            "op": .string("expectNoMatch"),
            "table": .string("tasks"),
            "filter": .object([
                "op": .string("eq"), "field": .string("title"), "value": .string("a")
            ])
        ])
        #expect(encoded == expected)
        let decoded = try JSONDecoder().decode(Step.self, from: JSONEncoder().encode(expected))
        #expect(decoded == step)
    }

    @Test func expectNoMatchRejectsUnknownFields() {
        let text = #"{"op":"expectNoMatch","table":"t","filter":{"op":"exists","field":"a"},"extra":1}"#
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(Step.self, from: Data(text.utf8))
        }
    }

    @Test func builderEmitsExpectNoMatchAndInsertIfPair() throws {
        let doc: [String: JSONValue] = ["title": .string("dup")]
        let filter = titleIs("dup")
        let direct = try MutationBuilder().expectNoMatch("tasks", filter: filter).build()
        #expect(direct.steps == [.expectNoMatch(table: "tasks", filter: filter)])
        let pair = try MutationBuilder().insertIf("tasks", filter: filter, doc).build()
        #expect(pair.steps == [
            .expectNoMatch(table: "tasks", filter: filter),
            .insert(table: "tasks", doc: doc)
        ])
    }

    @Test func insertIfInsertsWhenNoRowMatches() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema())
        let txn = try MutationBuilder()
            .insertIf("tasks", filter: titleIs("a"), ["title": .string("a"), "updatedAt": .int(1)])
            .build()
        let results = try client.mutate(txn)
        #expect(results.count == 2)
        #expect(results[0] == .null)
        guard case .insert = results[1] else {
            Issue.record("expected insert result")
            return
        }
        #expect(try titleCount(client) == 1)
    }

    @Test func insertIfAbortsAndRollsBackWhenARowMatches() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema())
        _ = try insertTask(client, "a")
        let txn = try MutationBuilder()
            .insert("tasks", ["title": .string("other"), "updatedAt": .int(1)])
            .insertIf("tasks", filter: titleIs("a"), ["title": .string("a"), "updatedAt": .int(1)])
            .build()
        expectNoMatchAbort { _ = try client.mutate(txn) }
        #expect(try titleCount(client) == 1)
    }

    @Test func expectNoMatchSeesEarlierStepsInTheSameTxn() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema())
        let txn = Transaction(steps: [
            .insert(table: "tasks", doc: ["title": .string("a"), "updatedAt": .int(1)]),
            .expectNoMatch(table: "tasks", filter: titleIs("a"))
        ])
        expectNoMatchAbort { _ = try client.mutate(txn) }
        #expect(try titleCount(client) == 0)
    }

    @Test func expectNoMatchIgnoresSoftDeletedRows() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema(softDelete: true))
        let id = try insertTask(client, "a")
        _ = try client.mutate(Transaction(steps: [.delete(table: "tasks", id: id)]))
        let results = try client.mutate(Transaction(steps: [
            .expectNoMatch(table: "tasks", filter: titleIs("a"))
        ]))
        #expect(results == [.null])
    }

    @Test func expectNoMatchAcceptsOlderThan() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema())
        _ = try insertTask(client, "fresh", updatedAt: future)
        let older = FilterExpr.olderThan(field: "updatedAt", ms: sweepMs)
        let passes = try client.mutate(Transaction(steps: [
            .expectNoMatch(table: "tasks", filter: older)
        ]))
        #expect(passes == [.null])
        _ = try insertTask(client, "stale", updatedAt: Int64(past))
        expectNoMatchAbort {
            _ = try client.mutate(Transaction(steps: [
                .expectNoMatch(table: "tasks", filter: older)
            ]))
        }
    }

    @Test func expectNoMatchCountsTowardTheByQueryStepCap() throws {
        let client = makeClient()
        try client.pushSchema(tasksSchema())
        let steps = (0 ... InMemoryLimits.maxByQueryStepsPerTxn).map { _ in
            Step.expectNoMatch(table: "tasks", filter: titleIs("zzz"))
        }
        do {
            _ = try client.mutate(Transaction(steps: steps))
            Issue.record("expected by-query step cap rejection")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
        }
    }

    @Test func optimisticProjectionTreatsExpectNoMatchAsNoDataEffect() throws {
        let query = try TableQuery("tasks").collect().build()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1),
                     "title": .string("x")])
        ])
        // A bare precondition changes nothing: skip.
        let bare = Transaction(steps: [.expectNoMatch(table: "tasks", filter: titleIs("x"))])
        #expect(projectOptimisticUpdate(query: query, last: last, txn: bare, now: 99) == .skip)
        // insertIf projects only its insert; the precondition is ignored.
        let pair = try MutationBuilder()
            .insertIf("tasks", filter: titleIs("y"), ["title": .string("y")])
            .build()
        guard case let .overlaid(.array(rows)) = projectOptimisticUpdate(
            query: query, last: last, txn: pair, now: 99
        ) else {
            Issue.record("expected overlay of the insert")
            return
        }
        #expect(rows.count == 2)
        // A precondition on another table never disturbs a projection.
        let other = Transaction(steps: [.expectNoMatch(table: "other", filter: titleIs("x"))])
        #expect(projectOptimisticUpdate(query: query, last: last, txn: other, now: 99) == .skip)
    }
}
