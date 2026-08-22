import Foundation
@testable import ParRtDbClient
import Testing

// Execution-time-relative `olderThan` predicates in by-query steps — the
// swift-client mirror of server/tests/relative_filter_test.rs: the by-query
// acceptance boundary (read filters reject), the deterministic match margins
// (OLD 1 is below any cutoff for centuries, FUTURE 9e15 above it, so the
// clock's exact value never matters), the int64 decimal-string wire form, and
// the push surfaces that refuse the op (authorize, partial-index `where`,
// computed `case` whens).

/// Below `now − SWEEP_MS` for centuries (epoch-ms today is ~1.8e12; the
/// cutoff is ~0.7e12 and rising by 1/year).
private let old = 1
/// 9e15 — above `now − 0` effectively forever; f64-exact, within i64.
private let future: Int64 = 9_000_000_000_000_000
private let sweepMs: Int64 = 1_000_000_000_000

/// Monotonic clock starting at the corpus's pinned epoch — later stamps are
/// strictly greater, and the margins make the exact tick irrelevant.
private func sweepClient() -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    return InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
}

private func numberSchema() throws -> SchemaDef {
    try SchemaBuilder()
        .table("tasks") {
            $0.field("title", .string)
                .field("updatedAt", .number)
                .index("by_title", on: ["title"])
        }
        .build()
}

/// `updatedAt` as int64 and indexed, so the exact i64 comparison path runs
/// (the typed bigint column on the server) over the decimal-string wire form.
private func int64IndexedSchema() throws -> SchemaDef {
    try SchemaBuilder()
        .table("tasks") {
            $0.field("title", .string)
                .field("updatedAt", .int64)
                .index("by_title", on: ["title"])
                .index("by_updatedAt", on: ["updatedAt"])
        }
        .build()
}

private func seed(
    _ client: InMemoryRtDbClient, _ title: String, _ updatedAt: JSONValue
) throws {
    _ = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: ["title": .string(title), "updatedAt": updatedAt])
    ]))
}

private func countTitles(_ client: InMemoryRtDbClient, _ title: String) throws -> Int {
    let result = try client.query(
        Query(table: "tasks", index: "by_title", eq: [.string(title)], count: true)
    )
    guard case let .int(count) = result else {
        throw RtDbError(code: .internal, message: "expected count, got \(result)")
    }
    return Int(count)
}

private func olderThan(_ field: String, _ ms: Int64) -> FilterExpr {
    .olderThan(field: field, ms: ms)
}

private func expectError(
    code: ErrorCode, _ fragment: String, _ body: () throws -> Void
) throws {
    do {
        try body()
        Issue.record("expected failure containing '\(fragment)'")
    } catch let error as RtDbError {
        #expect(error.code == code, "expected \(code), got \(error.code): \(error.message)")
        #expect(error.message.contains(fragment), "got: \(error.message)")
    }
}

struct RelativeFilterTests {
    @Test func patchByQueryOlderThanPatchesOldRowsOnly() throws {
        let client = sweepClient()
        try client.pushSchema(numberSchema())
        try seed(client, "old", .int(Int64(old)))
        try seed(client, "future", .int(future))

        let results = try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "tasks",
                filter: olderThan("updatedAt", sweepMs),
                patch: ["title": .string("swept")],
                limit: nil
            )
        ]))
        guard case let .patchByQuery(patched, truncated) = results[0] else {
            Issue.record("expected patchByQuery result")
            return
        }
        #expect(patched == 1)
        #expect(!truncated)
        #expect(try countTitles(client, "swept") == 1)
        #expect(try countTitles(client, "future") == 1)
    }

    @Test func deleteByQueryOlderThanDeletesOldRowsOnly() throws {
        let client = sweepClient()
        try client.pushSchema(numberSchema())
        try seed(client, "old", .int(Int64(old)))
        try seed(client, "future", .int(future))

        let results = try client.mutate(Transaction(steps: [
            .deleteByQuery(table: "tasks", filter: olderThan("updatedAt", sweepMs), limit: nil)
        ]))
        guard case let .deleteByQuery(deleted, truncated) = results[0] else {
            Issue.record("expected deleteByQuery result")
            return
        }
        #expect(deleted == 1)
        #expect(!truncated)
        #expect(try countTitles(client, "old") == 0)
        #expect(try countTitles(client, "future") == 1)
    }

    @Test func patchByQueryOlderThanTakesTheInt64ColumnPath() throws {
        let client = sweepClient()
        try client.pushSchema(int64IndexedSchema())
        // int64 wire form is a decimal string.
        try seed(client, "old", .string(String(old)))
        try seed(client, "future", .string(String(future)))

        let results = try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "tasks",
                filter: olderThan("updatedAt", sweepMs),
                patch: ["title": .string("swept")],
                limit: nil
            )
        ]))
        guard case let .patchByQuery(patched, _) = results[0] else {
            Issue.record("expected patchByQuery result")
            return
        }
        #expect(patched == 1)
        #expect(try countTitles(client, "future") == 1)
    }

    /// `and`/`or`/`not` compose around the leaf: the recursion carries both
    /// the by-query admission and the execution clock through every level.
    @Test func olderThanComposesInsideAndOrNot() throws {
        let client = sweepClient()
        try client.pushSchema(numberSchema())
        try seed(client, "old-keep", .int(Int64(old)))
        try seed(client, "old-skip", .int(Int64(old)))
        try seed(client, "future", .int(future))

        let results = try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "tasks",
                filter: .and(exprs: [
                    .or(exprs: [
                        olderThan("updatedAt", sweepMs),
                        .eq(field: "title", value: .string("future"))
                    ]),
                    .not(expr: .eq(field: "title", value: .string("old-skip")))
                ]),
                patch: ["title": .string("swept")],
                limit: nil
            )
        ]))
        guard case let .patchByQuery(patched, _) = results[0] else {
            Issue.record("expected patchByQuery result")
            return
        }
        // old-keep and future pass the or; old-skip is excluded by the not.
        #expect(patched == 2)
        #expect(try countTitles(client, "old-skip") == 1)
        #expect(try countTitles(client, "swept") == 2)
    }

    @Test func readQueryFilterOlderThanIsRejected() throws {
        let client = sweepClient()
        try client.pushSchema(numberSchema())
        try expectError(code: .badRequest, "only allowed in patchByQuery/deleteByQuery") {
            _ = try client.query(
                Query(table: "tasks", filter: olderThan("updatedAt", sweepMs))
            )
        }
    }

    @Test func patchByQueryOlderThanRejectsNonNumericFieldAndNegativeMs() throws {
        let client = sweepClient()
        let stringUpdatedAt = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .string)
                    .index("by_title", on: ["title"])
            }
            .build()
        try client.pushSchema(stringUpdatedAt)

        // A string-typed updatedAt and a negative window are both BAD_REQUEST
        // at the by-query validation chokepoint, in the server's order.
        try expectError(code: .badRequest, "must be a number or int64") {
            _ = try client.mutate(Transaction(steps: [
                .patchByQuery(
                    table: "tasks",
                    filter: olderThan("updatedAt", sweepMs),
                    patch: ["title": .string("swept")],
                    limit: nil
                )
            ]))
        }
        try expectError(code: .badRequest, "ms must be >= 0") {
            _ = try client.mutate(Transaction(steps: [
                .patchByQuery(
                    table: "tasks",
                    filter: olderThan("title", -1),
                    patch: ["title": .string("swept")],
                    limit: nil
                )
            ]))
        }
    }

    /// The push surfaces that refuse the op: `authorize` predicates
    /// (SCHEMA_VIOLATION via validate_structure), partial-index `where`
    /// predicates (rejected at DDL compile with their own message), and
    /// computed `case` whens (BAD_REQUEST, the computed rules' code).
    @Test func pushSurfacesRejectOlderThan() throws {
        let withAuthorize = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .number)
                    .index("by_title", on: ["title"])
                    .authorize(olderThan("updatedAt", sweepMs))
            }
            .build()
        try expectError(code: .schemaViolation, "only allowed in patchByQuery/deleteByQuery") {
            try sweepClient().pushSchema(withAuthorize)
        }

        let withWhere = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .number)
                    .index("by_title", on: ["title"])
                    .index("by_updatedAt", on: ["updatedAt"])
                    .whereClause(olderThan("updatedAt", sweepMs))
            }
            .build()
        try expectError(code: .badRequest, "not allowed in a partial-index predicate") {
            try sweepClient().pushSchema(withWhere)
        }

        let withCaseWhen = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .number)
                    .field("label", .string)
                    .index("by_title", on: ["title"])
                    .computed(
                        "label",
                        .caseExpr(
                            whens: [
                                CaseWhen(
                                    when: olderThan("updatedAt", sweepMs),
                                    then: .literal(value: .string("old"))
                                )
                            ],
                            otherwise: .literal(value: .string("fresh"))
                        )
                    )
            }
            .build()
        try expectError(code: .badRequest, "only allowed in patchByQuery/deleteByQuery") {
            try sweepClient().pushSchema(withCaseWhen)
        }
    }
}
