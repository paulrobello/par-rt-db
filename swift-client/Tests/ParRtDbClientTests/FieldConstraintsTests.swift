import Foundation
@testable import ParRtDbClient
import Testing

// Declarative per-field write constraints (the server's
// `TableDef::constraints`): `.constraints()` builder wire shape plus
// in-memory push-time and write-path enforcement. Cross-engine agreement is
// pinned by the `constraint-*` / `error-constraint-*` semantics-corpus
// cases; these tests pin the DSL builder surface and the enforcement seams,
// mirroring ts-client/tests/field-constraints.test.ts.

private func tasksSchema() -> SchemaDef {
    SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .field("score", .number)
                .field("counter", .int64)
                .field("note", .optional(.string))
                .index("by_name", on: ["name"])
                .constraints([
                    "score": FieldConstraints(min: 1, max: 10),
                    "counter": FieldConstraints(min: 1, max: 100),
                    "name": FieldConstraints(minLength: 2, maxLength: 5, pattern: "^[a-z]+$")
                ])
        }
        .build()
}

private func makeClient() -> InMemoryRtDbClient {
    InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { 1_700_000_000_000 },
        random: { 0 }
    ))
}

/// Asserts the closure throws an RtDbError whose code is SCHEMA_VIOLATION
/// and whose message contains every given part.
private struct ConstraintExpectationFailure: Error {}

private func expectConstraintError(
    _ messageParts: [String], _ body: () throws -> Void
) throws {
    do {
        try body()
    } catch let error as RtDbError {
        #expect(error.code == .schemaViolation)
        for part in messageParts {
            #expect(error.message.contains(part), "message '\(error.message)' lacks '\(part)'")
        }
        return
    }
    Issue.record("expected a SCHEMA_VIOLATION, but the call succeeded")
    throw ConstraintExpectationFailure()
}

// MARK: - DSL builder wire shape

@Test func constraintsRoundTripOnTheWireOmittedWhenAbsent() throws {
    let table = try #require(tasksSchema().tables["tasks"])
    #expect(table.constraints == [
        "score": FieldConstraints(min: 1, max: 10),
        "counter": FieldConstraints(min: 1, max: 100),
        "name": FieldConstraints(minLength: 2, maxLength: 5, pattern: "^[a-z]+$")
    ])

    // Round-trips through Codable unchanged...
    let encoded = try JSONEncoder().encode(tasksSchema())
    let decoded = try JSONDecoder().decode(SchemaDef.self, from: encoded)
    #expect(decoded == tasksSchema())

    // ...and the key is dropped entirely when the table declares none.
    let plain = SchemaBuilder()
        .table("plain") { $0.field("name", .string) }
        .build()
    let plainJson = try JSONDecoder().decode(
        JSONValue.self, from: JSONEncoder().encode(plain)
    )
    #expect(plainJson.objectValue?["plain"]?.objectValue?["constraints"] == nil)
}

// MARK: - In-memory push-time validation

@Test func pushRejectsMisappliedConstraintNamingTheField() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .index("by_name", on: ["name"])
                .constraints(["name": FieldConstraints(min: 1)])
        }
        .build()
    try expectConstraintError(["'name'", "number/int64"]) {
        try makeClient().pushSchema(schema)
    }
}

@Test func pushRejectsNonIntegerInt64Bound() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("counter", .int64)
                .constraints(["counter": FieldConstraints(min: 0.5)])
        }
        .build()
    try expectConstraintError(["'counter'", "must be an integer on an int64 field"]) {
        try makeClient().pushSchema(schema)
    }
}

@Test func pushRejectsStringConstraintOnNonStringField() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("score", .number)
                .constraints(["score": FieldConstraints(minLength: 2)])
        }
        .build()
    try expectConstraintError(["'score'", "'minLength' is legal only on string fields"]) {
        try makeClient().pushSchema(schema)
    }
}

@Test func pushRejectsCrossedBounds() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("score", .number)
                .constraints(["score": FieldConstraints(min: 10, max: 1)])
        }
        .build()
    try expectConstraintError(["'min' exceeds 'max'"]) {
        try makeClient().pushSchema(schema)
    }
}

@Test func pushRejectsUnknownConstraintsKey() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .constraints(["ghost": FieldConstraints(pattern: "^a")])
        }
        .build()
    try expectConstraintError(["constraints key 'ghost'", "tasks"]) {
        try makeClient().pushSchema(schema)
    }
}

@Test func pushRejectsInvalidRegexNamingTheField() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .index("by_name", on: ["name"])
                .constraints(["name": FieldConstraints(pattern: "([a-z")])
        }
        .build()
    try expectConstraintError(["invalid pattern"]) {
        try makeClient().pushSchema(schema)
    }
}

// MARK: - In-memory write-path enforcement

@Test func acceptsInclusiveBoundaryValues() throws {
    let client = makeClient()
    try client.pushSchema(tasksSchema())
    _ = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: [
            "name": .string("aa"), "score": .int(1), "counter": .string("1")
        ]),
        .insert(table: "tasks", doc: [
            "name": .string("abcde"), "score": .double(10), "counter": .string("100")
        ])
    ]))
}

@Test func failsInsertBelowMinWithRollback() throws {
    let client = makeClient()
    try client.pushSchema(tasksSchema())
    try expectConstraintError(["'min' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: [
                "name": .string("good"), "score": .int(5), "counter": .string("1")
            ]),
            .insert(table: "tasks", doc: [
                "name": .string("bad"), "score": .double(0.5), "counter": .string("1")
            ])
        ]))
    }
    // The failed txn rolled back its earlier step.
    let rows = try client.query(Query(table: "tasks"))
    guard case let .array(items) = rows else {
        Issue.record("expected an array result")
        return
    }
    #expect(items.isEmpty)
}

@Test func enforcesLengthAndPatternOnStrings() throws {
    let client = makeClient()
    try client.pushSchema(tasksSchema())
    try expectConstraintError(["'minLength' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: [
                "name": .string("a"), "score": .int(5), "counter": .string("1")
            ])
        ]))
    }
    try expectConstraintError(["'maxLength' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: [
                "name": .string("abcdef"), "score": .int(5), "counter": .string("1")
            ])
        ]))
    }
    try expectConstraintError(["'pattern' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: [
                "name": .string("ABC"), "score": .int(5), "counter": .string("1")
            ])
        ]))
    }
}

@Test func enforcesConstraintsThroughPatchReplaceUpsertPatchByQuery() throws {
    let client = makeClient()
    try client.pushSchema(tasksSchema())
    let results = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: [
            "name": .string("ok"), "score": .int(5), "counter": .string("1")
        ])
    ]))
    guard case let .insert(id) = results[0] else {
        Issue.record("expected an insert result")
        return
    }

    try expectConstraintError(["'max' constraint"]) {
        try client.mutate(Transaction(steps: [
            .patch(table: "tasks", id: id, fields: ["score": .int(50)])
        ]))
    }
    try expectConstraintError(["'max' constraint"]) {
        try client.mutate(Transaction(steps: [
            .replace(table: "tasks", id: id, doc: [
                "name": .string("ab"), "score": .int(99), "counter": .string("2")
            ])
        ]))
    }
    try expectConstraintError(["'max' constraint"]) {
        try client.mutate(Transaction(steps: [
            .upsert(
                table: "tasks", index: "by_name", eq: [.string("ok")],
                insert: ["name": .string("ok"), "score": .int(5), "counter": .string("1")],
                patch: ["counter": .string("101")]
            )
        ]))
    }
    try expectConstraintError(["'max' constraint"]) {
        try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "tasks",
                filter: .eq(field: "name", value: .string("ok")),
                patch: ["score": .int(99)],
                limit: nil
            )
        ]))
    }

    // Nothing survived: the stored doc is untouched.
    let doc = try client.query(Query(table: "tasks", get: id))
    #expect(doc.objectValue?["name"] == .string("ok"))
    #expect(doc.objectValue?["score"] == .int(5))
    #expect(doc.objectValue?["counter"] == .string("1"))
    #expect(doc.objectValue?["_version"] == .int(1))
}

@Test func neverViolatesOnNullOrAbsentValues() throws {
    let client = makeClient()
    try client.pushSchema(tasksSchema())
    _ = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: [
            "name": .string("ok"), "score": .int(5), "counter": .string("1"), "note": .null
        ])
    ]))
}

@Test func lengthCountsUnicodeScalarsNotGraphemeClusters() throws {
    // The server counts chars() (Unicode scalar values): a flag emoji is one
    // grapheme cluster but two scalars, so maxLength 1 rejects one flag and
    // maxLength 2 accepts it — String.count (graphemes) would see 1 either
    // way.
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .constraints(["name": FieldConstraints(maxLength: 1)])
        }
        .build()
    let client = makeClient()
    try client.pushSchema(schema)
    try expectConstraintError(["'maxLength' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: ["name": .string("\u{1F1FA}\u{1F1F8}")])
        ]))
    }
    let wider = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .constraints(["name": FieldConstraints(maxLength: 2)])
        }
        .build()
    let client2 = makeClient()
    try client2.pushSchema(wider)
    _ = try client2.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: ["name": .string("\u{1F1FA}\u{1F1F8}")])
    ]))
}

@Test func patternIsAnUnanchoredSearch() throws {
    let schema = SchemaBuilder()
        .table("tasks") {
            $0.field("name", .string)
                .constraints(["name": FieldConstraints(pattern: "b")])
        }
        .build()
    let client = makeClient()
    try client.pushSchema(schema)
    // No anchors: "abc" matches because 'b' appears anywhere.
    _ = try client.mutate(Transaction(steps: [
        .insert(table: "tasks", doc: ["name": .string("abc")])
    ]))
    try expectConstraintError(["'pattern' constraint"]) {
        try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: ["name": .string("acd")])
        ]))
    }
}
