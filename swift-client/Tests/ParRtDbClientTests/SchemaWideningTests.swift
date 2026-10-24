import Foundation
@testable import ParRtDbClient
import Testing

/// Nested optional-object widening (array<object> element shapes) — the port
/// of the server's `is_object_shape_widening` acceptance + rejection matrix
/// (server/tests/schema_evolution_test.rs). Kept in its own suite to stay
/// under InMemoryTests' type-body-length budget.
struct SchemaWideningTests {
    private func deterministicClient() -> InMemoryRtDbClient {
        InMemoryRtDbClient(
            options: InMemoryRtDbClientOptions(now: { 1_700_000_000_000 }, random: { 0 })
        )
    }

    /// One table whose `versions` field is `array<object>` with a required
    /// `tag`; `size` is optional only after the widening — the par-artifacts
    /// consumer shape. Mirrors the server integration test.
    private func versionsSchema(withSize: Bool) throws -> SchemaDef {
        try SchemaBuilder()
            .table("artifacts") {
                $0.field("name", .string)
                    .field(
                        "versions",
                        .array(.object(fields: withSize
                                ? ["tag": .string, "size": .optional(.number)]
                                : ["tag": .string]))
                    )
            }
            .build()
    }

    @Test func pushAcceptsAddingOptionalFieldInsideArrayObjectElements() throws {
        let client = deterministicClient()
        try client.pushSchema(versionsSchema(withSize: false))
        // Old row: elements lack `size`.
        let first = try client.mutate(Transaction(steps: [
            .insert(
                table: "artifacts",
                doc: ["name": .string("a"), "versions": .array([.object(["tag": .string("v1")])])]
            )
        ]))
        guard case let .insert(oldId) = first[0] else {
            Issue.record("expected insert result")
            return
        }
        // Widened push: additive, non-destructive.
        try client.pushSchema(versionsSchema(withSize: true))
        // Old row still validates (optional-absent passes) and reads back.
        let oldDoc = try client.query(Query(table: "artifacts", get: oldId))
        #expect(oldDoc.objectValue?["versions"] == .array([.object(["tag": .string("v1")])]))
        // New docs validate with and without the new field.
        let second = try client.mutate(Transaction(steps: [
            .insert(
                table: "artifacts",
                doc: [
                    "name": .string("b"),
                    "versions": .array([.object(["tag": .string("v2"), "size": .int(1024)])])
                ]
            ),
            .insert(
                table: "artifacts",
                doc: ["name": .string("c"), "versions": .array([.object(["tag": .string("v2")])])]
            )
        ]))
        let expectedVersions: [JSONValue] = [
            .array([.object(["tag": .string("v2"), "size": .int(1024)])]),
            .array([.object(["tag": .string("v2")])])
        ]
        for (result, expected) in zip(second, expectedVersions) {
            guard case let .insert(id) = result else {
                Issue.record("expected insert result")
                return
            }
            let doc = try client.query(Query(table: "artifacts", get: id))
            #expect(doc.objectValue?["versions"] == expected)
        }
    }

    /// `expectPushError` in InMemoryTests pushes to a FRESH client (a first
    /// push never runs destructive-change detection), so the rejection tests
    /// push the base schema on a shared client and assert the second push
    /// fails on it.
    private func expectSecondPushError(
        _ client: InMemoryRtDbClient, _ schema: SchemaDef, _ fragment: String
    ) throws {
        do {
            try client.pushSchema(schema)
            Issue.record("expected second pushSchema failure containing '\(fragment)'")
        } catch let error as RtDbError {
            #expect(error.message.contains(fragment))
        }
    }

    @Test func pushRejectsRemovingAnElementObjectField() throws {
        let client = deterministicClient()
        try client.pushSchema(versionsSchema(withSize: true))
        try expectSecondPushError(
            client, versionsSchema(withSize: false),
            "changed type of field 'artifacts.versions'"
        )
    }

    @Test func pushRejectsRetypingAnElementObjectField() throws {
        let base = try SchemaBuilder()
            .table("things") {
                $0.field("v", .array(.object(fields: ["size": .optional(.number)])))
            }
            .build()
        let swapped = try SchemaBuilder()
            .table("things") {
                $0.field("v", .array(.object(fields: ["size": .optional(.string)])))
            }
            .build()
        let client = deterministicClient()
        try client.pushSchema(base)
        try expectSecondPushError(client, swapped, "changed type of field 'things.v'")
    }

    @Test func pushRejectsAddingRequiredFieldInsideArrayObjectElements() throws {
        let base = try SchemaBuilder()
            .table("things") {
                $0.field("v", .array(.object(fields: ["tag": .string])))
            }
            .build()
        let widened = try SchemaBuilder()
            .table("things") {
                $0.field("v", .array(.object(fields: ["tag": .string, "size": .number])))
            }
            .build()
        let client = deterministicClient()
        try client.pushSchema(base)
        try expectSecondPushError(client, widened, "changed type of field 'things.v'")
    }

    @Test func pushRejectsNarrowingUnionInsideElementObject() throws {
        let base = try SchemaBuilder()
            .table("things") {
                $0.field(
                    "v",
                    .array(.object(fields: [
                        "state": .union([
                            .literal(.string("a")), .literal(.string("b")), .literal(.string("c"))
                        ])
                    ]))
                )
            }
            .build()
        let narrowed = try SchemaBuilder()
            .table("things") {
                $0.field(
                    "v",
                    .array(.object(fields: [
                        "state": .union([.literal(.string("a")), .literal(.string("b"))])
                    ]))
                )
            }
            .build()
        let client = deterministicClient()
        try client.pushSchema(base)
        try expectSecondPushError(client, narrowed, "changed type of field 'things.v'")
    }
}
