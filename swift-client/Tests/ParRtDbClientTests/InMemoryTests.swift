import Foundation
@testable import ParRtDbClient
import Testing

/// Compact smoke suite for the in-memory engine (InMemoryEngine/
/// InMemoryQuery/InMemoryValidate/InMemoryMigrate — the port of
/// ts-client/src/in_memory/). Deliberately NOT the ts/rust engine test
/// suites: the semantics/golden corpus runners are the real verification;
/// these tests pin the engine's load-bearing behaviors — deterministic id/
/// time minting, the terminal matrix, txn caps and results, soft delete,
/// migrations, schedules, storage, subscriptions, and presence.
struct InMemoryTests {
    // MARK: Fixtures

    private let pinnedNow: Int64 = 1_700_000_000_000
    private let firstId = "018bcfe5680070000000000000000001"
    private let secondId = "018bcfe5680070000000000000000002"

    private func deterministicClient() -> InMemoryRtDbClient {
        InMemoryRtDbClient(
            options: InMemoryRtDbClientOptions(now: { 1_700_000_000_000 }, random: { 0 })
        )
    }

    private func itemsSchema() throws -> SchemaDef {
        try SchemaBuilder()
            .table("items") {
                $0.field("title", .string)
                    .field("n", .number)
                    .field("tag", .optional(.string))
                    .index("by_n", on: ["n"])
                    .index("by_tag", on: ["tag"])
                    .index("by_tag_n", on: ["tag", "n"])
            }
            .build()
    }

    private func seededEngine() throws -> (InMemoryRtDbClient, [String]) {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let first = try client.mutate(Transaction(steps: [
            .insert(table: "items", doc: ["title": .string("a"), "n": .int(3), "tag": .string("x")]),
            .insert(table: "items", doc: ["title": .string("b"), "n": .int(1), "tag": .string("x")]),
            .insert(table: "items", doc: ["title": .string("c"), "n": .int(2)])
        ])).map { result -> String in
            guard case let .insert(id) = result else {
                throw RtDbError(code: .internal, message: "expected insert result")
            }
            return id
        }
        return (client, first)
    }

    private func array(_ value: JSONValue) throws -> [JSONValue] {
        guard case let .array(items) = value else {
            throw RtDbError(code: .internal, message: "not an array")
        }
        return items
    }

    private func count(_ value: JSONValue) throws -> Int {
        guard let number = value.doubleValue else {
            throw RtDbError(code: .internal, message: "not a count")
        }
        return Int(number)
    }

    // MARK: Determinism

    @Test func deterministicIdAndCreationTimeMinting() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "items", doc: ["title": .string("a"), "n": .int(1)]),
            .insert(table: "items", doc: ["title": .string("b"), "n": .int(2)])
        ]))
        guard case let .insert(first) = results[0], case let .insert(second) = results[1] else {
            Issue.record("expected insert step results")
            return
        }
        #expect(first == firstId)
        #expect(second == secondId)
        let doc = try client.query(Query(table: "items", get: first))
        #expect(doc.objectValue?["_creationTime"] == .int(pinnedNow))
        #expect(doc.objectValue?["_version"] == .int(1))
    }

    @Test func connectionIdDefaultsToCounterToken() {
        let client = InMemoryRtDbClient()
        #expect(client.connectionId == "c1")
        let named = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(connectionId: "sess-9"))
        #expect(named.connectionId == "sess-9")
    }

    // MARK: push + insert + collect

    @Test func pushInsertCollectOrderedByIndex() throws {
        let (client, _) = try seededEngine()
        let ascending = try client.query(Query(table: "items", index: "by_n", order: .asc))
        guard case let .array(ascendingDocs) = ascending else {
            Issue.record("collect did not return an array")
            return
        }
        #expect(ascendingDocs.map(\.objectValue?["title"]) == [.string("b"), .string("c"), .string("a")])

        let descending = try client.query(Query(table: "items", index: "by_n", order: .desc))
        guard case let .array(descendingDocs) = descending else {
            Issue.record("collect did not return an array")
            return
        }
        #expect(descendingDocs.map(\.objectValue?["title"]) == [.string("a"), .string("c"), .string("b")])
    }

    @Test func insertAppliesSchemaDefaults() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("tasks") {
                    $0.field("title", .string)
                        .field("done", .optional(.boolean))
                        .field("priority", .number)
                        .defaults(["done": .bool(false), "priority": .int(5)])
                }
                .build()
        )
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: ["title": .string("t1")])
        ]))
        guard case let .insert(id) = results[0] else {
            Issue.record("expected insert result")
            return
        }
        let doc = try client.query(Query(table: "tasks", get: id))
        #expect(doc.objectValue?["done"] == .bool(false))
        #expect(doc.objectValue?["priority"] == .int(5))
    }

    // MARK: updatedAtField (FM-36)

    /// Epoch-ms floor: any real stamp is far past this, so a stamped value is
    /// distinguishable from every client-supplied literal used below.
    private let ancient: Int64 = 1_000_000_000_000

    /// Client with a monotonically increasing clock (each `now` call +1), so
    /// every later stamp is strictly greater than every earlier one — the
    /// engine-level stand-in for the server tests' `tick()` sleeps.
    private func monotonicClient() -> InMemoryRtDbClient {
        let clock = MonotonicMs(1_700_000_000_000)
        return InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
            now: { clock.next() },
            random: { 0 }
        ))
    }

    private func updatedAtSchema(_ fieldType: FieldType) throws -> SchemaDef {
        try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", fieldType)
                    .index("by_title", on: ["title"])
                    .updatedAtField("updatedAt")
            }
            .build()
    }

    private func insertTask(
        _ client: InMemoryRtDbClient, _ doc: [String: JSONValue]
    ) throws -> String {
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "tasks", doc: doc)
        ]))
        guard case let .insert(id) = results[0] else {
            throw RtDbError(code: .internal, message: "expected insert result")
        }
        return id
    }

    private func stampedNumber(_ doc: JSONValue) -> Int64 {
        guard case let .int(stamped)? = doc.objectValue?["updatedAt"] else {
            Issue.record("expected numeric updatedAt stamp")
            return -1
        }
        return stamped
    }

    @Test func pushRejectsUndeclaredUpdatedAtField() throws {
        let schema = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .number)
                    .index("by_title", on: ["title"])
                    .updatedAtField("nope")
            }
            .build()
        try expectPushError(schema, "updatedAtField 'nope' is not a declared field")
    }

    @Test func pushRejectsNonNumericUpdatedAtField() throws {
        let schema = try updatedAtSchema(.string)
        try expectPushError(schema, "updatedAtField 'updatedAt' must be a number or bigint field")
    }

    @Test func pushRejectsUpdatedAtFieldMatchingTtlField() throws {
        let schema = try SchemaBuilder()
            .table("sessions") {
                $0.field("token", .string)
                    .field("expiresAt", .number)
                    .index("by_token", on: ["token"])
                    .index("by_expiresAt", on: ["expiresAt"])
                    .ttl("expiresAt")
                    .updatedAtField("expiresAt")
            }
            .build()
        try expectPushError(schema, "must differ from ttl.field")
    }

    private func expectPushError(_ schema: SchemaDef, _ fragment: String) throws {
        do {
            try deterministicClient().pushSchema(schema)
            Issue.record("expected pushSchema failure containing '\(fragment)'")
        } catch let error as RtDbError {
            #expect(error.message.contains(fragment))
        }
    }

    @Test func insertStampsUpdatedAtOverwritingClientValue() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.number))
        let id = try insertTask(client, ["title": .string("A"), "updatedAt": .int(123)])
        let doc = try client.query(Query(table: "tasks", get: id))
        let stamped = stampedNumber(doc)
        #expect(stamped > ancient)
    }

    @Test func insertStampsInt64UpdatedAtAsDecimalString() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.int64))
        let id = try insertTask(client, ["title": .string("A")])
        let doc = try client.query(Query(table: "tasks", get: id))
        // int64 fields hold decimal strings end to end (wire convention)
        guard case let .string(text) = doc.objectValue?["updatedAt"], let stamped = Int64(text)
        else {
            Issue.record("expected int64 updatedAt stamp as a decimal string")
            return
        }
        #expect(stamped > ancient)
    }

    @Test func patchRestampsOverwritingClientValue() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.number))
        let id = try insertTask(client, ["title": .string("A")])
        let first = try stampedNumber(client.query(Query(table: "tasks", get: id)))

        try client.mutate(Transaction(steps: [
            .patch(table: "tasks", id: id, fields: ["title": .string("B"), "updatedAt": .int(1)])
        ]))
        let doc = try client.query(Query(table: "tasks", get: id))
        #expect(stampedNumber(doc) > first)
        #expect(doc.objectValue?["title"] == .string("B"))
    }

    @Test func replaceRestamps() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.number))
        let id = try insertTask(client, ["title": .string("A")])
        let first = try stampedNumber(client.query(Query(table: "tasks", get: id)))

        try client.mutate(Transaction(steps: [
            .replace(table: "tasks", id: id, doc: ["title": .string("A2"), "updatedAt": .int(7)])
        ]))
        #expect(try stampedNumber(client.query(Query(table: "tasks", get: id))) > first)
    }

    @Test func upsertInsertStampsAndUpdateRestamps() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.number))
        let results = try client.mutate(Transaction(steps: [
            .upsert(
                table: "tasks", index: "by_title", eq: [.string("A")],
                insert: ["title": .string("A"), "updatedAt": .int(9)],
                patch: [:]
            )
        ]))
        guard case let .upsert(id, inserted) = results[0], inserted else {
            Issue.record("expected upsert-insert result")
            return
        }
        let first = try stampedNumber(client.query(Query(table: "tasks", get: id)))
        #expect(first > ancient)

        try client.mutate(Transaction(steps: [
            .upsert(
                table: "tasks", index: "by_title", eq: [.string("A")],
                insert: ["title": .string("A")],
                patch: ["title": .string("A3"), "updatedAt": .int(5)]
            )
        ]))
        #expect(try stampedNumber(client.query(Query(table: "tasks", get: id))) > first)
    }

    @Test func patchByQueryRestamps() throws {
        let client = monotonicClient()
        try client.pushSchema(updatedAtSchema(.number))
        let id = try insertTask(client, ["title": .string("A")])
        let first = try stampedNumber(client.query(Query(table: "tasks", get: id)))

        try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "tasks",
                filter: .eq(field: "title", value: .string("A")),
                patch: ["updatedAt": .int(3)],
                limit: nil
            )
        ]))
        #expect(try stampedNumber(client.query(Query(table: "tasks", get: id))) > first)
    }

    @Test func cascadeSetNullRestampsChild() throws {
        let client = monotonicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("parents") {
                    $0.field("name", .string)
                        .index("by_name", on: ["name"])
                }
                .table("children") {
                    $0.field("parentId", .optional(.id("parents").onDelete(.setNull)))
                        .field("title", .string)
                        .field("updatedAt", .number)
                        .index("by_parentId", on: ["parentId"])
                        .updatedAtField("updatedAt")
                }
                .build()
        )
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "parents", doc: ["name": .string("P")])
        ]))
        guard case let .insert(parent) = results[0] else {
            Issue.record("expected parent insert result")
            return
        }
        let childResults = try client.mutate(Transaction(steps: [
            .insert(
                table: "children",
                doc: ["parentId": .string(parent), "title": .string("C")]
            )
        ]))
        guard case let .insert(child) = childResults[0] else {
            Issue.record("expected child insert result")
            return
        }
        let childQuery = { try client.query(Query(table: "children", get: child)) }
        let first = try stampedNumber(childQuery())

        try client.mutate(Transaction(steps: [
            .delete(table: "parents", id: parent)
        ]))
        let doc = try childQuery()
        #expect(doc.objectValue?["parentId"] == nil)
        #expect(stampedNumber(doc) > first)
    }

    @Test func updatedAtStampWinsOverDefaultsEntry() throws {
        let client = monotonicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("tasks") {
                    $0.field("title", .string)
                        .field("updatedAt", .number)
                        .index("by_title", on: ["title"])
                        .updatedAtField("updatedAt")
                        .defaults(["updatedAt": .int(12345)])
                }
                .build()
        )
        let id = try insertTask(client, ["title": .string("A")])
        let stamped = try stampedNumber(client.query(Query(table: "tasks", get: id)))
        #expect(stamped > ancient)
        #expect(stamped != 12345)
    }

    // MARK: autoIncrementField (FM-37)

    private func counterSchema() throws -> SchemaDef {
        try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .int64)
                    .index("by_title", on: ["title"])
                    .autoIncrementField("num")
            }
            .build()
    }

    private func insertTicket(
        _ client: InMemoryRtDbClient, _ doc: [String: JSONValue]
    ) throws -> String {
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "tickets", doc: doc)
        ]))
        guard case let .insert(id) = results[0] else {
            throw RtDbError(code: .internal, message: "expected insert result")
        }
        return id
    }

    /// The stored `num` counter of a tickets doc — a decimal string (the
    /// int64 wire convention).
    private func storedCounter(_ doc: JSONValue) -> String? {
        guard case let .string(text)? = doc.objectValue?["num"] else { return nil }
        return text
    }

    /// Asserts `body` throws a BAD_REQUEST whose message contains `fragment`.
    private func expectBadRequest(_ fragment: String, _ body: () throws -> Void) throws {
        do {
            try body()
            Issue.record("expected BAD_REQUEST containing '\(fragment)'")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message.contains(fragment))
        }
    }

    @Test func pushRejectsUndeclaredAutoIncrementField() throws {
        let schema = try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .int64)
                    .index("by_title", on: ["title"])
                    .autoIncrementField("nope")
            }
            .build()
        try expectPushError(schema, "autoIncrementField 'nope' is not a declared field")
    }

    @Test func pushRejectsNonInt64AutoIncrementField() throws {
        let numberField = try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .number)
                    .index("by_title", on: ["title"])
                    .autoIncrementField("num")
            }
            .build()
        try expectPushError(numberField, "autoIncrementField 'num' must be an int64 field")
        let optionalField = try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .optional(.int64))
                    .autoIncrementField("num")
            }
            .build()
        try expectPushError(optionalField, "autoIncrementField 'num' must be an int64 field")
    }

    @Test func pushRejectsCounterCollidingWithTtlOrUpdatedAt() throws {
        let ttlCollision = try SchemaBuilder()
            .table("sessions") {
                $0.field("token", .string)
                    .field("expiresAt", .int64)
                    .index("by_token", on: ["token"])
                    .index("by_expiresAt", on: ["expiresAt"])
                    .ttl("expiresAt")
                    .autoIncrementField("expiresAt")
            }
            .build()
        try expectPushError(ttlCollision, "must differ from ttl.field")
        let updatedAtCollision = try SchemaBuilder()
            .table("tasks") {
                $0.field("title", .string)
                    .field("updatedAt", .int64)
                    .index("by_title", on: ["title"])
                    .updatedAtField("updatedAt")
                    .autoIncrementField("updatedAt")
            }
            .build()
        try expectPushError(updatedAtCollision, "must differ from updatedAtField")
    }

    @Test func insertAssignsSequentialValuesOverwritingClientValue() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        let first = try insertTicket(client, ["title": .string("A"), "num": .string("999")])
        let second = try insertTicket(client, ["title": .string("B")])
        let third = try insertTicket(client, ["title": .string("C"), "num": .string("5")])
        #expect(try storedCounter(client.query(Query(table: "tickets", get: first))) == "1")
        #expect(try storedCounter(client.query(Query(table: "tickets", get: second))) == "2")
        #expect(try storedCounter(client.query(Query(table: "tickets", get: third))) == "3")
    }

    @Test func autoIncrementStampWinsOverDefaultsEntry() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("tickets") {
                    $0.field("title", .string)
                        .field("num", .int64)
                        .index("by_title", on: ["title"])
                        .autoIncrementField("num")
                        .defaults(["num": .string("42")])
                }
                .build()
        )
        let id = try insertTicket(client, ["title": .string("A")])
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")
    }

    @Test func patchCannotChangeTheCounter() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        let id = try insertTicket(client, ["title": .string("A")])

        try expectBadRequest("autoIncrementField 'num' cannot be changed") {
            try client.mutate(Transaction(steps: [
                .patch(table: "tickets", id: id, fields: ["num": .string("99")])
            ]))
        }
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")

        // Round-tripping the same value is allowed.
        try client.mutate(Transaction(steps: [
            .patch(table: "tickets", id: id, fields: ["num": .string("1"), "title": .string("A2")])
        ]))
        let doc = try client.query(Query(table: "tickets", get: id))
        #expect(storedCounter(doc) == "1")
        #expect(doc.objectValue?["title"] == .string("A2"))
    }

    @Test func replacePreservesOrRejectsTheCounter() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        let id = try insertTicket(client, ["title": .string("A")])

        // A replace that omits the field keeps the stored value (it validates
        // as a complete document only because the engine fills it back in).
        try client.mutate(Transaction(steps: [
            .replace(table: "tickets", id: id, doc: ["title": .string("A2")])
        ]))
        var doc = try client.query(Query(table: "tickets", get: id))
        #expect(storedCounter(doc) == "1")
        #expect(doc.objectValue?["title"] == .string("A2"))

        // A replace that changes the value is rejected.
        try expectBadRequest("autoIncrementField 'num' cannot be changed") {
            try client.mutate(Transaction(steps: [
                .replace(
                    table: "tickets", id: id,
                    doc: ["title": .string("A3"), "num": .string("5")]
                )
            ]))
        }

        // Round-tripping the stored value works.
        try client.mutate(Transaction(steps: [
            .replace(
                table: "tickets", id: id,
                doc: ["title": .string("A4"), "num": .string("1")]
            )
        ]))
        doc = try client.query(Query(table: "tickets", get: id))
        #expect(storedCounter(doc) == "1")
        #expect(doc.objectValue?["title"] == .string("A4"))
    }

    @Test func upsertInsertAssignsAndUpdatePreserves() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        let results = try client.mutate(Transaction(steps: [
            .upsert(
                table: "tickets", index: "by_title", eq: [.string("A")],
                insert: ["title": .string("A"), "num": .string("999")],
                patch: [:]
            )
        ]))
        guard case let .upsert(id, inserted) = results[0], inserted else {
            Issue.record("expected upsert-insert result")
            return
        }
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")

        // Update branch: carrying the stored value is fine...
        try client.mutate(Transaction(steps: [
            .upsert(
                table: "tickets", index: "by_title", eq: [.string("A")],
                insert: [:],
                patch: ["title": .string("A2"), "num": .string("1")]
            )
        ]))
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")

        // ...changing it is not.
        try expectBadRequest("autoIncrementField 'num' cannot be changed") {
            try client.mutate(Transaction(steps: [
                .upsert(
                    table: "tickets", index: "by_title", eq: [.string("A2")],
                    insert: [:],
                    patch: ["num": .string("42")]
                )
            ]))
        }
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")
    }

    @Test func patchByQueryCannotChangeTheCounter() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        let id = try insertTicket(client, ["title": .string("A")])

        try expectBadRequest("autoIncrementField 'num' cannot be changed") {
            try client.mutate(Transaction(steps: [
                .patchByQuery(
                    table: "tickets",
                    filter: .eq(field: "title", value: .string("A")),
                    patch: ["num": .string("99")],
                    limit: nil
                )
            ]))
        }
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "1")
    }

    @Test func declarationAddedToPopulatedTableRepositionsPastMax() throws {
        let client = deterministicClient()
        // v1: plain int64 field, client-supplied values (no counter yet).
        let v1 = try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .int64)
                    .index("by_title", on: ["title"])
            }
            .build()
        try client.pushSchema(v1)
        _ = try insertTicket(client, ["title": .string("t1"), "num": .string("41")])
        _ = try insertTicket(client, ["title": .string("t2"), "num": .string("7")])

        // v2: same schema plus the declaration — additive push.
        try client.pushSchema(counterSchema())
        let id = try insertTicket(client, ["title": .string("new")])
        #expect(
            try storedCounter(client.query(Query(table: "tickets", get: id))) == "42",
            "the counter is repositioned past the stored max, not restarted at 1"
        )
    }

    @Test func rePushDoesNotDisturbTheCounter() throws {
        let client = deterministicClient()
        try client.pushSchema(counterSchema())
        _ = try insertTicket(client, ["title": .string("A")])
        _ = try insertTicket(client, ["title": .string("B")])

        // An unrelated additive push (new field) must not reposition anything.
        let evolved = try SchemaBuilder()
            .table("tickets") {
                $0.field("title", .string)
                    .field("num", .int64)
                    .field("owner", .optional(.string))
                    .index("by_title", on: ["title"])
                    .autoIncrementField("num")
            }
            .build()
        try client.pushSchema(evolved)
        let id = try insertTicket(client, ["title": .string("C")])
        #expect(try storedCounter(client.query(Query(table: "tickets", get: id))) == "3")
    }

    @Test func insertRejectsReservedAndUnknownFields() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: [
                .insert(table: "items", doc: ["_id": .string("x"), "n": .int(1)])
            ]))
        }
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: [
                .insert(table: "items", doc: ["bogus": .string("x"), "n": .int(1)])
            ]))
        }
    }

    @Test func getTerminalMergesSystemFieldsAndNullsForMissing() throws {
        let (client, ids) = try seededEngine()
        let doc = try client.query(Query(table: "items", get: ids[0]))
        #expect(doc.objectValue?.keys.sorted() == ["_creationTime", "_id", "_version", "n", "tag", "title"])
        let missing = try client.query(Query(table: "items", get: "ffffffffffffffffffffffffffffff"))
        #expect(missing == .null)
    }

    // MARK: Terminals

    @Test func countTerminalCountsMatchingSet() throws {
        let (client, _) = try seededEngine()
        let all = try client.query(Query(table: "items", count: true))
        #expect(try count(all) == 3)
        let tagged = try client.query(
            Query(table: "items", index: "by_tag", eq: [.string("x")], count: true)
        )
        #expect(try count(tagged) == 2)
    }

    @Test func distinctTerminalDeduplicatesAndSortsNullLast() throws {
        let (client, _) = try seededEngine()
        let distinct = try client.query(Query(table: "items", index: "by_tag", distinct: true))
        #expect(distinct == .array([.string("x"), .null]))
    }

    @Test func aggregateSumAndGroupBy() throws {
        let (client, _) = try seededEngine()
        let sum = try client.query(
            Query(table: "items", index: "by_n", aggregate: AggregateSpec(op: .sum))
        )
        #expect(sum == .int(6))
        let grouped = try client.query(Query(
            table: "items", index: "by_tag_n",
            aggregate: AggregateSpec(op: .sum, groupBy: true)
        ))
        // Group by the first index field (tag), sum the second (n): the x
        // group sums 3+1, the null group 2; nulls sort last, like Postgres.
        #expect(grouped == .array([
            .object(["key": .string("x"), "value": .int(4)]),
            .object(["key": .null, "value": .int(2)])
        ]))
    }

    @Test func paginateCursorRoundTrips() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        try client.mutate(Transaction(steps: (0 ..< 5).map { index in
            .insert(table: "items", doc: ["title": .string("t\(index)"), "n": .int(Int64(index))])
        }))
        let pageOne = try client.query(Query(
            table: "items", index: "by_n", order: .asc,
            paginate: Paginate(numItems: 2)
        ))
        guard case let .object(firstPage) = pageOne,
              case let .array(firstDocs)? = firstPage["docs"],
              let firstCursor = firstPage["nextCursor"]?.stringValue
        else {
            Issue.record("first page malformed")
            return
        }
        #expect(firstDocs.map(\.objectValue?["title"]) == [.string("t0"), .string("t1")])
        let pageTwo = try client.query(Query(
            table: "items", index: "by_n", order: .asc,
            paginate: Paginate(cursor: firstCursor, numItems: 2)
        ))
        guard case let .object(secondPage) = pageTwo,
              case let .array(secondDocs)? = secondPage["docs"]
        else {
            Issue.record("second page malformed")
            return
        }
        #expect(secondDocs.map(\.objectValue?["title"]) == [.string("t2"), .string("t3")])
        // Final page: last row, no next cursor.
        guard case let .object(thirdPage) = try client.query(Query(
            table: "items", index: "by_n", order: .asc,
            paginate: Paginate(cursor: secondPage["nextCursor"]?.stringValue, numItems: 2)
        )) else {
            Issue.record("third page malformed")
            return
        }
        #expect(try array(thirdPage["docs"] ?? .null).count == 1)
        #expect(thirdPage["nextCursor"] == nil)
    }

    @Test func filterRangeOnIndexNarrows() throws {
        let (client, _) = try seededEngine()
        let result = try client.query(Query(
            table: "items", index: "by_n", gte: .int(2), filter: .eq(field: "tag", value: .string("x"))
        ))
        guard case let .array(docs) = result else {
            Issue.record("expected array")
            return
        }
        #expect(docs.count == 1)
        #expect(docs[0].objectValue?["title"] == .string("a"))
    }

    @Test func uniqueTerminalThrowsOnMultipleMatches() throws {
        let (client, _) = try seededEngine()
        #expect(throws: RtDbError.self) {
            _ = try client.query(Query(table: "items", unique: true))
        }
        let one = try client.query(Query(
            table: "items", index: "by_n", eq: [.int(2)], unique: true
        ))
        #expect(one.objectValue?["title"] == .string("c"))
    }

    @Test func firstAndTakeLimitResults() throws {
        let (client, _) = try seededEngine()
        let first = try client.query(Query(table: "items", index: "by_n", first: true))
        #expect(first.objectValue?["title"] == .string("b"))
        let taken = try client.query(Query(table: "items", index: "by_n", order: .asc, take: 2))
        #expect(try array(taken).count == 2)
    }

    // MARK: Transactions

    @Test func txnReturnsOneResultPerStep() throws {
        let (client, ids) = try seededEngine()
        let results = try client.mutate(Transaction(steps: [
            .expectVersion(table: "items", id: ids[0], version: 1),
            .patch(table: "items", id: ids[0], fields: ["n": .int(10)])
        ]))
        #expect(results == [.null, .null])
        let doc = try client.query(Query(table: "items", get: ids[0]))
        #expect(doc.objectValue?["n"] == .int(10))
        #expect(doc.objectValue?["_version"] == .int(2))
    }

    @Test func txnRollsBackAtomicallyOnFailure() throws {
        let (client, ids) = try seededEngine()
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: [
                .patch(table: "items", id: ids[0], fields: ["n": .int(99)]),
                .expectVersion(table: "items", id: ids[0], version: 42)
            ]))
        }
        let doc = try client.query(Query(table: "items", get: ids[0]))
        #expect(doc.objectValue?["n"] == .int(3))
        #expect(doc.objectValue?["_version"] == .int(1))
    }

    @Test func txnEnforcesStepCap() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let steps = (0 ... InMemoryLimits.maxSteps).map { _ in
            Step.expectAbsent(table: "items", index: "by_n", eq: [.int(1)])
        }
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: steps))
        }
    }

    @Test func txnEnforcesByQueryStepCap() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let steps = (0 ... InMemoryLimits.maxByQueryStepsPerTxn).map { _ in
            Step.patchByQuery(
                table: "items", filter: .eq(field: "n", value: .int(1)),
                patch: ["n": .int(2)], limit: nil
            )
        }
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: steps))
        }
    }

    @Test func txnEnforcesWorstCaseAffectedRows() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        // 11 deleteByQuery steps x default limit 1000 = 11,000 > 10,000.
        let steps = (0 ..< 11).map { _ in
            Step.deleteByQuery(table: "items", filter: .eq(field: "n", value: .int(1)), limit: nil)
        }
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: steps))
        }
        #expect(worstCaseAffected(Transaction(steps: steps)) == 11000)
    }

    @Test func upsertInsertsThenPatches() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let inserted = try client.mutate(Transaction(steps: [
            .upsert(
                table: "items", index: "by_n", eq: [.int(7)],
                insert: ["title": .string("u"), "n": .int(7)],
                patch: ["title": .string("patched")]
            )
        ]))
        #expect(inserted == [.upsert(id: firstId, inserted: true)])
        let patched = try client.mutate(Transaction(steps: [
            .upsert(
                table: "items", index: "by_n", eq: [.int(7)],
                insert: ["title": .string("u"), "n": .int(7)],
                patch: ["title": .string("patched")]
            )
        ]))
        #expect(patched == [.upsert(id: firstId, inserted: false)])
        let doc = try client.query(Query(table: "items", get: firstId))
        #expect(doc.objectValue?["title"] == .string("patched"))
    }

    @Test func patchByQueryReportsTruncation() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        try client.mutate(Transaction(steps: (0 ..< 5).map { index in
            .insert(table: "items", doc: ["title": .string("t\(index)"), "n": .int(1), "tag": .string("x")])
        }))
        let results = try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "items", filter: .eq(field: "tag", value: .string("x")),
                patch: ["n": .int(9)], limit: 2
            )
        ]))
        #expect(results == [.patchByQuery(patched: 2, truncated: true)])
    }

    @Test func uniqueIndexRejectsDuplicateKeys() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("users") {
                    $0.field("email", .string)
                        .index("by_email", on: ["email"])
                        .unique()
                }
                .build()
        )
        try client.mutate(Transaction(steps: [
            .insert(table: "users", doc: ["email": .string("a@b.c")])
        ]))
        #expect(throws: RtDbError.self) {
            try client.mutate(Transaction(steps: [
                .insert(table: "users", doc: ["email": .string("a@b.c")])
            ]))
        }
    }

    // MARK: Soft delete

    @Test func softDeleteStampsAndHidesFromEveryRead() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("docs") {
                    $0.field("body", .string)
                        .field("n", .number)
                        .index("by_n", on: ["n"])
                        .softDelete()
                }
                .build()
        )
        let id = try client.mutate(Transaction(steps: [
            .insert(table: "docs", doc: ["body": .string("x"), "n": .int(1)])
        ])).first.map { result -> String in
            guard case let .insert(value) = result else {
                throw RtDbError(code: .internal, message: "expected insert")
            }
            return value
        } ?? ""
        try client.mutate(Transaction(steps: [.delete(table: "docs", id: id)]))
        #expect(try client.query(Query(table: "docs", get: id)) == .null)
        #expect(try count(client.query(Query(table: "docs", count: true))) == 0)
        // Stamped rows are absent to eq-lookup too: upsert inserts a new row.
        let upserted = try client.mutate(Transaction(steps: [
            .upsert(
                table: "docs", index: "by_n", eq: [.int(1)],
                insert: ["body": .string("fresh"), "n": .int(1)],
                patch: ["body": .string("patched")]
            )
        ]))
        #expect(upserted == [.upsert(id: secondId, inserted: true)])
    }

    @Test func undeleteRestoresSoftDeletedRow() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("docs") {
                    $0.field("body", .string)
                        .field("n", .number)
                        .index("by_n", on: ["n"])
                        .softDelete()
                }
                .build()
        )
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "docs", doc: ["body": .string("x"), "n": .int(1)]),
            .delete(table: "docs", id: firstId),
            .undelete(table: "docs", id: firstId)
        ]))
        #expect(results == [.insert(id: firstId), .null, .null])
        let doc = try client.query(Query(table: "docs", get: firstId))
        #expect(doc.objectValue?["body"] == .string("x"))
        #expect(doc.objectValue?["_version"] == .int(3))
    }

    // MARK: Migrations

    @Test func migrateRenameFieldMovesDataAndRetargetsIndex() throws {
        let (client, _) = try seededEngine()
        let result = try client.migrate(MigrateRequest(directives: [
            .renameField(table: "items", from: "n", to: "count")
        ]))
        #expect(result.applied)
        #expect(result.directives == [DirectiveReport(op: "renameField", affectedRows: 3)])
        // The index KEEPS its name but its field retargets: ordering by "by_n"
        // now orders by the renamed "count" field.
        let ordered = try client.query(Query(table: "items", index: "by_n", order: .asc))
        #expect(try array(ordered).map(\.objectValue?["title"]) == [.string("b"), .string("c"), .string("a")])
        let doc = try client.query(Query(table: "items", get: firstId))
        #expect(doc.objectValue?["count"] == .int(3))
        #expect(doc.objectValue?["n"] == nil)
    }

    @Test func migrateChangeTypeCoercesValues() throws {
        let (client, _) = try seededEngine()
        let result = try client.migrate(MigrateRequest(directives: [
            .changeType(table: "items", field: "n", to: .string, cast: .toString, default: nil)
        ]))
        #expect(result.directives == [DirectiveReport(op: "changeType", affectedRows: 3)])
        let doc = try client.query(Query(table: "items", get: firstId))
        #expect(doc.objectValue?["n"] == .string("3"))
    }

    @Test func migrateDryRunValidatesButCommitsNothing() throws {
        let (client, _) = try seededEngine()
        let result = try client.migrate(MigrateRequest(directives: [
            .renameField(table: "items", from: "n", to: "count")
        ], dryRun: true))
        #expect(!result.applied)
        #expect(result.directives.first?.affectedRows == 3)
        // The live schema is unchanged: the old index still resolves.
        let ordered = try client.query(Query(table: "items", index: "by_n", order: .asc))
        #expect(try array(ordered).count == 3)
    }

    // MARK: Schedules

    @Test func tickFiresDueScheduleExactlyOnce() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let scheduleId = try client.schedule(
            Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])]),
            when: .afterMs(ms: 1000)
        )
        // now is pinned to 1_700_000_000_000; the job is due at +1000.
        _ = try client.tick(nowMs: pinnedNow + 999)
        #expect(try count(client.query(Query(table: "items", count: true))) == 0)
        _ = try client.tick(nowMs: pinnedNow + 1000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
        // One-shot: a later tick does not fire it again, and the fired job is
        // removed from the schedule list.
        _ = try client.tick(nowMs: pinnedNow + 999_999)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
        #expect(client.listSchedules().isEmpty)
    }

    @Test func pausedScheduleDoesNotFireUntilResumed() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let id = try client.schedule(
            Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])]),
            when: .afterMs(ms: 0)
        )
        #expect(client.pauseSchedule(id))
        _ = try client.tick(nowMs: pinnedNow + 10)
        #expect(try count(client.query(Query(table: "items", count: true))) == 0)
        #expect(client.resumeSchedule(id))
        _ = try client.tick(nowMs: pinnedNow + 20)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
    }

    @Test func intervalScheduleFiresEveryIntervalAndRearmsFromFireTime() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let id = try client.schedule(
            Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])]),
            when: .interval(everyMs: 1000)
        )
        // Initial due is one full interval out; nothing fires before it.
        _ = try client.tick(nowMs: pinnedNow + 999)
        #expect(try count(client.query(Query(table: "items", count: true))) == 0)
        _ = try client.tick(nowMs: pinnedNow + 1000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
        // Recurring: re-armed one interval from each fire, it fires again and
        // the job stays listed with its everyMs exposed.
        _ = try client.tick(nowMs: pinnedNow + 1999)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
        _ = try client.tick(nowMs: pinnedNow + 2000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 2)
        let job = try #require(client.listSchedules().first { $0.id == id })
        #expect(job.kind == .interval)
        #expect(job.everyMs == 1000)
        #expect(job.dueAt == pinnedNow + 3000)
        #expect(job.firedCount == 2)
    }

    @Test func intervalScheduleSkipsMissedWindowsOnClockJump() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        _ = try client.schedule(
            Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])]),
            when: .interval(everyMs: 1000)
        )
        // A big clock jump lands 10 intervals past due: exactly one fire
        // (never a backfill burst), re-armed a full interval from the fire.
        _ = try client.tick(nowMs: pinnedNow + 10000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
        #expect(client.listSchedules().first?.dueAt == pinnedNow + 11000)
    }

    @Test func intervalScheduleResumeShiftsDueAtWithoutBackfill() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let id = try client.schedule(
            Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])]),
            when: .interval(everyMs: 1000)
        )
        #expect(client.pauseSchedule(id))
        // Several windows elapse while paused with no fire.
        _ = try client.tick(nowMs: pinnedNow + 9000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 0)
        #expect(client.resumeSchedule(id))
        // Resume re-arms one full interval from the resume clock (nowFn is
        // pinned), not the stale pre-pause dueAt.
        #expect(client.listSchedules().first?.dueAt == pinnedNow + 1000)
        _ = try client.tick(nowMs: pinnedNow + 1000)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
    }

    @Test func intervalScheduleValidatesEveryMs() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let txn = Transaction(steps: [.insert(table: "items", doc: ["title": .string("s"), "n": .int(1)])])
        for bad: Int64 in [0, -5] {
            do {
                _ = try client.schedule(txn, when: .interval(everyMs: bad))
                Issue.record("expected non-positive everyMs \(bad) to be rejected")
            } catch let error as RtDbError {
                #expect(error.code == .badRequest)
                #expect(error.message == "everyMs must be positive")
            }
        }
        do {
            _ = try client.schedule(txn, when: .interval(everyMs: InMemoryLimits.maxEveryMs + 1))
            Issue.record("expected over-cap everyMs to be rejected")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "everyMs must be at most \(InMemoryLimits.maxEveryMs)")
        }
        // The cap itself is accepted, and the Schedule step path validates
        // too (a BAD_REQUEST step aborts the whole txn — no job is written).
        _ = try client.schedule(txn, when: .interval(everyMs: InMemoryLimits.maxEveryMs))
        do {
            _ = try client.mutate(Transaction(steps: [
                .schedule(when: .interval(everyMs: 0), txn: Transaction(steps: []))
            ]))
            Issue.record("expected the schedule step to reject a non-positive everyMs")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
        }
        #expect(client.listSchedules().count == 1)
    }

    @Test func intervalScheduleRearmsAfterFailedFire() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        // A patch of a missing document throws NOT_FOUND: the fire fails but
        // a recurring interval re-arms instead of dying.
        let id = try client.schedule(
            Transaction(steps: [.patch(table: "items", id: "ffffffffffffffffffffffffffffff", fields: ["n": .int(2)])]),
            when: .interval(everyMs: 1000)
        )
        _ = try client.tick(nowMs: pinnedNow + 1000)
        let job = try #require(client.listSchedules().first { $0.id == id })
        #expect(job.status == .error)
        #expect(job.lastError != nil)
        #expect(job.dueAt == pinnedNow + 2000)
        // The errored interval still fires on its next window — recurring
        // kinds keep going after a failure.
        _ = try client.tick(nowMs: pinnedNow + 2000)
        let after = try #require(client.listSchedules().first { $0.id == id })
        #expect(after.dueAt == pinnedNow + 3000)
    }

    // MARK: Workflows

    @Test func workflowAdvancesToSuccessOnTick() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let info = try client.startWorkflow(WorkflowSpec(name: "w", steps: [
            WorkflowStepSpec(txn: Transaction(steps: [
                .insert(table: "items", doc: ["title": .string("step1"), "n": .int(1)])
            ]))
        ]))
        #expect(info.status == .pending)
        _ = try client.tick(nowMs: pinnedNow)
        let full = try client.getWorkflow(info.id)
        #expect(full.info.status == .success)
        #expect(full.stepOutcomes.count == 1)
        #expect(full.stepOutcomes.first?.status == .success)
        #expect(try count(client.query(Query(table: "items", count: true))) == 1)
    }

    @Test func awaitSignalParksThenDeliversPayload() throws {
        let client = deterministicClient()
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 60000)
        ]))
        // First tick parks: waiting + visibility columns, gate one timeout out.
        _ = try client.tick(nowMs: pinnedNow)
        let parked = try client.getWorkflow(info.id).info
        #expect(parked.status == .waiting)
        #expect(parked.waitingFor == "approve")
        #expect(parked.waitedSince == pinnedNow)
        #expect(parked.sleepUntil == pinnedNow + 60000)
        // Delivery flips the run due; the next tick consumes the payload as a
        // success outcome carrying it verbatim, then the run finishes (last
        // step) with the wait columns cleared.
        try client.signalWorkflow(info.id, name: "approve", payload: .object(["v": .int(2)]))
        _ = try client.tick(nowMs: pinnedNow + 1)
        let full = try client.getWorkflow(info.id)
        #expect(full.info.status == .success)
        #expect(full.info.waitingFor == nil)
        #expect(full.info.waitedSince == nil)
        #expect(full.stepOutcomes.count == 1)
        #expect(full.stepOutcomes.first?.status == .success)
        #expect(full.stepOutcomes.first?.attempts == 1)
        #expect(full.stepOutcomes.first?.signal == .object(["v": .int(2)]))
    }

    @Test func awaitSignalTimeoutRetriesWithFreshTimeoutThenDelivers() throws {
        let client = deterministicClient()
        // Backoff for attempt 1 would be 1000ms; the timeout gate is 5000ms —
        // pinning the re-park gate at now + 5000 proves FULL-timeout retry,
        // not backoff.
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 5000, retry: StepRetry(maxAttempts: 3))
        ]))
        _ = try client.tick(nowMs: pinnedNow)
        #expect(try client.getWorkflow(info.id).info.status == .waiting)
        // Gate expires: timed-out attempt 1 re-parks with a fresh full gate.
        _ = try client.tick(nowMs: pinnedNow + 5000)
        let state = try client.getWorkflow(info.id).info
        #expect(state.status == .waiting)
        #expect(state.attempts == 1)
        #expect(state.waitedSince == pinnedNow + 5000)
        #expect(state.sleepUntil == pinnedNow + 10000)
        // A delivery while re-parked still succeeds and resets the count.
        try client.signalWorkflow(info.id, name: "approve", payload: .string("go"))
        _ = try client.tick(nowMs: pinnedNow + 5001)
        let full = try client.getWorkflow(info.id)
        #expect(full.info.status == .success)
        #expect(full.info.attempts == 0)
        #expect(full.stepOutcomes.first?.attempts == 2)
        #expect(full.stepOutcomes.first?.signal == .string("go"))
    }

    @Test func awaitSignalExhaustsToTypedTimeoutError() throws {
        let client = deterministicClient()
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 100, retry: StepRetry(maxAttempts: 2))
        ]))
        _ = try client.tick(nowMs: pinnedNow) // park
        _ = try client.tick(nowMs: pinnedNow + 100) // timed-out attempt 1, re-park
        let state = try client.getWorkflow(info.id).info
        #expect(state.attempts == 1)
        _ = try client.tick(nowMs: pinnedNow + 200) // attempt 2 = maxAttempts: terminal
        let full = try client.getWorkflow(info.id)
        #expect(full.info.status == .failed)
        #expect(full.info.attempts == 2)
        #expect(full.info.lastError == "awaitSignal 'approve' timed out")
        #expect(full.info.waitingFor == nil)
        #expect(full.stepOutcomes.count == 1)
        #expect(full.stepOutcomes.first?.status == .failed)
        #expect(full.stepOutcomes.first?.error == "awaitSignal 'approve' timed out")
        #expect(full.stepOutcomes.first?.signal == nil)
        // Terminal: no further ticks resurrect it, and delivery is a typed
        // conflict (not waiting).
        #expect(throws: RtDbError.self) {
            try client.signalWorkflow(info.id, name: "approve")
        }
    }

    @Test func awaitSignalWithoutTimeoutWaitsForeverUntilSignal() throws {
        let client = deterministicClient()
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve")
        ]))
        _ = try client.tick(nowMs: pinnedNow) // park
        // Heat the clock arbitrarily far: an omitted timeoutMs is never due —
        // only a delivery or cancel wakes the run.
        _ = try client.tick(nowMs: pinnedNow + 10_000_000_000)
        let state = try client.getWorkflow(info.id).info
        #expect(state.status == .waiting)
        #expect(state.waitedSince == pinnedNow)
        // Deliver WITH a payload — like every server integration test. (An
        // absent payload leaves the slot nil, so the wake is classified by
        // the payload-slot discriminator exactly as the server classifies
        // it; payload-carrying deliveries are the tested path.)
        try client.signalWorkflow(info.id, name: "approve", payload: .bool(true))
        _ = try client.tick(nowMs: pinnedNow + 10_000_000_001)
        let full = try client.getWorkflow(info.id)
        #expect(full.info.status == .success)
        #expect(full.stepOutcomes.first?.signal == .bool(true))
    }

    @Test func awaitSignalDeliveryIsLatestWins() throws {
        let client = deterministicClient()
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 60000)
        ]))
        _ = try client.tick(nowMs: pinnedNow)
        // Every delivery while the wait is unconsumed acks and overwrites the
        // slot; the consumed payload is the last one delivered.
        try client.signalWorkflow(info.id, name: "approve", payload: .object(["v": .int(1)]))
        try client.signalWorkflow(info.id, name: "approve", payload: .object(["v": .int(2)]))
        try client.signalWorkflow(info.id, name: "approve", payload: .object(["v": .int(3)]))
        _ = try client.tick(nowMs: pinnedNow + 1)
        let outcome = try client.getWorkflow(info.id).stepOutcomes.first
        #expect(outcome?.signal == .object(["v": .int(3)]))
    }

    @Test func awaitSignalCancelWhileWaiting() throws {
        let client = deterministicClient()
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 60000)
        ]))
        _ = try client.tick(nowMs: pinnedNow)
        #expect(client.cancelWorkflow(info.id))
        let cancelled = try client.getWorkflow(info.id).info
        #expect(cancelled.status == .cancelled)
        // Leave-waiting rule: the wait columns drop with the flip.
        #expect(cancelled.waitingFor == nil)
        #expect(cancelled.waitedSince == nil)
        // A cancelled wait never wakes — delivery is a typed conflict.
        #expect(throws: RtDbError.self) {
            try client.signalWorkflow(info.id, name: "approve")
        }
        _ = try client.tick(nowMs: pinnedNow + 120_000)
        #expect(try client.getWorkflow(info.id).info.status == .cancelled)
    }

    @Test func awaitSignalTypedDeliveryErrors() throws {
        let client = deterministicClient()
        // Unknown id: NOT_FOUND.
        #expect(throws: RtDbError.self) {
            try client.signalWorkflow("nope", name: "approve")
        }
        // Name mismatch on a waiting row: CONFLICT naming both names.
        let info = try client.startWorkflow(WorkflowSpec(name: "gate", steps: [
            .awaitSignal(name: "approve", timeoutMs: 60000)
        ]))
        _ = try client.tick(nowMs: pinnedNow)
        do {
            try client.signalWorkflow(info.id, name: "deny")
            Issue.record("a mismatched signal name should reject")
        } catch let error as RtDbError {
            #expect(error.code == .conflict)
            #expect(error.message == "workflow waiting on 'approve', got 'deny'")
        }
        // Not waiting (an ordinary pending run has no wait name): CONFLICT.
        let plain = try client.startWorkflow(WorkflowSpec(name: "run", steps: [
            WorkflowStepSpec(txn: Transaction(steps: []))
        ]))
        do {
            try client.signalWorkflow(plain.id, name: "approve")
            Issue.record("signaling a run that is not waiting should reject")
        } catch let error as RtDbError {
            #expect(error.code == .conflict)
            #expect(error.message == "workflow is not waiting for a signal")
        }
    }

    @Test func awaitSignalSubmitValidationMirrorsTheServer() throws {
        let client = deterministicClient()
        // The builders make an illegal step hard to construct; decode one off
        // the wire instead. Neither txn nor awaitSignal:
        let neither = try JSONDecoder().decode(
            WorkflowSpec.self,
            from: Data(#"{"name":"bad","steps":[{"sleepBeforeMs":5}]}"#.utf8)
        )
        #expect(throws: RtDbError.self) {
            try client.startWorkflow(neither)
        }
        // Both:
        let both = try JSONDecoder().decode(
            WorkflowSpec.self,
            from: Data(
                #"{"name":"bad","steps":[{"txn":{"steps":[]},"awaitSignal":{"name":"a"}}]}"#.utf8
            )
        )
        #expect(throws: RtDbError.self) {
            try client.startWorkflow(both)
        }
        // Name and timeout bounds carry the server's verbatim messages. The
        // raw AwaitSignalSpec init bypasses the factory's eager checks so
        // the ENGINE's submit-time validation is what rejects these.
        do {
            _ = try client.startWorkflow(WorkflowSpec(name: "bad", steps: [
                WorkflowStepSpec(txn: nil, awaitSignal: AwaitSignalSpec(name: ""))
            ]))
            Issue.record("an empty signal name should reject")
        } catch let error as RtDbError {
            #expect(error.message == "steps[0].awaitSignal.name must be 1..=256 chars")
        }
        do {
            _ = try client.startWorkflow(WorkflowSpec(name: "bad", steps: [
                WorkflowStepSpec(
                    txn: nil, awaitSignal: AwaitSignalSpec(name: "a", timeoutMs: 0)
                )
            ]))
            Issue.record("a zero timeoutMs should reject")
        } catch let error as RtDbError {
            #expect(error.message == "steps[0].awaitSignal.timeoutMs must be > 0")
        }
    }

    // MARK: Storage

    @Test func uploadMintsDeterministicIdAndDigest() throws {
        let client = deterministicClient()
        let upload = client.upload(Data("hello".utf8), contentType: "text/plain")
        // The connectionId mint consumed counter slot 1; the file takes 2.
        #expect(upload.id == "f2")
        #expect(upload.sha256 == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        #expect(upload.size == 5)
        #expect(upload.contentType == "text/plain")
        let metadata = try client.getFileMetadata(upload.id)
        #expect(metadata.size == 5)
        #expect(metadata.contentType == "text/plain")
        #expect(metadata.creationTime == pinnedNow)
        #expect(client.getUrl(upload.id) == "memory://f2")
        try client.deleteFile(upload.id)
        #expect(throws: RtDbError.self) {
            try client.getFileMetadata(upload.id)
        }
    }

    // MARK: Subscriptions

    @Test func subscribeFiresInitialValueAndOnChange() throws {
        let (client, ids) = try seededEngine()
        final class Box {
            var values: [JSONValue] = []
        }
        let box = Box()
        let unsub = try client.subscribe(Query(table: "items", count: true)) { value in
            box.values.append(value)
        }
        #expect(box.values == [.int(3)])
        try client.mutate(Transaction(steps: [
            .delete(table: "items", id: ids[0])
        ]))
        #expect(box.values == [.int(3), .int(2)])
        unsub()
        try client.mutate(Transaction(steps: [
            .delete(table: "items", id: ids[1])
        ]))
        #expect(box.values.count == 2)
    }

    @Test func subscriptionSkipsWritesToOtherTables() throws {
        let client = deterministicClient()
        try client.pushSchema(
            SchemaBuilder()
                .table("a") { $0.field("x", .number).index("by_x", on: ["x"]) }
                .table("b") { $0.field("x", .number).index("by_x", on: ["x"]) }
                .build()
        )
        final class Box {
            var values: [JSONValue] = []
        }
        let box = Box()
        _ = try client.subscribe(Query(table: "a", count: true)) { value in
            box.values.append(value)
        }
        try client.mutate(Transaction(steps: [.insert(table: "b", doc: ["x": .int(1)])]))
        #expect(box.values == [.int(0)])
    }

    // MARK: Presence

    @Test func sharedPresenceRoomsFanOutAcrossClients() throws {
        let rooms = PresenceRooms()
        let alice = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
            now: { 0 }, random: { 0 }, connectionId: "alice", presenceRooms: rooms
        ))
        let bob = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
            now: { 0 }, random: { 0 }, connectionId: "bob", presenceRooms: rooms
        ))
        _ = try alice.presence("lobby", state: .string("here"))
        final class Box {
            var snapshots: [[PresenceMember]] = []
        }
        let box = Box()
        _ = try bob.presence("lobby", state: .null) { members in
            box.snapshots.append(members)
        }
        // Bob's join fired twice for him: once with alice alone... no — once,
        // with both members in join order.
        #expect(box.snapshots.count == 1)
        #expect(box.snapshots[0].map(\.connectionId) == ["alice", "bob"])
        alice.updatePresence("lobby", state: .string("away"))
        #expect(box.snapshots.count == 2)
        #expect(box.snapshots[1].first { $0.connectionId == "alice" }?.state == .string("away"))
        bob.leavePresence("lobby")
        #expect(rooms.snapshot("lobby").map(\.connectionId) == ["alice"])
    }

    @Test func presenceTtlExpiresStateToNull() throws {
        let rooms = PresenceRooms()
        let client = InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
            now: { 1000 }, random: { 0 }, connectionId: "c", presenceRooms: rooms
        ))
        _ = try client.presence("room", state: .string("busy"))
        client.updatePresence("room", state: .string("busy"), ttlMs: 500)
        #expect(rooms.snapshot("room").first?.state == .string("busy"))
        rooms.expire(now: 1499)
        #expect(rooms.snapshot("room").first?.state == .string("busy"))
        rooms.expire(now: 1500)
        // The member stays listed; only the state nulls.
        #expect(rooms.snapshot("room").count == 1)
        #expect(rooms.snapshot("room").first?.state == .null)
    }

    // MARK: Admin surface

    @Test func adminSeedsAndFiltersAuditRows() throws {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        client.admin.seedAudit([
            InMemoryAuditSeedRow(table: "items", op: "insert", docId: "d1"),
            InMemoryAuditSeedRow(table: "items", op: "delete", docId: "d2"),
            InMemoryAuditSeedRow(table: "other", op: "insert", docId: "d3")
        ])
        let all = client.admin.getAudit()
        #expect(all.count == 3)
        // Same pinned tsMs on every row: id desc puts the third seed first.
        #expect(all.first?.docId == "d3")
        let filtered = client.admin.getAudit(options: AuditQuery(table: "items", op: "delete"))
        #expect(filtered.count == 1)
        #expect(filtered.first?.docId == "d2")
        let response = client.admin.listSubscriptions()
        #expect(response.subscriptions.isEmpty)
        _ = try client.subscribe(Query(table: "items", count: true)) { _ in }
        #expect(client.admin.listSubscriptions().subscriptions.first?.terminal == "count")
        #expect(client.admin.listSubscriptions().subscriptions.first?.readSetClass == "table")
    }
}
