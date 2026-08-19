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
