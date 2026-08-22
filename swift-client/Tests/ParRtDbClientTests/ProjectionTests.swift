import Foundation
@testable import ParRtDbClient
import Testing

// Query.fields projection — builder/wire shapes plus the in-memory engine's
// execution semantics: the swift mirror of the server's projection tests
// (server/tests/query_test.rs + sub_invalidation_test.rs) and the
// wire-corpus projection cases. Split from QueryTests/InMemoryTests to stay
// under the type-body cap (the QueryTests header's convention).

// MARK: - Builder / wire

struct ProjectionTests {
    @Test func fieldsBuildsExactWireShape() throws {
        // Corpus `queries` entry: fields rides alongside index/eq/take, and a
        // system field (`_id`) may be listed explicitly (an accepted no-op).
        let obj = try TableQuery("workItems").withIndex("by_status")
            .eq(.string("backlog")).take(10).fields("title", "status", "_id")
            .build().wireObject()
        #expect(obj == [
            "table": .string("workItems"),
            "index": .string("by_status"),
            "eq": .array([.string("backlog")]),
            "take": .int(10),
            "fields": .array([.string("title"), .string("status"), .string("_id")])
        ])
    }

    @Test func fieldsOmittedWhenNotSet() throws {
        let built = try TableQuery("items").withIndex("by_n").take(5).build()
        #expect(built.fields == nil)
        #expect(try built.wireObject()["fields"] == nil)
    }

    @Test func emptyFieldsListEncodesAsEmptyArray() throws {
        // `[]` is meaningful — the system-fields-only (ids-only) view — so it
        // must reach the wire as `[]`, never collapse to an omission.
        let obj = try TableQuery("items").fields().build().wireObject()
        #expect(obj == ["table": .string("items"), "fields": .array([])])
    }

    @Test func fieldsRoundTripsThroughCodable() throws {
        // The corpus queries fixture, decoded and re-encoded. Comparison is
        // over parsed VALUES via the throwing helper (see WireTests for why
        // #expect around AnyObject.isEqual is avoided on this toolchain).
        let fixture = #"{"table":"workItems","index":"by_status","eq":["backlog"],"#
            + #""take":10,"fields":["title","status","_id"]}"#
        let query = try JSONDecoder().decode(Query.self, from: Data(fixture.utf8))
        #expect(query.fields == ["title", "status", "_id"])
        try expectValueEqual(JSONEncoder().encode(query), fixture)
    }

    @Test func fieldsComposesWithEveryTerminal() throws {
        // fields is not a combination peer — it composes with get, the
        // doc-less terminals, and the ranked-search family alike.
        _ = try TableQuery("items").get("i1").fields("title").build()
        _ = try TableQuery("items").withIndex("by_n").count().fields("n").build()
        _ = try TableQuery("notes").search("search_body", "hi").take(3).fields("title").build()
        _ = try TableQuery("items").fields().paginate(numItems: 5).build()
    }
}

/// Throws on value mismatch — the WireTests `expectEncodes` pattern: no
/// #expect around `AnyObject.isEqual` (a Swift 6.3.3 compiler-crash class on
/// this toolchain), and VALUES compare (JSONEncoder may collapse `2.0` to
/// `2`), never encoded bytes.
private func expectValueEqual(_ dumped: Data, _ json: String, _ what: String = "") throws {
    let dumpedObject = try JSONSerialization.jsonObject(with: dumped) as AnyObject
    let expectedObject = try JSONSerialization.jsonObject(with: Data(json.utf8)) as AnyObject
    guard dumpedObject.isEqual(expectedObject) else {
        throw ProjectionTestFailure("encoded \(String(data: dumped, encoding: .utf8) ?? "") "
            + "but expected \(json) \(what)")
    }
}

private struct ProjectionTestFailure: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String {
        message
    }
}

// MARK: - Engine execution

/// The in-memory engine's projection semantics: collect/paginate/get shapes,
/// validation, the ids-only view, doc-less terminals, and the projected
/// subscription's `_version`-stripped diff (server `diff_canonical`).
struct ProjectionEngineTests {
    // MARK: Fixtures

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
            }
            .build()
    }

    /// Seeds a(n=3, tag x), b(n=1, tag x), c(n=2); returns the client and
    /// the three minted ids in seed order.
    private func seededEngine() throws -> (InMemoryRtDbClient, [String]) {
        let client = deterministicClient()
        try client.pushSchema(itemsSchema())
        let results = try client.mutate(Transaction(steps: [
            .insert(table: "items", doc: ["title": .string("a"), "n": .int(3), "tag": .string("x")]),
            .insert(table: "items", doc: ["title": .string("b"), "n": .int(1), "tag": .string("x")]),
            .insert(table: "items", doc: ["title": .string("c"), "n": .int(2)])
        ])).map { result -> String in
            guard case let .insert(id) = result else {
                throw RtDbError(code: .internal, message: "expected insert result")
            }
            return id
        }
        return (client, results)
    }

    /// Sorted key set of a result doc, for exact projected-shape assertions
    /// (the server test's `sorted_doc_keys`).
    private func sortedDocKeys(_ doc: JSONValue) throws -> [String] {
        guard let object = doc.objectValue else {
            throw RtDbError(code: .internal, message: "result element is not an object")
        }
        return object.keys.sorted()
    }

    private func docsOf(_ result: JSONValue) throws -> [JSONValue] {
        guard case let .array(docs) = result else {
            throw RtDbError(code: .internal, message: "result is not an array")
        }
        return docs
    }

    // MARK: Projection shapes

    @Test func collectProjectionKeepsListedAndSystemFields() throws {
        let (client, _) = try seededEngine()
        let result = try client.query(
            Query(table: "items", index: "by_n", order: .asc, fields: ["title"])
        )
        // n-order is b, c, a; `n` and `tag` are dropped from every doc, the
        // system fields and `title` stay. Sorting ran pre-projection.
        let titles = try docsOf(result).map { try $0.objectValue?["title"] }
        #expect(titles == [.string("b"), .string("c"), .string("a")])
        for doc in try docsOf(result) {
            #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version", "title"])
        }
    }

    @Test func emptyProjectionIsSystemFieldsOnly() throws {
        let (client, _) = try seededEngine()
        let result = try client.query(Query(table: "items", index: "by_n", fields: []))
        for doc in try docsOf(result) {
            #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version"])
        }
    }

    @Test func systemFieldsListedExplicitlyAreNoOp() throws {
        let (client, _) = try seededEngine()
        let result = try client.query(
            Query(table: "items", fields: ["_id", "_creationTime", "_version", "title"])
        )
        for doc in try docsOf(result) {
            #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version", "title"])
        }
    }

    @Test func getProjectionAppliesToPointRead() throws {
        let (client, ids) = try seededEngine()
        let doc = try client.query(Query(table: "items", get: ids[0], fields: ["title"]))
        #expect(doc.objectValue?["title"] == .string("a"))
        #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version", "title"])
        // A missing id still projects to plain null.
        let missing = try client.query(Query(table: "items", get: "nope", fields: ["title"]))
        #expect(missing == .null)
    }

    @Test func paginateProjectionKeepsCursorMintedPreProjection() throws {
        let (client, _) = try seededEngine()
        let page1 = try client.query(Query(
            table: "items", index: "by_n", order: .asc,
            paginate: Paginate(numItems: 2), fields: ["title"]
        ))
        guard let page1Object = page1.objectValue,
              case let .array(page1Docs) = page1Object["docs"],
              let cursor = page1Object["nextCursor"]?.stringValue
        else {
            Issue.record("page 1 must carry projected docs and a next cursor")
            return
        }
        #expect(page1Docs.map { $0.objectValue?["title"] } == [.string("b"), .string("c")])
        for doc in page1Docs {
            #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version", "title"])
        }
        // The cursor was minted from the unprojected row inside the terminal,
        // so page 2 follows it and is projected too.
        let page2 = try client.query(Query(
            table: "items", index: "by_n", order: .asc,
            paginate: Paginate(cursor: cursor, numItems: 2), fields: ["title"]
        ))
        guard let page2Object = page2.objectValue,
              case let .array(page2Docs) = page2Object["docs"]
        else {
            Issue.record("page 2 must carry a docs array")
            return
        }
        #expect(page2Docs.map { $0.objectValue?["title"] } == [.string("a")])
        #expect(page2Object["nextCursor"] == nil)
    }

    // MARK: Validation

    @Test func unknownProjectionFieldIsBadRequest() throws {
        let (client, _) = try seededEngine()
        do {
            _ = try client.query(Query(table: "items", fields: ["title", "bogus"]))
            Issue.record("unknown projection field must be rejected")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "unknown projection field 'bogus'")
        }
        // Validation precedes the terminal arms (server compile_query), so
        // even a get query rejects the unknown name.
        do {
            _ = try client.query(Query(table: "items", get: "i1", fields: ["_versionn"]))
            Issue.record("typo'd system name must be rejected")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "unknown projection field '_versionn'")
        }
    }

    // MARK: Doc-less terminals

    @Test func docLessTerminalsUnaffected() throws {
        let (client, _) = try seededEngine()
        // count still counts.
        let counted = try client.query(
            Query(table: "items", index: "by_n", count: true, fields: ["title"])
        )
        #expect(counted == .int(3))
        // aggregate still aggregates (3 + 1 + 2).
        let summed = try client.query(Query(
            table: "items", index: "by_n", aggregate: AggregateSpec(op: .sum), fields: ["title"]
        ))
        #expect(summed.doubleValue == 6)
        // groupBy rows keep their {key, value} shape — the projection must
        // not mistake them for docs.
        let grouped = try client.query(Query(
            table: "items", index: "by_n",
            aggregate: AggregateSpec(op: .count, groupBy: true), fields: ["title"]
        ))
        let rows = try docsOf(grouped)
        #expect(rows.count == 3)
        for row in rows {
            #expect(try sortedDocKeys(row) == ["key", "value"])
        }
    }

    // MARK: Subscription diff

    @Test func projectedSubSkipsNonProjectedFieldChange() throws {
        let (client, ids) = try seededEngine()
        final class Box {
            var values: [JSONValue] = []
        }
        let box = Box()
        let unsub = try client.subscribe(
            Query(table: "items", index: "by_n", order: .asc, fields: ["title"])
        ) { value in
            box.values.append(value)
        }
        // Initial push carries the projected shape.
        #expect(box.values.count == 1)
        let initial = try docsOf(box.values[0])
        #expect(initial.map { $0.objectValue?["title"] } == [.string("b"), .string("c"), .string("a")])
        for doc in initial {
            #expect(try sortedDocKeys(doc) == ["_creationTime", "_id", "_version", "title"])
        }
        // Patching `tag` (non-projected, not in the sort index) bumps only
        // `_version` — the diff strips it, so nothing is pushed.
        try client.mutate(Transaction(steps: [
            .patch(table: "items", id: ids[0], fields: ["tag": .string("y")])
        ]))
        #expect(box.values.count == 1)
        // Patching the projected `title` changes the diff form: push.
        try client.mutate(Transaction(steps: [
            .patch(table: "items", id: ids[0], fields: ["title": .string("renamed")])
        ]))
        #expect(box.values.count == 2)
        let second = try docsOf(box.values[1])
        #expect(second.map { $0.objectValue?["title"] } == [.string("b"), .string("c"), .string("renamed")])
        // Pushed payloads still carry `_version`; only change detection ignores it.
        for doc in second {
            #expect(doc.objectValue?["_version"] != nil)
        }
        unsub()
    }

    @Test func unprojectedSubPushesOnVersionBump() throws {
        // Control for the test above: without fields, the same tag patch
        // pushes (the `_version` bump is visible to the diff) — proving the
        // strip is what silenced the projected sub, not a skipped re-run.
        let (client, ids) = try seededEngine()
        final class Box {
            var values: [JSONValue] = []
        }
        let box = Box()
        let unsub = try client.subscribe(
            Query(table: "items", index: "by_n", order: .asc)
        ) { value in
            box.values.append(value)
        }
        #expect(box.values.count == 1)
        try client.mutate(Transaction(steps: [
            .patch(table: "items", id: ids[0], fields: ["tag": .string("y")])
        ]))
        #expect(box.values.count == 2)
        unsub()
    }
}
