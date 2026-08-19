import Foundation
@testable import ParRtDbClient
import Testing

/// Pure-projection tests for `projectOptimisticUpdate` — the port of
/// rust-client/src/optimistic.rs's test module (conservative overlay: only
/// unambiguous collect/get/filter shapes project; everything else skips).
struct OptimisticTests {
    private func collectQuery() throws -> Query {
        try TableQuery("items").collect().build()
    }

    /// The array element at `index`'s `_id` as a String, nil when the value
    /// is not an array of objects carrying a string `_id`.
    private func id(at index: Int, of value: JSONValue) -> String? {
        guard case let .array(array) = value, array.indices.contains(index),
              case let .object(doc) = array[index],
              case let .string(id)? = doc["_id"] else { return nil }
        return id
    }

    /// The array element at `index`'s `name` field (`.null` when absent), nil
    /// when the value is not an array of objects.
    private func field(_ name: String, at index: Int, of value: JSONValue) -> JSONValue? {
        guard case let .array(array) = value, array.indices.contains(index),
              case let .object(doc) = array[index] else { return nil }
        return doc[name] ?? .null
    }

    @Test func insertOverlaysOnUnfilteredCollect() throws {
        let query = try collectQuery()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1),
                     "title": .string("x")])
        ])
        let txn = try MutationBuilder().insert("items", ["title": .string("y")]).build()
        guard case let .overlaid(value) = projectOptimisticUpdate(
            query: query, last: last, txn: txn, now: 99
        ) else {
            Issue.record("expected overlay")
            return
        }
        guard case let .array(array) = value else {
            Issue.record("overlaid value is not an array")
            return
        }
        #expect(array.count == 2)
        #expect(try #require(id(at: 1, of: value)).hasPrefix("__optimistic__"))
        #expect(field("_creationTime", at: 1, of: value) == .int(99))
        #expect(field("_version", at: 1, of: value) == .int(1))
        #expect(field("title", at: 1, of: value) == .string("y"))
    }

    @Test func patchOverlaysById() throws {
        let query = try collectQuery()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1),
                     "n": .int(1)])
        ])
        let txn = try MutationBuilder().patch("items", "a", ["n": .int(2)]).build()
        guard case let .overlaid(value) = projectOptimisticUpdate(
            query: query, last: last, txn: txn, now: 99
        ) else {
            Issue.record("expected overlay")
            return
        }
        #expect(field("n", at: 0, of: value) == .int(2))
    }

    @Test func deleteOverlaysById() throws {
        let query = try collectQuery()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)]),
            .object(["_id": .string("b"), "_creationTime": .int(2), "_version": .int(1)])
        ])
        let txn = try MutationBuilder().delete("items", "a").build()
        guard case let .overlaid(value) = projectOptimisticUpdate(
            query: query, last: last, txn: txn, now: 99
        ) else {
            Issue.record("expected overlay")
            return
        }
        guard case let .array(array) = value else {
            Issue.record("overlaid value is not an array")
            return
        }
        #expect(array.count == 1)
    }

    @Test func noopPatchReturnsSkip() throws {
        let query = try collectQuery()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1),
                     "n": .int(1)])
        ])
        // Patching to the same value → equal → skip (key-order-independent
        // Dictionary equality plays serde's canonical BTreeMap role).
        let txn = try MutationBuilder().patch("items", "a", ["n": .int(1)]).build()
        guard case .skip = projectOptimisticUpdate(query: query, last: last, txn: txn, now: 99)
        else {
            Issue.record("expected skip for a no-op patch")
            return
        }
    }

    @Test func insertSkipsWhenTakeWindowFull() throws {
        let query = try TableQuery("items").take(1).build()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)])
        ])
        let txn = try MutationBuilder().insert("items", ["title": .string("y")]).build()
        guard case .skip = projectOptimisticUpdate(query: query, last: last, txn: txn, now: 99)
        else {
            Issue.record("expected skip with the take window full")
            return
        }
    }

    @Test func filteredArrayDeleteOnly() throws {
        // index/eq filtered array: only delete projects.
        let query = try TableQuery("items")
            .withIndex("by_status").eq(.string("active"))
            .collect().build()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)])
        ])
        let del = try MutationBuilder().delete("items", "a").build()
        guard case .overlaid = projectOptimisticUpdate(
            query: query, last: last, txn: del, now: 99
        ) else {
            Issue.record("expected delete under an index filter to overlay")
            return
        }
        let ins = try MutationBuilder().insert("items", ["title": .string("y")]).build()
        guard case .skip = projectOptimisticUpdate(query: query, last: last, txn: ins, now: 99)
        else {
            Issue.record("expected insert under an index filter to skip")
            return
        }
    }

    @Test func filterPredicateTreatedAsFilteredArray() throws {
        // A collect with a `filter` predicate routes to delete-only
        // projection, not unfiltered-array: delete overlays, insert skips.
        let query = try TableQuery("items")
            .filter(.eq(field: "status", value: .string("done")))
            .collect().build()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)]),
            .object(["_id": .string("b"), "_creationTime": .int(2), "_version": .int(1)])
        ])
        let del = try MutationBuilder().delete("items", "a").build()
        guard case let .overlaid(value) = projectOptimisticUpdate(
            query: query, last: last, txn: del, now: 99
        ) else {
            Issue.record("expected delete under a filter predicate to overlay")
            return
        }
        guard case let .array(array) = value else {
            Issue.record("overlaid value is not an array")
            return
        }
        #expect(array.count == 1)
        let ins = try MutationBuilder().insert("items", ["title": .string("y")]).build()
        guard case .skip = projectOptimisticUpdate(query: query, last: last, txn: ins, now: 99)
        else {
            Issue.record("expected insert under a filter predicate to skip")
            return
        }
    }

    @Test func getPointReadPatch() throws {
        let query = try TableQuery("items").get("a").build()
        let last = JSONValue.object([
            "_id": .string("a"), "_creationTime": .int(1), "_version": .int(1), "n": .int(1)
        ])
        let txn = try MutationBuilder().patch("items", "a", ["n": .int(2)]).build()
        guard case let .overlaid(value) = projectOptimisticUpdate(
            query: query, last: last, txn: txn, now: 99
        ) else {
            Issue.record("expected overlay")
            return
        }
        guard case let .object(doc) = value else {
            Issue.record("overlaid value is not an object")
            return
        }
        #expect(doc["n"] == .int(2))
        #expect(doc["_id"] == .string("a"))
        #expect(doc["_creationTime"] == .int(1))
    }

    @Test func getPointReadDeleteNulAndReplacePreservesIdentity() throws {
        let query = try TableQuery("items").get("a").build()
        // Delete of the target nulls the point read.
        let last = JSONValue.object(["_id": .string("a"), "n": .int(1)])
        let del = try MutationBuilder().delete("items", "a").build()
        guard case let .overlaid(nulled) = projectOptimisticUpdate(
            query: query, last: last, txn: del, now: 99
        ) else {
            Issue.record("expected delete to overlay")
            return
        }
        #expect(nulled == .null)
        // Replace of the target keeps the cached _id/_creationTime and drops
        // _version until the server round-trip delivers the truth.
        let richer = JSONValue.object([
            "_id": .string("a"), "_creationTime": .int(7), "_version": .int(3), "n": .int(1)
        ])
        let repl = try MutationBuilder()
            .replace("items", "a", ["n": .int(9)]).build()
        guard case let .overlaid(replaced) = projectOptimisticUpdate(
            query: query, last: richer, txn: repl, now: 99
        ) else {
            Issue.record("expected replace to overlay")
            return
        }
        guard case let .object(doc) = replaced else {
            Issue.record("replaced value is not an object")
            return
        }
        #expect(doc["_id"] == .string("a"))
        #expect(doc["_creationTime"] == .int(7))
        #expect(doc["_version"] == nil)
        #expect(doc["n"] == .int(9))
        // A non-target delete leaves the point read alone.
        let otherDel = try MutationBuilder().delete("items", "zzz").build()
        guard case .skip = projectOptimisticUpdate(
            query: query, last: richer, txn: otherDel, now: 99
        ) else {
            Issue.record("expected non-target delete to skip")
            return
        }
    }

    @Test func alwaysSkipTerminals() throws {
        // unique, first, count, paginate, search, vectorSearch all → skip
        // regardless of txn.
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)])
        ])
        let txn = try MutationBuilder()
            .insert("items", ["title": .string("y")])
            .patch("items", "a", ["n": .int(2)])
            .delete("items", "a")
            .build()
        let terminals: [Query] = try [
            TableQuery("items").withIndex("by_status").eq(.string("active")).unique().build(),
            TableQuery("items").first().build(),
            TableQuery("items").count().build(),
            TableQuery("items").paginate(numItems: 10).build(),
            TableQuery("items").search("search_idx", "query").take(5).build(),
            TableQuery("items").vectorSearch("vec_idx", [1.0, 0.0], limit: 5).build()
        ]
        for query in terminals {
            guard case .skip = projectOptimisticUpdate(
                query: query, last: last, txn: txn, now: 99
            ) else {
                Issue.record("terminal query should skip: \(query)")
                continue
            }
        }
    }

    @Test func tableMismatchAndAmbiguousStepsSkip() throws {
        let query = try collectQuery()
        let last = JSONValue.array([
            .object(["_id": .string("a"), "_creationTime": .int(1), "_version": .int(1)])
        ])
        // A step on another table leaves this result alone (skip — no change).
        let otherTable = try MutationBuilder().insert("other", ["n": .int(1)]).build()
        guard case .skip = projectOptimisticUpdate(
            query: query, last: last, txn: otherTable, now: 99
        ) else {
            Issue.record("expected other-table insert to skip")
            return
        }
        // Upsert and by-query steps are membership-ambiguous → skip.
        for txn in try [
            MutationBuilder()
                .upsert("items", index: "by_title", eq: [.string("t")],
                        insert: ["title": .string("t")], patch: ["n": .int(1)])
                .build(),
            MutationBuilder()
                .patchByQuery("items", filter: .eq(field: "n", value: .int(1)),
                              patch: ["n": .int(2)])
                .build(),
            MutationBuilder()
                .deleteByQuery("items", filter: .eq(field: "n", value: .int(1)))
                .build()
        ] {
            guard case .skip = projectOptimisticUpdate(
                query: query, last: last, txn: txn, now: 99
            ) else {
                Issue.record("expected ambiguous step to skip")
                return
            }
        }
        // Preconditions and schedule/workflow steps are no-ops, not skips:
        // the txn's only effect is nothing → skip via finalize (equal value).
        let noOps = try MutationBuilder()
            .expectVersion("items", "a", 1)
            .schedule(.afterMs(ms: 1000), MutationBuilder().insert("items", ["n": .int(1)]).build())
            .build()
        guard case .skip = projectOptimisticUpdate(
            query: query, last: last, txn: noOps, now: 99
        ) else {
            Issue.record("expected no-op-only txn to skip")
            return
        }
    }

    @Test func syntheticIdFormat() throws {
        // Two inserts in two calls produce __optimistic__N with incrementing N.
        let query = try collectQuery()
        let last = JSONValue.array([])
        let txn1 = try MutationBuilder().insert("items", ["title": .string("a")]).build()
        let txn2 = try MutationBuilder().insert("items", ["title": .string("b")]).build()
        guard case let .overlaid(first) = projectOptimisticUpdate(
            query: query, last: last, txn: txn1, now: 1
        ) else {
            Issue.record("expected overlay for txn1")
            return
        }
        guard case let .overlaid(second) = projectOptimisticUpdate(
            query: query, last: last, txn: txn2, now: 2
        ) else {
            Issue.record("expected overlay for txn2")
            return
        }
        let id1 = try #require(id(at: 0, of: first))
        let id2 = try #require(id(at: 0, of: second))
        #expect(id1.hasPrefix("__optimistic__"))
        #expect(id2.hasPrefix("__optimistic__"))
        #expect(id1 != id2)
    }
}
