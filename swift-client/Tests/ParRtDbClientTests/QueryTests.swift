import Foundation
@testable import ParRtDbClient
import Testing

/// Task 8 — the fluent `TableQuery` builder. Three suites (to stay under the
/// type-body cap): QueryTests (builder → exact wire shapes), QueryExclusivityTests
/// (build()'s terminal exclusivity, ported verbatim from server/src/query/mod.rs +
/// terminals.rs so identical combinations produce identical BadRequest messages),
/// and QueryParseResultTests (parseResult + Paginated). Every wire-shape
/// assertion is whole-object, so a stray key fails the test.
struct QueryTests {
    // MARK: - Builder → exact wire shapes (rust builder tests + corpus cases)

    @Test func bareTableCollectBuildsMinimalShape() throws {
        // `collect` is the ABSENCE of a terminal — the wire Query has no
        // `collect` field, so the object is exactly `{table}`.
        let obj = try TableQuery("items").collect().build().wireObject()
        #expect(obj == ["table": .string("items")])
    }

    @Test func pointGetBuildsExactShape() throws {
        let obj = try TableQuery("items").get("abc").build().wireObject()
        #expect(obj == ["table": .string("items"), "get": .string("abc")])
    }

    @Test func indexEqUniqueBuildsExactShape() throws {
        let obj = try TableQuery("items").withIndex("by_project").eq(.string("p1"))
            .unique().build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_project"),
            "eq": .array([.string("p1")]),
            "unique": .bool(true)
        ])
    }

    @Test func indexEqOrderTakeCollectBuildsExactShape() throws {
        // The brief's draft test asserted a `collect` key; the shipped Query
        // struct has no such field — collect-all is the absence of `take`.
        let obj = try TableQuery("users").withIndex("by_email").eq(.string("a@b.c"))
            .order(.desc).take(10).collect().build().wireObject()
        #expect(obj == [
            "table": .string("users"),
            "index": .string("by_email"),
            "eq": .array([.string("a@b.c")]),
            "order": .string("desc"),
            "take": .int(10)
        ])
    }

    @Test func rangeBoundsOrderTakeBuildsExactShape() throws {
        let obj = try TableQuery("items").withIndex("by_project").eq(.string("p1"))
            .gte(.string("a")).lte(.string("m")).order(.desc).take(10)
            .build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_project"),
            "eq": .array([.string("p1")]),
            "gte": .string("a"),
            "lte": .string("m"),
            "order": .string("desc"),
            "take": .int(10)
        ])
    }

    @Test func countTerminalBuildsExactShape() throws {
        let obj = try TableQuery("items").withIndex("by_status").eq(.string("backlog"))
            .count().build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_status"),
            "eq": .array([.string("backlog")]),
            "count": .bool(true)
        ])
    }

    @Test func distinctTerminalBuildsExactShape() throws {
        // Corpus `queries` case: {"table":"workItems","index":...,"eq":["p1"],"distinct":true}
        let obj = try TableQuery("workItems").withIndex("by_project_and_status")
            .eq(.string("p1")).distinct().build().wireObject()
        #expect(obj == [
            "table": .string("workItems"),
            "index": .string("by_project_and_status"),
            "eq": .array([.string("p1")]),
            "distinct": .bool(true)
        ])
    }

    @Test func aggregateTerminalOmitsGroupByFalse() throws {
        let obj = try TableQuery("items").withIndex("by_project_and_order")
            .eq(.string("p1")).aggregate(.sum).build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_project_and_order"),
            "eq": .array([.string("p1")]),
            "aggregate": .object(["op": .string("sum")])
        ])
    }

    @Test func aggregateTerminalGroupByEmitsCamelFlag() throws {
        let obj = try TableQuery("items").withIndex("by_project_status_order")
            .eq(.string("p1")).aggregate(.sum, groupBy: true).build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_project_status_order"),
            "eq": .array([.string("p1")]),
            "aggregate": .object(["op": .string("sum"), "groupBy": .bool(true)])
        ])
    }

    @Test func paginateWithoutCursorOmitsIt() throws {
        // Corpus `queries` case: {"table":"workItems","paginate":{"numItems":10}}
        let obj = try TableQuery("workItems").paginate(numItems: 10).build().wireObject()
        #expect(obj == [
            "table": .string("workItems"),
            "paginate": .object(["numItems": .int(10)])
        ])
    }

    @Test func paginateWithCursorRoundTrips() throws {
        // Corpus `queries` case: {"table":"workItems","paginate":{"cursor":"abc","numItems":10}}
        let obj = try TableQuery("workItems").paginate(cursor: "abc", numItems: 10)
            .build().wireObject()
        #expect(obj == [
            "table": .string("workItems"),
            "paginate": .object(["cursor": .string("abc"), "numItems": .int(10)])
        ])
    }

    @Test func filterBuilderSerializesPredicate() throws {
        let obj = try TableQuery("items")
            .filter(.eq(field: "status", value: .string("done")))
            .collect().build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "filter": .object(["op": .string("eq"), "field": .string("status"), "value": .string("done")])
        ])
    }

    @Test func filterComposesWithIndexAndTake() throws {
        let obj = try TableQuery("items").withIndex("by_project").eq(.string("p1"))
            .filter(.gt(field: "order", value: .int(0))).take(10).build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "index": .string("by_project"),
            "eq": .array([.string("p1")]),
            "filter": .object(["op": .string("gt"), "field": .string("order"), "value": .int(0)]),
            "take": .int(10)
        ])
    }

    @Test func filterNestsCombinators() throws {
        let obj = try TableQuery("items")
            .filter(.or(exprs: [
                .inValues(field: "status", values: [.string("blocked"), .string("backlog")]),
                .lte(field: "order", value: .int(3))
            ]))
            .collect().build().wireObject()
        #expect(obj == [
            "table": .string("items"),
            "filter": .object(["op": .string("or"), "exprs": .array([
                .object(["op": .string("in"), "field": .string("status"),
                         "values": .array([.string("blocked"), .string("backlog")])]),
                .object(["op": .string("lte"), "field": .string("order"), "value": .int(3)])
            ])])
        ])
    }

    @Test func searchBuilderSerializesTerminal() throws {
        // Corpus `queries` case: {"table":"notes","search":{"index":"search_body","query":"hello world"}}
        let obj = try TableQuery("notes").search("search_body", "hello world")
            .collect().build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object(["index": .string("search_body"), "query": .string("hello world")])
        ])
    }

    @Test func searchComposesWithTake() throws {
        let obj = try TableQuery("notes").search("search_content", "hello world").take(10)
            .build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object(["index": .string("search_content"), "query": .string("hello world")]),
            "take": .int(10)
        ])
    }

    @Test func searchCarriesModeTrgm() throws {
        // Corpus `queries` case: mode "trgm" opts into substring matching.
        let obj = try TableQuery("notes").search("search_body", "conv", mode: .trgm)
            .take(10).build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object(["index": .string("search_body"), "query": .string("conv"),
                               "mode": .string("trgm")]),
            "take": .int(10)
        ])
    }

    @Test func searchCarriesModeFilterAndExplicitTsquery() throws {
        // Corpus `queries` case 5: explicit tsquery + nested filter.
        let obj = try TableQuery("notes").search(
            "search_body", "conv",
            filter: .eq(field: "status", value: .string("open")),
            mode: .tsquery
        ).take(10).build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object([
                "index": .string("search_body"),
                "query": .string("conv"),
                "filter": .object(["op": .string("eq"), "field": .string("status"),
                                   "value": .string("open")]),
                "mode": .string("tsquery")
            ]),
            "take": .int(10)
        ])
    }

    @Test func searchCarriesSnippet() throws {
        // Corpus `queries` case 7, with operator-syntax query text.
        let obj = try TableQuery("notes").search(
            "search_body", "\"exact phrase\" or -excluded",
            snippet: true
        ).collect().build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object(["index": .string("search_body"),
                               "query": .string("\"exact phrase\" or -excluded"),
                               "snippet": .bool(true)])
        ])
    }

    @Test func vectorSearchBuilderSerializesTerminal() throws {
        // Corpus `queries` case: vector [1.0,0.5,-0.5] — Foundation encodes the
        // integral 1.0 as `1`, which decodes back through JSONValue as .int(1).
        let obj = try TableQuery("embeds").vectorSearch("by_embedding", [1.0, 0.5, -0.5], limit: 5)
            .build().wireObject()
        #expect(obj == [
            "table": .string("embeds"),
            "vectorSearch": .object([
                "index": .string("by_embedding"),
                "vector": .array([.int(1), .double(0.5), .double(-0.5)]),
                "limit": .int(5)
            ])
        ])
    }

    @Test func vectorSearchCarriesNestedFilter() throws {
        // Corpus `queries` case 9.
        let obj = try TableQuery("embeds").vectorSearch(
            "by_embedding", [1.0], limit: 3,
            filter: .eq(field: "userId", value: .string("u1"))
        ).build().wireObject()
        #expect(obj == [
            "table": .string("embeds"),
            "vectorSearch": .object([
                "index": .string("by_embedding"),
                "vector": .array([.int(1)]),
                "limit": .int(3),
                "filter": .object(["op": .string("eq"), "field": .string("userId"),
                                   "value": .string("u1")])
            ])
        ])
    }

    @Test func hybridSearchBuilderSerializesTerminal() throws {
        let obj = try TableQuery("docs").hybridSearch("hello world", [1.0, 0.0, 0.0], limit: 5)
            .build().wireObject()
        #expect(obj == [
            "table": .string("docs"),
            "hybridSearch": .object([
                "query": .string("hello world"),
                "vector": .array([.int(1), .int(0), .int(0)]),
                "limit": .int(5)
            ])
        ])
    }

    @Test func hybridSearchFullOptsRoundTrip() throws {
        let obj = try TableQuery("docs").hybridSearch(
            "hello", [1.0, 0.0, 0.0], limit: 5,
            searchIndex: "search_body", vectorIndex: "by_embedding", k: 42
        ).build().wireObject()
        #expect(obj == [
            "table": .string("docs"),
            "hybridSearch": .object([
                "query": .string("hello"),
                "vector": .array([.int(1), .int(0), .int(0)]),
                "limit": .int(5),
                "searchIndex": .string("search_body"),
                "vectorIndex": .string("by_embedding"),
                "k": .int(42)
            ])
        ])
    }

    @Test func hybridSearchPartialKOnlyOmitsIndexes() throws {
        let obj = try TableQuery("docs").hybridSearch("hi", [1.0, 0.0, 0.0], limit: 5, k: 7)
            .build().wireObject()
        #expect(obj == [
            "table": .string("docs"),
            "hybridSearch": .object([
                "query": .string("hi"),
                "vector": .array([.int(1), .int(0), .int(0)]),
                "limit": .int(5),
                "k": .int(7)
            ])
        ])
    }
}

/// build()'s terminal-exclusivity rules, ported verbatim from the server
/// cascade (same rules, same order, same messages — first match wins), plus
/// the legal-combination positive controls. Kept out of QueryTests so both
/// suites stay under the type-body cap.
struct QueryExclusivityTests {
    @Test func terminalsAreMutuallyExclusive() {
        // The brief's test: first + count is a badRequest RtDbError.
        let error = buildError { $0.first().count() }
        #expect(error != nil)
        #expect(error?.code == .badRequest)
        #expect(error?.message == "count cannot be combined with first")
    }

    @Test func getConflictsWithIndexAndEq() {
        let error = buildError { $0.withIndex("by_email").eq(.string("a@b.c")).get("id1") }
        #expect(error?.code == .badRequest)
        #expect(
            error?.message
                == "get cannot be combined with index, eq, range bounds, order, take, unique, "
                + "first, count, distinct, aggregate, paginate, filter, search, or vector search"
        )
    }

    @Test func getConflictsWithTake() {
        #expect(buildError { $0.get("id1").take(5) }?.code == .badRequest)
    }

    @Test func uniqueCannotCombineWithTakeOrderDistinctAggregate() {
        let expected = "unique cannot be combined with take, order, distinct, or aggregate"
        #expect(buildError { $0.unique().take(5) }?.message == expected)
        #expect(buildError { $0.order(.asc).unique() }?.message == expected)
        #expect(buildError { $0.unique().distinct() }?.message == expected)
    }

    @Test func firstCannotCombineWithUniqueTakeDistinctAggregate() {
        #expect(buildError { $0.unique().first() }?.message == "first cannot be combined with unique")
        #expect(buildError { $0.first().take(3) }?.message == "first cannot be combined with take")
        #expect(
            buildError { $0.first().aggregate(.max) }?.message
                == "first cannot be combined with aggregate"
        )
    }

    @Test func countCannotCombineWithOtherTerminals() {
        #expect(buildError { $0.count().order(.desc) }?.message == "count cannot be combined with order")
        #expect(buildError { $0.count().take(5) }?.message == "count cannot be combined with take")
        #expect(buildError { $0.count().first() }?.message == "count cannot be combined with first")
        #expect(buildError { $0.count().distinct() }?.message == "count cannot be combined with distinct")
        #expect(
            buildError { $0.count().aggregate(.sum) }?.message
                == "count cannot be combined with aggregate"
        )
    }

    @Test func distinctCannotCombineWithOtherTerminals() {
        // get+distinct never reaches the distinct check — the cascade checks get
        // first (first-match-wins, like the server), so the GET message fires.
        let viaGet = buildError { $0.get("a").distinct() }
        #expect(viaGet?.message.hasPrefix("get cannot be combined") == true)
        #expect(buildError { $0.distinct().take(5) }?.message == "distinct cannot be combined with take")
        #expect(
            buildError { $0.distinct().order(.asc) }?.message
                == "distinct cannot be combined with order"
        )
        #expect(
            buildError { $0.distinct().paginate(numItems: 5) }?.message
                == "distinct cannot be combined with paginate"
        )
        #expect(
            buildError { $0.distinct().search("i", "q") }?.message
                == "distinct cannot be combined with search"
        )
        #expect(
            buildError { $0.distinct().vectorSearch("i", [1.0], limit: 5) }?.message
                == "distinct cannot be combined with vector search"
        )
        #expect(
            buildError { $0.distinct().hybridSearch("q", [1.0], limit: 5) }?.message
                == "distinct cannot be combined with hybrid search"
        )
    }

    @Test func aggregateCannotCombineWithOtherTerminals() {
        // get fires before aggregate in the cascade — the GET message wins.
        let viaGet = buildError { $0.get("a").aggregate(.sum) }
        #expect(viaGet?.message.hasPrefix("get cannot be combined") == true)
        #expect(buildError { $0.aggregate(.sum).take(5) }?.message == "aggregate cannot be combined with take")
        #expect(
            buildError { $0.aggregate(.sum).paginate(numItems: 5) }?.message
                == "aggregate cannot be combined with paginate"
        )
        #expect(
            buildError { $0.aggregate(.sum).search("i", "q") }?.message
                == "aggregate cannot be combined with search"
        )
        #expect(
            buildError { $0.aggregate(.sum).vectorSearch("i", [1.0], limit: 5) }?.message
                == "aggregate cannot be combined with vector search"
        )
    }

    @Test func paginateCannotCombineWithTakeCountUniqueFirst() {
        #expect(
            buildError { $0.take(5).paginate(numItems: 10) }?.message
                == "paginate cannot be combined with take"
        )
        #expect(
            buildError { $0.count().paginate(numItems: 10) }?.message
                == "paginate cannot be combined with count"
        )
        #expect(
            buildError { $0.unique().paginate(numItems: 10) }?.message
                == "paginate cannot be combined with unique"
        )
        #expect(
            buildError { $0.first().paginate(numItems: 10) }?.message
                == "paginate cannot be combined with first"
        )
    }

    @Test func gtAndGteCannotBothBeSet() {
        #expect(buildError { $0.gt(.int(1)).gte(.int(2)) }?.message == "gt and gte cannot both be set")
        #expect(buildError { $0.lt(.int(1)).lte(.int(2)) }?.message == "lt and lte cannot both be set")
    }

    @Test func vectorSearchCannotCombineWithAnyOtherTerminal() {
        let expected = "vectorSearch cannot be combined with any other terminal"
        #expect(buildError { $0.vectorSearch("by_e", [1.0], limit: 5).take(10) }?.message == expected)
        // The top-level filter conflicts; the NESTED filter (wire-shape test above) does not.
        let filterConflict = buildError {
            $0.vectorSearch("by_e", [1.0], limit: 5).filter(.eq(field: "a", value: .int(1)))
        }
        #expect(filterConflict?.message == expected)
        let indexConflict = buildError {
            $0.withIndex("i").eq(.string("x")).vectorSearch("by_e", [1.0], limit: 5)
        }
        #expect(indexConflict?.message == expected)
    }

    @Test func hybridSearchCannotCombineWithTake() {
        #expect(
            buildError { $0.hybridSearch("q", [1.0], limit: 5).take(3) }?.message
                == "hybridSearch cannot be combined with any other terminal"
        )
    }

    @Test func searchRejectsTopLevelFilterButNotNested() {
        #expect(
            buildError { $0.search("idx", "q").filter(.eq(field: "a", value: .int(1))) }?.message
                == "search cannot be combined with index, eq, range bounds, order, unique, first, "
                + "count, distinct, aggregate, paginate, filter, or vector search"
        )
        #expect(
            buildError { $0.search("idx", "q").count() }?.message
                == "search cannot be combined with index, eq, range bounds, order, unique, first, "
                + "count, distinct, aggregate, paginate, filter, or vector search"
        )
        // search+vectorSearch: the vectorSearch check runs BEFORE search, so its
        // message wins (first-match-wins order, same as the server).
        let ranked = buildError { $0.search("idx", "q").vectorSearch("i", [1.0], limit: 5) }
        #expect(ranked?.message == "vectorSearch cannot be combined with any other terminal")
    }

    @Test func takeOutOfRangeThrows() {
        #expect(buildError { $0.take(-1) }?.code == .badRequest)
        #expect(buildError { $0.take(Int(UInt32.max) + 1) }?.code == .badRequest)
        #expect(buildError { $0.paginate(numItems: -5) }?.code == .badRequest)
        #expect(buildError { $0.vectorSearch("i", [1.0], limit: -1) }?.code == .badRequest)
    }

    @Test func legalCombinationsBuild() throws {
        // search + take is the one deliberate terminal+take composition.
        _ = try TableQuery("notes").search("idx", "q").take(10).build()
        // unique composes with index/eq/bounds/filter.
        _ = try TableQuery("items").withIndex("by_a").eq(.string("x"))
            .filter(.gt(field: "n", value: .int(1))).unique().build()
        // collect with filter alone (corpus case 1).
        _ = try TableQuery("workItems").filter(.eq(field: "status", value: .string("done")))
            .collect().build()
        // paginate composes with index/eq/order.
        _ = try TableQuery("items").withIndex("by_b").eq(.string("y")).order(.desc)
            .paginate(cursor: "c", numItems: 20).build()
    }
}

/// parseResult over the server's untagged QueryResult, plus the Paginated wire
/// shape. Kept out of QueryTests so both suites stay under the type-body cap.
struct QueryParseResultTests {
    @Test func parseResultDecodesArray() throws {
        let docs: JSONValue = .array([.object(["id": .string("a")]), .object(["id": .string("b")])])
        let parsed: [Doc] = try parseResult(docs, terminal: .collect)
        #expect(parsed.map(\.id) == ["a", "b"])
    }

    @Test func parseResultObjectDecodesOptionalDoc() throws {
        let payload: JSONValue = .object(["id": .string("a"), "count": .int(1)])
        let doc: Doc2? = try parseResult(payload, terminal: .get)
        #expect(doc?.id == "a")
        #expect(doc?.count == 1)
    }

    @Test func parseResultNullDecodesToNilOptional() throws {
        let doc: Doc? = try parseResult(.null, terminal: .get)
        #expect(doc == nil)
        let unique: Doc? = try parseResult(.null, terminal: .unique)
        #expect(unique == nil)
        let first: Doc? = try parseResult(.null, terminal: .first)
        #expect(first == nil)
    }

    @Test func parseResultDecodesCount() throws {
        let count: Int = try parseResult(.int(42), terminal: .count)
        #expect(count == 42)
    }

    @Test func parseResultDecodesPaginated() throws {
        // Wire keys are `docs`/`nextCursor` (server dsl.rs PaginatedResult);
        // the Swift property is `items` per the SDK surface.
        let payload: JSONValue = .object([
            "docs": .array([.object(["id": .string("a")])]),
            "nextCursor": .string("c1")
        ])
        let page: Paginated<Doc> = try parseResult(payload, terminal: .paginate)
        #expect(page.items.map(\.id) == ["a"])
        #expect(page.nextCursor == "c1")
    }

    @Test func parseResultPaginatedLastPageHasNilCursor() throws {
        let payload: JSONValue = .object(["docs": .array([.object(["id": .string("a")])])])
        let page: Paginated<Doc> = try parseResult(payload, terminal: .paginate)
        #expect(page.items.count == 1)
        #expect(page.nextCursor == nil)
    }

    @Test func parseResultDecodesDistinctValues() throws {
        let values: [String] = try parseResult(
            .array([.string("alice"), .string("bob"), .string("carol")]),
            terminal: .distinct
        )
        #expect(values == ["alice", "bob", "carol"])
    }

    @Test func parseResultDecodesAggregateScalar() throws {
        let total: Double? = try parseResult(.int(42), terminal: .aggregate)
        #expect(total == 42)
        let none: Double? = try parseResult(.null, terminal: .aggregate)
        #expect(none == nil)
        let text: JSONValue? = try parseResult(.string("x"), terminal: .aggregate)
        #expect(text == .string("x"))
    }

    @Test func parseResultDecodesAggregateGroups() throws {
        let payload: JSONValue = .array([
            .object(["key": .string("backlog"), "value": .int(4)]),
            .object(["key": .string("done"), "value": .int(7)])
        ])
        let rows: [AggregateGroup] = try parseResult(payload, terminal: .aggregateGroups)
        #expect(rows.count == 2)
        #expect(rows[0].key == .string("backlog"))
        #expect(rows[0].value == .int(4))
        #expect(rows[1].key == .string("done"))
        #expect(rows[1].value == .int(7))
    }

    @Test func parseResultDecodesRankedTerminalsAsArrays() throws {
        let payload: JSONValue = .array([.object(["id": .string("a")])])
        for terminal in [QueryTerminal.search, .vectorSearch, .hybridSearch] {
            let docs: [Doc] = try parseResult(payload, terminal: terminal)
            #expect(docs.map(\.id) == ["a"])
        }
    }

    @Test func parseResultRejectsShapeMismatches() {
        do {
            _ = try parseResult(.array([]), terminal: .count) as Int
            Issue.record("count terminal with an array payload must throw")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
            #expect(error.message.hasPrefix("invalid query result"))
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
        do {
            _ = try parseResult(.object(["id": .string("a")]), terminal: .collect) as [Doc]
            Issue.record("collect terminal with an object payload must throw")
        } catch let error as RtDbError {
            #expect(error.message.hasPrefix("invalid query result"))
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
        do {
            // paginate payload missing the `docs` key
            _ = try parseResult(.object(["nextCursor": .string("c")]), terminal: .paginate) as Paginated<Doc>
            Issue.record("paginate terminal without a docs array must throw")
        } catch let error as RtDbError {
            #expect(error.message.hasPrefix("invalid query result"))
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
        do {
            _ = try parseResult(.int(3), terminal: .get) as Doc?
            Issue.record("get terminal with a number payload must throw")
        } catch let error as RtDbError {
            #expect(error.message.hasPrefix("invalid query result"))
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
    }

    @Test func parseResultDecodeFailureMapsToRtDbError() {
        do {
            // Doc expects a String id; the payload carries a number.
            _ = try parseResult(.array([.object(["id": .int(7)])]), terminal: .collect) as [Doc]
            Issue.record("mismatched decode must throw")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
            #expect(error.message.hasPrefix("invalid query result"))
        } catch {
            Issue.record("unexpected error type: \(error)")
        }
    }

    @Test func paginatedEncodesUnderDocsKey() throws {
        let page = Paginated<Doc>(items: [Doc(id: "a")], nextCursor: nil)
        let data = try JSONEncoder().encode(page)
        let text = String(data: data, encoding: .utf8) ?? ""
        #expect(text.contains(#""docs":[{"id":"a"}]"#))
        #expect(!text.contains("nextCursor"))
        let withCursor = Paginated<Doc>(items: [], nextCursor: "zzz")
        let text2 = try String(data: JSONEncoder().encode(withCursor), encoding: .utf8) ?? ""
        #expect(text2.contains(#""nextCursor":"zzz""#))
    }
}

/// Test models.
private struct Doc: Codable, Equatable, Sendable {
    var id: String
}

private struct Doc2: Codable, Equatable, Sendable {
    var id: String
    var count: Int
}

/// Runs `build()` on the composed query and returns the RtDbError it threw
/// (nil when it built successfully or threw some other error type).
/// Non-generic on purpose: `#expect` lives at the concrete call sites.
private func buildError(_ compose: (TableQuery) -> TableQuery) -> RtDbError? {
    do {
        _ = try compose(TableQuery("t")).build()
        return nil
    } catch let error as RtDbError {
        return error
    } catch {
        return nil
    }
}
