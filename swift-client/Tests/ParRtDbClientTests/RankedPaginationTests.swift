import Foundation
@testable import ParRtDbClient
import Testing

// ENH-030: `paginate` composes with the three ranked terminals
// (`search`/`vectorSearch`/`hybridSearch`) as a peer clause, the way `take`
// already composes with `search`. Two suites: the builder's wire shapes and
// the in-memory engine's execution semantics. Kept in their own file so
// neither `QueryTests` nor `InMemoryTests` pushes past the type-body cap.

/// The builder: chaining `.paginate(...)` after each ranked terminal produces
/// a wire `Query` carrying BOTH clauses, and `take` stays excluded.
struct RankedPaginateBuilderTests {
    @Test func searchPlusPaginateCarriesBothClauses() throws {
        let obj = try TableQuery("notes").search("search_body", "task")
            .paginate(numItems: 2).build().wireObject()
        #expect(obj == [
            "table": .string("notes"),
            "search": .object(["index": .string("search_body"), "query": .string("task")]),
            "paginate": .object(["numItems": .int(2)])
        ])
    }

    @Test func searchPlusPaginateCursorRoundTripsOnTheWire() throws {
        let obj = try TableQuery("notes").search("search_body", "task")
            .paginate(cursor: "WzEsMiwiYSJd", numItems: 5).build().wireObject()
        #expect(obj["paginate"] == .object([
            "cursor": .string("WzEsMiwiYSJd"), "numItems": .int(5)
        ]))
    }

    @Test func vectorSearchPlusPaginateCarriesBothClauses() throws {
        let obj = try TableQuery("tasks").vectorSearch("vector_embedding", [1.0, 0.0, 0.0], limit: 10)
            .paginate(numItems: 3).build().wireObject()
        #expect(obj == [
            "table": .string("tasks"),
            "vectorSearch": .object([
                "index": .string("vector_embedding"),
                "vector": .array([.int(1), .int(0), .int(0)]),
                "limit": .int(10)
            ]),
            "paginate": .object(["numItems": .int(3)])
        ])
    }

    @Test func hybridSearchPlusPaginateCarriesBothClauses() throws {
        let obj = try TableQuery("docs").hybridSearch("hello", [1.0, 0.0, 0.0], limit: 8)
            .paginate(numItems: 4).build().wireObject()
        #expect(obj == [
            "table": .string("docs"),
            "hybridSearch": .object([
                "query": .string("hello"),
                "vector": .array([.int(1), .int(0), .int(0)]),
                "limit": .int(8)
            ]),
            "paginate": .object(["numItems": .int(4)])
        ])
    }

    /// `take` is still rejected alongside `paginate` — the paginate cascade
    /// runs before the ranked checks, so its message wins (server order).
    @Test func takeStaysExcludedFromPaginate() {
        let error = rankedBuildError {
            $0.search("search_body", "task").take(3).paginate(numItems: 2)
        }
        #expect(error?.code == .badRequest)
        #expect(error?.message == "paginate cannot be combined with take")
    }

    /// The ranked terminals keep rejecting every OTHER peer they always have.
    @Test func otherRankedPeersStillRejected() {
        #expect(
            rankedBuildError { $0.search("i", "q").paginate(numItems: 2).count() }?.message
                == "paginate cannot be combined with count"
        )
        #expect(
            rankedBuildError {
                $0.vectorSearch("i", [1.0], limit: 5).paginate(numItems: 2).order(.asc)
            }?.message == "vectorSearch cannot be combined with any other terminal"
        )
    }
}

/// The in-memory engine's ranked pagination: page splits, cursor resume, and
/// the two terminals whose ranking this engine does not model.
struct RankedPaginateEngineTests {
    // MARK: Fixtures

    private func client() -> InMemoryRtDbClient {
        InMemoryRtDbClient(
            options: InMemoryRtDbClientOptions(now: { 1_700_000_000_000 }, random: { 0 })
        )
    }

    /// The semantics corpus' `notes` shape: one search index over `body`, and
    /// bodies that repeat the term a different number of times so the engine's
    /// lexeme-frequency score is a strict total order.
    private func notesClient(_ bodies: [(String, String)]) throws -> InMemoryRtDbClient {
        let engine = client()
        try engine.pushSchema(
            SchemaBuilder()
                .table("notes") {
                    $0.field("name", .string)
                        .field("body", .string)
                        .searchIndex("search_body", on: ["body"])
                }
                .build()
        )
        try engine.mutate(Transaction(steps: bodies.map { name, body in
            .insert(table: "notes", doc: ["name": .string(name), "body": .string(body)])
        }))
        return engine
    }

    private func tasksClient(_ names: [String]) throws -> InMemoryRtDbClient {
        let engine = client()
        try engine.pushSchema(
            SchemaBuilder()
                .table("tasks") {
                    $0.field("name", .string)
                        .field("embedding", .vector(dimensions: 3))
                        .vectorIndex("vector_embedding", on: "embedding", dimensions: 3)
                }
                .build()
        )
        try engine.mutate(Transaction(steps: names.map { name in
            .insert(table: "tasks", doc: [
                "name": .string(name),
                "embedding": .array([.int(1), .int(0), .int(0)])
            ])
        }))
        return engine
    }

    private func searchQuery(cursor: String?, numItems: UInt32) -> Query {
        Query(
            table: "notes",
            paginate: Paginate(cursor: cursor, numItems: numItems),
            search: SearchQuery(index: "search_body", query: "task")
        )
    }

    private func page(_ value: JSONValue) throws -> (names: [String], nextCursor: String?) {
        guard case let .object(object) = value, case let .array(docs)? = object["docs"] else {
            throw RtDbError(code: .internal, message: "not a paginated envelope: \(value)")
        }
        return (docs.map { $0.objectValue?["name"]?.stringValue ?? "?" },
                object["nextCursor"]?.stringValue)
    }

    // MARK: search + paginate

    /// Pages are non-overlapping, ranked (score DESC), and their concatenation
    /// is exactly the unpaginated ranked result — the assertion that catches a
    /// boundary duplicate or a skipped row.
    @Test func searchPagesConcatenateToTheUnpaginatedRankedResult() throws {
        let engine = try notesClient([
            ("b", "task task"),
            ("a", "task"),
            ("e", "task task task task task"),
            ("d", "task task task task"),
            ("c", "task task task")
        ])
        let unpaginated = try engine.query(
            Query(table: "notes", search: SearchQuery(index: "search_body", query: "task"))
        )
        guard case let .array(allDocs) = unpaginated else {
            Issue.record("unpaginated search did not return an array")
            return
        }
        let ranked = allDocs.map { $0.objectValue?["name"]?.stringValue ?? "?" }
        #expect(ranked == ["e", "d", "c", "b", "a"])

        var walked: [String] = []
        var cursor: String?
        var pages = 0
        repeat {
            let (names, next) = try page(engine.query(searchQuery(cursor: cursor, numItems: 2)))
            walked.append(contentsOf: names)
            cursor = next
            pages += 1
        } while cursor != nil && pages < 10
        #expect(pages == 3)
        #expect(walked == ranked)
        #expect(Set(walked).count == walked.count)
    }

    /// A short final page (1 of 2) carries no `nextCursor`; so does a page that
    /// exactly exhausts the result.
    @Test func lastPageCarriesNoCursor() throws {
        let engine = try notesClient([
            ("b", "task task"), ("a", "task"), ("c", "task task task")
        ])
        let first = try page(engine.query(searchQuery(cursor: nil, numItems: 2)))
        #expect(first.names == ["c", "b"])
        #expect(first.nextCursor != nil)

        let second = try page(engine.query(searchQuery(cursor: first.nextCursor, numItems: 2)))
        #expect(second.names == ["a"])
        #expect(second.nextCursor == nil)

        // Exact-fit page: 3 of 3 leaves nothing to probe, so no cursor either.
        let whole = try page(engine.query(searchQuery(cursor: nil, numItems: 3)))
        #expect(whole.names == ["c", "b", "a"])
        #expect(whole.nextCursor == nil)
    }

    /// The paginated path builds its docs the same way the unpaginated one
    /// does — `_searchSnippet` and the system fields survive paging.
    @Test func pagedDocsKeepSnippetAndSystemFields() throws {
        let engine = try notesClient([("a", "task one"), ("b", "task task two")])
        let result = try engine.query(Query(
            table: "notes",
            paginate: Paginate(numItems: 1),
            search: SearchQuery(index: "search_body", query: "task", snippet: true)
        ))
        guard case let .object(object) = result, case let .array(docs)? = object["docs"],
              let doc = docs.first?.objectValue
        else {
            Issue.record("paginated search did not return an envelope")
            return
        }
        #expect(doc["name"] == .string("b"))
        #expect(doc["_searchSnippet"]?.stringValue?.contains("task") == true)
        #expect(doc["_id"]?.stringValue != nil)
        #expect(doc["_creationTime"] != nil)
    }

    @Test func rankedCursorRejectsWrongShapeAndTypes() throws {
        let engine = try notesClient([("a", "task"), ("b", "task task")])
        // Two values where the ranked sort has three columns.
        #expect(throws: RtDbError.self) {
            _ = try engine.query(
                self.searchQuery(cursor: encodeCursor([.int(1), .string("x")]), numItems: 1)
            )
        }
        // Right arity, wrong type in the ranking-key slot.
        #expect(throws: RtDbError.self) {
            _ = try engine.query(self.searchQuery(
                cursor: encodeCursor([.string("nope"), .int(1), .string("x")]), numItems: 1
            ))
        }
        #expect(throws: RtDbError.self) {
            _ = try engine.query(self.searchQuery(cursor: "not-base64!!", numItems: 1))
        }
    }

    // MARK: vectorSearch + paginate

    /// `limit` is the candidate POOL and `numItems` slices it. This engine does
    /// not model distance, so the pool is the top-`limit` in the deterministic
    /// tie-breaker order (createdAt DESC, id DESC) and the pages walk that.
    @Test func vectorSearchPagesSliceTheCandidatePool() throws {
        let engine = try tasksClient(["a", "b", "c", "d"])
        func vectorQuery(cursor: String?) -> Query {
            Query(
                table: "tasks",
                paginate: Paginate(cursor: cursor, numItems: 2),
                vectorSearch: VectorSearchQuery(
                    index: "vector_embedding", vector: [1.0, 0.0, 0.0], limit: 3
                )
            )
        }
        let first = try page(engine.query(vectorQuery(cursor: nil)))
        // Equal createdAt (pinned clock), so id DESC orders newest-first.
        #expect(first.names == ["d", "c"])
        #expect(first.nextCursor != nil)

        let second = try page(engine.query(vectorQuery(cursor: first.nextCursor)))
        #expect(second.names == ["b"])
        // The pool is 3 of 4 rows, so it is exhausted here — "a" never pages in.
        #expect(second.nextCursor == nil)
    }

    /// The unpaginated terminal is unchanged by the pool restructure (which
    /// turned a break-at-limit loop into collect-then-prefix): a limit at or
    /// above the row count still returns every candidate, and a limit BELOW it
    /// still cuts to exactly that many.
    @Test func vectorSearchWithoutPaginateStillReturnsTheWholeCandidateSet() throws {
        let engine = try tasksClient(["a", "b", "c"])
        func names(_ limit: UInt32) throws -> [String] {
            let result = try engine.query(Query(
                table: "tasks",
                vectorSearch: VectorSearchQuery(
                    index: "vector_embedding", vector: [1.0, 0.0, 0.0], limit: limit
                )
            ))
            guard case let .array(docs) = result else {
                throw RtDbError(code: .internal, message: "vectorSearch did not return an array")
            }
            return docs.compactMap { $0.objectValue?["name"]?.stringValue }
        }
        #expect(try Set(names(5)) == ["a", "b", "c"])
        #expect(try names(2).count == 2)
    }

    // MARK: hybridSearch + paginate

    /// This engine fuses no ranking, so a paginated hybrid is an empty page —
    /// and an empty page is the last one.
    @Test func hybridSearchPaginateReturnsAnEmptyLastPage() throws {
        let engine = try tasksClient(["a", "b"])
        let result = try engine.query(Query(
            table: "tasks",
            paginate: Paginate(numItems: 2),
            hybridSearch: HybridSearchQuery(query: "a", vector: [1.0, 0.0, 0.0], limit: 5)
        ))
        #expect(result == .object(["docs": .array([])]))
    }

    // MARK: terminal naming

    /// `queryTerminal` mirrors server `Query::terminal_name`, which reports the
    /// RANKED terminal (not `paginate`) for a ranked+paginate query.
    @Test func rankedPlusPaginateReportsTheRankedTerminal() {
        let paginate = Paginate(numItems: 2)
        #expect(queryTerminal(Query(
            table: "notes", paginate: paginate,
            search: SearchQuery(index: "search_body", query: "task")
        )) == "search")
        #expect(queryTerminal(Query(
            table: "tasks", paginate: paginate,
            vectorSearch: VectorSearchQuery(index: "v", vector: [1.0], limit: 5)
        )) == "vectorSearch")
        #expect(queryTerminal(Query(
            table: "docs", paginate: paginate,
            hybridSearch: HybridSearchQuery(query: "q", vector: [1.0], limit: 5)
        )) == "hybridSearch")
        // Unchanged for a plain btree paginate.
        #expect(queryTerminal(Query(table: "items", paginate: paginate)) == "paginate")
    }
}

/// Local twin of QueryTests' `buildError` (that one is file-private).
private func rankedBuildError(_ compose: (TableQuery) -> TableQuery) -> RtDbError? {
    do {
        _ = try compose(TableQuery("t")).build()
        return nil
    } catch let error as RtDbError {
        return error
    } catch {
        return nil
    }
}
