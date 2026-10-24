import Foundation
@testable import ParRtDbClient
import Testing

// Query lookup expansion (the server's `Query.lookups`): `.lookup()` builder
// wire shape, compile-time validation, and in-memory expansion. Cross-engine
// agreement is pinned by the `lookup-*` / `query-combo-lookups-*`
// semantics-corpus cases; these tests pin the DSL builder surface and the
// engine seams, mirroring server lookup_test.rs at engine level (the harness
// models no principal, so no per-row-authorization cases).

private func lookupSchema(usersSoftDelete: Bool = false) -> SchemaDef {
    SchemaBuilder()
        .table("users") {
            $0.field("name", .string)
                .index("by_name", on: ["name"])
                .softDelete(usersSoftDelete)
        }
        .table("posts") {
            $0.field("title", .string)
                .field("authorId", .optional(.id(table: "users", onDelete: nil)))
                .index("by_title", on: ["title"])
        }
        .table("tags") {
            $0.field("label", .string)
                .index("by_label", on: ["label"])
        }
        .table("usersWithTags") {
            $0.field("name", .string)
                .field("tagIds", .array(element: .id(table: "tags", onDelete: nil)))
                .index("by_name", on: ["name"])
        }
        .build()
}

private func makeClient() -> InMemoryRtDbClient {
    InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { 1_700_000_000_000 },
        random: { 0 }
    ))
}

extension StepResult {
    /// The minted id of an insert step result.
    var lookupTestId: String {
        if case let .insert(id) = self {
            return id
        }
        return "<not-an-insert>"
    }
}

/// The minted ids `seedPostsAndUsers` returns.
private struct SeededIds {
    let ann: String
    let bo: String
    let p1: String
    let p2: String
    let p3: String
}

/// Seed two users and three posts (p1/p2 authored, p3 with an absent optional
/// authorId); returns the minted ids.
private func seedPostsAndUsers(_ client: InMemoryRtDbClient) throws -> SeededIds {
    let ann = try client.mutate(Transaction(steps: [
        .insert(table: "users", doc: ["name": .string("Ann")])
    ]))[0].lookupTestId
    let bo = try client.mutate(Transaction(steps: [
        .insert(table: "users", doc: ["name": .string("Bo")])
    ]))[0].lookupTestId
    let p1 = try client.mutate(Transaction(steps: [
        .insert(table: "posts", doc: ["title": .string("p1"), "authorId": .string(ann)])
    ]))[0].lookupTestId
    let p2 = try client.mutate(Transaction(steps: [
        .insert(table: "posts", doc: ["title": .string("p2"), "authorId": .string(bo)])
    ]))[0].lookupTestId
    let p3 = try client.mutate(Transaction(steps: [
        .insert(table: "posts", doc: ["title": .string("p3")])
    ]))[0].lookupTestId
    return SeededIds(ann: ann, bo: bo, p1: p1, p2: p2, p3: p3)
}

// MARK: - Wire shape

@Suite("Lookup wire shape")
struct LookupWireTests {
    @Test func lookupsOmitOnEmptyAndCarryAsRename() throws {
        let query = Query(table: "posts")
        let json = try JSONDecoder().decode(
            JSONValue.self, from: JSONEncoder().encode(query)
        )
        #expect(json.objectValue?["lookups"] == nil)

        let withLookup = Query(
            table: "posts",
            lookups: [LookupSpec(
                field: "authorId", table: "users", as: "author", cap: 20
            )]
        )
        let wire = try JSONDecoder().decode(
            JSONValue.self, from: JSONEncoder().encode(withLookup)
        )
        let spec = try #require(wire.objectValue?["lookups"]?.arrayValue?.first)
        #expect(spec.objectValue?["field"] == .string("authorId"))
        #expect(spec.objectValue?["table"] == .string("users"))
        #expect(spec.objectValue?["as"] == .string("author"))
        #expect(spec.objectValue?["cap"] == .int(20))

        // Round-trips through Codable unchanged; cap omitted when nil.
        let decoded = try JSONDecoder().decode(Query.self, from: JSONEncoder().encode(withLookup))
        #expect(decoded == withLookup)
        let noCap = Query(
            table: "posts", lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        )
        let noCapJson = try JSONDecoder().decode(
            JSONValue.self, from: JSONEncoder().encode(noCap)
        )
        #expect(noCapJson.objectValue?["lookups"]?.arrayValue?.first?.objectValue?["cap"] == nil)
    }

    @Test func lookupSpecRejectsUnknownFields() throws {
        let json = """
        {"table":"posts","lookups":[{"field":"authorId","table":"users","as":"author","extra":1}]}
        """
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(Query.self, from: Data(json.utf8))
        }
    }

    @Test func builderAccumulatesLookupSpecs() throws {
        let query = try TableQuery("posts")
            .withIndex("by_title")
            .eq(.string("p1"))
            .lookup(field: "authorId", table: "users", as: "author")
            .lookup(field: "tagIds", table: "tags", as: "tags", cap: 20)
            .build()
        #expect(query.lookups == [
            LookupSpec(field: "authorId", table: "users", as: "author"),
            LookupSpec(field: "tagIds", table: "tags", as: "tags", cap: 20)
        ])
    }
}

// MARK: - Compile-time validation

@Suite("Lookup compile-time validation")
struct LookupValidationTests {
    @Test func moreThanFourLookupsIsBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        let lookups = (0 ..< 5).map {
            LookupSpec(field: "authorId", table: "users", as: "alias\($0)")
        }
        #expect(throws: RtDbError.self) {
            _ = try client.query(Query(table: "posts", lookups: lookups))
        }
        #expect {
            _ = try client.query(Query(table: "posts", lookups: lookups))
        } throws: { error in
            (error as? RtDbError)?.code == .badRequest
        }
    }

    @Test func unknownLookupTableIsNotFound() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "authorId", table: "ghosts", as: "author")]
            ))
        } throws: { error in
            (error as? RtDbError)?.code == .notFound
        }
    }

    @Test func undeclaredOrNonIdFieldIsBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        // Not declared at all...
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "ghostField", table: "users", as: "author")]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("is not declared on table") == true
        }
        // ...and declared but not Id-shaped.
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "title", table: "users", as: "author")]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("must be declared Id") == true
        }
    }

    @Test func tableMismatchIsBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "authorId", table: "tags", as: "author")]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("declares Id table 'users'") == true
        }
    }

    @Test func aliasCollisionsAreBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        // Collides with a declared field.
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "authorId", table: "users", as: "title")]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("collides with a declared field") == true
        }
        // Collides with another lookup's alias.
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [
                    LookupSpec(field: "authorId", table: "users", as: "author"),
                    LookupSpec(field: "authorId", table: "users", as: "author")
                ]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("collides with another lookup alias") == true
        }
        // `_`-prefixed alias.
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(field: "authorId", table: "users", as: "_author")]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("must not start with '_'") == true
        }
    }

    @Test func capOverflowIsBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        #expect {
            _ = try client.query(Query(
                table: "posts",
                lookups: [LookupSpec(
                    field: "authorId", table: "users", as: "author", cap: 1001
                )]
            ))
        } throws: { error in
            let rt = error as? RtDbError
            return rt?.code == .badRequest
                && rt?.message.contains("lookup cap exceeds maximum") == true
        }
    }

    @Test func projectionMayNameLookupAliasesOnly() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        _ = try seedPostsAndUsers(client)
        // The alias is admitted when the query carries its lookup...
        let kept = try client.query(Query(
            table: "posts",
            fields: ["title", "author"],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        #expect(kept.arrayValue?.count == 3)
        // ...and rejected when it does not.
        #expect {
            _ = try client.query(Query(table: "posts", fields: ["author"]))
        } throws: { error in
            (error as? RtDbError)?.code == .badRequest
        }
    }

    @Test func lookupsWithCountDistinctAggregateAreBadRequest() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        let lookups = [LookupSpec(field: "authorId", table: "users", as: "author")]
        for query in [
            Query(table: "posts", count: true, lookups: lookups),
            Query(table: "posts", distinct: true, lookups: lookups),
            Query(table: "posts", aggregate: AggregateSpec(op: .count), lookups: lookups)
        ] {
            #expect {
                _ = try client.query(query)
            } throws: { error in
                (error as? RtDbError)?.code == .badRequest
            }
        }
    }
}

// MARK: - Expansion

@Suite("Lookup expansion semantics")
struct LookupExpansionTests {
    @Test func singleIdLookupJoinsChildWithSystemFields() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        let ids = try seedPostsAndUsers(client)

        // Full docs: the joined doc carries the child table's system fields.
        let full = try client.query(Query(
            table: "posts",
            index: "by_title",
            eq: [.string("p1")],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        let doc = try #require(full.arrayValue?.first?.objectValue)
        #expect(doc["title"] == .string("p1"))
        #expect(doc["author"]?.objectValue?["name"] == .string("Ann"))
        #expect(doc["author"]?.objectValue?["_id"] == .string(ids.ann))
        #expect(doc["author"]?.objectValue?["_creationTime"] != nil)
        #expect(doc["author"]?.objectValue?["_version"] != nil)

        // A null/absent optional Id reads as author:null.
        let p3 = try client.query(Query(
            table: "posts",
            index: "by_title",
            eq: [.string("p3")],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        #expect(
            p3.arrayValue?.first?.objectValue?["author"] == .null
        )

        // get terminal: the single doc gains the joined key.
        let got = try client.query(Query(
            table: "posts",
            get: ids.p1,
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        #expect(got.objectValue?["author"]?.objectValue?["name"] == .string("Ann"))
    }

    @Test func danglingIdReadsAsNullOrIsDropped() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        let ids = try seedPostsAndUsers(client)
        // Remove the referenced user (hard delete: users is not a softDelete
        // table here), leaving p2's authorId dangling.
        _ = try client.mutate(Transaction(steps: [
            .delete(table: "users", id: ids.bo)
        ]))
        let rows = try client.query(Query(
            table: "posts",
            fields: ["title", "author"],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        let byTitle = Dictionary(
            uniqueKeysWithValues: (rows.arrayValue ?? []).compactMap { (row: JSONValue) -> (String, JSONValue)? in
                guard let obj = row.objectValue, let title = obj["title"]?.stringValue else {
                    return nil
                }
                return (title, row)
            }
        )
        #expect(byTitle["p1"]?.objectValue?["author"]?.objectValue?["name"] == .string("Ann"))
        #expect(byTitle["p2"]?.objectValue?["author"] == .null)
        #expect(byTitle["p3"]?.objectValue?["author"] == .null)
    }

    @Test func softDeletedChildIsInvisible() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema(usersSoftDelete: true))
        let ids = try seedPostsAndUsers(client)
        // FM-33: a stamped child row is invisible to the join, like any read.
        _ = try client.mutate(Transaction(steps: [
            .delete(table: "users", id: ids.bo)
        ]))
        let rows = try client.query(Query(
            table: "posts",
            fields: ["title", "author"],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        let byTitle = Dictionary(
            uniqueKeysWithValues: (rows.arrayValue ?? []).compactMap { (row: JSONValue) -> (String, JSONValue)? in
                guard let obj = row.objectValue, let title = obj["title"]?.stringValue else {
                    return nil
                }
                return (title, row)
            }
        )
        #expect(byTitle["p1"]?.objectValue?["author"]?.objectValue?["name"] == .string("Ann"))
        #expect(byTitle["p2"]?.objectValue?["author"] == .null)
    }

    @Test func arrayLookupPreservesOrderAndTruncatesToCap() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        var tagIds: [String] = []
        for label in ["alpha", "bravo", "charlie", "delta"] {
            try tagIds.append(client.mutate(Transaction(steps: [
                .insert(table: "tags", doc: ["label": .string(label)])
            ]))[0].lookupTestId)
        }
        _ = try client.mutate(Transaction(steps: [
            .insert(table: "usersWithTags", doc: [
                "name": .string("U"),
                // Declared order: charlie, alpha, delta, bravo — cap keeps the
                // first two.
                "tagIds": .array([
                    .string(tagIds[2]), .string(tagIds[0]), .string(tagIds[3]), .string(tagIds[1])
                ])
            ])
        ]))[0].lookupTestId

        // cap: 2 — joined docs in the id-array's declared order.
        let capped = try client.query(Query(
            table: "usersWithTags",
            fields: ["name", "tags"],
            lookups: [LookupSpec(field: "tagIds", table: "tags", as: "tags", cap: 2)]
        ))
        let tags = try #require(capped.arrayValue?.first?.objectValue?["tags"]?.arrayValue)
        #expect(
            tags.compactMap { $0.objectValue?["label"] }
                == [.string("charlie"), .string("alpha")]
        )
    }

    @Test func arrayLookupDropsDanglingIdsAndReadsEmptyArrayAsEmpty() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        var tagIds: [String] = []
        for label in ["alpha", "bravo", "delta"] {
            try tagIds.append(client.mutate(Transaction(steps: [
                .insert(table: "tags", doc: ["label": .string(label)])
            ]))[0].lookupTestId)
        }
        _ = try client.mutate(Transaction(steps: [
            .insert(table: "usersWithTags", doc: [
                "name": .string("U"),
                // Declared order with a dangling third id (delta is deleted
                // below) — dropped from the joined output.
                "tagIds": .array([
                    .string(tagIds[2]), .string(tagIds[0]), .string(tagIds[1])
                ])
            ])
        ]))
        // Hard-delete delta so the first id in the user's array dangles, then
        // verify the joined output keeps declared order minus the dangling id.
        _ = try client.mutate(Transaction(steps: [
            .delete(table: "tags", id: tagIds[2])
        ]))
        let rows = try client.query(Query(
            table: "usersWithTags",
            index: "by_name",
            eq: [.string("U")],
            fields: ["name", "tags"],
            lookups: [LookupSpec(field: "tagIds", table: "tags", as: "tags")]
        ))
        let labels = try #require(rows.arrayValue?.first?.objectValue?["tags"]?.arrayValue)
        #expect(
            labels.compactMap { $0.objectValue?["label"] }
                == [.string("alpha"), .string("bravo")]
        )

        // An empty id-array reads as an empty joined array.
        _ = try client.mutate(Transaction(steps: [
            .insert(table: "usersWithTags", doc: ["name": .string("Bare"), "tagIds": .array([])])
        ]))
        let bareRows = try client.query(Query(
            table: "usersWithTags",
            index: "by_name",
            eq: [.string("Bare")],
            fields: ["name", "tags"],
            lookups: [LookupSpec(field: "tagIds", table: "tags", as: "tags")]
        ))
        #expect(
            bareRows.arrayValue?.first?.objectValue?["tags"] == .array([])
        )
    }

    @Test func projectionKeepsAliasOnlyWhenListed() throws {
        let client = makeClient()
        try client.pushSchema(lookupSchema())
        _ = try seedPostsAndUsers(client)
        // `fields` listing the alias keeps the joined key and drops the rest.
        let aliasOnly = try client.query(Query(
            table: "posts",
            fields: ["author"],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        let row = try #require(aliasOnly.arrayValue?.first?.objectValue)
        #expect(row["author"] != nil)
        #expect(row["title"] == nil)
        // Omitting the alias drops the joined docs.
        let dropped = try client.query(Query(
            table: "posts",
            fields: ["title"],
            lookups: [LookupSpec(field: "authorId", table: "users", as: "author")]
        ))
        #expect(dropped.arrayValue?.first?.objectValue?["author"] == nil)
    }
}
