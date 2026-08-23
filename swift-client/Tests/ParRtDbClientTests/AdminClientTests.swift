import Foundation
@testable import ParRtDbClient
import Testing

// MARK: - URLProtocol stub machinery (same pattern as HttpClientTests)

/// Assertion failure thrown from inside a stub handler. Surfacing it through
/// `URLProtocol(_:didFailWithError:)` fails the URLSession request, so the
/// test's `await` throws and the failure lands in the test result.
private struct StubFailure: Error, CustomStringConvertible {
    let description: String

    init(_ description: String) {
        self.description = description
    }
}

private func demand(_ condition: Bool, _ message: String) throws {
    guard condition else { throw StubFailure(message) }
}

/// Intercepts every request from the stubbed session. One static handler per
/// test (the suite is `.serialized`, so installs never race).
private final class StubProtocol: URLProtocol {
    nonisolated(unsafe) static var handler: ((URLRequest) throws -> (Int, Data))?

    override static func canInit(with _: URLRequest) -> Bool {
        true
    }

    override static func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let handler = Self.handler else {
            client?.urlProtocol(self, didFailWithError: StubFailure("no stub handler installed"))
            return
        }
        do {
            let (status, body) = try handler(request)
            let response = HTTPURLResponse(
                url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil
            )!
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: body)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

/// The request body as bytes. URLProtocol presents request bodies as a stream
/// (`httpBody` is nil by the time the protocol sees them), so drain
/// `httpBodyStream` when `httpBody` is absent.
private func stubRequestBody(_ request: URLRequest) -> Data {
    if let body = request.httpBody {
        return body
    }
    guard let stream = request.httpBodyStream else { return Data() }
    stream.open()
    defer { stream.close() }
    var data = Data()
    let capacity = 4096
    let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: capacity)
    defer { buffer.deallocate() }
    while stream.hasBytesAvailable {
        let read = stream.read(buffer, maxLength: capacity)
        guard read > 0 else { break }
        data.append(buffer, count: read)
    }
    return data
}

private func jsonObjectBody(_ request: URLRequest) throws -> [String: Any] {
    let data = stubRequestBody(request)
    do {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw StubFailure("request body is not a JSON object")
        }
        return object
    } catch let failure as StubFailure {
        throw failure
    } catch {
        throw StubFailure("request body is not valid JSON: \(error)")
    }
}

private func makeAdminClient() -> RtDbAdminClient {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [StubProtocol.self]
    return RtDbAdminClient(
        url: "http://rtdb.test/", adminKey: "root-key", session: URLSession(configuration: config)
    )
}

/// Asserts the admin bearer on every request — folded into each handler via
/// `adminAuth(request)`.
private func adminAuth(_ request: URLRequest) throws {
    try demand(
        request.value(forHTTPHeaderField: "Authorization") == "Bearer root-key",
        "missing admin-key bearer"
    )
}

// MARK: - Tests

/// Serialized: every test installs the one shared `StubProtocol.handler` —
/// splitting the suite would run the halves in parallel and race on it.
/// A representative port of rust-client/src/admin/tests.rs: one happy path
/// (with exact route/payload assertions) plus one error-envelope case per
/// route group.
@Suite(.serialized)
struct AdminClientTests {
    // MARK: db lifecycle / clone / export / import

    @Test func createDbPostsName() async throws {
        StubProtocol.handler = { request in
            try adminAuth(request)
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/create-db", "expected /admin/create-db")
            let body = try jsonObjectBody(request)
            try demand(body["name"] as? String == "app2", "name mismatch: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().createDb("app2")
    }

    @Test func deleteDbSurfacesConfirmationMismatchEnvelope() async throws {
        StubProtocol.handler = { _ in
            (
                400,
                Data(#"{"code":"BAD_REQUEST","message":"confirm must equal name"}"#.utf8)
            )
        }
        do {
            try await makeAdminClient().deleteDb("app", confirm: "wrong")
            Issue.record("deleteDb should throw on a confirmation mismatch")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "confirm must equal name")
        }
    }

    @Test func listDbsReturnsDatabases() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(request.url?.path == "/admin/dbs", "expected /admin/dbs")
            return (200, Data(#"{"databases":["app","app2"]}"#.utf8))
        }
        let dbs = try await makeAdminClient().listDbs()
        #expect(dbs == ["app", "app2"])
    }

    @Test func exportDbReturnsJsonlText() async throws {
        StubProtocol.handler = { request in
            try adminAuth(request)
            try demand(request.url?.path == "/admin/export-db", "expected /admin/export-db")
            try demand(request.url?.query == "db=app", "query mismatch: \(request.url?.query ?? "nil")")
            return (200, Data(#"{"kind":"schema"}\n{"t":[1,2]}\n"#.utf8))
        }
        let jsonl = try await makeAdminClient().exportDb("app")
        #expect(jsonl == #"{"kind":"schema"}\n{"t":[1,2]}\n"#)
    }

    @Test func importDbPostsNdjsonBody() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/import-db", "expected /admin/import-db")
            try demand(request.url?.query == "db=app", "query mismatch")
            try demand(
                request.value(forHTTPHeaderField: "Content-Type") == "application/x-ndjson",
                "Content-Type mismatch"
            )
            try demand(
                stubRequestBody(request) == Data(#"{"kind":"schema"}"#.utf8),
                "ndjson body mismatch"
            )
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().importDb("app", jsonl: #"{"kind":"schema"}"#)
    }

    @Test func cloneDbPostsFromToQuery() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/clone-db", "expected /admin/clone-db")
            try demand(request.url?.query == "from=a&to=b", "query mismatch")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().cloneDb(from: "a", to: "b")
    }

    // MARK: schema plane

    @Test func pushSchemaSerializesSchemaJson() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/push-schema", "expected /admin/push-schema")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let tables = (body["schema"] as? [String: Any])?["tables"] as? [String: Any]
            try demand(tables?["users"] != nil, "schema.tables.users missing: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().pushSchema(
            db: "app",
            schema: SchemaDef(tables: ["users": TableDef(fields: ["name": .string])])
        )
    }

    @Test func previewSchemaPostsSchemaAndParsesDiff() async throws {
        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/schema/preview",
                "expected /admin/db/app/schema/preview"
            )
            let body = try jsonObjectBody(request)
            try demand(body["schema"] != nil, "schema missing: \(body)")
            try demand(body["db"] == nil, "db rides the path, not the body: \(body)")
            let diff = """
            {"added":[{"table":"users","columns":[{"name":"email","fieldType":"string"}],
            "indexes":[{"name":"by_email","fields":["email"]}]}],
            "rejected":[{"table":"old","item":"gone","reason":"drop refused"}]}
            """
            return (200, Data(diff.utf8))
        }
        let diff = try await makeAdminClient().previewSchema(
            db: "app", schema: SchemaDef(tables: [:])
        )
        #expect(diff.added.count == 1)
        #expect(diff.added[0].table == "users")
        #expect(diff.added[0].columns == [SchemaPreviewColumnAdd(name: "email", fieldType: "string")])
        #expect(diff.added[0].indexes == [SchemaPreviewIndexAdd(name: "by_email", fields: ["email"])])
        #expect(diff.rejected == [SchemaPreviewRejection(table: "old", item: "gone", reason: "drop refused")])
    }

    @Test func migrateSchemaPostsDirectivesAndDryRun() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/migrate", "expected /admin/db/app/migrate")
            let body = try jsonObjectBody(request)
            let directives = body["directives"] as? [[String: Any]]
            try demand(directives?.count == 2, "directives mismatch: \(body)")
            try demand(directives?[0]["op"] as? String == "renameField", "op[0] mismatch")
            try demand(directives?[1]["op"] as? String == "dropTable", "op[1] mismatch")
            try demand((body["dryRun"] as? Bool) == true, "dryRun mismatch: \(body)")
            let result = """
            {"applied":false,"schema":{"tables":{"users":{"fields":{"fullName":{"type":"string"}},
            "indexes":[]}}},"directives":[{"op":"renameField","affectedRows":3}]}
            """
            return (200, Data(result.utf8))
        }
        let result = try await makeAdminClient().migrateSchema(
            db: "app",
            directives: [
                .renameField(table: "users", from: "name", to: "fullName"),
                .dropTable(name: "gone")
            ],
            dryRun: true
        )
        #expect(result.applied == false)
        #expect(result.schema.tables["users"]?.fields["fullName"] == .string)
        #expect(result.directives == [DirectiveReport(op: "renameField", affectedRows: 3)])
    }

    @Test func getSchemaReturnsSchemaDef() async throws {
        StubProtocol.handler = { request in
            // Plural dbs on the read side (the singular db paths are the
            // action routes) — pinned here like in rust's test.
            try demand(request.url?.path == "/admin/dbs/app/schema", "expected /admin/dbs/app/schema")
            return (
                200,
                Data(#"{"tables":{"users":{"fields":{"name":{"type":"string"}},"indexes":[]}}}"#.utf8)
            )
        }
        let schema = try await makeAdminClient().getSchema("app")
        #expect(schema.tables["users"]?.fields["name"] == .string)
    }

    @Test func schemaHistoryAndRestoreHitTheirRoutes() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/schema/history",
                "expected /admin/db/app/schema/history"
            )
            try demand(
                request.url?.query == "limit=10&offset=5",
                "query mismatch: \(request.url?.query ?? "nil")"
            )
            return (
                200,
                Data(
                    #"{"entries":[{"version":2,"capturedAt":9,"source":"migrate","principal":"u1"},"#.utf8
                )
                    + Data(#"{"version":1,"capturedAt":5,"source":"push"}]}"#.utf8)
            )
        }
        let history = try await client.schemaHistory("app", limit: 10, offset: 5)
        #expect(history.count == 2)
        #expect(history[0] == SchemaHistorySummary(version: 2, capturedAt: 9, source: "migrate", principal: "u1"))
        #expect(history[1].principal == nil)

        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/schema/history/2",
                "expected /admin/db/app/schema/history/2"
            )
            return (
                200,
                Data(#"{"version":2,"capturedAt":9,"source":"migrate","schema":{"tables":{}}}"#.utf8)
            )
        }
        let entry = try await client.schemaHistoryGet("app", version: 2)
        #expect(entry.version == 2)
        #expect(entry.schema.objectValue?["tables"] == .object([:]))

        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/schema/restore",
                "expected /admin/db/app/schema/restore"
            )
            let body = try jsonObjectBody(request)
            try demand((body["version"] as? Int) == 2, "version mismatch: \(body)")
            try demand(body["confirm"] as? String == "app", "confirm mismatch: \(body)")
            return (200, Data(#"{"ok":true,"restoredTo":2}"#.utf8))
        }
        let restoredTo = try await client.restoreSchema("app", version: 2, confirm: "app")
        #expect(restoredTo == 2)
    }

    @Test func dbStatsReturnsTableStatsAndQuotas() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/dbs/app/stats", "expected /admin/dbs/app/stats")
            let stats = """
            {"tables":[{"name":"users","rowCount":7,"sizeBytes":8192}],"totalSizeBytes":8192,
            "tablesQuota":10,"tablesUsed":1,"storageQuotaBytes":0,"storageUsedBytes":0,
            "subsQuota":100,"subsUsed":3}
            """
            return (200, Data(stats.utf8))
        }
        let stats = try await makeAdminClient().dbStats("app")
        #expect(stats.tables == [TableStat(name: "users", rowCount: 7, sizeBytes: 8192)])
        #expect(stats.tablesQuota == 10)
        #expect(stats.subsUsed == 3)
    }

    // MARK: tokens / allowlist / admins

    @Test func mintTokenWithOptionsPostsCapabilities() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/mint-token", "expected /admin/mint-token")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            try demand(body["name"] as? String == "ci", "name mismatch: \(body)")
            try demand((body["expiresAt"] as? Int) == 99, "expiresAt mismatch: \(body)")
            try demand((body["readOnly"] as? Bool) == true, "readOnly mismatch: \(body)")
            try demand(body["tables"] as? [String] == ["users"], "tables mismatch: \(body)")
            return (200, Data(#"{"tokenId":"t1","token":"tok_abc"}"#.utf8))
        }
        let minted = try await makeAdminClient().mintToken(
            "app", name: "ci",
            options: MintTokenOptions(expiresAt: 99, readOnly: true, tables: ["users"])
        )
        #expect(minted == MintedToken(tokenId: "t1", token: "tok_abc"))
    }

    @Test func mintTokenOmitsUnsetCapabilities() async throws {
        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            try demand(
                Set(body.keys) == ["db", "name"],
                "only db+name must be sent, got \(body.keys.sorted())"
            )
            return (200, Data(#"{"tokenId":"t1","token":"tok"}"#.utf8))
        }
        _ = try await makeAdminClient().mintToken("app", name: "ci")
    }

    @Test func revokeTokenPostsTokenId() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/revoke-token", "expected /admin/revoke-token")
            let body = try jsonObjectBody(request)
            try demand(body["tokenId"] as? String == "t1", "tokenId mismatch: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().revokeToken("t1")
    }

    @Test func allowlistAddPostsActionAndListUsesQuery() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/allowlist", "expected /admin/allowlist")
            let body = try jsonObjectBody(request)
            try demand(body["action"] as? String == "add", "action mismatch: \(body)")
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            try demand(body["email"] as? String == "a@b.com", "email mismatch: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.allowlistAdd(db: "app", email: "a@b.com")

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(request.url?.query == "db=app", "query mismatch")
            return (200, Data(#"{"emails":["a@b.com"]}"#.utf8))
        }
        let emails = try await client.allowlistList(db: "app")
        #expect(emails == ["a@b.com"])
    }

    @Test func adminsListUnwrapsAndRemoveDeletesWithBody() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(request.url?.path == "/admin/admins", "expected /admin/admins")
            return (
                200,
                Data(#"{"admins":[{"email":"a@b.com","githubId":42},{"email":"c@d.com"}]}"#.utf8)
            )
        }
        let admins = try await client.adminsList()
        #expect(admins == [
            AdminMember(email: "a@b.com", githubId: 42),
            AdminMember(email: "c@d.com", githubId: nil)
        ])

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/admin/admins", "expected /admin/admins")
            let body = try jsonObjectBody(request)
            try demand(body["email"] as? String == "a@b.com", "email mismatch: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.adminsRemove(email: "a@b.com")
    }

    @Test func listTokensReturnsCapabilityFields() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/tokens", "expected /admin/tokens")
            try demand(request.url?.query == "db=app", "query mismatch")
            let tokens = """
            {"tokens":[{"id":"t1","name":"ci","createdAt":5,"revoked":false,
            "expiresAt":99,"readOnly":true,"tables":["users"]}]}
            """
            return (200, Data(tokens.utf8))
        }
        let tokens = try await makeAdminClient().listTokens("app")
        #expect(tokens.count == 1)
        #expect(tokens[0].readOnly)
        #expect(tokens[0].tables == ["users"])
        #expect(tokens[0].expiresAt == 99)
    }

    // MARK: metrics / config / op feed

    @Test func metricsReturnsSnapshot() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/metrics", "expected /admin/metrics")
            let snapshot = """
            {"queriesTotal":10,"mutationsTotal":4,"uploadsTotal":1,"wsConnections":2,
            "activeSubscriptions":3,"poolSize":8,"poolIdle":5,"uptimeSeconds":600,
            "queryLatency":{"p50":1,"p95":2,"p99":3},"mutateLatency":{"p50":4,"p95":5,"p99":6},
            "subscribeLatency":{"p50":7,"p95":8,"p99":9},"subsRerunsTotal":11,
            "subsSkipsPointTotal":12,"subsSkipsIndexedTotal":13,"subsSkipsOrderedTotal":14,
            "subsMissedPushesTotal":0,"perDbSubs":[{"db":"app","reruns":1,"skipsPoint":2,
            "skipsIndexed":3,"skipsOrdered":4,"missed":0,"skips":9,"rerunRatio":0.1}]}
            """
            return (200, Data(snapshot.utf8))
        }
        let metrics = try await makeAdminClient().metrics()
        #expect(metrics.queriesTotal == 10)
        #expect(metrics.queryLatency == LatencyStats(p50: 1, p95: 2, p99: 3))
        #expect(metrics.subsRerunsTotal == 11)
        #expect(metrics.perDbSubs.count == 1)
        #expect(metrics.perDbSubs[0].skips == 9)
    }

    @Test func listSubscriptionsScopesByDbQuery() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/subscriptions", "expected /admin/subscriptions")
            try demand(request.url?.query == "db=app", "query mismatch")
            let body = """
            {"subscriptions":[{"db":"app","table":"users","terminal":"take",
            "readSetClass":"indexed","principal":{"userId":"u1","email":"a@b.com"}}],
            "subsRerunsTotal":1,"subsSkipsPointTotal":2,"subsSkipsIndexedTotal":3,
            "subsSkipsOrderedTotal":4,"subsMissedPushesTotal":0,"perDb":[]}
            """
            return (200, Data(body.utf8))
        }
        let response = try await makeAdminClient().listSubscriptions(db: "app")
        #expect(response.subscriptions.count == 1)
        #expect(response.subscriptions[0].readSetClass == "indexed")
        #expect(response.subscriptions[0].principal?.email == "a@b.com")
    }

    @Test func patchConfigPatchesAndReturnsConfig() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "PATCH", "expected PATCH")
            try demand(request.url?.path == "/admin/config", "expected /admin/config")
            let body = try jsonObjectBody(request)
            try demand(
                Set(body.keys) == ["sessionTtlDays"],
                "only the patched key must be sent, got \(body.keys.sorted())"
            )
            try demand((body["sessionTtlDays"] as? Int) == 7, "sessionTtlDays mismatch: \(body)")
            let config = """
            {"port":8300,"publicUrl":"http://x","githubBaseUrl":"https://gh",
            "githubApiUrl":"https://api","databaseUrlConfigured":true,
            "adminKeyConfigured":true,"githubConfigured":false,"googleConfigured":false,
            "gitlabConfigured":false,"oidcConfigured":false,
            "hot":{"allowedOrigins":[],"sessionTtlDays":7,"maxFileSize":1000,
            "idempotencyTtlMs":10,"maxTablesPerDb":10,"maxStorageBytesPerDb":0,"maxSubsPerDb":0},
            "version":"1.0.0","gitCommit":"abc","admins":[]}
            """
            return (200, Data(config.utf8))
        }
        let config = try await makeAdminClient().patchConfig(HotConfigPatch(sessionTtlDays: 7))
        #expect(config.hot.sessionTtlDays == 7)
        #expect(config.admins.isEmpty)
    }

    @Test func patchConfigSurfaces400Envelope() async throws {
        StubProtocol.handler = { _ in
            (400, Data(#"{"code":"BAD_REQUEST","message":"unknown field"}"#.utf8))
        }
        do {
            _ = try await makeAdminClient().patchConfig(HotConfigPatch(maxFileSize: 1))
            Issue.record("patchConfig should throw on a 400 envelope")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "unknown field")
        }
    }

    @Test func opsRecentBuildsQueryParams() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/ops/recent", "expected /admin/ops/recent")
            try demand(
                request.url?.query == "db=app&table=users&n=5",
                "query mismatch: \(request.url?.query ?? "nil")"
            )
            let ops = """
            {"ops":[{"db":"app","table":"users","docId":"u1","kind":"insert","ts":9,
            "owner":"u1"},{"db":"app","table":"users","docId":"u2","kind":"ttl","ts":10}]}
            """
            return (200, Data(ops.utf8))
        }
        let ops = try await makeAdminClient().opsRecent("app", table: "users", count: 5)
        #expect(ops.count == 2)
        #expect(ops[0].owner == "u1")
        #expect(ops[1].owner == nil)
    }

    // MARK: admin query / mutate / explain / slow queries

    @Test func adminQueryPostsToSingularPathAndUnwrapsResult() async throws {
        StubProtocol.handler = { request in
            // Singular `db` on the action routes (vs the plural `dbs` reads).
            try demand(request.url?.path == "/admin/db/app/query", "expected /admin/db/app/query")
            let body = try jsonObjectBody(request)
            try demand(body["db"] == nil, "db rides the path, not the body: \(body)")
            try demand(body["includeDeleted"] == nil, "includeDeleted must be omitted when nil")
            let query = body["query"] as? [String: Any]
            try demand(query?["table"] as? String == "items", "query.table mismatch: \(body)")
            try demand((query?["take"] as? Int) == 5, "query.take mismatch: \(body)")
            return (200, Data(#"{"result":[{"_id":"a"},{"_id":"b"}]}"#.utf8))
        }
        let query = try TableQuery("items").take(5).build()
        let docs: [JSONValue] = try await makeAdminClient().adminQuery(
            "app", query, as: [JSONValue].self
        )
        #expect(docs.count == 2)
    }

    @Test func adminQueryIncludesIncludeDeletedWhenSet() async throws {
        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            try demand((body["includeDeleted"] as? Bool) == true, "includeDeleted mismatch: \(body)")
            return (200, Data(#"{"result":[]}"#.utf8))
        }
        let query = try TableQuery("items").take(5).build()
        let docs: [JSONValue] = try await makeAdminClient().adminQuery(
            "app", query, includeDeleted: true, as: [JSONValue].self
        )
        #expect(docs.isEmpty)
    }

    @Test func adminMutatePostsTxnAndParsesStepResults() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/mutate", "expected /admin/db/app/mutate")
            let body = try jsonObjectBody(request)
            try demand(body["db"] == nil, "db rides the path, not the body: \(body)")
            try demand(body["idempotencyKey"] == nil, "idempotencyKey must be omitted when nil")
            let steps = (body["txn"] as? [String: Any])?["steps"] as? [[String: Any]]
            try demand(steps?.count == 1, "txn.steps mismatch: \(body)")
            try demand(steps?.first?["op"] as? String == "insert", "steps[0].op mismatch")
            return (200, Data(#"{"results":[{"id":"new1"}]}"#.utf8))
        }
        let txn = try MutationBuilder().insert("items", ["n": .int(1)]).build()
        let results = try await makeAdminClient().adminMutate("app", txn)
        #expect(results == [.insert(id: "new1")])
    }

    @Test func explainQueryPostsAndDeserializes() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/explain", "expected /admin/db/app/explain")
            let body = try jsonObjectBody(request)
            try demand(body["query"] != nil, "query missing: \(body)")
            let explain = """
            {"sql":"SELECT * FROM items","params":["a"],"terminal":"take",
            "warnings":["filter field not indexed"]}
            """
            return (200, Data(explain.utf8))
        }
        let query = try TableQuery("items").take(5).build()
        let explain = try await makeAdminClient().explainQuery("app", query)
        #expect(explain.sql == "SELECT * FROM items")
        #expect(explain.params == ["a"])
        #expect(explain.warnings == ["filter field not indexed"])
    }

    @Test func getSlowQueriesPassesDbAndLimit() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/slow-queries", "expected /admin/slow-queries")
            try demand(request.url?.query == "db=app&limit=3", "query mismatch")
            let slow = """
            {"queries":[{"startedAtMs":9,"durationMs":250,"db":"app","table":"items",
            "terminal":"collect","sql":"SELECT 1"}],"thresholdMs":200,"capacity":100}
            """
            return (200, Data(slow.utf8))
        }
        let slow = try await makeAdminClient().getSlowQueries(db: "app", limit: 3)
        #expect(slow.thresholdMs == 200)
        #expect(slow.queries.count == 1)
        #expect(slow.queries[0].params == nil)
    }

    // MARK: backups

    @Test func backupNowPostsEmptyJsonObject() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/backup", "expected /admin/backup")
            try demand(stubRequestBody(request) == Data("{}".utf8), "body must be {}")
            return (202, Data(#"{"ok":true}"#.utf8))
        }
        try await makeAdminClient().backupNow()
    }

    @Test func listBackupsParsesRunningAndEntries() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/backups", "expected /admin/backups")
            let backups = """
            {"running":false,"backups":[{"name":"dump-1.sql","sizeBytes":1024,"createdMs":9}]}
            """
            return (200, Data(backups.utf8))
        }
        let backups = try await makeAdminClient().listBackups()
        #expect(backups.running == false)
        #expect(backups.backups == [BackupFile(name: "dump-1.sql", sizeBytes: 1024, createdMs: 9)])
    }

    @Test func downloadBackupReturnsRawBytesAndSurfacesEnvelope() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try adminAuth(request)
            try demand(request.url?.path == "/admin/backups/dump-1.sql", "path mismatch")
            // Binary pg_dump output must not be JSON-decoded.
            return (200, Data([0x1F, 0x8B, 0x00, 0xFF]))
        }
        let bytes = try await client.downloadBackup("dump-1.sql")
        #expect(bytes == Data([0x1F, 0x8B, 0x00, 0xFF]))

        StubProtocol.handler = { _ in
            (404, Data(#"{"code":"NOT_FOUND","message":"no such dump"}"#.utf8))
        }
        do {
            _ = try await client.downloadBackup("gone.sql")
            Issue.record("downloadBackup should throw on a 404 envelope")
        } catch let error as RtDbError {
            #expect(error.code == .notFound)
        }
    }

    @Test func deleteBackupAcceptsNoContentAndRestoreSendsConfirm() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/admin/backups/dump-1.sql", "path mismatch")
            return (204, Data())
        }
        try await client.deleteBackup("dump-1.sql")

        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/restore", "expected /admin/restore")
            let body = try jsonObjectBody(request)
            try demand(body["name"] as? String == "dump-1.sql", "name mismatch: \(body)")
            try demand(body["confirm"] as? String == "dump-1.sql", "confirm mismatch: \(body)")
            return (
                200,
                Data(#"{"target":"rtdb_restored_9","instructions":"cut over now"}"#.utf8)
            )
        }
        let restored = try await client.restoreBackup("dump-1.sql")
        #expect(restored == RestoreResult(target: "rtdb_restored_9", instructions: "cut over now"))
    }

    // MARK: webhooks

    @Test func createWebhookPostsOptionsAndOmitsUnset() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/webhooks", "path mismatch")
            let body = try jsonObjectBody(request)
            try demand(body["url"] as? String == "https://x/hook", "url mismatch: \(body)")
            try demand(body["table"] as? String == "users", "table mismatch: \(body)")
            try demand(body["events"] as? [String] == ["insert"], "events mismatch: \(body)")
            try demand((body["enabled"] as? Bool) == false, "enabled mismatch: \(body)")
            try demand(body["rotateSecret"] == nil, "rotateSecret is edit-only: \(body)")
            return (200, Data(#"{"id":7}"#.utf8))
        }
        let id = try await client.createWebhook(
            "app",
            options: CreateWebhookOptions(
                url: "https://x/hook", table: "users", events: ["insert"], enabled: false
            )
        )
        #expect(id == 7)

        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            try demand(Set(body.keys) == ["url"], "only url must be sent, got \(body.keys.sorted())")
            return (200, Data(#"{"id":8}"#.utf8))
        }
        _ = try await client.createWebhook("app", options: CreateWebhookOptions(url: "https://x/hook"))
    }

    @Test func editWebhookTriStatesTheTableFilter() async throws {
        let client = makeAdminClient()
        // Some(Some) sets the filter; Some(nil) clears it via JSON null.
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "PUT", "expected PUT")
            try demand(request.url?.path == "/admin/db/app/webhooks/7", "path mismatch")
            let body = try jsonObjectBody(request)
            try demand(body["table"] as? String == "users", "table mismatch: \(body)")
            return (
                200,
                Data(
                    #"{"id":7,"db":"app","table":"users","url":"https://x/hook","events":["*"],"#.utf8
                )
                    + Data(#""createdAt":9,"enabled":true,"secret":"s1"}"#.utf8)
            )
        }
        var webhook = try await client.editWebhook(
            "app", id: 7, options: WebhookEditOptions(table: .some("users"))
        )
        #expect(webhook.table == "users")
        #expect(webhook.enabled)
        #expect(webhook.secret == "s1")

        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            // NSNull — the JSON null that clears the filter server-side.
            try demand(body["table"] is NSNull, "table must be JSON null to clear: \(body)")
            return (
                200,
                Data(
                    #"{"id":7,"db":"app","url":"https://x/hook","events":["*"],"createdAt":9,"#.utf8
                )
                    + Data(#""enabled":true}"#.utf8)
            )
        }
        webhook = try await client.editWebhook(
            "app", id: 7, options: WebhookEditOptions(table: .some(nil))
        )
        #expect(webhook.table == nil)

        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            try demand(body["table"] == nil, "nil table must be omitted entirely: \(body)")
            try demand((body["rotateSecret"] as? Bool) == true, "rotateSecret mismatch: \(body)")
            return (
                200,
                Data(
                    #"{"id":7,"db":"app","url":"https://x/hook","events":["*"],"createdAt":9,"#.utf8
                )
                    + Data(#""enabled":true}"#.utf8)
            )
        }
        _ = try await client.editWebhook(
            "app", id: 7, options: WebhookEditOptions(rotateSecret: true)
        )
    }

    @Test func deleteWebhookHitsPathAndListDeliveriesBuildsQuery() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/admin/db/app/webhooks/7", "path mismatch")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.deleteWebhook("app", id: 7)

        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/webhooks/7/deliveries",
                "path mismatch: \(request.url?.path ?? "nil")"
            )
            try demand(
                request.url?.query == "status=pending&limit=10&offset=5",
                "query mismatch: \(request.url?.query ?? "nil")"
            )
            let deliveries = """
            {"deliveries":[{"id":3,"attempts":1,"status":"pending","nextAttempt":99,
            "payload":{"op":"insert"}}]}
            """
            return (200, Data(deliveries.utf8))
        }
        let deliveries = try await client.listDeliveries(
            "app", id: 7,
            options: ListDeliveriesOptions(status: "pending", limit: 10, offset: 5)
        )
        #expect(deliveries.count == 1)
        #expect(deliveries[0].lastError == nil)
        #expect(deliveries[0].payload.objectValue?["op"] == .string("insert"))
    }

    // MARK: audit / sessions / merge

    @Test func getAuditBuildsQueryParamsFromOptions() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/audit", "expected /admin/audit")
            try demand(
                request.url?.query == "db=app&table=users&op=insert&principal=u1&source=mutate&limit=50&offset=10",
                "query mismatch: \(request.url?.query ?? "nil")"
            )
            let entries = """
            {"entries":[{"id":1,"tsMs":9,"db":"app","table":"users","op":"insert","docId":"u1",
            "principal":"u1","source":"mutate"},{"id":2,"tsMs":10,"db":"app","table":"users",
            "docId":"u2","source":"ttl"}]}
            """
            return (200, Data(entries.utf8))
        }
        let entries = try await makeAdminClient().getAudit(
            "app",
            options: AuditQuery(
                table: "users", op: "insert", principal: "u1", source: "mutate",
                limit: 50, offset: 10
            )
        )
        #expect(entries.count == 2)
        #expect(entries[1].op == nil)
        #expect(entries[1].principal == nil)
    }

    @Test func sessionsListRevokeAndRevokeUser() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/sessions", "expected /admin/sessions")
            try demand(request.url?.query == "user=u1&limit=10", "query mismatch")
            let sessions = """
            {"sessions":[{"tokenHash":"abc","userId":"u1","email":"a@b.com","anonymous":false,
            "createdAt":1,"expiresAt":9}]}
            """
            return (200, Data(sessions.utf8))
        }
        let sessions = try await client.listSessions(
            options: SessionListOptions(user: "u1", limit: 10)
        )
        #expect(sessions.count == 1)
        #expect(sessions[0].email == "a@b.com")

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(
                request.url?.path == "/admin/sessions/abc",
                "path mismatch: \(request.url?.path ?? "nil")"
            )
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.revokeSession("abc")

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.query == "user=u1", "query mismatch")
            return (200, Data(#"{"ok":true,"revoked":2}"#.utf8))
        }
        let response = try await client.revokeUserSessions(userId: "u1")
        #expect(response.revoked == 2)
    }

    @Test func revokeExpiredSessionsSendsExpiredTrueQuery() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(
                request.url?.path == "/admin/sessions",
                "path mismatch: \(request.url?.path ?? "nil")"
            )
            try demand(request.url?.query == "expired=true", "query mismatch")
            return (200, Data(#"{"ok":true,"revoked":3}"#.utf8))
        }
        let response = try await makeAdminClient().revokeExpiredSessions()
        #expect(response.ok)
        #expect(response.revoked == 3)
    }

    @Test func mergeUsersSendsConfirmEqualToRealUserId() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/merge-users", "expected /admin/merge-users")
            let body = try jsonObjectBody(request)
            try demand(body["anonUserId"] as? String == "anon1", "anonUserId mismatch: \(body)")
            try demand(body["realUserId"] as? String == "u1", "realUserId mismatch: \(body)")
            try demand(body["confirm"] as? String == "u1", "confirm mismatch: \(body)")
            let report = """
            {"dbs":{"app":{"tables":{"users":3},"conflicts":[{"table":"users","id":"u9"}]}},
            "storageRepointed":1,"sessionsRepointed":2,"anonDeleted":true}
            """
            return (200, Data(report.utf8))
        }
        let report = try await makeAdminClient().mergeUsers(anonUserId: "anon1", realUserId: "u1")
        #expect(report.anonDeleted)
        #expect(report.dbs["app"]?.tables["users"] == 3)
        #expect(report.dbs["app"]?.conflicts == [MergeConflict(table: "users", id: "u9")])
    }

    // MARK: workflows / schedules / files (admin view)

    @Test func listWorkflowsBuildsStatusAndLimitQuery() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/workflows", "path mismatch")
            try demand(request.url?.query == "status=running&limit=5", "query mismatch")
            return (
                200,
                Data(
                    #"{"workflows":[{"id":"wf1","name":"drip","status":"running","currentStep":1,"#.utf8
                )
                    + Data(#""stepCount":2,"attempts":1,"createdAt":1,"updatedAt":9}]}"#.utf8)
            )
        }
        let workflows = try await makeAdminClient().listWorkflows(
            "app", options: WorkflowListOptions(status: .running, limit: 5)
        )
        #expect(workflows.count == 1)
        #expect(workflows[0].status == .running)
    }

    @Test func getWorkflowReturnsFlattenedInfoWithOutcomes() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/workflows/wf1", "path mismatch")
            let full = """
            {"id":"wf1","name":"drip","status":"success","currentStep":2,"stepCount":2,
            "attempts":1,"createdAt":1,"updatedAt":9,"startedAt":2,"finishedAt":9,
            "stepOutcomes":[{"stepIndex":0,"status":"success","attempts":1,"at":5},
            {"stepIndex":1,"status":"failed","attempts":3,"at":8,"error":"version mismatch"}]}
            """
            return (200, Data(full.utf8))
        }
        let full = try await makeAdminClient().getWorkflow("app", id: "wf1")
        #expect(full.info.id == "wf1")
        #expect(full.info.finishedAt == 9)
        #expect(full.stepOutcomes.count == 2)
        #expect(full.stepOutcomes[1].status == .failed)
        #expect(full.stepOutcomes[1].error == "version mismatch")
    }

    @Test func adminWorkflowStartCancelDeleteHitTheirRoutes() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/workflows", "path mismatch")
            // The bare spec is the body — no wrapper object.
            let body = try jsonObjectBody(request)
            try demand(body["spec"] == nil, "spec must be the body itself: \(body)")
            try demand(body["name"] as? String == "drip", "name mismatch: \(body)")
            return (200, Data(#"{"id":"wf-7"}"#.utf8))
        }
        let spec = WorkflowSpec(name: "drip", steps: [WorkflowStepSpec(txn: Transaction(steps: []))])
        let id = try await client.startWorkflow("app", spec: spec)
        #expect(id == "wf-7")

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(
                request.url?.path == "/admin/db/app/workflows/wf-7/cancel", "path mismatch"
            )
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        #expect(try await client.cancelWorkflow("app", id: "wf-7") == true)

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/admin/db/app/workflows/wf-7", "path mismatch")
            return (200, Data(#"{"ok":false}"#.utf8))
        }
        #expect(try await client.deleteWorkflow("app", id: "wf-7") == false)
    }

    @Test func adminSchedulesListCreateAndManage() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/schedules", "path mismatch")
            return (
                200,
                Data(
                    #"{"schedules":[{"id":"job-1","kind":"oneshot","dueAt":9000,"status":"pending","#.utf8
                )
                    + Data(#""createdAt":1000,"firedCount":0}]}"#.utf8)
            )
        }
        let schedules = try await client.listSchedules("app")
        #expect(schedules.count == 1)
        #expect(schedules[0].id == "job-1")

        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/schedules", "path mismatch")
            let body = try jsonObjectBody(request)
            try demand(body["db"] == nil, "db rides the path, not the body: \(body)")
            let when = body["when"] as? [String: Any]
            try demand(when?["type"] as? String == "afterMs", "when.type mismatch: \(body)")
            try demand((when?["ms"] as? Int) == 5000, "when.ms mismatch: \(body)")
            try demand((body["txn"] as? [String: Any])?["steps"] != nil, "txn missing: \(body)")
            return (200, Data(#"{"id":"job-2"}"#.utf8))
        }
        let id = try await client.createSchedule(
            "app", when: .afterMs(ms: 5000), txn: Transaction(steps: [])
        )
        #expect(id == "job-2")

        // cancel + pause + resume hit their op paths bodyless; ok:false
        // (unknown/terminal id) is a no-op flag, not an error.
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(stubRequestBody(request).isEmpty, "manage ops send no body")
            return (
                200,
                Data((request.url?.path.hasSuffix("/cancel") ?? false ? #"{"ok":true}"# : #"{"ok":false}"#).utf8)
            )
        }
        #expect(try await client.cancelSchedule("app", id: "job-2") == true)
        #expect(try await client.pauseSchedule("app", id: "job-2") == false)
        #expect(try await client.resumeSchedule("app", id: "job-2") == false)
    }

    @Test func adminFilesListUploadDelete() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/admin/db/app/storage", "path mismatch")
            let files = """
            {"files":[{"id":"f1","sha256":"abc","size":4,"contentType":"image/png",
            "creationTime":9},{"id":"f2","sha256":"def","size":1,"creationTime":8}]}
            """
            return (200, Data(files.utf8))
        }
        let files = try await client.listFiles("app")
        #expect(files.count == 2)
        #expect(files[0] == FileMetadata(id: "f1", sha256: "abc", size: 4, contentType: "image/png", creationTime: 9))
        #expect(files[1].contentType == nil)

        StubProtocol.handler = { request in
            try adminAuth(request)
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/db/app/storage", "path mismatch")
            try demand(
                request.value(forHTTPHeaderField: "Content-Type") == "image/png",
                "Content-Type mismatch"
            )
            try demand(stubRequestBody(request) == Data([1, 2, 3, 4]), "raw body mismatch")
            return (200, Data(#"{"id":"f3"}"#.utf8))
        }
        let id = try await client.uploadFile("app", bytes: Data([1, 2, 3, 4]), contentType: "image/png")
        #expect(id == "f3")

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/admin/db/app/storage/f3", "path mismatch")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.deleteFile("app", id: "f3")
    }

    @Test func anonymousAccessGetAndSet() async throws {
        let client = makeAdminClient()
        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/anonymous-access", "path mismatch"
            )
            return (200, Data(#"{"enabled":true}"#.utf8))
        }
        #expect(try await client.getAnonymousAccess("app"))

        StubProtocol.handler = { request in
            try demand(request.httpMethod == "PATCH", "expected PATCH")
            try demand(
                request.url?.path == "/admin/db/app/anonymous-access", "path mismatch"
            )
            let body = try jsonObjectBody(request)
            try demand((body["enabled"] as? Bool) == false, "enabled mismatch: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await client.setAnonymousAccess("app", enabled: false)
    }
}
