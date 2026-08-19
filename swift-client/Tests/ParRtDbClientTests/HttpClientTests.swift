import Foundation
@testable import ParRtDbClient
import Testing

// MARK: - URLProtocol stub machinery

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

/// Thread-safe request counter / path recorder for multi-call handlers.
private final class CallRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [String] = []

    func record(_ item: String) {
        lock.lock()
        defer { lock.unlock() }
        items.append(item)
    }

    func count() -> Int {
        lock.lock()
        defer { lock.unlock() }
        return items.count
    }

    func recorded() -> [String] {
        lock.lock()
        defer { lock.unlock() }
        return items
    }
}

private func makeClient() -> RtDbHttpClient {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [StubProtocol.self]
    return RtDbHttpClient(
        url: "http://rtdb.test/", db: "app", token: "tok",
        session: URLSession(configuration: config)
    )
}

// MARK: - Tests

/// Serialized: every test installs the one shared `StubProtocol.handler` —
/// splitting the suite would run the halves in parallel and race on it.
@Suite(.serialized)
struct HttpClientTests {
    // MARK: run / get / findOneByIndex

    @Test func runPostsQueryEnvelopeAndParsesResult() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST, got \(request.httpMethod ?? "nil")")
            try demand(
                request.url?.path == "/api/query",
                "expected /api/query, got \(request.url?.path ?? "nil")"
            )
            try demand(
                request.value(forHTTPHeaderField: "Authorization") == "Bearer tok",
                "missing bearer authorization"
            )
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let query = body["query"] as? [String: Any]
            try demand(query?["table"] as? String == "users", "query.table mismatch: \(body)")
            try demand(query?["index"] as? String == "by_status", "query.index mismatch: \(body)")
            try demand((query?["eq"] as? [String])?.first == "active", "query.eq mismatch: \(body)")
            try demand((query?["take"] as? Int) == 2, "query.take mismatch: \(body)")
            return (200, Data(#"{"result":[{"_id":"a"},{"_id":"b"}]}"#.utf8))
        }
        let client = makeClient()
        let query = try TableQuery("users").withIndex("by_status").eq(.string("active")).take(2).build()
        let docs: [JSONValue] = try await client.run(query, as: [JSONValue].self)
        #expect(docs.count == 2)
        let raw = try await client.run(query)
        if case let .array(items) = raw {
            #expect(items.count == 2)
        } else {
            Issue.record("untyped run should return the raw array result, got \(raw)")
        }
    }

    @Test func runDecodesCountAsInt() async throws {
        StubProtocol.handler = { _ in (200, Data(#"{"result":5}"#.utf8)) }
        let total: Int = try await makeClient().run(
            TableQuery("items").count().build(), as: Int.self
        )
        #expect(total == 5)
    }

    @Test func getReturnsDocForHit() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/query", "expected /api/query")
            let body = try jsonObjectBody(request)
            let query = body["query"] as? [String: Any]
            try demand(query?["table"] as? String == "items", "query.table mismatch: \(body)")
            try demand(query?["get"] as? String == "a", "query.get mismatch: \(body)")
            return (200, Data(#"{"result":{"_id":"a","n":1}}"#.utf8))
        }
        let doc = try await makeClient().get("items", "a")
        #expect(doc?.objectValue?["_id"] == .string("a"))
    }

    @Test func getReturnsNilForMiss() async throws {
        StubProtocol.handler = { _ in (200, Data(#"{"result":null}"#.utf8)) }
        let doc = try await makeClient().get("items", "none")
        #expect(doc == nil)
    }

    @Test func findOneByIndexBuildsFirstTerminalQuery() async throws {
        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            let query = body["query"] as? [String: Any]
            try demand(query?["table"] as? String == "users", "query.table mismatch: \(body)")
            try demand(query?["index"] as? String == "by_email", "query.index mismatch: \(body)")
            try demand((query?["eq"] as? [String])?.first == "a@b.com", "query.eq mismatch: \(body)")
            try demand((query?["first"] as? Bool) == true, "query.first mismatch: \(body)")
            return (200, Data(#"{"result":{"_id":"u1","email":"a@b.com"}}"#.utf8))
        }
        let doc = try await makeClient().findOneByIndex("users", "by_email", .string("a@b.com"))
        #expect(doc?.objectValue?["email"] == .string("a@b.com"))
    }

    @Test func findOneByIndexMissReturnsNil() async throws {
        StubProtocol.handler = { _ in (200, Data(#"{"result":null}"#.utf8)) }
        let doc = try await makeClient().findOneByIndex("users", "by_email", .string("none@x.com"))
        #expect(doc == nil)
    }

    // MARK: batchQuery

    @Test func batchQueryReturnsAlignedOutcomes() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(
                request.url?.path == "/api/query-batch",
                "expected /api/query-batch, got \(request.url?.path ?? "nil")"
            )
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let queries = body["queries"] as? [[String: Any]]
            try demand(queries?.count == 1, "queries mismatch: \(body)")
            try demand(queries?.first?["table"] as? String == "items", "queries[0].table mismatch")
            // {results: [outcomeOne, outcomeTwo]} — assembled to stay under 120 cols.
            let firstOutcome = #"{"ok":true,"result":[{"_id":"a"}]}"#
            let secondOutcome = #"{"ok":false,"error":{"code":"NOT_FOUND","message":"no such table"}}"#
            let responseBody = #"{"results":["# + firstOutcome + "," + secondOutcome + "]}"
            return (200, Data(responseBody.utf8))
        }
        let outcomes = try await makeClient().batchQuery([TableQuery("items").take(5).build()])
        #expect(outcomes.count == 2)
        #expect(outcomes[0].ok)
        if case let .array(docs) = outcomes[0].result {
            #expect(docs.count == 1)
        } else {
            Issue.record("outcomes[0].result should be an array")
        }
        #expect(outcomes[0].error == nil)
        #expect(!outcomes[1].ok)
        #expect(outcomes[1].result == nil)
        #expect(outcomes[1].error?.code == .notFound)
        #expect(outcomes[1].error?.message == "no such table")
    }

    // MARK: mutate / upsert / retry

    @Test func mutatePostsTxnAndParsesStepResults() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/mutate", "expected /api/mutate")
            try demand(
                request.value(forHTTPHeaderField: "Authorization") == "Bearer tok",
                "missing bearer authorization"
            )
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            try demand(body["idempotencyKey"] == nil, "idempotencyKey must be omitted when nil")
            let steps = (body["txn"] as? [String: Any])?["steps"] as? [[String: Any]]
            try demand(steps?.count == 2, "txn.steps mismatch: \(body)")
            try demand(steps?.first?["op"] as? String == "insert", "steps[0].op mismatch")
            try demand(steps?.last?["op"] as? String == "patch", "steps[1].op mismatch")
            return (200, Data(#"{"results":[{"id":"new1"},null]}"#.utf8))
        }
        let txn = try MutationBuilder()
            .insert("items", ["name": .string("x")])
            .patch("items", "i1", ["y": .int(1)])
            .build()
        let results = try await makeClient().mutate(txn)
        #expect(results == [.insert(id: "new1"), .null])
    }

    @Test func mutateSendsIdempotencyKey() async throws {
        StubProtocol.handler = { request in
            let body = try jsonObjectBody(request)
            try demand(body["idempotencyKey"] as? String == "k1", "idempotencyKey mismatch: \(body)")
            return (200, Data(#"{"results":[]}"#.utf8))
        }
        let txn = try MutationBuilder().delete("items", "i1").build()
        let results = try await makeClient().mutate(txn, idempotencyKey: "k1")
        #expect(results.isEmpty)
    }

    @Test func upsertByIndexBuildsUpsertTxnAndReturnsId() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/mutate", "expected /api/mutate")
            let body = try jsonObjectBody(request)
            let steps = (body["txn"] as? [String: Any])?["steps"] as? [[String: Any]]
            try demand(steps?.count == 1, "expected one step: \(body)")
            let step = steps?.first ?? [:]
            try demand(step["op"] as? String == "upsert", "op mismatch: \(step)")
            try demand(step["table"] as? String == "users", "table mismatch: \(step)")
            try demand(step["index"] as? String == "by_email", "index mismatch: \(step)")
            try demand((step["eq"] as? [String])?.first == "a@b.com", "eq mismatch: \(step)")
            try demand(
                (step["insert"] as? [String: Any])?.keys.contains("email") == true,
                "insert mismatch: \(step)"
            )
            try demand(
                (step["patch"] as? [String: Any])?.keys.contains("n") == true,
                "patch mismatch: \(step)"
            )
            return (200, Data(#"{"results":[{"id":"new1","inserted":true}]}"#.utf8))
        }
        let id = try await makeClient().upsertByIndex(
            "users",
            index: "by_email",
            eq: [.string("a@b.com")],
            insert: .object(["email": .string("a@b.com")]),
            patch: .object(["n": .int(1)])
        )
        #expect(id == "new1")
    }

    @Test func mutateWithRetryRetriesOnPreconditionFailure() async throws {
        let calls = CallRecorder()
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/mutate", "expected /api/mutate")
            if calls.count() == 0 {
                calls.record("conflict")
                return (409, Data(#"{"code":"PRECONDITION_FAILED","message":"version mismatch"}"#.utf8))
            }
            calls.record("ok")
            return (200, Data(#"{"results":[{"id":"i1"}]}"#.utf8))
        }
        let txn = try MutationBuilder().insert("items", ["n": .int(1)]).build()
        let results = try await makeClient().mutateWithRetry(txn)
        #expect(results == [.insert(id: "i1")])
        #expect(calls.recorded() == ["conflict", "ok"])
    }

    // MARK: error paths

    @Test func errorEnvelopeBecomesRtDbError() async throws {
        StubProtocol.handler = { _ in
            (409, Data(#"{"code":"PRECONDITION_FAILED","message":"version mismatch"}"#.utf8))
        }
        do {
            _ = try await makeClient().mutate(Transaction(steps: []))
            Issue.record("mutate should throw on a 409 envelope")
        } catch let error as RtDbError {
            #expect(error.code == .preconditionFailed)
            #expect(error.message == "version mismatch")
        }
    }

    @Test func nonEnvelopeErrorReportsHttpStatusOnly() async throws {
        StubProtocol.handler = { _ in (500, Data("gateway down".utf8)) }
        do {
            _ = try await makeClient().mutate(Transaction(steps: []))
            Issue.record("mutate should throw on a non-2xx")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message == "HTTP 500")
            #expect(!error.message.contains("gateway"))
        }
    }

    @Test func transportErrorsWrapAsRtDbError() async throws {
        StubProtocol.handler = { _ in throw URLError(.notConnectedToInternet) }
        do {
            _ = try await makeClient().mutate(Transaction(steps: []))
            Issue.record("mutate should throw on a transport failure")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
            #expect(error.message.contains("request failed"))
        }
    }

    // MARK: scheduler

    @Test func schedulePostsWhenAndTxnReturnsId() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/api/schedule", "expected /api/schedule")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let when = body["when"] as? [String: Any]
            try demand(when?["type"] as? String == "afterMs", "when.type mismatch: \(body)")
            try demand((when?["ms"] as? Int) == 5000, "when.ms mismatch: \(body)")
            let steps = (body["txn"] as? [String: Any])?["steps"] as? [Any]
            try demand(steps?.isEmpty == true, "txn.steps mismatch: \(body)")
            return (200, Data(#"{"id":"job-7"}"#.utf8))
        }
        let id = try await makeClient().schedule(
            Transaction(steps: []), when: .afterMs(ms: 5000)
        )
        #expect(id == "job-7")
    }

    @Test func scheduleManageOpsPostTheirPathAndDbBody() async throws {
        let paths = CallRecorder()
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(
                request.value(forHTTPHeaderField: "Authorization") == "Bearer tok",
                "missing bearer authorization"
            )
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            paths.record(request.url?.path ?? "nil")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        let client = makeClient()
        try await client.cancelSchedule("job-1")
        try await client.pauseSchedule("job-1")
        try await client.resumeSchedule("job-1")
        #expect(paths.recorded() == [
            "/api/schedule/job-1/cancel",
            "/api/schedule/job-1/pause",
            "/api/schedule/job-1/resume"
        ])
    }

    @Test func scheduleManageEncodesIdPathSegment() async throws {
        StubProtocol.handler = { request in
            // `URL.path` percent-DECODES; the encoded form is what hits the wire.
            let encoded = URLComponents(
                url: request.url!, resolvingAgainstBaseURL: false
            )?.percentEncodedPath
            try demand(
                encoded == "/api/schedule/a%20b%2Fc/cancel",
                "id must percent-encode like encodeURIComponent: \(encoded ?? "nil")"
            )
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeClient().cancelSchedule("a b/c")
    }

    @Test func listSchedulesReturnsScheduleInfo() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/schedules", "expected /api/schedules")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            return (
                200,
                Data(
                    #"{"schedules":[{"id":"job-1","kind":"cron","dueAt":9000,"cron":"*/5 * * * *","#.utf8
                )
                    + Data(#""status":"pending","createdAt":1000,"firedCount":0}]}"#.utf8)
            )
        }
        let schedules = try await makeClient().listSchedules()
        #expect(schedules.count == 1)
        #expect(schedules[0].id == "job-1")
        #expect(schedules[0].kind == .cron)
        #expect(schedules[0].cron == "*/5 * * * *")
        #expect(schedules[0].status == .pending)
    }

    // MARK: workflows

    @Test func startWorkflowPostsSpecReturnsId() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/workflows", "expected /api/workflows")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let spec = body["spec"] as? [String: Any]
            try demand(spec?["name"] as? String == "drip", "spec.name mismatch: \(body)")
            let steps = spec?["steps"] as? [[String: Any]]
            try demand(steps?.count == 1, "spec.steps mismatch: \(body)")
            let nested = (steps?.first?["txn"] as? [String: Any])?["steps"] as? [Any]
            try demand(nested?.isEmpty == true, "spec.steps[0].txn mismatch: \(body)")
            return (200, Data(#"{"id":"wf-7"}"#.utf8))
        }
        let spec = WorkflowSpec(
            name: "drip", steps: [WorkflowStepSpec(txn: Transaction(steps: []))]
        )
        let id = try await makeClient().startWorkflow(spec)
        #expect(id == "wf-7")
    }

    @Test func cancelWorkflowPostsDbBody() async throws {
        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/api/workflows/wf-1/cancel",
                "expected /api/workflows/wf-1/cancel"
            )
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            return (200, Data(#"{"cancelled":false}"#.utf8))
        }
        try await makeClient().cancelWorkflow("wf-1")
    }

    @Test func listWorkflowsReturnsWorkflowInfo() async throws {
        StubProtocol.handler = { request in
            try demand(request.url?.path == "/api/workflows/list", "expected /api/workflows/list")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            return (
                200,
                Data(
                    #"{"workflows":[{"id":"wf1","name":"drip","status":"success","currentStep":2,"#.utf8
                )
                    + Data(#""stepCount":2,"attempts":1,"createdAt":1,"updatedAt":9,"finishedAt":9}]}"#.utf8)
            )
        }
        let workflows = try await makeClient().listWorkflows()
        #expect(workflows.count == 1)
        #expect(workflows[0].id == "wf1")
        #expect(workflows[0].status == .success)
        #expect(workflows[0].finishedAt == 9)
        #expect(workflows[0].startedAt == nil)
    }

    // MARK: auth + schema facade

    @Test func authMeReturnsUser() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(request.url?.path == "/auth/me", "expected /auth/me")
            try demand(
                request.value(forHTTPHeaderField: "Authorization") == "Bearer tok",
                "missing bearer authorization"
            )
            return (
                200,
                Data(#"{"user":{"kind":"user","email":"a@b.com","name":null}}"#.utf8)
            )
        }
        let user = try await makeClient().authMe()
        #expect(user.kind == .user)
        #expect(user.email == "a@b.com")
        #expect(user.name == nil)
    }

    @Test func pushSchemaPostsDbAndSchema() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/admin/push-schema", "expected /admin/push-schema")
            let body = try jsonObjectBody(request)
            try demand(body["db"] as? String == "app", "db mismatch: \(body)")
            let schema = body["schema"] as? [String: Any]
            try demand(schema?["tables"] != nil, "schema.tables missing: \(body)")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeClient().pushSchema(SchemaDef(tables: [:]))
    }

    @Test func pushSchemaOkFalseThrows() async throws {
        StubProtocol.handler = { _ in (200, Data(#"{"ok":false}"#.utf8)) }
        do {
            try await makeClient().pushSchema(SchemaDef(tables: [:]))
            Issue.record("pushSchema should throw on ok=false")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
        }
    }

    @Test func previewSchemaPostsSchemaAndReturnsDiff() async throws {
        StubProtocol.handler = { request in
            try demand(
                request.url?.path == "/admin/db/app/schema/preview",
                "expected /admin/db/app/schema/preview"
            )
            let body = try jsonObjectBody(request)
            try demand(body["schema"] != nil, "schema missing: \(body)")
            try demand(body["db"] == nil, "db rides the path, not the body: \(body)")
            return (
                200,
                Data(#"{"added":[{"table":"users","columns":["email"]}],"rejected":[]}"#.utf8)
            )
        }
        let diff = try await makeClient().previewSchema(SchemaDef(tables: [:]))
        if case let .array(added) = diff.objectValue?["added"] {
            #expect(added.count == 1)
        } else {
            Issue.record("diff.added should be an array, got \(diff)")
        }
    }

    // MARK: storage

    @Test func uploadSendsRawBodyContentTypeAndReturnsId() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "POST", "expected POST")
            try demand(request.url?.path == "/api/storage/app", "expected /api/storage/app")
            try demand(
                request.value(forHTTPHeaderField: "Content-Type") == "image/png",
                "Content-Type mismatch"
            )
            try demand(
                request.value(forHTTPHeaderField: "Authorization") == "Bearer tok",
                "missing bearer authorization"
            )
            let bytes = stubRequestBody(request)
            try demand(bytes == Data([1, 2, 3, 4]), "raw body mismatch: \(bytes)")
            return (
                200,
                Data(#"{"id":"f1","sha256":"abc","size":4,"contentType":"image/png"}"#.utf8)
            )
        }
        let id = try await makeClient().upload(Data([1, 2, 3, 4]), contentType: "image/png")
        #expect(id == "f1")
    }

    @Test func deleteFileSendsDeleteAndExpectsOk() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "DELETE", "expected DELETE")
            try demand(request.url?.path == "/api/storage/app/f1", "expected /api/storage/app/f1")
            return (200, Data(#"{"ok":true}"#.utf8))
        }
        try await makeClient().deleteFile("f1")
    }

    @Test func getFileMetadataReturnsBodyObject() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(
                request.url?.path == "/api/storage/app/f1/metadata",
                "expected /api/storage/app/f1/metadata"
            )
            return (
                200,
                Data(#"{"id":"f1","sha256":"abc","size":9,"creationTime":5}"#.utf8)
            )
        }
        let metadata = try await makeClient().getFileMetadata("f1")
        #expect(metadata.objectValue?["size"] == .int(9))
        #expect(metadata.objectValue?["creationTime"] == .int(5))
    }

    @Test func getSignedUrlSendsTtlAndReturnsUrl() async throws {
        StubProtocol.handler = { request in
            try demand(request.httpMethod == "GET", "expected GET")
            try demand(
                request.url?.path == "/api/storage/app/f1/signed-url",
                "expected /api/storage/app/f1/signed-url"
            )
            try demand(
                request.url?.query == "ttlSeconds=120",
                "ttlSeconds query mismatch: \(request.url?.query ?? "nil")"
            )
            return (
                200,
                Data(#"{"url":"http://x/storage/f1?exp=9&sig=ab","expiresAt":9}"#.utf8)
            )
        }
        let url = try await makeClient().getSignedUrl("f1", ttlSeconds: 120)
        #expect(url == "http://x/storage/f1?exp=9&sig=ab")
    }

    // MARK: pure URL builders

    @Test func getUrlIsALocalBuilderThatTrimsTrailingSlash() {
        let client = makeClient()
        #expect(client.getUrl("f1") == "http://rtdb.test/storage/f1")
    }

    @Test func transformUrlAppendsParamsInFixedOrder() {
        let client = makeClient()
        let url = client.transformUrl(
            "f1", width: 100, height: 50, fit: .cover, quality: 80, format: .auto
        )
        #expect(url == "http://rtdb.test/storage/f1?w=100&h=50&fit=cover&q=80")
    }

    @Test func transformUrlScaleDownJpegAndPng() {
        let client = makeClient()
        #expect(
            client.transformUrl("f1", height: 200, fit: .scaleDown, format: .jpeg)
                == "http://rtdb.test/storage/f1?h=200&fit=scale-down&format=jpeg"
        )
        #expect(client.transformUrl("f1", format: .png) == "http://rtdb.test/storage/f1?format=png")
        #expect(client.transformUrl("f1") == "http://rtdb.test/storage/f1")
    }
}
