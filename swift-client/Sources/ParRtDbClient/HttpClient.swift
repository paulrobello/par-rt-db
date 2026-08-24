import Foundation

// MARK: - Transform option enums

/// Mirrors rust-client/src/http.rs::Fit — the image-transform `fit` mode
/// (ENH-014). Wire strings are kebab-case: `contain` (default), `cover`,
/// `scale-down`.
public enum Fit: String, Codable, Sendable {
    /// Fit entirely inside the target box, preserving aspect ratio (default).
    case contain
    /// Fill the box, cropping the overflow.
    case cover
    /// Never upscale; downscale as `contain` would.
    case scaleDown = "scale-down"
}

/// Mirrors rust-client/src/http.rs::OutFormat — the image-transform output
/// format. Wire strings are lowercase: `auto` (server default), `jpeg`, `png`.
public enum OutFormat: String, Codable, Sendable {
    /// Let the server pick (default).
    case auto
    /// JPEG output.
    case jpeg
    /// PNG output.
    case png
}

// MARK: - BatchQueryOutcome

/// Mirrors rust-client/src/wire.rs::BatchQueryOutcome (itself the mirror of
/// the server's http_api::BatchQueryOutcome) — camelCase, omit-when-nil.
/// `result` is the raw untagged query-result value (present when `ok`);
/// `error` is the standard `{code, message}` envelope (present when not).
public struct BatchQueryOutcome: Equatable, Codable, Sendable {
    /// Whether the query executed.
    public var ok: Bool
    /// The raw untagged query result (present when `ok`).
    public var result: JSONValue?
    /// The `{code, message}` envelope (present when not `ok`).
    public var error: RtDbError?

    public init(ok: Bool, result: JSONValue? = nil, error: RtDbError? = nil) {
        self.ok = ok
        self.result = result
        self.error = error
    }

    enum CodingKeys: String, CodingKey {
        case ok, result, error
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        ok = try container.decode(Bool.self, forKey: .ok)
        result = try container.decodeIfPresent(JSONValue.self, forKey: .result)
        error = try container.decodeIfPresent(RtDbError.self, forKey: .error)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(ok, forKey: .ok)
        try container.encodeIfPresent(result, forKey: .result)
        try container.encodeIfPresent(error, forKey: .error)
    }
}

// MARK: - RtDbHttpClient

/// One-shot HTTP client for one par-rt-db database: typed queries, atomic
/// transactions, scheduling/cron, durable workflows, and file storage, all
/// bearer-token authorized — the Swift mirror of rust-client/src/http.rs
/// `RtDbHttpClient`. Route/method/body shapes are ported one-to-one; for live
/// query subscriptions use the WS client.
public actor RtDbHttpClient {
    /// Base URL with any trailing `/` trimmed (rust `trim_end_matches('/')`).
    private let baseUrl: String
    private let db: String
    private let token: String
    private let session: URLSession

    public init(url: String, db: String, token: String, session: URLSession = .shared) {
        var base = url
        while base.hasSuffix("/") {
            base.removeLast()
        }
        baseUrl = base
        self.db = db
        self.token = token
        self.session = session
    }

    // MARK: Data plane

    /// Run any built query and return the raw untagged `QueryResult` payload
    /// (`POST /api/query` `{db, query}` → `{result}`).
    public func run(_ query: Query) async throws -> JSONValue {
        let response: QueryResponse = try await postJson(
            "query", "/api/query", QueryRequest(db: db, query: query)
        )
        return response.result
    }

    /// Run any built query, decoding `{result}` into `T` via `parseResult` —
    /// the terminal is derived from the query itself. Use the type that
    /// matches the terminal: `[Doc]` for array terminals, `Doc?` for
    /// get/unique/first, `Int` for count, `Paginated<Doc>` for paginate.
    public func run<T: Codable & Sendable>(_ query: Query, as _: T.Type) async throws -> T {
        let raw = try await run(query)
        return try parseResult(raw, terminal: query.readTerminal)
    }

    /// Point read: `{"table", "get": id}` → the doc, or nil when absent.
    public func get(_ table: String, _ id: String) async throws -> JSONValue? {
        let result = try await run(TableQuery(table).get(id).build())
        return result == .null ? nil : result
    }

    /// The single doc matching `value` on `index`, or nil when none matches
    /// (the indexed `eq` query with the `first` terminal — server runs
    /// `LIMIT 1`, so duplicate matches never error).
    public func findOneByIndex(
        _ table: String, _ index: String, _ value: JSONValue
    ) async throws -> JSONValue? {
        let result = try await run(
            TableQuery(table).withIndex(index).eq(value).first().build()
        )
        return result == .null ? nil : result
    }

    /// Run a batch of independent queries in one round trip
    /// (`POST /api/query-batch`). A per-query execution error becomes that
    /// slot's `{ok: false, error}` outcome and never fails the call; the
    /// returned array is length-aligned with the input order.
    public func batchQuery(_ queries: [Query]) async throws -> [BatchQueryOutcome] {
        let response: BatchResponse = try await postJson(
            "batch query", "/api/query-batch", BatchRequest(db: db, queries: queries)
        )
        return response.results
    }

    /// Run a transaction (`POST /api/mutate` `{db, txn, idempotencyKey?}`);
    /// returns one `StepResult` per step. `idempotencyKey` replays a cached
    /// result when the server has seen it before.
    public func mutate(
        _ txn: Transaction, idempotencyKey: String? = nil
    ) async throws -> [StepResult] {
        let response: MutateResponse = try await postJson(
            "mutate", "/api/mutate",
            MutateRequest(db: db, txn: txn, idempotencyKey: idempotencyKey)
        )
        return response.results
    }

    /// Upsert by index-field value: a one-step transaction that matches `eq`
    /// on `index` — match → patch, no match → insert — returning the doc id.
    /// More than one match rejects with PRECONDITION_FAILED (not transient,
    /// so no retry).
    public func upsertByIndex(
        _ table: String,
        index: String,
        eq: [JSONValue],
        insert: JSONValue,
        patch: JSONValue
    ) async throws -> String {
        guard let insertObject = insert.objectValue else {
            throw RtDbError(code: .badRequest, message: "upsertByIndex insert must be a JSON object")
        }
        guard let patchObject = patch.objectValue else {
            throw RtDbError(code: .badRequest, message: "upsertByIndex patch must be a JSON object")
        }
        let txn = try MutationBuilder()
            .upsert(table, index: index, eq: eq, insert: insertObject, patch: patchObject)
            .build()
        let results = try await mutate(txn)
        guard case let .upsert(id, _)? = results.last else {
            throw RtDbError(code: .internal, message: "upsert returned no result")
        }
        return id
    }

    /// Run a transaction through `retryOnPrecondition`, re-running the SAME
    /// txn when the server rejects with PRECONDITION_FAILED (useful for
    /// `expectAbsent` races; a read-modify-write needs a rebuilt txn).
    public func mutateWithRetry(_ txn: Transaction) async throws -> [StepResult] {
        try await Self.retryMutate(txn, client: self)
    }

    /// The retry closure must form in a nonisolated context — one formed
    /// inside the actor is actor-isolated and Swift 6 rejects passing it to
    /// the nonisolated `retryOnPrecondition`.
    private nonisolated static func retryMutate(
        _ txn: Transaction, client: RtDbHttpClient
    ) async throws -> [StepResult] {
        try await retryOnPrecondition { try await client.mutate(txn) }
    }

    // MARK: Scheduler

    /// Schedule `txn` to fire at `when` (`POST /api/schedule`
    /// `{db, when, txn}`); returns the new schedule's id. The server validates
    /// cron expressions and resolves the due time.
    public func schedule(_ txn: Transaction, when: ScheduleWhen) async throws -> String {
        let response: IdResponse = try await postJson(
            "schedule", "/api/schedule", ScheduleRequest(db: db, when: when, txn: txn)
        )
        return response.id
    }

    /// Cancel a scheduled job (`POST /api/schedule/{id}/cancel`).
    public func cancelSchedule(_ id: String) async throws {
        try await manageSchedule(id, op: "cancel")
    }

    /// Pause a scheduled job (`POST /api/schedule/{id}/pause`).
    public func pauseSchedule(_ id: String) async throws {
        try await manageSchedule(id, op: "pause")
    }

    /// Resume a paused scheduled job (`POST /api/schedule/{id}/resume`).
    public func resumeSchedule(_ id: String) async throws {
        try await manageSchedule(id, op: "resume")
    }

    /// Shared body for the three manage ops. A 200 `{ok: false}` (unknown or
    /// already-terminal id) is a no-op ack, not an error — this Void surface
    /// drops the flag the rust client returns.
    private func manageSchedule(_ id: String, op: String) async throws {
        let response: OkResponse = try await postJson(
            "schedule \(op)", "/api/schedule/\(encodePath(id))/\(op)", DbRequest(db: db)
        )
        _ = response.ok
    }

    /// List scheduled jobs for this database (`POST /api/schedules`).
    public func listSchedules() async throws -> [ScheduleInfo] {
        let response: SchedulesResponse = try await postJson(
            "list schedules", "/api/schedules", DbRequest(db: db)
        )
        return response.schedules
    }

    // MARK: Workflows

    /// Start a durable workflow run (`POST /api/workflows` `{db, spec}`);
    /// returns the new run's id.
    public func startWorkflow(_ spec: WorkflowSpec) async throws -> String {
        let response: IdResponse = try await postJson(
            "start workflow", "/api/workflows", StartWorkflowRequest(db: db, spec: spec)
        )
        return response.id
    }

    /// Cancel a workflow run (`POST /api/workflows/{id}/cancel`). A
    /// `{cancelled: false}` body (missing/already-terminal run) is a no-op
    /// ack, not an error — this Void surface drops the flag the rust client
    /// returns.
    public func cancelWorkflow(_ id: String) async throws {
        let response: CancelledResponse = try await postJson(
            "cancel workflow", "/api/workflows/\(encodePath(id))/cancel", DbRequest(db: db)
        )
        _ = response.cancelled
    }

    /// Deliver a named signal to a waiting run (`POST /api/workflows/{id}/signal`
    /// `{db, name, payload?}`). A 200 always carries `{delivered: true}` — the
    /// typed failures (unknown id, not waiting, name mismatch) surface as
    /// NOT_FOUND/CONFLICT errors, so this Void surface drops the flag.
    public func signalWorkflow(_ id: String, name: String, payload: JSONValue? = nil) async throws {
        let response: SignalWorkflowResponse = try await postJson(
            "signal workflow", "/api/workflows/\(encodePath(id))/signal",
            SignalWorkflowRequest(db: db, name: name, payload: payload)
        )
        _ = response.delivered
    }

    /// List this database's workflow runs, newest first
    /// (`POST /api/workflows/list`).
    public func listWorkflows() async throws -> [WorkflowInfo] {
        let response: WorkflowsResponse = try await postJson(
            "list workflows", "/api/workflows/list", DbRequest(db: db)
        )
        return response.workflows
    }

    // MARK: Auth + schema facade

    /// Validate the bearer (session) token via `GET /auth/me`; machine tokens
    /// get 401.
    public func authMe() async throws -> AuthedUser {
        let response: MeResponse = try await getJson("auth_me", "/auth/me")
        return response.user
    }

    /// Push a schema to this client's database (`POST /admin/push-schema`
    /// `{db, schema}` → `{ok: true}`) — the admin route with the same token.
    public func pushSchema(_ schema: SchemaDef) async throws {
        let response: OkResponse = try await postJson(
            "push schema", "/admin/push-schema", PushSchemaRequest(db: db, schema: schema)
        )
        guard response.ok else {
            throw RtDbError(code: .internal, message: "admin request returned ok=false")
        }
    }

    /// Validate a pending schema and diff it against the applied one WITHOUT
    /// applying (`POST /admin/db/{db}/schema/preview` `{schema}`); returns the
    /// raw `{added, rejected}` diff object. Pure/advisory — `pushSchema`
    /// remains the authoritative gate.
    public func previewSchema(_ schema: SchemaDef) async throws -> JSONValue {
        try await postJson(
            "preview schema", "/admin/db/\(db)/schema/preview",
            PreviewSchemaRequest(schema: schema)
        )
    }

    // MARK: Storage

    /// Upload raw bytes (`POST /api/storage/{db}`, body is the bytes,
    /// `contentType` sets Content-Type and the stored type); returns the
    /// server-assigned file id.
    public func upload(_ data: Data, contentType: String) async throws -> String {
        let response: UploadResponse = try await request(
            "upload", method: "POST", path: "/api/storage/\(db)",
            body: data, contentType: contentType
        )
        return response.id
    }

    /// Delete the file `id` (`DELETE /api/storage/{db}/{id}`) — also revokes
    /// its public serve URL. Idempotent: deleting an unknown id still returns
    /// `{ok: true}`.
    public func deleteFile(_ id: String) async throws {
        let response: OkResponse = try await request(
            "delete file", method: "DELETE", path: "/api/storage/\(db)/\(encodePath(id))"
        )
        guard response.ok else {
            throw RtDbError(code: .internal, message: "delete file returned ok=false")
        }
    }

    /// Stored metadata for `id` (`GET /api/storage/{db}/{id}/metadata`);
    /// returns the raw `{id, sha256, size, contentType?, creationTime}` object.
    public func getFileMetadata(_ id: String) async throws -> JSONValue {
        try await getJson(
            "file metadata", "/api/storage/\(db)/\(encodePath(id))/metadata"
        )
    }

    /// Mint an HMAC-signed, time-limited public URL
    /// (`GET /api/storage/{db}/{id}/signed-url?ttlSeconds=N`); returns the URL
    /// — the body's `expiresAt` (epoch ms) is dropped by this String surface.
    public func getSignedUrl(_ id: String, ttlSeconds: Int) async throws -> String {
        let response: SignedUrlResponse = try await getJson(
            "signed url", "/api/storage/\(db)/\(encodePath(id))/signed-url",
            query: [URLQueryItem(name: "ttlSeconds", value: String(ttlSeconds))]
        )
        return response.url
    }

    /// The public serve URL — no request is made.
    public nonisolated func getUrl(_ id: String) -> String {
        "\(baseUrl)/storage/\(id)"
    }

    /// The public serve URL with image-transform query params appended
    /// (ENH-014). Params are emitted in rust's fixed order `w, h, fit, q,
    /// format`, only when set; `format: .auto` is the server default and is
    /// omitted. No request is made.
    public nonisolated func transformUrl(
        _ id: String,
        width: Int? = nil,
        height: Int? = nil,
        fit: Fit? = nil,
        quality: Int? = nil,
        format: OutFormat? = nil
    ) -> String {
        var parts: [String] = []
        if let width {
            parts.append("w=\(width)")
        }
        if let height {
            parts.append("h=\(height)")
        }
        if let fit {
            parts.append("fit=\(fit.rawValue)")
        }
        if let quality {
            parts.append("q=\(quality)")
        }
        if let format, format != .auto {
            parts.append("format=\(format.rawValue)")
        }
        let base = getUrl(id)
        return parts.isEmpty ? base : "\(base)?\(parts.joined(separator: "&"))"
    }
}

// MARK: - Terminal derivation

extension Query {
    /// The read terminal this query carries — the discriminator `parseResult`
    /// gates the untagged QueryResult payload with. First set field wins:
    /// `TableQuery.build()` has already rejected invalid combinations, and
    /// `search` before `take` is safe because they compose and both produce
    /// arrays.
    var readTerminal: QueryTerminal {
        if get != nil {
            return .get
        }
        if unique {
            return .unique
        }
        if first {
            return .first
        }
        if count {
            return .count
        }
        if distinct {
            return .distinct
        }
        if let aggregate {
            return aggregate.groupBy ? .aggregateGroups : .aggregate
        }
        if paginate != nil {
            return .paginate
        }
        if search != nil {
            return .search
        }
        if vectorSearch != nil {
            return .vectorSearch
        }
        if hybridSearch != nil {
            return .hybridSearch
        }
        if take != nil {
            return .take
        }
        return .collect
    }
}

// MARK: - Transport

extension RtDbHttpClient {
    /// encodeURIComponent parity (rust http.rs `encode_uri_component`) for
    /// caller-supplied path segments: ASCII alphanumerics plus `-_.!~*'()`
    /// pass through; every other byte (each byte of a multi-byte UTF-8
    /// sequence included) percent-encodes with uppercase hex.
    private static let pathSegmentAllowed = CharacterSet(
        charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()"
    )

    private func encodePath(_ segment: String) -> String {
        segment.addingPercentEncoding(withAllowedCharacters: Self.pathSegmentAllowed) ?? segment
    }

    /// POST a JSON-encoded body and decode the JSON response.
    private func postJson<Res: Decodable>(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws -> Res {
        let data: Data
        do {
            data = try JSONEncoder().encode(body)
        } catch {
            throw RtDbError(code: .internal, message: "invalid \(what) request body: \(error)")
        }
        return try await request(
            what, method: "POST", path: path, body: data, contentType: "application/json"
        )
    }

    /// GET and decode a JSON response.
    private func getJson<Res: Decodable>(
        _ what: String, _ path: String, query: [URLQueryItem] = []
    ) async throws -> Res {
        try await request(what, method: "GET", path: path, query: query)
    }

    /// Issue a request and decode the response envelope as `Res`.
    private func request<Res: Decodable>(
        _ what: String,
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String? = nil,
        query: [URLQueryItem] = []
    ) async throws -> Res {
        let (status, data) = try await execute(
            what, method: method, path: path, body: body,
            contentType: contentType, query: query
        )
        return try decode(status, data, as: Res.self)
    }

    /// Issue a request; returns `(status, body)`. Transport failures wrap as
    /// `RtDbError(internal)` — raw URL errors never escape to callers.
    private func execute(
        _ what: String,
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String? = nil,
        query: [URLQueryItem] = []
    ) async throws -> (status: Int, data: Data) {
        var components = URLComponents(string: baseUrl + path)
        if !query.isEmpty {
            components?.queryItems = query
        }
        guard let url = components?.url else {
            throw RtDbError(code: .internal, message: "invalid request URL for \(what)")
        }
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        // ARC-013: lets the server diagnose/reject a version mismatch instead
        // of a generic 400 from `deny_unknown_fields`.
        request.setValue(String(WireProtocol.version), forHTTPHeaderField: "X-Rtdb-Protocol")
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        request.httpBody = body
        do {
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw RtDbError(code: .internal, message: "\(what) returned a non-HTTP response")
            }
            return (http.statusCode, data)
        } catch let error as RtDbError {
            throw error
        } catch {
            throw RtDbError(
                code: .internal, message: "\(what) request failed: \(String(describing: error))"
            )
        }
    }

    /// 2xx decodes `T`; any other status decodes the `{code, message}`
    /// envelope, falling back to `HTTP <status>` — the raw body text never
    /// leaks into a thrown error.
    private func decode<T: Decodable>(_ status: Int, _ data: Data, as _: T.Type) throws -> T {
        guard (200 ..< 300).contains(status) else {
            throw RtDbError.decodeEnvelope(from: data)
                ?? RtDbError(code: .badRequest, message: "HTTP \(status)")
        }
        do {
            return try JSONDecoder().decode(T.self, from: data)
        } catch {
            throw RtDbError(code: .internal, message: "invalid response body: \(error)")
        }
    }
}

// MARK: - Request bodies (private; nil optionals are omitted when encoded)

private struct QueryRequest: Encodable {
    let db: String
    let query: Query
}

private struct BatchRequest: Encodable {
    let db: String
    let queries: [Query]
}

private struct MutateRequest: Encodable {
    let db: String
    let txn: Transaction
    let idempotencyKey: String?
}

private struct ScheduleRequest: Encodable {
    let db: String
    let when: ScheduleWhen
    let txn: Transaction
}

private struct DbRequest: Encodable {
    let db: String
}

private struct StartWorkflowRequest: Encodable {
    let db: String
    let spec: WorkflowSpec
}

private struct PushSchemaRequest: Encodable {
    let db: String
    let schema: SchemaDef
}

private struct PreviewSchemaRequest: Encodable {
    let schema: SchemaDef
}

// MARK: - Response envelopes (private)

private struct QueryResponse: Decodable {
    let result: JSONValue
}

private struct BatchResponse: Decodable {
    let results: [BatchQueryOutcome]
}

private struct MutateResponse: Decodable {
    let results: [StepResult]
}

private struct IdResponse: Decodable {
    let id: String
}

private struct OkResponse: Decodable {
    let ok: Bool
}

private struct CancelledResponse: Decodable {
    let cancelled: Bool
}

private struct SignalWorkflowRequest: Encodable {
    let db: String
    let name: String
    let payload: JSONValue?
}

private struct SignalWorkflowResponse: Decodable {
    let delivered: Bool
}

private struct SchedulesResponse: Decodable {
    let schedules: [ScheduleInfo]
}

private struct WorkflowsResponse: Decodable {
    let workflows: [WorkflowInfo]
}

private struct MeResponse: Decodable {
    let user: AuthedUser
}

private struct UploadResponse: Decodable {
    let id: String
}

private struct SignedUrlResponse: Decodable {
    let url: String
}
