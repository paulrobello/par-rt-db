import Foundation
@testable import ParRtDbClient
import Testing

// Opt-in live-server integration tests — the Swift mirror of
// rust-client/tests/http_integration.rs and python-client's
// tests/test_ws_integration.py. Skipped unless BOTH env vars are set; run
// against a dev server:
//
//   make dev-db-up   # Postgres on 127.0.0.1:55434 (rtdb/rtdb, db rtdb)
//   (cd server && RTDB_DATABASE_URL=postgres://rtdb:rtdb@127.0.0.1:55434/rtdb \
//      RTDB_PORT=8300 RTDB_ADMIN_KEY=dev-admin-key cargo run)
//   cd swift-client && RTDB_TEST_SERVER_URL=http://127.0.0.1:8300 \
//      RTDB_TEST_ADMIN_KEY=dev-admin-key swift test --filter LiveIntegrationTests
//
// The harness mints a machine token for a fresh uniquely-named `t<uuid>` db
// and deletes the db on exit; tests never touch a db they didn't create.

// MARK: - Harness

/// Doc shape decoded out of query results (`_id` is the server-assigned id;
/// `num` decodes the wire field `n`, named for the identifier-length lint).
private struct Item: Codable, Equatable, Sendable {
    let id: String
    let name: String
    let num: Double

    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case name
        case num = "n"
    }
}

private struct LiveCtx: Sendable {
    let url: String
    let db: String
    let token: String
    let adminKey: String
}

private struct OkResp: Decodable {
    let ok: Bool
}

private struct MintedToken: Decodable {
    let tokenId: String
    let token: String
}

private struct LiveTimeout: Error, CustomStringConvertible {
    let what: String
    var description: String {
        "live test: \(what)"
    }
}

/// `t` + a lowercase-hex UUID (32 chars) = 33 chars total, the server's whole
/// db-name budget (`^[a-z][a-z0-9_]{0,32}$`) — the same `t<uuid>` scheme as
/// rust-client/tests/common/mod.rs.
private func uniqueDbName() -> String {
    "t" + UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
}

/// POST an admin-route JSON body with the admin bearer; returns the raw bytes.
private func adminPost(
    _ what: String, url: String, adminKey: String, path: String, body: JSONValue
) async throws -> Data {
    guard let target = URL(string: url + path) else {
        throw RtDbError(code: .internal, message: "invalid \(what) URL")
    }
    var request = URLRequest(url: target)
    request.httpMethod = "POST"
    request.setValue("Bearer \(adminKey)", forHTTPHeaderField: "Authorization")
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try JSONEncoder().encode(body)
    let (data, response) = try await URLSession.shared.data(for: request)
    guard let http = response as? HTTPURLResponse else {
        throw RtDbError(code: .internal, message: "\(what): non-HTTP response")
    }
    guard (200 ..< 300).contains(http.statusCode) else {
        throw RtDbError.decodeEnvelope(from: data)
            ?? RtDbError(code: .internal, message: "\(what): HTTP \(http.statusCode)")
    }
    return data
}

/// Create the db, push the schema through the shipped `pushSchema` surface
/// (the admin key is a valid bearer for admin routes), and mint a machine
/// token for the data plane.
private func setupLiveCtx() async throws -> LiveCtx {
    let env = ProcessInfo.processInfo.environment
    guard var url = env["RTDB_TEST_SERVER_URL"],
          let adminKey = env["RTDB_TEST_ADMIN_KEY"],
          !url.isEmpty, !adminKey.isEmpty
    else {
        throw RtDbError(
            code: .badRequest, message: "RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY must be set"
        )
    }
    while url.hasSuffix("/") {
        url.removeLast()
    }
    let db = uniqueDbName()

    let created = try await JSONDecoder().decode(
        OkResp.self,
        from: adminPost(
            "create-db", url: url, adminKey: adminKey, path: "/admin/create-db",
            body: .object(["name": .string(db)])
        )
    )
    guard created.ok else {
        throw RtDbError(code: .internal, message: "create-db returned ok=false for \(db)")
    }

    // Leak window: the db now exists on the server, but setupLiveCtx has not
    // returned a ctx yet — a throw from pushSchema or mint-token propagates
    // before withLiveCtx can reach its teardown, so delete the db here.
    do {
        let schema = SchemaBuilder().table("items") {
            $0.field("name", .string).field("n", .number).index("by_n", on: ["n"])
        }.build()
        try await RtDbHttpClient(url: url, db: db, token: adminKey).pushSchema(schema)

        let minted = try await JSONDecoder().decode(
            MintedToken.self,
            from: adminPost(
                "mint-token", url: url, adminKey: adminKey, path: "/admin/mint-token",
                body: .object(["db": .string(db), "name": .string("swift-live")])
            )
        )

        return LiveCtx(url: url, db: db, token: minted.token, adminKey: adminKey)
    } catch {
        try? await deleteDb(url: url, adminKey: adminKey, db: db)
        throw error
    }
}

private func deleteDb(url: String, adminKey: String, db: String) async throws {
    _ = try await adminPost(
        "delete-db", url: url, adminKey: adminKey, path: "/admin/delete-db",
        body: .object(["name": .string(db), "confirm": .string(db)])
    )
}

/// `defer` cannot run async work in Swift, so teardown lives here: the db is
/// deleted whether the body passed, failed, or the harness itself errored
/// (setup's own post-create window is covered in setupLiveCtx).
private func withLiveCtx(_ body: (LiveCtx) async throws -> Void) async throws {
    let ctx = try await setupLiveCtx()
    do {
        try await body(ctx)
    } catch {
        try? await deleteDb(url: ctx.url, adminKey: ctx.adminKey, db: ctx.db)
        throw error
    }
    try await deleteDb(url: ctx.url, adminKey: ctx.adminKey, db: ctx.db)
}

/// Poll `condition` every 50 ms until it holds or `timeout` seconds elapse.
private func until(
    _ what: String, timeout: TimeInterval = 10, _ condition: () async -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
        if await condition() {
            return
        }
        try await Task.sleep(for: .milliseconds(50))
    }
    throw LiveTimeout(what: "timed out after \(timeout)s waiting for \(what)")
}

/// Consume `stream` (single-consumer: consumed exactly once, here) until
/// `matching` holds, racing a `timeout`-second deadline. A `.failed` snapshot
/// wins immediately so the caller can surface the rejection.
private func awaitSnapshot<T: Codable & Sendable>(
    _ stream: AsyncStream<QuerySnapshot<T>>,
    timeout: TimeInterval,
    matching: @escaping @Sendable (QuerySnapshot<T>) -> Bool
) async throws -> QuerySnapshot<T> {
    try await withThrowingTaskGroup(of: QuerySnapshot<T>.self) { group in
        group.addTask {
            try await Task.sleep(for: .seconds(timeout))
            throw LiveTimeout(what: "no matching snapshot within \(timeout)s")
        }
        group.addTask {
            for await snapshot in stream {
                if case .failed = snapshot {
                    return snapshot
                }
                if matching(snapshot) {
                    return snapshot
                }
            }
            throw LiveTimeout(what: "subscription stream ended before a matching snapshot")
        }
        guard let winner = try await group.next() else {
            throw LiveTimeout(what: "deadline race produced no winner")
        }
        group.cancelAll()
        _ = try? await group.next()
        return winner
    }
}

/// Insert a doc via a second (one-shot HTTP) writer, then wait for the live
/// subscription's `current` snapshot — the stream is single-consumer and was
/// consumed for the initial snapshot, so `current` is the update signal.
private func awaitLiveUpdateFromSecondWriter(
    _ ctx: LiveCtx, sub: Subscription<[Item]>
) async throws {
    let writer = RtDbHttpClient(url: ctx.url, db: ctx.db, token: ctx.token)
    let inserted = try await writer.mutate(
        MutationBuilder()
            .insert("items", ["name": .string("live"), "n": .int(42)])
            .build()
    )
    guard case let .insert(id)? = inserted.first else {
        Issue.record("expected an Insert step result, got \(inserted)")
        return
    }
    #expect(!id.isEmpty)

    try await until("live update snapshot") {
        if case let .value(docs) = sub.current {
            return docs.contains { $0.name == "live" && $0.num == 42 }
        }
        return false
    }
}

// MARK: - Tests

// `.skip(if:)` is a TestTrait only in this toolchain; `.disabled(if:)` is the
// suite-level conditional (same effect: both tests report skipped with reason).
@Suite(
    .disabled(
        if: ProcessInfo.processInfo.environment["RTDB_TEST_SERVER_URL"] == nil
            || ProcessInfo.processInfo.environment["RTDB_TEST_ADMIN_KEY"] == nil,
        "live server not configured (RTDB_TEST_SERVER_URL/RTDB_TEST_ADMIN_KEY)"
    )
)
struct LiveIntegrationTests {
    @Test(.timeLimit(.minutes(2)))
    func httpPushQueryMutateRoundTrip() async throws {
        try await withLiveCtx { ctx in
            let client = RtDbHttpClient(url: ctx.url, db: ctx.db, token: ctx.token)

            // insert two docs (the schema was pushed by the harness)
            let txn = try MutationBuilder()
                .insert("items", ["name": .string("a"), "n": .int(1)])
                .insert("items", ["name": .string("b"), "n": .int(2)])
                .build()
            let results = try await client.mutate(txn)
            #expect(results.count == 2)
            guard case let .insert(firstId)? = results.first else {
                Issue.record("expected an Insert step result, got \(results)")
                return
            }

            // ordered scan: both docs ascending by n
            let docs: [Item] = try await client.run(
                TableQuery("items").withIndex("by_n").order(.asc).take(10).build(),
                as: [Item].self
            )
            #expect(docs.map(\.name) == ["a", "b"])

            // count terminal
            let count: Int = try await client.run(
                TableQuery("items").withIndex("by_n").count().build(), as: Int.self
            )
            #expect(count == 2)

            // blob round trip: upload via the authed route, download via the
            // public serve URL (the one unauthenticated route)
            let blob = Data("swift live blob \(ctx.db)".utf8)
            let fileId = try await client.upload(blob, contentType: "text/plain")
            guard let servedUrl = URL(string: client.getUrl(fileId)) else {
                Issue.record("invalid serve URL for file \(fileId)")
                return
            }
            let (served, _) = try await URLSession.shared.data(for: URLRequest(url: servedUrl))
            #expect(served == blob)

            // error envelope against a real server: wrong version on an
            // existing doc rejects with PRECONDITION_FAILED
            let bad = try MutationBuilder().expectVersion("items", firstId, 999).build()
            do {
                _ = try await client.mutate(bad)
                Issue.record("expected expectVersion to fail with PRECONDITION_FAILED")
            } catch let error as RtDbError {
                #expect(error.code == .preconditionFailed)
            }
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func wsSubscribeReceivesLiveUpdate() async throws {
        try await withLiveCtx { ctx in
            let client = RtDbClient(
                url: ctx.url,
                db: ctx.db,
                getToken: { ctx.token },
                transportFactory: { _ in URLSessionWebSocketTransport() }
            )
            await client.connect()
            do {
                // connect() returns before auth completes; wait for the
                // authenticated state. 35 s sits above the client's own
                // sequential worst case (15 s dial + 15 s auth deadline), so a
                // slow server fails here for the right reason, not the
                // default's earlier one.
                try await until("ws connect", timeout: 35) {
                    await client.status().state == .connected
                }

                let sub = try await client.subscribe(
                    TableQuery("items").collect().build(), as: [Item].self
                )

                // the initial (empty) snapshot arrives over the stream
                let initial = try await awaitSnapshot(sub.stream, timeout: 10) { snapshot in
                    if case .value = snapshot {
                        return true
                    }
                    return false
                }
                switch initial {
                case let .value(docs):
                    #expect(docs.isEmpty)
                case let .failed(error):
                    Issue.record("subscription rejected: \(error)")
                    return
                case .pending:
                    Issue.record("stream delivered .pending")
                    return
                }

                // second writer: a one-shot HTTP mutation must reach the live
                // subscription (`_id` is reserved — insert user data only)
                try await awaitLiveUpdateFromSecondWriter(ctx, sub: sub)
            } catch {
                await client.close()
                throw error
            }
            await client.close()
        }
    }
}
