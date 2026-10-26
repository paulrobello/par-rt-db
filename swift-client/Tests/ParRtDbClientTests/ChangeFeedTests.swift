import Foundation
@testable import ParRtDbClient
import Testing

/// WebSocket change-feed channel (`watchChanges`): resume cursor on reconnect,
/// dedupe on `(feedEpoch, seq)`, the `tables` filter, and terminal `feedErr`.
struct ChangeFeedTests {
    private let authOkFrame = #"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#
    private let epoch = "0a1b2c3d4e5f6071"

    private struct Harness: Sendable {
        let fake: FakeTransport
        let client: RtDbClient
        let scheduler: ManualScheduler
    }

    private func connectedClient() async throws -> Harness {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: { "tok" },
            config: RtDbClientConfig(),
            transportFactory: { _ in fake }, scheduler: scheduler, random: { 0.0 }
        )
        await client.connect()
        await fake.release(authOkFrame)
        try await waitUntil("connected") { await client.status().state == .connected }
        return Harness(fake: fake, client: client, scheduler: scheduler)
    }

    private func waitUntil(_ what: String, _ condition: @Sendable () async -> Bool) async throws {
        let deadline = Date().addingTimeInterval(5)
        while await !condition() {
            if Date() > deadline {
                struct WaiterTimeout: Error { let what: String }
                throw WaiterTimeout(what: what)
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
    }

    private func frames(_ type: String, _ fake: FakeTransport) async -> [ClientMessage] {
        await fake.sent.filter { $0.contains(#""type":"\#(type)""#) }.compactMap {
            try? JSONDecoder().decode(ClientMessage.self, from: Data($0.utf8))
        }
    }

    private func op(_ seq: Int64, table: String = "tasks") -> ChangeOp {
        ChangeOp(seq: seq, table: table, docId: "d\(seq)", kind: "insert", doc: nil, ts: 1)
    }

    private func eventsFrame(_ feedId: String, ops: [ChangeOp], head: Int64) throws -> String {
        let message = ServerMessage.feedEvents(feedId: feedId, feedEpoch: epoch, ops: ops, head: head)
        return try String(data: JSONEncoder().encode(message), encoding: .utf8) ?? ""
    }

    private func subscribeId(_ fake: FakeTransport, index: Int) async throws -> String {
        try await waitUntil("subscribe #\(index)") { await frames("feedSubscribe", fake).count > index }
        guard case let .feedSubscribe(feedId, _, _, _) = await frames("feedSubscribe", fake)[index] else {
            throw RtDbError(code: .internal, message: "not a feedSubscribe")
        }
        return feedId
    }

    @Test(.timeLimit(.minutes(1)))
    func subscribeSendsTablesSinceAndEpoch() async throws {
        let harness = try await connectedClient()
        let stream = await harness.client.watchChanges(tables: ["tasks", "projects"], since: 41, feedEpoch: epoch)
        let feedId = try await subscribeId(harness.fake, index: 0)
        let sent = await frames("feedSubscribe", harness.fake)[0]
        #expect(sent == .feedSubscribe(feedId: feedId, since: 41, feedEpoch: epoch, tables: ["tasks", "projects"]))
        try await harness.fake.release(eventsFrame(feedId, ops: [op(42)], head: 42))
        var iterator = stream.makeAsyncIterator()
        let batch = try await iterator.next()
        #expect(batch == ChangeFeedBatch(feedEpoch: epoch, ops: [op(42)], head: 42))
        try await waitUntil("ack") { await frames("feedAck", harness.fake).count == 1 }
        #expect(await frames("feedAck", harness.fake)[0] == .feedAck(feedId: feedId, seq: 42))
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func reconnectResumesFromLastSeqAndDedupes() async throws {
        let harness = try await connectedClient()
        let stream = await harness.client.watchChanges()
        let feedId = try await subscribeId(harness.fake, index: 0)
        let first = await frames("feedSubscribe", harness.fake)[0]
        #expect(first == .feedSubscribe(feedId: feedId, since: nil, feedEpoch: nil, tables: []))
        var iterator = stream.makeAsyncIterator()
        try await harness.fake.release(eventsFrame(feedId, ops: [op(1), op(2)], head: 2))
        #expect(try await iterator.next()?.ops.map(\.seq) == [1, 2])
        // Drop the session and reconnect.
        await harness.fake.enqueueClose(nil)
        try await waitUntil("reconnecting") { await harness.client.status().state == .reconnecting }
        harness.scheduler.advance(250)
        try await waitUntil("second dial") { await harness.fake.connectedUrls.count == 2 }
        await harness.fake.release(authOkFrame)
        let resumed = try await subscribeId(harness.fake, index: 1)
        #expect(resumed == feedId)
        let replay = await frames("feedSubscribe", harness.fake)[1]
        #expect(replay == .feedSubscribe(feedId: feedId, since: 2, feedEpoch: epoch, tables: []))
        // Replayed op 2 is dropped; 3 passes through.
        try await harness.fake.release(eventsFrame(feedId, ops: [op(2), op(3)], head: 3))
        #expect(try await iterator.next()?.ops.map(\.seq) == [3])
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func emptyConfirmationAdoptsHeadAsCursor() async throws {
        let harness = try await connectedClient()
        let stream = await harness.client.watchChanges()
        let feedId = try await subscribeId(harness.fake, index: 0)
        try await harness.fake.release(eventsFrame(feedId, ops: [], head: 57))
        try await harness.fake.release(eventsFrame(feedId, ops: [op(58)], head: 58))
        var iterator = stream.makeAsyncIterator()
        #expect(try await iterator.next()?.ops.map(\.seq) == [58])
        await harness.fake.enqueueClose(nil)
        try await waitUntil("reconnecting") { await harness.client.status().state == .reconnecting }
        harness.scheduler.advance(250)
        try await waitUntil("second dial") { await harness.fake.connectedUrls.count == 2 }
        await harness.fake.release(authOkFrame)
        _ = try await subscribeId(harness.fake, index: 1)
        guard case let .feedSubscribe(_, since, _, _) = await frames("feedSubscribe", harness.fake)[1] else {
            Issue.record("expected feedSubscribe")
            return
        }
        #expect(since == 58)
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func cursorExpiredSurfacesAndIsNotRetried() async throws {
        let harness = try await connectedClient()
        let stream = await harness.client.watchChanges(since: 3)
        let feedId = try await subscribeId(harness.fake, index: 0)
        let error = RtDbError(code: .cursorExpired, message: "cursor 3 predates the retention window")
        let frame = try String(
            data: JSONEncoder().encode(ServerMessage.feedErr(feedId: feedId, error: error)), encoding: .utf8
        ) ?? ""
        await harness.fake.release(frame)
        var iterator = stream.makeAsyncIterator()
        do {
            _ = try await iterator.next()
            Issue.record("stream should throw")
        } catch let thrown as RtDbError {
            #expect(thrown.code == .cursorExpired)
        }
        // A reconnect must not re-subscribe the dead feed.
        await harness.fake.enqueueClose(nil)
        try await waitUntil("reconnecting") { await harness.client.status().state == .reconnecting }
        harness.scheduler.advance(250)
        try await waitUntil("second dial") { await harness.fake.connectedUrls.count == 2 }
        await harness.fake.release(authOkFrame)
        try await waitUntil("reconnected") { await harness.client.status().state == .connected }
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(await frames("feedSubscribe", harness.fake).count == 1)
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func cancellingStreamSendsUnsubscribe() async throws {
        let harness = try await connectedClient()
        let task = Task {
            let stream = await harness.client.watchChanges()
            for try await _ in stream {}
        }
        let feedId = try await subscribeId(harness.fake, index: 0)
        task.cancel()
        try await waitUntil("unsubscribe") { await frames("feedUnsubscribe", harness.fake).count == 1 }
        #expect(await frames("feedUnsubscribe", harness.fake)[0] == .feedUnsubscribe(feedId: feedId))
        await harness.client.close()
    }
}
