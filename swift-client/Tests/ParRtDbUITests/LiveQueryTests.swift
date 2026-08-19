import Foundation
import ParRtDbClient
@testable import ParRtDbUI
import Testing

/// Doc model for UI live-query tests (wire field `_id`).
struct UiDoc: Codable, Equatable, Sendable {
    var id: String

    enum CodingKeys: String, CodingKey {
        case id = "_id"
    }
}

@MainActor
struct LiveQueryTests {
    /// A connected client on a fresh fake — the common prefix of every test
    /// that pumps frames. Polls `status()` (public) instead of the client
    /// target's internal `awaitConnected` seam.
    private func makeConnectedClient(_ fake: FakeTransport) async throws -> RtDbClient {
        let client = RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: { "tok" },
            transportFactory: { _ in fake }
        )
        await client.connect()
        await fake.release(authOkFrame)
        try await waitUntil("connected") {
            await client.status().state == .connected
        }
        return client
    }

    /// House teardown: drive the suspended receive loop out BEFORE close(), so
    /// the non-cancellation-aware fake never leaves a cancelled waiter behind.
    private func teardown(_ client: RtDbClient, _ fake: FakeTransport) async {
        await fake.enqueueClose(nil)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func startedFalseDefersSubscribeAndStartIsIdempotent() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build(), started: false
        )
        guard case .pending = live.state else {
            Issue.record("a fresh LiveQuery must start .pending")
            return
        }
        // Only a LiveQuery subscribes on this client — with started:false, none was sent.
        #expect(await frames(ofType: "subscribe", in: fake.sent).isEmpty)
        await live.start()
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        // Idempotent: a second start attaches nothing.
        await live.start()
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(await frames(ofType: "subscribe", in: fake.sent).count == 1)
        await live.stop()
        await teardown(client, fake)
    }

    @Test(.timeLimit(.minutes(1)))
    func publishesValueAndFollowsUpdates() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        guard case .pending = live.state else {
            Issue.record("state must be .pending before the first queryUpdate")
            return
        }
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        await fake.release(#"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a"}]}"#)
        try await waitUntil("first value") {
            if case .value = live.state {
                true
            } else {
                false
            }
        }
        guard case let .value(docs) = live.state else {
            Issue.record("expected .value after queryUpdate")
            return
        }
        #expect(docs == [UiDoc(id: "a")])
        // The pump keeps running: a second update replaces the value.
        await fake.release(
            #"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a"},{"_id":"b"}]}"#
        )
        try await waitUntil("second value") {
            if case let .value(docs) = live.state {
                docs.count == 2
            } else {
                false
            }
        }
        await live.stop()
        await teardown(client, fake)
    }

    @Test(.timeLimit(.minutes(1)))
    func subscribeErrFailsTheState() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        await fake.release(
            #"{"type":"subscribeErr","queryId":"sub-1","error":{"code":"FORBIDDEN","message":"denied"}}"#
        )
        try await waitUntil("failed state") {
            if case .failed = live.state {
                true
            } else {
                false
            }
        }
        guard case let .failed(error) = live.state else {
            Issue.record("expected .failed after subscribeErr")
            return
        }
        #expect(error == RtDbError(code: .forbidden, message: "denied"))
        await live.stop()
        await teardown(client, fake)
    }

    @Test(.timeLimit(.minutes(1)))
    func closedClientSurfacesSubscribeFailure() async throws {
        let fake = FakeTransport()
        let client = RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: { "tok" },
            transportFactory: { _ in fake }
        )
        await client.close()
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        try await waitUntil("failed state") {
            if case .failed = live.state {
                true
            } else {
                false
            }
        }
        guard case let .failed(error) = live.state else {
            Issue.record("expected .failed from a subscribe on a closed client")
            return
        }
        #expect(error == RtDbError(code: .internal, message: "client is closed"))
    }

    @Test(.timeLimit(.minutes(1)))
    func stopCancelsSubscriptionExactlyOnce() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        await fake.release(#"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a"}]}"#)
        try await waitUntil("value") {
            if case .value = live.state {
                true
            } else {
                false
            }
        }
        await live.stop()
        try await waitUntil("unsubscribe frame") {
            await frames(ofType: "unsubscribe", in: fake.sent).count == 1
        }
        // Idempotent: a second stop releases nothing further.
        await live.stop()
        try await Task.sleep(nanoseconds: 50_000_000)
        #expect(await frames(ofType: "unsubscribe", in: fake.sent).count == 1)
        // stop() cancels the subscription; it does not reset the state.
        guard case .value = live.state else {
            Issue.record("state should keep the last value after stop")
            return
        }
        await teardown(client, fake)
    }

    @Test(.timeLimit(.minutes(1)))
    func deinitReleasesSubscription() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        var live: LiveQuery<[UiDoc]>? = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        guard live != nil else {
            Issue.record("LiveQuery must exist before deinit")
            return
        }
        live = nil
        // deinit hops through a Task, so the unsubscribe lands asynchronously.
        try await waitUntil("unsubscribe after deinit") {
            await frames(ofType: "unsubscribe", in: fake.sent).count == 1
        }
        await teardown(client, fake)
    }

    @Test(.timeLimit(.minutes(1)))
    func stopStartOverlapDoesNotOrphanSubscription() async throws {
        let fake = FakeTransport()
        let client = try await makeConnectedClient(fake)
        // Hold the subscribe frame in flight so start A suspends inside
        // client.subscribe — the window SwiftUI stop→start actually hits.
        await fake.armSendGate()
        let live = try LiveQuery<[UiDoc]>(
            client: client, query: TableQuery("t").collect().build()
        )
        try await waitUntil("subscribe send parked") {
            await fake.parkedSendCount == 1
        }
        // stop() lands while start A is still suspended (its handles are not
        // assigned yet), then start B runs to completion — B attaches to the
        // shape A registered, so no second subscribe frame goes out.
        await live.stop()
        await live.start()
        await fake.releaseSends()
        try await waitUntil("subscribe frame on the wire") {
            await frames(ofType: "subscribe", in: fake.sent).count == 1
        }
        // Let the displaced start A run out its resume before the final stop.
        try await Task.sleep(nanoseconds: 50_000_000)
        await live.stop()
        // The orphan discriminates here: if A's handle displaced B's without
        // cancelling, the shape's refcount is still 2 and this unsubscribe
        // never fires — the server subscription would live until disconnect.
        try await waitUntil("unsubscribe frame") {
            await frames(ofType: "unsubscribe", in: fake.sent).count == 1
        }
        #expect(await frames(ofType: "subscribe", in: fake.sent).count == 1)
        await teardown(client, fake)
    }
}

/// Frames of one wire type, in send order.
private func frames(ofType type: String, in sent: [String]) -> [String] {
    sent.filter { $0.contains(#""type":"\#(type)""#) }
}
