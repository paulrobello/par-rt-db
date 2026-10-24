import Foundation
@testable import ParRtDbClient
import Testing

// MARK: - Offline mutation queue (ts-client offlineQueue parity)

//
// Mirrors ts-client/tests/offline-queue.test.ts against the Swift client's
// FakeTransport + ManualScheduler harness: mutations issued while the client
// cannot dispatch queue (opt-in), replay on the next successful auth in call
// order under their ORIGINAL idempotency keys, expire lazily by TTL, and are
// observable via getOfflineQueueStatus(). The default config never queues.

/// Scriptable authOk frame (same shape WsClientTests uses).
private let offlineAuthOkFrame =
    #"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#

/// Fresh FakeTransport per dial, retained in dial order — the offline-queue
/// tests reconnect, so each session needs its own scriptable socket.
private final class TransportRoster: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [FakeTransport] = []

    func add() -> FakeTransport {
        lock.withLock {
            let transport = FakeTransport()
            items.append(transport)
            return transport
        }
    }

    var count: Int {
        lock.withLock { items.count }
    }

    func get(_ index: Int) -> FakeTransport {
        lock.withLock { items[index] }
    }
}

/// Offline-queue client under test: FakeTransport I/O, ManualScheduler clock,
/// heartbeat disabled (only reconnect backoffs touch the scheduler), and an
/// injected jitter of 0.0 so the first backoff is exactly 250 ms.
private func makeOfflineClient(
    roster: TransportRoster,
    scheduler: ManualScheduler,
    configure: (inout RtDbClientConfig) -> Void = { _ in }
) -> RtDbClient {
    var config = RtDbClientConfig()
    config.heartbeatMs = 0
    configure(&config)
    return RtDbClient(
        url: "ws://rtdb.test", db: "app", getToken: { "tok" },
        config: config,
        transportFactory: { _ in roster.add() },
        scheduler: scheduler, random: { 0.0 }
    )
}

/// Bounded poll (5 s ceiling, 5 ms real ticks) until `condition` holds.
private func offlineWaitUntil(
    _ what: String, _ condition: @Sendable () async -> Bool
) async throws {
    let deadline = Date().addingTimeInterval(5)
    while await !condition() {
        if Date() > deadline {
            throw RtDbError(code: .internal, message: "timed out: \(what)")
        }
        try await Task.sleep(nanoseconds: 5_000_000)
    }
}

private func mutateFrames(in sent: [String]) -> [[String: Any]] {
    sent.compactMap { frame in
        guard let data = frame.data(using: .utf8),
              let object = try? JSONSerialization.jsonObject(with: data),
              let object = object as? [String: Any],
              object["type"] as? String == "mutate"
        else { return nil }
        return object
    }
}

private struct OfflineHarness {
    let roster: TransportRoster
    let client: RtDbClient
    let scheduler: ManualScheduler
}

/// Connects and leaves the handshake stuck pre-auth: every offline-eligible
/// call queues (the ts harness's `connectAndDrop` equivalent — the client
/// cannot dispatch either way).
private func queuedHarness(
    configure: (inout RtDbClientConfig) -> Void = { $0.offlineQueue = true }
) -> OfflineHarness {
    let roster = TransportRoster()
    let scheduler = ManualScheduler()
    let client = makeOfflineClient(roster: roster, scheduler: scheduler, configure: configure)
    return OfflineHarness(roster: roster, client: client, scheduler: scheduler)
}

@Suite(.timeLimit(.minutes(2)))
struct OfflineQueueTests {
    private func insertTxn(_ value: Int64) throws -> Transaction {
        try MutationBuilder().insert("items", ["n": .int(value)]).build()
    }

    private func acknowledge(_ frames: [[String: Any]], on fake: FakeTransport) async throws {
        for (index, frame) in frames.enumerated() {
            let mutId = try #require(frame["mutId"] as? String)
            await fake.release(
                #"{"type":"mutateOk","mutId":"\#(mutId)","results":[{"id":"id-\#(index + 1)","inserted":true}]}"#
            )
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func queuesWhileDisconnectedAndReplaysInCallOrderWithStableKeys() async throws {
        let harness = queuedHarness()
        await harness.client.connect()

        let task1 = Task { try await harness.client.mutate(insertTxn(1)) }
        try await offlineWaitUntil("first call queued") {
            await harness.client.getOfflineQueueStatus().depth == 1
        }
        let task2 = Task { try await harness.client.mutate(insertTxn(2), idempotencyKey: "caller-key-2") }
        try await offlineWaitUntil("second call queued") {
            await harness.client.getOfflineQueueStatus().depth == 2
        }

        // Nothing beyond the auth frame left the client while stuck pre-auth.
        try await offlineWaitUntil("first dial") { harness.roster.count > 0 }
        let fake0 = harness.roster.get(0)
        try await offlineWaitUntil("auth frame") { await fake0.sent.count == 1 }

        // Completing the handshake replays the queue on authOk, in call order.
        await fake0.release(offlineAuthOkFrame)
        try await offlineWaitUntil("two replayed mutate frames") {
            await mutateFrames(in: fake0.sent).count == 2
        }
        let sent = await mutateFrames(in: fake0.sent)
        #expect(sent.count == 2)
        // A queued mutation always carries a key — one is minted when the
        // caller supplied none (ts: ^off-).
        let minted = try #require(sent[0]["idempotencyKey"] as? String)
        #expect(minted.hasPrefix("off-"))
        #expect(try #require(sent[1]["idempotencyKey"] as? String) == "caller-key-2")

        #expect((sent[0]["txn"] as? [String: Any]) != nil)
        #expect((sent[0]["mutId"] as? String) == "mut-1")
        #expect((sent[1]["mutId"] as? String) == "mut-2")
        // Lose both acknowledgments and reconnect. The minted and supplied
        // keys must both survive unchanged, not just the caller's key.
        await fake0.enqueueClose(4000)
        try await offlineWaitUntil("reconnect backoff") {
            harness.scheduler.sleepRequests.contains(250)
        }
        harness.scheduler.advance(250)
        try await offlineWaitUntil("second transport") { harness.roster.count > 1 }
        let fake1 = harness.roster.get(1)
        await fake1.release(offlineAuthOkFrame)
        try await offlineWaitUntil("replayed on new connection") {
            await mutateFrames(in: fake1.sent).count == 2
        }
        let replay = await mutateFrames(in: fake1.sent)
        #expect(replay.map { $0["idempotencyKey"] as? String } == [minted, "caller-key-2"])
        try await acknowledge(replay, on: fake1)
        let resolved1 = try await task1.value
        let resolved2 = try await task2.value
        #expect(resolved1 == [.upsert(id: "id-1", inserted: true)])
        #expect(resolved2 == [.upsert(id: "id-2", inserted: true)])
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func inflightDropRequeuesWithOriginalKeysAcrossReplays() async throws {
        let harness = queuedHarness()
        await harness.client.connect()
        try await offlineWaitUntil("first dial") { harness.roster.count > 0 }
        let fake0 = harness.roster.get(0)
        await fake0.release(offlineAuthOkFrame)
        try await offlineWaitUntil("connected") {
            await harness.client.status().state == .connected
        }

        // Both mutations dispatch live, then the connection drops before
        // either is acked.
        let task1 = Task { try await harness.client.mutate(insertTxn(1), idempotencyKey: "k-1") }
        try await offlineWaitUntil("first live mutation") {
            await mutateFrames(in: fake0.sent).count == 1
        }
        let task2 = Task { try await harness.client.mutate(insertTxn(2), idempotencyKey: "k-2") }
        try await offlineWaitUntil("both live mutate frames") {
            await mutateFrames(in: fake0.sent).count == 2
        }
        await fake0.enqueueClose(4000)

        // The in-flight mutations re-queue (in call order, original keys).
        try await offlineWaitUntil("both requeued") {
            await harness.client.getOfflineQueueStatus().depth == 2
        }

        // First replay.
        try await offlineWaitUntil("reconnecting") {
            await harness.client.status().state == .reconnecting
        }
        harness.scheduler.advance(250)
        try await offlineWaitUntil("second transport") { harness.roster.count > 1 }
        let fake1 = harness.roster.get(1)
        try await offlineWaitUntil("second dial") {
            await harness.client.status().state == .connecting
        }
        await fake1.release(offlineAuthOkFrame)
        try await offlineWaitUntil("two replayed frames on fake1") {
            await mutateFrames(in: fake1.sent).count == 2
        }
        let replay1 = await mutateFrames(in: fake1.sent)
        #expect(replay1.map { $0["idempotencyKey"] as? String } == ["k-1", "k-2"])

        let attempts = await mutateFrames(in: fake0.sent) + mutateFrames(in: fake1.sent)
        #expect(attempts.compactMap { $0["idempotencyKey"] as? String } == ["k-1", "k-2", "k-1", "k-2"])
        try await acknowledge(replay1, on: fake1)
        let resolved1 = try await task1.value
        let resolved2 = try await task2.value
        #expect(resolved1 == [.upsert(id: "id-1", inserted: true)])
        #expect(resolved2 == [.upsert(id: "id-2", inserted: true)])
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func statusReportsDepthAndOldestAgeAndDrainsOnAuth() async throws {
        let harness = queuedHarness()
        await harness.client.connect()

        let task1 = Task { try await harness.client.mutate(insertTxn(1)) }
        try await offlineWaitUntil("first queued") {
            await harness.client.getOfflineQueueStatus().depth == 1
        }
        let task2 = Task { try await harness.client.mutate(insertTxn(2)) }
        try await offlineWaitUntil("second queued") {
            await harness.client.getOfflineQueueStatus().depth == 2
        }

        var status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 2)
        #expect(status.oldestAgeMs == 0)

        harness.scheduler.advance(5000)
        status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 2)
        #expect(status.oldestAgeMs == 5000)

        // Flushing on auth drains the queue and the status reflects it.
        try await offlineWaitUntil("first transport") { harness.roster.count > 0 }
        await harness.roster.get(0).release(offlineAuthOkFrame)
        try await offlineWaitUntil("queue drained") {
            await harness.client.getOfflineQueueStatus().depth == 0
        }
        status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 0)
        #expect(status.oldestAgeMs == nil)

        try await offlineWaitUntil("first dial") { harness.roster.count > 0 }
        let fake0 = harness.roster.get(0)
        try await offlineWaitUntil("both replayed") {
            await mutateFrames(in: fake0.sent).count == 2
        }
        for frame in await mutateFrames(in: fake0.sent) {
            let mutId = try #require(frame["mutId"] as? String)
            await fake0.release(
                #"{"type":"mutateOk","mutId":"\#(mutId)","results":[]}"#
            )
        }
        _ = try await task1.value
        _ = try await task2.value
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func expiredEntriesRejectAndNeverReplay() async throws {
        let harness = queuedHarness { config in
            config.offlineQueue = true
            config.offlineQueueTtlMs = 1000
        }
        await harness.client.connect()

        let task = Task { try await harness.client.mutate(insertTxn(1), idempotencyKey: "will-expire") }
        try await offlineWaitUntil("queued") {
            await harness.client.getOfflineQueueStatus().depth == 1
        }
        harness.scheduler.advance(2000) // past the 1000ms TTL

        // Reading the status is itself a prune point: the expired entry
        // rejects here (ts: INTERNAL).
        let status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 0)
        #expect(status.oldestAgeMs == nil)
        do {
            _ = try await task.value
            Issue.record("the expired mutation should reject")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
            #expect(error.message.contains("TTL"))
        }

        // The auth completes — no mutate frame may go out for the expired entry.
        try await offlineWaitUntil("first dial") { harness.roster.count > 0 }
        let fake0 = harness.roster.get(0)
        await fake0.release(offlineAuthOkFrame)
        try await offlineWaitUntil("connected") {
            await harness.client.status().state == .connected
        }
        #expect(await mutateFrames(in: fake0.sent).isEmpty)
        await harness.client.close()
    }

    @Test
    func defaultTtlIsFiveMinutesAndExpiresAtBoundary() async throws {
        #expect(RtDbClientConfig().offlineQueueTtlMs == 300_000)
        let harness = queuedHarness()
        // Do not connect: advancing the clock must not trigger auth deadlines.
        let task = Task { try await harness.client.mutate(insertTxn(1)) }
        try await offlineWaitUntil("queued") {
            await harness.client.getOfflineQueueStatus().depth == 1
        }
        harness.scheduler.advance(299_999)
        #expect(await harness.client.getOfflineQueueStatus().oldestAgeMs == 299_999)
        harness.scheduler.advance(1)
        #expect(await harness.client.getOfflineQueueStatus().depth == 0)
        do {
            _ = try await task.value
            Issue.record("must expire at the default TTL boundary")
        } catch let error as RtDbError {
            #expect(error.message == "offline mutation queue TTL expired")
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func closeRejectsQueuedEntriesAndClearsTheQueue() async throws {
        let harness = queuedHarness()
        await harness.client.connect()
        let task = Task { try await harness.client.mutate(insertTxn(1)) }
        try await offlineWaitUntil("queued") {
            await harness.client.getOfflineQueueStatus().depth == 1
        }
        await harness.client.close()
        do {
            _ = try await task.value
            Issue.record("the queued mutation should reject on close()")
        } catch let error as RtDbError {
            #expect(error.code == .internal)
        }
        let status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 0)
        #expect(status.oldestAgeMs == nil)
    }

    @Test(.timeLimit(.minutes(1)))
    func defaultConfigDoesNotQueueOrMintKeys() async throws {
        let harness = queuedHarness { _ in } // offlineQueue stays OFF
        await harness.client.connect()

        let task = Task { try await harness.client.mutate(insertTxn(1)) }
        // The ordinary pre-auth buffer holds the call — the offline queue
        // never sees it.
        try await offlineWaitUntil("pre-auth queued") {
            await harness.client.queuedMutationCountForTesting() == 1
        }
        let status = await harness.client.getOfflineQueueStatus()
        #expect(status.depth == 0)
        #expect(status.oldestAgeMs == nil)

        try await offlineWaitUntil("first dial") { harness.roster.count > 0 }
        let fake0 = harness.roster.get(0)
        await fake0.release(offlineAuthOkFrame)
        try await offlineWaitUntil("mutate flushed") {
            await mutateFrames(in: fake0.sent).count == 1
        }
        let frame = await fake0.sent.first { $0.contains(#""type":"mutate""#) }
        // No auto-minted key on the disabled path.
        #expect(try #require(frame).contains("idempotencyKey") == false)

        let mutId = try #require(
            await mutateFrames(in: fake0.sent).first?["mutId"] as? String
        )
        await fake0.release(
            #"{"type":"mutateOk","mutId":"\#(mutId)","results":[{"id":"id-1","inserted":true}]}"#
        )
        let resolved = try await task.value
        #expect(resolved == [.upsert(id: "id-1", inserted: true)])
        await harness.client.close()
    }
}
