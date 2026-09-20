import Foundation
@testable import ParRtDbClient
import Testing

// Tests for the /admin/stream op-feed mirror (AdminStream.swift) against a
// scripted mock transport — the same fake-transport pattern WsClientTests
// uses. Covers the rust/ts semantics: unknown-kind frames are skipped,
// transport drops reconnect (with a duplicate replay), a mid-stream 4401
// close is terminal UNAUTHORIZED, an initial-connect failure finishes the
// stream, and a consumer break closes the socket.

/// Thread-safe frame script holder (pump + test run on different tasks).
private final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) {
        self.value = value
    }

    var wrapped: Value {
        get { lock.withLock { value } }
        set { lock.withLock { value = newValue } }
    }
}

/// Scripted WebSocketTransport: `receive` hands out the scripted actions
/// (frames or throws); once exhausted it sleeps until cancelled, so the pump
/// blocks exactly like a live socket with no traffic.
private final class ScriptedTransport: WebSocketTransport, @unchecked Sendable {
    private let actions: Locked<[Result<String, Error>]>
    private let connectsBox = Locked(0)
    private let closedBox = Locked(false)

    init(_ actions: [Result<String, Error>]) {
        self.actions = Locked(actions)
    }

    var connectCount: Int {
        connectsBox.wrapped
    }

    var closed: Bool {
        closedBox.wrapped
    }

    func connect(to _: URL) async throws {
        connectsBox.wrapped += 1
    }

    func send(_: String) async throws {}

    func receive() async throws -> String {
        while true {
            if let action = actions.wrapped.isEmpty ? nil : actions.wrapped.removeFirst() {
                switch action {
                case let .success(text):
                    return text
                case let .failure(error):
                    throw error
                }
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    func close(code _: UInt16) async {
        closedBox.wrapped = true
    }
}

/// Collects stream events from a child task; the test polls the collector.
private final class StreamCollector: @unchecked Sendable {
    private let framesBox = Locked([AdminStreamFrame]())
    private let errorBox = Locked<Error?>(nil)
    private let doneBox = Locked(false)

    var frames: [AdminStreamFrame] {
        framesBox.wrapped
    }

    var error: Error? {
        errorBox.wrapped
    }

    var done: Bool {
        doneBox.wrapped
    }

    func record(_ frame: AdminStreamFrame) {
        framesBox.wrapped.append(frame)
    }

    func finish(error: Error?) {
        errorBox.wrapped = error
        doneBox.wrapped = true
    }

    /// Poll until `want` frames (or an error) arrive; fails after `seconds`.
    func waitFrames(_ want: Int, seconds: Double = 5) async throws -> [AdminStreamFrame] {
        let deadline = Date().addingTimeInterval(seconds)
        while Date() < deadline {
            // Frames first: a terminal error may already be pending behind
            // the frames the caller asked for.
            if frames.count >= want {
                return frames
            }
            if let err = error {
                throw err
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(Bool(false), "timed out waiting for \(want) frames; got \(frames.count)")
        return frames
    }
}

/// Failure type for the frame-builder helpers.
private struct TestFailure: Error {}

@Suite(.serialized)
struct AdminStreamTests {
    /// Build the client with scripted transports handed out per connection.
    private func makeClient(
        scripts: [[Result<String, Error>]],
        backoffMs _: UInt64 = 1
    ) async -> (RtDbAdminClient, [ScriptedTransport]) {
        let client = RtDbAdminClient(url: "http://s.test", adminKey: "k")
        let transports = scripts.map { ScriptedTransport($0) }
        let indexBox = Locked(0)
        await client.setStreamOverrides(
            factory: { _ in
                let idx = indexBox.wrapped
                indexBox.wrapped = idx + 1
                return transports[idx]
            },
            backoffBaseMs: 1,
            backoffMaxMs: 2
        )
        return (client, transports)
    }

    /// A wire-shaped op frame (encode a real OpEvent through the envelope).
    private struct OpEnvelope: Encodable {
        let kind = "op"
        let event: OpEvent
    }

    private static func opJSON(_ docId: String) throws -> String {
        let event = OpEvent(db: "d", table: "items", docId: docId, kind: "insert", ts: 1)
        let data = try JSONEncoder().encode(OpEnvelope(event: event))
        return try Self.jsonText(data)
    }

    /// A wire-shaped gauges frame (encode a real MetricsSnapshot).
    private struct GaugesEnvelope: Encodable {
        let kind = "gauges"
        let gauges: MetricsSnapshot
    }

    private static func gaugesJSON() throws -> String {
        let data = try JSONEncoder().encode(GaugesEnvelope(gauges: Self.sampleGauges))
        return try Self.jsonText(data)
    }

    private static func jsonText(_ data: Data) throws -> String {
        guard let text = String(bytes: data, encoding: .utf8) else {
            throw TestFailure()
        }
        return text
    }

    private static var sampleGauges: MetricsSnapshot {
        MetricsSnapshot(
            queriesTotal: 1, mutationsTotal: 2, uploadsTotal: 0, wsConnections: 0,
            activeSubscriptions: 0, poolSize: 1, poolIdle: 1, uptimeSeconds: 1,
            queryLatency: LatencyStats(p50: 1, p95: 2, p99: 3),
            mutateLatency: LatencyStats(p50: 1, p95: 2, p99: 3),
            subscribeLatency: LatencyStats(p50: 1, p95: 2, p99: 3),
            subsRerunsTotal: 0, subsSkipsPointTotal: 0, subsSkipsIndexedTotal: 0,
            subsSkipsOrderedTotal: 0, subsSkipVerificationsTotal: 0,
            subsMissedPushesTotal: 0, perDbSubs: [], presenceDetail: [],
            presenceRooms: 0, presenceSessions: 0, quotaRejectionsTablesTotal: 0,
            quotaRejectionsStorageTotal: 0, quotaRejectionsSubsTotal: 0
        )
    }

    // MARK: Tests

    /// op, unknown kind, gauges from connection 1 (dropped with a plain
    /// close error), then connection 2 replays. The unknown frame must
    /// never surface and the stream must continue after the drop; breaking
    /// out of the loop closes the current socket.
    @Test func skipsUnknownKindsAndReconnectsOnDrop() async throws {
        let (client, transports) = try await makeClient(
            scripts: [
                [
                    .success(Self.opJSON("doc-1")),
                    .success(#"{"kind":"futureKind","x":1}"#),
                    .success(Self.gaugesJSON()),
                    .failure(TransportCloseError(code: nil)) // transport loss
                ],
                [
                    .success(Self.opJSON("doc-2"))
                ]
            ],
        )
        let stream = await client.streamAdmin(db: "mydb")
        let collector = StreamCollector()
        let consumer = Task {
            do {
                for try await frame in stream {
                    collector.record(frame)
                }
                collector.finish(error: nil)
            } catch {
                collector.finish(error: error)
            }
        }
        let frames = try await collector.waitFrames(3)
        #expect(frames.count == 3)
        guard case let .op(op1) = frames[0] else {
            return #expect(Bool(false), "expected op first")
        }
        #expect(op1.docId == "doc-1")
        guard case .gauges = frames[1] else {
            return #expect(Bool(false), "expected gauges second (unknown kind skipped)")
        }
        guard case let .op(op2) = frames[2] else {
            return #expect(Bool(false), "expected op after reconnect")
        }
        #expect(op2.docId == "doc-2")
        #expect(transports.count == 2 && transports[1].connectCount == 1)

        consumer.cancel()
        // Give the pump a beat to observe cancellation and close the socket.
        try await Task.sleep(for: .milliseconds(200))
        #expect(transports[1].closed)
    }

    /// A mid-stream close 4401 (credential revoked) surfaces as terminal
    /// UNAUTHORIZED after the frames before it, with no reconnect.
    @Test func credentialRevokedMidStreamIsTerminal() async throws {
        let (client, transports) = try await makeClient(
            scripts: [
                [
                    .success(Self.opJSON("doc-1")),
                    .failure(TransportCloseError(code: 4401))
                ]
            ]
        )
        let stream = await client.streamAdmin()
        let collector = StreamCollector()
        let consumer = Task {
            do {
                for try await frame in stream {
                    collector.record(frame)
                }
                collector.finish(error: nil)
            } catch {
                collector.finish(error: error)
            }
        }
        let frames = try await collector.waitFrames(1)
        guard case let .op(op1) = frames[0] else {
            return #expect(Bool(false), "expected op first")
        }
        #expect(op1.docId == "doc-1")
        // Wait for the terminal error.
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, collector.error == nil, !collector.done {
            try await Task.sleep(for: .milliseconds(10))
        }
        let err = try #require(collector.error)
        #expect((err as? RtDbError)?.code == .unauthorized, "expected UNAUTHORIZED, got \(err)")
        #expect(transports[0].connectCount == 1) // no reconnect after 4401
        consumer.cancel()
    }

    /// An initial-connect failure finishes the stream with an RtDbError
    /// instead of retrying (a bad credential must not look like a hang).
    @Test func initialConnectFailureFinishesTheStream() async throws {
        struct ConnectFailed: Error {}
        let client = RtDbAdminClient(url: "http://s.test", adminKey: "k")
        await client.setStreamOverrides(
            factory: { _ in ThrowingConnectTransport() },
            backoffBaseMs: 1,
            backoffMaxMs: 2
        )
        let stream = await client.streamAdmin()
        let collector = StreamCollector()
        let consumer = Task {
            do {
                for try await _ in stream {}
                collector.finish(error: nil)
            } catch {
                collector.finish(error: error)
            }
        }
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, collector.error == nil, !collector.done {
            try await Task.sleep(for: .milliseconds(10))
        }
        let err = try #require(collector.error)
        #expect(
            (err as? RtDbError)?.code == .internal,
            "expected INTERNAL for a failed initial connect, got \(err)"
        )
        consumer.cancel()
    }
}

/// A transport whose connect always throws (initial-connect failure test).
private final class ThrowingConnectTransport: WebSocketTransport, @unchecked Sendable {
    func connect(to _: URL) async throws {
        throw ConnectFailedMarker()
    }

    struct ConnectFailedMarker: Error {}

    func send(_: String) async throws {}

    func receive() async throws -> String {
        throw TransportCloseError(code: nil)
    }

    func close(code _: UInt16) async {}
}
