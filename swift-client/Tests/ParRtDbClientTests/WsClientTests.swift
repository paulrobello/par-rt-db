import CryptoKit
import Foundation
import Network
@testable import ParRtDbClient
import Testing

// MARK: - FakeTransport (scriptable test double — Tasks 13/14 build on this)

/// Scriptable in-process `WebSocketTransport` for WS-client tests. Frames are
/// delivered FIFO; `enqueue`/`enqueueClose`/`release` all resume a suspended
/// `receive()` first and only buffer when nobody is waiting — so a scripted
/// close reaches an already-running receive loop instead of deadlocking it.
/// `receive()` is cancellation-aware: a cancelled waiter is resumed with
/// `CancellationError` and never hangs.
actor FakeTransport: WebSocketTransport {
    private(set) var sent: [String] = []
    private(set) var connectedUrls: [URL] = []
    private(set) var closeCode: UInt16?
    private var incoming: [Result<String, Error>] = []
    private var waiters: [(id: UInt64, continuation: CheckedContinuation<String, Error>)] = []
    private var nextWaiterId: UInt64 = 0

    /// Buffer `frame` for the next `receive()` — or hand it straight to a
    /// suspended receiver.
    func enqueue(_ frame: String) {
        deliver(.success(frame))
    }

    /// Make the next `receive()` throw `TransportCloseError(code:)` — or fail
    /// a suspended receiver with it.
    func enqueueClose(_ code: UInt16?) {
        deliver(.failure(TransportCloseError(code: code)))
    }

    /// Resume the first suspended `receive()` waiter with `frame`; if none is
    /// waiting, buffer it for the next `receive()`.
    func release(_ frame: String) {
        deliver(.success(frame))
    }

    /// Test introspection: how many `receive()` calls are currently suspended.
    var waiterCount: Int {
        waiters.count
    }

    func connect(to url: URL) async throws {
        connectedUrls.append(url)
    }

    func send(_ text: String) async throws {
        sent.append(text)
    }

    func receive() async throws -> String {
        try Task.checkCancellation()
        if !incoming.isEmpty {
            return try incoming.removeFirst().get()
        }
        let id = nextWaiterId
        nextWaiterId += 1
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                waiters.append((id, continuation))
                // Cancellation may have fired before registration; re-check
                // inside the actor so the waiter can never dangle.
                if Task.isCancelled {
                    removeWaiter(id: id)?.resume(throwing: CancellationError())
                }
            }
        } onCancel: {
            // Never resume from here directly — that would race `deliver`.
            // Hop to the actor: the membership-checked removal below is
            // serialized with `deliver` (which removes the waiter from the
            // array before resuming), so exactly one of the two paths wins.
            Task { await self.cancelWaiter(id: id) }
        }
    }

    func close(code: UInt16) async {
        closeCode = code
    }

    /// Shared delivery path: waiter first (keeping the queue/waiters
    /// invariant — never both nonempty — which `receive()` relies on).
    private func deliver(_ result: Result<String, Error>) {
        if waiters.isEmpty {
            incoming.append(result)
        } else {
            waiters.removeFirst().continuation.resume(with: result)
        }
    }

    /// Resume waiter `id` with `CancellationError` iff it is still queued.
    private func cancelWaiter(id: UInt64) {
        removeWaiter(id: id)?.resume(throwing: CancellationError())
    }

    /// Membership-checked removal — the one-resume guarantee for the
    /// deliver-vs-cancel race.
    private func removeWaiter(id: UInt64) -> CheckedContinuation<String, Error>? {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return nil }
        return waiters.remove(at: index).continuation
    }
}

// MARK: - Loopback WebSocket server (Network.framework)

/// One-shot resume-once continuation gate for NWListener readiness.
/// @unchecked Sendable: `continuation` is only touched under `lock`, and each
/// resume path nils it so it can only fire once.
private final class OnceContinuation: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Error>?

    func set(_ continuation: CheckedContinuation<Void, Error>) {
        lock.withLock { self.continuation = continuation }
    }

    func resumeReturning() {
        lock.withLock {
            continuation?.resume()
            continuation = nil
        }
    }

    func resumeThrowing(_ error: Error) {
        lock.withLock {
            continuation?.resume(throwing: error)
            continuation = nil
        }
    }
}

/// Minimal loopback WebSocket server for exercising `URLSessionWebSocketTransport`
/// against a real socket: completes the HTTP upgrade handshake, answers the
/// client's ping with a pong, then closes with the scripted close code.
/// @unchecked Sendable: `listener` is read under `lock`; every other mutable
/// property is confined to the serial `queue` all NW callbacks run on.
private final class LoopbackWsServer: @unchecked Sendable {
    private let queue = DispatchQueue(label: "ParRtDbClientTests.loopback-ws")
    private let lock = NSLock()
    private var listener: NWListener?
    private let closeCode: UInt16
    private var handshake = Data()
    private var handshaken = false
    private var closed = false

    init(closeCode: UInt16) {
        self.closeCode = closeCode
    }

    var port: UInt16 {
        lock.withLock { listener?.port?.rawValue ?? 0 }
    }

    /// Start listening; returns once the listener is ready to accept.
    func start() async throws {
        let listener = try NWListener(using: .tcp, on: .any)
        lock.withLock { self.listener = listener }
        let gate = OnceContinuation()
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            // Store the continuation and start the listener from inside the
            // body: .ready can only fire after start(), so the gate can never
            // lose the wakeup.
            gate.set(continuation)
            listener.stateUpdateHandler = { state in
                switch state {
                case .ready: gate.resumeReturning()
                case let .failed(error): gate.resumeThrowing(error)
                default: break
                }
            }
            listener.newConnectionHandler = { [weak self] connection in
                self?.accept(connection)
            }
            listener.start(queue: self.queue)
        }
    }

    func stop() {
        queue.async { [weak self] in
            self?.connection?.cancel()
            self?.listener?.cancel()
        }
    }

    private var connection: NWConnection?

    private func accept(_ connection: NWConnection) {
        self.connection = connection
        connection.stateUpdateHandler = { [weak connection] state in
            guard let connection else { return }
            if case .ready = state {
                self.receiveLoop(connection)
            }
        }
        connection.start(queue: queue)
    }

    /// Byte-stream state machine: drain the HTTP upgrade request, then treat
    /// any further bytes (the transport's readiness ping) as the trigger to
    /// pong and close. Everything runs on `queue`, so no locking is needed.
    private func receiveLoop(_ connection: NWConnection) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 1 << 16) { data, _, _, error in
            if let data, !data.isEmpty {
                // Re-wrap so all downstream indexing is zero-based regardless
                // of how the callback's Data was constructed.
                self.handle(Data(data), from: connection)
            }
            guard error == nil, !self.closed else { return }
            self.receiveLoop(connection)
        }
    }

    private func handle(_ data: Data, from connection: NWConnection) {
        var frameBytes = data
        if !handshaken {
            handshake.append(data)
            guard let range = handshake.range(of: Data("\r\n\r\n".utf8)) else { return }
            let request = String(data: handshake[..<range.lowerBound], encoding: .utf8) ?? ""
            let key = Self.webSocketKey(in: request)
            let upgrade = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: \(Self.acceptValue(for: key))\r\n\r\n"
            )
            handshake.removeSubrange(...range.upperBound.advanced(by: -1))
            handshaken = true
            connection.send(content: Data(upgrade.utf8), completion: .contentProcessed { _ in })
            // Bytes past the upgrade terminator arrived with the handshake
            // itself — the residual, not the raw chunk, is the ping frame.
            frameBytes = handshake
            handshake.removeAll()
            guard !frameBytes.isEmpty else { return }
        }
        guard !closed else { return }
        closed = true
        // Pong MUST echo the ping's application data (RFC 6455 §5.5.3) —
        // URLSession validates the match and never completes sendPing on a
        // payload-mismatched pong. Server-to-client frames are never masked.
        let payload = Self.clientFramePayload(frameBytes) ?? Data()
        connection.send(
            content: Data([0x8A, UInt8(payload.count)] + payload),
            completion: .contentProcessed { _ in }
        )
        let code = closeCode
        connection.send(
            content: Data([0x88, 0x02, UInt8(code >> 8), UInt8(code & 0xFF)]),
            completion: .contentProcessed { _ in }
        )
    }

    /// Unmask a small (≤125-byte payload) client frame's application data —
    /// the only client frame this server ever sees is the readiness ping.
    private static func clientFramePayload(_ data: Data) -> Data? {
        guard data.count >= 2 else { return nil }
        let masked = data[data.startIndex + 1] & 0x80 != 0
        let length = Int(data[data.startIndex + 1] & 0x7F)
        guard length < 126 else { return nil }
        var payloadStart = data.startIndex + 2
        // The mask must be a value copy, not a Data slice — slices keep the
        // parent's index space, so subscripting them from 0 traps.
        var mask: [UInt8] = []
        if masked {
            guard data.count >= 6 else { return nil }
            mask = [UInt8](data[data.startIndex + 2 ..< data.startIndex + 6])
            payloadStart = data.startIndex + 6
        }
        guard data.count >= payloadStart - data.startIndex + length else { return nil }
        var payload = Data()
        for offset in 0 ..< length {
            let byte = data[payloadStart + offset]
            payload.append(mask.isEmpty ? byte : byte ^ mask[offset % mask.count])
        }
        return payload
    }

    /// `Sec-WebSocket-Key` header value from the upgrade request. Line bounds
    /// come from a `"\r\n"` range search — CRLF is a single grapheme in Swift,
    /// so per-character scans for "\r" never match it.
    private static func webSocketKey(in request: String) -> String {
        guard
            let marker = request.range(of: "sec-websocket-key:", options: .caseInsensitive),
            let lineEnd = request.range(of: "\r\n", range: marker.upperBound ..< request.endIndex)
        else { return "" }
        return request[marker.upperBound ..< lineEnd.lowerBound]
            .trimmingCharacters(in: .whitespaces)
    }

    /// `base64(SHA1(key + GUID))` — RFC 6455 Sec-WebSocket-Accept.
    private static func acceptValue(for key: String) -> String {
        let magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        let digest = Insecure.SHA1.hash(data: Data((key + magic).utf8))
        return Data(digest).base64EncodedString()
    }
}

/// Failure thrown when a bounded poll's deadline lapses — fails the test
/// outright instead of stalling on an await that can never complete. Carries
/// the condition's label so the failure names what stalled.
private struct WaiterTimeout: Error, CustomStringConvertible {
    private let what: String

    init(_ what: String = "the condition") {
        self.what = what
    }

    var description: String {
        "timed out after 5 s waiting for: \(what)"
    }
}

// MARK: - ManualScheduler (controllable WScheduler)

/// Controllable `WScheduler` for WS-client lifecycle tests: a virtual `now()`,
/// a record of every requested sleep, and `advance(by:)` that resumes every
/// sleep whose deadline has passed — tests never real-sleep. A lock-guarded
/// class, not an actor, because `WScheduler.now()` is a synchronous
/// requirement. `sleep` is cancellation-aware (a cancelled sleeper resumes
/// immediately so the client's task groups can always drain); `onCancel` and
/// `advance` both remove by membership-checked id under `lock`, so exactly one
/// of the two paths resumes each sleeper.
/// @unchecked Sendable: all mutable state is only ever touched while holding
/// `lock`, and continuations are resumed after it is released.
final class ManualScheduler: WScheduler, @unchecked Sendable {
    private struct Sleeper {
        let id: UInt64
        let deadline: UInt64
        let continuation: CheckedContinuation<Void, Never>
    }

    private let lock = NSLock()
    private var nowMs: UInt64 = 0
    private var nextSleeperId: UInt64 = 0
    private var requests: [UInt64] = []
    private var sleepers: [Sleeper] = []

    /// Every sleep duration requested so far, in order (including sleeps that
    /// were later cancelled).
    var sleepRequests: [UInt64] {
        lock.withLock { requests }
    }

    func now() -> UInt64 {
        lock.withLock { nowMs }
    }

    func sleep(_ ms: UInt64) async {
        let id: UInt64 = lock.withLock {
            let id = nextSleeperId
            nextSleeperId += 1
            requests.append(ms)
            return id
        }
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                // One critical section decides park-or-resume and computes the
                // deadline under the lock: cancellation may have fired before
                // registration, and time cannot advance between deciding and
                // parking — so the sleeper can neither dangle nor miss its
                // own deadline.
                let parked: Bool = lock.withLock {
                    if Task.isCancelled {
                        return false
                    }
                    sleepers.append(Sleeper(id: id, deadline: nowMs + ms, continuation: continuation))
                    return true
                }
                if !parked {
                    continuation.resume()
                }
            }
        } onCancel: {
            let continuation: CheckedContinuation<Void, Never>? = lock.withLock {
                if let index = sleepers.firstIndex(where: { $0.id == id }) {
                    return sleepers.remove(at: index).continuation
                }
                return nil
            }
            continuation?.resume()
        }
    }

    /// Move virtual time forward `ms`, resuming every due sleeper in order.
    func advance(_ ms: UInt64) {
        let due: [CheckedContinuation<Void, Never>] = lock.withLock {
            nowMs += ms
            let due = sleepers.filter { $0.deadline <= nowMs }
            sleepers.removeAll { $0.deadline <= nowMs }
            return due.map(\.continuation)
        }
        due.forEach { $0.resume() }
    }
}

// MARK: - Lifecycle fixtures

/// The authOk frame every lifecycle test uses to complete a handshake.
private let authOkFrame = #"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#

/// getToken behavior + call counter. @unchecked Sendable: `value`/`calls` are
/// only ever touched while holding `lock`.
final class TokenBox: @unchecked Sendable {
    private let lock = NSLock()
    private var value: String?
    private(set) var calls: Int = 0

    init(_ value: String? = "tok") {
        self.value = value
    }

    var token: String? {
        get { lock.withLock { value } }
        set { lock.withLock { value = newValue } }
    }

    func next() -> String? {
        lock.withLock {
            calls += 1
            return value
        }
    }
}

/// getToken provider that parks inside `next()` until the test releases it —
/// reproduces `close()` landing while the provider call is suspended.
/// Structured like FakeTransport (actor, waiter list, one-resume guarantee).
actor GatedTokenBox {
    private var waiters: [CheckedContinuation<String?, Never>] = []
    private var resolved: String?
    private var isResolved = false
    private(set) var parked = false

    func next() async -> String? {
        if isResolved {
            return resolved
        }
        parked = true
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }

    /// Resolve the parked (or next) call with `value`.
    func resume(with value: String?) {
        resolved = value
        isResolved = true
        parked = false
        let toResume = waiters
        waiters.removeAll()
        toResume.forEach { $0.resume(returning: value) }
    }
}

/// Collected `ClientStatus` values from a status-stream consumer.
/// @unchecked Sendable: `items` is only ever touched while holding `lock`.
final class StatusLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [ClientStatus] = []

    func append(_ status: ClientStatus) {
        lock.withLock { items.append(status) }
    }

    var all: [ClientStatus] {
        lock.withLock { items }
    }
}

/// Collected `ServerMessage`s from a registered hook. @unchecked Sendable:
/// `items` is only ever touched while holding `lock`.
final class MessageLog: @unchecked Sendable {
    private let lock = NSLock()
    private var items: [ServerMessage] = []

    func record(_ message: ServerMessage) {
        lock.withLock { items.append(message) }
    }

    var all: [ServerMessage] {
        lock.withLock { items }
    }
}

/// Doc model shared by the Task 14 subscription tests (wire fields `_id`/`n`).
private struct Task14Doc: Codable, Equatable, Sendable {
    var id: String
    var num: Int

    enum CodingKeys: String, CodingKey {
        case id = "_id"
        case num = "n"
    }
}

/// A connected client on a fresh fake — the common prefix of Task 14 tests.
/// `scheduler` is exposed because reconnect tests advance it past backoffs.
private struct Task14Harness: Sendable {
    let fake: FakeTransport
    let client: RtDbClient
    let scheduler: ManualScheduler
}

/// FakeTransport decorator that fails the FIRST send issued after `after`
/// frames have already reached the underlying fake (the auth frame is send
/// #1, so `after: 1` lets the handshake pass and fails the next send once).
/// Exercises the client's failed-send re-queue path deterministically.
private actor FailSendAfterTransport: WebSocketTransport {
    private let underlying: FakeTransport
    private let after: Int
    private var failedOnce = false

    init(underlying: FakeTransport, after: Int) {
        self.underlying = underlying
        self.after = after
    }

    func connect(to url: URL) async throws {
        try await underlying.connect(to: url)
    }

    func send(_ text: String) async throws {
        if !failedOnce, await underlying.sent.count >= after {
            failedOnce = true
            throw TransportCloseError(code: nil)
        }
        try await underlying.send(text)
    }

    func receive() async throws -> String {
        try await underlying.receive()
    }

    func close(code: UInt16) async {
        await underlying.close(code: code)
    }
}

// MARK: - Tests

struct WsClientTests {
    // MARK: FakeTransport behavior

    @Test func fakeTransportRecordsAndCloses() async throws {
        let fake = FakeTransport()
        await fake.enqueue(#"{"type":"pong"}"#)
        let frame = try await fake.receive()
        #expect(frame.contains("pong"))
        await fake.enqueueClose(4401)
        do {
            _ = try await fake.receive()
            Issue.record("receive should throw after enqueueClose")
        } catch let error as TransportCloseError {
            #expect(error.code == 4401)
        }
    }

    @Test func fakeTransportDeliversBufferedFramesInOrder() async throws {
        let fake = FakeTransport()
        await fake.enqueue("one")
        await fake.enqueue("two")
        #expect(try await fake.receive() == "one")
        #expect(try await fake.receive() == "two")
    }

    @Test func fakeTransportRecordsConnectSendAndClose() async throws {
        let fake = FakeTransport()
        let url = try #require(URL(string: "ws://rtdb.test/sync"))
        try await fake.connect(to: url)
        try await fake.send(#"{"type":"ping"}"#)
        await fake.close(code: 1000)
        let sent = await fake.sent
        #expect(sent == [#"{"type":"ping"}"#])
        let closeCode = await fake.closeCode
        #expect(closeCode == 1000)
        let urls = await fake.connectedUrls
        #expect(urls == [url])
    }

    @Test func fakeTransportReleaseResumesSuspendedReceiver() async throws {
        let fake = FakeTransport()
        async let frame: String = fake.receive()
        try await waitForWaiter(on: fake, count: 1)
        await fake.release("late")
        #expect(try await frame == "late")
        // With no waiter anymore, a release must buffer instead.
        await fake.release("buffered")
        #expect(try await fake.receive() == "buffered")
    }

    @Test func fakeTransportEnqueueCloseWakesSuspendedReceiver() async throws {
        let fake = FakeTransport()
        async let frame: String = fake.receive()
        try await waitForWaiter(on: fake, count: 1)
        await fake.enqueueClose(4401)
        do {
            _ = try await frame
            Issue.record("suspended receive should throw when enqueueClose fires")
        } catch let error as TransportCloseError {
            #expect(error.code == 4401)
        }
    }

    /// The WS client's receive loop suspends inside `receive()` for the whole
    /// connection lifetime, so scripted input has to reach a WAITING receiver,
    /// not just buffer. Bounded poll (5 s ceiling) until the waiter registers;
    /// a timeout throws rather than leaving the caller's pending await hung.
    private func waitForWaiter(on fake: FakeTransport, count: Int) async throws {
        let deadline = Date().addingTimeInterval(5)
        while await fake.waiterCount < count {
            if Date() > deadline {
                throw WaiterTimeout("waiter count to reach \(count)")
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func fakeTransportReceiveThrowsOnCancellation() async throws {
        let fake = FakeTransport()
        let suspended = Task { try await fake.receive() }
        try await waitForWaiter(on: fake, count: 1)
        suspended.cancel()
        do {
            _ = try await suspended.value
            Issue.record("cancelled receive should throw CancellationError")
        } catch is CancellationError {
            // expected — and prompt: one actor hop, no deliver required
        }
        let waiters = await fake.waiterCount
        #expect(waiters == 0)
        // The cancelled receive leaves the fake fully usable.
        await fake.enqueue("still-usable")
        #expect(try await fake.receive() == "still-usable")
    }

    // MARK: WScheduler

    @Test func systemSchedulerReportsEpochMsAndSleeps() async throws {
        let scheduler = SystemScheduler()
        let before = UInt64(Date().addingTimeInterval(-1).timeIntervalSince1970 * 1000)
        let now = scheduler.now()
        let after = UInt64(Date().addingTimeInterval(1).timeIntervalSince1970 * 1000)
        #expect(now >= before && now <= after)
        let start = scheduler.now()
        try await scheduler.sleep(5)
        let end = scheduler.now()
        #expect(end >= start)
    }

    // MARK: URLSessionWebSocketTransport

    @Test func urlSessionTransportSatisfiesSeam() {
        let transport: any WebSocketTransport = URLSessionWebSocketTransport()
        #expect(transport is URLSessionWebSocketTransport)
    }

    @Test(.timeLimit(.minutes(1)))
    func urlSessionTransportSurfacesPeerClose() async throws {
        let server = LoopbackWsServer(closeCode: 4401)
        try await server.start()
        defer { server.stop() }
        let transport = URLSessionWebSocketTransport()
        let url = try #require(URL(string: "ws://127.0.0.1:\(server.port)/sync"))
        try await transport.connect(to: url)
        do {
            _ = try await transport.receive()
            Issue.record("receive should throw after the peer closes")
        } catch let error as TransportCloseError {
            // Empirically pinned on macOS 14: after the peer closes,
            // `task.closeCode` reflects the received code, so the transport
            // can surface the server's terminal 4401.
            #expect(error.code == 4401)
        }
    }

    // MARK: WS client lifecycle (Task 13)

    /// Client under test: FakeTransport I/O, ManualScheduler clock, and an
    /// injected jitter of 0.0 so the backoff multiplier (0.5 + 0.5 * random)
    /// is exactly 0.5 — every recorded backoff sleep is half its raw delay.
    /// (Each session also records a 15 s connect-timeout sleep that is
    /// cancelled the moment the instant fake dial completes.)
    private func makeClient(
        fake: FakeTransport,
        scheduler: ManualScheduler,
        getToken: @escaping @Sendable () async -> String? = { "tok" },
        random: @escaping @Sendable () -> Double = { 0.0 }
    ) -> RtDbClient {
        RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: getToken,
            transportFactory: { _ in fake }, scheduler: scheduler, random: random
        )
    }

    /// Bounded poll (5 s ceiling, 5 ms real ticks) until `condition` holds —
    /// a timeout throws WaiterTimeout (naming the stalled condition) rather
    /// than stalling the test forever.
    private func waitUntil(
        _ what: String,
        _ condition: @Sendable () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(5)
        while await !condition() {
            if Date() > deadline {
                throw WaiterTimeout(what)
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
    }

    /// Wait until `scheduler.sleepRequests` stops growing across a 30 ms
    /// settle gap — proof the run loop's post-close unwind quiesced — then
    /// return the stable count. Bounded by the same 5 s ceiling.
    private func waitForStableSleepRequests(on scheduler: ManualScheduler) async throws -> Int {
        let deadline = Date().addingTimeInterval(5)
        var count = scheduler.sleepRequests.count
        while true {
            try await Task.sleep(nanoseconds: 30_000_000)
            let next = scheduler.sleepRequests.count
            if next == count {
                return count
            }
            count = next
            if Date() > deadline {
                throw WaiterTimeout("a stable sleep-request count")
            }
        }
    }

    private func waitUntilState(_ client: RtDbClient, _ target: WsState) async throws -> ClientStatus {
        try await waitUntil("state \(target)") {
            await client.status().state == target
        }
        return await client.status()
    }

    private func waitUntilPingCount(_ fake: FakeTransport, _ count: Int) async throws {
        try await waitUntil("\(count) pings") {
            await fake.sent.filter { $0.contains(#""type":"ping""#) }.count >= count
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func connectAuthsAndBecomesConnected() async {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        async let connected: Void = client.awaitConnected()
        await client.connect()
        await fake.release(authOkFrame)
        await connected
        let sent = await fake.sent
        guard let authFrame = sent.first else {
            Issue.record("expected the auth frame to be the first frame sent")
            return
        }
        #expect(authFrame.contains(#""type":"auth""#))
        #expect(authFrame.contains(#""token":"tok""#))
        #expect(authFrame.contains(#""db":"app""#))
        let urls = await fake.connectedUrls
        #expect(urls.map(\.absoluteString) == ["ws://rtdb.test/sync"])
        let status = await client.status()
        #expect(status.state == .connected)
        #expect(status.user?.kind == .machine)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func authDeadlineTearsDownWhenNoAuthOk() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let tokens = TokenBox("tok")
        let client = makeClient(fake: fake, scheduler: scheduler, getToken: { tokens.next() })
        await client.connect()
        // The dial records its (instantly cancelled) connect-timeout sleep,
        // then the handshake parks on receive behind the auth-deadline sleep.
        try await waitUntil("auth deadline sleep") {
            scheduler.sleepRequests.count >= 2
        }
        #expect(scheduler.sleepRequests == [connectTimeoutMs, authDeadlineMs])
        // Jump past the deadline: the handshake loses, the transport is torn
        // down, and the client goes to .reconnecting with a backoff pending.
        scheduler.advance(authDeadlineMs)
        try await waitUntil("backoff sleep") {
            scheduler.sleepRequests.count >= 3
        }
        #expect(await client.status().state == .reconnecting)
        #expect(await fake.closeCode == 1000)
        // Attempt-0 backoff with jitter 0.0 is half of 500 ms; burning it
        // dials again with a fresh getToken and a fresh auth frame.
        scheduler.advance(250)
        try await waitUntil("second auth deadline sleep") {
            scheduler.sleepRequests.count >= 5
        }
        #expect(await fake.connectedUrls.count == 2)
        let sent = await fake.sent
        #expect(sent.filter { $0.contains(#""type":"auth""#) }.count == 2)
        #expect(tokens.calls == 2)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func close4401IsTerminal() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        try await waitForWaiter(on: fake, count: 1)
        await fake.enqueueClose(4401)
        let status = try await waitUntilState(client, .closed)
        #expect(status.user == nil)
        // Terminal: far-future time produces no second dial, and connect()
        // cannot revive the client.
        scheduler.advance(60000)
        #expect(await fake.connectedUrls.count == 1)
        await client.connect()
        scheduler.advance(60000)
        #expect(await client.status().state == .closed)
        #expect(await fake.connectedUrls.count == 1)
    }

    @Test(.timeLimit(.minutes(1)))
    func authErrFrameIsTerminal() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(#"{"type":"authErr","error":{"code":"UNAUTHORIZED","message":"bad token"}}"#)
        _ = try await waitUntilState(client, .closed)
        scheduler.advance(60000)
        #expect(await fake.connectedUrls.count == 1)
    }

    @Test(.timeLimit(.minutes(1)))
    func heartbeatPingsAndDetectsDeath() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        // authOk starts the heartbeat: [connect timeout (cancelled),
        // auth deadline (cancelled), heartbeat].
        try await waitUntil("heartbeat sleep") {
            scheduler.sleepRequests.count >= 3
        }
        // One heartbeat interval in: a ping went out, and liveness (2x
        // heartbeat since the last pong) has NOT expired.
        scheduler.advance(20000)
        try await waitUntilPingCount(fake, 1)
        #expect(await client.status().state == .connected)
        // A pong refreshes liveness; wait for it to be consumed before the
        // next tick so the test is order-deterministic.
        await fake.release(#"{"type":"pong"}"#)
        try await waitUntil("pong consumed") {
            await client.livenessLastPongMs() == 20000
        }
        // Second tick: still alive only because the pong landed.
        scheduler.advance(20000)
        try await waitUntilPingCount(fake, 2)
        #expect(await client.status().state == .connected)
        // Third tick with no further pong: 2x heartbeat since the last pong —
        // presumed dead, torn down, .reconnecting.
        scheduler.advance(20000)
        _ = try await waitUntilState(client, .reconnecting)
        try await waitUntil("backoff pending") {
            // [connect, auth deadline, heartbeat x3, backoff]
            scheduler.sleepRequests.count >= 6
        }
        #expect(scheduler.sleepRequests.last == 250)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func backoffProgressionBaseToMaxWithJitterBounds() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        var expected: [UInt64] = []
        for attempt in 0 ... 5 {
            // Each failed session records [connect timeout (instantly
            // cancelled), auth deadline], then fails at the deadline...
            let deadlineCount = expected.count + 2
            try await waitUntil("auth deadline sleep \(attempt)") {
                scheduler.sleepRequests.count >= deadlineCount
            }
            scheduler.advance(authDeadlineMs)
            expected.append(connectTimeoutMs)
            expected.append(authDeadlineMs)
            // ...then the jittered backoff (jitter 0.0 -> multiplier 0.5, so
            // the sleep is half of min(500 * 2^attempt, 15_000) exactly).
            let raw: UInt64 = min(500 * (1 << UInt64(attempt)), 15000)
            let backoff: UInt64 = raw / 2
            expected.append(backoff)
            let backoffCount = expected.count
            try await waitUntil("backoff sleep \(attempt)") {
                scheduler.sleepRequests.count >= backoffCount
            }
            scheduler.advance(backoff)
        }
        #expect(scheduler.sleepRequests == expected)
        #expect(
            expected == [15000, 15000, 250, 15000, 15000, 500,
                         15000, 15000, 1000, 15000, 15000, 2000,
                         15000, 15000, 4000, 15000, 15000, 7500]
        )
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func backoffJitterStaysWithinHalfToFull() async throws {
        // Jitter 0.0 -> multiplier 0.5: attempt 0's backoff is half of 500.
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler, random: { 0.0 })
        await client.connect()
        try await waitUntil("auth deadline sleep") {
            scheduler.sleepRequests.count >= 2
        }
        scheduler.advance(authDeadlineMs)
        try await waitUntil("backoff sleep") {
            scheduler.sleepRequests.count >= 3
        }
        #expect(scheduler.sleepRequests.last == 250)
        await client.close()
        // Jitter 0.9 -> multiplier 0.95: attempt 0's backoff is 475 of 500.
        let fakeUpper = FakeTransport()
        let schedulerUpper = ManualScheduler()
        let clientUpper = makeClient(
            fake: fakeUpper, scheduler: schedulerUpper, random: { 0.9 }
        )
        await clientUpper.connect()
        try await waitUntil("upper auth deadline sleep") {
            schedulerUpper.sleepRequests.count >= 2
        }
        schedulerUpper.advance(authDeadlineMs)
        try await waitUntil("upper backoff sleep") {
            schedulerUpper.sleepRequests.count >= 3
        }
        #expect(schedulerUpper.sleepRequests.last == 475)
        await clientUpper.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func repeatedConnectSpawnsOneLoopAndOneSocket() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await client.connect()
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        // Exactly one dial and one auth frame despite the connect() spam, and
        // connect() while connected changes nothing.
        await client.connect()
        await client.connect()
        #expect(await fake.connectedUrls.count == 1)
        let sent = await fake.sent
        #expect(sent.filter { $0.contains(#""type":"auth""#) }.count == 1)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func closeSetsClosedAndCancelsWork() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        try await waitUntil("heartbeat sleep") {
            scheduler.sleepRequests.count >= 3
        }
        await client.close()
        // The cancelled session unwinds over several actor hops: wait for a
        // stable sleep-request count (proof of quiescence) before asserting
        // the terminal invariants — close() must be the last word.
        let stableCount = try await waitForStableSleepRequests(on: scheduler)
        #expect(stableCount == 3)
        let status = await client.status()
        #expect(status.state == .closed)
        #expect(status.user == nil)
        // No heartbeat fires, no reconnect dials, no new sleeps — the parked
        // heartbeat sleeper was cancelled, so advancing time resumes nothing.
        scheduler.advance(120_000)
        #expect(scheduler.sleepRequests.count == stableCount)
        #expect(await fake.sent.filter { $0.contains(#""type":"ping""#) }.isEmpty)
        #expect(await fake.connectedUrls.count == 1)
        // Idempotent, and terminal: connect() cannot revive a closed client.
        await client.close()
        await client.connect()
        scheduler.advance(120_000)
        #expect(await client.status().state == .closed)
        #expect(await fake.connectedUrls.count == 1)
    }

    @Test(.timeLimit(.minutes(1)))
    func closeDuringHandshakeTearsDown() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        try await waitUntil("auth deadline sleep") {
            scheduler.sleepRequests.count >= 2
        }
        await client.close()
        // The in-flight handshake unwinds over several actor hops — a stable
        // sleep-request count proves it finished. The unwind must NOT record
        // a backoff or flip .closed -> .reconnecting on its way out.
        let stableCount = try await waitForStableSleepRequests(on: scheduler)
        #expect(stableCount == 2)
        #expect(await client.status().state == .closed)
        scheduler.advance(60000)
        #expect(scheduler.sleepRequests.count == stableCount)
        #expect(await fake.connectedUrls.count == 1)
        #expect(await client.status().state == .closed)
    }

    /// Window-B regression: close() lands while getToken() is suspended; the
    /// provider then returns nil. The nil arm must not setState(.idle) after
    /// close() — terminal holds, nothing further, ever.
    @Test(.timeLimit(.minutes(1)))
    func closeWhileGetTokenSuspendedStaysClosedWhenNilReturns() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let tokens = GatedTokenBox()
        let client = makeClient(fake: fake, scheduler: scheduler, getToken: { await tokens.next() })
        await client.connect()
        try await waitUntil("getToken parked") {
            await tokens.parked
        }
        await client.close()
        await tokens.resume(with: nil)
        let stableCount = try await waitForStableSleepRequests(on: scheduler)
        #expect(stableCount == 0)
        #expect(await client.status().state == .closed)
        #expect(await fake.connectedUrls.isEmpty)
    }

    @Test(.timeLimit(.minutes(1)))
    func getTokenNilParksIdleUntilPoked() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let tokens = TokenBox(nil)
        let client = makeClient(fake: fake, scheduler: scheduler, getToken: { tokens.next() })
        await client.connect()
        try await waitUntil("getToken called") {
            tokens.calls >= 1
        }
        // No credential: the loop parks idle without ever dialing.
        #expect(await fake.connectedUrls.isEmpty)
        #expect(await client.status().state == .idle)
        tokens.token = "tok"
        await client.connect()
        await fake.release(authOkFrame)
        let status = try await waitUntilState(client, .connected)
        #expect(status.user?.kind == .machine)
        // The poke woke the parked loop; it did not spawn a second one.
        #expect(await fake.connectedUrls.count == 1)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func statusStreamEmitsTransitions() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        let log = StatusLog()
        let stream = await client.statusStream
        let consumer = Task {
            for await status in stream {
                log.append(status)
            }
        }
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        try await waitForWaiter(on: fake, count: 1)
        await fake.enqueueClose(nil)
        _ = try await waitUntilState(client, .reconnecting)
        scheduler.advance(250)
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        await client.close()
        try await waitUntil("all seven statuses") {
            log.all.count >= 7
        }
        consumer.cancel()
        let statuses = log.all
        let expectedStates: [WsState] = [
            .idle, .connecting, .connected, .reconnecting, .connecting, .connected, .closed
        ]
        #expect(statuses.map(\.state) == expectedStates)
        #expect(statuses[2].user?.kind == .machine)
        #expect(statuses[3].user?.kind == .machine)
        #expect(statuses.last?.user == nil)
    }

    @Test(.timeLimit(.minutes(1)))
    func nonLifecycleFramesReachRegisteredHooks() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        try await waitForWaiter(on: fake, count: 1)
        let log = MessageLog()
        await client.addMessageHook("hook-1") { message in
            log.record(message)
        }
        await fake.release(#"{"type":"queryUpdate","queryId":"sub-1","result":null}"#)
        try await waitUntil("hook fired") {
            !log.all.isEmpty
        }
        // Lifecycle frames never reach hooks: a pong feeds liveness only.
        await fake.release(#"{"type":"pong"}"#)
        try await waitUntil("pong consumed") {
            await client.livenessLastPongMs() != nil
        }
        #expect(log.all.count == 1)
        guard case let .queryUpdate(queryId, _)? = log.all.first else {
            Issue.record("expected a queryUpdate to reach the hook")
            return
        }
        #expect(queryId == "sub-1")
        // Removal stops delivery.
        await client.removeMessageHook("hook-1")
        await fake.release(#"{"type":"queryUpdate","queryId":"sub-2","result":null}"#)
        try await waitUntil("second frame consumed") {
            await client.livenessLastPongMs() != nil
        }
        #expect(log.all.count == 1)
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func urlConstructionMirrorsRustSyncUrl() async throws {
        let cases: [(base: String, expected: String)] = [
            ("http://h:8000", "ws://h:8000/sync"),
            ("https://h", "wss://h/sync"),
            ("https://h/", "wss://h/sync"),
            ("https://h//", "wss://h/sync"),
            ("wss://h", "wss://h/sync"),
            ("ws://h:123", "ws://h:123/sync")
        ]
        for testCase in cases {
            let fake = FakeTransport()
            let client = RtDbClient(
                url: testCase.base, db: "app", getToken: { "tok" },
                transportFactory: { _ in fake }, scheduler: ManualScheduler()
            )
            await client.connect()
            try await waitUntil("dialed \(testCase.base)") {
                await fake.connectedUrls.count == 1
            }
            let urls = await fake.connectedUrls
            #expect(urls.map(\.absoluteString) == [testCase.expected])
            await client.close()
        }
    }

    // MARK: WS client subscriptions + mutations + scheduler ops (Task 14)

    /// Fold-in from Task 13's review: a late authOk racing close() must not
    /// leave status() reporting .closed with a non-nil user. The .ok arm's
    /// assignment needs the same terminal guard its sibling paths carry.
    /// Ordering: release(authOk) resumes the handshake child, whose completion
    /// must hop twice before runSession's .ok arm can run; close() enqueues
    /// directly, so terminal lands first and the assignment runs after it.
    @Test(.timeLimit(.minutes(1)))
    func lateAuthOkRacingCloseLeavesUserNil() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        try await waitUntil("handshake parked") {
            scheduler.sleepRequests.count >= 2
        }
        try await waitForWaiter(on: fake, count: 1)
        await fake.release(authOkFrame)
        await client.close()
        let status = await client.status()
        #expect(status.state == .closed)
        #expect(status.user == nil)
    }

    /// The first JSON string value of `"field":"…"` in a frame — enough to
    /// pull correlation ids (`queryId`/`mutId`/`scheduleId`/`workflowId`)
    /// out of the compact frames the encoder emits.
    private func stringValue(in frame: String, _ field: String) -> String? {
        guard let start = frame.range(of: "\"\(field)\":\"") else { return nil }
        let rest = frame[start.upperBound...]
        guard let end = rest.range(of: "\"") else { return nil }
        return String(rest[..<end.lowerBound])
    }

    /// Frames of one wire type, in send order.
    private func frames(ofType type: String, in sent: [String]) -> [String] {
        sent.filter { $0.contains(#""type":"\#(type)""#) }
    }

    private func connectedClient() async throws -> Task14Harness {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        return Task14Harness(fake: fake, client: client, scheduler: scheduler)
    }

    private func oneInsertTxn(_ value: Int64) throws -> Transaction {
        try MutationBuilder().insert("t", ["n": .int(value)]).build()
    }

    /// Mirror of the client's queued-mutation counter, so tests can
    /// deterministically order two queued calls.
    private func waitUntilQueuedMutations(_ client: RtDbClient, _ count: Int) async throws {
        try await waitUntil("\(count) queued mutations") {
            await client.queuedMutationCountForTesting() == count
        }
    }

    // MARK: Task 14: subscriptions

    @Test(.timeLimit(.minutes(1)))
    // swiftlint:disable:next function_body_length
    func subscribeDeliversUpdatesAndRefcounts() async throws {
        let harness = try await connectedClient()
        let query = try TableQuery("t").collect().build()
        let sub1 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        let sub2 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        try await waitUntil("one subscribe frame") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 1
        }
        let sent = await harness.fake.sent
        let subscribeFrame = try #require(frames(ofType: "subscribe", in: sent).first)
        let queryId = try #require(stringValue(in: subscribeFrame, "queryId"))
        #expect(queryId == "sub-1")
        #expect(sub1.current == .pending)
        #expect(sub2.current == .pending)
        // The first update fans out to every handle of the shape.
        await harness.fake.release(
            #"{"type":"queryUpdate","queryId":"\#(queryId)","result":[{"_id":"a","n":1}]}"#
        )
        let expected = [Task14Doc(id: "a", num: 1)]
        try await waitUntil("both handles valued") {
            sub1.current == .value(expected) && sub2.current == .value(expected)
        }
        // The stream yields actual snapshots only (pending is `current`'s seed).
        var iterator = sub1.stream.makeAsyncIterator()
        let firstSnapshot = await iterator.next()
        if case .value = firstSnapshot {
            // expected — the stream's first element is the first real snapshot
        } else {
            Issue.record("expected the stream's first element to be .value")
        }
        // A late joiner attaches to the live shape: current value immediately,
        // and still just ONE server subscription.
        let sub3 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        #expect(sub3.current == .value(expected))
        #expect(await frames(ofType: "subscribe", in: harness.fake.sent).count == 1)
        // A second update changes all three.
        await harness.fake.release(
            #"{"type":"queryUpdate","queryId":"\#(queryId)","result":[{"_id":"a","n":2}]}"#
        )
        let expected2 = [Task14Doc(id: "a", num: 2)]
        try await waitUntil("all handles updated") {
            sub1.current == .value(expected2) && sub3.current == .value(expected2)
        }
        // Refcount release: only the LAST cancel sends unsubscribe.
        await sub1.cancel()
        #expect(await frames(ofType: "unsubscribe", in: harness.fake.sent).isEmpty)
        await sub2.cancel()
        #expect(await frames(ofType: "unsubscribe", in: harness.fake.sent).isEmpty)
        await sub3.cancel()
        try await waitUntil("unsubscribe sent") {
            await frames(ofType: "unsubscribe", in: harness.fake.sent).count == 1
        }
        let unsubscribe = try #require(await frames(ofType: "unsubscribe", in: harness.fake.sent).first)
        #expect(stringValue(in: unsubscribe, "queryId") == queryId)
        // The shape is gone: a fresh subscribe mints a new queryId and re-sends.
        let sub4 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        try await waitUntil("re-subscribe") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 2
        }
        let resub = try #require(await frames(ofType: "subscribe", in: harness.fake.sent).last)
        let newQueryId = try #require(stringValue(in: resub, "queryId"))
        #expect(newQueryId == "sub-2")
        #expect(newQueryId != queryId)
        await sub4.cancel()
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func subscribeErrFailsSnapshotAndClearsShape() async throws {
        let harness = try await connectedClient()
        let sub = try await harness.client.subscribe(
            TableQuery("t").collect().build(), as: [Task14Doc].self
        )
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(
            #"{"type":"subscribeErr","queryId":"sub-1","error":{"code":"FORBIDDEN","message":"denied"}}"#
        )
        let expectedError = RtDbError(code: .forbidden, message: "denied")
        try await waitUntil("failed snapshot") {
            sub.current == .failed(expectedError)
        }
        // The errored shape was removed: a new subscribe sends a fresh frame
        // with a NEW queryId (rust drops both maps on subscribeErr).
        let sub2 = try await harness.client.subscribe(
            TableQuery("t").collect().build(), as: [Task14Doc].self
        )
        try await waitUntil("fresh subscribe") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 2
        }
        let fresh = try #require(await frames(ofType: "subscribe", in: harness.fake.sent).last)
        #expect(stringValue(in: fresh, "queryId") == "sub-2")
        await sub2.cancel()
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func reconnectReplaysSubscriptionsWithSameQueryId() async throws {
        let harness = try await connectedClient()
        let sub = try await harness.client.subscribe(
            TableQuery("t").collect().build(), as: [Task14Doc].self
        )
        try await waitUntil("subscribe frame") {
            await frames(ofType: "subscribe", in: harness.fake.sent).isEmpty == false
        }
        // Drop the session: the shape survives for replay.
        try await waitForWaiter(on: harness.fake, count: 1)
        await harness.fake.enqueueClose(nil)
        _ = try await waitUntilState(harness.client, .reconnecting)
        // Burn the attempt-0 backoff (250 ms at jitter 0.0) to dial again.
        harness.scheduler.advance(250)
        try await waitUntil("second dial") {
            await harness.fake.connectedUrls.count == 2
        }
        try await waitForWaiter(on: harness.fake, count: 1)
        await harness.fake.release(authOkFrame)
        _ = try await waitUntilState(harness.client, .connected)
        try await waitUntil("replayed subscribe") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 2
        }
        let replayed = try #require(await frames(ofType: "subscribe", in: harness.fake.sent).last)
        #expect(stringValue(in: replayed, "queryId") == "sub-1")
        // The replayed subscription is live again: updates flow.
        await harness.fake.release(
            #"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"b","n":9}]}"#
        )
        try await waitUntil("snapshot after replay") {
            sub.current == .value([Task14Doc(id: "b", num: 9)])
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func doubleCancelOfOneHandleKeepsShapeAlive() async throws {
        let harness = try await connectedClient()
        let query = try TableQuery("t").collect().build()
        let sub1 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        let sub2 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        try await waitUntil("one subscribe frame") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 1
        }
        // A repeated cancel() of ONE handle must release the shared shape
        // exactly once: `Subscription` is a copyable struct and cancel() is
        // the deinit-replacement, so double-release is realistic. Rust is
        // immune (Drop fires once, Subscription is not Clone).
        await sub1.cancel()
        await sub1.cancel()
        #expect(await frames(ofType: "unsubscribe", in: harness.fake.sent).isEmpty)
        // The surviving handle still receives updates.
        await harness.fake.release(
            #"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a","n":1}]}"#
        )
        let expected = [Task14Doc(id: "a", num: 1)]
        try await waitUntil("survivor valued") {
            sub2.current == .value(expected)
        }
        // Only the last genuine release unsubscribes.
        await sub2.cancel()
        try await waitUntil("unsubscribe sent") {
            await frames(ofType: "unsubscribe", in: harness.fake.sent).count == 1
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func copiedHandleCancelReleasesOnce() async throws {
        let harness = try await connectedClient()
        let query = try TableQuery("t").collect().build()
        let sub1 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        let sub2 = try await harness.client.subscribe(query, as: [Task14Doc].self)
        try await waitUntil("one subscribe frame") {
            await frames(ofType: "subscribe", in: harness.fake.sent).count == 1
        }
        // A copy shares the sinkId and the cancel closure: cancelling through
        // the copy and then the original releases the handle exactly once.
        let copy = sub1
        await copy.cancel()
        await sub1.cancel()
        #expect(await frames(ofType: "unsubscribe", in: harness.fake.sent).isEmpty)
        await harness.fake.release(
            #"{"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a","n":2}]}"#
        )
        let expected = [Task14Doc(id: "a", num: 2)]
        try await waitUntil("survivor valued") {
            sub2.current == .value(expected)
        }
        await sub2.cancel()
        try await waitUntil("unsubscribe sent") {
            await frames(ofType: "unsubscribe", in: harness.fake.sent).count == 1
        }
        await harness.client.close()
    }

    // MARK: Task 14: mutations

    @Test(.timeLimit(.minutes(1)))
    func mutateOkResolvesByMutId() async throws {
        let harness = try await connectedClient()
        let txn = try oneInsertTxn(1)
        async let results: [StepResult] = harness.client.mutate(txn, idempotencyKey: "op-1")
        try await waitUntil("mutate frame") {
            await frames(ofType: "mutate", in: harness.fake.sent).isEmpty == false
        }
        let frame = try #require(await frames(ofType: "mutate", in: harness.fake.sent).first)
        let mutId = try #require(stringValue(in: frame, "mutId"))
        #expect(mutId == "mut-1")
        #expect(frame.contains(#""idempotencyKey":"op-1""#))
        await harness.fake.release(
            #"{"type":"mutateOk","mutId":"\#(mutId)","results":[{"id":"doc-1","inserted":true}]}"#
        )
        let resolved = try await results
        #expect(resolved == [.upsert(id: "doc-1", inserted: true)])
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func mutateErrRejectsWithRtDbError() async throws {
        let harness = try await connectedClient()
        let txn = try oneInsertTxn(1)
        async let results: [StepResult] = harness.client.mutate(txn)
        try await waitUntil("mutate frame") {
            await frames(ofType: "mutate", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(
            #"{"type":"mutateErr","mutId":"mut-1","error":{"code":"SCHEMA_VIOLATION","message":"bad doc"}}"#
        )
        do {
            _ = try await results
            Issue.record("mutateErr should reject the mutate call")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .schemaViolation, message: "bad doc"))
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func queuedMutationsFlushInOrderAfterAuthOk() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        // Not connected: both mutations queue (FIFO), each parked on its reply.
        let txnA = try oneInsertTxn(1)
        let txnB = try oneInsertTxn(2)
        let first = Task { try await client.mutate(txnA) }
        try await waitUntilQueuedMutations(client, 1)
        let second = Task { try await client.mutate(txnB) }
        try await waitUntilQueuedMutations(client, 2)
        await client.connect()
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        try await waitUntil("flushed both frames") {
            await frames(ofType: "mutate", in: fake.sent).count == 2
        }
        let mutIds = await frames(ofType: "mutate", in: fake.sent)
            .compactMap { stringValue(in: $0, "mutId") }
        #expect(mutIds == ["mut-1", "mut-2"])
        // Correlation: replies resolve the right callers.
        await fake.release(
            #"{"type":"mutateOk","mutId":"mut-1","results":[{"id":"a","inserted":true}]}"#
        )
        await fake.release(
            #"{"type":"mutateOk","mutId":"mut-2","results":[{"id":"b","inserted":true}]}"#
        )
        #expect(try await first.value == [.upsert(id: "a", inserted: true)])
        #expect(try await second.value == [.upsert(id: "b", inserted: true)])
        await client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func sentMutationRejectedOnDisconnectQueuedOneSurvives() async throws {
        let harness = try await connectedClient()
        // Sent-but-unacked: rejected when the session dies (at-most-once).
        let txn = try oneInsertTxn(1)
        let sent = Task { try await harness.client.mutate(txn) }
        try await waitUntil("mutate frame") {
            await frames(ofType: "mutate", in: harness.fake.sent).isEmpty == false
        }
        try await waitForWaiter(on: harness.fake, count: 1)
        await harness.fake.enqueueClose(nil)
        do {
            _ = try await sent.value
            Issue.record("a sent-but-unacked mutation should reject on disconnect")
        } catch let error as RtDbError {
            #expect(
                error == RtDbError(
                    code: .internal, message: "connection closed before acknowledgment"
                )
            )
        }
        _ = try await waitUntilState(harness.client, .reconnecting)
        // Queued while disconnected: survives for the next session.
        let txn2 = try oneInsertTxn(2)
        let queued = Task { try await harness.client.mutate(txn2) }
        try await waitUntilQueuedMutations(harness.client, 1)
        harness.scheduler.advance(250)
        try await waitUntil("second dial") {
            await harness.fake.connectedUrls.count == 2
        }
        try await waitForWaiter(on: harness.fake, count: 1)
        await harness.fake.release(authOkFrame)
        _ = try await waitUntilState(harness.client, .connected)
        try await waitUntil("queued frame flushed") {
            await frames(ofType: "mutate", in: harness.fake.sent).count == 2
        }
        let flushed = try #require(await frames(ofType: "mutate", in: harness.fake.sent).last)
        #expect(stringValue(in: flushed, "mutId") == "mut-2")
        await harness.fake.release(
            #"{"type":"mutateOk","mutId":"mut-2","results":[{"id":"c","inserted":true}]}"#
        )
        #expect(try await queued.value == [.upsert(id: "c", inserted: true)])
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func failedSendRequeuesMutationForNextSession() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let transport = FailSendAfterTransport(underlying: fake, after: 1)
        let client = RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: { "tok" },
            transportFactory: { _ in transport }, scheduler: scheduler, random: { 0.0 }
        )
        await client.connect()
        // Send #1 is the auth frame — it passes the decorator's gate.
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        // The mutate send fails (scripted): the frame never hits the wire, so
        // the call re-queues for the next session instead of hanging in the
        // pending table (rust re-queues on a failed mid-session send).
        let txn = try oneInsertTxn(1)
        let call = Task { try await client.mutate(txn) }
        try await waitUntilQueuedMutations(client, 1)
        #expect(await frames(ofType: "mutate", in: fake.sent).isEmpty)
        // Drop the session: the queued (never-sent) call survives.
        try await waitForWaiter(on: fake, count: 1)
        await fake.enqueueClose(nil)
        _ = try await waitUntilState(client, .reconnecting)
        scheduler.advance(250)
        try await waitUntil("second dial") {
            await fake.connectedUrls.count == 2
        }
        try await waitForWaiter(on: fake, count: 1)
        await fake.release(authOkFrame)
        _ = try await waitUntilState(client, .connected)
        // The next session's flush delivers it — same mutId, exactly once.
        try await waitUntil("flushed") {
            await frames(ofType: "mutate", in: fake.sent).count == 1
        }
        let frame = try #require(await frames(ofType: "mutate", in: fake.sent).first)
        #expect(stringValue(in: frame, "mutId") == "mut-1")
        await fake.release(
            #"{"type":"mutateOk","mutId":"mut-1","results":[{"id":"a","inserted":true}]}"#
        )
        #expect(try await call.value == [.upsert(id: "a", inserted: true)])
        await client.close()
    }

    // MARK: Task 14: scheduler + workflow ops

    @Test(.timeLimit(.minutes(1)))
    func scheduleOpsRoundTrip() async throws {
        let harness = try await connectedClient()
        let txn = try oneInsertTxn(1)
        async let scheduleId: String = harness.client.schedule(txn, when: .afterMs(ms: 1000))
        try await waitUntil("schedule frame") {
            await frames(ofType: "schedule", in: harness.fake.sent).isEmpty == false
        }
        let frame = try #require(await frames(ofType: "schedule", in: harness.fake.sent).first)
        #expect(stringValue(in: frame, "scheduleId") == "sch-1")
        #expect(frame.contains(#""type":"afterMs""#))
        await harness.fake.release(#"{"type":"scheduleOk","scheduleId":"sch-1","id":"job-9"}"#)
        #expect(try await scheduleId == "job-9")
        // cancel/pause/resume: scheduleAck ok:true resolves without throwing.
        async let cancelOp: Void = harness.client.cancelSchedule("job-9")
        try await waitUntil("cancel frame") {
            await frames(ofType: "cancelSchedule", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(#"{"type":"scheduleAck","scheduleId":"sch-2","ok":true}"#)
        try await cancelOp
        async let pauseOp: Void = harness.client.pauseSchedule("job-9")
        try await waitUntil("pause frame") {
            await frames(ofType: "pauseSchedule", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(#"{"type":"scheduleAck","scheduleId":"sch-3","ok":true}"#)
        try await pauseOp
        async let resumeOp: Void = harness.client.resumeSchedule("job-9")
        try await waitUntil("resume frame") {
            await frames(ofType: "resumeSchedule", in: harness.fake.sent).isEmpty == false
        }
        // Bare ok:false (unknown id) is a no-op ack, not an error.
        await harness.fake.release(#"{"type":"scheduleAck","scheduleId":"sch-4","ok":false}"#)
        try await resumeOp
        async let schedules: [ScheduleInfo] = harness.client.listSchedules()
        try await waitUntil("list frame") {
            await frames(ofType: "listSchedules", in: harness.fake.sent).isEmpty == false
        }
        let listedJob = #"{"id":"job-9","kind":"oneshot","dueAt":123,"status":"pending","#
            + #""createdAt":100,"firedCount":0}"#
        await harness.fake.release(
            #"{"type":"listSchedulesOk","scheduleId":"sch-5","schedules":[\#(listedJob)]}"#
        )
        let listed = try await schedules
        #expect(listed.map(\.id) == ["job-9"])
        #expect(listed.first?.kind == .oneshot)
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func workflowOpsRoundTrip() async throws {
        let harness = try await connectedClient()
        let spec = try WorkflowSpec(name: "run", steps: [WorkflowStepSpec(txn: oneInsertTxn(1))])
        async let runId: String = harness.client.startWorkflow(spec)
        try await waitUntil("startWorkflow frame") {
            await frames(ofType: "startWorkflow", in: harness.fake.sent).isEmpty == false
        }
        let frame = try #require(await frames(ofType: "startWorkflow", in: harness.fake.sent).first)
        #expect(stringValue(in: frame, "workflowId") == "wf-1")
        let infoJson = #"{"id":"run-9","name":"run","status":"pending","currentStep":0,"#
            + #""stepCount":1,"attempts":0,"createdAt":100,"updatedAt":100}"#
        await harness.fake.release(
            #"{"type":"startWorkflowOk","workflowId":"wf-1","info":\#(infoJson)}"#
        )
        #expect(try await runId == "run-9")
        async let cancelOp: Void = harness.client.cancelWorkflow("run-9")
        try await waitUntil("cancelWorkflow frame") {
            await frames(ofType: "cancelWorkflow", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(#"{"type":"workflowAck","workflowId":"wf-2","ok":true}"#)
        try await cancelOp
        async let runs: [WorkflowInfo] = harness.client.listWorkflows()
        try await waitUntil("listWorkflows frame") {
            await frames(ofType: "listWorkflows", in: harness.fake.sent).isEmpty == false
        }
        let doneJson = #"{"id":"run-9","name":"run","status":"success","currentStep":1,"#
            + #""stepCount":1,"attempts":1,"createdAt":100,"updatedAt":200,"#
            + #""startedAt":110,"finishedAt":190}"#
        await harness.fake.release(
            #"{"type":"listWorkflowsOk","workflowId":"wf-3","workflows":[\#(doneJson)]}"#
        )
        let listed = try await runs
        #expect(listed.map(\.id) == ["run-9"])
        #expect(listed.first?.status == .success)
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func scheduleErrRejectsSchedule() async throws {
        let harness = try await connectedClient()
        let txn = try oneInsertTxn(1)
        async let scheduleId: String = harness.client.schedule(txn, when: .cron(expr: "bad"))
        try await waitUntil("schedule frame") {
            await frames(ofType: "schedule", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(
            #"{"type":"scheduleErr","scheduleId":"sch-1","error":{"code":"BAD_REQUEST","message":"invalid cron"}}"#
        )
        do {
            _ = try await scheduleId
            Issue.record("scheduleErr should reject the schedule call")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .badRequest, message: "invalid cron"))
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func startWorkflowErrRejectsStart() async throws {
        let harness = try await connectedClient()
        let spec = WorkflowSpec(name: "run", steps: [])
        async let runId: String = harness.client.startWorkflow(spec)
        try await waitUntil("startWorkflow frame") {
            await frames(ofType: "startWorkflow", in: harness.fake.sent).isEmpty == false
        }
        await harness.fake.release(
            #"{"type":"startWorkflowErr","workflowId":"wf-1","error":{"code":"BAD_REQUEST","message":"empty steps"}}"#
        )
        do {
            _ = try await runId
            Issue.record("startWorkflowErr should reject the start call")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .badRequest, message: "empty steps"))
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func queuedMutationsRejectOnClose() async throws {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        let txn = try oneInsertTxn(1)
        let queued = Task { try await client.mutate(txn) }
        try await waitUntilQueuedMutations(client, 1)
        await client.close()
        do {
            _ = try await queued.value
            Issue.record("close() should reject queued mutations")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .internal, message: "client is closed"))
        }
    }

    @Test(.timeLimit(.minutes(1)))
    func mutateAndSubscribeRejectOnClosedClient() async throws {
        let harness = try await connectedClient()
        await harness.client.close()
        let txn = try oneInsertTxn(1)
        do {
            _ = try await harness.client.mutate(txn)
            Issue.record("mutate on a closed client should throw")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .internal, message: "client is closed"))
        }
        let query = try TableQuery("t").collect().build()
        do {
            _ = try await harness.client.subscribe(query, as: [Task14Doc].self)
            Issue.record("subscribe on a closed client should throw")
        } catch let error as RtDbError {
            #expect(error == RtDbError(code: .internal, message: "client is closed"))
        }
    }
}
