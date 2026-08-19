import Foundation
import ParRtDbClient

// MARK: - Minimal FakeTransport (UI-test copy)

/// Scriptable in-process `WebSocketTransport` — a deliberately trimmed copy of
/// the ParRtDbClientTests fake (test targets cannot share files). Frames are
/// delivered FIFO; `enqueue`/`enqueueClose`/`release` hand straight to a
/// suspended receiver when one is waiting and buffer otherwise. `receive()` is
/// NOT cancellation-aware: no UI test cancels a suspended receiver — teardown
/// always drives the receive loop out with `enqueueClose` BEFORE `close()`.
actor FakeTransport: WebSocketTransport {
    private(set) var sent: [String] = []
    private var incoming: [Result<String, Error>] = []
    private var waiters: [CheckedContinuation<String, Error>] = []
    private var gateArmed = false
    private var gatedSends: [CheckedContinuation<Void, Never>] = []

    /// Buffer `frame` for the next `receive()` — or hand it to a suspended receiver.
    func enqueue(_ frame: String) {
        deliver(.success(frame))
    }

    /// Make the next `receive()` throw `TransportCloseError(code:)` — or fail a
    /// suspended receiver with it.
    func enqueueClose(_ code: UInt16?) {
        deliver(.failure(TransportCloseError(code: code)))
    }

    /// `enqueue` under its intent-revealing name: a frame aimed at a receive
    /// loop that is (or is about to be) suspended.
    func release(_ frame: String) {
        deliver(.success(frame))
    }

    func connect(to _: URL) async throws {}

    /// Park every subsequent `send` until `releaseSends()` — holds a client
    /// `subscribe` frame in flight so a test can interleave across
    /// `client.subscribe`'s suspension point deterministically.
    func armSendGate() {
        gateArmed = true
    }

    /// Resume every send parked by the armed gate (and disarm it).
    func releaseSends() {
        gateArmed = false
        let parked = gatedSends
        gatedSends.removeAll()
        parked.forEach { $0.resume() }
    }

    /// Test introspection: how many `send` calls are currently parked on the gate.
    var parkedSendCount: Int {
        gatedSends.count
    }

    func send(_ text: String) async throws {
        if gateArmed {
            await withCheckedContinuation { continuation in
                gatedSends.append(continuation)
            }
        }
        sent.append(text)
    }

    func receive() async throws -> String {
        if !incoming.isEmpty {
            return try incoming.removeFirst().get()
        }
        return try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
        }
    }

    func close(code _: UInt16) async {}

    /// Waiter first, buffer otherwise — the queue and the waiter list are never
    /// both nonempty, which `receive()` relies on.
    private func deliver(_ result: Result<String, Error>) {
        if waiters.isEmpty {
            incoming.append(result)
        } else {
            waiters.removeFirst().resume(with: result)
        }
    }
}

// MARK: - Fixtures

/// The authOk frame every UI test uses to complete the handshake.
let authOkFrame = #"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#

/// Failure thrown when a bounded poll's deadline lapses — fails the test
/// outright instead of stalling on an await that can never complete. Carries
/// the condition's label so the failure names what stalled.
struct WaiterTimeout: Error, CustomStringConvertible {
    private let what: String

    init(_ what: String = "the condition") {
        self.what = what
    }

    var description: String {
        "timed out after 5 s waiting for: \(what)"
    }
}

/// Bounded poll (5 s ceiling, 5 ms real ticks) until `condition` holds — a
/// timeout throws WaiterTimeout (naming the stalled condition) rather than
/// stalling the test forever. @MainActor: UI tests assert on LiveQuery state,
/// so the condition runs on the main actor alongside the pump it observes.
@MainActor
func waitUntil(_ what: String, _ condition: () async -> Bool) async throws {
    let deadline = Date().addingTimeInterval(5)
    while await !condition() {
        if Date() > deadline {
            throw WaiterTimeout(what)
        }
        try await Task.sleep(nanoseconds: 5_000_000)
    }
}
