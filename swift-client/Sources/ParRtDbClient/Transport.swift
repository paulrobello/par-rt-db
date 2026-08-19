import Foundation

// MARK: - Transport seam

/// Transport seam — the WS client's entire I/O. Production:
/// `URLSessionWebSocketTransport`; tests script an in-process fake (see
/// WsClientTests). Every frame is text (the protocol is JSON over text
/// WebSocket messages), and each method expects at most one connection per
/// transport: `connect` first, then sends/`receive` until a throw or `close`.
public protocol WebSocketTransport: Sendable {
    /// Establish the connection; throws on invalid or unreachable endpoints.
    func connect(to url: URL) async throws
    /// Send one text frame; throws the underlying transport error.
    func send(_ text: String) async throws
    /// Await the next text frame. Any end of the connection throws
    /// `TransportCloseError` — with the peer's close code when it sent one
    /// and the platform surfaces it.
    func receive() async throws -> String
    /// Close the connection; best-effort and idempotent.
    func close(code: UInt16) async
}

/// The connection ended. `code` is the peer's WebSocket close code when it
/// sent one and the platform surfaces it (e.g. the server's terminal 4401);
/// nil otherwise. The WS client ALSO reads `CloseReason` from server
/// `authErr` frames — frame-carried reasons are authoritative, this code is
/// the socket-level signal.
public struct TransportCloseError: Error, Sendable {
    public let code: UInt16?

    public init(code: UInt16?) {
        self.code = code
    }
}

// MARK: - Production transport (URLSession)

/// Production transport over `URLSessionWebSocketTask`. One connection per
/// `connect(to:)` — the WS client's transport factory builds a fresh
/// transport per connection attempt, and a repeat `connect` cancels any task
/// it would supersede.
public struct URLSessionWebSocketTransport: WebSocketTransport {
    private let session: URLSession
    private let state = State()

    public init(session: URLSession = .shared) {
        self.session = session
    }

    public func connect(to url: URL) async throws {
        guard let scheme = url.scheme?.lowercased(), scheme == "ws" || scheme == "wss" else {
            throw RtDbError(
                code: .badRequest,
                message: "WebSocket URL must be ws:// or wss://, got \(url.scheme ?? "no scheme")"
            )
        }
        let task = session.webSocketTask(with: url)
        state.replaceTask(task)?.cancel(with: .normalClosure, reason: nil)
        task.resume()
        // No delegate is attached to the (possibly injected) session, so
        // there is no handshake callback to await — readiness is the first
        // successful ping round trip, which also fails fast on refused or
        // invalid endpoints.
        do {
            try await Self.ping(task)
        } catch {
            state.replaceTask(nil)?.cancel()
            throw error
        }
    }

    public func send(_ text: String) async throws {
        guard let task = state.task else {
            throw TransportCloseError(code: nil)
        }
        try await task.send(.string(text))
    }

    public func receive() async throws -> String {
        guard let task = state.task else {
            throw TransportCloseError(code: nil)
        }
        let message: URLSessionWebSocketTask.Message
        do {
            message = try await task.receive()
        } catch {
            // The connection ended — peer close, dropped socket, or local
            // cancel. These OS floors expose no typed close event on the
            // receive path; `closeCode` carries the peer's code when the
            // task recorded one (see peerCloseCode).
            throw TransportCloseError(code: Self.peerCloseCode(of: task))
        }
        guard case let .string(text) = message else {
            throw RtDbError(code: .internal, message: "unexpected binary WebSocket frame")
        }
        return text
    }

    public func close(code: UInt16) async {
        guard let task = state.replaceTask(nil) else { return }
        if let closeCode = URLSessionWebSocketTask.CloseCode(rawValue: Int(code)) {
            task.cancel(with: closeCode, reason: nil)
        } else {
            task.cancel()
        }
    }

    /// `sendPing` has no async overload; the pong (or the error) resumes the
    /// continuation. The continuation is Sendable and consumed exactly once
    /// by the completion handler.
    private static func ping(_ task: URLSessionWebSocketTask) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            task.sendPing { error in
                if let error {
                    continuation.resume(throwing: error)
                } else {
                    continuation.resume()
                }
            }
        }
    }

    /// The peer's close code from the finished task, or nil when none was
    /// recorded. Empirically pinned on macOS 14 (WsClientTests loopback):
    /// after a peer close, `closeCode` reflects the received code, so the
    /// server's terminal 4401 reaches the WS client; with no close frame
    /// (dropped socket) it stays `.invalid` (raw 0) → nil. Registry
    /// (1000–1014) and private-use (3000–4999) ranges are surfaced.
    private static func peerCloseCode(of task: URLSessionWebSocketTask) -> UInt16? {
        let raw = task.closeCode.rawValue
        guard (1000 ... 1014).contains(raw) || (3000 ... 4999).contains(raw) else { return nil }
        return UInt16(raw)
    }

    /// The current connection's task. @unchecked Sendable: `current` is only
    /// ever touched while holding `lock`, and URLSessionWebSocketTask is
    /// thread-safe to call from any queue.
    private final class State: @unchecked Sendable {
        private let lock = NSLock()
        private var current: URLSessionWebSocketTask?

        var task: URLSessionWebSocketTask? {
            lock.withLock { current }
        }

        /// Store `new`, returning the superseded task for cancellation.
        @discardableResult
        func replaceTask(_ new: URLSessionWebSocketTask?) -> URLSessionWebSocketTask? {
            lock.withLock {
                let old = current
                current = new
                return old
            }
        }
    }
}

// MARK: - Scheduler

/// Injectable clock + sleep, so backoff/heartbeat logic is testable with a
/// manual scheduler (the python client's now/sleep injection).
public protocol WScheduler: Sendable {
    /// Milliseconds since the Unix epoch.
    func now() -> UInt64
    /// Suspend for `ms` milliseconds; tolerant of task cancellation (never
    /// throws).
    func sleep(_ ms: UInt64) async
}

/// Wall-clock scheduler over `Date` and `Task.sleep`.
public struct SystemScheduler: WScheduler {
    public init() {}

    public func now() -> UInt64 {
        UInt64(Date.now.timeIntervalSince1970 * 1000)
    }

    public func sleep(_ ms: UInt64) async {
        try? await Task.sleep(for: .milliseconds(Int(clamping: ms)))
    }
}
