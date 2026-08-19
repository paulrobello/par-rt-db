import Foundation

// MARK: - Module constants

/// Milliseconds the client waits after dialing for `authOk` before tearing the
/// connection down and retrying (rust ws.rs `AUTH_DEADLINE`). The server
/// enforces its own auth timeout; this is the local backstop.
let authDeadlineMs: UInt64 = 15000

/// The server's WS close code for a rejected credential (`authErr` / bad or
/// revoked token) — rust ws.rs `CLOSE_AUTH_FAILED`. Terminal for this client:
/// no reconnect, ever; `connect()` after it is a no-op.
let authFailedCloseCode: UInt16 = 4401

/// Ceiling on the dial itself (rust ws.rs: `timeout(15s, connect_async)`). The
/// transport's readiness ping has no internal timeout, so this race owns
/// bounding it.
let connectTimeoutMs: UInt64 = 15000

// MARK: - Public types

/// Tunables for `RtDbClient`. Defaults mirror the rust client's `Config`.
public struct RtDbClientConfig: Sendable {
    /// Exponential-backoff base for reconnect delays.
    public var backoffBaseMs: UInt64 = 500
    /// Ceiling for the exponential reconnect delay.
    public var backoffMaxMs: UInt64 = 15000
    /// How often to send a `{"type":"ping"}` keepalive; the connection is
    /// presumed dead after `2 * heartbeatMs` without a pong. `0` disables the
    /// heartbeat (ts-client parity).
    public var heartbeatMs: UInt64 = 20000

    public init() {}
}

/// Coarse connection state surfaced through `ClientStatus`.
public enum WsState: Equatable, Sendable {
    /// Not dialing; either never connected or no credential was available. An
    /// explicit `connect()` starts dialing.
    case idle
    /// A socket is being opened and authenticated.
    case connecting
    /// Authenticated and usable; subscriptions and mutations flow.
    case connected
    /// Disconnected mid-session; a reconnect is scheduled.
    case reconnecting
    /// Terminal: `close()` was called or the server sent `4401`. Nothing
    /// further happens, ever.
    case closed
}

/// Snapshot of the client's connection + auth state. `user` is set once
/// `authOk` arrives and cleared again on idle/closed (rust `set_state`).
public struct ClientStatus: Equatable, Sendable {
    public var state: WsState
    public var user: AuthedUser?

    public init(state: WsState, user: AuthedUser? = nil) {
        self.state = state
        self.user = user
    }
}

// MARK: - Query snapshots (Task 14)

/// A subscription's state at one instant: nothing yet, the latest result, or
/// the `subscribeErr` rejection. The typed mirror of rust's `Snapshot` — there
/// the watch channel carries raw `serde_json::Value`s and typing happens at
/// the subscriber; here the registry's typed sinks decode once per fan-out so
/// `current` and `stream` are already `T`.
public enum QuerySnapshot<T: Codable & Sendable>: Sendable {
    /// Awaiting the first `queryUpdate` (or a replay after reconnect).
    case pending
    /// The latest full result — sent only on change.
    case value(T)
    /// The subscription was rejected (`subscribeErr`).
    case failed(RtDbError)
}

extension QuerySnapshot: Equatable where T: Equatable {
    public static func == (lhs: QuerySnapshot<T>, rhs: QuerySnapshot<T>) -> Bool {
        switch (lhs, rhs) {
        case (.pending, .pending):
            true
        case let (.value(left), .value(right)):
            left == right
        case let (.failed(left), .failed(right)):
            left == right
        default:
            false
        }
    }
}

/// Latest-value cell behind `Subscription.current` (a synchronous read needs a
/// non-actor home; the registry writes, the handle reads).
/// @unchecked Sendable: `snapshot` is only ever touched while holding `lock`,
/// and `T` is Sendable by constraint.
final class LatestSnapshotBox<T: Codable & Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var snapshot: QuerySnapshot<T>

    init(_ snapshot: QuerySnapshot<T>) {
        self.snapshot = snapshot
    }

    var value: QuerySnapshot<T> {
        lock.withLock { snapshot }
    }

    func store(_ snapshot: QuerySnapshot<T>) {
        lock.withLock { self.snapshot = snapshot }
    }
}

/// A live-query handle. `current` is the shape's latest snapshot (a late
/// joiner sees the current value immediately — watch-channel parity); `stream`
/// yields each subsequent snapshot and never yields `.pending`; `cancel()`
/// drops this handle's ref — the last one for the shape sends
/// `{type:"unsubscribe"}`. The stream is single-consumer.
public struct Subscription<T: Codable & Sendable>: Sendable {
    private let box: LatestSnapshotBox<T>
    private let storedStream: AsyncStream<QuerySnapshot<T>>
    private let onCancel: @Sendable () async -> Void

    init(
        box: LatestSnapshotBox<T>,
        stream: AsyncStream<QuerySnapshot<T>>,
        onCancel: @escaping @Sendable () async -> Void
    ) {
        self.box = box
        storedStream = stream
        self.onCancel = onCancel
    }

    /// The latest snapshot without waiting.
    public var current: QuerySnapshot<T> {
        box.value
    }

    /// Every snapshot from here on; finishes when the subscription errors, its
    /// shape is released, or the client becomes terminal.
    public var stream: AsyncStream<QuerySnapshot<T>> {
        storedStream
    }

    /// Drop this handle's reference to the shape (rust: dropping `Subscription`).
    public func cancel() async {
        await onCancel()
    }
}

/// The registry's currency: a snapshot before typing.
enum RawSnapshot: Sendable {
    case pending
    case value(JSONValue)
    case failed(RtDbError)
}

/// One typed subscriber's delivery target. The registry holds one per live
/// `Subscription` handle and is agnostic of `T` — the sink decodes via
/// `parseResult` with the shape's terminal.
protocol SubscriptionSink: Sendable {
    var id: UUID { get }
    func deliver(_ snapshot: RawSnapshot)
    func finish()
}

struct TypedSubscriptionSink<T: Codable & Sendable>: SubscriptionSink {
    let id = UUID()
    let terminal: QueryTerminal
    let box: LatestSnapshotBox<T>
    let continuation: AsyncStream<QuerySnapshot<T>>.Continuation

    func deliver(_ raw: RawSnapshot) {
        let typed: QuerySnapshot<T>
        switch raw {
        case .pending:
            typed = .pending
        case let .failed(error):
            typed = .failed(error)
        case let .value(value):
            do {
                typed = try .value(parseResult(value, terminal: terminal))
            } catch let error as RtDbError {
                typed = .failed(error)
            } catch {
                typed = .failed(RtDbError(code: .internal, message: "invalid query result: \(error)"))
            }
        }
        box.store(typed)
        continuation.yield(typed)
    }

    func finish() {
        continuation.finish()
    }
}

// MARK: - Deadline race

/// Task-group child outcome for `withDeadline`: which side of the race won.
private enum DeadlineRace<T: Sendable>: Sendable {
    case completed(T)
    case deadline
}

/// Race `operation` against a `ms` deadline; the operation's value if it won,
/// nil if the deadline fired first. The loser is cancelled, and — when the
/// deadline won — `onDeadline` runs BEFORE the group drains: closing the
/// transport there drives cancellation-blind platform callbacks (URLSession's
/// pending `connect` ping / `receive`) to completion, so the group can always
/// drain and structured concurrency never leaks a hung child.
private func withDeadline<T: Sendable>(
    _ ms: UInt64,
    scheduler: WScheduler,
    onDeadline: @escaping @Sendable () async -> Void,
    operation: @escaping @Sendable () async throws -> T
) async throws -> T? {
    try await withThrowingTaskGroup(of: DeadlineRace<T>.self) { group in
        group.addTask {
            await scheduler.sleep(ms)
            return .deadline
        }
        group.addTask {
            try await .completed(operation())
        }
        let winner = try await group.next() ?? .deadline
        group.cancelAll()
        if case .deadline = winner {
            await onDeadline()
        }
        while true {
            do {
                if try await group.next() == nil {
                    break
                }
            } catch {
                // The loser's error is expected — swallow and keep draining.
            }
        }
        switch winner {
        case let .completed(value):
            return value
        case .deadline:
            return nil
        }
    }
}

// MARK: - RtDbClient

/// Reactive WS client for one par-rt-db database — the Swift mirror of
/// rust-client/src/ws.rs. One background run loop owns the socket: dial,
/// authenticate (auth is the first frame, `authOk` within `authDeadlineMs`),
/// then run receive + heartbeat until the connection ends; reconnect with
/// jittered exponential backoff unless the server sent the terminal `4401`.
/// Lifecycle mirrors the rust driver task; subscriptions and mutations land in
/// Task 14 on top of the message-hook table.
public actor RtDbClient {
    private let syncUrl: URL?
    private let db: String
    private let getToken: @Sendable () async -> String?
    private let config: RtDbClientConfig
    private let transportFactory: @Sendable (URL) -> any WebSocketTransport
    private let scheduler: WScheduler
    private let random: @Sendable () -> Double

    // Lifecycle state — only touched on the actor.
    private var state: WsState = .idle
    private var user: AuthedUser?
    /// Terminal flag: `close()` or a 4401/authErr. Nothing further, ever.
    private var terminal = false
    /// Bumped on every (re)open and on `close()`; awaited wakeups capture the
    /// value they were scheduled under and abort if it has advanced — the
    /// guard against stale sessions (rust `generation`).
    private var generation = 0
    private var runLoopTask: Task<Void, Never>?
    private var currentTransport: (any WebSocketTransport)?
    /// Set when `connect()` pokes a loop that has not reached its park point
    /// yet — closes the lost-wakeup window (rust `wait_for_poke`).
    private var pendingPokes = 0
    private var pokeWaiters: [CheckedContinuation<Bool, Never>] = []
    private var connectedWaiters: [CheckedContinuation<Void, Never>] = []
    private var statusContinuations: [UUID: AsyncStream<ClientStatus>.Continuation] = [:]
    /// Bookkeeping for the heartbeat: virtual/epoch time of the last pong.
    private var lastPongMs: UInt64?

    /// Task 14's dispatch seam: handlers for every inbound `ServerMessage`
    /// that is not lifecycle-owned (authOk/authErr/pong).
    private var messageHooks: [String: ServerMessageHook] = [:]

    // Task 14 state — only touched on the actor (rust: SubMaps + the driver's
    // pending/unsent pairs).

    /// Thrown by every op on a terminal client (rust's `is_closed` guard).
    private static let closedError = RtDbError(code: .internal, message: "client is closed")

    /// One server subscription shared by every caller subscribed to the same
    /// query shape (rust `SubState`): the wire id, the query for replay, the
    /// live handles' sinks, the refcount, and the latest snapshot for
    /// late-joiner seeding (watch-channel parity).
    private struct SubscriptionEntry {
        let queryId: String
        let query: Query
        var refcount: Int
        var latest: RawSnapshot
        var sinks: [any SubscriptionSink]
    }

    /// Canonical query shape -> entry (rust `by_key`).
    private var subscriptionsByKey: [String: SubscriptionEntry] = [:]
    /// Wire queryId -> canonical shape (rust `by_id`).
    private var queryIdToKey: [String: String] = [:]
    /// Registration order, so replay is deterministic (a Dict iterates
    /// unordered; rust's HashMap replay order is unspecified — this is stricter).
    private var subscriptionOrder: [String] = []
    private var subCounter = 1

    /// A mutate call awaiting its turn (rust `QueuedMutate`): queued while
    /// unauthenticated or re-queued when a send failed mid-session; moved to
    /// the family's pending map once on the wire. The reply mailbox is an
    /// unbounded AsyncStream — a reply that races the send's return is
    /// buffered, never lost.
    private struct MutationCall: OpCall {
        let id: String
        let idempotencyKey: String?
        let txn: Transaction
        let continuation: AsyncStream<Result<[JSONValue], RtDbError>>.Continuation

        func message() -> ClientMessage {
            .mutate(mutId: id, idempotencyKey: idempotencyKey, txn: txn)
        }

        func fail(_ error: RtDbError) {
            continuation.yield(.failure(error))
            continuation.finish()
        }
    }

    private let mutations = OpFamily<MutationCall>(prefix: "mut-")

    /// The typed success payload of a schedule-family reply (rust
    /// `ScheduleOutcome`).
    private enum ScheduleReply: Sendable {
        case id(String)
        case ack(Bool)
        case list([ScheduleInfo])
    }

    /// The request a schedule call will send once authenticated (rust
    /// `ScheduleMsg`).
    private enum ScheduleRequest: Sendable {
        case schedule(when: ScheduleWhen, txn: Transaction)
        case cancel(id: String)
        case pause(id: String)
        case resume(id: String)
        case list
    }

    private struct ScheduleCall: OpCall {
        let id: String
        let request: ScheduleRequest
        let continuation: AsyncStream<Result<ScheduleReply, RtDbError>>.Continuation

        func message() -> ClientMessage {
            RtDbClient.scheduleFrame(scheduleId: id, request: request)
        }

        func fail(_ error: RtDbError) {
            continuation.yield(.failure(error))
            continuation.finish()
        }
    }

    private let schedules = OpFamily<ScheduleCall>(prefix: "sch-")

    /// The typed success payload of a workflow-family reply (rust
    /// `WorkflowOutcome`).
    private enum WorkflowReply: Sendable {
        case info(WorkflowInfo)
        case ack(Bool)
        case list([WorkflowInfo])
    }

    /// The request a workflow call will send once authenticated (rust
    /// `WorkflowMsg`).
    private enum WorkflowRequest: Sendable {
        case start(spec: WorkflowSpec)
        case cancel(id: String)
        case list
    }

    private struct WorkflowCall: OpCall {
        let id: String
        let request: WorkflowRequest
        let continuation: AsyncStream<Result<WorkflowReply, RtDbError>>.Continuation

        func message() -> ClientMessage {
            RtDbClient.workflowFrame(workflowId: id, request: request)
        }

        func fail(_ error: RtDbError) {
            continuation.yield(.failure(error))
            continuation.finish()
        }
    }

    private let workflows = OpFamily<WorkflowCall>(prefix: "wf-")

    public init(
        url: String,
        db: String,
        getToken: @escaping @Sendable () async -> String?,
        config: RtDbClientConfig = RtDbClientConfig(),
        transportFactory: @escaping @Sendable (URL) -> any WebSocketTransport,
        scheduler: WScheduler = SystemScheduler(),
        random: @escaping @Sendable () -> Double = { Double.random(in: 0 ..< 1) }
    ) {
        syncUrl = Self.makeSyncUrl(from: url)
        self.db = db
        self.getToken = getToken
        self.config = config
        self.transportFactory = transportFactory
        self.scheduler = scheduler
        self.random = random
    }

    /// Start (or resume) connecting. Idempotent: a no-op while connecting,
    /// connected, or reconnecting; a no-op forever once terminal. Revives a
    /// client parked idle (no credential) by poking its run loop.
    public func connect() async {
        guard !terminal else { return }
        switch state {
        case .connecting, .connected, .reconnecting:
            return
        case .idle:
            if !pokeWaiters.isEmpty {
                pokeWaiters.removeFirst().resume(returning: true)
            } else if runLoopTask == nil {
                runLoopTask = Task { await self.runLoop() }
            } else {
                // The loop exists but has not parked yet — leave it a poke.
                pendingPokes += 1
            }
        case .closed:
            break
        }
    }

    /// Stop the run loop, drop the socket, and finish every stream/waiter.
    /// Idempotent, and terminal: `connect()` after it is a no-op.
    public func close() async {
        guard !terminal else { return }
        terminal = true
        generation &+= 1
        for waiter in pokeWaiters {
            waiter.resume(returning: false)
        }
        pokeWaiters.removeAll()
        // A poke that raced close() must never spawn a later getToken round.
        pendingPokes = 0
        let loop = runLoopTask
        runLoopTask = nil
        loop?.cancel()
        let transport = currentTransport
        currentTransport = nil
        // Drives URLSession's pending connect/receive callbacks to completion
        // so the cancelled run loop can actually finish.
        await transport?.close(code: 1000)
        // Task 14 (rust close -> drive-loop exit): reject every in-flight AND
        // queued op, finish subscription streams — deterministic here rather
        // than waiting on the cancelled loop's unwind.
        rejectAllOperations(reason: "client is closed")
        setState(.closed)
    }

    /// Current connection/auth snapshot.
    public func status() -> ClientStatus {
        ClientStatus(state: state, user: user)
    }

    /// Live state for UIs: seeded with the current status, then one value per
    /// transition; finishes when the client becomes terminal.
    public var statusStream: AsyncStream<ClientStatus> {
        AsyncStream(bufferingPolicy: .bufferingNewest(16)) { continuation in
            let id = UUID()
            statusContinuations[id] = continuation
            continuation.yield(ClientStatus(state: state, user: user))
            if terminal {
                continuation.finish()
                statusContinuations[id] = nil
            } else {
                continuation.onTermination = { [weak self] _ in
                    guard let self else { return }
                    Task { await self.dropStatusContinuation(id) }
                }
            }
        }
    }

    /// Test/support helper: suspends until `.connected` — or a terminal state,
    /// so a caller can never hang on a client that will not connect.
    func awaitConnected() async {
        if state == .connected || terminal {
            return
        }
        await withCheckedContinuation { continuation in
            connectedWaiters.append(continuation)
        }
    }

    /// Test seam: the scheduler time of the last pong, so tests can observe
    /// that a pong was consumed before advancing a manual clock.
    func livenessLastPongMs() -> UInt64? {
        lastPongMs
    }

    // MARK: Task 14 dispatch seam

    /// A handler invoked for every inbound `ServerMessage` that is not
    /// lifecycle-owned. Registered by id so Task 14 can remove per-subscription
    /// / per-mutation handlers cheaply.
    typealias ServerMessageHook = @Sendable (ServerMessage) async -> Void

    func addMessageHook(_ id: String, _ hook: @escaping ServerMessageHook) {
        messageHooks[id] = hook
    }

    func removeMessageHook(_ id: String) {
        messageHooks.removeValue(forKey: id)
    }

    // MARK: Run loop

    /// Why a session ended, and what the run loop does next.
    private enum SessionOutcome: Sendable {
        /// `close()` was called or the generation moved on.
        case shutdown
        /// Transient disconnect: reconnect after backoff.
        case reconnect
        /// Credential rejected (authErr frame / 4401 close): terminal.
        case authFailed
    }

    /// How a handshake attempt resolved.
    private enum HandshakeResult: Sendable {
        case ok(AuthedUser)
        case authFailed
        case reconnect
    }

    /// The single driver task (rust `drive`): resolve the token, run one
    /// session, then back off and retry — or park idle with no credential.
    /// The `generation` epoch guard plus post-await `terminal` checks keep a
    /// stale iteration from opening a duplicate socket.
    private func runLoop() async {
        defer { runLoopTask = nil }
        // Task 14's router: every non-lifecycle inbound message (subscription
        // updates, op replies) reaches `route` through the hook table.
        // Registered here rather than in init — the closure captures self,
        // which a nonisolated initializer cannot.
        messageHooks["ops"] = { [weak self] message in
            await self?.route(message)
        }
        var attempt = 0
        mainLoop: while true {
            if terminal {
                break
            }

            generation &+= 1
            let gen = generation

            guard let token = await getToken() else {
                // close() may have landed while getToken() was suspended: the
                // late nil return must not move the client out of .closed.
                if terminal || generation != gen || Task.isCancelled {
                    break mainLoop
                }
                setState(.idle)
                guard await parkUntilPoked() else { return }
                continue mainLoop
            }

            if terminal || generation != gen {
                break
            }

            setState(.connecting)
            let outcome: SessionOutcome = if let url = syncUrl {
                await runSession(gen: gen, url: url, token: token)
            } else {
                // Unroutable URL — mirrors rust's connect_async failing: retry.
                .reconnect
            }

            guard await proceed(after: outcome, attempt: &attempt, gen: gen) else {
                break mainLoop
            }
        }
        // Task 14 (rust drive-loop exit): nothing survives the driver. Idempotent
        // with close()'s own rejectAll — whichever runs first drains the maps.
        rejectAllOperations(reason: "client is closed")
    }

    /// Apply a session outcome: true to keep driving (the backoff for this
    /// failure is spent), false to exit the loop.
    private func proceed(
        after outcome: SessionOutcome,
        attempt: inout Int,
        gen: Int
    ) async -> Bool {
        switch outcome {
        case .shutdown:
            return false
        case .authFailed:
            // 4401/authErr is terminal: no reconnect, ever.
            terminal = true
            // Task 14 (rust AuthFailed arm): nothing survives terminal.
            rejectAllOperations(reason: "authentication failed")
            setState(.closed)
            return false
        case .reconnect:
            // close() may have landed mid-session: this arm runs AFTER close()
            // already set .closed, so gate first — a stale unwind must not
            // flip the terminal state or record a backoff.
            if terminal || generation != gen || Task.isCancelled {
                return false
            }
            // Task 14 (rust Reconnect arm): sent-but-unacked ops are rejected
            // (at-most-once, never auto-resent); queued ones survive for the
            // next session's flush.
            rejectInflightOperations(reason: "connection closed before acknowledgment")
            setState(.reconnecting)
            await scheduler.sleep(backoffDelay(attempt: attempt))
            // SystemScheduler.sleep swallows CancellationError — liveness
            // is re-established here, after every sleep.
            if terminal || generation != gen || Task.isCancelled {
                return false
            }
            attempt += 1
            return true
        }
    }

    /// With no credential, park instead of spinning a dial loop (rust
    /// `wait_for_poke`). `connect()` pokes — via a resumed waiter or the
    /// pending flag when it fired before the park point; `close()` terminates.
    /// Returns false when the client shut down while parked.
    private func parkUntilPoked() async -> Bool {
        while true {
            if pendingPokes > 0 {
                pendingPokes -= 1
                return true
            }
            if terminal {
                return false
            }
            return await withCheckedContinuation { continuation in
                pokeWaiters.append(continuation)
            }
        }
    }

    /// Open, authenticate, and run one session until it ends (rust
    /// `run_session`): dial (deadline-bounded) → auth frame first → await
    /// authOk within `authDeadlineMs` → receive + heartbeat until either dies.
    private func runSession(gen: Int, url: URL, token: String) async -> SessionOutcome {
        if terminal || generation != gen {
            return .shutdown
        }
        let transport = transportFactory(url)
        currentTransport = transport
        defer { currentTransport = nil }

        guard await dial(transport, url: url) else { return .reconnect }

        if terminal || generation != gen {
            await transport.close(code: 1000)
            return .shutdown
        }

        switch await authenticate(on: transport, token: token) {
        case .authFailed:
            await transport.close(code: authFailedCloseCode)
            return .authFailed
        case .reconnect, nil:
            return .reconnect
        case let .ok(user)?:
            // Fold-in (Task 13 review residual): a late authOk racing close()
            // must not populate `user` on a terminal client — the same guard
            // the dial path carries, so status() can never report .closed
            // with a non-nil user.
            guard !terminal, generation == gen else {
                await transport.close(code: 1000)
                return .shutdown
            }
            self.user = user
            lastPongMs = scheduler.now()
            setState(.connected)
            // Task 14 (rust run_session post-authOk): re-establish every live
            // subscription, then flush the ops queued while unauthenticated.
            guard
                await replaySubscriptions(on: transport),
                await flushQueuedOps(on: transport)
            else {
                return .reconnect
            }
        }

        // Receive + heartbeat run until either ends (rust's session select).
        // The first child to finish decides the outcome; the loser is
        // cancelled and the transport closed so the group can always drain.
        return await withTaskGroup(of: SessionOutcome.self) { group in
            group.addTask { await self.receiveLoop(transport: transport) }
            if self.config.heartbeatMs > 0 {
                group.addTask { await self.heartbeatLoop(transport: transport, gen: gen) }
            }
            let winner = await group.next() ?? .shutdown
            group.cancelAll()
            await transport.close(code: 1000)
            while await group.next() != nil {}
            return winner
        }
    }

    /// Dial, bounded by `connectTimeoutMs` (rust: `timeout(15s,
    /// connect_async)`). The transport's readiness ping has no internal
    /// timeout of its own, so this race owns the ceiling. False when the dial
    /// failed or the deadline fired.
    private func dial(_ transport: any WebSocketTransport, url: URL) async -> Bool {
        do {
            let connected: Void? = try await withDeadline(
                connectTimeoutMs,
                scheduler: scheduler,
                onDeadline: { await transport.close(code: 1000) },
                operation: { try await transport.connect(to: url) }
            )
            return connected != nil
        } catch {
            return false
        }
    }

    /// Send the auth frame (it must be the first frame), then await authOk
    /// within `authDeadlineMs` — the server closes on a no-show; this race is
    /// the local backstop. nil when the deadline fired.
    private func authenticate(on transport: any WebSocketTransport, token: String) async -> HandshakeResult? {
        do {
            try await transport.send(Self.frame(ClientMessage.auth(token: token, db: db)))
        } catch {
            return .reconnect
        }
        do {
            return try await withDeadline(
                authDeadlineMs,
                scheduler: scheduler,
                onDeadline: { await transport.close(code: 1000) },
                operation: { await Self.handshake(on: transport) }
            )
        } catch {
            return .reconnect
        }
    }

    /// The handshake loop: frames until authOk/authErr, a close, or an
    /// unexpected message. Runs inside `withDeadline`'s operation.
    private static func handshake(on transport: any WebSocketTransport) async -> HandshakeResult {
        while true {
            let frame: String
            do {
                frame = try await transport.receive()
            } catch let error as TransportCloseError {
                return error.code == authFailedCloseCode ? .authFailed : .reconnect
            } catch {
                // Includes CancellationError after losing the deadline race.
                return .reconnect
            }
            guard let message = decodeServerMessage(frame) else { continue }
            switch message {
            case let .authOk(user):
                return .ok(user)
            case .authErr:
                return .authFailed
            default:
                return .reconnect
            }
        }
    }

    /// Inbound frames for the authenticated session (rust session loop):
    /// pong feeds liveness; authOk/authErr arrive only at the handshake and
    /// are ignored mid-session; everything else goes to the Task 14 hooks.
    private func receiveLoop(transport: any WebSocketTransport) async -> SessionOutcome {
        while true {
            let frame: String
            do {
                frame = try await transport.receive()
            } catch let error as TransportCloseError {
                return error.code == authFailedCloseCode ? .authFailed : .reconnect
            } catch {
                return .reconnect
            }
            guard let message = Self.decodeServerMessage(frame) else { continue }
            switch message {
            case .pong:
                lastPongMs = scheduler.now()
            case .authOk, .authErr:
                break
            default:
                for hook in messageHooks.values {
                    await hook(message)
                }
            }
        }
    }

    /// Heartbeat (rust `Liveness` + session ticker): every `heartbeatMs` send a
    /// ping; if `2 * heartbeatMs` has passed without a pong, the connection is
    /// presumed dead — close and reconnect.
    private func heartbeatLoop(transport: any WebSocketTransport, gen: Int) async -> SessionOutcome {
        while true {
            await scheduler.sleep(config.heartbeatMs)
            // SystemScheduler.sleep swallows CancellationError — re-check.
            if Task.isCancelled || terminal || generation != gen {
                return .shutdown
            }
            let nowMs = scheduler.now()
            // lastPongMs defaults to the connect time; a backwards wall clock
            // (NTP step) skips the death check rather than trapping.
            let lastPong = lastPongMs ?? 0
            if nowMs >= lastPong, nowMs - lastPong >= config.heartbeatMs &* 2 {
                await transport.close(code: 1000)
                return .reconnect
            }
            try? await transport.send(Self.frame(ClientMessage.ping))
        }
    }

    // MARK: Task 14 — subscriptions

    /// Subscribe to a live query. Multiple subscribes to the same query shape
    /// (canonical serialized form) share ONE server subscription: the first
    /// mints the wire queryId, the rest attach to it. The handle exists
    /// immediately (`.pending`); the first `queryUpdate` flips it to `.value`.
    /// While disconnected the shape is registered and (re)sent on the next
    /// successful auth.
    public func subscribe<T: Codable & Sendable>(
        _ query: Query,
        as _: T.Type = T.self
    ) async throws -> Subscription<T> {
        guard !terminal else {
            throw Self.closedError
        }
        let key = try Self.canonicalKey(query)
        let (stream, continuation) = AsyncStream<QuerySnapshot<T>>.makeStream(
            of: QuerySnapshot<T>.self, bufferingPolicy: .bufferingNewest(16)
        )
        let sink = TypedSubscriptionSink<T>(
            terminal: query.readTerminal,
            box: LatestSnapshotBox(.pending),
            continuation: continuation
        )
        var seeded: RawSnapshot = .pending
        if var entry = subscriptionsByKey[key] {
            seeded = entry.latest
            entry.refcount += 1
            entry.sinks.append(sink)
            subscriptionsByKey[key] = entry
        } else {
            let queryId = "sub-\(subCounter)"
            subCounter += 1
            subscriptionsByKey[key] = SubscriptionEntry(
                queryId: queryId, query: query, refcount: 1, latest: .pending, sinks: [sink]
            )
            queryIdToKey[queryId] = key
            subscriptionOrder.append(key)
            if let transport = currentTransport, state == .connected {
                // Best-effort: a failed send means the session is dying — the
                // entry stays registered and the next session's replay covers
                // it (the rust driver replays from `subs`, same as here).
                try? await transport.send(try Self.frame(.subscribe(queryId: queryId, query: query)))
            }
        }
        // Watch-channel parity: a late joiner's `current` (and stream) start
        // at the shape's LATEST snapshot — never re-deliver `.pending` (rust's
        // into_stream skips Pending too).
        if case .pending = seeded {} else {
            sink.deliver(seeded)
        }
        return Subscription(box: sink.box, stream: stream) { [weak self] in
            await self?.cancelSubscription(key: key, sinkId: sink.id)
        }
    }

    /// Drop one handle's reference to its shape (rust `maybe_unsubscribe`):
    /// the last release removes the shape and sends `{type:"unsubscribe"}`.
    /// Idempotent per handle: `Subscription` is a copyable struct whose copies
    /// share one sinkId, and rust's Drop-fires-once guarantee has no Swift
    /// equivalent — a repeated or copied-handle cancel releases exactly once.
    private func cancelSubscription(key: String, sinkId: UUID) async {
        guard var entry = subscriptionsByKey[key],
              let sinkIndex = entry.sinks.firstIndex(where: { $0.id == sinkId })
        else { return }
        entry.sinks.remove(at: sinkIndex)
        entry.refcount -= 1
        if entry.refcount > 0 {
            subscriptionsByKey[key] = entry
            return
        }
        subscriptionsByKey.removeValue(forKey: key)
        queryIdToKey.removeValue(forKey: entry.queryId)
        subscriptionOrder.removeAll { $0 == key }
        if let transport = currentTransport, state == .connected {
            // Best-effort — the server drops the shape with the socket anyway.
            try? await transport.send(try Self.frame(.unsubscribe(queryId: entry.queryId)))
        }
    }

    // MARK: Task 14 — mutations

    /// Submit a transaction, resolving to one `StepResult` per step. Pass
    /// `idempotencyKey` to safely retry a mutation whose reply was lost. While
    /// disconnected the call queues and fires on the next auth; it is rejected
    /// only if the connection drops after it was sent but before
    /// acknowledgment (at-most-once, never auto-resent).
    public func mutate(_ txn: Transaction, idempotencyKey: String? = nil) async throws -> [StepResult] {
        try await Self.parseStepResults(submitMutation(txn, idempotencyKey: idempotencyKey))
    }

    private func submitMutation(_ txn: Transaction, idempotencyKey: String?) async throws -> [JSONValue] {
        try await submitOp(mutations) { id, continuation in
            MutationCall(id: id, idempotencyKey: idempotencyKey, txn: txn, continuation: continuation)
        }
    }

    /// Shared submit for every op family (mutation / schedule / workflow —
    /// structurally identical in the rust driver, differing only in payload):
    /// mint the correlation id, register the call as pending BEFORE the send,
    /// re-queue on a failed send, or queue outright while unauthenticated —
    /// then park for the reply.
    private func submitOp<Reply: Sendable, Call: OpCall>(
        _ family: OpFamily<Call>,
        makeCall: (String, AsyncStream<Result<Reply, RtDbError>>.Continuation) -> Call
    ) async throws -> Reply {
        guard !terminal else {
            throw Self.closedError
        }
        let id = family.nextId()
        let (stream, continuation) = AsyncStream<Result<Reply, RtDbError>>.makeStream(
            of: Result<Reply, RtDbError>.self, bufferingPolicy: .unbounded
        )
        let call = makeCall(id, continuation)
        if let transport = currentTransport, state == .connected {
            // Registered BEFORE the send: a reply that races the send's return
            // finds its entry and is buffered, never dropped.
            family.pending[id] = call
            do {
                try await transport.send(Self.frame(call.message()))
            } catch {
                // The frame never hit the wire: re-queue for the next session's
                // flush (rust re-queues on a failed mid-session send). If the
                // reply somehow raced the failed send, the entry is gone — the
                // existence check keeps this a no-op.
                if family.pending[id] != nil {
                    family.pending.removeValue(forKey: id)
                    family.queued.append(call)
                }
            }
        } else {
            family.queued.append(call)
        }
        return try await Self.park(stream)
    }

    // MARK: Task 14 — scheduler ops

    /// Schedule `txn` to fire at `when`; resolves with the new schedule's id
    /// on `scheduleOk`, rejects with `RtDbError` on `scheduleErr`.
    public func schedule(_ txn: Transaction, when: ScheduleWhen) async throws -> String {
        guard case let .id(id) = try await submitSchedule(.schedule(when: when, txn: txn)) else {
            throw RtDbError(code: .internal, message: "unexpected schedule reply")
        }
        return id
    }

    /// Cancel a scheduled job. A bare `ok:false` ack (unknown or
    /// already-terminal id) is a no-op, not an error.
    public func cancelSchedule(_ id: String) async throws {
        try await manageSchedule(.cancel(id: id))
    }

    /// Pause a cron job until `resumeSchedule`. Same ack contract as
    /// `cancelSchedule`.
    public func pauseSchedule(_ id: String) async throws {
        try await manageSchedule(.pause(id: id))
    }

    /// Resume a paused cron job. Same ack contract as `cancelSchedule`.
    public func resumeSchedule(_ id: String) async throws {
        try await manageSchedule(.resume(id: id))
    }

    /// List this database's scheduled jobs.
    public func listSchedules() async throws -> [ScheduleInfo] {
        guard case let .list(schedules) = try await submitSchedule(.list) else {
            throw RtDbError(code: .internal, message: "unexpected schedule reply")
        }
        return schedules
    }

    private func manageSchedule(_ request: ScheduleRequest) async throws {
        guard case .ack = try await submitSchedule(request) else {
            throw RtDbError(code: .internal, message: "unexpected schedule reply")
        }
    }

    private func submitSchedule(_ request: ScheduleRequest) async throws -> ScheduleReply {
        try await submitOp(schedules) { id, continuation in
            ScheduleCall(id: id, request: request, continuation: continuation)
        }
    }

    // MARK: Task 14 — workflow ops

    /// Start a durable workflow run; resolves with the new run's id on
    /// `startWorkflowOk`, rejects on `startWorkflowErr`.
    public func startWorkflow(_ spec: WorkflowSpec) async throws -> String {
        guard case let .info(info) = try await submitWorkflow(.start(spec: spec)) else {
            throw RtDbError(code: .internal, message: "unexpected workflow reply")
        }
        return info.id
    }

    /// Cancel a workflow run. Same ack contract as `cancelSchedule`.
    public func cancelWorkflow(_ id: String) async throws {
        guard case .ack = try await submitWorkflow(.cancel(id: id)) else {
            throw RtDbError(code: .internal, message: "unexpected workflow reply")
        }
    }

    /// List this database's workflow runs.
    public func listWorkflows() async throws -> [WorkflowInfo] {
        guard case let .list(workflows) = try await submitWorkflow(.list) else {
            throw RtDbError(code: .internal, message: "unexpected workflow reply")
        }
        return workflows
    }

    private func submitWorkflow(_ request: WorkflowRequest) async throws -> WorkflowReply {
        try await submitOp(workflows) { id, continuation in
            WorkflowCall(id: id, request: request, continuation: continuation)
        }
    }

    // MARK: Task 14 — session wiring

    /// Replay `subscribe` frames for every live shape, in registration order
    /// (rust run_session's subs replay). False when a send failed — the
    /// session is dying; the caller returns `.reconnect` and the next session
    /// replays again.
    private func replaySubscriptions(on transport: any WebSocketTransport) async -> Bool {
        for key in subscriptionOrder {
            // A last-cancel may have dropped the shape mid-replay.
            guard let entry = subscriptionsByKey[key] else { continue }
            do {
                try await transport.send(
                    Self.frame(.subscribe(queryId: entry.queryId, query: entry.query))
                )
            } catch {
                return false
            }
        }
        return true
    }

    /// Send every op queued while unauthenticated, in queue order. Each call
    /// is registered as pending BEFORE its send; on a failed send it stays at
    /// the queue head (order preserved) for the next session.
    private func flushQueuedOps(on transport: any WebSocketTransport) async -> Bool {
        guard await flushFamily(mutations, on: transport) else { return false }
        guard await flushFamily(schedules, on: transport) else { return false }
        return await flushFamily(workflows, on: transport)
    }

    /// Flush one family's queue. Each call is registered as pending BEFORE its
    /// send; on a failed send it stays at the queue head (order preserved) for
    /// the next session.
    private func flushFamily(
        _ family: OpFamily<some OpCall>, on transport: any WebSocketTransport
    ) async -> Bool {
        while let call = family.queued.first {
            family.pending[call.id] = call
            let frame = try? Self.frame(call.message())
            guard let frame, await (try? transport.send(frame)) != nil else {
                family.unpendIfUnacked(call.id)
                return false
            }
            family.queued.removeFirst()
        }
        return true
    }

    // swiftlint:disable cyclomatic_complexity function_body_length
    /// Route one inbound non-lifecycle `ServerMessage` (rust
    /// `apply_server_message`): query updates to their shape's sinks, op
    /// replies to their pending calls by correlation id. Unknown ids (late or
    /// duplicate replies) are dropped, exactly like rust's `if let` guards.
    private func route(_ message: ServerMessage) {
        switch message {
        case let .queryUpdate(queryId, result):
            guard let key = queryIdToKey[queryId], var entry = subscriptionsByKey[key] else { return }
            entry.latest = .value(result)
            subscriptionsByKey[key] = entry
            for sink in entry.sinks {
                sink.deliver(.value(result))
            }
        case let .subscribeErr(queryId, error):
            // The shape is rejected and removed: every handle sees the error,
            // a fresh subscribe to the same shape mints a new queryId.
            guard let key = queryIdToKey.removeValue(forKey: queryId) else { return }
            guard let entry = subscriptionsByKey.removeValue(forKey: key) else { return }
            subscriptionOrder.removeAll { $0 == key }
            for sink in entry.sinks {
                sink.deliver(.failed(error))
                sink.finish()
            }
        case let .mutateOk(mutId, results):
            if let call = mutations.pending.removeValue(forKey: mutId) {
                call.continuation.yield(.success(results))
                call.continuation.finish()
            }
        case let .mutateErr(mutId, error):
            if let call = mutations.pending.removeValue(forKey: mutId) {
                call.continuation.yield(.failure(error))
                call.continuation.finish()
            }
        case let .scheduleOk(scheduleId, id):
            replySchedule(scheduleId, .success(.id(id)))
        case let .scheduleErr(scheduleId, error):
            replySchedule(scheduleId, .failure(error))
        case let .scheduleAck(scheduleId, ok, error):
            if ok {
                replySchedule(scheduleId, .success(.ack(true)))
            } else if let error {
                replySchedule(scheduleId, .failure(error))
            } else {
                // Bare ok:false = no such pending job (unknown or already
                // terminal): a no-op, not a failure.
                replySchedule(scheduleId, .success(.ack(false)))
            }
        case let .listSchedulesOk(scheduleId, schedules):
            replySchedule(scheduleId, .success(.list(schedules)))
        case let .startWorkflowOk(workflowId, info):
            replyWorkflow(workflowId, .success(.info(info)))
        case let .startWorkflowErr(workflowId, error):
            // Also carries a failed listWorkflows' correlation id — the frame
            // vocabulary has no distinct list-error frame.
            replyWorkflow(workflowId, .failure(error))
        case let .workflowAck(workflowId, ok, error):
            if ok {
                replyWorkflow(workflowId, .success(.ack(true)))
            } else if let error {
                replyWorkflow(workflowId, .failure(error))
            } else {
                replyWorkflow(workflowId, .success(.ack(false)))
            }
        case let .listWorkflowsOk(workflowId, workflows):
            replyWorkflow(workflowId, .success(.list(workflows)))
        case .authOk, .authErr, .pong:
            break // lifecycle-owned — the receive loop never dispatches these
        case .presenceSnapshot, .presenceErr:
            break // presence lands with a later task
        }
    }

    // swiftlint:enable cyclomatic_complexity function_body_length

    private func replySchedule(_ scheduleId: String, _ result: Result<ScheduleReply, RtDbError>) {
        if let call = schedules.pending.removeValue(forKey: scheduleId) {
            call.continuation.yield(result)
            call.continuation.finish()
        }
    }

    private func replyWorkflow(_ workflowId: String, _ result: Result<WorkflowReply, RtDbError>) {
        if let call = workflows.pending.removeValue(forKey: workflowId) {
            call.continuation.yield(result)
            call.continuation.finish()
        }
    }

    /// Reject every SENT-but-unacked op (rust `reject_inflight`): the
    /// connection dropped after the send and before the acknowledgment.
    /// Queued (never-sent) calls survive for the next session.
    private func rejectInflightOperations(reason: String) {
        let error = RtDbError(code: .internal, message: reason)
        mutations.rejectPending(error: error)
        schedules.rejectPending(error: error)
        workflows.rejectPending(error: error)
    }

    /// Reject every op, in-flight AND queued, and finish every subscription
    /// stream (rust `reject_all` at terminal teardown). Idempotent.
    private func rejectAllOperations(reason: String) {
        rejectInflightOperations(reason: reason)
        let error = RtDbError(code: .internal, message: reason)
        mutations.rejectQueued(error: error)
        schedules.rejectQueued(error: error)
        workflows.rejectQueued(error: error)
        for entry in subscriptionsByKey.values {
            for sink in entry.sinks {
                sink.finish()
            }
        }
        subscriptionsByKey.removeAll()
        queryIdToKey.removeAll()
        subscriptionOrder.removeAll()
    }

    /// Test seam (Task 14): queued-but-unsent mutation count, so tests can
    /// deterministically order two queued calls.
    func queuedMutationCountForTesting() -> Int {
        mutations.queued.count
    }

    // MARK: Helpers

    /// Apply a state transition: clear `user` on idle/closed (rust
    /// `set_state`), fan out to every status stream, and release connected
    /// waiters / finish streams on the states they can't wait past.
    private func setState(_ newState: WsState) {
        guard newState != state else { return }
        // Terminal is one-way: .closed is only ever set alongside `terminal`,
        // and once there no stale runLoop unwind may move the client out of
        // it — "nothing further, ever".
        if state == .closed {
            return
        }
        state = newState
        if newState == .idle || newState == .closed {
            user = nil
        }
        let status = ClientStatus(state: state, user: user)
        for continuation in statusContinuations.values {
            continuation.yield(status)
        }
        switch newState {
        case .connected, .closed:
            let waiters = connectedWaiters
            connectedWaiters.removeAll()
            waiters.forEach { $0.resume() }
        case .idle, .connecting, .reconnecting:
            break
        }
        if newState == .closed {
            for continuation in statusContinuations.values {
                continuation.finish()
            }
            statusContinuations.removeAll()
        }
    }

    private func dropStatusContinuation(_ id: UUID) {
        statusContinuations.removeValue(forKey: id)
    }

    /// `http(s)://` -> `ws(s)://`, trimming trailing slashes; `ws(s)://` and
    /// anything else pass through unchanged (rust `sync_url`), then `/sync` is
    /// appended (rust `run_session`). nil when the result is not a URL.
    private static func makeSyncUrl(from base: String) -> URL? {
        var trimmed = base
        while trimmed.hasSuffix("/") {
            trimmed.removeLast()
        }
        if trimmed.hasPrefix("https://") {
            trimmed = "wss://" + trimmed.dropFirst("https://".count)
        } else if trimmed.hasPrefix("http://") {
            trimmed = "ws://" + trimmed.dropFirst("http://".count)
        }
        return URL(string: trimmed + "/sync")
    }

    /// Jittered exponential backoff — rust `backoff_delay`:
    /// `min(max, base * 2^min(attempt, 20)) * (0.5 + random * 0.5)`. The
    /// product saturates (rust `saturating_mul`) so an absurd config caps at
    /// the ceiling instead of trapping.
    private func backoffDelay(attempt: Int) -> UInt64 {
        let exponent = UInt64(1) << UInt64(min(attempt, 20))
        let (product, overflow) = config.backoffBaseMs.multipliedReportingOverflow(by: exponent)
        guard !overflow else {
            return config.backoffMaxMs
        }
        let raw = min(product, config.backoffMaxMs)
        return UInt64(Double(raw) * (0.5 + random() * 0.5))
    }

    private static func frame(_ message: ClientMessage) throws -> String {
        let data = try JSONEncoder().encode(message)
        guard let text = String(data: data, encoding: .utf8) else {
            throw RtDbError(code: .internal, message: "encoded frame is not UTF-8")
        }
        return text
    }

    private static func decodeServerMessage(_ text: String) -> ServerMessage? {
        try? JSONDecoder().decode(ServerMessage.self, from: Data(text.utf8))
    }

    /// Park for an op's reply. A stream that ends without a value means
    /// terminal teardown finished it — or the caller's task was cancelled.
    private static func park<Reply: Sendable>(
        _ stream: AsyncStream<Result<Reply, RtDbError>>
    ) async throws -> Reply {
        var iterator = stream.makeAsyncIterator()
        guard let result = await iterator.next() else {
            if Task.isCancelled {
                throw CancellationError()
            }
            throw closedError
        }
        return try result.get()
    }

    /// Canonical dedup key for a query shape (rust `canonical_key`: the
    /// serialized query). `.sortedKeys` makes nested JSONValue object keys
    /// deterministic, so two `==` queries always canonicalize identically.
    private static func canonicalKey(_ query: Query) throws -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data = try encoder.encode(query)
        guard let text = String(data: data, encoding: .utf8) else {
            throw RtDbError(code: .internal, message: "query canonicalization produced non-UTF-8 bytes")
        }
        return text
    }

    /// rust `parse_step_results`: decode each raw `mutateOk` result into a
    /// `StepResult`, mapping decode failures to RtDbError (internal).
    private static func parseStepResults(_ values: [JSONValue]) throws -> [StepResult] {
        try values.map { value in
            do {
                let data = try JSONEncoder().encode(value)
                return try JSONDecoder().decode(StepResult.self, from: data)
            } catch {
                throw RtDbError(code: .internal, message: "invalid step result: \(error)")
            }
        }
    }

    private static func scheduleFrame(scheduleId: String, request: ScheduleRequest) -> ClientMessage {
        switch request {
        case let .schedule(when, txn):
            .schedule(scheduleId: scheduleId, when: when, txn: txn)
        case let .cancel(id):
            .cancelSchedule(scheduleId: scheduleId, id: id)
        case let .pause(id):
            .pauseSchedule(scheduleId: scheduleId, id: id)
        case let .resume(id):
            .resumeSchedule(scheduleId: scheduleId, id: id)
        case .list:
            .listSchedules(scheduleId: scheduleId)
        }
    }

    private static func workflowFrame(workflowId: String, request: WorkflowRequest) -> ClientMessage {
        switch request {
        case let .start(spec):
            .startWorkflow(workflowId: workflowId, spec: spec)
        case let .cancel(id):
            .cancelWorkflow(workflowId: workflowId, id: id)
        case .list:
            .listWorkflows(workflowId: workflowId, status: nil)
        }
    }
}

/// One op call's shared surface — the three families (mutation, schedule,
/// workflow) differ only in their reply payload and wire frame; correlation,
/// frame construction, and rejection are identical.
private protocol OpCall: Sendable {
    /// The correlation id the call travels (and is replied to) under.
    var id: String { get }
    /// The wire frame this call sends.
    func message() -> ClientMessage
    /// The rejection path — payload-free.
    func fail(_ error: RtDbError)
}

/// One op family's bookkeeping (rust: the driver's pending + unsent maps, one
/// pair per op kind): calls queued while unauthenticated, calls on the wire
/// awaiting their reply, and the correlation-id mint. A class so the shared
/// submit/flush generics can reach the same actor-confined storage across
/// `await`s — every access is actor-isolated, so a reply racing a send finds
/// (or has already consumed) its entry exactly as with the direct maps.
private final class OpFamily<Call: OpCall> {
    var queued: [Call] = []
    var pending: [String: Call] = [:]

    private var counter = 1
    private let prefix: String

    init(prefix: String) {
        self.prefix = prefix
    }

    /// Mint the next correlation id (`mut-1`, `sch-2`, …) in submit order.
    func nextId() -> String {
        let id = "\(prefix)\(counter)"
        counter += 1
        return id
    }

    /// Drop a flushed call's pending registration on a failed send — no-op if
    /// the reply already raced the send's return (the entry is gone).
    func unpendIfUnacked(_ id: String) {
        if pending[id] != nil {
            pending.removeValue(forKey: id)
        }
    }

    /// Reject every pending (sent, unacked) call of this family.
    func rejectPending(error: RtDbError) {
        for call in pending.values {
            call.fail(error)
        }
        pending.removeAll()
    }

    /// Reject every queued (never-sent) call of this family.
    func rejectQueued(error: RtDbError) {
        for call in queued {
            call.fail(error)
        }
        queued.removeAll()
    }
}
