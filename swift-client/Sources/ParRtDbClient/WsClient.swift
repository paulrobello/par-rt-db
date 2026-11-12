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
    /// When true, a `mutate` overlays `projectOptimisticUpdate`'s projected
    /// effect on each matching subscription immediately (before the server
    /// round-trip); the overlay is reconciled to the authoritative
    /// `queryUpdate` and rolled back on `mutateErr`/reject/close. Off ⇒
    /// byte-for-byte the pre-optimistic behavior (rust `Config::
    /// optimistic_updates`).
    public var optimisticUpdates: Bool = false
    /// Opt in to ordered replay of offline and sent-but-unacknowledged
    /// mutations. Every call retains its original idempotency key (a UUID
    /// key is minted when omitted). Close/authentication failure rejects the
    /// queue; server mutation errors are never retried. Off by default.
    public var offlineQueue: Bool = false
    /// Lazy queue expiry, measured from the original call, in milliseconds.
    /// Matches the server's default idempotency dedup TTL (five minutes).
    /// Pruned on enqueue, status reads, disconnect, and replay, without timers.
    public var offlineQueueTtlMs: UInt64 = 300_000

    public init() {}
}

/// Snapshot of mutations waiting for offline replay (excludes in-flight calls).
public struct OfflineQueueStatus: Equatable, Sendable {
    public let depth: Int
    /// Age from the original call, in milliseconds, or nil for an empty queue.
    public let oldestAgeMs: UInt64?
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
    var state: WsState = .idle
    private var user: AuthedUser?
    /// Terminal flag: `close()` or a 4401/authErr. Nothing further, ever.
    var terminal = false
    /// Bumped on every (re)open and on `close()`; awaited wakeups capture the
    /// value they were scheduled under and abort if it has advanced — the
    /// guard against stale sessions (rust `generation`).
    private var generation = 0
    private var runLoopTask: Task<Void, Never>?
    var currentTransport: (any WebSocketTransport)?
    /// Live change-feed channels by `feedId` (see ChangeFeed.swift).
    var feedStates: [String: ChangeFeedState] = [:]
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
    static let closedError = RtDbError(code: .internal, message: "client is closed")

    /// Canonical query shape -> entry (rust `by_key`).
    var subscriptionsByKey: [String: SubscriptionEntry] = [:]
    /// Wire queryId -> canonical shape (rust `by_id`).
    var queryIdToKey: [String: String] = [:]
    /// Registration order, so replay is deterministic (a Dict iterates
    /// unordered; rust's HashMap replay order is unspecified — this is stricter).
    var subscriptionOrder: [String] = []
    var subCounter = 1

    /// Reverse index for optimistic rollback (rust `SubMaps::overlays`):
    /// mutId → the queryIds that mutation overlaid. When a `mutateErr` or a
    /// reject path drops a mutation, every subscription it overlaid is
    /// reverted to its `serverLast` base.
    var overlaysByMutId: [String: Set<String>] = [:]

    /// Joined presence rooms (rust `PresenceMaps::by_room`), plus the join
    /// order so replay is deterministic (a Dict iterates unordered).
    var presenceRooms: [String: PresenceRoom] = [:]
    var presenceOrder: [String] = []
    /// Rooms whose join frame was sent on the CURRENT session (rust's
    /// per-session `sent_rooms` set): dedups join-vs-replay, and gates
    /// `presenceState`/`leavePresence` to rooms actually joined this session.
    /// Cleared at each session start; replay (and any join that raced it)
    /// repopulates it.
    var sentPresenceRooms: Set<String> = []

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
        /// Scheduler-clock time of the original `mutate` call. Seed of the
        /// offline queue's TTL and age observability — measured from the call,
        /// across reconnects (not from the send).
        let queuedAtMs: UInt64

        func message() -> ClientMessage {
            .mutate(mutId: id, idempotencyKey: idempotencyKey, txn: txn)
        }

        func fail(_ error: RtDbError) {
            continuation.yield(.failure(error))
            continuation.finish()
        }
    }

    private let mutations = OpFamily<MutationCall>(prefix: "mut-")

    /// Opt-in offline mutation queue (ts `offlineQueue`), in replay order.
    /// Invariant: every entry carries an idempotency key (caller-supplied or
    /// client-minted), so a replayed entry can never double-apply against the
    /// server's dedup table. `MutationCall.queuedAtMs` (the original call's
    /// clock reading) seeds the TTL and age observability across reconnects.
    private var offlineQueue: [MutationCall] = []
    /// True while the post-authOk replay is draining `offlineQueue`. The ts
    /// client's flush is synchronous so a `mutate()` issued during replay can
    /// never overtake the queue on the wire; here each send is an `await`, so
    /// new calls append to the queue behind the drain until it finishes —
    /// strict call order survives the actor's interleaving.
    private var drainingOfflineQueue = false

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
        case schedule(when: ScheduleWhen, txn: Transaction, external: Bool?)
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
        case signal(id: String, name: String, payload: JSONValue?)
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
            if config.offlineQueue {
                // Correlation IDs are minted in actor call order. Dictionary
                // iteration is unordered, so explicitly restore that order.
                let pending = mutations.pending.values.sorted {
                    (Int($0.id.dropFirst(4)) ?? 0) < (Int($1.id.dropFirst(4)) ?? 0)
                }
                offlineQueue.insert(contentsOf: pending, at: 0)
                mutations.pending.removeAll()
                pruneOfflineQueue()
            }
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
        defer {
            currentTransport = nil
            drainingOfflineQueue = false
        }

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
            // Fresh per-session presence-join tracker (rust's run_session-
            // local `sent_rooms`): replay repopulates it just below. A join
            // racing the replays may send its own frame first — the replay's
            // set-insert then dedups it.
            sentPresenceRooms.removeAll()
            drainingOfflineQueue = config.offlineQueue
            setState(.connected)
            // Task 14 (rust run_session post-authOk): re-establish every live
            // subscription, then replay joined presence rooms, then flush the
            // ops queued while unauthenticated.
            guard
                await replaySubscriptions(on: transport),
                await replayPresenceRooms(on: transport),
                await replayFeeds(on: transport),
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
            try await transport.send(
                Self.frame(
                    ClientMessage.auth(
                        token: token, db: db, protocolVersion: WireProtocol.version
                    )
                )
            )
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
            case let .authOk(user, _):
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

    // MARK: Task 14 — mutations

    /// Submit a transaction, resolving to one `StepResult` per step. Pass
    /// `idempotencyKey` to safely retry a mutation whose reply was lost.
    ///
    /// With the opt-in `offlineQueue` config enabled, a call made while the
    /// client cannot dispatch (not connected) queues — with an idempotency
    /// key that is ALWAYS present (the caller's, or a client-minted one) —
    /// and replays on the next successful auth, in original call order,
    /// under its original key; an in-flight call whose connection drops
    /// before acknowledgment re-queues the same way. The `mutate` call stays
    /// pending while queued and resolves with the replayed commit's step
    /// results; it rejects only on queue-TTL expiry, terminal teardown
    /// (`close()`), or auth failure. Server-side `mutateErr` rejections are
    /// never re-queued — the server already evaluated and refused them.
    public func mutate(_ txn: Transaction, idempotencyKey: String? = nil) async throws -> [StepResult] {
        try await Self.parseStepResults(submitMutation(txn, idempotencyKey: idempotencyKey))
    }

    private func submitMutation(_ txn: Transaction, idempotencyKey: String?) async throws -> [JSONValue] {
        // Terminal gate before the optimistic apply (rust's mutate checks
        // is_closed first — a closed client must not overlay anything).
        guard !terminal else {
            throw Self.closedError
        }
        // Mint the correlation id here (rust's mutate does) so the optimistic
        // overlay can be recorded under the mutId the reply will correlate on
        // — before the send, so subscribers see the overlay before the caller
        // awaits (rust applies in `mutate` before dispatching the command).
        let id = mutations.nextId()
        if config.optimisticUpdates {
            applyOptimistic(mutId: id, txn: txn)
        }
        // With the offline queue enabled a key is ALWAYS present — the
        // caller's when supplied, otherwise a client-minted one — so a
        // replayed entry can never double-apply against the server's dedup
        // table (ts ARC-133 invariant).
        let effectiveKey = idempotencyKey ?? (config.offlineQueue ? "off-\(UUID().uuidString)" : nil)
        let queuedAtMs = scheduler.now()
        return try await submitOp(mutations, id: id, offlineQueueable: true) { id, continuation in
            MutationCall(
                id: id, idempotencyKey: effectiveKey, txn: txn,
                continuation: continuation, queuedAtMs: queuedAtMs
            )
        }
    }

    /// Shared submit for every op family (mutation / schedule / workflow —
    /// structurally identical in the rust driver, differing only in payload):
    /// mint the correlation id (or take the caller's pre-minted one — the
    /// optimistic apply hook must run under the same id), register the call
    /// as pending BEFORE the send, re-queue on a failed send, or queue
    /// outright while unauthenticated — then park for the reply.
    private func submitOp<Reply: Sendable, Call: OpCall>(
        _ family: OpFamily<Call>,
        id preMintedId: String? = nil,
        offlineQueueable: Bool = false,
        makeCall: (String, AsyncStream<Result<Reply, RtDbError>>.Continuation) -> Call
    ) async throws -> Reply {
        guard !terminal else {
            throw Self.closedError
        }
        let id = preMintedId ?? family.nextId()
        let (stream, continuation) = AsyncStream<Result<Reply, RtDbError>>.makeStream(
            of: Result<Reply, RtDbError>.self, bufferingPolicy: .unbounded
        )
        let call = makeCall(id, continuation)
        if let transport = currentTransport, state == .connected,
           !(offlineQueueable && config.offlineQueue && drainingOfflineQueue) {
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
                    if offlineQueueable, let mutation = call as? MutationCall, config.offlineQueue {
                        // Only the mutation family is ever offlineQueueable.
                        offlineQueue.append(mutation)
                    } else {
                        family.queued.append(call)
                    }
                }
            }
        } else if offlineQueueable, config.offlineQueue {
            // Opt-in offline queue: the call waits for the next successful
            // auth — or the in-progress replay's drain — replaying in call
            // order under its original idempotency key. Enqueue is a prune
            // point.
            if let mutation = call as? MutationCall, config.offlineQueue {
                // Only the mutation family is ever offlineQueueable.
                pruneOfflineQueue()
                offlineQueue.append(mutation)
            } else {
                family.queued.append(call)
            }
        } else {
            family.queued.append(call)
        }
        return try await Self.park(stream)
    }

    /// Snapshot of the opt-in offline mutation queue: how many mutations are
    /// queued for replay and the age (ms, on the scheduler clock) of the
    /// oldest entry, or nil when the queue is empty (ts
    /// `getOfflineQueueStatus`). Reading is itself a prune point: entries
    /// older than `offlineQueueTtlMs` are rejected (and removed) here, so
    /// polling the getter surfaces expiry without any background timer.
    public func getOfflineQueueStatus() -> OfflineQueueStatus {
        pruneOfflineQueue()
        let now = scheduler.now()
        let oldest = offlineQueue.map(\.queuedAtMs).min()
        return OfflineQueueStatus(
            depth: offlineQueue.count,
            oldestAgeMs: oldest.map { now >= $0 ? now - $0 : 0 }
        )
    }

    /// Lazy TTL pruning (ts parity: no background timer) at the enqueue,
    /// status-read, disconnect, and replay prune points. Expired entries
    /// reject with the ts error text.
    private func pruneOfflineQueue() {
        let now = scheduler.now()
        offlineQueue.removeAll { entry in
            guard now >= entry.queuedAtMs,
                  now - entry.queuedAtMs >= config.offlineQueueTtlMs
            else { return false }
            revertOverlays(forMutId: entry.id)
            entry.fail(RtDbError(code: .internal, message: "offline mutation queue TTL expired"))
            return true
        }
    }

    /// Drain the offline queue, in call order, registering each call as
    /// pending BEFORE its send. On a failed send the entry stays registered —
    /// the session dies (the send failure closes it) and the reconnect arm
    /// re-queues it at the front with its original key. Runs only in the
    /// session task (post-authOk), so it can never outlive its transport; the
    /// `generation` guard additionally stops a late actor wakeup from
    /// continuing a superseded session's drain.
    private func flushOfflineQueue(on transport: any WebSocketTransport) async -> Bool {
        guard !drainingOfflineQueue else { return true }
        drainingOfflineQueue = true
        defer { drainingOfflineQueue = false }
        let gen = generation
        while !terminal, generation == gen, state == .connected {
            pruneOfflineQueue()
            guard !offlineQueue.isEmpty else { return true }
            let entry = offlineQueue.removeFirst()
            mutations.pending[entry.id] = entry
            do {
                try await transport.send(Self.frame(entry.message()))
            } catch {
                // A failed async send may already have applied on the server.
                // Keep it pending for the reconnect arm to requeue with its key.
                await transport.close(code: 4000)
                return false
            }
        }
        return false
    }

    // MARK: Task 14 — scheduler ops

    /// Schedule `txn` to fire at `when`; resolves with the new schedule's id
    /// on `scheduleOk`, rejects with `RtDbError` on `scheduleErr`. `external`
    /// marks an external-claim job: the internal scheduler never executes it —
    /// an application worker claims it over HTTP and finalizes it with the
    /// returned fencing token.
    public func schedule(
        _ txn: Transaction, when: ScheduleWhen, external: Bool? = nil
    ) async throws -> String {
        guard
            case let .id(id) = try await submitSchedule(
                .schedule(when: when, txn: txn, external: external)
            )
        else {
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

    /// Deliver a named signal to a waiting run (`awaitSignal` steps). Same
    /// ack contract as `cancelWorkflow` — typed delivery failures (unknown id,
    /// not waiting, name mismatch) ride the ack's `error` envelope as
    /// NOT_FOUND/CONFLICT rejections.
    public func signalWorkflow(
        _ id: String, name: String, payload: JSONValue? = nil
    ) async throws {
        guard case .ack = try await submitWorkflow(.signal(id: id, name: name, payload: payload))
        else {
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

    /// Send every op queued while unauthenticated, in queue order. Each call
    /// is registered as pending BEFORE its send; on a failed send it stays at
    /// the queue head (order preserved) for the next session.
    private func flushQueuedOps(on transport: any WebSocketTransport) async -> Bool {
        drainingOfflineQueue = false
        guard await flushOfflineQueue(on: transport),
              await flushFamily(mutations, on: transport), await flushFamily(schedules, on: transport)
        else { return false }
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

    /// Route one inbound non-lifecycle `ServerMessage` (rust
    /// `apply_server_message`) to its family handler: query updates to their
    /// shape's sinks, op replies to their pending calls by correlation id.
    /// Unknown ids (late or duplicate replies) are dropped, exactly like
    /// rust's `if let` guards.
    private func route(_ message: ServerMessage) async {
        switch message {
        case .queryUpdate, .subscribeErr:
            onQueryUpdate(message)
        case .mutateOk, .mutateErr:
            onMutateReply(message)
        case .scheduleOk, .scheduleErr, .scheduleAck, .listSchedulesOk:
            onScheduleReply(message)
        case .startWorkflowOk, .startWorkflowErr, .workflowAck, .listWorkflowsOk:
            onWorkflowReply(message)
        case .authOk, .authErr, .pong:
            break // lifecycle-owned — the receive loop never dispatches these
        case .presenceSnapshot, .presenceDelta, .presenceErr:
            onPresence(message)
        case let .feedEvents(feedId, epoch, ops, head):
            await onFeedEvents(feedId: feedId, epoch: epoch, ops: ops, head: head)
        case let .feedErr(feedId, error):
            onFeedErr(feedId: feedId, error: error)
        }
    }

    /// Route a `mutateOk`/`mutateErr` frame to its pending call, syncing the
    /// optimistic-overlay reverse index for both outcomes.
    private func onMutateReply(_ message: ServerMessage) {
        switch message {
        case let .mutateOk(mutId, results):
            if let call = mutations.pending.removeValue(forKey: mutId) {
                call.continuation.yield(.success(results))
                call.continuation.finish()
            }
            // No revert (rust MutateOk arm): the reconciling `queryUpdate`(s)
            // arrive and supersede any overlay. Just drop the reverse index —
            // the overlays are no longer rollback-eligible.
            overlaysByMutId.removeValue(forKey: mutId)
        case let .mutateErr(mutId, error):
            if let call = mutations.pending.removeValue(forKey: mutId) {
                call.continuation.yield(.failure(error))
                call.continuation.finish()
            }
            revertOverlays(forMutId: mutId)
        default:
            preconditionFailure("onMutateReply called with a non-mutate message")
        }
    }

    /// Route a `scheduleOk`/`scheduleErr`/`scheduleAck`/`listSchedulesOk`
    /// frame to its pending call.
    private func onScheduleReply(_ message: ServerMessage) {
        switch message {
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
        default:
            preconditionFailure("onScheduleReply called with a non-schedule message")
        }
    }

    /// Route a `startWorkflowOk`/`startWorkflowErr`/`workflowAck`/
    /// `listWorkflowsOk` frame to its pending call.
    private func onWorkflowReply(_ message: ServerMessage) {
        switch message {
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
        default:
            preconditionFailure("onWorkflowReply called with a non-workflow message")
        }
    }

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
    /// Queued (never-sent) calls survive for the next session. A rejected
    /// mutation never gets its `mutateOk`/`mutateErr`, so its overlays are
    /// rolled back here.
    private func rejectInflightOperations(reason: String) {
        let error = RtDbError(code: .internal, message: reason)
        for call in mutations.pending.values {
            revertOverlays(forMutId: call.id)
            call.fail(error)
        }
        mutations.pending.removeAll()
        schedules.rejectPending(error: error)
        workflows.rejectPending(error: error)
    }

    /// Reject every op, in-flight AND queued, and finish every subscription
    /// and presence stream (rust `reject_all` at terminal teardown; rust
    /// drops the presence watch senders with the driver). Idempotent.
    private func rejectAllOperations(reason: String) {
        rejectInflightOperations(reason: reason)
        let error = RtDbError(code: .internal, message: reason)
        // Queued mutations also had overlays applied (the apply hook runs in
        // submitMutation before the send), so revert them too.
        for call in mutations.queued {
            revertOverlays(forMutId: call.id)
            call.fail(error)
        }
        mutations.queued.removeAll()
        for call in offlineQueue {
            revertOverlays(forMutId: call.id)
            call.fail(error)
        }
        offlineQueue.removeAll()
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
        overlaysByMutId.removeAll()
        for room in presenceRooms.values {
            for sink in room.sinks {
                sink.finish()
            }
        }
        presenceRooms.removeAll()
        presenceOrder.removeAll()
        finishAllFeeds()
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

    static func frame(_ message: ClientMessage) throws -> String {
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
    static func canonicalKey(_ query: Query) throws -> String {
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
        case let .schedule(when, txn, external):
            .schedule(scheduleId: scheduleId, when: when, txn: txn, external: external)
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
        case let .signal(id, name, payload):
            .signalWorkflow(workflowId: workflowId, id: id, name: name, payload: payload)
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
