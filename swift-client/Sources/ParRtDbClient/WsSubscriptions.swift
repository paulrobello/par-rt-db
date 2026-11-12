import Foundation

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
/// `{type:"unsubscribe"}`. Cancelling the task iterating `stream` releases
/// the same ref via the stream's termination handler (the Swift stand-in for
/// rust's `Drop`; a plain `break` from the loop is unobservable while the
/// shape is registered, so explicit `cancel()` remains the deterministic
/// teardown path). The stream is single-consumer.
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

extension RtDbClient {
    /// One server subscription shared by every caller subscribed to the same
    /// query shape (rust `SubState`): the wire id, the query for replay, the
    /// live handles' sinks, the refcount, and the latest snapshot for
    /// late-joiner seeding (watch-channel parity). The two `optimistic*`
    /// fields are rust's per-sub `OptimisticState`: `serverLast` is the
    /// projection base (most recent authoritative result, set on each
    /// `queryUpdate`); `optimisticActive` is true while an overlay is
    /// covering the sinks (set on apply, cleared on reconcile/rollback).
    struct SubscriptionEntry {
        let queryId: String
        let query: Query
        var refcount: Int
        var latest: RawSnapshot
        var sinks: [any SubscriptionSink]
        var optimisticActive: Bool = false
        var serverLast: JSONValue?
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
        // Cancelling the task iterating the stream (a SwiftUI `.task` consumer
        // that disappears) must release the handle's ref without an explicit
        // cancel() — the rust client gets this from Drop. A mere `break` from
        // the loop is not observable while the registry retains the sink's
        // continuation, so deterministic teardown still needs cancel(). The
        // handler is synchronous and off-actor: it only hops to the actor,
        // where the sink-membership guard in cancelSubscription makes the
        // double-fire (task cancellation + explicit cancel) release exactly
        // once.
        continuation.onTermination = { [weak self] _ in
            Task { await self?.cancelSubscription(key: key, sinkId: sink.id) }
        }
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
    /// equivalent — a repeated or copied-handle cancel, and an explicit
    /// cancel() racing the stream's onTermination, all release exactly once
    /// (the sink is removed from the registry synchronously, before this
    /// function's only suspension point, so every later attempt fails the
    /// membership guard).
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

    /// Route a `queryUpdate`/`subscribeErr` frame to its subscription's sinks.
    func onQueryUpdate(_ message: ServerMessage) {
        switch message {
        case let .queryUpdate(queryId, result):
            guard let key = queryIdToKey[queryId], var entry = subscriptionsByKey[key] else { return }
            // Reconcile (rust apply_server_message): the authoritative result
            // supersedes any in-flight overlay and becomes the new projection
            // base for the next optimistic apply.
            entry.optimisticActive = false
            entry.serverLast = result
            entry.latest = .value(result)
            subscriptionsByKey[key] = entry
            for sink in entry.sinks {
                sink.deliver(.value(result))
            }
        case let .subscribeErr(queryId, error):
            // The shape is rejected and removed: every handle sees the error,
            // a fresh subscribe to the same shape mints a new queryId.
            guard let key = queryIdToKey.removeValue(forKey: queryId),
                  let entry = subscriptionsByKey.removeValue(forKey: key)
            else { return }
            subscriptionOrder.removeAll { $0 == key }
            for sink in entry.sinks {
                sink.deliver(.failed(error))
                sink.finish()
            }
        default:
            preconditionFailure("onQueryUpdate called with a non-query message")
        }
    }

    /// Replay `subscribe` frames for every live shape, in registration order
    /// (rust run_session's subs replay). False when a send failed — the
    /// session is dying; the caller returns `.reconnect` and the next session
    /// replays again.
    func replaySubscriptions(on transport: any WebSocketTransport) async -> Bool {
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

    // MARK: Optimistic updates (wiring for projectOptimisticUpdate)

    /// Mirror of rust's `apply_optimistic` (ts `applyOptimistic`): for each
    /// live subscription whose `serverLast` base is known, project `txn`
    /// onto it; for each non-`.skip` projection, push the overlaid value to
    /// the shape's sinks immediately (before the server round-trip) and
    /// record its queryId under `mutId` in `overlaysByMutId` so a later
    /// rollback can find it. Runs in `submitMutation` before the send — no
    /// suspension points — so no shape can vanish mid-apply.
    func applyOptimistic(mutId: String, txn: Transaction) {
        let now = Int64(Date().timeIntervalSince1970 * 1000)
        var touched: Set<String> = []
        var projections: [(key: String, value: JSONValue)] = []
        for (key, entry) in subscriptionsByKey {
            guard let base = entry.serverLast else { continue }
            if case let .overlaid(value) = projectOptimisticUpdate(
                query: entry.query, last: base, txn: txn, now: now
            ) {
                projections.append((key, value))
                touched.insert(entry.queryId)
            }
        }
        for (key, value) in projections {
            guard var entry = subscriptionsByKey[key] else { continue }
            entry.optimisticActive = true
            entry.latest = .value(value)
            subscriptionsByKey[key] = entry
            for sink in entry.sinks {
                sink.deliver(.value(value))
            }
        }
        if !touched.isEmpty {
            overlaysByMutId[mutId] = touched
        }
    }

    /// Revert one subscription's overlay (rust `revert_overlay`): if one is
    /// active and a `serverLast` base exists, push the base back to the
    /// sinks and clear `optimisticActive`. No-op when no overlay is active
    /// (e.g. a `queryUpdate` already reconciled).
    func revertOverlay(queryId: String) {
        guard let key = queryIdToKey[queryId], var entry = subscriptionsByKey[key],
              entry.optimisticActive, let base = entry.serverLast
        else { return }
        entry.optimisticActive = false
        entry.latest = .value(base)
        subscriptionsByKey[key] = entry
        for sink in entry.sinks {
            sink.deliver(.value(base))
        }
    }

    /// Reverse-index revert (rust `revert_overlays_for`): drop `mutId`'s
    /// entry from `overlaysByMutId` and revert every subscription it had
    /// overlaid. Called from the `mutateErr` arm and every reject path.
    func revertOverlays(forMutId mutId: String) {
        if let queryIds = overlaysByMutId.removeValue(forKey: mutId) {
            for queryId in queryIds {
                revertOverlay(queryId: queryId)
            }
        }
    }
}
