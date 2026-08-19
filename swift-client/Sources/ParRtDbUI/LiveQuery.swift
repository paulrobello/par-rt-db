import Foundation
import Observation
import ParRtDbClient

/// The UI-facing live query: pumps one `Subscription`'s snapshots into
/// observable `state` on the main actor, so SwiftUI/Observation views update
/// as query results change. Thin by design — connection, refcounting,
/// replay, and typing all live in `ParRtDbClient`; this type owns exactly one
/// subscription handle and the task that drains it.
@MainActor @Observable
public final class LiveQuery<T: Codable & Sendable> {
    /// The subscription's state at one instant: nothing yet, the latest typed
    /// result, or the rejection — a `subscribeErr`, or a `subscribe` call that
    /// threw outright (e.g. on a closed client).
    public enum State: Sendable {
        case pending
        case value(T)
        case failed(RtDbError)
    }

    /// Latest snapshot; observable. `stop()`/`start()` do not reset it — it
    /// reflects the last snapshot received.
    public private(set) var state: State = .pending

    private let client: RtDbClient
    private let query: Query
    // @ObservationIgnored: internal bookkeeping, never view state. For
    // `subscription`/`pumpTask` it ALSO keeps them true stored properties —
    // the macro would rewrite them into tracked computed properties, which
    // the nonisolated deinit cannot read.
    @ObservationIgnored private var subscription: Subscription<T>?
    @ObservationIgnored private var pumpTask: Task<Void, Never>?
    @ObservationIgnored private var started = false
    /// Invalidation epoch for the await inside `start`: a stop→start cycle
    /// while an older `start` is suspended in `client.subscribe` must leave
    /// the newest cycle's handles in place — the superseded call cancels its
    /// fresh handle and assigns nothing (otherwise the orphaned handle keeps
    /// the shape's refcount high and the server subscription never releases).
    @ObservationIgnored private var generation = 0

    /// - Parameters:
    ///   - client: the WS client to subscribe on.
    ///   - query: the query shape to keep live.
    ///   - started: subscribe immediately (via a Task — subscribing is async)
    ///     or wait for an explicit `start()`.
    public init(client: RtDbClient, query: Query, started: Bool = true) {
        self.client = client
        self.query = query
        if started {
            Task { [weak self] in await self?.start() }
        }
    }

    deinit {
        // deinit is nonisolated on a @MainActor class. The two handles are
        // Sendable stored properties, exclusively owned here (deinit runs only
        // when no other reference exists), so move them out and hop: the
        // server subscription must not outlive this LiveQuery by more than one
        // task hop.
        let subscription = self.subscription
        let pump = pumpTask
        Task {
            pump?.cancel()
            await subscription?.cancel()
        }
    }

    /// Subscribe (unless already started) and pump snapshots into `state`.
    /// Idempotent: a second call while started is a no-op. After `stop()`,
    /// starts a fresh subscription — `state` keeps the previous snapshot until
    /// the new subscription's first update lands.
    public func start() async {
        guard !started else { return }
        started = true
        generation += 1
        let gen = generation
        do {
            let sub = try await client.subscribe(query, as: T.self)
            // A stop() (or a whole stop→start cycle) may have landed while
            // subscribe was suspended. Unless this call is still the newest
            // generation, release the just-minted handle instead of displacing
            // the live one.
            guard started, generation == gen else {
                await sub.cancel()
                return
            }
            subscription = sub
            pumpTask = Task { [weak self] in
                for await snapshot in sub.stream {
                    self?.apply(snapshot)
                }
            }
        } catch let error as RtDbError {
            // Same staleness rule as the handle assignment above: a start
            // superseded while suspended in subscribe must not clobber the
            // newest cycle's state with its own failure.
            guard generation == gen else { return }
            state = .failed(error)
        } catch {
            guard generation == gen else { return }
            state = .failed(RtDbError(code: .internal, message: "live query failed: \(error)"))
        }
    }

    /// Cancel the subscription and stop the pump. Idempotent; `state` keeps
    /// the last snapshot.
    public func stop() async {
        guard started else { return }
        started = false
        // Invalidate any start still suspended in subscribe (it will cancel
        // its own fresh handle on resume).
        generation += 1
        pumpTask?.cancel()
        pumpTask = nil
        await subscription?.cancel()
        subscription = nil
    }

    private func apply(_ snapshot: QuerySnapshot<T>) {
        switch snapshot {
        case .pending:
            break // the stream never yields .pending; nothing to publish
        case let .value(value):
            state = .value(value)
        case let .failed(error):
            state = .failed(error)
        }
    }
}
