import Foundation

// MARK: - Presence (ENH-015)

/// One observable member-list state for a presence room — the presence
/// counterpart of `QuerySnapshot` (rust `ws.rs::PresenceSnapshot`):
/// `.pending` until the first `presenceSnapshot`, then the latest member
/// list on each fan-out, or `.rejected` if the server rejects the join.
public enum PresenceSnapshot: Equatable, Sendable {
    /// No `presenceSnapshot` has arrived yet (a fresh handle starts here).
    case pending
    /// The latest member list from a `presenceSnapshot`.
    case members([PresenceMember])
    /// The server rejected the join (e.g. presence not enabled).
    case rejected(RtDbError)
}

/// Latest-value cell behind `PresenceHandle.current` — the non-generic
/// sibling of `LatestSnapshotBox` (a synchronous read needs a non-actor
/// home; the room entry writes, the handle reads).
/// @unchecked Sendable: `snapshot` is only ever touched while holding `lock`.
final class LatestPresenceBox: @unchecked Sendable {
    private let lock = NSLock()
    private var snapshot: PresenceSnapshot

    init(_ snapshot: PresenceSnapshot) {
        self.snapshot = snapshot
    }

    var value: PresenceSnapshot {
        lock.withLock { snapshot }
    }

    func store(_ snapshot: PresenceSnapshot) {
        lock.withLock { self.snapshot = snapshot }
    }
}

/// A presence-room handle returned by `RtDbClient.presence(room:state:)`.
/// `current` is the room's latest snapshot; `stream` yields each subsequent
/// snapshot (a detected `presenceDelta` seq gap briefly re-publishes
/// `.pending` while the client re-joins to resync) and finishes when the room
/// is left or the client becomes terminal. Stopping iteration stops LISTENING
/// only — the room membership survives until `leavePresence(room:)` (rust:
/// `Presence` is a watch receiver; dropping it does not leave the room).
public struct PresenceHandle: Sendable {
    private let box: LatestPresenceBox
    private let storedStream: AsyncStream<PresenceSnapshot>

    init(box: LatestPresenceBox, stream: AsyncStream<PresenceSnapshot>) {
        self.box = box
        storedStream = stream
    }

    /// The latest snapshot without waiting.
    public var current: PresenceSnapshot {
        box.value
    }

    /// Every snapshot from here on; finishes when the room is left or the
    /// client becomes terminal. Single-consumer.
    public var stream: AsyncStream<PresenceSnapshot> {
        storedStream
    }
}

/// One presence listener's delivery target, held by the room entry and
/// written from the actor (the presence sibling of `SubscriptionSink`).
struct PresenceSink: Sendable {
    let id = UUID()
    let box: LatestPresenceBox
    let continuation: AsyncStream<PresenceSnapshot>.Continuation

    func deliver(_ snapshot: PresenceSnapshot) {
        box.store(snapshot)
        continuation.yield(snapshot)
    }

    func finish() {
        continuation.finish()
    }
}

extension RtDbClient {
    /// One presence room, shared by every caller that joined it (rust
    /// `PresenceRoomState`): the latest join/update `state` (so a reconnect
    /// replays with the freshest value), the latest snapshot for late-joiner
    /// seeding, and the live handles' sinks. Rooms are unique per client —
    /// one connection joins a room once.
    struct PresenceRoom {
        let room: String
        var state: JSONValue?
        var latest: PresenceSnapshot
        var sinks: [PresenceSink]
        /// The last applied `presenceDelta`'s per-room `seq` (nil until the
        /// first delta after a snapshot — snapshots carry no `seq`). A delta
        /// whose `seq` is not `lastSeq + 1` means a frame was missed: the
        /// fold resets the room's member view and re-sends the join frame to
        /// force a fresh full snapshot rather than applying a delta to a
        /// stale baseline.
        var lastSeq: UInt64?
    }

    /// Join presence room `room`, optionally with initial `state` — the Swift
    /// mirror of rust's `presence`. The first `presenceSnapshot` (the server
    /// sends one on join listing current members) resolves `.pending` →
    /// `.members`. Multiple joins to the same room share ONE wire membership
    /// (dedup by room name); each call returns a fresh handle seeded with the
    /// room's current snapshot. While disconnected the join is registered and
    /// replayed on the next successful auth — the same gate as `subscribe`.
    /// Stopping the returned handle's stream stops listening but does NOT
    /// leave the room; call `leavePresence(room:)` for that, mirroring the TS
    /// client where the returned unsubscribe only removes the listener.
    public func presence(room: String, state: JSONValue? = nil) async -> PresenceHandle {
        let (stream, continuation) = AsyncStream<PresenceSnapshot>.makeStream(
            of: PresenceSnapshot.self, bufferingPolicy: .bufferingNewest(16)
        )
        let sink = PresenceSink(box: LatestPresenceBox(.pending), continuation: continuation)
        continuation.onTermination = { [weak self] _ in
            Task { await self?.dropPresenceListener(room: room, sinkId: sink.id) }
        }
        var seeded: PresenceSnapshot = .pending
        if var entry = presenceRooms[room] {
            // Refresh the cached join state so a reconnect replays with the
            // latest value (mirrors the TS client's joinedRooms.set on join).
            if let state {
                entry.state = state
            }
            seeded = entry.latest
            entry.sinks.append(sink)
            presenceRooms[room] = entry
        } else {
            presenceRooms[room] = PresenceRoom(
                room: room, state: state, latest: .pending, sinks: [sink], lastSeq: nil
            )
            presenceOrder.append(room)
        }
        // Join frame, deduped per session (rust's Cmd::PresenceJoin arm): a
        // join racing the session-start replay must not double-send, and a
        // replay that already joined skips. Best-effort — a failed send means
        // the session is dying; the entry stays registered and the next
        // session's replay covers it.
        if let transport = currentTransport, self.state == .connected, sentPresenceRooms.insert(room).inserted {
            try? await transport.send(
                try Self.frame(.presence(room: room, state: presenceRooms[room]?.state))
            )
        }
        // Watch-channel parity: a late joiner's `current` (and stream) start
        // at the room's LATEST snapshot — never re-deliver `.pending`.
        if case .pending = seeded {} else {
            sink.deliver(seeded)
        }
        return PresenceHandle(box: sink.box, stream: stream)
    }

    /// Broadcast updated `state` for this connection in `room` — the server
    /// fans out a fresh `presenceSnapshot` to every member. Also updates the
    /// cached join state so a reconnect re-joins with the latest value.
    /// No-op if this client has not joined `room` (mirrors the live server,
    /// which would not relay an update from a non-member).
    ///
    /// `ttlMs` (ENH-015 presence-ttl) tells the server to clear this
    /// connection's `state` to null `ttlMs` after the last refresh (the
    /// member stays in the room); nil means no expiry. Pass a value for a
    /// heartbeat-style refresh, nil for a plain update. While disconnected
    /// the frame is not sent — the cached state above rides the next join
    /// replay, which re-joins with the freshest value.
    public func updatePresence(room: String, state: JSONValue, ttlMs: UInt64? = nil) async {
        guard var entry = presenceRooms[room] else { return }
        entry.state = state
        presenceRooms[room] = entry
        // Relay only while joined on this session (rust's Cmd::PresenceUpdate
        // arm checks `sent_rooms`).
        if sentPresenceRooms.contains(room), let transport = currentTransport, self.state == .connected {
            try? await transport.send(
                try Self.frame(.presenceState(room: room, state: state, ttlMs: ttlMs))
            )
        }
    }

    /// Leave presence room `room`: drops the local membership (finishing
    /// every handle's stream — rust drops the room's watch sender) so the
    /// next reconnect does not replay the join, and sends
    /// `{type:"leavePresence"}` if this session had joined the room. Local
    /// state is cleared regardless of auth state, so a buffered pre-auth
    /// join does not replay after the caller has already left.
    public func leavePresence(room: String) async {
        if let entry = presenceRooms.removeValue(forKey: room) {
            for sink in entry.sinks {
                sink.finish()
            }
        }
        presenceOrder.removeAll { $0 == room }
        // Only send leavePresence if this room was joined on this session
        // (rust's Cmd::PresenceLeave arm): a leave issued while disconnected
        // must not fire on the next session — the room was never joined there.
        if sentPresenceRooms.remove(room) != nil, let transport = currentTransport, state == .connected {
            try? await transport.send(try Self.frame(.leavePresence(room: room)))
        }
    }

    /// One handle's stream terminated: remove only that listener (rust: a
    /// dropped watch receiver). The room membership itself survives.
    private func dropPresenceListener(room: String, sinkId: UUID) {
        guard var entry = presenceRooms[room] else { return }
        entry.sinks.removeAll { $0.id == sinkId }
        presenceRooms[room] = entry
    }

    /// Re-send a room's stored join frame after a detected `presenceDelta`
    /// seq gap: the server treats `presence` as an idempotent re-join that
    /// resets the connection's caught-up flag, so the next broadcast is a
    /// full `presenceSnapshot` the fold can resync from. Best-effort,
    /// mirroring the join path — a failed send means the session is dying
    /// and the next session's replay covers it.
    private func resendPresenceJoin(room: String) async {
        guard let transport = currentTransport, state == .connected else { return }
        try? await transport.send(
            try Self.frame(.presence(room: room, state: presenceRooms[room]?.state))
        )
    }

    /// Replay `presence` join frames for every joined room, in join order,
    /// each with the latest cached state (rust run_session's presence replay)
    /// — the server lost this connection's presence on disconnect, so a
    /// fresh join frame is required (not `presenceState`). Rooms already
    /// joined on this session (a `presence` call that raced the replay) are
    /// deduped via `sentPresenceRooms`. False when a send failed — the
    /// session is dying; the caller returns `.reconnect` and the next session
    /// replays again.
    func replayPresenceRooms(on transport: any WebSocketTransport) async -> Bool {
        for room in presenceOrder {
            guard let entry = presenceRooms[room], sentPresenceRooms.insert(room).inserted else { continue }
            do {
                try await transport.send(Self.frame(.presence(room: room, state: entry.state)))
            } catch {
                return false
            }
        }
        return true
    }

    /// Route a `presenceSnapshot`/`presenceDelta`/`presenceErr` frame to its
    /// room's sinks.
    func onPresence(_ message: ServerMessage) {
        switch message {
        case let .presenceSnapshot(room, members):
            // Per-room fan-out (rust PresenceSnapshot arm): anyone holding a
            // handle for this room observes the new member list. A snapshot
            // for a room this client has not joined is dropped, exactly like
            // the by_id guard on queryUpdate. A snapshot carries no `seq`, so
            // it also re-arms the delta stream: the next delta may arrive at
            // any seq.
            guard var presenceRoom = presenceRooms[room] else { return }
            presenceRoom.latest = .members(members)
            presenceRoom.lastSeq = nil
            presenceRooms[room] = presenceRoom
            for sink in presenceRoom.sinks {
                sink.deliver(.members(members))
            }
        case let .presenceDelta(room, seq, joined, left, stateChanged):
            applyPresenceDelta(
                room: room, seq: seq, joined: joined, left: left, stateChanged: stateChanged
            )
        case let .presenceErr(room, error):
            // The server rejected the join (e.g. presence not enabled).
            // Surface the error on the room's handles; the room stays
            // registered (rust keeps the map entry — a later snapshot can
            // still arrive if the server changes its mind).
            guard var presenceRoom = presenceRooms[room] else { return }
            presenceRoom.latest = .rejected(error)
            presenceRooms[room] = presenceRoom
            for sink in presenceRoom.sinks {
                sink.deliver(.rejected(error))
            }
        default:
            preconditionFailure("onPresence called with a non-presence message")
        }
    }

    /// Fold one `presenceDelta` into its room's last known member list and
    /// re-publish the full list — the handle API stays "you get the member
    /// list"; deltas are a transport detail. No baseline yet (a pre-snapshot
    /// delta) or an unjoined room: ignored. A `seq` gap resets the view to
    /// `.pending` (a state every consumer already handles from join) and
    /// re-sends the join frame — the server's idempotent join resets the
    /// connection's caught-up flag, so the next broadcast is a full snapshot
    /// to resync from; applying a delta to a stale baseline would corrupt
    /// the member list. Stale duplicates (`seq` ≤ last) are already
    /// reflected and dropped.
    private func applyPresenceDelta(
        room: String, seq: UInt64, joined: [PresenceMember], left: [String],
        stateChanged: [PresenceMember]
    ) {
        guard var presenceRoom = presenceRooms[room], case var .members(members) = presenceRoom.latest else { return }
        if let last = presenceRoom.lastSeq {
            if seq <= last {
                return
            }
            if seq > last + 1 {
                presenceRoom.latest = .pending
                presenceRoom.lastSeq = nil
                presenceRooms[room] = presenceRoom
                for sink in presenceRoom.sinks {
                    sink.deliver(.pending)
                }
                Task { await self.resendPresenceJoin(room: room) }
                return
            }
        }
        for member in joined {
            members.removeAll { $0.connectionId == member.connectionId }
            members.append(member)
        }
        for id in left {
            members.removeAll { $0.connectionId == id }
        }
        for member in stateChanged {
            members.removeAll { $0.connectionId == member.connectionId }
            members.append(member)
        }
        presenceRoom.latest = .members(members)
        presenceRoom.lastSeq = seq
        presenceRooms[room] = presenceRoom
        for sink in presenceRoom.sinks {
            sink.deliver(.members(members))
        }
    }
}
