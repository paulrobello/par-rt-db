import Foundation

// Shared presence backing (store.ts `PresenceRooms`), extracted from
// ``InMemoryEngine`` (QA-003b) and moved verbatim into a sibling file.

// MARK: - Presence

/// Shared in-memory presence backing (store.ts `PresenceRooms`): a
/// `room -> connectionId -> member` map with a per-room subscriber set. Two
/// engines that share an instance see each other's joins/updates/leaves fan
/// out — one client, one connection, like the live server's per-ConnId
/// keying. Snapshot order is join order (a JS Map iterates insertion order;
/// the order array preserves it). Not Sendable: same-context test infra.
public final class PresenceRooms {
    private var members: [String: [String: PresenceMember]] = [:]
    private var memberOrder: [String: [String]] = [:]
    private var subs: [String: [Int: ([PresenceMember]) -> Void]] = [:]
    private var expiry: [String: [String: Int64]] = [:]
    private var nextSubToken = 0

    /// Returns a stable-order snapshot of `room`'s current members.
    public func snapshot(_ room: String) -> [PresenceMember] {
        (memberOrder[room] ?? []).compactMap { members[room]?[$0] }
    }

    /// Adds or replaces `member` in `room` and fans out a fresh snapshot.
    public func join(_ room: String, _ member: PresenceMember) {
        if members[room] == nil {
            members[room] = [:]
            memberOrder[room] = []
        }
        if members[room]?[member.connectionId] == nil {
            memberOrder[room]?.append(member.connectionId)
        }
        members[room]?[member.connectionId] = member
        fanOut(room)
    }

    /// Updates the member's state in `room` and fans out. No-op when the
    /// connection is not in the room. When `ttlMs` > 0, schedules an expiry
    /// sweep that nulls this member's `state` at `now + ttlMs` (the member
    /// stays listed); a refresh without one clears any pending expiry
    /// (store.ts `update`).
    public func update(
        _ room: String, _ connectionId: String, state: JSONValue, ttlMs: Int64?, now: Int64
    ) {
        guard var member = members[room]?[connectionId] else { return }
        member.state = state
        members[room]?[connectionId] = member
        if let ttlMs, ttlMs > 0 {
            expiry[room, default: [:]][connectionId] = now + ttlMs
        } else {
            expiry[room]?.removeValue(forKey: connectionId)
        }
        fanOut(room)
    }

    /// Removes the connection from `room` and fans out. No-op if absent.
    /// Clears any pending expiry so a re-join doesn't inherit a stale ttl.
    public func leave(_ room: String, _ connectionId: String) {
        guard members[room] != nil else { return }
        members[room]?.removeValue(forKey: connectionId)
        memberOrder[room]?.removeAll { $0 == connectionId }
        expiry[room]?.removeValue(forKey: connectionId)
        if members[room]?.isEmpty == true {
            members.removeValue(forKey: room)
            memberOrder.removeValue(forKey: room)
            expiry.removeValue(forKey: room)
        }
        fanOut(room)
    }

    /// Clears expired members' `state` to null (the member stays listed) and
    /// fans out each touched room once. Returns true if anything expired.
    @discardableResult
    public func expire(now: Int64) -> Bool {
        var any = false
        var touched: [String] = []
        for (room, roomExpiry) in expiry {
            guard let roomMap = members[room] else {
                expiry.removeValue(forKey: room)
                continue
            }
            var roomTouched = false
            for (connectionId, at) in roomExpiry where at <= now {
                if var member = roomMap[connectionId] {
                    member.state = .null
                    members[room]?[connectionId] = member
                    any = true
                    roomTouched = true
                }
                expiry[room]?.removeValue(forKey: connectionId)
            }
            if roomTouched {
                touched.append(room)
            }
        }
        for room in touched {
            fanOut(room)
        }
        return any
    }

    /// Registers `fn` for `room` snapshots and immediately fires it with the
    /// current snapshot (the server's first `presenceSnapshot` on join).
    /// Returns an unsubscribe.
    public func subscribe(
        _ room: String, _ fn: @escaping ([PresenceMember]) -> Void
    ) -> () -> Void {
        let token = nextSubToken
        nextSubToken += 1
        subs[room, default: [:]][token] = fn
        fn(snapshot(room))
        return { [weak self] in
            self?.subs[room]?.removeValue(forKey: token)
            if self?.subs[room]?.isEmpty == true {
                self?.subs.removeValue(forKey: room)
            }
        }
    }

    private func fanOut(_ room: String) {
        guard let listeners = subs[room] else { return }
        let snap = snapshot(room)
        for fn in listeners.values {
            fn(snap)
        }
    }
}
