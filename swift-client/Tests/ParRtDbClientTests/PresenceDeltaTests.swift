import Foundation
@testable import ParRtDbClient
import Testing

/// `presenceDelta` client mirroring (protocol v3): the seq-aware fold in
/// `RtDbClient`'s presence handling — deltas fold into the room's member
/// list, a `seq` gap resets the view and re-joins to force a full snapshot,
/// and stale duplicates are ignored. Owns its own harness (the shared
/// `FakeTransport`/`ManualScheduler` doubles live in WsClientTests.swift).
struct PresenceDeltaTests {
    private let authOkFrame = #"{"type":"authOk","user":{"kind":"machine","email":null,"name":null}}"#

    private struct Harness: Sendable {
        let fake: FakeTransport
        let client: RtDbClient
    }

    private func makeClient(fake: FakeTransport, scheduler: ManualScheduler) -> RtDbClient {
        RtDbClient(
            url: "ws://rtdb.test", db: "app", getToken: { "tok" },
            config: RtDbClientConfig(),
            transportFactory: { _ in fake }, scheduler: scheduler, random: { 0.0 }
        )
    }

    private func connectedClient() async throws -> Harness {
        let fake = FakeTransport()
        let scheduler = ManualScheduler()
        let client = makeClient(fake: fake, scheduler: scheduler)
        await client.connect()
        await fake.release(authOkFrame)
        try await waitUntil("connected") { await client.status().state == .connected }
        return Harness(fake: fake, client: client)
    }

    /// Bounded poll (5 s ceiling) until `condition` holds.
    private func waitUntil(
        _ what: String, _ condition: @Sendable () async -> Bool
    ) async throws {
        let deadline = Date().addingTimeInterval(5)
        while await !condition() {
            if Date() > deadline {
                struct WaiterTimeout: Error { let what: String }
                throw WaiterTimeout(what: what)
            }
            try await Task.sleep(nanoseconds: 5_000_000)
        }
    }

    /// Frames of one wire type, in send order.
    private func frames(ofType type: String, in sent: [String]) -> [String] {
        sent.filter { $0.contains(#""type":"\#(type)""#) }
    }

    private func stringValue(in frame: String, _ field: String) -> String? {
        guard let start = frame.range(of: "\"\(field)\":\"") else { return nil }
        let rest = frame[start.upperBound...]
        guard let end = rest.range(of: "\"") else { return nil }
        return String(rest[..<end.lowerBound])
    }

    /// A member wire object with a controllable state (a `stateChanged`
    /// bucket needs a distinguishable one).
    private func memberFrame(cid: String, state: String) -> String {
        #"{"connectionId":"\#(cid)","user":{"kind":"machine"},"state":\#(state)}"#
    }

    private func member(_ cid: String, state: JSONValue) -> PresenceMember {
        PresenceMember(connectionId: cid, user: AuthedUser(kind: .machine), state: state)
    }

    private func members(_ cids: [String]) -> [PresenceMember] {
        cids.map { member($0, state: .object([:])) }
    }

    /// Join a room and deliver its first full snapshot, returning the handle
    /// — the setup every delta test shares.
    private func joinedRoom(
        _ harness: Harness, room: String, members cids: [String]
    ) async throws -> PresenceHandle {
        let handle = await harness.client.presence(room: room)
        try await waitUntil("join frame") {
            await frames(ofType: "presence", in: harness.fake.sent).count == 1
        }
        let list = cids.map { memberFrame(cid: $0, state: "{}") }.joined(separator: ",")
        await harness.fake.release(
            #"{"type":"presenceSnapshot","room":"\#(room)","members":[\#(list)]}"#
        )
        try await waitUntil("initial snapshot") {
            handle.current == .members(members(cids))
        }
        return handle
    }

    @Test(.timeLimit(.minutes(1)))
    func presenceDeltaFoldsIntoMemberList() async throws {
        let harness = try await connectedClient()
        let handle = try await joinedRoom(harness, room: "doc:1", members: ["c1"])
        // joined: c2 appends.
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":1,"joined":[\#(memberFrame(cid: "c2", state: "{}"))]}"#
        )
        let two = members(["c1", "c2"])
        try await waitUntil("c2 joined") {
            handle.current == .members(two)
        }
        // left: c1 removed.
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":2,"left":["c1"]}"#
        )
        try await waitUntil("c1 left") {
            handle.current == .members(members(["c2"]))
        }
        // stateChanged: c2's state replaced.
        let changed = memberFrame(cid: "c2", state: #"{"k":1}"#)
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":3,"stateChanged":[\#(changed)]}"#
        )
        try await waitUntil("c2 state changed") {
            handle.current == .members([member("c2", state: .object(["k": .int(1)]))])
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func presenceDeltaGapTriggersResyncRejoin() async throws {
        let harness = try await connectedClient()
        let handle = try await joinedRoom(harness, room: "doc:1", members: ["c1"])
        // seq 1 applies; then a jump to seq 3 skips seq 2 — a missed delta.
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":1,"joined":[\#(memberFrame(cid: "c2", state: "{}"))]}"#
        )
        let two = members(["c1", "c2"])
        try await waitUntil("c2 joined") {
            handle.current == .members(two)
        }
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":3,"joined":[\#(memberFrame(cid: "c9", state: "{}"))]}"#
        )
        // The gap must NOT be applied to the stale baseline: the room resets
        // to pending and the client re-sends the join frame (the second
        // "presence" frame on the wire) to force a full snapshot.
        try await waitUntil("resync re-join sent") {
            await frames(ofType: "presence", in: harness.fake.sent).count == 2
        }
        let rejoin = try #require(await frames(ofType: "presence", in: harness.fake.sent).last)
        #expect(stringValue(in: rejoin, "room") == "doc:1")
        #expect(handle.current == .pending)
        // The server answers the re-join with a full snapshot, which both
        // resyncs the member list and re-arms the delta stream.
        let resynced = ["c1", "c2", "c9"]
            .map { memberFrame(cid: $0, state: "{}") }
            .joined(separator: ",")
        await harness.fake.release(
            #"{"type":"presenceSnapshot","room":"doc:1","members":[\#(resynced)]}"#
        )
        try await waitUntil("resynced to full list") {
            handle.current == .members(members(["c1", "c2", "c9"]))
        }
        // Deltas apply again after the snapshot (seq restarts from wherever
        // the room's counter is — any next value is accepted).
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":4,"left":["c9"]}"#
        )
        try await waitUntil("post-resync delta applied") {
            handle.current == .members(two)
        }
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func presenceDeltaStaleDuplicateIsIgnored() async throws {
        let harness = try await connectedClient()
        let handle = try await joinedRoom(harness, room: "doc:1", members: ["c1"])
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":1,"joined":[\#(memberFrame(cid: "c2", state: "{}"))]}"#
        )
        let two = members(["c1", "c2"])
        try await waitUntil("c2 joined") {
            handle.current == .members(two)
        }
        // Replaying seq 1 (duplicate) changes nothing and never re-joins.
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:1","seq":1,"left":["c1"]}"#
        )
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(handle.current == .members(two))
        #expect(await frames(ofType: "presence", in: harness.fake.sent).count == 1)
        await harness.client.close()
    }

    @Test(.timeLimit(.minutes(1)))
    func presenceDeltaForUnknownRoomIsIgnored() async throws {
        let harness = try await connectedClient()
        let handle = try await joinedRoom(harness, room: "doc:1", members: ["c1"])
        // A delta for a room this client has not joined is dropped.
        await harness.fake.release(
            #"{"type":"presenceDelta","room":"doc:other","seq":1,"joined":[\#(memberFrame(cid: "c9", state: "{}"))]}"#
        )
        try await Task.sleep(nanoseconds: 150_000_000)
        #expect(handle.current == .members(members(["c1"])))
        #expect(await frames(ofType: "presence", in: harness.fake.sent).count == 1)
        await harness.client.close()
    }
}
