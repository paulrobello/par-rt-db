import Foundation
@testable import ParRtDbClient
import Testing

// Cross-client wire-parity corpus test (ARC-008) — swift-client view.
//
// Loads the shared `wire-corpus/wire-corpus.json` at the repo root and
// asserts every entry round-trips value-identically through the swift
// client's wire types (the fifth implementation of the protocol; the server
// and the TS / Rust / Python clients run equivalent tests on the same
// corpus). Drift here means the swift client drifted from the wire
// contract. A failing case is a bug in the TYPE, never in this test.
//
// Comparison semantics (probed on this toolchain and pinned by
// `deepCompareIsGenuinelyValueBased` below): both sides are parsed by
// JSONSerialization and deep-compared via `isEqual(_:)`, which compares
// VALUES inside nested containers — key order is not contract (JSONEncoder
// emits CodingKeys order), numbers compare numerically across int/double
// (required because JSONEncoder collapses an integral Double to its
// shortest form, `2.0` -> `2`, the documented JSONValue caveat), and
// booleans stay distinct from numbers inside containers.
//
// Sections covered: client_messages (30), server_messages (30),
// authed_users (4), schedule_whens (3), schedule_infos (8), queries (13),
// the five rejects_* sections (6 total), and protocol_constants.max_steps.
// The admin-plane migrate sections are intentionally not covered (the
// swift client has no admin surface yet); query_results / error_envelopes /
// db_stats belong to their owning tasks' types.
//
// NOTE: the generic helpers below use Issue.record rather than #expect —
// the expectation macro's autoclosure thunk inside a generic function trips
// a Swift 6.3.3 compiler crash (see WireTests.swift for the same workaround).

// MARK: - Corpus loading

private struct CorpusFailure: Error, CustomStringConvertible {
    let message: String

    init(_ message: String) {
        self.message = message
    }

    var description: String {
        message
    }
}

/// The shared corpus file, parsed once per test. `wire-corpus.json` is
/// read-only input here — this runner must never write it.
private struct WireCorpus {
    let json: [String: Any]

    init() throws {
        // WireCorpusTests.swift -> ParRtDbClientTests -> Tests -> swift-client -> repo root
        let url = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent() // ParRtDbClientTests
            .deletingLastPathComponent() // Tests
            .deletingLastPathComponent() // swift-client
            .deletingLastPathComponent() // repo root
            .appendingPathComponent("wire-corpus/wire-corpus.json")
        guard let object = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as? [String: Any] else {
            throw CorpusFailure("wire-corpus.json: top level is not a JSON object")
        }
        json = object
    }

    private var sectionNames: String {
        json.keys.sorted().joined(separator: ", ")
    }

    func section(_ name: String) throws -> [[String: Any]] {
        guard let entries = json[name] as? [[String: Any]] else {
            throw CorpusFailure("corpus missing array section '\(name)' — has: \(sectionNames)")
        }
        return entries
    }

    func object(_ name: String) throws -> [String: Any] {
        guard let object = json[name] as? [String: Any] else {
            throw CorpusFailure("corpus missing object section '\(name)' — has: \(sectionNames)")
        }
        return object
    }
}

// MARK: - Helpers

private func pretty(_ data: Data) -> String {
    guard
        let object = try? JSONSerialization.jsonObject(with: data),
        let prettyData = try? JSONSerialization.data(withJSONObject: object, options: [.prettyPrinted, .sortedKeys])
    else { return String(data: data, encoding: .utf8) ?? "<unencodable>" }
    return String(data: prettyData, encoding: .utf8) ?? "<unencodable>"
}

/// Decode -> encode -> deep value-compare against the corpus entry. Key
/// order is not contract; values are (see the header for number/bool
/// semantics). Failures name the section, index, and both payloads.
private func corpusRoundTrip<T: Codable>(_: T.Type, _ section: String, _ corpus: WireCorpus) throws {
    for (idx, raw) in try corpus.section(section).enumerated() {
        let input = try JSONSerialization.data(withJSONObject: raw)
        let parsed: T
        do {
            parsed = try JSONDecoder().decode(T.self, from: input)
        } catch {
            Issue.record("\(section) #\(idx): parse failure: \(error)\n  input: \(pretty(input))")
            continue
        }
        let dumped: Data
        do {
            dumped = try JSONEncoder().encode(parsed)
        } catch {
            Issue.record("\(section) #\(idx): encode failure: \(error)\n  input: \(pretty(input))")
            continue
        }
        let inputObject = try JSONSerialization.jsonObject(with: input) as AnyObject
        let dumpedObject = try JSONSerialization.jsonObject(with: dumped) as AnyObject
        if !dumpedObject.isEqual(inputObject) {
            Issue.record(
                "\(section) #\(idx): wire drift —\n  dumped: \(pretty(dumped))\n  input:  \(pretty(input))"
            )
        }
    }
}

/// Every entry in a rejects_* section must fail to decode — with a
/// DecodingError specifically, since the types reject via the decoder
/// (unknown-field keys, unknown enum tags).
private func corpusRejects<T: Decodable>(_: T.Type, _ section: String, _ corpus: WireCorpus) throws {
    for (idx, raw) in try corpus.section(section).enumerated() {
        let input = try JSONSerialization.data(withJSONObject: raw)
        do {
            _ = try JSONDecoder().decode(T.self, from: input)
            Issue.record("\(section) #\(idx): expected rejection but parsed successfully\n  input: \(pretty(input))")
        } catch is DecodingError {
            // The expected rejection.
        } catch {
            Issue.record("\(section) #\(idx): expected DecodingError, got \(error)\n  input: \(pretty(input))")
        }
    }
}

private func parsedObject(_ text: String) throws -> AnyObject {
    try JSONSerialization.jsonObject(with: Data(text.utf8)) as AnyObject
}

/// Throw-based assertions for the comparison pin — `#expect` around an
/// `AnyObject.isEqual` expression reabstracts the macro's autoclosure thunk
/// and crashes this Swift 6.3.3 frontend (same class as the generic-helper
/// crash noted in the header), so the pin asserts via throws instead.
private func expectDeepDrift(_ lhs: String, _ rhs: String, _ what: String) throws {
    if try parsedObject(lhs).isEqual(parsedObject(rhs)) {
        throw CorpusFailure("deep compare must catch \(what):\n  \(lhs)\n  \(rhs)")
    }
}

private func expectDeepEqual(_ lhs: String, _ rhs: String, _ what: String) throws {
    guard try parsedObject(lhs).isEqual(parsedObject(rhs)) else {
        throw CorpusFailure("deep compare must treat these as equal — \(what):\n  \(lhs)\n  \(rhs)")
    }
}

// MARK: - Round-trip sections

struct WireCorpusTests {
    @Test func clientMessagesRoundTrip() throws {
        try corpusRoundTrip(ClientMessage.self, "client_messages", WireCorpus())
    }

    @Test func serverMessagesRoundTrip() throws {
        try corpusRoundTrip(ServerMessage.self, "server_messages", WireCorpus())
    }

    @Test func authedUsersRoundTrip() throws {
        try corpusRoundTrip(AuthedUser.self, "authed_users", WireCorpus())
    }

    @Test func scheduleWhensRoundTrip() throws {
        try corpusRoundTrip(ScheduleWhen.self, "schedule_whens", WireCorpus())
    }

    @Test func scheduleInfosRoundTrip() throws {
        try corpusRoundTrip(ScheduleInfo.self, "schedule_infos", WireCorpus())
    }

    /// Embedded `Query` wire shapes — filter/search/vectorSearch/paginate.
    /// `Query` carries per-variant unknown-field rejection (it shares the
    /// deny-unknown-fields decoding), so this section doubles as its
    /// acceptance net.
    @Test func queriesRoundTrip() throws {
        try corpusRoundTrip(Query.self, "queries", WireCorpus())
    }

    // MARK: - Reject sections

    @Test func rejectsUnknownClientMessageField() throws {
        try corpusRejects(ClientMessage.self, "rejects_client_message_unknown_field", WireCorpus())
    }

    @Test func rejectsUnknownScheduleWhenField() throws {
        try corpusRejects(ScheduleWhen.self, "rejects_schedule_when_unknown_field", WireCorpus())
    }

    @Test func rejectsUnknownAuthedUserKind() throws {
        try corpusRejects(AuthedUser.self, "rejects_authed_user_unknown_kind", WireCorpus())
    }

    @Test func rejectsUnknownScheduleInfoKind() throws {
        try corpusRejects(ScheduleInfo.self, "rejects_schedule_info_unknown_kind", WireCorpus())
    }

    @Test func rejectsUnknownScheduleInfoStatus() throws {
        try corpusRejects(ScheduleInfo.self, "rejects_schedule_info_unknown_status", WireCorpus())
    }

    // MARK: - Protocol constants

    /// ARC-104: `maxSteps` is part of the four-client wire contract. The
    /// corpus records the canonical agreed value; assert the swift client's
    /// `MutationLimits.maxSteps` matches, so a server change fails here
    /// unless the corpus (and every client) is updated too.
    @Test func protocolConstantsMaxSteps() throws {
        let constants = try WireCorpus().object("protocol_constants")
        guard let maxSteps = constants["max_steps"] as? Int else {
            throw CorpusFailure("protocol_constants.max_steps missing or not an integer: \(constants)")
        }
        #expect(
            maxSteps == MutationLimits.maxSteps,
            "MutationLimits.maxSteps (\(MutationLimits.maxSteps)) != corpus max_steps (\(maxSteps))"
        )
    }

    // MARK: - Comparison pin

    /// Pins the deep-compare semantics this runner relies on: it must catch
    /// a value difference inside nested containers (a bare `as NSDictionary`
    /// reference compare would not), discriminate booleans from numbers
    /// inside containers, catch key-set differences, stay key-order
    /// insensitive, and accept int-vs-integral-double as equal — Foundation
    /// collapses an integral Double to its shortest form on encode (`2.0`
    /// encodes as `2`, see JSONValue), so numeric equality across NSNumber
    /// representations is required, not a loophole.
    @Test func deepCompareIsGenuinelyValueBased() throws {
        let base = #"{"a":{"list":[1,2,3],"keep":"x"}}"#
        try expectDeepDrift(base, #"{"a":{"list":[1,2,4],"keep":"x"}}"#, "a value difference inside a nested array")
        try expectDeepDrift(base, #"{"a":{"list":[1,2,3],"keep":"y"}}"#, "a value difference in a nested dictionary")
        try expectDeepDrift(base, #"{"a":{"list":[1,2,true],"keep":"x"}}"#, "true vs 1 inside a container")
        try expectDeepDrift(base, #"{"a":{"list":[1,2,3],"drop":"x"}}"#, "a key-set difference")
        try expectDeepEqual(base, #"{"a":{"keep":"x","list":[1,2,3]}}"#, "key order is not contract")
        try expectDeepEqual(base, #"{"a":{"list":[1,2,3.0],"keep":"x"}}"#, "2 vs 2.0 (integral-double collapse)")
    }
}
