import Foundation
@testable import ParRtDbClient
import Testing

// HTTP-shape drift verification (swift-client view) over
// `wire-corpus/http-shapes.json` (repo root — the single source of truth; the
// server-side parity test `protocol::tests::http_shapes_fixture_parity`
// constructs the real wire structs against it, and every client mirrors it).
//
// Decodes the fixture's `ClaimedSchedule` and `ScheduleInfo` instances,
// asserts every field (absent optionals must decode nil), and round-trips the
// cron claim through JSONEncoder for canonical structural parity with the
// fixture. The fixture exists because the ClaimedSchedule.tz drift shipped
// undetected (fixed in 8a55d03); the WS corpus does not cover HTTP-only
// shapes.

private struct HttpShapesFailure: Error, CustomStringConvertible {
    let description: String
    init(_ description: String) {
        self.description = description
    }
}

private func httpShapesFixtureURL() -> URL {
    URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // ParRtDbClientTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // swift-client
        .deletingLastPathComponent() // repo root
        .appendingPathComponent("wire-corpus/http-shapes.json")
}

private func loadHttpShapesData(forKey key: String) throws -> Data {
    let root = try JSONSerialization.jsonObject(with: Data(contentsOf: httpShapesFixtureURL()))
    guard let object = root as? [String: Any], let value = object[key] else {
        throw HttpShapesFailure("http-shapes.json: missing key \(key)")
    }
    return try JSONSerialization.data(withJSONObject: value)
}

private func loadHttpShapesObject(forKey key: String) throws -> [String: Any] {
    let root = try JSONSerialization.jsonObject(with: Data(contentsOf: httpShapesFixtureURL()))
    guard let object = root as? [String: Any], let value = object[key] as? [String: Any] else {
        throw HttpShapesFailure("http-shapes.json: missing object key \(key)")
    }
    return value
}

struct HttpShapesTests {
    @Test func claimedScheduleCronDecodesEveryField() throws {
        let data = try loadHttpShapesData(forKey: "claimedScheduleCron")
        let job = try JSONDecoder().decode(ClaimedSchedule.self, from: data)
        #expect(job.id == "job_cron_1")
        #expect(job.kind == .cron)
        #expect(job.dueAt == 1_794_470_400_000)
        #expect(job.txn.steps.isEmpty)
        #expect(job.cron == "0 5 * * *")
        #expect(job.tz == "America/Los_Angeles")
        #expect(job.everyMs == nil)
        #expect(job.leaseGeneration == 7)
        #expect(job.leaseDeadlineMs == 1_794_470_500_000)
    }

    @Test func claimedScheduleOneshotToleratesAbsentOptionals() throws {
        let data = try loadHttpShapesData(forKey: "claimedScheduleOneshot")
        let job = try JSONDecoder().decode(ClaimedSchedule.self, from: data)
        #expect(job.id == "job_oneshot_1")
        #expect(job.kind == .oneshot)
        #expect(job.cron == nil)
        #expect(job.tz == nil)
        #expect(job.everyMs == nil)
        #expect(job.leaseGeneration == 8)
    }

    @Test func claimedScheduleCronRoundTripsToFixtureShape() throws {
        let data = try loadHttpShapesData(forKey: "claimedScheduleCron")
        let job = try JSONDecoder().decode(ClaimedSchedule.self, from: data)
        // JSONEncoder key order is CodingKeys order, so compare as parsed
        // value trees (key order irrelevant) rather than bytes.
        let reencoded = try JSONEncoder().encode(job)
        let got = try #require(JSONSerialization.jsonObject(with: reencoded) as? [String: Any])
        let want = try loadHttpShapesObject(forKey: "claimedScheduleCron")
        #expect((got as NSDictionary) == (want as NSDictionary))
    }

    @Test func scheduleInfoCronDecodesEveryField() throws {
        let data = try loadHttpShapesData(forKey: "scheduleInfoCron")
        let info = try JSONDecoder().decode(ScheduleInfo.self, from: data)
        #expect(info.id == "job_cron_1")
        #expect(info.kind == .cron)
        #expect(info.dueAt == 1_794_470_400_000)
        #expect(info.cron == "0 5 * * *")
        #expect(info.tz == "America/Los_Angeles")
        #expect(info.status == .paused)
        #expect(info.lastError == "connection reset")
        #expect(info.createdAt == 1_794_470_000_000)
        #expect(info.firedCount == 3)
        #expect(info.external == true)
        #expect(info.missedCount == 2)
        #expect(info.lastMissedAt == 1_794_470_300_000)
    }

    @Test func scheduleInfoMinimalDecodesDefaults() throws {
        let data = try loadHttpShapesData(forKey: "scheduleInfoMinimal")
        let info = try JSONDecoder().decode(ScheduleInfo.self, from: data)
        #expect(info.id == "job_oneshot_1")
        #expect(info.kind == .oneshot)
        #expect(info.cron == nil)
        #expect(info.tz == nil)
        #expect(info.status == .pending)
        #expect(info.lastError == nil)
        #expect(info.firedCount == 0)
        #expect(info.external == false)
        #expect(info.missedCount == 0)
        #expect(info.lastMissedAt == nil)
    }
}
