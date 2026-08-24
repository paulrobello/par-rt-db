import Foundation
@testable import ParRtDbClient
import Testing

struct ErrorsCursorTests {
    // MARK: ErrorCode

    @Test func errorCodeCasingIsScreamingSnake() {
        for code in ErrorCode.allCases {
            #expect(code.rawValue == code.rawValue.uppercased())
        }
    }

    @Test func errorCodeIsTheFullServerSet() throws {
        // Every wire string from server/src/error.rs::ErrorCode — the full set.
        let expected: Set = [
            "UNAUTHORIZED", "FORBIDDEN", "NOT_FOUND", "SCHEMA_VIOLATION",
            "PRECONDITION_FAILED", "BAD_REQUEST", "INTERNAL", "RATE_LIMITED",
            "CONFLICT", "QUOTA_EXCEEDED", "UNSUPPORTED_PROTOCOL"
        ]
        #expect(ErrorCode.allCases.count == expected.count)
        for code in ErrorCode.allCases {
            #expect(expected.contains(code.rawValue))
            // The rawValue IS the wire string: encode → decode round-trips.
            let encoded = try JSONEncoder().encode([code])
            #expect(String(data: encoded, encoding: .utf8) == "[\"\(code.rawValue)\"]")
            let back = try JSONDecoder().decode([ErrorCode].self, from: encoded)
            #expect(back == [code])
        }
    }

    // MARK: RtDbError envelope

    @Test func errorEnvelopeRoundTrips() throws {
        let err = RtDbError(code: .preconditionFailed, message: "version mismatch")
        let data = try JSONEncoder().encode(err)
        let text = String(data: data, encoding: .utf8) ?? ""
        #expect(text.contains(#""PRECONDITION_FAILED""#))
        #expect(text.contains(#""message":"version mismatch""#))
        // Non-rate-limited envelopes stay byte-identical {code, message}.
        #expect(!text.contains("retryAfter"))
        #expect(RtDbError.decodeEnvelope(from: data) == err)
    }

    @Test func rateLimitedCarriesRetryAfter() throws {
        let err = RtDbError(code: .rateLimited, message: "rate limit exceeded", retryAfter: 42)
        let data = try JSONEncoder().encode(err)
        #expect((String(data: data, encoding: .utf8) ?? "").contains(#""retryAfter":42"#))

        // A real server RATE_LIMITED body decodes (retryAfter is a declared key,
        // absent elsewhere — server/src/error.rs skip_serializing_if).
        let serverBody = Data(
            #"{"code":"RATE_LIMITED","message":"rate limit exceeded","retryAfter":42}"#.utf8
        )
        #expect(RtDbError.decodeEnvelope(from: serverBody) == err)
        #expect(RtDbError.decodeEnvelope(from: serverBody)?.retryAfter == 42)

        // The field is optional both directions.
        let bare = Data(#"{"code":"RATE_LIMITED","message":"rate limit exceeded"}"#.utf8)
        #expect(RtDbError.decodeEnvelope(from: bare)?.retryAfter == nil)
    }

    @Test func decodeEnvelopeRejectsNonEnvelopes() {
        #expect(RtDbError.decodeEnvelope(from: Data("hello".utf8)) == nil)
        #expect(RtDbError.decodeEnvelope(from: Data()) == nil)
        #expect(RtDbError.decodeEnvelope(from: Data(#"{"message":"no code"}"#.utf8)) == nil)
        #expect(RtDbError.decodeEnvelope(from: Data(#"{"code":"NOT_A_CODE","message":"x"}"#.utf8)) == nil)
        // Unknown keys are rejected (deny_unknown_fields parity).
        let extra = Data(#"{"code":"BAD_REQUEST","message":"x","bogus":1}"#.utf8)
        #expect(RtDbError.decodeEnvelope(from: extra) == nil)
    }

    // MARK: retryOnPrecondition

    @Test func retryRetriesOnlyOnPrecondition() async throws {
        let attempts = AttemptCounter()
        let got: Int = try await retryOnPrecondition(attempts: 5) {
            let attempt = await attempts.record()
            if attempt < 3 {
                throw RtDbError(code: .preconditionFailed, message: "conflict \(attempt)")
            }
            return 7
        }
        #expect(got == 7)
        #expect(await attempts.count == 3)
    }

    @Test func retryDoesNotRetryOtherErrors() async throws {
        let attempts = AttemptCounter()
        let body: () async throws -> Int = {
            _ = await attempts.record()
            throw RtDbError(code: .notFound, message: "x")
        }
        await #expect(throws: RtDbError(code: .notFound, message: "x")) {
            _ = try await retryOnPrecondition(attempts: 5, body)
        }
        #expect(await attempts.count == 1)
    }

    @Test func retryExhaustsAndRethrowsLastConflict() async throws {
        let attempts = AttemptCounter()
        let body: () async throws -> Int = {
            let attempt = await attempts.record()
            throw RtDbError(code: .preconditionFailed, message: "conflict \(attempt)")
        }
        await #expect(throws: RtDbError(code: .preconditionFailed, message: "conflict 3")) {
            _ = try await retryOnPrecondition(attempts: 3, body)
        }
        #expect(await attempts.count == 3)
    }

    // MARK: Cursor codec

    @Test func cursorRoundTrips() {
        let cursor: [JSONValue] = [.string("abc"), .int(3), .bool(true), .null, .double(2.5)]
        let encoded = encodeCursor(cursor)
        #expect(decodeCursor(encoded) == cursor)
        #expect(decodeCursor("not-a-cursor!!") == nil)
        #expect(encodeCursor([]) == "W10=")
        #expect(decodeCursor("W10=") == [])
    }

    @Test func cursorIsByteCompatibleWithRust() {
        // The rust-client round_trip fixture: serde_json compact encoding of
        // ["p1","backlog",1700000000000,"id1"], standard base64 with padding.
        let rustCursor = "WyJwMSIsImJhY2tsb2ciLDE3MDAwMDAwMDAwMDAsImlkMSJd"
        let values: [JSONValue] = [
            .string("p1"), .string("backlog"), .int(1_700_000_000_000), .string("id1")
        ]
        #expect(encodeCursor(values) == rustCursor)
        #expect(decodeCursor(rustCursor) == values)
    }

    @Test func decodeCursorRejectsNonArray() {
        // base64 of the JSON string "hello" — valid JSON, not an array.
        #expect(decodeCursor("ImhlbGxvIg==") == nil)
    }
}

/// Serial attempt counter for the retry tests (an actor — strict-concurrency safe).
private actor AttemptCounter {
    private(set) var count = 0

    func record() -> Int {
        count += 1
        return count
    }
}
