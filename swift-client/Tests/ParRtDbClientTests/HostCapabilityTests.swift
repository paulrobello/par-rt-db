import Foundation
@testable import ParRtDbClient
import Testing

// MARK: - HostCapability wire shapes (frozen at server commit 7460912)

// Pure encode/decode tests for the host-machine-authorization wire types —
// the Swift mirror of core/src/host_capability.rs's strict semantics:
// unknown capability types, extra fields, and malformed hosts all fail
// closed; nothing is normalized.

struct HostCapabilityTests {
    @Test func encodesExactTaggedShape() throws {
        let data = try JSONEncoder().encode(HostCapability.parMeshV1(host: "macdaddy"))
        // Foundation's JSONEncoder does not guarantee key order between
        // processes, so pin the exact key set and values, not byte order.
        let object = try JSONSerialization.jsonObject(with: data) as? [String: String]
        #expect(object == ["type": "parMeshV1", "host": "macdaddy"])
    }

    @Test func roundTripsThroughJSON() throws {
        let original = HostCapability.parMeshV1(host: "macdaddy")
        let data = try JSONEncoder().encode(original)
        let decoded = try JSONDecoder().decode(HostCapability.self, from: data)
        #expect(decoded == original)
    }

    @Test func unknownTypeThrows() {
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(
                HostCapability.self,
                from: Data(#"{"type":"parMeshV2","host":"macdaddy"}"#.utf8)
            )
        }
    }

    @Test func unknownFieldThrows() {
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(
                HostCapability.self,
                from: Data(#"{"type":"parMeshV1","host":"macdaddy","scope":"rw"}"#.utf8)
            )
        }
    }

    @Test func missingHostThrows() {
        #expect(throws: (any Error).self) {
            try JSONDecoder().decode(
                HostCapability.self,
                from: Data(#"{"type":"parMeshV1"}"#.utf8)
            )
        }
    }

    @Test func malformedHostThrows() throws {
        // The Unicode cases pass grapheme-cluster (Character) comparisons but
        // must be rejected like the server rejects them: per-scalar ASCII
        // membership. See final-review F2.
        for host in [
            "", "Macdaddy", "-lead", "has space", "a.b",
            String(repeating: "a", count: 33),
            "b\u{0301}", "a\u{200d}", "a\u{fe0f}", "a\u{0308}"
        ] {
            let body = #"{"type":"parMeshV1","host":"\#(host)"}"#
            #expect(
                throws: (any Error).self,
                "host '\(host)' must be rejected"
            ) {
                try JSONDecoder().decode(HostCapability.self, from: Data(body.utf8))
            }
        }
    }

    @Test func validHostBoundaryShapesDecode() throws {
        for host in ["a", "a-b_c9", String(repeating: "z", count: 32)] {
            let body = #"{"type":"parMeshV1","host":"\#(host)"}"#
            let decoded = try JSONDecoder().decode(HostCapability.self, from: Data(body.utf8))
            #expect(decoded == .parMeshV1(host: host), "host '\(host)' must decode")
        }
    }
}

// MARK: - Enrollment grant DTO shapes

struct EnrollmentGrantDtoTests {
    /// `POST /admin/enrollment-grants` response shape (frozen): camelCase
    /// `{grantId, grant, db, capability, expiresAt}`.
    @Test func grantResponseDecodes() throws {
        let body = """
        {"grantId":"g1","grant":"grt_secret","db":"app",
         "capability":{"type":"parMeshV1","host":"macdaddy"},"expiresAt":1761100000000}
        """
        let grant = try JSONDecoder().decode(EnrollmentGrant.self, from: Data(body.utf8))
        #expect(grant == EnrollmentGrant(
            grantId: "g1", grant: "grt_secret", db: "app",
            capability: .parMeshV1(host: "macdaddy"), expiresAt: 1_761_100_000_000
        ))
    }

    /// `POST /auth/enroll` response shape (frozen): camelCase
    /// `{tokenId, token, db, capability}`.
    @Test func enrollResultDecodes() throws {
        let body = """
        {"tokenId":"t1","token":"tok_abc","db":"app",
         "capability":{"type":"parMeshV1","host":"macdaddy"}}
        """
        let result = try JSONDecoder().decode(EnrollResult.self, from: Data(body.utf8))
        #expect(result == EnrollResult(
            tokenId: "t1", token: "tok_abc", db: "app",
            capability: .parMeshV1(host: "macdaddy")
        ))
    }

    /// The create-grant request encodes only its declared keys — the server
    /// denies unknown fields — and omits `tokenExpiresAt` when nil.
    @Test func createOptionsOmitNilTokenExpiresAt() throws {
        let data = try JSONEncoder().encode(CreateEnrollmentGrantOptions(
            db: "app", name: "macdaddy-enroll",
            capability: .parMeshV1(host: "macdaddy"), expiresAt: 1_761_100_000_000
        ))
        let object = try #require(
            JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(
            Set(object.keys) == ["db", "name", "capability", "expiresAt"],
            "nil tokenExpiresAt must be omitted, got \(object.keys.sorted())"
        )
    }

    @Test func createOptionsSendTokenExpiresAtWhenSet() throws {
        let data = try JSONEncoder().encode(CreateEnrollmentGrantOptions(
            db: "app", name: "macdaddy-enroll",
            capability: .parMeshV1(host: "macdaddy"),
            expiresAt: 1_761_100_000_000, tokenExpiresAt: 1_761_100_900_000
        ))
        let object = try #require(
            JSONSerialization.jsonObject(with: data) as? [String: Any]
        )
        #expect(
            Set(object.keys)
                == ["db", "name", "capability", "expiresAt", "tokenExpiresAt"]
        )
    }
}
