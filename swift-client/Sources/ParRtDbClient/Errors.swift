import Foundation

/// Mirrors server/src/error.rs::ErrorCode one-to-one — SCREAMING_SNAKE on the
/// wire. The set is the contract; port every code, never sample it.
public enum ErrorCode: String, Codable, Sendable, CaseIterable {
    /// Missing or invalid credentials (HTTP 401).
    case unauthorized = "UNAUTHORIZED"
    /// Authenticated but denied — allowlist or per-row-rule rejection (HTTP 403).
    case forbidden = "FORBIDDEN"
    /// Target document does not exist (HTTP 404).
    case notFound = "NOT_FOUND"
    /// Document or schema violates the pushed schema (HTTP 422).
    case schemaViolation = "SCHEMA_VIOLATION"
    /// `expectVersion`/`expectAbsent` mismatch — the retryable write conflict (HTTP 409).
    case preconditionFailed = "PRECONDITION_FAILED"
    /// Malformed request or DSL shape (HTTP 400).
    case badRequest = "BAD_REQUEST"
    /// Server-side failure; carries a generic, non-leaking message (HTTP 500).
    case `internal` = "INTERNAL"
    /// Unique-index violation (HTTP 409).
    case conflict = "CONFLICT"
    /// Rate-limit denial (HTTP 429); the envelope carries `retryAfter` seconds.
    case rateLimited = "RATE_LIMITED"
    /// Per-database resource quota exceeded (HTTP 507).
    case quotaExceeded = "QUOTA_EXCEEDED"
    /// ARC-013: requested `protocolVersion` (WS `auth` frame or the
    /// `X-Rtdb-Protocol` HTTP header) is newer than the server's (HTTP 400).
    case unsupportedProtocol = "UNSUPPORTED_PROTOCOL"
}

/// Every failure is this envelope: `{code, message, retryAfter?}` on the wire
/// (server/src/error.rs::RtDbError). `retryAfter` appears only on
/// RATE_LIMITED bodies; encoding skips it when nil, so every other code stays
/// exactly `{code, message}`.
public struct RtDbError: Error, Equatable, Codable, Sendable {
    public var code: ErrorCode
    public var message: String
    /// Seconds to wait, present only on `RATE_LIMITED`.
    public var retryAfter: UInt32?

    public init(code: ErrorCode, message: String, retryAfter: UInt32? = nil) {
        self.code = code
        self.message = message
        self.retryAfter = retryAfter
    }

    private enum CodingKeys: String, CodingKey, CaseIterable {
        case code, message, retryAfter
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("RtDbError", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        code = try container.decode(ErrorCode.self, forKey: .code)
        message = try container.decode(String.self, forKey: .message)
        retryAfter = try container.decodeIfPresent(UInt32.self, forKey: .retryAfter)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(code, forKey: .code)
        try container.encode(message, forKey: .message)
        try container.encodeIfPresent(retryAfter, forKey: .retryAfter)
    }

    /// Decode a server error body into the envelope. nil when the body isn't
    /// the envelope: not JSON, a missing/unknown code, or an unknown field.
    public static func decodeEnvelope(from data: Data) -> RtDbError? {
        try? JSONDecoder().decode(RtDbError.self, from: data)
    }
}

/// Exact Int -> UInt32 conversion for DSL builder arguments: builders keep
/// numeric arguments as `Int` and convert at `build()` time, throwing
/// badRequest on out-of-range instead of trapping at method time. The single
/// shared copy (hoisted from QueryDsl/MutationDsl's private duplicates,
/// flagged by the Task 9 review) — new DSL layers call this, never re-declare
/// it.
func uint32(_ value: Int, _ name: String) throws -> UInt32 {
    guard let exact = UInt32(exactly: value) else {
        throw RtDbError(
            code: .badRequest,
            message: "\(name) must be a non-negative 32-bit integer, got \(value)"
        )
    }
    return exact
}

/// Retry helper for optimistic-concurrency conflicts (rust-client/src/error.rs
/// `retry_on_precondition`): runs `body` up to `attempts` times, retrying only
/// when it throws an `RtDbError` whose code is PRECONDITION_FAILED. Any other
/// error propagates immediately; exhausting attempts rethrows the last conflict.
public func retryOnPrecondition<T: Sendable>(
    attempts: Int = 8,
    _ body: () async throws -> T
) async throws -> T {
    var last: RtDbError?
    for _ in 0 ..< max(1, attempts) {
        do {
            return try await body()
        } catch let error as RtDbError where error.code == .preconditionFailed {
            last = error
        }
    }
    throw last ?? RtDbError(
        code: .preconditionFailed, message: "retryOnPrecondition: attempts exhausted"
    )
}
