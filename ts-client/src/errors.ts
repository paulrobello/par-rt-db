/**
 * Error envelope for `@par-rt-db/client`. Every failure — over WebSocket,
 * HTTP, or the in-memory test harness — surfaces as an {@link RtDbError}
 * carrying a stable {@link RtDbErrorCode} from the closed set below, plus a
 * human-readable message. The server's wire envelope is
 * `{ code, message, retryAfter? }`; {@link RtDbError.fromEnvelope} materializes
 * it into the thrown class. Codes map 1:1 to the server's `error.rs` table:
 *
 * - `UNAUTHORIZED` (401) — missing or invalid bearer/session token.
 * - `FORBIDDEN` (403) — authenticated but not allowed (per-row `ownerField`,
 *   `collaboratorsField`, or `authorize` predicate denied the write/read).
 * - `NOT_FOUND` (404) — `get(id)` miss or unknown route.
 * - `SCHEMA_VIOLATION` (400) — push-schema rejected (destructive change,
 *   type mismatch, quota over-cap on tables).
 * - `PRECONDITION_FAILED` (412) — `expectVersion`/`expectAbsent` step failed.
 * - `CONFLICT` (409) — `unique` (or partial-`where` unique) index violation.
 * - `BAD_REQUEST` (400) — malformed DSL, bad cast, bad cursor.
 * - `INTERNAL` (500) — server-side failure (generic message; details in logs).
 * - `RATE_LIMITED` (429) — per-token or per-db rate limit hit; carries
 *   `retryAfter` (seconds).
 * - `QUOTA_EXCEEDED` (507) — ENH-011 per-database resource quota hit
 *   (tables / storage bytes / subs); carries `retryAfter` when applicable.
 * - `UNSUPPORTED_PROTOCOL` (400) — ARC-013: the client requested a
 *   `protocolVersion` (WS `auth` frame or the `X-Rtdb-Protocol` HTTP header)
 *   newer than the server's.
 */

export type RtDbErrorCode =
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "SCHEMA_VIOLATION"
  | "PRECONDITION_FAILED"
  | "CONFLICT"
  | "BAD_REQUEST"
  | "INTERNAL"
  | "RATE_LIMITED"
  | "QUOTA_EXCEEDED"
  | "UNSUPPORTED_PROTOCOL";

/** Every {@link RtDbErrorCode}, in the order declared above. The single
 *  source of truth for "is this a known code" — {@link CODES} and the
 *  ARC-017 wire-corpus test both derive from this array, so a code added
 *  here without a matching `wire-corpus/error-codes.json` row (or vice
 *  versa) fails that test. TypeScript has no enum reflection, so unlike the
 *  server's compile-enforced exhaustive match, forgetting to extend this
 *  array is a test-time, not compile-time, catch. */
export const ALL_ERROR_CODES: readonly RtDbErrorCode[] = [
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "SCHEMA_VIOLATION",
  "PRECONDITION_FAILED",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
  "RATE_LIMITED",
  "QUOTA_EXCEEDED",
  "UNSUPPORTED_PROTOCOL",
];

const CODES: ReadonlySet<string> = new Set<RtDbErrorCode>(ALL_ERROR_CODES);

/**
 * The JSON wire envelope structure for errors returned by the server.
 */
export interface RtDbErrorEnvelope {
  /**
   * The structured error code.
   */
  code: RtDbErrorCode;
  /**
   * Human-readable explanation of the error.
   */
  message: string;
  /**
   * Optional number of seconds the client should wait before retrying.
   */
  retryAfter?: number;
}

/** The single error type surfaced by every client transport. `status`, when
 *  present, is the originating HTTP response code (the wire envelope itself
 *  carries only `code`/`message`/`retryAfter`; the HTTP status is thread-able
 *  through the admin/HTTP error path for callers that surface it in the UI).
 *
 *  ARC-133: `retryAfter`/`status` are typed `number | undefined` (not
 *  `?:`-optional) because the constructor assigns the param verbatim — absent
 *  flows through as a literal `undefined`, which `exactOptionalPropertyTypes`
 *  forbids for `?:` fields but admits for `T | undefined`. Runtime is
 *  unchanged: a `err.retryAfter === undefined` check behaves identically. */
export class RtDbError extends Error {
  readonly code: RtDbErrorCode;
  readonly retryAfter: number | undefined;
  readonly status: number | undefined;

  constructor(code: RtDbErrorCode, message: string, retryAfter?: number, status?: number) {
    super(message);
    this.name = "RtDbError";
    this.code = code;
    this.retryAfter = retryAfter;
    this.status = status;
  }

  /**
   * Checks whether an unknown object matches the structure of `RtDbErrorEnvelope`.
   */
  static isEnvelope(value: unknown): value is RtDbErrorEnvelope {
    return (
      typeof value === "object" &&
      value !== null &&
      "code" in value &&
      "message" in value &&
      typeof (value as { message: unknown }).message === "string" &&
      typeof (value as { code: unknown }).code === "string" &&
      CODES.has((value as { code: string }).code)
    );
  }

  /**
   * Creates an `RtDbError` from a wire `RtDbErrorEnvelope`.
   */
  static fromEnvelope(envelope: RtDbErrorEnvelope, status?: number): RtDbError {
    return new RtDbError(envelope.code, envelope.message, envelope.retryAfter, status);
  }
}
