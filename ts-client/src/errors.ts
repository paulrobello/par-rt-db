/**
 * Error envelope for `@par-rt-db/client`. Every failure — over WebSocket,
 * HTTP, or the in-memory test harness — surfaces as an {@link RtDbError}
 * carrying a stable {@link RtDbErrorCode} (generated into `errors_gen.ts`
 * from wire-corpus/error-codes.json), plus a
 * human-readable message. The server's wire envelope is
 * `{ code, message, retryAfter? }`; {@link RtDbError.fromEnvelope} materializes
 * it into the thrown class. Codes map 1:1 to the server's `error.rs` table:
 *
 * - `UNAUTHORIZED` (401) — missing or invalid bearer/session token.
 * - `FORBIDDEN` (403) — authenticated but not allowed (per-row `ownerField`,
 *   `collaboratorsField`, or `authorize` predicate denied the write/read).
 * - `NOT_FOUND` (404) — `get(id)` miss or unknown route.
 * - `SCHEMA_VIOLATION` (422) — push-schema rejected (destructive change,
 *   type mismatch, quota over-cap on tables).
 * - `PRECONDITION_FAILED` (409) — an `expectVersion`, `expectAbsent`,
 *   `expectFields` or `expectBefore` guard failed.
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
 * - `CURSOR_EXPIRED` (410) — a change-feed `since` cursor older than the
 *   retained log or ahead of its head; the consumer must resync from
 *   `since: 0` (`RtDbHttpClient.listChanges`).
 * - `READ_ONLY` (409) — the database's operator froze it read-only via
 *   `PATCH /admin/db/{db}/readonly`; client-plane writes are rejected until
 *   it is unfrozen.
 */

import { ALL_ERROR_CODES } from "./errors_gen.js";
import type { RtDbErrorCode } from "./errors_gen.js";

export { ALL_ERROR_CODES };
export type { RtDbErrorCode };

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

/**
 * Thrown when an HTTP/admin request exceeds its `requestTimeoutMs` deadline.
 * Client-local by design: the deadline is armed by this client, not the
 * server, so `DEADLINE_EXCEEDED` is deliberately absent from the wire error
 * corpus (`errors_gen.ts`) and never appears in a server envelope. It does not
 * extend {@link RtDbError} — whose `code` is the generated wire-union type —
 * so a consumer branching on `instanceof RtDbError` sees server failures and
 * this client-originated timeout as distinct classes. Branch on this class
 * (or `RtDbDeadlineError.is`) to handle deadlines.
 */
export class RtDbDeadlineError extends Error {
  /** Stable client-local code, never emitted on the wire. */
  readonly code = "DEADLINE_EXCEEDED" as const;
  /** The deadline that fired, in milliseconds. */
  readonly timeoutMs: number;

  constructor(message: string, timeoutMs: number) {
    super(message);
    this.name = "RtDbDeadlineError";
    this.timeoutMs = timeoutMs;
  }

  /** Type guard: whether `value` is a deadline error thrown by this client. */
  static is(value: unknown): value is RtDbDeadlineError {
    return value instanceof RtDbDeadlineError;
  }
}
