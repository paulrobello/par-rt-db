export type RtDbErrorCode =
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "SCHEMA_VIOLATION"
  | "PRECONDITION_FAILED"
  | "CONFLICT"
  | "BAD_REQUEST"
  | "INTERNAL"
  | "RATE_LIMITED";

const CODES: ReadonlySet<string> = new Set<RtDbErrorCode>([
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "SCHEMA_VIOLATION",
  "PRECONDITION_FAILED",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
  "RATE_LIMITED",
]);

export interface RtDbErrorEnvelope {
  code: RtDbErrorCode;
  message: string;
  retryAfter?: number;
}

/** The single error type surfaced by every client transport. */
export class RtDbError extends Error {
  readonly code: RtDbErrorCode;
  readonly retryAfter?: number;

  constructor(code: RtDbErrorCode, message: string, retryAfter?: number) {
    super(message);
    this.name = "RtDbError";
    this.code = code;
    this.retryAfter = retryAfter;
  }

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

  static fromEnvelope(envelope: RtDbErrorEnvelope): RtDbError {
    return new RtDbError(envelope.code, envelope.message, envelope.retryAfter);
  }
}
