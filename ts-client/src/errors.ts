export type RtDbErrorCode =
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "SCHEMA_VIOLATION"
  | "PRECONDITION_FAILED"
  | "CONFLICT"
  | "BAD_REQUEST"
  | "INTERNAL";

const CODES: ReadonlySet<string> = new Set<RtDbErrorCode>([
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "SCHEMA_VIOLATION",
  "PRECONDITION_FAILED",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
]);

export interface RtDbErrorEnvelope {
  code: RtDbErrorCode;
  message: string;
}

/** The single error type surfaced by every client transport. */
export class RtDbError extends Error {
  readonly code: RtDbErrorCode;

  constructor(code: RtDbErrorCode, message: string) {
    super(message);
    this.name = "RtDbError";
    this.code = code;
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
    return new RtDbError(envelope.code, envelope.message);
  }
}
