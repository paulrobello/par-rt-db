import { describe, expect, it } from "vitest";
import { RtDbError } from "@par-rt-db/client";
import { formatWriteError, toErrorMessage } from "./errors";

// toErrorMessage must preserve the inline idiom it replaced
// (`e instanceof Error ? e.message : String(e)`); formatWriteError adds the
// server's wire `{code, message}` envelope rendering for RtDbError.

describe("toErrorMessage", () => {
  it("returns an Error's message", () => {
    expect(toErrorMessage(new Error("boom"))).toBe("boom");
  });

  it("String()s non-Error values", () => {
    expect(toErrorMessage("plain string")).toBe("plain string");
    expect(toErrorMessage(42)).toBe("42");
    expect(toErrorMessage(null)).toBe("null");
    expect(toErrorMessage(undefined)).toBe("undefined");
  });
});

describe("formatWriteError", () => {
  it("renders the RtDbError wire envelope verbatim as `code: message`", () => {
    const err = new RtDbError("NOT_FOUND", "table missing");
    expect(formatWriteError(err)).toBe("NOT_FOUND: table missing");
  });

  it("falls back to toErrorMessage for plain Errors", () => {
    expect(formatWriteError(new Error("network dropped"))).toBe("network dropped");
  });

  it("falls back to String() for non-Error values", () => {
    expect(formatWriteError("weird")).toBe("weird");
  });
});
