import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";

describe("RtDbError", () => {
  it("is an Error carrying code and message", () => {
    const e = new RtDbError("PRECONDITION_FAILED", "version mismatch");
    expect(e).toBeInstanceOf(Error);
    expect(e.name).toBe("RtDbError");
    expect(e.code).toBe("PRECONDITION_FAILED");
    expect(e.message).toBe("version mismatch");
  });

  it("recognizes and rebuilds a wire envelope", () => {
    const raw: unknown = { code: "NOT_FOUND", message: "no such doc" };
    expect(RtDbError.isEnvelope(raw)).toBe(true);
    const e = RtDbError.fromEnvelope(raw as { code: "NOT_FOUND"; message: string });
    expect(e.code).toBe("NOT_FOUND");
    expect(e.message).toBe("no such doc");
  });

  it("rejects non-envelopes and unknown codes", () => {
    expect(RtDbError.isEnvelope({ code: "WAT", message: "x" })).toBe(false);
    expect(RtDbError.isEnvelope({ message: "x" })).toBe(false);
    expect(RtDbError.isEnvelope("nope")).toBe(false);
  });
});
