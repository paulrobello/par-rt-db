import { describe, expect, it, vi } from "vitest";
import { RtDbError } from "../src/errors.js";
import { retryOnPrecondition } from "../src/retry.js";

describe("retryOnPrecondition", () => {
  it("returns on first success without retrying", async () => {
    const fn = vi.fn().mockResolvedValue("ok");
    await expect(retryOnPrecondition(fn)).resolves.toBe("ok");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("retries on PRECONDITION_FAILED then succeeds", async () => {
    const fn = vi
      .fn()
      .mockRejectedValueOnce(new RtDbError("PRECONDITION_FAILED", "stale"))
      .mockRejectedValueOnce(new RtDbError("PRECONDITION_FAILED", "stale"))
      .mockResolvedValue("ok");
    await expect(retryOnPrecondition(fn, { retries: 4 })).resolves.toBe("ok");
    expect(fn).toHaveBeenCalledTimes(3);
  });

  it("rethrows after exhausting the bounded retries", async () => {
    const fn = vi.fn().mockRejectedValue(new RtDbError("PRECONDITION_FAILED", "stale"));
    await expect(retryOnPrecondition(fn, { retries: 2 })).rejects.toMatchObject({
      code: "PRECONDITION_FAILED",
    });
    expect(fn).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it("does not retry other error codes", async () => {
    const fn = vi.fn().mockRejectedValue(new RtDbError("SCHEMA_VIOLATION", "bad"));
    await expect(retryOnPrecondition(fn)).rejects.toMatchObject({ code: "SCHEMA_VIOLATION" });
    expect(fn).toHaveBeenCalledTimes(1);
  });
});
