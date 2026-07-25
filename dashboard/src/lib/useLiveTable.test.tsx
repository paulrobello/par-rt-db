import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// useLiveTable has two paths — the OAuth realtime path (subscribes over /sync
// via RtDbClient) and the admin-key polling path (adminQuery every 2s). Both
// must clean up on unmount: the live path must unsub + close the client; the
// polling path must clear its interval. These are regression tests for the
// "subscription/timer leaked after unmount" class of bug.

const mocks = vi.hoisted(() => {
  const unsubSpy = vi.fn();
  const closeSpy = vi.fn();
  const connectSpy = vi.fn();
  const adminQuery = vi.fn();
  // mutable box so the session mock can return a per-test method without
  // re-running the (hoisted) mock factory.
  const box: {
    method: "oauth" | "adminkey";
    lastSubscribeCb?: (v: unknown[]) => void;
  } = { method: "oauth" };
  const rtDbClientInstance = {
    connect: connectSpy,
    subscribe: vi.fn((_q: unknown, cb: (v: unknown[]) => void) => {
      box.lastSubscribeCb = cb;
      return unsubSpy;
    }),
    close: closeSpy,
  };
  return { unsubSpy, closeSpy, connectSpy, adminQuery, rtDbClientInstance, box };
});

vi.mock("@par-rt-db/client", () => ({
  RtDbClient: vi.fn(() => mocks.rtDbClientInstance),
}));
vi.mock("./admin", () => ({
  useAdmin: () => ({ client: { adminQuery: mocks.adminQuery } }),
}));
vi.mock("./session", () => ({
  // read the box at call time so per-test overrides win
  useSession: () => ({ token: "tok", method: mocks.box.method }),
}));

import { useLiveTable } from "./useLiveTable";

describe("useLiveTable cleanup on unmount", () => {
  beforeEach(() => {
    mocks.unsubSpy.mockClear();
    mocks.closeSpy.mockClear();
    mocks.connectSpy.mockClear();
    mocks.adminQuery.mockReset();
    mocks.box.method = "oauth";
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("live (oauth) path: unsubscribes and closes the RtDbClient on unmount", () => {
    mocks.box.method = "oauth";
    const { unmount } = renderHook(() => useLiveTable("db1", "items", "asc", 10));

    expect(mocks.connectSpy).toHaveBeenCalledTimes(1);
    expect(mocks.unsubSpy).not.toHaveBeenCalled();

    unmount();

    expect(mocks.unsubSpy).toHaveBeenCalledTimes(1);
    expect(mocks.closeSpy).toHaveBeenCalledTimes(1);
  });

  it("polling (admin-key) path: clears the timer so adminQuery stops after unmount", async () => {
    vi.useFakeTimers();
    mocks.box.method = "adminkey";
    mocks.adminQuery.mockResolvedValue({ result: [] });

    const { unmount } = renderHook(() => useLiveTable("db1", "items", "asc", 10));

    // The hook fires poll() synchronously inside the effect; the awaited
    // adminQuery resolves on the microtask queue and triggers setDocs — wrap
    // the flush in act so React doesn't warn about the state update.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    const callsAfterInitial = mocks.adminQuery.mock.calls.length;
    expect(callsAfterInitial).toBeGreaterThanOrEqual(1);

    unmount();

    // Advance well past the 2s poll interval; cleanup must have stopped it.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(mocks.adminQuery.mock.calls.length).toBe(callsAfterInitial);
  });
});
