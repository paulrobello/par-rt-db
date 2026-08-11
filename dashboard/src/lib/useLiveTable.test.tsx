import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// useLiveTable has two paths. The OAuth realtime path subscribes over /sync via
// RtDbClient. The non-OAuth path refreshes off the /admin/stream op feed: while
// the stream reports "connected" a matching {db, table} op triggers a refresh,
// and while it reports anything else a 2s poll arms as a reconnect fallback
// (ARC-123). Both the live and polling paths must clean up on unmount: live
// unsubscribes + closes the client; polling clears its interval.

const mocks = vi.hoisted(() => {
  const unsubSpy = vi.fn();
  const closeSpy = vi.fn();
  const connectSpy = vi.fn();
  const adminQuery = vi.fn();
  // mutable box so the session/admin mocks can return per-test values without
  // re-running the (hoisted) mock factory.
  const box: {
    method: "oauth" | "adminkey";
    connection: "idle" | "connecting" | "connected" | "closed";
    ops: { db: string; table: string; kind: string; ts: number }[];
  } = { method: "oauth", connection: "closed", ops: [] };
  const rtDbClientInstance = {
    connect: connectSpy,
    subscribe: vi.fn((_q: unknown, _cb: (v: unknown[]) => void) => unsubSpy),
    close: closeSpy,
  };
  // Stable identity so the hook's polling effect (which depends on `admin`)
  // doesn't tear down + re-fire on every render. The real AdminProvider memos
  // the client; the mock must too, or stream-driven test rerenders would
  // spuriously re-run the mount-time poll.
  const adminClient = { adminQuery };
  return { unsubSpy, closeSpy, connectSpy, adminQuery, rtDbClientInstance, adminClient, box };
});

vi.mock("@par-rt-db/client", () => ({
  // vitest 4 forbids arrow functions as mock constructors; a regular function
  // body is constructable and returns the shared instance when invoked with new.
  RtDbClient: vi.fn(function rtDbClientCtor() {
    return mocks.rtDbClientInstance;
  }),
}));
vi.mock("./admin", () => ({
  useAdmin: () => ({
    client: mocks.adminClient,
    ops: mocks.box.ops,
    connection: mocks.box.connection,
  }),
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
    mocks.box.connection = "closed";
    mocks.box.ops = [];
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

  it("polling path: clears the timer so adminQuery stops after unmount (reconnect fallback)", async () => {
    vi.useFakeTimers();
    mocks.box.method = "adminkey";
    // Stream NOT connected → 2s fallback interval arms (the historical behavior).
    mocks.box.connection = "closed";
    mocks.adminQuery.mockResolvedValue([]);

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

describe("useLiveTable stream-driven refresh (ARC-123)", () => {
  beforeEach(() => {
    mocks.adminQuery.mockReset();
    mocks.box.method = "adminkey";
    mocks.box.connection = "connected";
    mocks.box.ops = [];
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("does not arm the 2s poll while the stream is connected", async () => {
    vi.useFakeTimers();
    mocks.adminQuery.mockResolvedValue([]);

    const { unmount } = renderHook(() => useLiveTable("db1", "items", "asc", 10));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
    // Exactly one call — the mount-time poll. The fallback interval must NOT be
    // armed while connected, so advancing well past 2s adds no further calls.
    const mountCalls = mocks.adminQuery.mock.calls.length;
    expect(mountCalls).toBeGreaterThanOrEqual(1);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(mocks.adminQuery.mock.calls.length).toBe(mountCalls);
    unmount();
  });

  it("refreshes when a stream op matches the viewed {db, table}", async () => {
    mocks.adminQuery.mockResolvedValue([]);

    const { rerender } = renderHook(() => useLiveTable("db1", "items", "asc", 10));
    // Let the mount-time poll flush.
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    const mountCalls = mocks.adminQuery.mock.calls.length;
    expect(mountCalls).toBeGreaterThanOrEqual(1);

    // Seed the high-water mark with a non-matching op (the first sighting marks
    // the watermark without refreshing).
    mocks.box.ops = [{ db: "other", table: "x", kind: "insert", ts: 1 }];
    await act(async () => {
      rerender();
    });
    expect(mocks.adminQuery.mock.calls.length).toBe(mountCalls);

    // A matching op arrives → one extra refresh.
    mocks.box.ops = [
      { db: "db1", table: "items", kind: "insert", ts: 2 },
      { db: "other", table: "x", kind: "insert", ts: 1 },
    ];
    await act(async () => {
      rerender();
    });
    expect(mocks.adminQuery.mock.calls.length).toBe(mountCalls + 1);

    // A non-matching op arrives → no extra refresh.
    mocks.box.ops = [
      { db: "other", table: "y", kind: "patch", ts: 3 },
      { db: "db1", table: "items", kind: "insert", ts: 2 },
      { db: "other", table: "x", kind: "insert", ts: 1 },
    ];
    await act(async () => {
      rerender();
    });
    expect(mocks.adminQuery.mock.calls.length).toBe(mountCalls + 1);
  });
});
