import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { MetricsSnapshot } from "./types";

const mocks = vi.hoisted(() => ({
  // mutable so each test can drive a different stream of snapshots
  current: null as MetricsSnapshot | null,
}));

vi.mock("./admin", () => ({
  useAdmin: () => ({ metrics: mocks.current }),
}));

import { useMetricsHistory } from "./useMetricsHistory";

const snap = (over: Partial<MetricsSnapshot>): MetricsSnapshot => ({
  queriesTotal: 0,
  mutationsTotal: 0,
  uploadsTotal: 0,
  wsConnections: 0,
  activeSubscriptions: 0,
  poolSize: 0,
  poolIdle: 0,
  uptimeSeconds: 0,
  ...over,
});

describe("useMetricsHistory", () => {
  beforeEach(() => {
    mocks.current = null;
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("accumulates a sample per new snapshot, stamped with receive-time", () => {
    vi.useFakeTimers();
    const { result, rerender } = renderHook(() => useMetricsHistory());

    expect(result.current.samples).toEqual([]);

    vi.setSystemTime(1_000);
    mocks.current = snap({ wsConnections: 3 });
    rerender();
    expect(result.current.samples).toHaveLength(1);
    expect(result.current.samples[0].t).toBe(1_000);
    expect(result.current.samples[0].snap.wsConnections).toBe(3);

    vi.setSystemTime(2_000);
    mocks.current = snap({ wsConnections: 7 });
    rerender();
    expect(result.current.samples).toHaveLength(2);
    expect(result.current.samples[1].snap.wsConnections).toBe(7);
  });

  it("caps the buffer at maxSamples (FIFO)", () => {
    vi.useFakeTimers();
    const { result, rerender } = renderHook(() => useMetricsHistory(3));
    for (let i = 1; i <= 5; i++) {
      vi.setSystemTime(i * 1000);
      mocks.current = snap({ wsConnections: i });
      rerender();
    }
    expect(result.current.samples).toHaveLength(3);
    expect(result.current.samples[0].snap.wsConnections).toBe(3); // oldest kept
    expect(result.current.samples[2].snap.wsConnections).toBe(5); // newest
  });

  it("ignores null metrics and does not accumulate", () => {
    vi.useFakeTimers();
    const { result, rerender } = renderHook(() => useMetricsHistory());
    vi.setSystemTime(1_000);
    mocks.current = snap({ wsConnections: 3 });
    rerender();
    mocks.current = null; // disconnect
    rerender();
    expect(result.current.samples).toHaveLength(1);
  });
});
