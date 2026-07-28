import { describe, expect, it } from "vitest";
import {
  DT_MAX,
  DT_MIN,
  formatRate,
  lastValue,
  levelSeries,
  MAX_SAMPLES,
  nearestIndex,
  rateSeries,
  type Sample,
} from "./metrics-series";
import type { MetricsSnapshot } from "./types";

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

// samples at t0=0ms, t1=1000ms, ... (1s apart)
const mk = (counts: number[]): Sample[] =>
  counts.map((c, i) => ({ t: i * 1000, snap: snap({ queriesTotal: c }) }));

describe("rateSeries", () => {
  it("derives per-second rate between consecutive counters", () => {
    const r = rateSeries(mk([100, 112, 125]), (s) => s.queriesTotal);
    expect(r[0]).toBeNull(); // needs a predecessor
    expect(r[1]).toBe(12); // (112-100)/1s
    expect(r[2]).toBe(13); // (125-112)/1s
  });

  it("emits a gap (null) on counter reset (server restart)", () => {
    const r = rateSeries(mk([100, 120, 5, 10]), (s) => s.queriesTotal);
    expect(r).toEqual([null, 20, null, 5]);
  });

  it("clamps dt into [DT_MIN, DT_MAX] so jitter can't spike the rate", () => {
    // 10 queries in 0.1s (100ms) -> raw 100/s, clamped dt=0.5 -> 20/s
    const samples = [
      { t: 0, snap: snap({ queriesTotal: 0 }) },
      { t: 100, snap: snap({ queriesTotal: 10 }) },
    ];
    expect(rateSeries(samples, (s) => s.queriesTotal)[1]).toBe(10 / DT_MIN);
  });

  it("emits a gap when dt exceeds DT_MAX (reconnect / tab throttle)", () => {
    const samples = [
      { t: 0, snap: snap({ queriesTotal: 0 }) },
      { t: (DT_MAX + 1) * 1000, snap: snap({ queriesTotal: 30 }) },
    ];
    expect(rateSeries(samples, (s) => s.queriesTotal)[1]).toBeNull();
  });

  it("returns all-null for empty or single-sample windows", () => {
    expect(rateSeries([], (s) => s.queriesTotal)).toEqual([]);
    expect(rateSeries(mk([5]), (s) => s.queriesTotal)).toEqual([null]);
  });
});

describe("levelSeries", () => {
  it("projects a gauge value per sample, null-padded to the window length", () => {
    const samples = mk([0, 0, 0]);
    samples[0].snap = snap({ wsConnections: 3 });
    samples[1].snap = snap({ wsConnections: 7 });
    samples[2].snap = snap({ wsConnections: 4 });
    expect(levelSeries(samples, (s) => s.wsConnections)).toEqual([3, 7, 4]);
    expect(levelSeries([], (s) => s.wsConnections)).toEqual([]);
  });
});

describe("lastValue", () => {
  it("returns the last non-null point", () => {
    expect(lastValue([null, 5, null, 9])).toBe(9);
    expect(lastValue([null, null])).toBeNull();
    expect(lastValue([])).toBeNull();
  });
});

describe("nearestIndex", () => {
  it("maps a 0..1 fraction to the nearest point index", () => {
    expect(nearestIndex(0, 5)).toBe(0);
    expect(nearestIndex(1, 5)).toBe(4);
    expect(nearestIndex(0.5, 5)).toBe(2);
  });
  it("clamps out-of-range fractions", () => {
    expect(nearestIndex(-0.5, 5)).toBe(0);
    expect(nearestIndex(2, 5)).toBe(4);
  });
  it("returns 0 for a single-point series", () => {
    expect(nearestIndex(0.5, 1)).toBe(0);
  });
});

describe("formatRate", () => {
  it("shows one decimal under 100/s and rounds above", () => {
    expect(formatRate(12.4)).toBe("12.4/s");
    expect(formatRate(0.2)).toBe("0.2/s");
    expect(formatRate(234.7)).toBe("235/s");
  });
  it("returns an em dash for non-finite input", () => {
    expect(formatRate(Number.NaN)).toBe("—");
  });
});

describe("constants", () => {
  it("MAX_SAMPLES holds 60s + the current point", () => {
    expect(MAX_SAMPLES).toBe(61);
  });
});
