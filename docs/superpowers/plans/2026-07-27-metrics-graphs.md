# Metrics Trend Graphs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add live 60-second trend sparklines to the dashboard `/metrics` page, fed by the existing 1 Hz `gauges` snapshot stream, with zero new dependencies.

**Architecture:** A `useMetricsHistory()` hook accumulates a rolling ring buffer of received `MetricsSnapshot` samples (the server pushes one per second over `/admin/stream`). Pure functions in `metrics-series.ts` derive per-second rates from cumulative counters and project gauge levels, handling counter resets and reconnect gaps. A hand-rolled `Sparkline` SVG component renders a line + soft area, breaking across gaps. `MetricsPage` wires a sparkline into each instrument tile.

**Tech Stack:** React 19, TypeScript, Vite, vitest + @testing-library/react, biome. No new packages.

**Spec:** `docs/superpowers/specs/2026-07-27-metrics-graphs-design.md`

## Global Constraints

- **Dashboard only.** Touch no server, client SDK, or other dashboard page. `AdminProvider` is unchanged.
- **Zero new dependencies.** Do not add anything to `dashboard/package.json`.
- **Match the design system.** Use tokens from `dashboard/src/styles/tokens.css` (`--accent #3dd68c`, `--accent-soft`, `--ink`/`--ink-2`/`--ink-3`, `--mono`, spacing `--sp-*`). Monospace, tabular numerics, squared radii.
- **Wire contract.** `MetricsSnapshot` field names are camelCase, defined in `dashboard/src/lib/types.ts` — do not rename.
- **biome rules.** Double quotes, 2-space indent, `lineWidth: 100`, `useImportType: warn` (use `import type` for type-only imports), `organizeImports: on`, no unused vars, **no array-index React keys** (key SVG elements by their data string).
- **The 1 Hz cadence is fixed.** Server pushes a snapshot every 1 s (`server/src/admin.rs:864`). The client stamps receive-time; it does not read a server timestamp.
- **StrictMode is on** (`dashboard/src/main.tsx`) — effects double-invoke on mount in dev; the hook dedupes by snapshot-ref identity.
- **Gate.** `make checkall` must pass before any commit. Dashboard typecheck resolves `@par-rt-db/client` from `ts-client/dist` — run `make ts-client-build` first on a fresh worktree (no gitignored `node_modules` carries over).

## File Structure

**New files (each one responsibility):**

| File | Responsibility |
|---|---|
| `dashboard/src/lib/metrics-series.ts` | Pure derivation: types `Sample`/`Point`, constants, `rateSeries`, `levelSeries`, `lastValue`, `nearestIndex`, `formatRate`. Zero React. |
| `dashboard/src/lib/metrics-series.test.ts` | Unit tests for everything in `metrics-series.ts`. |
| `dashboard/src/lib/useMetricsHistory.ts` | The hook: accumulates received snapshots into a capped ring buffer. |
| `dashboard/src/lib/useMetricsHistory.test.ts` | Hook tests (fake timers + mutable `useAdmin` mock). |
| `dashboard/src/components/Sparkline.tsx` | Pure SVG sparkline + built-in hover layer. |
| `dashboard/src/components/Sparkline.test.tsx` | Render + interaction tests. |
| `dashboard/src/pages/MetricsPage.test.tsx` | Page smoke test. |

**Edited files (surgical):**

| File | Change |
|---|---|
| `dashboard/src/pages/MetricsPage.tsx` | Add `sparkline` slot to `Instrument`; mount the hook; derive series; render sparklines. |
| `dashboard/src/pages/MetricsPage.module.css` | Sparkline wrapper + tile layout tweaks. |

---

## Task 1: Pure series derivation + formatting

**Files:**
- Create: `dashboard/src/lib/metrics-series.ts`
- Test: `dashboard/src/lib/metrics-series.test.ts`

**Interfaces:**
- Consumes: `MetricsSnapshot` type from `./types`.
- Produces (used by Tasks 2, 4, 5):
  - `export type Point = number | null`
  - `export interface Sample { t: number; snap: MetricsSnapshot }`
  - `export const MAX_SAMPLES = 61`
  - `export const DT_MIN = 0.5`, `export const DT_MAX = 5`
  - `export function rateSeries(samples: Sample[], counter: (s: MetricsSnapshot) => number): Point[]`
  - `export function levelSeries(samples: Sample[], gauge: (s: MetricsSnapshot) => number): Point[]`
  - `export function lastValue(points: Point[]): number | null`
  - `export function nearestIndex(fraction: number, n: number): number` (Task 4 uses it; define + test now)
  - `export function formatRate(perSecond: number): string`

- [ ] **Step 1: Write the failing tests**

Create `dashboard/src/lib/metrics-series.test.ts`:

```ts
import { describe, expect, it } from "vitest";
import type { MetricsSnapshot } from "./types";
import {
  DT_MAX,
  DT_MIN,
  MAX_SAMPLES,
  type Sample,
  formatRate,
  lastValue,
  levelSeries,
  nearestIndex,
  rateSeries,
} from "./metrics-series";

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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd dashboard && bunx vitest run src/lib/metrics-series.test.ts
```
Expected: FAIL — module `./metrics-series` not found.

- [ ] **Step 3: Write the implementation**

Create `dashboard/src/lib/metrics-series.ts`:

```ts
import type { MetricsSnapshot } from "./types";

/** A point in a plotted series; `null` means "no value / gap" (break the line). */
export type Point = number | null;

/** One received snapshot, stamped with client receive-time (epoch ms). */
export interface Sample {
  t: number;
  snap: MetricsSnapshot;
}

/** Rolling window length: 60 s of history at 1 Hz, plus the current point. */
export const MAX_SAMPLES = 61;

/** Rate-derivation dt clamp bounds, in seconds. */
export const DT_MIN = 0.5;
export const DT_MAX = 5;

/**
 * Derive per-second rates from a cumulative counter across the sample window.
 * Index 0 is always null (a rate needs two samples). Counter resets (server
 * restart) and reconnect gaps (dt > DT_MAX) emit null; dt is clamped to
 * [DT_MIN, DT_MAX] so network jitter can't spike the rate.
 */
export function rateSeries(
  samples: Sample[],
  counter: (s: MetricsSnapshot) => number,
): Point[] {
  const out: Point[] = new Array(samples.length).fill(null);
  for (let i = 1; i < samples.length; i++) {
    const dtMs = samples[i].t - samples[i - 1].t;
    const dt = dtMs / 1000;
    if (dt > DT_MAX) continue; // gap — leave null
    const prev = counter(samples[i - 1].snap);
    const curr = counter(samples[i].snap);
    if (curr < prev) continue; // counter reset — leave null, resume next step
    const clamped = Math.min(Math.max(dt, DT_MIN), DT_MAX);
    out[i] = (curr - prev) / clamped;
  }
  return out;
}

/** Project a gauge's instantaneous value per sample (no derivation). */
export function levelSeries(
  samples: Sample[],
  gauge: (s: MetricsSnapshot) => number,
): Point[] {
  return samples.map((s) => gauge(s.snap));
}

/** The last non-null point, or null if there is none. */
export function lastValue(points: Point[]): number | null {
  for (let i = points.length - 1; i >= 0; i--) {
    if (points[i] != null) return points[i];
  }
  return null;
}

/**
 * Map a horizontal fraction (0..1 across the plot) to the nearest point index.
 * Used by the Sparkline hover layer; pure so it is unit-testable without layout.
 */
export function nearestIndex(fraction: number, n: number): number {
  if (n <= 1) return 0;
  const i = Math.round(fraction * (n - 1));
  return Math.min(Math.max(i, 0), n - 1);
}

/** Format a per-second rate for display (one decimal under 100/s, else rounded). */
export function formatRate(perSecond: number): string {
  if (!Number.isFinite(perSecond)) return "—";
  const text = perSecond >= 100 ? String(Math.round(perSecond)) : perSecond.toFixed(1);
  return `${text}/s`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd dashboard && bunx vitest run src/lib/metrics-series.test.ts
```
Expected: PASS (all tests green).

- [ ] **Step 5: Lint + typecheck the new files**

```bash
cd dashboard && bunx biome check src/lib/metrics-series.ts src/lib/metrics-series.test.ts && bunx tsc --noEmit
```
Expected: no errors. (Run `make ts-client-build` first if `tsc` fails resolving `@par-rt-db/client`.)

- [ ] **Step 6: Commit**

```bash
git add dashboard/src/lib/metrics-series.ts dashboard/src/lib/metrics-series.test.ts
git commit -m "feat(dashboard): pure metrics series derivation (rates, levels, gaps)"
```

---

## Task 2: The rolling-history hook

**Files:**
- Create: `dashboard/src/lib/useMetricsHistory.ts`
- Test: `dashboard/src/lib/useMetricsHistory.test.ts`

**Interfaces:**
- Consumes: `Sample`, `MAX_SAMPLES` from `./metrics-series`; `MetricsSnapshot` from `./types`; `useAdmin` from `./admin` (returns `{ metrics: MetricsSnapshot | null }`).
- Produces: `export function useMetricsHistory(maxSamples?: number): { samples: Sample[] }` (used by Task 5).

- [ ] **Step 1: Write the failing test**

Create `dashboard/src/lib/useMetricsHistory.test.ts`. Pattern follows `useLiveTable.test.tsx`: a `vi.hoisted` mutable box backs the `useAdmin` mock; fake timers control `Date.now()`.

```ts
import { act, renderHook } from "@testing-library/react";
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
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd dashboard && bunx vitest run src/lib/useMetricsHistory.test.ts
```
Expected: FAIL — module `./useMetricsHistory` not found.

- [ ] **Step 3: Write the implementation**

Create `dashboard/src/lib/useMetricsHistory.ts`:

```ts
import { useEffect, useRef, useState } from "react";
import { useAdmin } from "./admin";
import { type Sample, MAX_SAMPLES } from "./metrics-series";
import type { MetricsSnapshot } from "./types";

/**
 * Accumulates the /admin/stream gauge snapshots (one per second) into a rolling
 * ring buffer of the most recent `maxSamples` samples, each stamped with its
 * client receive-time. Only the `/metrics` page mounts this, localizing the 1 Hz
 * re-render churn. `AdminProvider` is untouched.
 *
 * StrictMode double-invokes the effect on mount; the `lastRef` guard dedupes by
 * snapshot-ref identity so a snapshot is never recorded twice.
 */
export function useMetricsHistory(maxSamples = MAX_SAMPLES): { samples: Sample[] } {
  const { metrics } = useAdmin();
  const bufRef = useRef<Sample[]>([]);
  const lastRef = useRef<MetricsSnapshot | null>(null);
  const [samples, setSamples] = useState<Sample[]>([]);

  useEffect(() => {
    if (!metrics || metrics === lastRef.current) return;
    lastRef.current = metrics;
    const buf = bufRef.current;
    buf.push({ t: Date.now(), snap: metrics });
    while (buf.length > maxSamples) buf.shift();
    setSamples([...buf]);
  }, [metrics, maxSamples]);

  return { samples };
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd dashboard && bunx vitest run src/lib/useMetricsHistory.test.ts
```
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

```bash
cd dashboard && bunx biome check src/lib/useMetricsHistory.ts src/lib/useMetricsHistory.test.ts && bunx tsc --noEmit
```
Expected: no errors. (Note: biome's `useExhaustiveDependencies` — the effect deps `[metrics, maxSamples]` cover every reactive value read; `bufRef`/`lastRef`/`setSamples` are stable refs/setters and intentionally excluded.)

- [ ] **Step 6: Commit**

```bash
git add dashboard/src/lib/useMetricsHistory.ts dashboard/src/lib/useMetricsHistory.test.ts
git commit -m "feat(dashboard): useMetricsHistory rolling snapshot buffer"
```

---

## Task 3: Sparkline component (pure rendering)

**Files:**
- Create: `dashboard/src/components/Sparkline.tsx`
- Test: `dashboard/src/components/Sparkline.test.tsx`

**Interfaces:**
- Consumes: `Point` from `../lib/metrics-series`.
- Produces: `export function Sparkline(props: SparklineProps): ReactElement` (used by Task 5). Props:
  - `values: Point[]`
  - `stroke?: string` (default `"var(--accent)"`)
  - `fill?: string` (default `"var(--accent-soft)"`; pass `"none"` for no area)
  - `height?: number` (default `40`)
  - `min?: number`, `max?: number` (optional fixed scale; else autoscale to the non-null range)
  - `ariaLabel: string` (required)

This task ships **rendering only** (no hover). Task 4 adds the hover layer inside the same component.

- [ ] **Step 1: Write the failing test**

Create `dashboard/src/components/Sparkline.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Sparkline } from "./Sparkline";

describe("Sparkline", () => {
  it("renders a polyline + area for valid input and exposes an a11y label", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3, 2]} ariaLabel="queries per second over 60s" />,
    );
    const svg = screen.getByRole("img");
    expect(svg).toHaveAttribute("aria-label", "queries per second over 60s");
    expect(container.querySelectorAll("polyline").length).toBeGreaterThanOrEqual(1);
    expect(container.querySelector("path")).toBeTruthy(); // area fill
  });

  it("renders an empty track (no polyline, no crash) for an all-null series", () => {
    const { container } = render(<Sparkline values={[null, null]} ariaLabel="empty" />);
    expect(screen.getByRole("img")).toBeInTheDocument();
    expect(container.querySelector("polyline")).toBeNull();
  });

  it("breaks the line across a null gap (two polyline segments)", () => {
    const { container } = render(
      <Sparkline values={[1, 2, null, 4, 5]} ariaLabel="gapped" />,
    );
    expect(container.querySelectorAll("polyline").length).toBe(2);
  });

  it("respects a fixed min/max scale", () => {
    const { container } = render(
      <Sparkline values={[0, 10]} min={0} max={10} ariaLabel="scaled" />,
    );
    // the polyline exists; exact coords are covered by the geometry being deterministic
    expect(container.querySelector("polyline")).toBeTruthy();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd dashboard && bunx vitest run src/components/Sparkline.test.tsx
```
Expected: FAIL — module `./Sparkline` not found.

- [ ] **Step 3: Write the implementation**

Create `dashboard/src/components/Sparkline.tsx`. (A `children` slot is accepted so Task 4 can overlay the hover crosshair without restructuring.)

```tsx
import type { ReactNode } from "react";
import { type Point } from "../lib/metrics-series";

const W = 100; // viewBox width; the svg stretches to its container via width:100%

export interface SparklineProps {
  values: Point[];
  stroke?: string; // default var(--accent)
  fill?: string; // default var(--accent-soft); "none" disables the area
  height?: number; // default 40 (px)
  min?: number; // optional fixed floor; else autoscale
  max?: number; // optional fixed ceiling; else autoscale
  ariaLabel: string;
  children?: ReactNode; // overlay slot (hover crosshair — Task 4)
}

export function Sparkline({
  values,
  stroke = "var(--accent)",
  fill = "var(--accent-soft)",
  height = 40,
  min,
  max,
  ariaLabel,
  children,
}: SparklineProps) {
  const n = values.length;
  const finite = values.filter((v): v is number => v != null && Number.isFinite(v));
  const lo = min ?? (finite.length ? Math.min(...finite) : 0);
  const hi = max ?? (finite.length ? Math.max(...finite) : 1);
  const span = hi - lo || 1;

  const x = (i: number) => (n <= 1 ? 0 : (i / (n - 1)) * W);
  const y = (v: number) => height - ((v - lo) / span) * height;

  // Split into contiguous non-null runs: each becomes one polyline, and the line
  // breaks (move) across nulls. Collect runs for area fill as well.
  const runs: string[][] = [];
  let cur: string[] = [];
  for (let i = 0; i < n; i++) {
    const v = values[i];
    if (v == null || !Number.isFinite(v)) {
      if (cur.length) {
        runs.push(cur);
        cur = [];
      }
      continue;
    }
    cur.push(`${x(i).toFixed(2)},${y(v).toFixed(2)}`);
  }
  if (cur.length) runs.push(cur);

  const baseline = height;
  const linePoints = runs.map((run) => run.join(" "));
  const areaPaths = runs.map((run) => {
    const first = run[0].split(",");
    const last = run[run.length - 1].split(",");
    return `M${first[0]},${baseline} L${run.join(" ")} L${last[0]},${baseline} Z`;
  });

  // Last non-null point gets a rounded dot anchored at the right edge.
  let last: { cx: number; cy: number } | null = null;
  for (let i = n - 1; i >= 0; i--) {
    const v = values[i];
    if (v != null && Number.isFinite(v)) {
      last = { cx: x(i), cy: y(v) };
      break;
    }
  }

  return (
    <svg
      viewBox={`0 0 ${W} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={ariaLabel}
      style={{ width: "100%", height, display: "block" }}
    >
      {fill !== "none" &&
        areaPaths.map((d) => <path key={d} d={d} style={{ fill }} stroke="none" />)}
      {linePoints.map((pts) => (
        <polyline
          key={pts}
          points={pts}
          fill="none"
          style={{ stroke, strokeWidth: 2, vectorEffect: "non-scaling-stroke" }}
        />
      ))}
      {last && (
        <circle
          cx={last.cx}
          cy={last.cy}
          r={2}
          style={{ fill: stroke, vectorEffect: "non-scaling-stroke" }}
        />
      )}
      {children}
    </svg>
  );
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
cd dashboard && bunx vitest run src/components/Sparkline.test.tsx
```
Expected: PASS.

- [ ] **Step 5: Lint + typecheck**

```bash
cd dashboard && bunx biome check src/components/Sparkline.tsx src/components/Sparkline.test.tsx && bunx tsc --noEmit
```
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add dashboard/src/components/Sparkline.tsx dashboard/src/components/Sparkline.test.tsx
git commit -m "feat(dashboard): Sparkline SVG component (line + area, gap-aware)"
```

---

## Task 4: Hover layer inside Sparkline

**Files:**
- Modify: `dashboard/src/components/Sparkline.tsx`
- Test: `dashboard/src/components/Sparkline.test.tsx` (extend), `dashboard/src/lib/metrics-series.test.ts` (already covers `nearestIndex` from Task 1)

**Interfaces:**
- Consumes: `nearestIndex` from `../lib/metrics-series` (defined + tested in Task 1).
- Produces: `Sparkline` gains optional `interactive?: boolean` (default `true`) and `formatTip?: (v: number) => string`; when interactive and the pointer moves over the plot, a crosshair line + a value tooltip appear.

- [ ] **Step 1: Write the failing test (append to `Sparkline.test.tsx`)**

```tsx
import { fireEvent } from "@testing-library/react";
// (existing imports above; add fireEvent to the existing import from "@testing-library/react")

describe("Sparkline hover", () => {
  it("shows a tooltip with the hovered value on pointer move", () => {
    const { container } = render(
      <Sparkline values={[10, 20, 30]} formatTip={(v) => `${v}/s`} ariaLabel="rates" />,
    );
    const svg = container.querySelector("svg")!;
    // jsdom returns a zero-size rect by default; give it a real width so the
    // pointer-fraction → index math resolves.
    vi.spyOn(svg, "getBoundingClientRect").mockReturnValue({
      left: 0,
      width: 100,
      right: 100,
      top: 0,
      bottom: 40,
      height: 40,
      x: 0,
      y: 0,
      toJSON() {},
    } as DOMRect);

    expect(container.querySelector("[data-spark-tip]")).toBeNull();
    fireEvent.mouseMove(svg, { clientX: 100 }); // far right -> last point (30)
    const tip = container.querySelector("[data-spark-tip]");
    expect(tip).toBeTruthy();
    expect(tip!.textContent).toContain("30/s");
  });

  it("can be disabled via interactive={false}", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3]} interactive={false} ariaLabel="static" />,
    );
    const svg = container.querySelector("svg")!;
    fireEvent.mouseMove(svg, { clientX: 50 });
    expect(container.querySelector("[data-spark-tip]")).toBeNull();
  });
});
```

Add `vi` to the test file's `vitest` import (it currently imports `{ describe, expect, it }`).

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd dashboard && bunx vitest run src/components/Sparkline.test.tsx
```
Expected: FAIL — no `interactive`/`formatTip` props, no tooltip rendered.

- [ ] **Step 3: Implement the hover layer**

Modify `dashboard/src/components/Sparkline.tsx`:

1. Add imports at the top:
```tsx
import { useRef, useState } from "react";
import { nearestIndex, type Point } from "../lib/metrics-series";
```
(remove the old `import { type Point } from "../lib/metrics-series";` line — the new import replaces it)

2. Extend `SparklineProps` with two optional fields:
```tsx
  interactive?: boolean; // default true — crosshair + tooltip
  formatTip?: (v: number) => string; // default: String(value)
```

3. Inside the component, after computing `runs`/`last`, add hover state + handlers:
```tsx
  const svgRef = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<number | null>(null);
  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!interactive) return;
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return;
    const fraction = (e.clientX - rect.left) / rect.width;
    setHover(nearestIndex(fraction, n));
  };
  const onLeave = () => setHover(null);

  const hoverValue =
    hover != null && values[hover] != null && Number.isFinite(values[hover] as number)
      ? (values[hover] as number)
      : null;
  const tipText =
    hoverValue != null ? (formatTip ? formatTip(hoverValue) : String(hoverValue)) : null;
```

4. Wire them onto the `<svg>` and render the overlay (crosshair line + tooltip) via the `children` pattern — but since the component owns it, render directly inside the svg and add an HTML tooltip in a wrapping relative container. Replace the `return (...)` block with:

```tsx
  return (
    <div style={{ position: "relative", width: "100%" }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={ariaLabel}
        onMouseMove={onMove}
        onMouseLeave={onLeave}
        style={{ width: "100%", height, display: "block" }}
      >
        {fill !== "none" &&
          areaPaths.map((d) => <path key={d} d={d} style={{ fill }} stroke="none" />)}
        {linePoints.map((pts) => (
          <polyline
            key={pts}
            points={pts}
            fill="none"
            style={{ stroke, strokeWidth: 2, vectorEffect: "non-scaling-stroke" }}
          />
        ))}
        {last && (
          <circle
            cx={last.cx}
            cy={last.cy}
            r={2}
            style={{ fill: stroke, vectorEffect: "non-scaling-stroke" }}
          />
        )}
        {hover != null && (
          <line
            x1={x(hover)}
            y1={0}
            x2={x(hover)}
            y2={height}
            style={{ stroke: "var(--rule-strong)", strokeWidth: 1, vectorEffect: "non-scaling-stroke" }}
          />
        )}
        {children}
      </svg>
      {tipText != null && hover != null && (
        <span
          data-spark-tip
          style={{
            position: "absolute",
            left: `${(hover / Math.max(n - 1, 1)) * 100}%`,
            top: 0,
            transform: "translateX(-50%)",
            padding: "1px 4px",
            fontFamily: "var(--mono)",
            fontSize: "var(--t-mono-xs)",
            color: "var(--ink)",
            background: "var(--inset)",
            border: "1px solid var(--rule)",
            whiteSpace: "nowrap",
            pointerEvents: "none",
          }}
        >
          {tipText}
        </span>
      )}
    </div>
  );
```

Destructure `interactive = true` and `formatTip` in the component params alongside the existing props.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd dashboard && bunx vitest run src/components/Sparkline.test.tsx
```
Expected: PASS (all Sparkline tests, old and new).

- [ ] **Step 5: Lint + typecheck**

```bash
cd dashboard && bunx biome check src/components/Sparkline.tsx src/components/Sparkline.test.tsx && bunx tsc --noEmit
```
Expected: no errors. (If biome flags the inline `style` objects under `noImportantStyles` — that rule is `off` in this config, so inline styles are fine.)

- [ ] **Step 6: Commit**

```bash
git add dashboard/src/components/Sparkline.tsx dashboard/src/components/Sparkline.test.tsx
git commit -m "feat(dashboard): Sparkline hover crosshair + tooltip"
```

---

## Task 5: Wire sparklines into MetricsPage

**Files:**
- Modify: `dashboard/src/pages/MetricsPage.tsx`
- Modify: `dashboard/src/pages/MetricsPage.module.css`
- Create: `dashboard/src/pages/MetricsPage.test.tsx`

**Interfaces:**
- Consumes: `useMetricsHistory` from `../lib/useMetricsHistory`; `rateSeries`, `levelSeries`, `lastValue`, `formatRate`, `Point` from `../lib/metrics-series`; `Sparkline` from `../components/Sparkline`; existing `MetricsSnapshot` from `../lib/types`; `formatNumber` from `../lib/format`.
- Produces: the enriched `/metrics` page.

- [ ] **Step 1: Write the failing page test**

Create `dashboard/src/pages/MetricsPage.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { MetricsSnapshot } from "../lib/types";

const snap: MetricsSnapshot = {
  queriesTotal: 423_901,
  mutationsTotal: 1_204,
  uploadsTotal: 7,
  wsConnections: 42,
  activeSubscriptions: 118,
  poolSize: 10,
  poolIdle: 4,
  uptimeSeconds: 3_600,
};

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ metrics: snap }),
}));

import { MetricsPage } from "./MetricsPage";

describe("MetricsPage", () => {
  it("renders the heading and instrument labels", () => {
    render(<MetricsPage />);
    expect(screen.getByText("Live instruments")).toBeInTheDocument();
    expect(screen.getByText("queries")).toBeInTheDocument();
    expect(screen.getByText("subscriptions")).toBeInTheDocument();
  });

  it("renders a sparkline per metric (role=img)", () => {
    const { container } = render(<MetricsPage />);
    expect(container.querySelectorAll("svg[role='img']").length).toBeGreaterThanOrEqual(1);
  });

  it("shows cumulative totals as sub-lines", () => {
    render(<MetricsPage />);
    expect(screen.getByText(/423,901 total/i)).toBeInTheDocument();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
cd dashboard && bunx vitest run src/pages/MetricsPage.test.tsx
```
Expected: FAIL — no sparkline `svg[role='img']`, and no "total" sub-line yet.

- [ ] **Step 3: Update the page**

Modify `dashboard/src/pages/MetricsPage.tsx`. Replace the full file contents with:

```tsx
import type { ReactNode } from "react";
import { Placard, Spinner, StatusLamp } from "../components/ui";
import { Sparkline } from "../components/Sparkline";
import { useAdmin } from "../lib/admin";
import { formatDuration, formatNumber } from "../lib/format";
import {
  type Point,
  formatRate,
  lastValue,
  levelSeries,
  rateSeries,
} from "../lib/metrics-series";
import { useMetricsHistory } from "../lib/useMetricsHistory";
import type { MetricsSnapshot } from "../lib/types";
import s from "./MetricsPage.module.css";

function Instrument({
  label,
  value,
  sub,
  sparkline,
}: {
  label: string;
  value: string;
  sub?: string;
  sparkline?: ReactNode;
}) {
  return (
    <div className={s.instrument}>
      <span className={s.instrumentLabel}>{label}</span>
      <span className={s.instrumentValue}>{value}</span>
      {sub && <span className={s.instrumentSub}>{sub}</span>}
      {sparkline && <div className={s.spark}>{sparkline}</div>}
    </div>
  );
}

function Panel({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className={s.panel}>
      <Placard>{title}</Placard>
      <div className={s.grid}>{children}</div>
    </section>
  );
}

export function MetricsPage() {
  const { metrics } = useAdmin();
  const { samples } = useMetricsHistory();
  if (!metrics) return <Spinner label="reading instruments" />;
  const m: MetricsSnapshot = metrics;
  const poolInUse = Math.max(0, m.poolSize - m.poolIdle);

  const qRate: Point[] = rateSeries(samples, (s) => s.queriesTotal);
  const mRate: Point[] = rateSeries(samples, (s) => s.mutationsTotal);
  const uRate: Point[] = rateSeries(samples, (s) => s.uploadsTotal);
  const wsLevel: Point[] = levelSeries(samples, (s) => s.wsConnections);
  const subsLevel: Point[] = levelSeries(samples, (s) => s.activeSubscriptions);
  const poolBusy: Point[] = levelSeries(samples, (s) => s.poolSize - s.poolIdle);

  return (
    <section className={s.page}>
      <div className={s.head}>
        <h1 className={s.title}>Live instruments</h1>
        <StatusLamp status="ok" label="live · 1s" />
      </div>

      <Panel title="Activity · since start">
        <Instrument
          label="queries"
          value={formatRate(lastValue(qRate) ?? Number.NaN)}
          sub={`${formatNumber(m.queriesTotal)} total`}
          sparkline={
            <Sparkline values={qRate} ariaLabel="queries per second over the last minute" />
          }
        />
        <Instrument
          label="mutations"
          value={formatRate(lastValue(mRate) ?? Number.NaN)}
          sub={`${formatNumber(m.mutationsTotal)} total`}
          sparkline={
            <Sparkline values={mRate} ariaLabel="mutations per second over the last minute" />
          }
        />
        <Instrument
          label="uploads"
          value={formatRate(lastValue(uRate) ?? Number.NaN)}
          sub={`${formatNumber(m.uploadsTotal)} total`}
          sparkline={
            <Sparkline values={uRate} ariaLabel="uploads per second over the last minute" />
          }
        />
      </Panel>

      <Panel title="Live">
        <Instrument
          label="ws connections"
          value={formatNumber(m.wsConnections)}
          sparkline={
            <Sparkline values={wsLevel} ariaLabel="open websocket connections over the last minute" />
          }
        />
        <Instrument
          label="subscriptions"
          value={formatNumber(m.activeSubscriptions)}
          sparkline={
            <Sparkline values={subsLevel} ariaLabel="active subscriptions over the last minute" />
          }
        />
        <Instrument
          label="pool"
          value={formatNumber(m.poolSize)}
          sub={`${formatNumber(poolInUse)} busy · ${formatNumber(m.poolIdle)} idle`}
          sparkline={
            <Sparkline
              values={poolBusy}
              min={0}
              max={m.poolSize || 1}
              ariaLabel="busy connections out of pool size over the last minute"
            />
          }
        />
      </Panel>

      <Panel title="System">
        <Instrument label="uptime" value={formatDuration(m.uptimeSeconds)} />
      </Panel>
    </section>
  );
}
```

- [ ] **Step 4: Add the sparkline styles**

Append to `dashboard/src/pages/MetricsPage.module.css`:

```css
.spark {
  margin-top: var(--sp-2);
  width: 100%;
  opacity: 0.95;
}
```

And raise the instrument's `min-height` so the sparkline has room — change the existing `.instrument` rule's `min-height: 88px;` to `min-height: 128px;`.

- [ ] **Step 5: Run the page test to verify it passes**

```bash
cd dashboard && bunx vitest run src/pages/MetricsPage.test.tsx
```
Expected: PASS.

- [ ] **Step 6: Lint + typecheck**

```bash
cd dashboard && bunx biome check src/pages/MetricsPage.tsx src/pages/MetricsPage.test.tsx && bunx tsc --noEmit
```
Expected: no errors.

- [ ] **Step 7: Run the full gate**

From the repo root (the worktree root):
```bash
make checkall
```
Expected: PASS (fmt-check + clippy `-D warnings` + typecheck + tests across all five packages). If `tsc` fails resolving `@par-rt-db/client`, run `make ts-client-build` first and re-run.

- [ ] **Step 8: Eyeball it live**

```bash
make dev-db-up && cd dashboard && bun run dev:bg
```
Open `http://localhost:8310/metrics`, sign in, and confirm: sparklines fill in over the first 60 s; the pool area reflects busy/capacity; headlines show `/s` for the rate tiles; hover shows a tooltip. Stop the dev server and `make dev-db-down` when done.

- [ ] **Step 9: Commit**

```bash
git add dashboard/src/pages/MetricsPage.tsx dashboard/src/pages/MetricsPage.module.css dashboard/src/pages/MetricsPage.test.tsx
git commit -m "feat(dashboard): trend sparklines on the /metrics page"
```

---

## Self-Review (run after writing, before execution)

**Spec coverage:**
- Rolling history buffer + 60 s window → Task 2 (`MAX_SAMPLES = 61`). ✓
- Pure derivation with reset/gap/clamp edge cases → Task 1 (`rateSeries`). ✓
- Sparkline (line + area, gap-aware, last-dot, a11y) → Task 3. ✓
- Hover layer (default-on, pure index math) → Task 4 + `nearestIndex` (Task 1). ✓
- Counters become rate tiles (headline `/s`, sub total) → Task 5. ✓
- Gauges get level sparklines; pool = busy/capacity single-series → Task 5. ✓
- Uptime tile unchanged → Task 5 (System panel, no sparkline). ✓
- Zero new deps, AdminProvider/server untouched → no `package.json`/provider edits anywhere. ✓
- Tests for pure logic, hook, component, page → Tasks 1–5. ✓
- Gate `make checkall` → Task 5 Step 7. ✓

**Placeholder scan:** none — every code step contains real, correct code (no "TODO", "implement later", or illustrative snippets).

**Type consistency:** `Point`, `Sample`, `MAX_SAMPLES`, `rateSeries`, `levelSeries`, `lastValue`, `nearestIndex`, `formatRate` are defined once (Task 1) and consumed with identical names/signatures in Tasks 2, 4, 5. `Sparkline` props (`values`, `stroke`, `fill`, `height`, `min`, `max`, `ariaLabel`, `interactive`, `formatTip`, `children`) are consistent across Task 3 → Task 4 → Task 5.

## Execution Handoff

Per the user's standing preference, this plan executes via **superpowers:subagent-driven-development** (Subagent-Driven, this session) — a fresh implementer subagent per task, with spec-compliance and code-quality review between tasks, beginning immediately at Task 1.
