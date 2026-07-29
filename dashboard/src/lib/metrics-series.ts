import type { MetricsSnapshot } from "./types";

/** A point in a plotted series; `null` means "no value / gap" (break the line). */
export type Point = number | null;

/** One received snapshot, stamped with client receive-time (epoch ms). */
export interface Sample {
  t: number;
  snap: MetricsSnapshot;
}

/** Rolling window length: 60 s of history at 1Hz, plus the current point. */
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
export function rateSeries(samples: Sample[], counter: (s: MetricsSnapshot) => number): Point[] {
  const out: Point[] = new Array(samples.length).fill(null);
  for (let i = 1; i < samples.length; i++) {
    const dtMs = samples[i].t - samples[i - 1].t;
    const dt = dtMs / 1000;
    if (dt > DT_MAX) continue; // gap — leave null
    const prev = counter(samples[i - 1].snap);
    const curr = counter(samples[i].snap);
    if (curr < prev) continue; // counter reset — leave null, resume next step
    const clamped = Math.max(dt, DT_MIN);
    out[i] = (curr - prev) / clamped;
  }
  return out;
}

/** Project a gauge's instantaneous value per sample (no derivation). */
export function levelSeries(samples: Sample[], gauge: (s: MetricsSnapshot) => number): Point[] {
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

/**
 * Fraction of subscription-invalidation decisions resolved by a proven skip
 * (point/indexed/ordered) rather than a fan-out re-run. Null when no decision
 * has been made yet — never divide by zero.
 */
export function subsSkipRate(m: MetricsSnapshot): number | null {
  const skips = m.subsSkipsPointTotal + m.subsSkipsIndexedTotal + m.subsSkipsOrderedTotal;
  const decisions = skips + m.subsRerunsTotal;
  return decisions > 0 ? skips / decisions : null;
}

/** Format a 0..1 fraction as a percentage; null renders as an em dash. */
export function formatPercent(fraction: number | null): string {
  if (fraction == null) return "—";
  return `${(fraction * 100).toFixed(1)}%`;
}
