/** Latency percentile helpers shared by every scenario. */

export interface LatencySummary {
  count: number;
  p50Ms: number;
  p99Ms: number;
  meanMs: number;
}

/** Computes p50/p99/mean from a list of millisecond samples. Sorts a copy —
 * callers may keep appending to the original array before calling this. */
export function summarize(samplesMs: number[]): LatencySummary {
  if (samplesMs.length === 0) {
    return { count: 0, p50Ms: 0, p99Ms: 0, meanMs: 0 };
  }
  const sorted = [...samplesMs].sort((a, b) => a - b);
  const pick = (p: number) => sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))];
  const mean = sorted.reduce((sum, v) => sum + v, 0) / sorted.length;
  return {
    count: sorted.length,
    p50Ms: pick(0.5) as number,
    p99Ms: pick(0.99) as number,
    meanMs: mean,
  };
}

/** A ~1 KB JSON-serializable payload for write scenarios (a/d): padded text
 * plus a few scalar fields so the doc isn't one giant string. */
export function makePayload(seed: number): Record<string, unknown> {
  return {
    seed,
    text: "x".repeat(950),
    createdAtMs: Date.now(),
  };
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
