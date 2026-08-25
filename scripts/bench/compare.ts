#!/usr/bin/env bun
/** ENH-033 phase B — baseline regression check.
 *
 * `bun run scripts/bench/compare.ts <baseline.json> <result.json>`
 *
 * Walks every numeric leaf present in BOTH files and computes percent
 * change. A metric name containing "latency"/"Latency"/"TimeMs"/"Ms" is
 * treated as a latency (regression = increase); everything else numeric is
 * treated as throughput/count-shaped (regression = decrease) — matching the
 * plan's "exits 1 if any metric regresses more than 15% (latencies up,
 * throughput down)". Exits 1 and prints a table on any regression beyond the
 * threshold; exits 0 and prints a short summary otherwise. A metric present
 * in only one file is skipped with a note, never a crash.
 */

import { readFile } from "node:fs/promises";

const REGRESSION_THRESHOLD = 0.15;

type Json = { [key: string]: Json } | Json[] | number | string | boolean | null;

interface Metric {
  path: string;
  baseline: number;
  candidate: number;
  pctChange: number;
  kind: "latency" | "throughput";
  regressed: boolean;
}

function isLatencyPath(path: string): boolean {
  return /latency|latencyms|timems|holdtime|roundtrip|takeovertime/i.test(path);
}

/** Flattens numeric leaves of a JSON object into `path -> value` pairs,
 * joining keys with `.`. Skips non-numeric leaves and arrays of non-numbers. */
function flattenNumeric(value: Json, prefix: string, out: Map<string, number>): void {
  if (typeof value === "number" && Number.isFinite(value)) {
    out.set(prefix, value);
    return;
  }
  if (value === null || typeof value !== "object") {
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((v, i) => flattenNumeric(v, `${prefix}[${i}]`, out));
    return;
  }
  for (const [k, v] of Object.entries(value)) {
    flattenNumeric(v as Json, prefix ? `${prefix}.${k}` : k, out);
  }
}

function compare(baseline: Json, candidate: Json): { metrics: Metric[]; skipped: string[] } {
  const baseFlat = new Map<string, number>();
  const candFlat = new Map<string, number>();
  flattenNumeric(baseline, "", baseFlat);
  flattenNumeric(candidate, "", candFlat);

  const metrics: Metric[] = [];
  const skipped: string[] = [];
  const allPaths = new Set([...baseFlat.keys(), ...candFlat.keys()]);

  for (const path of allPaths) {
    const b = baseFlat.get(path);
    const c = candFlat.get(path);
    if (b === undefined || c === undefined) {
      skipped.push(path);
      continue;
    }
    const kind: Metric["kind"] = isLatencyPath(path) ? "latency" : "throughput";
    // pctChange is signed: positive means "went up" regardless of kind.
    const pctChange = b === 0 ? (c === 0 ? 0 : Number.POSITIVE_INFINITY) : (c - b) / Math.abs(b);
    const regressed =
      kind === "latency" ? pctChange > REGRESSION_THRESHOLD : pctChange < -REGRESSION_THRESHOLD;
    metrics.push({ path, baseline: b, candidate: c, pctChange, kind, regressed });
  }

  metrics.sort((a, b) => a.path.localeCompare(b.path));
  return { metrics, skipped };
}

function fmtPct(p: number): string {
  if (!Number.isFinite(p)) return "n/a";
  return `${p >= 0 ? "+" : ""}${(p * 100).toFixed(1)}%`;
}

function printTable(metrics: Metric[]): void {
  const rows = metrics.map((m) => ({
    metric: m.path,
    kind: m.kind,
    baseline: m.baseline,
    candidate: m.candidate,
    change: fmtPct(m.pctChange),
    regressed: m.regressed ? "REGRESSED" : "",
  }));
  console.table(rows);
}

async function main(): Promise<void> {
  const [baselinePath, candidatePath] = process.argv.slice(2);
  if (!baselinePath || !candidatePath) {
    console.error("Usage: bun run scripts/bench/compare.ts <baseline.json> <result.json>");
    process.exit(2);
  }

  const [baselineRaw, candidateRaw] = await Promise.all([
    readFile(baselinePath, "utf8"),
    readFile(candidatePath, "utf8"),
  ]);
  const baseline = JSON.parse(baselineRaw) as Json;
  const candidate = JSON.parse(candidateRaw) as Json;

  const { metrics, skipped } = compare(baseline, candidate);
  const regressed = metrics.filter((m) => m.regressed);

  if (skipped.length > 0) {
    console.error(
      `[compare] skipped ${skipped.length} metric(s) present in only one file: ${skipped.join(", ")}`,
    );
  }

  if (regressed.length > 0) {
    console.error(
      `[compare] ${regressed.length} metric(s) regressed beyond ${(REGRESSION_THRESHOLD * 100).toFixed(0)}%:`,
    );
    printTable(regressed);
    process.exit(1);
  }

  console.error(
    `[compare] OK — ${metrics.length} metric(s) compared, none regressed beyond ${(REGRESSION_THRESHOLD * 100).toFixed(0)}%.`,
  );
}

main().catch((err) => {
  console.error("[compare] fatal:", err);
  process.exit(1);
});
