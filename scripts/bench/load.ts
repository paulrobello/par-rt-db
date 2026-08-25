#!/usr/bin/env bun
/** ENH-033 phase B — black-box load script.
 *
 * Drives one or two REAL running par-rt-db servers over HTTP + WS and writes
 * one JSON result file (`bench/results/<git-sha>.json`) with the numbers the
 * ENH-033 plan asks for: commit throughput/latency (scenario a), subscription
 * fan-out latency (scenario b), multi-instance forward round-trip + takeover
 * time (scenario c), and bulk-mutation turn hold time (scenario d).
 *
 * Assumes the server(s) are already running — a later Makefile phase (`make
 * bench`) is responsible for `dev-db-up`, starting `cargo run --release` on
 * :8300 (and :8301 for scenario c), running this script, then stopping them.
 *
 * Run `bun run scripts/bench/load.ts --help` for the full option list.
 */

import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { RtDbAdminClient } from "@par-rt-db/client";
import { parseArgs, type Scenario } from "./lib/args.js";
import { ensureBenchDb } from "./lib/setup.js";
import { runScenarioA, type ScenarioAResult } from "./lib/scenarioA.js";
import { runScenarioB, type ScenarioBResult } from "./lib/scenarioB.js";
import { runScenarioC, type ScenarioCResult } from "./lib/scenarioC.js";
import { runScenarioD, type ScenarioDResult } from "./lib/scenarioD.js";

interface BenchOutput {
  sha: string;
  ranAtMs: number;
  scenarios: Scenario[];
  a?: ScenarioAResult;
  b?: ScenarioBResult;
  c?: ScenarioCResult;
  d?: ScenarioDResult;
  /** Subscription-invalidation effectiveness at the end of the run, from
   * `GET /admin/metrics` (`ts-client/src/admin.ts::MetricsSnapshot`) — the
   * `subsRerunsTotal`/`subsSkips*Total` counters ENH-024 already exposes.
   * There is no separate "/admin/stats" rerun-ratio endpoint; `/admin/metrics`
   * is the real one. */
  adminMetrics?: {
    subsRerunsTotal: number;
    subsSkipsPointTotal: number;
    subsSkipsIndexedTotal: number;
    subsSkipsOrderedTotal: number;
    rerunRatio: number | null;
  };
}

async function main(): Promise<void> {
  const opts = parseArgs(process.argv.slice(2));
  console.error(
    `[bench] scenarios=${opts.scenarios.join(",")} url=${opts.url} db=${opts.db} sha=${opts.sha}`,
  );

  const output: BenchOutput = {
    sha: opts.sha,
    ranAtMs: Date.now(),
    scenarios: opts.scenarios,
  };

  const primary = await ensureBenchDb(opts.url, opts.adminKey, opts.db);

  if (opts.scenarios.includes("a")) {
    console.error(`[bench] scenario a: ${opts.concurrency} writers, ${opts.durationSec}s`);
    output.a = await runScenarioA(primary, opts.concurrency, opts.durationSec);
  }

  if (opts.scenarios.includes("b")) {
    console.error(`[bench] scenario b: ${opts.concurrency} subscribers`);
    output.b = await runScenarioB(primary, opts.concurrency);
  }

  if (opts.scenarios.includes("c")) {
    console.error(`[bench] scenario c: multi-instance ${opts.url} vs ${opts.shadowUrl}`);
    const secondary = await ensureBenchDb(opts.shadowUrl, opts.adminKey, opts.db);
    output.c = await runScenarioC(primary, secondary, opts.ownerPid);
  }

  if (opts.scenarios.includes("d")) {
    console.error(`[bench] scenario d: ${opts.bulkRows} rows, ${opts.subscribers} subscribers`);
    output.d = await runScenarioD(primary, opts.bulkRows, opts.subscribers);
  }

  output.adminMetrics = await readAdminMetrics(primary.admin);

  await mkdir(dirname(opts.out), { recursive: true });
  await writeFile(opts.out, `${JSON.stringify(output, null, 2)}\n`, "utf8");
  console.error(`[bench] wrote ${opts.out}`);
}

async function readAdminMetrics(admin: RtDbAdminClient): Promise<BenchOutput["adminMetrics"]> {
  try {
    const m = await admin.metrics();
    const reruns = m.subsRerunsTotal;
    const skips = m.subsSkipsPointTotal + m.subsSkipsIndexedTotal + m.subsSkipsOrderedTotal;
    const total = reruns + skips;
    return {
      subsRerunsTotal: reruns,
      subsSkipsPointTotal: m.subsSkipsPointTotal,
      subsSkipsIndexedTotal: m.subsSkipsIndexedTotal,
      subsSkipsOrderedTotal: m.subsSkipsOrderedTotal,
      rerunRatio: total > 0 ? reruns / total : null,
    };
  } catch (err) {
    console.error(`[bench] warning: /admin/metrics fetch failed: ${(err as Error).message}`);
    return undefined;
  }
}

main().catch((err) => {
  console.error("[bench] fatal:", err);
  process.exit(1);
});
