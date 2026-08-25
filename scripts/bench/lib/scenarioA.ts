/** Scenario a: N concurrent writers inserting ~1 KB docs for a fixed
 * duration against a single server. Reports commits/s and commit latency. */

import { mutation, RtDbHttpClient } from "@par-rt-db/client";
import { makePayload, summarize, type LatencySummary } from "./stats.js";
import type { BenchTarget } from "./setup.js";

export interface ScenarioAResult {
  writers: number;
  durationSec: number;
  commits: number;
  commitsPerSec: number;
  commitLatency: LatencySummary;
  errors: number;
}

export async function runScenarioA(
  target: BenchTarget,
  writers: number,
  durationSec: number,
): Promise<ScenarioAResult> {
  const client = new RtDbHttpClient({ url: target.url, db: target.db, token: target.token });
  const deadline = Date.now() + durationSec * 1000;
  const latenciesMs: number[] = [];
  let commits = 0;
  let errors = 0;
  let seed = 0;

  async function writerLoop(): Promise<void> {
    while (Date.now() < deadline) {
      const txn = mutation().insert("items", makePayload(seed++)).build();
      const start = performance.now();
      try {
        await client.mutate(txn);
        latenciesMs.push(performance.now() - start);
        commits++;
      } catch {
        errors++;
      }
    }
  }

  const startedAt = Date.now();
  await Promise.all(Array.from({ length: writers }, () => writerLoop()));
  const elapsedSec = (Date.now() - startedAt) / 1000;

  return {
    writers,
    durationSec,
    commits,
    commitsPerSec: elapsedSec > 0 ? commits / elapsedSec : 0,
    commitLatency: summarize(latenciesMs),
    errors,
  };
}
