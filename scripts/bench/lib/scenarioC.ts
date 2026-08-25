/** Scenario c: multi-instance. Two servers share one Postgres with
 * `RTDB_MULTI_INSTANCE=true`; exactly one holds each db's write-ownership
 * lease (`server/src/committer/lease.rs`) and the other forwards writes to it
 * transparently (`server/src/forward.rs`) — a client sees no difference in
 * the *result*, only in round-trip latency.
 *
 * There is no HTTP-exposed leadership/ownership endpoint (checked
 * `server/src/admin/mod.rs`'s route table and `committer/lease.rs`/
 * `forward.rs`: the lease is a Postgres advisory lock held on a
 * single dedicated connection, never surfaced over HTTP). So this scenario
 * determines "owner" vs "shadow" *empirically*: it writes a probe batch to
 * both ports and labels the lower-latency one the (likely) owner — a local
 * write skips the forward round trip entirely, so the gap should be clear
 * under any real contention. Takeover timing additionally needs the owner's
 * OS pid (`--owner-pid`), which only the caller that started the two
 * processes (a later Makefile phase) can supply; without it this scenario
 * reports forward round-trip latency only. */

import { mutation, RtDbHttpClient } from "@par-rt-db/client";
import { makePayload, sleep, summarize, type LatencySummary } from "./stats.js";
import type { BenchTarget } from "./setup.js";

export interface ScenarioCResult {
  probeSamples: number;
  portALatency: LatencySummary;
  portBLatency: LatencySummary;
  likelyOwnerUrl: string;
  likelyShadowUrl: string;
  forwardRoundTripLatency: LatencySummary;
  takeoverTimeMs: number | null;
  takeoverMeasured: boolean;
}

const PROBE_SAMPLES = 20;
const TAKEOVER_POLL_MS = 200;
const TAKEOVER_DEADLINE_MS = 30_000;

async function probeLatencies(client: RtDbHttpClient, n: number): Promise<number[]> {
  const samplesMs: number[] = [];
  for (let i = 0; i < n; i++) {
    const txn = mutation()
      .insert("items", makePayload(-1 - i))
      .build();
    const start = performance.now();
    try {
      await client.mutate(txn);
      samplesMs.push(performance.now() - start);
    } catch {
      // A probe failure (e.g. a mid-run takeover) is skipped, not fatal.
    }
  }
  return samplesMs;
}

export async function runScenarioC(
  targetA: BenchTarget,
  targetB: BenchTarget,
  ownerPid?: number,
): Promise<ScenarioCResult> {
  const clientA = new RtDbHttpClient({ url: targetA.url, db: targetA.db, token: targetA.token });
  const clientB = new RtDbHttpClient({ url: targetB.url, db: targetB.db, token: targetB.token });

  const [aSamples, bSamples] = await Promise.all([
    probeLatencies(clientA, PROBE_SAMPLES),
    probeLatencies(clientB, PROBE_SAMPLES),
  ]);
  const portALatency = summarize(aSamples);
  const portBLatency = summarize(bSamples);

  const aIsLikelyOwner = portALatency.meanMs <= portBLatency.meanMs;
  const ownerTarget = aIsLikelyOwner ? targetA : targetB;
  const shadowTarget = aIsLikelyOwner ? targetB : targetA;
  const shadowClient = aIsLikelyOwner ? clientB : clientA;
  const forwardRoundTripLatency = aIsLikelyOwner ? portBLatency : portALatency;

  let takeoverTimeMs: number | null = null;
  let takeoverMeasured = false;
  if (ownerPid !== undefined) {
    takeoverMeasured = true;
    try {
      process.kill(ownerPid, "SIGKILL");
    } catch {
      // Already dead, or not our process to kill — measurement below still
      // reports whatever recovery time it observes (or null on timeout).
    }
    const killedAt = performance.now();
    const deadline = killedAt + TAKEOVER_DEADLINE_MS;
    while (performance.now() < deadline) {
      try {
        const txn = mutation().insert("items", makePayload(-9999)).build();
        await shadowClient.mutate(txn);
        takeoverTimeMs = performance.now() - killedAt;
        break;
      } catch {
        await sleep(TAKEOVER_POLL_MS);
      }
    }
  }

  return {
    probeSamples: PROBE_SAMPLES,
    portALatency,
    portBLatency,
    likelyOwnerUrl: ownerTarget.url,
    likelyShadowUrl: shadowTarget.url,
    forwardRoundTripLatency,
    takeoverTimeMs,
    takeoverMeasured,
  };
}
