/** Scenario e: the ARC-005 background bulk-delete path — the same 5k-row
 * delete shape scenario d measures on the txn path, driven through
 * `POST /api/bulk/delete` instead. Measures what chunking buys: total op wall
 * time, interactive-write latency DURING the op (the interleave proof —
 * writes submitted while chunks run must complete in well under the total
 * wall), and progressive subscriber pushes (per-chunk queryUpdate frames).
 *
 * Comparison anchors: scenario d on this machine at the pre-bulk tip measured
 * the 5k deleteByQuery txn turn-hold median 882.6ms with 100 subscribers
 * (2026-11-12); interactive writes submitted during that delete can only
 * complete after the whole turn commits, so their latency is bounded below by
 * the turn hold (882.6ms here; 3418.6ms in the documented pre-ENH-034
 * baseline).
 */
import { mutation, RtDbClient, RtDbHttpClient } from "@par-rt-db/client";
import type { RtQuery } from "@par-rt-db/client";
import { httpToWs } from "./ws.js";
import { makePayload, sleep, summarize, type LatencySummary } from "./stats.js";
import type { BenchTarget } from "./setup.js";

export interface ScenarioEResult {
  rowsInserted: number;
  rowsRequested: number;
  subscribers: number;
  /** Wall time from bulk submit to status=complete (all chunk turns included). */
  opWallMs: LatencySummary;
  /** Interactive-write round-trips submitted DURING the op. */
  interleaveWritesMs: LatencySummary;
  /** queryUpdate pushes the tally subscriber received while the op ran. */
  subscriberPushes: number;
  deletedAtCompletion: number;
}

const INSERT_BATCH_SIZE = 500;
const POLL_INTERVAL_MS = 50;
const INTERLEAVE_EVERY_MS = 40;
const INTERLEAVE_BATCH = 5;
const AUTH_TIMEOUT_MS = 10_000;
const OP_TIMEOUT_MS = 120_000;

function waitAuthenticated(client: RtDbClient): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("auth timeout")), AUTH_TIMEOUT_MS);
    const off = client.onAuthChange((state) => {
      if (state === "authenticated") {
        clearTimeout(timer);
        off();
        resolve();
      }
    });
    client.connect();
  });
}

export async function runScenarioE(
  target: BenchTarget,
  rows: number,
  subscribers: number,
): Promise<ScenarioEResult> {
  const httpClient = new RtDbHttpClient({ url: target.url, db: target.db, token: target.token });

  // Tag rows with a unique offset so the bulk filter only touches this run's rows.
  const base = Date.now() * 1_000_000;
  let inserted = 0;
  for (let start = 0; start < rows; start += INSERT_BATCH_SIZE) {
    const batchSize = Math.min(INSERT_BATCH_SIZE, rows - start);
    const builder = mutation();
    for (let i = 0; i < batchSize; i++) {
      builder.insert("items", makePayload(base + start + i));
    }
    await httpClient.mutate(builder.build());
    inserted += batchSize;
  }

  const wsUrl = httpToWs(target.url);
  const countQuery: RtQuery<number> = { json: { table: "items", count: true } };
  const subClients: RtDbClient[] = [];
  for (let i = 0; i < subscribers; i++) {
    subClients.push(new RtDbClient({ url: wsUrl, db: target.db, getToken: () => target.token }));
  }
  await Promise.all(subClients.map((c) => waitAuthenticated(c)));
  for (const c of subClients) {
    c.subscribe(countQuery, () => {});
  }
  // Let the initial subscribe round trip settle before the timed op.
  await sleep(200);

  // Per-chunk progressive pushes, tallied on the first subscriber.
  let pushes = 0;
  subClients[0].subscribe(countQuery, () => {
    pushes += 1;
  });

  const filter = {
    op: "and",
    exprs: [
      { op: "gte", field: "seed", value: base },
      { op: "lt", field: "seed", value: base + rows },
    ],
  } as const;

  const accepted = await httpClient.bulkDelete(target.db, "items", filter);
  if (accepted.matched <= 0) {
    for (const c of subClients) c.close();
    throw new Error(`bulk op matched ${accepted.matched} rows`);
  }
  const started = performance.now();

  // Interactive writes DURING the op: one small mutate every INTERLEAVE_EVERY_MS
  // until the op completes. Their latency distribution is the interleave proof.
  const writeLatencies: number[] = [];
  let probing = true;
  const interleave = (async () => {
    let probeSeed = base + rows + 1;
    while (probing) {
      const w0 = performance.now();
      const builder = mutation();
      for (let i = 0; i < INTERLEAVE_BATCH; i++) {
        builder.insert("items", makePayload(probeSeed));
        probeSeed += 1;
      }
      await httpClient.mutate(builder.build());
      writeLatencies.push(performance.now() - w0);
      await sleep(INTERLEAVE_EVERY_MS);
    }
  })();

  let status = await httpClient.bulkStatus(target.db, accepted.opId);
  const deadline = Date.now() + OP_TIMEOUT_MS;
  while (status.state === "running" && Date.now() < deadline) {
    await sleep(POLL_INTERVAL_MS);
    status = await httpClient.bulkStatus(target.db, accepted.opId);
  }
  const opWallMs = performance.now() - started;
  probing = false;
  await interleave;
  for (const c of subClients) c.close();
  if (status.state !== "complete") {
    throw new Error(`bulk op ended in state ${status.state}: ${status.error ?? "-"}`);
  }

  return {
    rowsInserted: inserted,
    rowsRequested: rows,
    subscribers,
    opWallMs: summarize([opWallMs]),
    interleaveWritesMs: summarize(writeLatencies),
    subscriberPushes: pushes,
    deletedAtCompletion: status.deleted,
  };
}