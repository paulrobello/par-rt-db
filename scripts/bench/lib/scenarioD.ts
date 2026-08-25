/** Scenario d: bulk `deleteByQuery` of ~5k rows with ~100 concurrent
 * subscribers on the affected table. Measures "turn hold time" — how long
 * the deleting mutation's committer turn takes while many subscriptions
 * re-run against it (ARC-006's per-op `pg_notify`-inside-the-turn concern).
 *
 * The committer turn itself has no client-observable boundary (there is no
 * admin-exposed per-turn timer — checked `server/src/committer/mod.rs` and
 * `taps.rs`), so this uses the one-shot HTTP mutate's round-trip time as the
 * proxy: `POST /api/mutate` only returns once the committer turn that ran the
 * deletes (and re-ran every affected subscription) has fully committed and
 * `publish_taps` has returned, so the wall-clock the caller sees IS the turn
 * plus a network hop.
 *
 * A single `deleteByQuery` step is capped server-side at `MAX_BY_QUERY_ROWS`
 * (1000, `server/src/txn.rs`), so ~5k rows means multiple `deleteByQuery`
 * steps in ONE transaction — still one serialized committer turn, matching
 * what the plan is measuring. */

import { mutation, RtDbClient, RtDbHttpClient } from "@par-rt-db/client";
import type { RtQuery } from "@par-rt-db/client";
import { httpToWs } from "./ws.js";
import { makePayload, summarize, type LatencySummary } from "./stats.js";
import type { BenchTarget } from "./setup.js";

export interface ScenarioDResult {
  rowsInserted: number;
  rowsRequested: number;
  subscribers: number;
  turnHoldTimeMs: LatencySummary;
}

const MAX_BY_QUERY_ROWS = 1000;
const INSERT_BATCH_SIZE = 500;
const AUTH_TIMEOUT_MS = 10_000;

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

export async function runScenarioD(
  target: BenchTarget,
  rows: number,
  subscribers: number,
): Promise<ScenarioDResult> {
  const httpClient = new RtDbHttpClient({ url: target.url, db: target.db, token: target.token });

  // Tag this run's rows with a unique offset so the delete filter only ever
  // touches rows this run inserted.
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
    const c = new RtDbClient({ url: wsUrl, db: target.db, getToken: () => target.token });
    subClients.push(c);
  }
  await Promise.all(subClients.map((c) => waitAuthenticated(c)));
  for (const c of subClients) {
    c.subscribe(countQuery, () => {});
  }
  // Let the initial subscribe round trip settle before the timed delete.
  await new Promise((resolve) => setTimeout(resolve, 200));

  const builder = mutation();
  for (let start = 0; start < rows; start += MAX_BY_QUERY_ROWS) {
    const end = Math.min(start + MAX_BY_QUERY_ROWS, rows);
    builder.deleteByQuery(
      "items",
      {
        op: "and",
        exprs: [
          { op: "gte", field: "seed", value: base + start },
          { op: "lt", field: "seed", value: base + end },
        ],
      },
      MAX_BY_QUERY_ROWS,
    );
  }
  const start = performance.now();
  await httpClient.mutate(builder.build());
  const turnHoldMs = performance.now() - start;

  for (const c of subClients) c.close();

  return {
    rowsInserted: inserted,
    rowsRequested: rows,
    subscribers,
    turnHoldTimeMs: summarize([turnHoldMs]),
  };
}
