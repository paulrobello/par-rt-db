/** Scenario b: M subscribers on one live query + one writer. Measures the
 * time from a commit's ack to each subscriber's `queryUpdate` receipt
 * (fan-out latency), p50/p99. */

import { mutation, RtDbClient, RtDbHttpClient } from "@par-rt-db/client";
import type { RtQuery } from "@par-rt-db/client";
import { httpToWs } from "./ws.js";
import { makePayload, summarize, type LatencySummary } from "./stats.js";
import type { BenchTarget } from "./setup.js";

export interface ScenarioBResult {
  subscribers: number;
  iterations: number;
  fanoutLatency: LatencySummary;
  timeouts: number;
}

const PER_WRITE_TIMEOUT_MS = 5000;
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

export async function runScenarioB(
  target: BenchTarget,
  subscribers: number,
  iterations = 20,
): Promise<ScenarioBResult> {
  const wsUrl = httpToWs(target.url);
  const query: RtQuery<unknown> = {
    json: { table: "items", index: "by_creation", order: "desc", take: 1 },
  };

  const subClients: RtDbClient[] = [];
  for (let i = 0; i < subscribers; i++) {
    const c = new RtDbClient({ url: wsUrl, db: target.db, getToken: () => target.token });
    subClients.push(c);
  }
  await Promise.all(subClients.map((c) => waitAuthenticated(c)));

  // One waiter per (subscriber, expected seed) — resolved when that
  // subscriber's onUpdate reports the matching seed value.
  const waiters = new Map<string, () => void>();
  for (const [idx, c] of subClients.entries()) {
    c.subscribe(query, (value: unknown) => {
      const rows = value as Array<{ seed?: number }>;
      const seedSeen = rows[0]?.seed;
      if (seedSeen === undefined) return;
      const key = `${idx}:${seedSeen}`;
      const resolve = waiters.get(key);
      if (resolve) {
        waiters.delete(key);
        resolve();
      }
    });
  }

  const httpClient = new RtDbHttpClient({ url: target.url, db: target.db, token: target.token });
  const fanoutLatenciesMs: number[] = [];
  let timeouts = 0;

  for (let i = 0; i < iterations; i++) {
    const payload = makePayload(i);
    const txn = mutation().insert("items", payload).build();
    await httpClient.mutate(txn);
    const commitAckAt = performance.now();

    const perSubscriber = subClients.map(
      (_, idx) =>
        new Promise<number | null>((resolve) => {
          const key = `${idx}:${i}`;
          const timer = setTimeout(() => {
            waiters.delete(key);
            resolve(null);
          }, PER_WRITE_TIMEOUT_MS);
          waiters.set(key, () => {
            clearTimeout(timer);
            resolve(performance.now() - commitAckAt);
          });
        }),
    );
    const results = await Promise.all(perSubscriber);
    for (const r of results) {
      if (r === null) timeouts++;
      else fanoutLatenciesMs.push(r);
    }
  }

  for (const c of subClients) c.close();

  return {
    subscribers,
    iterations,
    fanoutLatency: summarize(fanoutLatenciesMs),
    timeouts,
  };
}
