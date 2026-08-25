/** Scenario b: M subscribers on one live query + one writer. Measures the
 * time from issuing each write to each subscriber's `queryUpdate` receipt
 * (fan-out latency), p50/p99. Timed from send rather than from the HTTP
 * mutate's ack — see the in-loop comment for why. */

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

    // Register every subscriber's waiter BEFORE sending the mutation. The
    // server pushes a subscriber's queryUpdate over its own already-open
    // WebSocket as soon as it commits — a single hop — while this HTTP
    // mutate still has a full request/response round trip left to complete.
    // Waiting for `httpClient.mutate` to resolve before registering waiters
    // lost that race almost every time (confirmed against a real deployed
    // server: 151/160 iterations timed out), because the push routinely
    // lands before the HTTP response does, finds no waiter for its key, and
    // is dropped with nothing left to trigger the match once one is set.
    let sentAt = 0;
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
            resolve(performance.now() - sentAt);
          });
        }),
    );
    sentAt = performance.now();
    await httpClient.mutate(txn);
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
