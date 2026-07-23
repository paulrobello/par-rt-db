// @vitest-environment node
// Integration tests hit a real server over the network — they need Node's real
// fetch/WebSocket, not the happy-dom environment the default config uses for the
// React unit tests.
import { beforeAll, describe, expect, it } from "vitest";
import type { RtDbClient } from "../../src/client.js";
import { createApi } from "../../src/query.js";
import { mutation } from "../../src/mutation.js";
import { defineSchema, defineTable, t } from "../../src/schema.js";
import { httpClient, provisionDb, testServer, wsClient } from "./harness.js";

const schema = defineSchema({
  items: defineTable({
    projectId: t.string(),
    title: t.string(),
    order: t.number(),
  }).index("by_project", ["projectId"]),
});
const api = createApi(schema);

const server = testServer();

// Generous per-test ceiling for tests that make multiple sequential network
// round trips or poll via `waitFor` — comfortably above `waitFor`'s own
// internal timeout so its clearer error message surfaces instead of vitest's
// generic "Test timed out" when a server round trip is slow.
const E2E_TEST_TIMEOUT_MS = 15000;

describe.skipIf(server === null)("e2e against a running server", () => {
  // biome-ignore lint/style/noNonNullAssertion: guarded by describe.skipIf
  const srv = server!;
  let db = "";
  let token = "";

  beforeAll(async () => {
    const provisioned = await provisionDb(srv, schema);
    db = provisioned.db;
    token = provisioned.token;
  }, E2E_TEST_TIMEOUT_MS);

  it(
    "HTTP insert then HTTP query round-trips with system fields",
    async () => {
      const http = httpClient(srv, db, token);
      // An insert step returns `{ id }` (not a bare id string); patch/delete/expect*
      // return null; upsert returns `{ id, inserted }`.
      const [insertResult] = await http.mutate(
        mutation().insert("items", { projectId: "p1", title: "first", order: 1 }).build(),
      );
      const id = (insertResult as { id: string }).id;
      expect(typeof id).toBe("string");

      const rows = await http.query(api.items.query().withIndex("by_project", ["p1"]).collect());
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        _id: id,
        projectId: "p1",
        title: "first",
        _version: expect.any(Number),
      });
    },
    E2E_TEST_TIMEOUT_MS,
  );

  it(
    "delivers a live queryUpdate over WS when an HTTP mutation writes",
    async () => {
      const ws = wsClient(srv, db, token);
      const http = httpClient(srv, db, token);
      const updates: unknown[][] = [];

      ws.connect();
      try {
        await waitForAuthenticated(ws, E2E_TEST_TIMEOUT_MS);

        ws.subscribe(api.items.query().withIndex("by_project", ["p2"]).collect(), (v) => {
          updates.push(v as unknown[]);
        });

        // Wait for the initial (empty) push.
        await waitFor(() => updates.length >= 1);
        expect(updates.at(-1)).toEqual([]);

        await http.mutate(
          mutation().insert("items", { projectId: "p2", title: "live", order: 1 }).build(),
        );

        await waitFor(() => (updates.at(-1)?.length ?? 0) === 1);
        expect((updates.at(-1) as Array<{ title: string }>)[0].title).toBe("live");
      } finally {
        // Guaranteed even if an assertion above throws, so a failed run
        // doesn't leak an open WS connection into the rest of the suite.
        ws.close();
      }
    },
    E2E_TEST_TIMEOUT_MS,
  );
});

/** Resolves once `ws` reaches the "authenticated" state, rejecting (and unsubscribing) on timeout. */
function waitForAuthenticated(ws: RtDbClient, timeoutMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      off();
      reject(new Error(`timed out waiting for WS authentication after ${timeoutMs}ms`));
    }, timeoutMs);
    const off = ws.onAuthChange((state) => {
      if (state === "authenticated") {
        clearTimeout(timer);
        off();
        resolve();
      }
    });
  });
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor timed out");
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}
