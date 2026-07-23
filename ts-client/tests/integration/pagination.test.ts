// @vitest-environment node
// Integration tests hit a real server over the network — they need Node's real
// fetch/WebSocket, not the happy-dom environment the default config uses for the
// React unit tests.
import { beforeAll, describe, expect, it } from "vitest";
import { mutation } from "../../src/mutation.js";
import { createApi } from "../../src/query.js";
import { defineSchema, defineTable, t } from "../../src/schema.js";
import { httpClient, provisionDb, testServer } from "./harness.js";

// Single-column index is in scope for the current keyset implementation; the
// cursor encodes every sort column, so `by_priority` (fields=["priority"])
// round-trips a cursor that fully determines the next page's start.
const schema = defineSchema({
  items: defineTable({
    name: t.string(),
    priority: t.number(),
  }).index("by_priority", ["priority"]),
});
const api = createApi(schema);

const server = testServer();

// Same per-test ceiling the other e2e tests use — comfortably above vitest's
// internal timeout so a slow server round trip surfaces as a clear assertion
// failure rather than a generic "Test timed out".
const E2E_TEST_TIMEOUT_MS = 15000;

describe.skipIf(server === null)("pagination e2e against a running server", () => {
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
    "paginates through all matching docs across multiple pages",
    async () => {
      const http = httpClient(srv, db, token);

      // Seed 25 docs in one transaction; priorities 1..25.
      const seed = mutation();
      for (let i = 1; i <= 25; i++) {
        seed.insert("items", { name: `item ${i}`, priority: i });
      }
      await http.mutate(seed.build());

      // Page 1 of 3: first 10 of 25 — no cursor, expect a nextCursor.
      const first = await http.query(
        api.items.query().withIndex("by_priority").order("asc").paginate(undefined, 10),
      );
      expect(first.docs).toHaveLength(10);
      expect(first.docs[0]).toMatchObject({ priority: 1 });
      expect(first.docs[9]).toMatchObject({ priority: 10 });
      expect(typeof first.nextCursor).toBe("string");

      // Page 2 of 3: next 10 — resume from the returned cursor.
      const second = await http.query(
        api.items.query().withIndex("by_priority").order("asc").paginate(first.nextCursor, 10),
      );
      expect(second.docs).toHaveLength(10);
      expect(second.docs[0]).toMatchObject({ priority: 11 });
      expect(second.docs[9]).toMatchObject({ priority: 20 });
      expect(typeof second.nextCursor).toBe("string");

      // Page 3 of 3: final partial page — the server omits nextCursor when
      // there is no next page, which surfaces as `undefined` in the parsed JSON.
      const third = await http.query(
        api.items.query().withIndex("by_priority").order("asc").paginate(second.nextCursor, 10),
      );
      expect(third.docs).toHaveLength(5);
      expect(third.docs[0]).toMatchObject({ priority: 21 });
      expect(third.docs[4]).toMatchObject({ priority: 25 });
      expect(third.nextCursor).toBeUndefined();

      // Every doc was returned exactly once across the three pages, with no
      // gaps or duplicates — the correctness property of keyset pagination.
      const all = [...first.docs, ...second.docs, ...third.docs];
      expect(all).toHaveLength(25);
      const priorities = (all as Array<{ priority: number }>)
        .map((d) => d.priority)
        .sort((a, b) => a - b);
      expect(priorities).toEqual(Array.from({ length: 25 }, (_, i) => i + 1));
    },
    E2E_TEST_TIMEOUT_MS,
  );

  it(
    "returns the full set in one page with no nextCursor when the page size exceeds the row count",
    async () => {
      const http = httpClient(srv, db, token);

      // Page size larger than the table: every doc lands in the single page and
      // there is no second page — the canonical "no more data" wire shape. The
      // server omits nextCursor when there is no next page (undefined in JSON).
      const page = await http.query(
        api.items.query().withIndex("by_priority").order("asc").paginate(undefined, 1000),
      );
      expect(page.docs).toHaveLength(25);
      expect(page.nextCursor).toBeUndefined();
    },
    E2E_TEST_TIMEOUT_MS,
  );
});
