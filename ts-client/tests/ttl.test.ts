import { describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  sessions: defineTable({ expiresAt: t.number() })
    .index("by_expiresAt", ["expiresAt"])
    .ttl("expiresAt", 1000),
});

const api = createApi(schema);

describe("InMemoryRtDbClient — ttl", () => {
  it("stamps the default expiry at insert and reaps it on tick", async () => {
    const now = 10_000;
    const c = new InMemoryRtDbClient({ now: () => now, random: () => 0 });
    c.pushSchema(schema);

    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "sessions", doc: {} }],
    });
    const id = (res as { id: string }).id;

    const doc = await c.query(api.sessions.get(id));
    expect(doc).not.toBeNull();
    expect(doc?.expiresAt).toBe(now + 1000);

    const removed = c.tick(now + 2000);
    expect(removed).toBe(1);
    expect(await c.query(api.sessions.get(id))).toBeNull();
  });

  it("respects an explicit expiry over the default", async () => {
    const now = 50_000;
    const c = new InMemoryRtDbClient({ now: () => now, random: () => 0 });
    c.pushSchema(schema);

    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "sessions", doc: { expiresAt: now + 500 } }],
    });
    const id = (res as { id: string }).id;

    expect((await c.query(api.sessions.get(id)))?.expiresAt).toBe(now + 500);

    // Below the explicit expiry: still present.
    c.tick(now + 499);
    expect(await c.query(api.sessions.get(id))).not.toBeNull();

    // Past it: reaped.
    expect(c.tick(now + 501)).toBe(1);
    expect(await c.query(api.sessions.get(id))).toBeNull();
  });
});
