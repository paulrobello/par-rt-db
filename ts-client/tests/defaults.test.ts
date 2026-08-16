import { describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  tasks: defineTable({
    title: t.string(),
    status: t.union(t.literal("backlog"), t.literal("done")),
    priority: t.number(),
    notes: t.optional(t.string()),
  })
    .index("by_title", ["title"])
    .defaults({ status: "backlog", priority: 0, notes: "none" }),
});

const api = createApi(schema);

function makeClient(): InMemoryRtDbClient {
  const c = new InMemoryRtDbClient({ now: () => 10_000, random: () => 0 });
  c.pushSchema(schema);
  return c;
}

describe("defaults wire shape (FM-32)", () => {
  it("emits defaults on the wire when declared", () => {
    expect(schema.toJSON()).toEqual({
      tables: {
        tasks: {
          fields: {
            title: { type: "string" },
            status: {
              type: "union",
              variants: [
                { type: "literal", value: "backlog" },
                { type: "literal", value: "done" },
              ],
            },
            priority: { type: "number" },
            notes: { type: "optional", inner: { type: "string" } },
          },
          indexes: [{ name: "by_title", fields: ["title"] }],
          defaults: { status: "backlog", priority: 0, notes: "none" },
        },
      },
    });
  });

  it("omits defaults on the wire when the table declares none", () => {
    const s = defineSchema({ things: defineTable({ n: t.number() }) });
    expect(s.toJSON().tables.things).toEqual({ fields: { n: { type: "number" } } });
    expect(s.toJSON().tables.things).not.toHaveProperty("defaults");
  });
});

describe("InMemoryRtDbClient — field-level defaults", () => {
  it("stamps defaults for omitted keys at insert", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "a" } }],
    });
    const id = (res as { id: string }).id;
    const doc = await c.query(api.tasks.get(id));
    expect(doc?.status).toBe("backlog");
    expect(doc?.priority).toBe(0);
    expect(doc?.notes).toBe("none");
  });

  it("lets client-provided values win over defaults", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "a", status: "done", priority: 5 } }],
    });
    const id = (res as { id: string }).id;
    const doc = await c.query(api.tasks.get(id));
    expect(doc?.status).toBe("done");
    expect(doc?.priority).toBe(5);
  });

  it("patch never re-applies defaults after clearing an optional field", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "a" } }],
    });
    const id = (res as { id: string }).id;
    expect((await c.query(api.tasks.get(id)))?.notes).toBe("none");

    // `null` on an optional field clears it; the default must not come back.
    await c.mutate({ steps: [{ op: "patch", table: "tasks", id, fields: { notes: null } }] });
    expect(await c.query(api.tasks.get(id))).not.toHaveProperty("notes");

    // A later patch still must not re-stamp it.
    await c.mutate({ steps: [{ op: "patch", table: "tasks", id, fields: { priority: 9 } }] });
    const doc = await c.query(api.tasks.get(id));
    expect(doc).not.toHaveProperty("notes");
    expect(doc?.priority).toBe(9);
  });

  it("replace re-applies defaults for omitted keys", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "a" } }],
    });
    const id = (res as { id: string }).id;
    await c.mutate({
      steps: [{ op: "patch", table: "tasks", id, fields: { status: "done", notes: null } }],
    });
    expect(await c.query(api.tasks.get(id))).not.toHaveProperty("notes");

    // Replace is a NEW document: omitted keys get their defaults again.
    await c.mutate({
      steps: [{ op: "replace", table: "tasks", id, doc: { title: "b" } }],
    });
    const doc = await c.query(api.tasks.get(id));
    expect(doc?.title).toBe("b");
    expect(doc?.status).toBe("backlog");
    expect(doc?.priority).toBe(0);
    expect(doc?.notes).toBe("none");
  });

  it("upsert-insert applies defaults; upsert-update does not re-apply", async () => {
    const c = makeClient();
    const step = (patch: Record<string, unknown>) => ({
      op: "upsert" as const,
      table: "tasks",
      index: "by_title",
      eq: ["a"],
      insert: { title: "a" },
      patch,
    });
    const id = expect.any(String);

    const [r1] = await c.mutate({ steps: [step({ priority: 5 })] });
    expect(r1).toEqual({ id, inserted: true });
    const docId = (r1 as { id: string }).id;
    const inserted = await c.query(api.tasks.get(docId));
    expect(inserted?.status).toBe("backlog");
    expect(inserted?.notes).toBe("none");

    // Match found → update branch: patch clears notes, no re-stamp.
    const [r2] = await c.mutate({ steps: [step({ notes: null })] });
    expect(r2).toEqual({ id: docId, inserted: false });
    expect(await c.query(api.tasks.get(docId))).not.toHaveProperty("notes");

    // An update-branch no-op patch still must not re-apply the default.
    const [r3] = await c.mutate({ steps: [step({})] });
    expect(r3).toEqual({ id: docId, inserted: false });
    expect(await c.query(api.tasks.get(docId))).not.toHaveProperty("notes");
  });

  it("respects an explicit value over the default on replace", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "tasks", doc: { title: "a" } }],
    });
    const id = (res as { id: string }).id;
    await c.mutate({
      steps: [{ op: "replace", table: "tasks", id, doc: { title: "b", priority: 7 } }],
    });
    const doc = await c.query(api.tasks.get(id));
    expect(doc?.priority).toBe(7);
    expect(doc?.status).toBe("backlog");
  });
});

describe("InMemoryRtDbClient — ttl default wins over a defaults entry", () => {
  const ttlSchema = defineSchema({
    sessions: defineTable({ expiresAt: t.number(), kind: t.string() })
      .index("by_expiresAt", ["expiresAt"])
      .ttl("expiresAt", 1000)
      .defaults({ expiresAt: 999_999, kind: "session" }),
  });
  const ttlApi = createApi(ttlSchema);

  it("stamps the ttl default on the same field, not the defaults entry", async () => {
    const now = 10_000;
    const c = new InMemoryRtDbClient({ now: () => now, random: () => 0 });
    c.pushSchema(ttlSchema);

    const [res] = await c.mutate({
      steps: [{ op: "insert", table: "sessions", doc: {} }],
    });
    const id = (res as { id: string }).id;
    const doc = await c.query(ttlApi.sessions.get(id));
    expect(doc?.expiresAt).toBe(now + 1000);
    expect(doc?.kind).toBe("session");
  });
});
