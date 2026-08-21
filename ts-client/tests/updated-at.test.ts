import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import type { SchemaJson } from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

// FM-36 server-stamped updatedAtField — engine-level mirror of the server's
// `server/tests/updated_at_test.rs`. The injected clock makes "restamps"
// assertions deterministic: advance it between mutations and require the
// second stamp to be strictly greater (the server test's `tick()` + `>`).

const numberSchema = defineSchema({
  tasks: defineTable({
    title: t.string(),
    updatedAt: t.number(),
  })
    .index("by_title", ["title"])
    .updatedAtField("updatedAt"),
});

const int64Schema = defineSchema({
  tasks: defineTable({
    title: t.string(),
    updatedAt: t.int64(),
  })
    .index("by_title", ["title"])
    .updatedAtField("updatedAt"),
});

const cascadeSchema = defineSchema({
  parents: defineTable({ name: t.string() }).index("by_name", ["name"]),
  children: defineTable({
    parentId: t.optional(t.id("parents", { onDelete: "setNull" })),
    title: t.string(),
    updatedAt: t.number(),
  })
    .index("by_parentId", ["parentId"])
    .updatedAtField("updatedAt"),
});

const numberApi = createApi(numberSchema);
const int64Api = createApi(int64Schema);
const cascadeApi = createApi(cascadeSchema);

function makeClient(
  schema: SchemaJson,
  startMs = 1_000,
): { c: InMemoryRtDbClient; tick: () => void } {
  let ms = startMs;
  const c = new InMemoryRtDbClient({ now: () => ms, random: () => 0 });
  c.pushSchema(schema);
  return { c, tick: () => (ms += 1_000) };
}

async function insert(c: InMemoryRtDbClient, doc: Record<string, unknown>): Promise<string> {
  const [res] = await c.mutate({ steps: [{ op: "insert", table: "tasks", doc }] });
  return (res as { id: string }).id;
}

/** Push-time validation assertions — SCHEMA_VIOLATION with the server's message. */
function pushShouldReject(schema: SchemaJson, message: RegExp): void {
  const c = new InMemoryRtDbClient();
  try {
    c.pushSchema(schema);
  } catch (err) {
    expect(err).toBeInstanceOf(RtDbError);
    expect((err as RtDbError).code).toBe("SCHEMA_VIOLATION");
    expect((err as RtDbError).message).toMatch(message);
    return;
  }
  throw new Error(`expected pushSchema to reject with /${message.source}/`);
}

describe("updatedAtField wire shape (FM-36)", () => {
  it("emits updatedAtField on the wire when declared", () => {
    expect(numberSchema.toJSON()).toEqual({
      tables: {
        tasks: {
          fields: { title: { type: "string" }, updatedAt: { type: "number" } },
          indexes: [{ name: "by_title", fields: ["title"] }],
          updatedAtField: "updatedAt",
        },
      },
    });
  });

  it("omits updatedAtField on the wire when the table declares none", () => {
    const s = defineSchema({ things: defineTable({ n: t.number() }) });
    expect(s.toJSON().tables.things).toEqual({ fields: { n: { type: "number" } } });
    expect(s.toJSON().tables.things).not.toHaveProperty("updatedAtField");
  });
});

describe("updatedAtField push validation (FM-36)", () => {
  it("rejects an updatedAtField that is not a declared field", () => {
    pushShouldReject(
      { tables: { tasks: { fields: { title: { type: "string" } }, updatedAtField: "updatedAt" } } },
      /updatedAtField 'updatedAt' is not a declared field/,
    );
  });

  it("rejects a non-numeric updatedAtField", () => {
    pushShouldReject(
      {
        tables: {
          tasks: { fields: { updatedAt: { type: "string" } }, updatedAtField: "updatedAt" },
        },
      },
      /updatedAtField 'updatedAt' must be a number or bigint field/,
    );
  });

  it("rejects an updatedAtField equal to ttl.field", () => {
    pushShouldReject(
      {
        tables: {
          tasks: {
            fields: { title: { type: "string" }, updatedAt: { type: "number" } },
            indexes: [{ name: "by_updatedAt", fields: ["updatedAt"] }],
            ttl: { field: "updatedAt" },
            updatedAtField: "updatedAt",
          },
        },
      },
      /updatedAtField 'updatedAt' must differ from ttl\.field/,
    );
  });

  it("accepts and round-trips a declared numeric updatedAtField", () => {
    const c = new InMemoryRtDbClient();
    expect(() => c.pushSchema(numberSchema.toJSON())).not.toThrow();
  });
});

describe("InMemoryRtDbClient — server-stamped updatedAtField", () => {
  it("insert stamps and overwrites a client-supplied value", async () => {
    const { c } = makeClient(numberSchema.toJSON());
    const id = await insert(c, { title: "A", updatedAt: 123 });
    const doc = await c.query(numberApi.tasks.get(id));
    expect(doc?.updatedAt).toBe(1_000); // the stamp, not the client's 123
  });

  it("insert stamps an int64 field as a decimal string", async () => {
    const { c } = makeClient(int64Schema.toJSON());
    const id = await insert(c, { title: "A" });
    const doc = await c.query(int64Api.tasks.get(id));
    expect(typeof doc?.updatedAt).toBe("string");
    expect(doc?.updatedAt).toBe("1000"); // int64 travels as decimal strings
  });

  it("patch restamps and overwrites a client-supplied value", async () => {
    const { c, tick } = makeClient(numberSchema.toJSON());
    const id = await insert(c, { title: "A" });
    expect((await c.query(numberApi.tasks.get(id)))?.updatedAt).toBe(1_000);
    tick();

    await c.mutate({
      steps: [{ op: "patch", table: "tasks", id, fields: { title: "B", updatedAt: 1 } }],
    });
    const doc = await c.query(numberApi.tasks.get(id));
    expect(doc?.updatedAt).toBe(2_000); // restamped, not the client's 1
    expect(doc?.title).toBe("B");
  });

  it("patch restamps even when the patch omits the field", async () => {
    const { c, tick } = makeClient(numberSchema.toJSON());
    const id = await insert(c, { title: "A" });
    tick();

    await c.mutate({ steps: [{ op: "patch", table: "tasks", id, fields: { title: "B" } }] });
    expect((await c.query(numberApi.tasks.get(id)))?.updatedAt).toBe(2_000);
  });

  it("replace restamps and overwrites a client-supplied value", async () => {
    const { c, tick } = makeClient(numberSchema.toJSON());
    const id = await insert(c, { title: "A" });
    tick();

    await c.mutate({
      steps: [{ op: "replace", table: "tasks", id, doc: { title: "A2", updatedAt: 7 } }],
    });
    expect((await c.query(numberApi.tasks.get(id)))?.updatedAt).toBe(2_000);
  });

  it("upsert-insert stamps and upsert-update restamps", async () => {
    const { c, tick } = makeClient(numberSchema.toJSON());

    const [first] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "tasks",
          index: "by_title",
          eq: ["a"],
          insert: { title: "a", updatedAt: 123 },
          patch: { title: "a" },
        },
      ],
    });
    expect(first).toEqual({ id: expect.any(String), inserted: true });
    const id = (first as { id: string }).id;
    expect((await c.query(numberApi.tasks.get(id)))?.updatedAt).toBe(1_000);
    tick();

    const [second] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "tasks",
          index: "by_title",
          eq: ["a"],
          insert: { title: "a" },
          patch: { title: "a2", updatedAt: 1 },
        },
      ],
    });
    expect(second).toEqual({ id, inserted: false });
    const doc = await c.query(numberApi.tasks.get(id));
    expect(doc?.updatedAt).toBe(2_000); // update branch restamped
    expect(doc?.title).toBe("a2");
  });

  it("patchByQuery restamps matched rows only", async () => {
    const { c, tick } = makeClient(numberSchema.toJSON());
    const a = await insert(c, { title: "a" });
    const b = await insert(c, { title: "b" });
    tick();

    const [res] = await c.mutate({
      steps: [
        {
          op: "patchByQuery",
          table: "tasks",
          filter: { op: "eq", field: "title", value: "a" },
          patch: { title: "a2", updatedAt: 1 },
        },
      ],
    });
    expect(res).toEqual({ patched: 1, truncated: false });
    expect((await c.query(numberApi.tasks.get(a)))?.updatedAt).toBe(2_000);
    expect((await c.query(numberApi.tasks.get(b)))?.updatedAt).toBe(1_000); // untouched
  });

  it("cascade setNull restamps the child", async () => {
    const { c, tick } = makeClient(cascadeSchema.toJSON());

    const [parentRes] = await c.mutate({
      steps: [{ op: "insert", table: "parents", doc: { name: "P" } }],
    });
    const parentId = (parentRes as { id: string }).id;
    const [childRes] = await c.mutate({
      steps: [{ op: "insert", table: "children", doc: { parentId, title: "C" } }],
    });
    const childId = (childRes as { id: string }).id;
    expect((await c.query(cascadeApi.children.get(childId)))?.updatedAt).toBe(1_000);
    tick();

    await c.mutate({ steps: [{ op: "delete", table: "parents", id: parentId }] });
    const child = await c.query(cascadeApi.children.get(childId));
    expect(child).not.toHaveProperty("parentId"); // setNull removed the ref
    expect(child?.updatedAt).toBe(2_000); // the setNull patch carried the stamp
  });

  it("stamp wins over a defaults entry on the same field", async () => {
    const schema = defineSchema({
      tasks: defineTable({
        title: t.string(),
        updatedAt: t.number(),
      })
        .index("by_title", ["title"])
        .defaults({ updatedAt: 12_345 })
        .updatedAtField("updatedAt"),
    });
    const { c } = makeClient(schema.toJSON());
    const id = await insert(c, { title: "A" });
    expect((await c.query(createApi(schema).tasks.get(id)))?.updatedAt).toBe(1_000); // not 12345
  });
});
