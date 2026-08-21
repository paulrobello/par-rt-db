import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import type { FieldTypeJson, SchemaJson } from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

// FM-37 server-assigned autoIncrementField — engine-level mirror of the
// server's `server/tests/auto_increment_test.rs`: push-time validation
// (undeclared / non-int64 / ttl and updatedAt collisions), insert authority
// (sequential assignment overwriting client-supplied values, the stamp
// winning over a defaults entry), and post-insert immutability (patch /
// replace / upsert-update / patchByQuery rejections with round-trip-friendly
// equal values). The server's concurrency, unique-index-via-snapshot-import,
// and sequence-repositioning cases are Postgres-sequence specifics with no
// in-memory counterpart (the engine's counter is a plain monotonic map).

const counterSchema = defineSchema({
  tickets: defineTable({
    title: t.string(),
    num: t.int64(),
  })
    .index("by_title", ["title"])
    .autoIncrementField("num"),
});

const api = createApi(counterSchema);

function makeClient(schema: SchemaJson = counterSchema.toJSON()): InMemoryRtDbClient {
  const c = new InMemoryRtDbClient({ now: () => 1_000, random: () => 0 });
  c.pushSchema(schema);
  return c;
}

async function insert(c: InMemoryRtDbClient, doc: Record<string, unknown>): Promise<string> {
  const [res] = await c.mutate({ steps: [{ op: "insert", table: "tickets", doc }] });
  return (res as { id: string }).id;
}

async function counterOf(c: InMemoryRtDbClient, id: string): Promise<string> {
  const doc = await c.query(api.tickets.get(id));
  expect(typeof doc?.num).toBe("string"); // int64 travels as decimal strings
  return doc?.num as string;
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

/** Step-path assertions — BAD_REQUEST with the server's message. */
async function shouldReject(promise: Promise<unknown>, message: RegExp): Promise<void> {
  try {
    await promise;
  } catch (err) {
    expect(err).toBeInstanceOf(RtDbError);
    expect((err as RtDbError).code).toBe("BAD_REQUEST");
    expect((err as RtDbError).message).toMatch(message);
    return;
  }
  throw new Error(`expected the mutation to reject with /${message.source}/`);
}

// ---- wire shape ----

describe("autoIncrementField wire shape (FM-37)", () => {
  it("emits autoIncrementField on the wire when declared", () => {
    expect(counterSchema.toJSON()).toEqual({
      tables: {
        tickets: {
          fields: { title: { type: "string" }, num: { type: "int64" } },
          indexes: [{ name: "by_title", fields: ["title"] }],
          autoIncrementField: "num",
        },
      },
    });
  });

  it("omits autoIncrementField on the wire when the table declares none", () => {
    const s = defineSchema({ things: defineTable({ n: t.number() }) });
    expect(s.toJSON().tables.things).toEqual({ fields: { n: { type: "number" } } });
    expect(s.toJSON().tables.things).not.toHaveProperty("autoIncrementField");
  });
});

// ---- push-time validation ----

describe("autoIncrementField push validation (FM-37)", () => {
  it("rejects an autoIncrementField that is not a declared field", () => {
    pushShouldReject(
      {
        tables: { tickets: { fields: { title: { type: "string" } }, autoIncrementField: "nope" } },
      },
      /autoIncrementField 'nope' is not a declared field/,
    );
  });

  it("rejects a non-int64 autoIncrementField (number, string, optional)", () => {
    const nonInt64: FieldTypeJson[] = [
      { type: "number" },
      { type: "string" },
      { type: "optional", inner: { type: "int64" } },
    ];
    for (const ty of nonInt64) {
      pushShouldReject(
        {
          tables: {
            tickets: { fields: { title: { type: "string" }, num: ty }, autoIncrementField: "num" },
          },
        },
        /autoIncrementField 'num' must be an int64 field/,
      );
    }
  });

  it("rejects a counter colliding with ttl.field", () => {
    pushShouldReject(
      {
        tables: {
          tickets: {
            fields: { title: { type: "string" }, num: { type: "int64" } },
            indexes: [{ name: "by_num", fields: ["num"] }],
            ttl: { field: "num" },
            autoIncrementField: "num",
          },
        },
      },
      /autoIncrementField 'num' must differ from ttl\.field/,
    );
  });

  it("rejects a counter colliding with updatedAtField", () => {
    pushShouldReject(
      {
        tables: {
          tickets: {
            fields: { title: { type: "string" }, num: { type: "int64" } },
            autoIncrementField: "num",
            updatedAtField: "num",
          },
        },
      },
      /autoIncrementField 'num' must differ from updatedAtField/,
    );
  });

  it("accepts and round-trips a declared int64 autoIncrementField", () => {
    expect(() => makeClient()).not.toThrow();
  });
});

// ---- insert authority ----

describe("InMemoryRtDbClient — server-assigned autoIncrementField", () => {
  it("insert assigns sequential values and overwrites a client-supplied value", async () => {
    const c = makeClient();

    // A client-supplied value (even a plausible one) is overwritten: the
    // first insert is 1 regardless.
    const id = await insert(c, { title: "A", num: "999" });
    expect(await counterOf(c, id)).toBe("1");

    const id2 = await insert(c, { title: "B" });
    expect(await counterOf(c, id2)).toBe("2");

    const id3 = await insert(c, { title: "C" });
    expect(await counterOf(c, id3)).toBe("3");
  });

  it("upsert's insert branch assigns the same way", async () => {
    const c = makeClient();
    const [res] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "tickets",
          index: "by_title",
          eq: ["A"],
          insert: { title: "A", num: "999" },
          patch: { title: "A" },
        },
      ],
    });
    expect(res).toEqual({ id: expect.any(String), inserted: true });
    expect(await counterOf(c, (res as { id: string }).id)).toBe("1");
  });

  it("the stamp wins over a defaults entry on the same field", async () => {
    const schema = defineSchema({
      tickets: defineTable({
        title: t.string(),
        num: t.int64(),
      })
        .index("by_title", ["title"])
        .defaults({ num: "42" })
        .autoIncrementField("num"),
    });
    const c = makeClient(schema.toJSON());
    const id = await insert(c, { title: "A" });
    expect(await counterOf(c, id)).toBe("1"); // not "42"
  });

  it("distinct values satisfy a unique index on the counter", async () => {
    const schema = defineSchema({
      tickets: defineTable({
        title: t.string(),
        num: t.int64(),
      })
        .index("by_title", ["title"])
        .index("by_num", ["num"])
        .unique()
        .autoIncrementField("num"),
    });
    const c = makeClient(schema.toJSON());
    const ids = [await insert(c, { title: "A" }), await insert(c, { title: "B" })];
    expect(await counterOf(c, ids[0])).toBe("1");
    expect(await counterOf(c, ids[1])).toBe("2"); // no CONFLICT — distinct by construction
  });

  // ---- post-insert immutability ----

  it("patch cannot change the counter; round-tripping the value is allowed", async () => {
    const c = makeClient();
    const id = await insert(c, { title: "A" });

    await shouldReject(
      c.mutate({ steps: [{ op: "patch", table: "tickets", id, fields: { num: "99" } }] }),
      /autoIncrementField 'num' cannot be changed/,
    );
    // A type-shifted form of the same number is still a change.
    await shouldReject(
      c.mutate({ steps: [{ op: "patch", table: "tickets", id, fields: { num: 1 } }] }),
      /autoIncrementField 'num' cannot be changed/,
    );

    // Round-tripping the same value is allowed.
    await c.mutate({ steps: [{ op: "patch", table: "tickets", id, fields: { num: "1" } }] });
    expect(await counterOf(c, id)).toBe("1");
  });

  it("replace preserves the stored value when omitted and rejects a change", async () => {
    const c = makeClient();
    const id = await insert(c, { title: "A" });

    // A replace that omits the field keeps the stored value (it validates as
    // a complete document only because the engine fills it back in).
    await c.mutate({
      steps: [{ op: "replace", table: "tickets", id, doc: { title: "A2" } }],
    });
    expect(await counterOf(c, id)).toBe("1");
    expect((await c.query(api.tickets.get(id)))?.title).toBe("A2");

    // A replace that changes the value is rejected.
    await shouldReject(
      c.mutate({
        steps: [{ op: "replace", table: "tickets", id, doc: { title: "A3", num: "5" } }],
      }),
      /autoIncrementField 'num' cannot be changed/,
    );

    // Round-tripping the stored value works.
    await c.mutate({
      steps: [{ op: "replace", table: "tickets", id, doc: { title: "A4", num: "1" } }],
    });
    expect(await counterOf(c, id)).toBe("1");
    expect((await c.query(api.tickets.get(id)))?.title).toBe("A4");
  });

  it("upsert-update preserves the counter and rejects changing it", async () => {
    const c = makeClient();

    const [first] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "tickets",
          index: "by_title",
          eq: ["A"],
          insert: { title: "A" },
          patch: { title: "A" },
        },
      ],
    });
    const id = (first as { id: string }).id;
    expect(await counterOf(c, id)).toBe("1");

    // Update branch: a patch without the counter preserves it.
    const [second] = await c.mutate({
      steps: [
        {
          op: "upsert",
          table: "tickets",
          index: "by_title",
          eq: ["A"],
          insert: { title: "A" },
          patch: { title: "A2" },
        },
      ],
    });
    expect(second).toEqual({ id, inserted: false });
    expect(await counterOf(c, id)).toBe("1");

    // Update branch: changing the counter is rejected.
    await shouldReject(
      c.mutate({
        steps: [
          {
            op: "upsert",
            table: "tickets",
            index: "by_title",
            eq: ["A2"],
            insert: { title: "A2" },
            patch: { num: "7" },
          },
        ],
      }),
      /autoIncrementField 'num' cannot be changed/,
    );
  });

  it("patchByQuery cannot change the counter", async () => {
    const c = makeClient();
    const id = await insert(c, { title: "A" });

    await shouldReject(
      c.mutate({
        steps: [
          {
            op: "patchByQuery",
            table: "tickets",
            filter: { op: "eq", field: "title", value: "A" },
            patch: { num: "50" },
          },
        ],
      }),
      /autoIncrementField 'num' cannot be changed/,
    );
    expect(await counterOf(c, id)).toBe("1");
  });
});
