import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory.js";
import { mutation } from "../src/mutation.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  items: defineTable({
    name: t.string(),
    status: t.string(),
    order: t.number(),
    note: t.optional(t.string()),
  })
    .index("by_name", ["name"])
    .index("by_status", ["status"])
    .index("by_status_and_order", ["status", "order"]),
});

const api = createApi(schema);

/** Deterministic clock + RNG so ids, `_creationTime`, and ordering are stable. */
function newClient(): InMemoryRtDbClient {
  let ms = 1_700_000_000_000;
  const client = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
  client.pushSchema(schema);
  return client;
}

const HEX_ID = /^[0-9a-f]{32}$/;

describe("InMemoryRtDbClient — schema push", () => {
  it("collects [] from an empty table after pushSchema", async () => {
    const c = newClient();
    const docs = await c.query(api.items.query().collect());
    expect(docs).toEqual([]);
  });

  it("point-reads a missing id as null", async () => {
    const c = newClient();
    expect(await c.query(api.items.get("0123456789abcdef0123456789abcdef"))).toBeNull();
  });
});

describe("InMemoryRtDbClient — insert + read", () => {
  it("inserts a doc and merges the system fields at read time", async () => {
    const c = newClient();
    const [res] = await c.mutate(
      mutation().insert("items", { name: "a", status: "todo", order: 1 }).build(),
    );
    const id = (res as { id: string }).id;
    expect(id).toMatch(HEX_ID);

    const doc = await c.query(api.items.get(id));
    expect(doc).not.toBeNull();
    expect(doc).toMatchObject({
      _id: id,
      name: "a",
      status: "todo",
      order: 1,
      _version: 1,
    });
    expect(typeof (doc as { _creationTime: number })._creationTime).toBe("number");
  });

  it("strips an optional field set to null on insert (server strip_unset_optionals)", async () => {
    const c = newClient();
    const [res] = await c.mutate({
      steps: [
        { op: "insert", table: "items", doc: { name: "a", status: "todo", order: 1, note: null } },
      ],
    });
    const doc = await c.query(api.items.get((res as { id: string }).id));
    expect(doc).not.toHaveProperty("note");
  });

  it("rejects an insert missing a required field (SCHEMA_VIOLATION)", async () => {
    const c = newClient();
    await expect(
      c.mutate({
        steps: [{ op: "insert", table: "items", doc: { status: "todo", order: 1 } }],
      }),
    ).rejects.toMatchObject({ name: "RtDbError", code: "SCHEMA_VIOLATION" });
  });
});

describe("InMemoryRtDbClient — upsert by index", () => {
  const upsertTxn = (patch: Record<string, unknown>) =>
    mutation()
      .upsert("items", {
        index: "by_name",
        eq: ["a"],
        insert: { name: "a", status: "todo", order: 1 },
        patch,
      })
      .build();

  it("inserts on no match (inserted: true) and patches on match (inserted: false)", async () => {
    const c = newClient();

    const [first] = await c.mutate(upsertTxn({ order: 2 }));
    expect(first).toMatchObject({ inserted: true });
    const id = (first as { id: string }).id;
    expect(id).toMatch(HEX_ID);

    const [second] = await c.mutate(upsertTxn({ order: 3 }));
    expect(second).toEqual({ id, inserted: false });

    const doc = await c.query(api.items.get(id));
    expect(doc).toMatchObject({ order: 3, _version: 2 });
  });

  it("patches a matched doc onto an index field and reflects it in a later query", async () => {
    const c = newClient();
    await c.mutate(upsertTxn({ order: 2 }));
    const id = ((await c.mutate(upsertTxn({ order: 3 })))[0] as { id: string }).id;

    const matched = await c.query(api.items.query().withIndex("by_name", ["a"]).unique());
    expect((matched as { _id: string })._id).toBe(id);
  });
});

describe("InMemoryRtDbClient — query by index", () => {
  async function seed(c: InMemoryRtDbClient): Promise<void> {
    for (const order of [3, 1, 2]) {
      await c.mutate(
        mutation()
          .insert("items", { name: `n${order}`, status: "todo", order })
          .build(),
      );
    }
  }

  it("filters by an eq index prefix and orders by the remaining index field", async () => {
    const c = newClient();
    await seed(c);

    const asc = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).order("asc").collect(),
    );
    expect(asc.map((d) => (d as { order: number }).order)).toEqual([1, 2, 3]);

    const desc = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).order("desc").take(2),
    );
    expect(desc.map((d) => (d as { order: number }).order)).toEqual([3, 2]);
  });

  it("counts matching rows over an eq prefix", async () => {
    const c = newClient();
    await seed(c);
    const n = await c.query(api.items.query().withIndex("by_status", ["todo"]).count());
    expect(n).toBe(3);
  });

  it("unique throws PRECONDITION_FAILED when more than one doc matches", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "dup", status: "todo", order: 1 }).build());
    await c.mutate(mutation().insert("items", { name: "dup", status: "todo", order: 2 }).build());
    await expect(
      c.query(api.items.query().withIndex("by_name", ["dup"]).unique()),
    ).rejects.toMatchObject({ name: "RtDbError", code: "PRECONDITION_FAILED" });
  });
});

describe("InMemoryRtDbClient — transactions", () => {
  it("runs a multi-step txn and returns one result per step in order", async () => {
    const c = newClient();
    const results = await c.mutate(
      mutation()
        .insert("items", { name: "a", status: "todo", order: 1 })
        .insert("items", { name: "b", status: "todo", order: 2 })
        .build(),
    );
    expect(results).toHaveLength(2);
    for (const r of results) {
      expect((r as { id: string }).id).toMatch(HEX_ID);
    }
    const docs = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(docs).toHaveLength(2);
  });

  it("patches a doc inside a txn and bumps its version", async () => {
    const c = newClient();
    const [ins] = await c.mutate(
      mutation().insert("items", { name: "a", status: "todo", order: 1 }).build(),
    );
    const id = (ins as { id: string }).id;

    await c.mutate(
      mutation().patch("items", id, { order: 9 }).expectVersion("items", id, 2).build(),
    );
    const doc = await c.query(api.items.get(id));
    expect(doc).toMatchObject({ order: 9, _version: 2 });
  });

  it("rolls back the whole txn when a later step fails (atomicity)", async () => {
    const c = newClient();
    const [ins] = await c.mutate(
      mutation().insert("items", { name: "a", status: "todo", order: 1 }).build(),
    );
    const id = (ins as { id: string }).id;

    await expect(
      c.mutate(
        mutation()
          .insert("items", { name: "b", status: "todo", order: 2 })
          .expectVersion("items", id, 999) // mismatch → aborts
          .build(),
      ),
    ).rejects.toBeInstanceOf(RtDbError);

    const docs = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(docs).toHaveLength(1);
    expect((docs[0] as { name: string }).name).toBe("a");
  });
});

describe("InMemoryRtDbClient — subscribe", () => {
  it("delivers the initial value and recomputes only on a real change", async () => {
    const c = newClient();
    const updates: number[] = [];
    const unsub = c.subscribe(api.items.query().withIndex("by_status", ["todo"]).count(), (n) =>
      updates.push(n),
    );

    expect(updates).toEqual([0]); // initial queryUpdate
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    expect(updates).toEqual([0, 1]);
    // A write to a different status does not change the todo count.
    await c.mutate(mutation().insert("items", { name: "b", status: "done", order: 2 }).build());
    expect(updates).toEqual([0, 1]);

    unsub();
    await c.mutate(mutation().insert("items", { name: "c", status: "todo", order: 3 }).build());
    expect(updates).toEqual([0, 1]); // unsubscribed: no further updates
  });
});
