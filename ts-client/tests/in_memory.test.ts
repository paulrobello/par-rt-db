import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { evalFilterExpr, InMemoryRtDbClient, validateFilter } from "../src/in_memory.js";
import { mutation } from "../src/mutation.js";
import { decodeCursor, encodeCursor } from "../src/pagination.js";
import type { FilterExpr, PaginatedResultJson, ScheduleInfo } from "../src/protocol.js";
import { createApi, type RtQuery } from "../src/query.js";
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

/** Fixed (non-incrementing) clock so schedule due-times are stable under `tick`. */
function newClockClient(): { c: InMemoryRtDbClient; setNow: (t: number) => void } {
  let ms = 1_700_000_000_000;
  const c = new InMemoryRtDbClient({ now: () => ms, random: () => 0 });
  c.pushSchema(schema);
  return { c, setNow: (t) => (ms = t) };
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

  it("distinct returns the unique values of the next index field over the matching set", async () => {
    const c = newClient();
    await seed(c); // 3× todo (orders 3, 1, 2)
    // by_status_and_order has [status, order]; consuming `status` leaves `order`
    // as the distinct field. The set of unique orders within todo rows is
    // {1, 2, 3}, returned ascending.
    const values = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).distinct(),
    );
    expect(values).toEqual([1, 2, 3]);
  });

  it("distinct narrows to the matching set when given a range bound on the distinct field", async () => {
    const c = newClient();
    await seed(c);
    const values = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).gt(1).distinct(),
    );
    // Orders > 1 within todo rows = {2, 3}.
    expect(values).toEqual([2, 3]);
  });

  it("distinct rejects when the eq prefix consumes every index field", async () => {
    const c = newClient();
    await seed(c);
    await expect(
      c.query(api.items.query().withIndex("by_status_and_order", ["todo", 1]).distinct()),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /distinct requires an index field beyond the eq prefix/,
    });
  });

  it("aggregate sums the next index field over the matching set", async () => {
    const c = newClient();
    await seed(c); // 3× todo with orders 3, 1, 2 → sum = 6
    const v = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).aggregate("sum"),
    );
    expect(v).toBe(6);
  });

  it("aggregate avg / min / max over the matching set", async () => {
    const c = newClient();
    await seed(c); // orders {3,1,2}
    const avg = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).aggregate("avg"),
    );
    expect(avg).toBe(2);
    const min = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).aggregate("min"),
    );
    expect(min).toBe(1);
    const max = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).aggregate("max"),
    );
    expect(max).toBe(3);
  });

  it("aggregate respects a range bound on the aggregate field", async () => {
    const c = newClient();
    await seed(c); // orders {3,1,2}; gt 1 → {3,2} → sum = 5
    const v = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).gt(1).aggregate("sum"),
    );
    expect(v).toBe(5);
  });

  it("aggregate returns null when no rows match", async () => {
    const c = newClient();
    await seed(c); // 3× todo, no `done` rows
    // by_status_and_order has [status, order]; eq=["done"] leaves `order` as
    // the aggregate field. Zero matching rows → sum is null.
    const v = await c.query(
      api.items.query().withIndex("by_status_and_order", ["done"]).aggregate("sum"),
    );
    expect(v).toBeNull();
  });

  it("aggregate sum on a non-numeric index field is bad request", async () => {
    const c = newClient();
    await seed(c);
    // by_name's only field is `name` (string); sum/avg require numeric.
    await expect(
      c.query(api.items.query().withIndex("by_name", ["a"]).aggregate("sum")),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /requires a numeric index field/,
    });
  });

  it("aggregate without an index is bad request", async () => {
    const c = newClient();
    await seed(c);
    await expect(c.query(api.items.query().aggregate("sum"))).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /aggregate requires an index field beyond the eq prefix/,
    });
  });

  it("aggregate groupBy returns one {key,value} per group, ordered by key", async () => {
    const c = newClient();
    // Seed three statuses; each `status` group sums its `order` values.
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    await c.mutate(mutation().insert("items", { name: "b", status: "todo", order: 2 }).build());
    await c.mutate(mutation().insert("items", { name: "c", status: "done", order: 5 }).build());
    // by_status_and_order has [status, order]; eq=[] leaves status as group key
    // and order as the aggregate field.
    const rows = (await c.query(
      api.items.query().withIndex("by_status_and_order", []).aggregate("sum", true),
    )) as { key: unknown; value: unknown }[];
    expect(rows).toEqual([
      { key: "done", value: 5 },
      { key: "todo", value: 3 },
    ]);
  });

  it("aggregate groupBy requires two index fields beyond the eq prefix", async () => {
    const c = newClient();
    await seed(c);
    // by_status has only one field; groupBy needs two (one to group by, one to
    // aggregate). Without an eq prefix, eqLen=0 so eqLen+1 >= fields.length (1).
    await expect(
      c.query(api.items.query().withIndex("by_status", []).aggregate("sum", true)),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /requires two index fields beyond the eq prefix/,
    });
  });

  it("aggregate combined with take is bad request", async () => {
    const c = newClient();
    await seed(c);
    await expect(
      c.query({
        json: {
          table: "items",
          index: "by_status_and_order",
          eq: ["todo"],
          aggregate: { op: "sum" },
          take: 10,
        },
      } as RtQuery<unknown>),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /aggregate cannot be combined with take/,
    });
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

describe("InMemoryRtDbClient — paginate (cursor keyset)", () => {
  /** Inserts `count` items with `order` = 1..count and `status` cycling through
   * `statuses`. The deterministic clock/RNG make both `_creationTime` and `_id`
   * rise with insertion order, so an ascending sort yields insertion order. */
  async function seedItems(c: InMemoryRtDbClient, count: number, statuses: string[]) {
    const ids: string[] = [];
    for (let i = 1; i <= count; i++) {
      const [res] = await c.mutate(
        mutation()
          .insert("items", {
            name: `n${i}`,
            status: statuses[(i - 1) % statuses.length],
            order: i,
          })
          .build(),
      );
      ids.push((res as { id: string }).id);
    }
    return ids;
  }

  /** Walks the full cursor chain until `nextCursor` is absent. */
  async function walkPages(
    c: InMemoryRtDbClient,
    build: (cursor: string | undefined) => RtQuery<PaginatedResultJson>,
  ): Promise<{
    pageSizes: number[];
    cursors: (string | undefined)[];
    docs: Record<string, unknown>[];
  }> {
    const pageSizes: number[] = [];
    const cursors: (string | undefined)[] = [];
    const docs: Record<string, unknown>[] = [];
    let cursor: string | undefined;
    for (let guard = 0; guard < 1000; guard++) {
      const page: PaginatedResultJson = await c.query(build(cursor));
      pageSizes.push(page.docs.length);
      cursors.push(page.nextCursor);
      docs.push(...(page.docs as Record<string, unknown>[]));
      if (page.nextCursor === undefined) {
        return { pageSizes, cursors, docs };
      }
      cursor = page.nextCursor;
    }
    throw new Error("pagination did not terminate");
  }

  it("returns an empty page with no nextCursor on an empty table", async () => {
    const c = newClient();
    const page = await c.query(api.items.query().paginate(undefined, 3));
    expect(page).toEqual({ docs: [], nextCursor: undefined });
  });

  it("walks all pages in order, terminating on a short last page", async () => {
    const c = newClient();
    await seedItems(c, 7, ["todo"]);
    const { pageSizes, cursors, docs } = await walkPages(c, (cursor) =>
      api.items.query().paginate(cursor, 3),
    );
    // Page sizes 3, 3, 1; the walk must equal a plain collect() with no skips/dups.
    expect(pageSizes).toEqual([3, 3, 1]);
    expect(cursors.slice(0, -1).every((x) => x !== undefined)).toBe(true);
    expect(cursors[cursors.length - 1]).toBeUndefined();

    const collected = (await c.query(api.items.query().collect())) as unknown as Record<
      string,
      unknown
    >[];
    expect(docs.map((d) => d._id)).toEqual(collected.map((d) => d._id));
    expect(new Set(docs.map((d) => d._id)).size).toBe(docs.length);
  });

  it("terminates on a full last page when the count is an exact multiple", async () => {
    const c = newClient();
    await seedItems(c, 6, ["todo"]);
    const { pageSizes, cursors, docs } = await walkPages(c, (cursor) =>
      api.items.query().paginate(cursor, 3),
    );
    expect(pageSizes).toEqual([3, 3]);
    expect(cursors).toEqual([expect.any(String), undefined]);
    expect(docs).toHaveLength(6);
  });

  it("paginates within an eq-prefixed multi-field index in index order", async () => {
    const c = newClient();
    // status cycles todo/done/todo ⇒ todos are orders 1,3,4,6,7,9.
    await seedItems(c, 9, ["todo", "done", "todo"]);
    const { pageSizes, docs } = await walkPages(c, (cursor) =>
      api.items.query().withIndex("by_status_and_order", ["todo"]).paginate(cursor, 4),
    );
    expect(pageSizes).toEqual([4, 2]);
    expect(docs.map((d) => d.order)).toEqual([1, 3, 4, 6, 7, 9]);
    expect(docs.every((d) => d.status === "todo")).toBe(true);
  });

  it("walks descending pages in reverse index order", async () => {
    const c = newClient();
    await seedItems(c, 9, ["todo", "done", "todo"]); // todo orders 1,3,4,6,7,9
    const { pageSizes, docs } = await walkPages(c, (cursor) =>
      api.items
        .query()
        .withIndex("by_status_and_order", ["todo"])
        .order("desc")
        .paginate(cursor, 4),
    );
    expect(pageSizes).toEqual([4, 2]);
    expect(docs.map((d) => d.order)).toEqual([9, 7, 6, 4, 3, 1]);
  });

  it("emits cursors decodable by the live client; resume continues the chain", async () => {
    const c = newClient();
    await seedItems(c, 5, ["todo"]); // todo orders 1..5
    const first = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).paginate(undefined, 2),
    );
    expect((first.docs as Record<string, unknown>[]).map((d) => d.order)).toEqual([1, 2]);
    expect(first.nextCursor).toBeDefined();

    // The live client decodes the in-memory-produced cursor to the last row's
    // key tuple [order, _creationTime, _id] — cursors are interchangeable.
    const nextCursor = first.nextCursor;
    if (nextCursor === undefined) {
      throw new Error("expected a nextCursor on the first page");
    }
    const decoded = decodeCursor(nextCursor) as unknown[];
    const last = first.docs[1] as Record<string, unknown>;
    expect(decoded).toEqual([last.order, last._creationTime, last._id]);

    const second = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).paginate(nextCursor, 2),
    );
    expect((second.docs as Record<string, unknown>[]).map((d) => d.order)).toEqual([3, 4]);
  });

  it("rejects a malformed (non-base64) cursor with BAD_REQUEST, not INTERNAL", async () => {
    const c = newClient();
    await seedItems(c, 3, ["todo"]);
    await expect(
      c.query(api.items.query().paginate("not-valid-base64!!!", 3)),
    ).rejects.toMatchObject({ name: "RtDbError", code: "BAD_REQUEST" });
  });

  it("rejects a cursor whose arity mismatches the sort columns", async () => {
    const c = newClient();
    await seedItems(c, 3, ["todo"]);
    // No-index query sorts over 2 columns (createdAt, id); 3 values mismatch.
    const bad = encodeCursor([1, 2, 3]);
    await expect(c.query(api.items.query().paginate(bad, 3))).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /sorts over 2 column\(s\)/,
    });
  });

  it("rejects a cursor whose created_at value is not a number", async () => {
    const c = newClient();
    await seedItems(c, 3, ["todo"]);
    // No-index cursor = [createdAt, id]; a non-numeric createdAt fails type-check.
    const bad = encodeCursor(["not-a-number", "0123456789abcdef0123456789abcdef"]);
    await expect(c.query(api.items.query().paginate(bad, 3))).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /created_at must be a number/,
    });
  });

  it("rejects paginate combined with take or count", async () => {
    const c = newClient();
    await seedItems(c, 3, ["todo"]);
    await expect(
      c.query({
        json: { table: "items", paginate: { numItems: 3 }, take: 3 },
      } as RtQuery<PaginatedResultJson>),
    ).rejects.toMatchObject({ name: "RtDbError", code: "BAD_REQUEST", message: /take/ });
    await expect(
      c.query({
        json: { table: "items", paginate: { numItems: 3 }, count: true },
      } as RtQuery<PaginatedResultJson>),
    ).rejects.toMatchObject({ name: "RtDbError", code: "BAD_REQUEST", message: /count/ });
  });
});

describe("InMemoryRtDbClient — schedules", () => {
  const insertTxn = mutation().insert("items", { name: "a", status: "todo", order: 1 }).build();
  const BASE = 1_700_000_000_000;

  it("schedule + tick fires a due one-shot and the write is visible via query", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });
    expect(id).toMatch(HEX_ID);

    setNow(BASE + 2000); // past the due time
    c.tick();

    const docs = await c.query(api.items.query().collect());
    expect(docs).toHaveLength(1);
    expect((docs[0] as { name: string }).name).toBe("a");
    // A fired one-shot is removed from the schedule registry.
    const remaining = await c.listSchedules();
    expect(remaining.find((s) => s.id === id)).toBeUndefined();
  });

  it("does not fire a not-yet-due one-shot on tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    await c.schedule(insertTxn, { type: "afterMs", ms: 5000 });

    setNow(BASE + 1000); // before the due time
    c.tick();

    const docs = await c.query(api.items.query().collect());
    expect(docs).toHaveLength(0);
  });

  it("a paused scheduled job does not fire on tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });
    await c.pauseSchedule(id);

    setNow(BASE + 2000); // due, but paused
    c.tick();

    const docs = await c.query(api.items.query().collect());
    expect(docs).toHaveLength(0);
    const info = (await c.listSchedules()).find((s) => s.id === id) as ScheduleInfo;
    expect(info.status).toBe("paused");
  });

  it("cancelSchedule removes the job so it does not fire on tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });
    await c.cancelSchedule(id);
    expect((await c.listSchedules()).find((s) => s.id === id)).toBeUndefined();

    setNow(BASE + 2000);
    c.tick();

    const docs = await c.query(api.items.query().collect());
    expect(docs).toHaveLength(0);
  });

  it("pause then resume lets the job fire on a later tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });
    await c.pauseSchedule(id);
    setNow(BASE + 2000);
    c.tick();
    expect(await c.query(api.items.query().collect())).toHaveLength(0);

    await c.resumeSchedule(id);
    expect(((await c.listSchedules()).find((s) => s.id === id) as ScheduleInfo).status).toBe(
      "pending",
    );
    c.tick();
    expect(await c.query(api.items.query().collect())).toHaveLength(1);
  });

  it("listSchedules returns schedule info with server-aligned status/kind names", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "cron", expr: "* * * * *" });

    const list = await c.listSchedules();
    expect(list).toHaveLength(1);
    const info = list[0];
    expect(info.id).toBe(id);
    expect(info.kind).toBe("cron");
    expect(info.status).toBe("pending");
    expect(info.cron).toBe("* * * * *");
    expect(info.firedCount).toBe(0);
    expect(typeof info.dueAt).toBe("number");
    expect(typeof info.createdAt).toBe("number");
  });

  it("cancel/pause/resume on an unknown id reject with NOT_FOUND", async () => {
    const { c } = newClockClient();
    await expect(c.cancelSchedule("nope")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
    });
    await expect(c.pauseSchedule("nope")).rejects.toMatchObject({ code: "NOT_FOUND" });
    await expect(c.resumeSchedule("nope")).rejects.toMatchObject({ code: "NOT_FOUND" });
  });
});

describe("evalFilterExpr + validateFilter", () => {
  const fields = new Set(["name", "age", "active", "score", "tags"]);

  it("eq/neq on strings compare the doc field's text", () => {
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: "ada" })).toBe(true);
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: "bob" })).toBe(false);
    expect(evalFilterExpr({ op: "neq", field: "name", value: "ada" }, { name: "bob" })).toBe(true);
  });

  it("number domain compares numerically (gt/gte/lt/lte)", () => {
    expect(evalFilterExpr({ op: "gt", field: "age", value: 30 }, { age: 42 })).toBe(true);
    expect(evalFilterExpr({ op: "gt", field: "age", value: 50 }, { age: 42 })).toBe(false);
    expect(evalFilterExpr({ op: "lte", field: "age", value: 42 }, { age: 42 })).toBe(true);
  });

  it("string ordering is lexicographic", () => {
    expect(evalFilterExpr({ op: "lt", field: "name", value: "b" }, { name: "ada" })).toBe(true);
    expect(evalFilterExpr({ op: "gte", field: "name", value: "a" }, { name: "ada" })).toBe(true);
  });

  it("boolean domain compares booleans", () => {
    expect(evalFilterExpr({ op: "eq", field: "active", value: true }, { active: true })).toBe(true);
    expect(evalFilterExpr({ op: "eq", field: "active", value: true }, { active: false })).toBe(
      false,
    );
  });

  it("a number filter value matches a numeric string field (float8 cast)", () => {
    expect(evalFilterExpr({ op: "eq", field: "score", value: 5 }, { score: "5" })).toBe(true);
  });

  it("null/absent doc field never matches (SQL NULL exclusion)", () => {
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, { name: null })).toBe(false);
    expect(evalFilterExpr({ op: "eq", field: "name", value: "ada" }, {})).toBe(false);
    expect(evalFilterExpr({ op: "neq", field: "name", value: "ada" }, {})).toBe(false);
  });

  it("and / or nest recursively", () => {
    const expr: FilterExpr = {
      op: "and",
      exprs: [
        { op: "gte", field: "age", value: 30 },
        {
          op: "or",
          exprs: [
            { op: "eq", field: "name", value: "ada" },
            { op: "eq", field: "name", value: "bob" },
          ],
        },
      ],
    };
    expect(evalFilterExpr(expr, { age: 42, name: "ada" })).toBe(true);
    expect(evalFilterExpr(expr, { age: 42, name: "zed" })).toBe(false);
    expect(evalFilterExpr(expr, { age: 10, name: "ada" })).toBe(false);
  });

  it("in matches membership", () => {
    expect(
      evalFilterExpr({ op: "in", field: "name", values: ["ada", "bob"] }, { name: "bob" }),
    ).toBe(true);
    expect(
      evalFilterExpr({ op: "in", field: "name", values: ["ada", "bob"] }, { name: "zed" }),
    ).toBe(false);
  });

  it("validateFilter rejects an unknown field", () => {
    expect(() => validateFilter({ op: "eq", field: "missing", value: "x" }, fields)).toThrow(
      /unknown field/,
    );
  });

  it("validateFilter rejects empty and/or and empty in", () => {
    expect(() => validateFilter({ op: "and", exprs: [] }, fields)).toThrow(/at least one expr/);
    expect(() => validateFilter({ op: "or", exprs: [] }, fields)).toThrow(/at least one expr/);
    expect(() => validateFilter({ op: "in", field: "name", values: [] }, fields)).toThrow(
      /at least one value/,
    );
  });

  it("validateFilter rejects a non-string/number/boolean value", () => {
    expect(() => validateFilter({ op: "eq", field: "name", value: null }, fields)).toThrow(
      /string, number, or boolean/,
    );
    expect(() => validateFilter({ op: "eq", field: "tags", value: ["a"] }, fields)).toThrow(
      /string, number, or boolean/,
    );
  });

  it("validateFilter accepts a well-formed nested filter", () => {
    expect(() =>
      validateFilter(
        {
          op: "and",
          exprs: [
            { op: "eq", field: "name", value: "ada" },
            { op: "in", field: "age", values: [1, 2] },
          ],
        },
        fields,
      ),
    ).not.toThrow();
  });

  it("validateFilter rejects mixed-type in values", () => {
    expect(() => validateFilter({ op: "in", field: "age", values: [5, "ada"] }, fields)).toThrow(
      /same type/,
    );
  });

  it("validateFilter accepts same-type in values", () => {
    expect(() =>
      validateFilter({ op: "in", field: "age", values: [5, 6, 7] }, fields),
    ).not.toThrow();
  });
});

describe("InMemoryRtDbClient filter", () => {
  // Self-contained schema so this block doesn't perturb the shared `items`
  // harness above. Uses the typed `api.users.query().filter(...)` builder, the
  // same surface live app code uses.
  const usersSchema = defineSchema({
    users: defineTable({
      name: t.string(),
      age: t.number(),
      active: t.boolean(),
    }).index("by_name", ["name"]),
  });
  const usersApi = createApi(usersSchema);

  function newClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(usersSchema);
    return c;
  }

  async function seed(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation()
        .insert("users", { name: "ada", age: 42, active: true })
        .insert("users", { name: "bob", age: 17, active: false })
        .insert("users", { name: "cy", age: 65, active: true })
        .build(),
    );
  }

  it("a filter reduces the result set to matching docs", async () => {
    const c = newClient();
    await seed(c);
    const rows = await c.query(
      usersApi.users.query().filter({ op: "gt", field: "age", value: 20 }).collect(),
    );
    const names = (rows as Array<{ name: string }>).map((r) => r.name).sort();
    expect(names).toEqual(["ada", "cy"]);
  });

  it("a filter composes with an index eq prefix and take", async () => {
    const c = newClient();
    await seed(c);
    const rows = await c.query(
      usersApi.users
        .query()
        .withIndex("by_name", ["ada"])
        .filter({ op: "eq", field: "active", value: true })
        .take(10),
    );
    expect(rows).toHaveLength(1);
    expect((rows as Array<{ name: string }>)[0].name).toBe("ada");
  });

  it("an and/or/in filter evaluates correctly end-to-end", async () => {
    const c = newClient();
    await seed(c);
    const rows = await c.query(
      usersApi.users
        .query()
        .filter({
          op: "or",
          exprs: [
            { op: "lt", field: "age", value: 18 },
            { op: "gte", field: "age", value: 65 },
          ],
        })
        .collect(),
    );
    expect((rows as Array<{ name: string }>).map((r) => r.name).sort()).toEqual(["bob", "cy"]);

    const inRows = await c.query(
      usersApi.users
        .query()
        .filter({ op: "in", field: "name", values: ["ada", "cy"] })
        .collect(),
    );
    expect((inRows as Array<{ name: string }>).map((r) => r.name).sort()).toEqual(["ada", "cy"]);
  });

  it("an unknown filter field throws BAD_REQUEST", async () => {
    const c = newClient();
    await seed(c);
    await expect(
      c.query(usersApi.users.query().filter({ op: "eq", field: "nope", value: "x" }).collect()),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });

  it("filter combined with get is rejected (mirrors server)", async () => {
    const c = newClient();
    await seed(c);
    const [inserted] = await c.mutate(
      mutation().insert("users", { name: "ada", age: 42, active: true }).build(),
    );
    const id = (inserted as { id: string }).id;
    await expect(
      c.query({
        json: { table: "users", get: id, filter: { op: "eq", field: "age", value: 42 } },
      } as RtQuery<unknown>),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });
});
