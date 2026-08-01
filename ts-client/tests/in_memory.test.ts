import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { evalFilterExpr, InMemoryRtDbClient, validateFilter } from "../src/in_memory.js";
import { Migration } from "../src/migration.js";
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

describe("InMemoryRtDbClient — int64 index", () => {
  // Self-contained schema (mirrors the items/users pattern). `ts` is a
  // decimal-string int64 — the comparator must parse it to BigInt so ordering
  // and ranges are numeric, not lexicographic. eq stays string === string.
  const int64Schema = defineSchema({
    events: defineTable({
      ts: t.int64(),
      kind: t.string(),
    }).index("by_ts", ["ts"]),
  });
  const int64Api = createApi(int64Schema);

  function newClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(int64Schema);
    return c;
  }

  async function seed(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(mutation().insert("events", { ts: "100", kind: "a" }).build());
    await c.mutate(mutation().insert("events", { ts: "20", kind: "b" }).build());
    await c.mutate(mutation().insert("events", { ts: "3", kind: "c" }).build());
  }

  it("orders ascending by numeric value, not lexicographic string order", async () => {
    const c = newClient();
    await seed(c);

    const asc = await c.query(int64Api.events.query().withIndex("by_ts").order("asc").take(10));
    // Numeric: 3, 20, 100 — lexicographic would be "100","20","3".
    expect((asc as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["c", "b", "a"]);

    const desc = await c.query(int64Api.events.query().withIndex("by_ts").order("desc").take(10));
    expect((desc as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["a", "b", "c"]);
  });

  it("ranges numerically (gte/gt/lt/lte bounds compare as BigInt)", async () => {
    const c = newClient();
    await seed(c);

    const gte = await c.query(int64Api.events.query().withIndex("by_ts").gte("20").take(10));
    expect((gte as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["b", "a"]); // 20, 100

    const gt = await c.query(int64Api.events.query().withIndex("by_ts").gt("20").take(10));
    expect((gt as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["a"]); // 100

    const lte = await c.query(int64Api.events.query().withIndex("by_ts").lte("20").take(10));
    expect((lte as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["c", "b"]); // 3, 20

    const lt = await c.query(int64Api.events.query().withIndex("by_ts").lt("20").take(10));
    expect((lt as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["c"]); // 3
  });

  it("distinct returns numeric-ordered unique int64 values", async () => {
    const c = newClient();
    await seed(c);
    // Add a duplicate ts to confirm distinct collapses by string equality.
    await c.mutate(mutation().insert("events", { ts: "20", kind: "d" }).build());

    const values = await c.query(int64Api.events.query().withIndex("by_ts").distinct());
    expect(values).toEqual(["3", "20", "100"]);
  });

  it("eq matches an int64 by decimal-string equality", async () => {
    const c = newClient();
    await seed(c);

    const rows = await c.query(int64Api.events.query().withIndex("by_ts", ["20"]).take(10));
    expect((rows as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["b"]);
  });

  it("rejects a non-int64 eq value with BAD_REQUEST", async () => {
    const c = newClient();
    await seed(c);
    await expect(
      c.query(int64Api.events.query().withIndex("by_ts", ["not-an-int"]).take(10)),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });

  it("rejects a non-canonical int64 eq value (leading + / out of range) to match the server", async () => {
    const c = newClient();
    await seed(c);
    // The server's `i64::from_str` rejects a leading `+`; the harness must too
    // (the bind side now uses the same `isInt64String` validator as insert).
    await expect(
      c.query(int64Api.events.query().withIndex("by_ts", ["+20"]).take(10)),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
    // 2^63 is one past the signed i64 max — also rejected.
    await expect(
      c.query(int64Api.events.query().withIndex("by_ts", ["9223372036854775808"]).take(10)),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });

  it("orders values beyond the 2^53 number-precision range", async () => {
    const c = newClient();
    // 2^63 - 1 is the int64 max; well outside Number.MAX_SAFE_INTEGER.
    await c.mutate(mutation().insert("events", { ts: "9223372036854775807", kind: "max" }).build());
    await c.mutate(
      mutation().insert("events", { ts: "9223372036854775806", kind: "prev" }).build(),
    );
    await c.mutate(mutation().insert("events", { ts: "0", kind: "zero" }).build());

    const asc = await c.query(int64Api.events.query().withIndex("by_ts").order("asc").take(10));
    expect((asc as Array<{ kind: string }>).map((d) => d.kind)).toEqual(["zero", "prev", "max"]);
  });

  it("aggregate sum/avg/min/max over an int64 index field are numeric", async () => {
    const c = newClient();
    // ts values: 3, 20, 100 — chosen so lexicographic order (100, 20, 3) differs
    // from numeric order, and string concat ("3"+"20"+"100" = "320100") differs
    // from numeric sum (123).
    await seed(c);

    const sum = await c.query(int64Api.events.query().withIndex("by_ts").aggregate("sum"));
    expect(sum).toBe(123);

    const avg = await c.query(int64Api.events.query().withIndex("by_ts").aggregate("avg"));
    expect(avg).toBe(41);

    const min = await c.query(int64Api.events.query().withIndex("by_ts").aggregate("min"));
    expect(min).toBe("3");

    const max = await c.query(int64Api.events.query().withIndex("by_ts").aggregate("max"));
    expect(max).toBe("100");
  });

  it("paginates an int64 index in numeric order with no gaps or duplicates", async () => {
    const c = newClient();
    // Insert in an order where lexicographic vs numeric pagination would differ:
    // ts strings "100", "3", "20", "100" again (dup ts, distinct id), "2".
    await c.mutate(mutation().insert("events", { ts: "100", kind: "a" }).build());
    await c.mutate(mutation().insert("events", { ts: "3", kind: "b" }).build());
    await c.mutate(mutation().insert("events", { ts: "20", kind: "c" }).build());
    await c.mutate(mutation().insert("events", { ts: "100", kind: "d" }).build());
    await c.mutate(mutation().insert("events", { ts: "2", kind: "e" }).build());

    const docs: Record<string, unknown>[] = [];
    const pageSizes: number[] = [];
    let cursor: string | undefined;
    for (let guard = 0; guard < 100; guard++) {
      const page = (await c.query(
        int64Api.events.query().withIndex("by_ts").paginate(cursor, 2),
      )) as PaginatedResultJson;
      pageSizes.push(page.docs.length);
      docs.push(...(page.docs as Record<string, unknown>[]));
      if (page.nextCursor === undefined) break;
      cursor = page.nextCursor;
    }

    // Page sizes 2, 2, 1 across 5 rows; numeric ts order is 2, 3, 20, 100, 100.
    expect(pageSizes).toEqual([2, 2, 1]);
    expect(docs.map((d) => d.kind)).toEqual(["e", "b", "c", "a", "d"]);
    // No id duplicates across pages.
    expect(new Set(docs.map((d) => d._id)).size).toBe(docs.length);
  });
});

describe("InMemoryRtDbClient — additive schema push", () => {
  // Variants of the shared `schema` fixture at the top of the file. The server
  // (`ddl.rs::detect_destructive_changes`) only allows additive schema pushes:
  // new tables, new fields, and new indexes; removing or retyping any existing
  // table/field/index is rejected with BAD_REQUEST. The in-memory harness now
  // mirrors that, so a second `pushSchema` preserves existing docs instead of
  // wiping them.

  /** items + a new optional field + a brand-new `tags` table (purely additive). */
  const additiveSchema = defineSchema({
    items: defineTable({
      name: t.string(),
      status: t.string(),
      order: t.number(),
      note: t.optional(t.string()),
      priority: t.optional(t.string()),
    })
      .index("by_name", ["name"])
      .index("by_status", ["status"])
      .index("by_status_and_order", ["status", "order"]),
    tags: defineTable({ label: t.string() }).index("by_label", ["label"]),
  });

  /** items with the `note` field dropped — destructive. */
  const itemsWithoutNote = defineSchema({
    items: defineTable({
      name: t.string(),
      status: t.string(),
      order: t.number(),
    })
      .index("by_name", ["name"])
      .index("by_status", ["status"])
      .index("by_status_and_order", ["status", "order"]),
  });

  /** An unrelated single-table schema — pushing it removes the `items` table. */
  const soloSchema = defineSchema({
    solo: defineTable({ x: t.number() }),
  });

  /** items with `order` retyped number -> string — destructive. */
  const itemsWithChangedFieldType = defineSchema({
    items: defineTable({
      name: t.string(),
      status: t.string(),
      order: t.string(),
      note: t.optional(t.string()),
    })
      .index("by_name", ["name"])
      .index("by_status", ["status"])
      .index("by_status_and_order", ["status", "order"]),
  });

  it("an additive second push preserves existing docs and they remain queryable", async () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(schema);
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    c.pushSchema(additiveSchema); // additive: items.priority + tags

    const docs = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(docs).toHaveLength(1);
    expect((docs[0] as { name: string }).name).toBe("a");
  });

  it("a destructive second push missing a table throws BAD_REQUEST", async () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(schema);
    expect(() => c.pushSchema(soloSchema)).toThrow(/removed table 'items'/);
    // Schema is unchanged on a rejected push, so `items` is still usable — the
    // query resolves (to []) rather than rejecting.
    await expect(
      c.query(api.items.query().withIndex("by_status", ["todo"]).collect()),
    ).resolves.toEqual([]);
  });

  it("removing a field is destructive", () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(schema);
    expect(() => c.pushSchema(itemsWithoutNote)).toThrow(/removed field 'items\./);
  });

  it("changing a field's type is destructive", () => {
    const c = new InMemoryRtDbClient();
    c.pushSchema(schema);
    expect(() => c.pushSchema(itemsWithChangedFieldType)).toThrow(/changed type of field 'items\./);
  });
});

describe("InMemoryRtDbClient — literal-union widening (pushSchema parity)", () => {
  // Mirrors server `schema::is_widening_of` (server/src/schema.rs). A second
  // `pushSchema` that widens a finite literal-union field (or a single literal
  // into a union) is an additive, non-destructive change and must pass; every
  // other field-type change (narrowing, swap, collapse, scalar swap) is still
  // rejected with BAD_REQUEST "changed type of field".

  it("widening a literal union by adding a variant succeeds", () => {
    const base = defineSchema({
      things: defineTable({ state: t.union(t.literal("a"), t.literal("b")) }),
    });
    const widened = defineSchema({
      things: defineTable({
        state: t.union(t.literal("a"), t.literal("b"), t.literal("c")),
      }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(widened)).not.toThrow();
  });

  it("widening a single literal to a union succeeds", () => {
    const base = defineSchema({
      things: defineTable({ state: t.literal("a") }),
    });
    const widened = defineSchema({
      things: defineTable({
        state: t.union(t.literal("a"), t.literal("b")),
      }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(widened)).not.toThrow();
  });

  it("narrowing a union is rejected with BAD_REQUEST", () => {
    const base = defineSchema({
      things: defineTable({
        state: t.union(t.literal("a"), t.literal("b"), t.literal("c")),
      }),
    });
    const narrowed = defineSchema({
      things: defineTable({ state: t.union(t.literal("a"), t.literal("b")) }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(narrowed)).toThrow(/changed type of field 'things\./);
  });

  it("replacing one literal with another is rejected", () => {
    const base = defineSchema({
      things: defineTable({ state: t.literal("a") }),
    });
    const swapped = defineSchema({
      things: defineTable({ state: t.literal("b") }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(swapped)).toThrow(/changed type of field 'things\./);
  });

  it("collapsing a union to a literal is rejected", () => {
    const base = defineSchema({
      things: defineTable({
        state: t.union(t.literal("a"), t.literal("b")),
      }),
    });
    const collapsed = defineSchema({
      things: defineTable({ state: t.literal("a") }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(collapsed)).toThrow(/changed type of field 'things\./);
  });

  it("a non-literal type change (string -> number) is still rejected", () => {
    const base = defineSchema({
      things: defineTable({ state: t.string() }),
    });
    const swapped = defineSchema({
      things: defineTable({ state: t.number() }),
    });
    const c = new InMemoryRtDbClient();
    c.pushSchema(base);
    expect(() => c.pushSchema(swapped)).toThrow(/changed type of field 'things\./);
  });
});

describe("InMemoryRtDbClient — migrate", () => {
  it("renameField rewrites the doc key and updates the installed schema", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    const res = c.migrate(new Migration().renameField("items", "name", "title").build());
    expect(res.applied).toBe(true);
    expect(res.directives).toEqual([{ op: "renameField", affectedRows: 1 }]);
    expect(res.schema.tables.items.fields.title).toEqual({ type: "string" });
    expect(res.schema.tables.items.fields).not.toHaveProperty("name");

    const docs = await c.query(api.items.query().collect());
    const first = docs[0] as unknown as Record<string, unknown>;
    expect(first.title).toBe("a");
    expect(first).not.toHaveProperty("name");
  });

  it("renameField only counts rows that carry the renamed key", () => {
    const c = newClient();
    // `note` is optional; only the second row carries it.
    c.mutate(
      mutation()
        .insert("items", { name: "a", status: "todo", order: 1 })
        .insert("items", { name: "b", status: "done", order: 2, note: "x" })
        .build(),
    );
    const res = c.migrate(new Migration().renameField("items", "note", "memo").build());
    expect(res.directives[0].affectedRows).toBe(1);
  });

  it("renameTable relabels the table in the schema and the doc map", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    const res = c.migrate(new Migration().renameTable("items", "tasks").build());
    expect(res.directives).toEqual([{ op: "renameTable", affectedRows: 0 }]);
    expect(res.schema.tables).toHaveProperty("tasks");
    expect(res.schema.tables).not.toHaveProperty("items");

    // The doc map follows the rename: the row is reachable under the new table.
    const docs = await c.query<unknown[]>({ json: { table: "tasks" } } as unknown as RtQuery<
      unknown[]
    >);
    expect(docs).toHaveLength(1);
  });

  it("setDefault fills the field on rows that lack it", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    const res = c.migrate(new Migration().setDefault("items", "note", "none").build());
    expect(res.directives).toEqual([{ op: "setDefault", affectedRows: 1 }]);

    const docs = await c.query(api.items.query().collect());
    expect(docs[0].note).toBe("none");
  });

  it("setDefault leaves rows that already have the field untouched", () => {
    const c = newClient();
    c.mutate(
      mutation()
        .insert("items", { name: "a", status: "todo", order: 1 })
        .insert("items", { name: "b", status: "done", order: 2, note: "keep" })
        .build(),
    );
    const res = c.migrate(new Migration().setDefault("items", "note", "none").build());
    expect(res.directives[0].affectedRows).toBe(1);
  });

  it("changeType coerces a number to a string via toString", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 42 }).build());

    const res = c.migrate(
      new Migration().changeType("items", "order", { type: "string" }, "toString").build(),
    );
    expect(res.directives).toEqual([{ op: "changeType", affectedRows: 1 }]);
    expect(res.schema.tables.items.fields.order).toEqual({ type: "string" });

    const docs = await c.query(api.items.query().collect());
    expect(docs[0].order).toBe("42");
  });

  it("changeType coerces a numeric string to a number via toNumber", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "21", status: "todo", order: 1 }).build());

    c.migrate(new Migration().changeType("items", "name", { type: "number" }, "toNumber").build());
    const docs = await c.query(api.items.query().collect());
    expect(docs[0].name).toBe(21);
  });

  it("changeType falls back to a coerced default when a value cannot be coerced", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    c.migrate(
      new Migration().changeType("items", "name", { type: "number" }, "toNumber", 0).build(),
    );
    const docs = await c.query(api.items.query().collect());
    expect(docs[0].name).toBe(0);
  });

  it("changeType throws BAD_REQUEST when a value cannot be coerced and no default is given", () => {
    const c = newClient();
    c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    expect(() =>
      c.migrate(
        new Migration().changeType("items", "name", { type: "number" }, "toNumber").build(),
      ),
    ).toThrow(RtDbError);
  });

  it("changeType rejects a cast the source type does not admit", () => {
    const c = newClient();
    c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    // `order` is a number; ToNumber admits only String|Boolean|Int64 (not Number),
    // so this cast is outside the matrix and must be rejected.
    expect(() =>
      c.migrate(
        new Migration().changeType("items", "order", { type: "number" }, "toNumber").build(),
      ),
    ).toThrow(RtDbError);
  });

  it("dropField removes the key from every doc and the field from the schema", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    const res = c.migrate(new Migration().dropField("items", "status").build());
    expect(res.directives).toEqual([{ op: "dropField", affectedRows: 1 }]);
    expect(res.schema.tables.items.fields).not.toHaveProperty("status");

    const docs = await c.query(api.items.query().collect());
    expect(docs[0]).not.toHaveProperty("status");
  });

  it("dropField affectedRows counts only rows carrying the field", () => {
    const c = newClient();
    c.mutate(
      mutation().insert("items", { name: "a", status: "todo", order: 1, note: "x" }).build(),
    );
    c.mutate(mutation().insert("items", { name: "b", status: "todo", order: 2 }).build()); // no `note`
    const res = c.migrate(new Migration().dropField("items", "note").build());
    // `note` is optional; only the first row carries it, so affectedRows is the
    // carrier count, not the total row count (server parity).
    expect(res.directives[0].affectedRows).toBe(1);
  });

  it("dropTable removes the table from the schema and the doc map", () => {
    const c = newClient();
    c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    const res = c.migrate(new Migration().dropTable("items").build());
    expect(res.directives).toEqual([{ op: "dropTable", affectedRows: 1 }]);
    expect(res.schema.tables).not.toHaveProperty("items");
  });

  it("dropIndex removes the index from the schema", () => {
    const c = newClient();
    const res = c.migrate(new Migration().dropIndex("items", "by_name").build());
    expect(res.directives).toEqual([{ op: "dropIndex", affectedRows: 0 }]);
    const remaining = res.schema.tables.items.indexes?.map((i) => i.name) ?? [];
    expect(remaining).not.toContain("by_name");
  });

  it("evalExpr throws BAD_REQUEST (no SQL engine in-memory)", () => {
    const c = newClient();
    let caught: unknown;
    try {
      c.migrate(new Migration().evalExpr("items", "x", "1 = 1").build());
    } catch (e) {
      caught = e;
    }
    // Assert the code, not just the message — the harness rejects evalExpr with
    // BAD_REQUEST (not INTERNAL), matching how the live server rejects it.
    expect(caught).toMatchObject({ code: "BAD_REQUEST", message: /evalExpr/i });
  });

  it("dryRun validates and reports affectedRows without committing", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    const res = c.migrate(new Migration().renameField("items", "name", "title").dryRun().build());
    expect(res.applied).toBe(false);
    expect(res.directives).toEqual([{ op: "renameField", affectedRows: 1 }]);
    expect(res.schema.tables.items.fields).toHaveProperty("title");

    // Nothing committed — the doc still carries the old key.
    const docs = await c.query(api.items.query().collect());
    expect(docs[0]).toHaveProperty("name", "a");
    expect(docs[0]).not.toHaveProperty("title");
  });

  it("rolls back every prior effect when a later directive fails (atomic)", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());

    expect(() =>
      c.migrate(
        new Migration()
          .renameField("items", "name", "title") // would apply
          .renameField("items", "nope", "x") // fails — should roll back the first
          .build(),
      ),
    ).toThrow(RtDbError);

    const docs = await c.query(api.items.query().collect());
    expect(docs[0]).toHaveProperty("name", "a");
    expect(docs[0]).not.toHaveProperty("title");
  });
});
