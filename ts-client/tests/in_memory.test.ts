import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import {
  evalFilterExpr,
  InMemoryRtDbClient,
  MAX_AFFECTED_ROWS_PER_TXN,
  MAX_BY_QUERY_STEPS_PER_TXN,
  MAX_STEPS,
  PresenceRooms,
  validateFilter,
} from "../src/in_memory.js";
import { Migration } from "../src/migration.js";
import { mutation } from "../src/mutation.js";
import { decodeCursor, encodeCursor } from "../src/pagination.js";
import type {
  FilterExpr,
  PaginatedResultJson,
  PresenceMember,
  ScheduleInfo,
} from "../src/protocol.js";
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

  it("aggregate count counts matching rows (scalar, needs no index field)", async () => {
    const c = newClient();
    await seed(c); // 3× todo
    // Over an eq prefix: count of todo rows.
    const n = await c.query(
      api.items.query().withIndex("by_status_and_order", ["todo"]).aggregate("count"),
    );
    expect(n).toBe(3);
    // Scalar count needs no aggregate index field at all — a full-table count
    // (no index) is allowed for `count`, unlike sum/avg/min/max.
    const all = await c.query(api.items.query().aggregate("count"));
    expect(all).toBe(3);
    // Zero matching rows → 0 (not null, unlike the field-bearing ops).
    const none = await c.query(
      api.items.query().withIndex("by_status_and_order", ["done"]).aggregate("count"),
    );
    expect(none).toBe(0);
  });

  it("aggregate count groupBy returns one {key, count} per group", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "todo", order: 1 }).build());
    await c.mutate(mutation().insert("items", { name: "b", status: "todo", order: 2 }).build());
    await c.mutate(mutation().insert("items", { name: "c", status: "done", order: 5 }).build());
    // by_status_and_order has [status, order]; eq=[] leaves `status` as the group
    // key. Count consumes no second index field (unlike sum groupBy).
    const rows = (await c.query(
      api.items.query().withIndex("by_status_and_order", []).aggregate("count", true),
    )) as { key: unknown; value: unknown }[];
    expect(rows).toEqual([
      { key: "done", value: 1 },
      { key: "todo", value: 2 },
    ]);
  });

  it("aggregate count groupBy requires an index field beyond the eq prefix", async () => {
    const c = newClient();
    await seed(c);
    // by_status has only one field; groupBy needs one to group by (count needs
    // no second field, but still needs the group field).
    await expect(
      c.query(
        // eq consumes the only field, leaving none to group by.
        api.items.query().withIndex("by_status", ["todo"]).aggregate("count", true),
      ),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: /requires an index field beyond the eq prefix/,
    });
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

  it("rejects a txn exceeding MAX_STEPS with BAD_REQUEST (ARC-104)", async () => {
    // MAX_STEPS mirrors server/src/txn.rs (1024). A txn with MAX_STEPS+1 steps
    // is rejected at the top of executeTransaction, before any step runs.
    const c = newClient();
    let b = mutation();
    for (let i = 0; i <= MAX_STEPS; i++) {
      b = b.insert("items", { name: `n${i}`, status: "todo", order: i });
    }
    await expect(c.mutate(b.build())).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: expect.stringContaining("maximum"),
    });
  });

  it("rejects a nested tree whose recursive step count exceeds MAX_STEPS (FM-28)", async () => {
    // Flat length is 1 (just the schedule step); the recursive count is
    // 1 + 1025 = 1026 > MAX_STEPS — a nested tree can't smuggle past the
    // flat cap. Rejected pre-execution, so no doc lands and no job enqueues.
    const c = newClient();
    let nested = mutation();
    for (let i = 0; i <= MAX_STEPS; i++) {
      nested = nested.insert("items", { name: `n${i}`, status: "todo", order: i });
    }
    const txn = mutation().schedule({ type: "afterMs", ms: 1000 }, nested.build()).build();
    await expect(c.mutate(txn)).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: expect.stringContaining("maximum"),
    });
    expect(await c.query(api.items.query().collect())).toEqual([]);
    expect(await c.listSchedules()).toEqual([]);
  });

  it("accepts a 300-step txn (ARC-104: cap raised 256 -> 1024)", async () => {
    // The in-memory engine previously capped at 256, rejecting a legal 300-step
    // txn. With MAX_STEPS=1024 it must execute.
    const c = newClient();
    let b = mutation();
    for (let i = 0; i < 300; i++) {
      b = b.insert("items", { name: `n${i}`, status: "todo", order: i });
    }
    const results = await c.mutate(b.build());
    expect(results).toHaveLength(300);
  });
});

describe("InMemoryRtDbClient — by-query (patchByQuery / deleteByQuery)", () => {
  const todoFilter: FilterExpr = { op: "eq", field: "status", value: "todo" };

  it("patchByQuery patches every matching row and reports {patched, truncated}", async () => {
    const c = newClient();
    for (const order of [1, 2, 3]) {
      await c.mutate(
        mutation()
          .insert("items", { name: `n${order}`, status: "todo", order })
          .build(),
      );
    }
    await c.mutate(mutation().insert("items", { name: "done1", status: "done", order: 9 }).build());

    const [res] = await c.mutate(
      mutation().patchByQuery("items", todoFilter, { order: 0 }).build(),
    );
    expect(res).toEqual({ patched: 3, truncated: false });

    // Only the three todo rows were patched; the done row is unchanged.
    const docs = (await c.query(api.items.query().withIndex("by_status", ["todo"]).collect())) as {
      order: number;
    }[];
    expect(docs).toHaveLength(3);
    expect(docs.every((d) => d.order === 0)).toBe(true);
    const done = (await c.query(api.items.query().withIndex("by_status", ["done"]).collect())) as {
      order: number;
    }[];
    expect(done[0].order).toBe(9);
  });

  it("patchByQuery truncates when the match set exceeds limit", async () => {
    const c = newClient();
    for (const order of [1, 2, 3, 4, 5]) {
      await c.mutate(
        mutation()
          .insert("items", { name: `n${order}`, status: "todo", order })
          .build(),
      );
    }
    const [res] = await c.mutate(
      mutation().patchByQuery("items", todoFilter, { order: 0 }, 2).build(),
    );
    expect(res).toEqual({ patched: 2, truncated: true });
  });

  it("deleteByQuery deletes every matching row and reports {deleted, truncated}", async () => {
    const c = newClient();
    for (const order of [1, 2, 3]) {
      await c.mutate(
        mutation()
          .insert("items", { name: `n${order}`, status: "todo", order })
          .build(),
      );
    }
    await c.mutate(mutation().insert("items", { name: "done1", status: "done", order: 9 }).build());

    const [res] = await c.mutate(mutation().deleteByQuery("items", todoFilter).build());
    expect(res).toEqual({ deleted: 3, truncated: false });

    const todo = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(todo).toEqual([]);
    const done = await c.query(api.items.query().withIndex("by_status", ["done"]).collect());
    expect(done).toHaveLength(1);
  });

  it("deleteByQuery truncates when the match set exceeds limit", async () => {
    const c = newClient();
    for (const order of [1, 2, 3, 4, 5]) {
      await c.mutate(
        mutation()
          .insert("items", { name: `n${order}`, status: "todo", order })
          .build(),
      );
    }
    const [res] = await c.mutate(mutation().deleteByQuery("items", todoFilter, 2).build());
    expect(res).toEqual({ deleted: 2, truncated: true });
    // The remaining 3 todo rows are still present.
    const remaining = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(remaining).toHaveLength(3);
  });

  it("patchByQuery over an empty match set is a no-op (patched:0)", async () => {
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "a", status: "done", order: 1 }).build());
    const [res] = await c.mutate(
      mutation().patchByQuery("items", todoFilter, { order: 0 }).build(),
    );
    expect(res).toEqual({ patched: 0, truncated: false });
  });

  it("SEC-104: rejects a txn with too many by-query steps before any step runs", async () => {
    // Mirrors server `sec104_rejects_over_budget_by_query_step_count`. A txn
    // with MAX_BY_QUERY_STEPS_PER_TXN+1 patchByQuery steps is rejected at the
    // top of executeTransaction, before any step applies. The original AUDIT
    // finding was 1024 by-query steps (~1M-row stall); the 16-step cap rejects
    // it pre-execution.
    expect(MAX_BY_QUERY_STEPS_PER_TXN).toBeLessThan(MAX_STEPS);
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "seed", status: "todo", order: 0 }).build());
    let b = mutation();
    for (let i = 0; i <= MAX_BY_QUERY_STEPS_PER_TXN; i++) {
      b = b.patchByQuery("items", todoFilter, { order: i });
    }
    await expect(c.mutate(b.build())).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: expect.stringContaining("by-query steps"),
    });
    // Pre-execution rejection commits nothing.
    const docs = (await c.query(api.items.query().withIndex("by_status", ["todo"]).collect())) as {
      order: number;
    }[];
    expect(docs).toHaveLength(1);
    expect(docs[0].order).toBe(0);
  });

  it("SEC-104: rejects a txn over the aggregate affected-row budget", async () => {
    // Mirrors server `sec104_rejects_over_budget_aggregate_affected`. A txn
    // with few by-query steps (under the step cap) but each at the default
    // 1000-row limit can still exceed MAX_AFFECTED_ROWS_PER_TXN; reject it.
    const overSteps = Math.ceil(MAX_AFFECTED_ROWS_PER_TXN / 1000) + 1;
    expect(overSteps).toBeLessThanOrEqual(MAX_BY_QUERY_STEPS_PER_TXN);
    const c = newClient();
    await c.mutate(mutation().insert("items", { name: "seed", status: "todo", order: 0 }).build());
    let b = mutation();
    for (let i = 0; i < overSteps; i++) {
      b = b.deleteByQuery("items", todoFilter);
    }
    await expect(c.mutate(b.build())).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: expect.stringContaining("affect up to"),
    });
    const docs = await c.query(api.items.query().withIndex("by_status", ["todo"]).collect());
    expect(docs).toHaveLength(1);
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

describe("InMemoryRtDbClient — presence", () => {
  it("two clients sharing PresenceRooms see each other's cursor updates and leaves", () => {
    const rooms = new PresenceRooms();
    const alice = new InMemoryRtDbClient({
      connectionId: "alice",
      presenceUser: { kind: "user", email: "alice@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });
    const bob = new InMemoryRtDbClient({
      connectionId: "bob",
      presenceUser: { kind: "user", email: "bob@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });

    const aliceUpdates: PresenceMember[][] = [];
    alice.presence("doc:1", { cursor: { x: 0, y: 0 } }, (m) => aliceUpdates.push(m));

    // Alice's first snapshot lists just herself.
    expect(aliceUpdates).toHaveLength(1);
    expect(aliceUpdates[0]).toEqual([
      {
        connectionId: "alice",
        user: { kind: "user", email: "alice@example.com" },
        state: { cursor: { x: 0, y: 0 } },
      },
    ]);

    // Bob joins; Alice's listener fires again with both members.
    bob.presence("doc:1", { cursor: { x: 5, y: 5 } });
    expect(aliceUpdates.at(-1)).toEqual([
      {
        connectionId: "alice",
        user: { kind: "user", email: "alice@example.com" },
        state: { cursor: { x: 0, y: 0 } },
      },
      {
        connectionId: "bob",
        user: { kind: "user", email: "bob@example.com" },
        state: { cursor: { x: 5, y: 5 } },
      },
    ]);

    // Alice moves her cursor; she sees her own updated state in the next snapshot.
    alice.updatePresence("doc:1", { cursor: { x: 10, y: 10 } });
    const aliceSelf = aliceUpdates.at(-1)?.find((m) => m.connectionId === "alice");
    expect(aliceSelf?.state).toEqual({ cursor: { x: 10, y: 10 } });

    // updatePresence on a room the client has not joined is a no-op (mirrors
    // the live server, which would not relay for a non-member).
    const beforeLen = aliceUpdates.length;
    const carol = new InMemoryRtDbClient({ connectionId: "carol", presenceRooms: rooms });
    carol.updatePresence("doc:1", { cursor: { x: 99 } });
    expect(aliceUpdates).toHaveLength(beforeLen);

    // Bob leaves; Alice's snapshot shrinks back to just herself.
    bob.leavePresence("doc:1");
    expect(aliceUpdates.at(-1)?.map((m) => m.connectionId)).toEqual(["alice"]);

    // Unsub stops further fan-out to that listener.
    const unsubLen = aliceUpdates.length;
    alice.leavePresence("doc:1");
    bob.presence("doc:1", { cursor: { x: 50 } });
    expect(aliceUpdates).toHaveLength(unsubLen); // alice has left: no further updates
  });

  it("isolated clients see only themselves in their rooms", () => {
    // No shared `presenceRooms`: each client gets a private PresenceRooms and
    // therefore cannot observe peers. Mirrors the contract that the in-memory
    // harness does not multiplex between independent clients by default.
    const a = new InMemoryRtDbClient({ connectionId: "a" });
    const b = new InMemoryRtDbClient({ connectionId: "b" });
    const seen: PresenceMember[][] = [];
    a.presence("doc:1", undefined, (m) => seen.push(m));
    b.presence("doc:1", undefined);
    expect(seen.at(-1)?.map((m) => m.connectionId)).toEqual(["a"]);
  });

  it("ttlMs expires a member's state to null while keeping them listed", () => {
    // Two clients share PresenceRooms. Alice refreshes with ttlMs=50; the
    // injected clock pins `now` at 0 so the expiry lands at 50. Calling
    // `rooms.expire(60)` clears Alice's state to null but she STAYS in the
    // member list — mirrors the live server's "state to null after ttl"
    // semantics (the member stays; only its state clears).
    const rooms = new PresenceRooms();
    const alice = new InMemoryRtDbClient({
      connectionId: "alice",
      presenceUser: { kind: "user", email: "alice@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });
    const bob = new InMemoryRtDbClient({
      connectionId: "bob",
      presenceUser: { kind: "user", email: "bob@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });

    const bobSees: PresenceMember[][] = [];
    bob.presence("doc:1", undefined, (m) => bobSees.push(m));
    alice.presence("doc:1", undefined);
    // The ttl is a refresh concept — set via updatePresence, not the join.
    alice.updatePresence("doc:1", { typing: true }, 50);

    // Pre-expiry: Alice is listed with her typing state.
    const before = bobSees.at(-1)?.find((m) => m.connectionId === "alice");
    expect(before?.state).toEqual({ typing: true });

    // Before the expiry elapses, expire() is a no-op: state survives.
    expect(rooms.expire(40)).toBe(false);
    const mid = bobSees.at(-1)?.find((m) => m.connectionId === "alice");
    expect(mid?.state).toEqual({ typing: true });

    // At/after the expiry, the member's state nulls but the member stays.
    expect(rooms.expire(60)).toBe(true);
    const after = bobSees.at(-1)?.find((m) => m.connectionId === "alice");
    expect(after?.state).toBeNull();
    expect(bobSees.at(-1)?.map((m) => m.connectionId)).toContain("alice");

    // Idempotent: a second sweep with nothing to expire returns false and
    // does not fire an extra fan-out.
    const lenBefore = bobSees.length;
    expect(rooms.expire(10_000)).toBe(false);
    expect(bobSees).toHaveLength(lenBefore);
  });

  it("a presence refresh without ttlMs clears a pending expiry", () => {
    // Mirrors the server's "ttlMs after the LAST refresh" semantics: if a
    // client refreshes state without ttlMs, any prior ttl is withdrawn — the
    // member's state must not later spontaneously null out.
    const rooms = new PresenceRooms();
    const alice = new InMemoryRtDbClient({
      connectionId: "alice",
      presenceUser: { kind: "user", email: "alice@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });
    const seen: PresenceMember[][] = [];
    alice.presence("doc:1", undefined, (m) => seen.push(m));

    alice.updatePresence("doc:1", { typing: true }, 50); // schedules expiry at 50
    alice.updatePresence("doc:1", { typing: false }); // no ttlMs → clears expiry

    // Even far in the future, expire() must not null out her state.
    expect(rooms.expire(10_000)).toBe(false);
    const self = seen.at(-1)?.find((m) => m.connectionId === "alice");
    expect(self?.state).toEqual({ typing: false });
  });

  it("leave clears a pending expiry so a re-join does not inherit it", () => {
    // Regression guard: `leave` must delete the expiry entry alongside the
    // member, otherwise a re-join could resurrect a stale ttl that nulls the
    // fresh state out from under the new membership.
    const rooms = new PresenceRooms();
    const alice = new InMemoryRtDbClient({
      connectionId: "alice",
      presenceUser: { kind: "user", email: "alice@example.com" },
      presenceRooms: rooms,
      now: () => 0,
      random: () => 0,
    });
    alice.presence("doc:1", { typing: true });
    alice.updatePresence("doc:1", { typing: true }, 50); // schedules expiry at 50
    alice.leavePresence("doc:1");

    // Re-join with a fresh state and no ttl. A stale expiry from the prior
    // membership would null this out; the cleared expiry must keep it alive.
    alice.presence("doc:1", { typing: false });
    expect(rooms.expire(10_000)).toBe(false);
    const self = rooms.snapshot("doc:1").find((m) => m.connectionId === "alice");
    expect(self?.state).toEqual({ typing: false });
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

  it("a failed txn rolls back a schedule step's enqueue (FM-28)", async () => {
    // The schedule step's enqueue joins the atomicity snapshot — a later
    // step's error must not leave a phantom job that tick() would fire
    // (mirrors the server's single sqlx transaction around the insert).
    const { c, setNow } = newClockClient();
    setNow(BASE);
    await expect(
      c.mutate(
        mutation()
          .schedule({ type: "afterMs", ms: 1000 }, insertTxn)
          .delete("items", "nonexistent") // NOT_FOUND -> rollback the enqueue
          .build(),
      ),
    ).rejects.toMatchObject({ name: "RtDbError", code: "NOT_FOUND" });
    expect(await c.listSchedules()).toEqual([]);
    // Past the would-be due time: nothing fires.
    setNow(BASE + 2000);
    c.tick();
    expect(await c.query(api.items.query().collect())).toEqual([]);
  });

  it("a failed txn rolls back a cancelSchedule step (FM-28)", async () => {
    // Same snapshot covers a cancel step's removal: a pre-existing job
    // survives a txn that cancelled it and then failed.
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.schedule(insertTxn, { type: "afterMs", ms: 1000 });
    await expect(
      c.mutate(
        mutation()
          .cancelSchedule(id)
          .delete("items", "nonexistent") // NOT_FOUND -> rollback the cancel
          .build(),
      ),
    ).rejects.toMatchObject({ name: "RtDbError", code: "NOT_FOUND" });
    const jobs = await c.listSchedules();
    expect(jobs).toHaveLength(1);
    expect(jobs[0].id).toBe(id);
    // The surviving job still fires on its original schedule.
    setNow(BASE + 2000);
    c.tick();
    expect(await c.query(api.items.query().collect())).toHaveLength(1);
  });
});

describe("InMemoryRtDbClient — workflows (FM-29)", () => {
  const BASE = 1_700_000_000_000;
  const insertTxn = (name: string) =>
    mutation().insert("items", { name, status: "todo", order: 1 }).build();
  /** A step that fails while a `name: "a"` row exists (expectAbsent on the
   * by_name index) — deleting the row makes the next attempt succeed, so a
   * test can drive deterministic fail-then-succeed retries. */
  const failingWhilePresent = { txn: mutation().expectAbsent("items", "by_name", ["a"]).build() };

  it("startWorkflow returns a pending info row; a no-sleep spec completes in one tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const info = await c.startWorkflow({
      name: "two-step",
      steps: [{ txn: insertTxn("one") }, { txn: insertTxn("two") }],
    });
    expect(info).toMatchObject({
      name: "two-step",
      status: "pending",
      currentStep: 0,
      stepCount: 2,
      attempts: 0,
    });
    expect(info.sleepUntil).toBe(BASE);
    expect("startedAt" in info).toBe(false);
    expect("finishedAt" in info).toBe(false);

    c.tick();
    const docs = await c.query(api.items.query().collect());
    expect(docs.map((d) => (d as { name: string }).name).sort()).toEqual(["one", "two"]);
    const after = (await c.listWorkflows()).find((w) => w.id === info.id);
    expect(after?.status).toBe("success");
    // Server `finalize_success` does NOT advance current_step — it stays at
    // the 0-based index of the last executed step (1 for a 2-step run).
    expect(after?.currentStep).toBe(1);
    expect(after?.finishedAt).toBe(BASE);
  });

  it("sleepBeforeMs gates the next step across ticks", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "gated",
      steps: [{ txn: insertTxn("one") }, { txn: insertTxn("two"), sleepBeforeMs: 5000 }],
    });

    c.tick(); // step 0 advances; step 1 gated until BASE + 5000
    let names = (await c.query(api.items.query().collect())).map(
      (d) => (d as { name: string }).name,
    );
    expect(names).toEqual(["one"]);
    const gated = (await c.listWorkflows()).find((w) => w.id === id);
    expect(gated).toMatchObject({ status: "pending", currentStep: 1 });
    expect(gated?.sleepUntil).toBe(BASE + 5000);

    setNow(BASE + 4999);
    c.tick(); // still before the gate
    names = (await c.query(api.items.query().collect())).map((d) => (d as { name: string }).name);
    expect(names).toEqual(["one"]);

    setNow(BASE + 5000);
    c.tick();
    names = (await c.query(api.items.query().collect())).map((d) => (d as { name: string }).name);
    expect(names.sort()).toEqual(["one", "two"]);
    expect((await c.listWorkflows()).find((w) => w.id === id)?.status).toBe("success");
  });

  it("retry policy re-fires a failing step on later ticks with server backoff, then succeeds", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const blocker = (await c.mutate(insertTxn("a")))[0] as { id: string }; // blocks expectAbsent
    const { id } = await c.startWorkflow({
      name: "flaky",
      steps: [
        {
          ...failingWhilePresent,
          retry: { maxAttempts: 3, initialRetryMs: 1000, maxRetryMs: 60_000 },
        },
      ],
    });

    c.tick(); // attempt 1 fails -> backoff 1000 * 2^0
    let info = (await c.listWorkflows()).find((w) => w.id === id);
    expect(info).toMatchObject({ status: "pending", currentStep: 0, attempts: 1 });
    expect(info?.sleepUntil).toBe(BASE + 1000);

    setNow(BASE + 1000);
    c.tick(); // attempt 2 also fails (row still present) -> backoff 1000 * 2^1
    info = (await c.listWorkflows()).find((w) => w.id === id);
    expect(info).toMatchObject({ status: "pending", attempts: 2 });
    expect(info?.sleepUntil).toBe(BASE + 1000 + 2000);

    // Clear the blocker: attempt 3 (final allowed) now succeeds.
    setNow(BASE + 3000);
    await c.mutate(mutation().delete("items", blocker.id).build());
    c.tick();
    const full = await c.getWorkflow(id);
    expect(full.status).toBe("success");
    expect(full.attempts).toBe(0); // reset on finalize (server parity)
    expect(full.stepOutcomes).toEqual([
      { stepIndex: 0, status: "success", attempts: 3, at: BASE + 3000 },
    ]);
  });

  it("exhausted attempts mark the run failed with the outcome trail and stop advancing", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    await c.mutate(insertTxn("a")); // blocker never clears
    const { id } = await c.startWorkflow({
      name: "doomed",
      steps: [
        { txn: insertTxn("never") },
        {
          ...failingWhilePresent,
          retry: { maxAttempts: 2, initialRetryMs: 1000, maxRetryMs: 60_000 },
        },
      ],
    });

    c.tick(); // step 0 succeeds; step 1 attempt 1 fails
    let info = (await c.listWorkflows()).find((w) => w.id === id);
    expect(info).toMatchObject({ status: "pending", currentStep: 1, attempts: 1 });

    setNow(BASE + 1000);
    c.tick(); // attempt 2 = maxAttempts -> failed
    info = (await c.listWorkflows()).find((w) => w.id === id);
    expect(info?.status).toBe("failed");
    expect(typeof info?.lastError).toBe("string");
    expect(info?.finishedAt).toBe(BASE + 1000);

    const full = await c.getWorkflow(id);
    expect(full.stepOutcomes).toHaveLength(2);
    expect(full.stepOutcomes[0]).toMatchObject({ stepIndex: 0, status: "success", attempts: 1 });
    expect(full.stepOutcomes[1]).toMatchObject({ stepIndex: 1, status: "failed", attempts: 2 });
    expect(typeof full.stepOutcomes[1].error).toBe("string");

    // A failed run is terminal: later ticks change nothing.
    setNow(BASE + 10_000);
    c.tick();
    expect((await c.getWorkflow(id)).status).toBe("failed");
    const names = (await c.query(api.items.query().collect())).map(
      (d) => (d as { name: string }).name,
    );
    expect(names).toEqual(["a", "never"]);
  });

  it("cancelWorkflow flips a pending run to cancelled and stops advancement", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id } = await c.startWorkflow({
      name: "cancelme",
      steps: [{ txn: insertTxn("one") }, { txn: insertTxn("two"), sleepBeforeMs: 5000 }],
    });

    c.tick(); // step 0 done; step 1 gated
    expect(await c.cancelWorkflow(id)).toBe(true);
    expect((await c.listWorkflows()).find((w) => w.id === id)).toMatchObject({
      status: "cancelled",
      finishedAt: BASE,
    });

    setNow(BASE + 6000);
    c.tick(); // cancelled runs never advance
    const names = (await c.query(api.items.query().collect())).map(
      (d) => (d as { name: string }).name,
    );
    expect(names).toEqual(["one"]);
    // Terminal: a second cancel is ok:false (a no-op, not an error).
    expect(await c.cancelWorkflow(id)).toBe(false);
    expect(await c.cancelWorkflow("nope")).toBe(false);
  });

  it("listWorkflows filters by status, newest first", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const { id: first } = await c.startWorkflow({ name: "a", steps: [{ txn: insertTxn("one") }] });
    setNow(BASE + 10);
    const { id: second } = await c.startWorkflow({ name: "b", steps: [{ txn: insertTxn("two") }] });
    setNow(BASE + 20);
    const { id: third } = await c.startWorkflow({
      name: "c",
      steps: [{ txn: insertTxn("three") }],
    });
    await c.cancelWorkflow(third);

    const all = await c.listWorkflows();
    expect(all.map((w) => w.id)).toEqual([third, second, first]); // createdAt DESC
    expect((await c.listWorkflows("cancelled")).map((w) => w.id)).toEqual([third]);
    expect((await c.listWorkflows("pending")).map((w) => w.id)).toEqual([second, first]);
  });

  it("startWorkflow / cancelWorkflow txn steps return wire results and drive tick", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    const spec = { name: "from-step", steps: [{ txn: insertTxn("one") }] };
    const [started] = await c.mutate(mutation().startWorkflow(spec).build());
    expect(typeof started).toBe("object");
    expect("workflowId" in (started as object)).toBe(true);
    expect(await c.listWorkflows()).toHaveLength(1);

    // A cancel step for a live run cancels; for a missing run returns false.
    const results = await c.mutate(
      mutation()
        .cancelWorkflow((started as { workflowId: string }).workflowId)
        .cancelWorkflow("nope")
        .build(),
    );
    expect(results).toEqual([{ cancelled: true }, { cancelled: false }]);
    expect((await c.listWorkflows())[0].status).toBe("cancelled");

    // Nothing fires on a later tick.
    setNow(BASE + 5000);
    c.tick();
    expect(await c.query(api.items.query().collect())).toEqual([]);
  });

  it("a failed txn rolls back a startWorkflow step's run row", async () => {
    const { c, setNow } = newClockClient();
    setNow(BASE);
    await expect(
      c.mutate(
        mutation()
          .startWorkflow({ name: "phantom", steps: [{ txn: insertTxn("one") }] })
          .delete("items", "nonexistent") // NOT_FOUND -> rollback the run row
          .build(),
      ),
    ).rejects.toMatchObject({ name: "RtDbError", code: "NOT_FOUND" });
    expect(await c.listWorkflows()).toEqual([]);
    setNow(BASE + 5000);
    c.tick();
    expect(await c.query(api.items.query().collect())).toEqual([]);
  });

  it("rejects an empty spec like the server's validate_spec", async () => {
    const { c } = newClockClient();
    await expect(c.startWorkflow({ name: "empty", steps: [] })).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
    });
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

describe("InMemoryRtDbClient — unique index enforcement", () => {
  const uniqueSchema = defineSchema({
    users: defineTable({
      email: t.string(),
      verified: t.boolean(),
    })
      .index("by_email", ["email"])
      .unique(),
  });
  const uniqueApi = createApi(uniqueSchema);

  function uniqueClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(uniqueSchema);
    return c;
  }

  it("rejects a colliding insert with CONFLICT and rolls the txn back", async () => {
    const c = uniqueClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    await expect(
      c.mutate(mutation().insert("users", { email: "a@x", verified: false }).build()),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "CONFLICT",
      message: /unique index 'by_email' violated/,
    });
    // Rollback: the second (colliding) insert never became durable.
    const docs = await c.query(uniqueApi.users.query().collect());
    expect(docs).toHaveLength(1);
  });

  it("allows non-colliding inserts under a unique index", async () => {
    const c = uniqueClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    await c.mutate(mutation().insert("users", { email: "b@x", verified: true }).build());
    const docs = await c.query(uniqueApi.users.query().collect());
    expect(docs.map((d) => (d as { email: string }).email).sort()).toEqual(["a@x", "b@x"]);
  });

  it("rejects a collision caused by a patch onto an indexed field", async () => {
    const c = uniqueClient();
    const [a] = await c.mutate(
      mutation().insert("users", { email: "a@x", verified: true }).build(),
    );
    await c.mutate(mutation().insert("users", { email: "b@x", verified: true }).build());
    const aId = (a as { id: string }).id;
    await expect(
      c.mutate(mutation().patch("users", aId, { email: "b@x" }).build()),
    ).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
  });

  it("rejects a collision caused by a replace", async () => {
    const c = uniqueClient();
    const [a] = await c.mutate(
      mutation().insert("users", { email: "a@x", verified: true }).build(),
    );
    await c.mutate(mutation().insert("users", { email: "b@x", verified: true }).build());
    const aId = (a as { id: string }).id;
    await expect(
      c.mutate(mutation().replace("users", aId, { email: "b@x", verified: false }).build()),
    ).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
  });

  it("allows a row to patch itself to the same indexed value (self is excluded)", async () => {
    const c = uniqueClient();
    const [a] = await c.mutate(
      mutation().insert("users", { email: "a@x", verified: true }).build(),
    );
    const aId = (a as { id: string }).id;
    await expect(
      c.mutate(mutation().patch("users", aId, { verified: false }).build()),
    ).resolves.toBeTruthy();
  });

  it("rolls back the whole txn when a later insert collides (atomicity)", async () => {
    const c = uniqueClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    await expect(
      c.mutate(
        mutation()
          .insert("users", { email: "b@x", verified: true })
          .insert("users", { email: "a@x", verified: false }) // collides → aborts
          .build(),
      ),
    ).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
    const docs = await c.query(uniqueApi.users.query().collect());
    expect(docs).toHaveLength(1);
  });
});

describe("InMemoryRtDbClient — partial unique index (where predicate)", () => {
  const partialSchema = defineSchema({
    users: defineTable({
      email: t.string(),
      verified: t.boolean(),
    })
      .index("by_email_verified", ["email"])
      .unique()
      .where({ op: "eq", field: "verified", value: true }),
  });
  const partialApi = createApi(partialSchema);

  function partialClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(partialSchema);
    return c;
  }

  it("allows duplicate emails among predicate-excluded (unverified) rows", async () => {
    const c = partialClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: false }).build());
    await c.mutate(mutation().insert("users", { email: "a@x", verified: false }).build());
    const docs = await c.query(partialApi.users.query().collect());
    expect(docs).toHaveLength(2);
  });

  it("rejects a duplicate among predicate-matching (verified) rows", async () => {
    const c = partialClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    await expect(
      c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build()),
    ).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
  });

  it("allows a verified row to share an email with an unverified row", async () => {
    const c = partialClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: false }).build());
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    const docs = await c.query(partialApi.users.query().collect());
    expect(docs).toHaveLength(2);
  });

  it("rejects when a patch flips an unverified duplicate into the predicate", async () => {
    const c = partialClient();
    await c.mutate(mutation().insert("users", { email: "a@x", verified: true }).build());
    const [b] = await c.mutate(
      mutation().insert("users", { email: "a@x", verified: false }).build(),
    );
    const bId = (b as { id: string }).id;
    await expect(
      c.mutate(mutation().patch("users", bId, { verified: true }).build()),
    ).rejects.toMatchObject({ name: "RtDbError", code: "CONFLICT" });
  });
});

describe("InMemoryRtDbClient — full-text search", () => {
  const searchSchema = defineSchema({
    notes: defineTable({
      title: t.string(),
      body: t.string(),
    }).searchIndex("search_text", ["title", "body"]),
  });
  const searchApi = createApi(searchSchema);

  function searchClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(searchSchema);
    return c;
  }

  async function seed(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation().insert("notes", { title: "Alpha release", body: "ship the new search" }).build(),
    );
    await c.mutate(
      mutation().insert("notes", { title: "Beta plan", body: "refine search ranking" }).build(),
    );
    await c.mutate(mutation().insert("notes", { title: "Unrelated", body: "lunch menu" }).build());
  }

  it("matches docs whose indexed text contains the query token", async () => {
    const c = searchClient();
    await seed(c);
    const docs = (await c.query(
      searchApi.notes.query().search("search_text", "search").take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title).sort()).toEqual(["Alpha release", "Beta plan"]);
  });

  it("returns [] when no doc contains the query token", async () => {
    const c = searchClient();
    await seed(c);
    const docs = await c.query(
      searchApi.notes.query().search("search_text", "nonexistent").take(10),
    );
    expect(docs).toEqual([]);
  });

  it("requires every query token to be present (token-AND, not OR)", async () => {
    const c = searchClient();
    await seed(c);
    // "lunch" + "search" never co-occur ⇒ AND yields no match
    const none = await c.query(
      searchApi.notes.query().search("search_text", "lunch search").take(10),
    );
    expect(none).toEqual([]);
    // "search" + "ranking" co-occur only in the Beta note
    const ranked = (await c.query(
      searchApi.notes.query().search("search_text", "search ranking").take(10),
    )) as Array<{ title: string }>;
    expect(ranked.map((d) => d.title)).toEqual(["Beta plan"]);
  });

  it("respects take(N)", async () => {
    const c = searchClient();
    await seed(c);
    const docs = await c.query(searchApi.notes.query().search("search_text", "search").take(1));
    expect(docs).toHaveLength(1);
  });

  it("rejects an empty query (BAD_REQUEST)", async () => {
    const c = searchClient();
    await seed(c);
    await expect(
      c.query(searchApi.notes.query().search("search_text", "   ").take(10)),
    ).rejects.toThrow(/search query text must not be empty/);
  });

  it("rejects an unknown search index (BAD_REQUEST)", async () => {
    const c = searchClient();
    await seed(c);
    await expect(c.query(searchApi.notes.query().search("nope", "x").take(10))).rejects.toThrow(
      /search index 'nope' not found/,
    );
  });

  it("narrows results by a search-level filter (full FilterExpr)", async () => {
    const c = searchClient();
    await seed(c);
    // "search" matches Alpha + Beta; the eq filter on `title` narrows to Alpha.
    const docs = (await c.query(
      searchApi.notes
        .query()
        .search("search_text", "search", {
          filter: { op: "eq", field: "title", value: "Alpha release" },
        })
        .take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title)).toEqual(["Alpha release"]);
  });

  it("rejects a search-level filter on an unknown field (BAD_REQUEST)", async () => {
    const c = searchClient();
    await seed(c);
    await expect(
      c.query(
        searchApi.notes
          .query()
          .search("search_text", "search", {
            filter: { op: "eq", field: "nope", value: "x" },
          })
          .take(10),
      ),
    ).rejects.toThrow(/filter references unknown field 'nope'/);
  });
});

describe("InMemoryRtDbClient — trgm search (mode: 'trgm')", () => {
  const trgmSchema = defineSchema({
    notes: defineTable({
      title: t.string(),
      body: t.string(),
    }).searchIndex("search_text", ["title", "body"]),
  });
  const trgmApi = createApi(trgmSchema);

  function trgmClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(trgmSchema);
    return c;
  }

  async function seedTrgm(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation()
        .insert("notes", { title: "Convexity of trigrams", body: "similarity study" })
        .build(),
    );
    await c.mutate(mutation().insert("notes", { title: "Release plan", body: "ship it" }).build());
    await c.mutate(mutation().insert("notes", { title: "conv", body: "exact note" }).build());
  }

  it("matches infix substrings that tsquery word-matching cannot", async () => {
    const c = trgmClient();
    await seedTrgm(c);
    // trgm: "conv" appears inside "Convexity of trigrams" (infix) and as an
    // exact title.
    const trgmDocs = (await c.query(
      trgmApi.notes.query().search("search_text", "conv", { mode: "trgm" }).take(10),
    )) as Array<{ title: string }>;
    expect(trgmDocs.map((d) => d.title).sort()).toEqual(["Convexity of trigrams", "conv"]);
    // Default (tsquery): token-AND requires the exact word "conv" — only the
    // exact-title note matches; the infix hit is invisible.
    const ftsDocs = (await c.query(
      trgmApi.notes.query().search("search_text", "conv").take(10),
    )) as Array<{ title: string }>;
    expect(ftsDocs.map((d) => d.title)).toEqual(["conv"]);
  });

  it("matches case-insensitively", async () => {
    const c = trgmClient();
    await seedTrgm(c);
    const docs = (await c.query(
      trgmApi.notes.query().search("search_text", "cOnV", { mode: "trgm" }).take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title).sort()).toEqual(["Convexity of trigrams", "conv"]);
  });

  it("ranks the shorter containing field first (query.length / field.length)", async () => {
    const c = trgmClient();
    await seedTrgm(c);
    // "conv" in the exact 4-char title scores 1.0; inside the 22-char
    // "Convexity of trigrams" it scores 4/22 — score dominates the
    // createdAt tie-break (the infix note was inserted first).
    const docs = (await c.query(
      trgmApi.notes.query().search("search_text", "conv", { mode: "trgm" }).take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title)).toEqual(["conv", "Convexity of trigrams"]);
  });

  it("narrows by a search-level filter and respects take", async () => {
    const c = trgmClient();
    await seedTrgm(c);
    const filtered = (await c.query(
      trgmApi.notes
        .query()
        .search("search_text", "conv", {
          mode: "trgm",
          filter: { op: "eq", field: "title", value: "Convexity of trigrams" },
        })
        .take(10),
    )) as Array<{ title: string }>;
    expect(filtered.map((d) => d.title)).toEqual(["Convexity of trigrams"]);
    const taken = await c.query(
      trgmApi.notes.query().search("search_text", "conv", { mode: "trgm" }).take(1),
    );
    expect(taken).toHaveLength(1);
  });

  it("explicit mode 'tsquery' behaves exactly like the omitted default", async () => {
    const c = trgmClient();
    await seedTrgm(c);
    const infix = (await c.query(
      trgmApi.notes.query().search("search_text", "conv", { mode: "tsquery" }).take(10),
    )) as Array<{ title: string }>;
    expect(infix.map((d) => d.title)).toEqual(["conv"]);
    const word = (await c.query(
      trgmApi.notes.query().search("search_text", "release", { mode: "tsquery" }).take(10),
    )) as Array<{ title: string }>;
    expect(word.map((d) => d.title)).toEqual(["Release plan"]);
  });
});

describe("InMemoryRtDbClient — phrase/operator search (websearch syntax, FM-31)", () => {
  const phraseSchema = defineSchema({
    notes: defineTable({
      title: t.string(),
      body: t.string(),
    }).searchIndex("search_text", ["title", "body"]),
  });
  const phraseApi = createApi(phraseSchema);

  function phraseClient(): InMemoryRtDbClient {
    let ms = 1_800_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(phraseSchema);
    return c;
  }

  async function seedPhrases(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation().insert("notes", { title: "Database notes", body: "keep them close" }).build(),
    );
    await c.mutate(
      mutation()
        .insert("notes", { title: "Database", body: "scattered notes about lunch" })
        .build(),
    );
  }

  async function seedOr(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(mutation().insert("notes", { title: "Alpha doc", body: "first" }).build());
    await c.mutate(mutation().insert("notes", { title: "Beta doc", body: "second" }).build());
    await c.mutate(mutation().insert("notes", { title: "Gamma doc", body: "neither" }).build());
  }

  async function seedMinus(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation().insert("notes", { title: "Database intro", body: "cooking recipes" }).build(),
    );
    await c.mutate(
      mutation().insert("notes", { title: "Database advanced", body: "query planning" }).build(),
    );
  }

  it("a quoted phrase requires adjacent words", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", '"database notes"').take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title)).toEqual(["Database notes"]);
  });

  it("the same words unquoted match both docs (plain AND is unchanged)", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", "database notes").take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title).sort()).toEqual(["Database", "Database notes"]);
  });

  it("a bare 'or' unions alternatives", async () => {
    const c = phraseClient();
    await seedOr(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", "alpha or beta").take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title).sort()).toEqual(["Alpha doc", "Beta doc"]);
    // 'or' outside quotes only — a quoted "or" is a literal term.
    const literal = (await c.query(
      phraseApi.notes.query().search("search_text", '"alpha or beta"').take(10),
    )) as Array<{ title: string }>;
    expect(literal).toEqual([]);
  });

  it("'-term' excludes docs carrying the negated word", async () => {
    const c = phraseClient();
    await seedMinus(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", "database -cooking").take(10),
    )) as Array<{ title: string }>;
    expect(docs.map((d) => d.title)).toEqual(["Database advanced"]);
  });

  it("snippet:true attaches a <mark>-wrapped _searchSnippet to each hit", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", "database", { snippet: true }).take(10),
    )) as Array<{ title: string; _searchSnippet?: string }>;
    expect(docs).toHaveLength(2);
    for (const d of docs) {
      expect(typeof d._searchSnippet).toBe("string");
      expect(d._searchSnippet).toContain("<mark>Database</mark>");
      expect(d._searchSnippet?.split(" ").length).toBeLessThanOrEqual(35);
    }
  });

  it("a phrase query snippet marks each adjacent word", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", '"database notes"', { snippet: true }).take(10),
    )) as Array<{ title: string; _searchSnippet?: string }>;
    expect(docs).toHaveLength(1);
    expect(docs[0]._searchSnippet).toContain("<mark>Database</mark> <mark>notes</mark>");
  });

  it("snippet honors the ≤35-word bound on a long doc", async () => {
    const c = phraseClient();
    const words = Array.from({ length: 60 }, (_, i) => `w${i}`);
    words[30] = "database";
    await c.mutate(
      mutation()
        .insert("notes", { title: "long doc", body: words.join(" ") })
        .build(),
    );
    const docs = (await c.query(
      phraseApi.notes.query().search("search_text", "database", { snippet: true }).take(10),
    )) as Array<{ title: string; _searchSnippet?: string }>;
    expect(docs).toHaveLength(1);
    expect(docs[0]._searchSnippet).toBeDefined();
    expect(docs[0]._searchSnippet?.split(" ").length).toBe(35);
  });

  it("_searchSnippet is absent when snippet is omitted and when explicitly false", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    const omitted = (await c.query(
      phraseApi.notes.query().search("search_text", "database").take(10),
    )) as Array<{ title: string; _searchSnippet?: string }>;
    expect(omitted).toHaveLength(2);
    for (const d of omitted) expect(d._searchSnippet).toBeUndefined();
    const off = (await c.query(
      phraseApi.notes.query().search("search_text", "database", { snippet: false }).take(10),
    )) as Array<{ title: string; _searchSnippet?: string }>;
    expect(off).toHaveLength(2);
    for (const d of off) expect(d._searchSnippet).toBeUndefined();
  });

  it("rejects snippet:true combined with mode 'trgm' (BAD_REQUEST)", async () => {
    const c = phraseClient();
    await seedPhrases(c);
    await expect(
      c.query(
        phraseApi.notes
          .query()
          .search("search_text", "database", { mode: "trgm", snippet: true })
          .take(10),
      ),
    ).rejects.toThrow(/snippet is only supported in tsquery mode/);
  });
});

describe("InMemoryRtDbClient — vector search", () => {
  const vectorSchema = defineSchema({
    docs: defineTable({
      embedding: t.vector(3),
      userId: t.string(),
      status: t.string(),
    }).vectorIndex("by_embedding", "embedding", 3, ["userId"]),
  });
  const vectorApi = createApi(vectorSchema);

  function vectorClient(): InMemoryRtDbClient {
    let ms = 1_700_000_000_000;
    const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
    c.pushSchema(vectorSchema);
    return c;
  }

  async function seed(c: InMemoryRtDbClient): Promise<void> {
    await c.mutate(
      mutation()
        .insert("docs", { embedding: [1, 0, 0], userId: "u1", status: "open" })
        .build(),
    );
    await c.mutate(
      mutation()
        .insert("docs", { embedding: [0, 1, 0], userId: "u2", status: "open" })
        .build(),
    );
    await c.mutate(
      mutation()
        .insert("docs", { embedding: [0, 0, 1], userId: "u1", status: "closed" })
        .build(),
    );
  }

  it("narrows results by a vector-search-level filter (full FilterExpr)", async () => {
    const c = vectorClient();
    await seed(c);
    // Three docs total; the eq filter on `userId` narrows to the two u1 docs.
    const docs = (await c.query(
      vectorApi.docs
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], {
          limit: 10,
          filter: { op: "eq", field: "userId", value: "u1" },
        })
        .collect(),
    )) as Array<{ userId: string }>;
    expect(docs.map((d) => d.userId)).toEqual(["u1", "u1"]);
  });

  it("respects `limit` after applying the filter", async () => {
    const c = vectorClient();
    await seed(c);
    // Two u1 docs match the filter; limit:1 returns just the first.
    const docs = (await c.query(
      vectorApi.docs
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], {
          limit: 1,
          filter: { op: "eq", field: "userId", value: "u1" },
        })
        .collect(),
    )) as Array<{ userId: string }>;
    expect(docs).toHaveLength(1);
    expect(docs[0].userId).toBe("u1");
  });

  it("a compound and/in filter narrows on multiple fields", async () => {
    const c = vectorClient();
    await seed(c);
    // u1 + status in [open, closed] matches both u1 docs.
    const docs = (await c.query(
      vectorApi.docs
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], {
          limit: 10,
          filter: {
            op: "and",
            exprs: [
              { op: "eq", field: "userId", value: "u1" },
              { op: "in", field: "status", values: ["closed"] },
            ],
          },
        })
        .collect(),
    )) as Array<{ userId: string; status: string }>;
    expect(docs.map((d) => ({ userId: d.userId, status: d.status }))).toEqual([
      { userId: "u1", status: "closed" },
    ]);
  });

  it("rejects a vector-search-level filter on an unknown field (BAD_REQUEST)", async () => {
    const c = vectorClient();
    await seed(c);
    await expect(
      c.query(
        vectorApi.docs
          .query()
          .vectorSearch("by_embedding", [1, 0, 0], {
            limit: 10,
            filter: { op: "eq", field: "nope", value: "x" },
          })
          .collect(),
      ),
    ).rejects.toThrow(/filter references unknown field 'nope'/);
  });

  it("rejects an unknown vector index (BAD_REQUEST)", async () => {
    const c = vectorClient();
    await seed(c);
    await expect(
      c.query(vectorApi.docs.query().vectorSearch("nope", [1, 0, 0], { limit: 10 }).collect()),
    ).rejects.toThrow(/vector index 'nope' not found/);
  });
});

describe("InMemoryRtDbClient — admin surface", () => {
  it("getAudit returns seeded rows newest-first in the documented shape", async () => {
    const c = newClient();
    c.admin.seedAudit([
      {
        tsMs: 100,
        db: "db",
        table: "items",
        op: "insert",
        docId: "a",
        principal: "u1",
        source: "client",
      },
      { tsMs: 300, table: "items", op: "patch", docId: "b" },
      { tsMs: 200, table: "items", op: "delete", docId: "c" },
    ]);
    const rows = await c.admin.getAudit();
    expect(rows.map((r) => r.docId)).toEqual(["b", "c", "a"]); // newest tsMs first
    expect(rows[0]).toMatchObject({
      id: 2,
      tsMs: 300,
      db: "db",
      table: "items",
      op: "patch",
      docId: "b",
      principal: null,
      source: "client",
    });
  });

  it("getAudit filters by table/op/principal and pages with limit/offset", async () => {
    const c = newClient();
    c.admin.seedAudit([
      { tsMs: 1, table: "items", op: "insert", docId: "a", principal: "u1" },
      { tsMs: 2, table: "items", op: "insert", docId: "b", principal: "u2" },
      { tsMs: 3, table: "notes", op: "insert", docId: "c", principal: "u1" },
    ]);
    expect((await c.admin.getAudit({ op: "insert" })).length).toBe(3);
    expect((await c.admin.getAudit({ principal: "u1" })).map((r) => r.docId).sort()).toEqual([
      "a",
      "c",
    ]);
    // Newest-first order is [c, b, a]; offset 1 + limit 1 ⇒ [b].
    const paged = await c.admin.getAudit({ limit: 1, offset: 1 });
    expect(paged).toHaveLength(1);
    expect(paged[0].docId).toBe("b");
  });

  it("getAudit returns [] when nothing is seeded or nothing matches", async () => {
    const c = newClient();
    expect(await c.admin.getAudit()).toEqual([]);
    c.admin.seedAudit([{ tsMs: 1, table: "items", op: "insert", docId: "a" }]);
    expect(await c.admin.getAudit({ table: "nope" })).toEqual([]);
  });

  it("listSubscriptions reflects registered subscriptions and clears on unsubscribe", async () => {
    const c = newClient();
    expect((await c.admin.listSubscriptions()).subscriptions).toEqual([]);
    const unsub = c.subscribe(api.items.query().withIndex("by_status", ["todo"]).count(), () => {});
    const res = await c.admin.listSubscriptions();
    expect(res.subscriptions).toHaveLength(1);
    expect(res.subscriptions[0]).toMatchObject({
      db: "db",
      table: "items",
      terminal: "count",
      readSetClass: "indexed",
      principal: null,
    });
    expect(res.perDb).toHaveLength(1);
    unsub();
    expect((await c.admin.listSubscriptions()).subscriptions).toEqual([]);
  });

  it("listSubscriptions filters by db", async () => {
    const c = newClient();
    c.subscribe(api.items.query().collect(), () => {});
    const other = await c.admin.listSubscriptions({ db: "other" });
    expect(other.subscriptions).toEqual([]);
    const mine = await c.admin.listSubscriptions({ db: "db" });
    expect(mine.subscriptions).toHaveLength(1);
  });

  it("mergeUsers is a no-op that resolves an empty report", async () => {
    const c = newClient();
    await expect(c.admin.mergeUsers("anon1", "real1")).resolves.toEqual({
      dbs: {},
      storageRepointed: 0,
      sessionsRepointed: 0,
      anonDeleted: false,
    });
  });
});
