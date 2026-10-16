import { describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { mutation } from "../src/mutation.js";
import { projectOptimisticUpdate } from "../src/optimistic.js";
import type { SchemaJson, StepJson } from "../src/protocol.js";
import type { RtQuery } from "../src/query.js";

const schema: SchemaJson = {
  tables: { tasks: { fields: { name: { type: "string" } }, indexes: [] } },
};
const all: RtQuery<unknown[]> = { json: { table: "tasks" } };
function fixture(now: () => number) {
  const client = new InMemoryRtDbClient({ now, random: () => 0 });
  client.pushSchema(schema);
  return client;
}
describe("transaction server-clock deadlines", () => {
  it("rechecks guard-only transactions at the final decision and preserves resource-cap error precedence", async () => {
    let reads = 0;
    const client = fixture(() => (++reads === 1 ? 1000 : 1500));
    reads = 0;
    await expect(client.mutate(mutation().expectBefore(1500).build())).rejects.toMatchObject({
      code: "PRECONDITION_FAILED",
    });
    const huge = {
      steps: Array.from({ length: 1025 }, () => ({ op: "expectBefore" as const, deadlineMs: 0 })),
    };
    await expect(client.mutate(huge)).rejects.toMatchObject({ code: "BAD_REQUEST" });
  });
  it("encodes the additive DSL step and commits before its exclusive deadline", async () => {
    const client = fixture(() => 1000);
    const txn = mutation().expectBefore(1001).insert("tasks", { name: "accepted" }).build();
    expect(txn.steps[0]).toEqual({ op: "expectBefore", deadlineMs: 1001 });
    expect(await client.mutate(txn)).toMatchObject([null, { id: expect.any(String) }]);
    expect(await client.query(all)).toMatchObject([{ name: "accepted" }]);
  });
  it.each([1000, 999, 0])(
    "rejects at or after deadline %s without document or subscription effects",
    async (deadline) => {
      const client = fixture(() => 1000);
      const updates: unknown[] = [];
      const stop = client.subscribe(all, (value) => updates.push(value));
      const txn = mutation().insert("tasks", { name: "forbidden" }).expectBefore(deadline).build();
      await expect(client.mutate(txn)).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
      expect(await client.query(all)).toEqual([]);
      expect(updates).toEqual([[]]);
      stop();
    },
  );
  it("rolls back a transaction whose clock expires while its insert is being applied", async () => {
    let reads = 0;
    let armed = false;
    const client = fixture(() => (armed && ++reads > 1 ? 1500 : 1000));
    armed = true;
    await expect(
      client.mutate(mutation().expectBefore(1500).insert("tasks", { name: "rolled-back" }).build()),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    expect(reads).toBeGreaterThan(1);
    expect(await client.query(all)).toEqual([]);
  });
  it("uses the earliest of several guards and preserves independent scheduled execution boundaries", async () => {
    let now = 1000;
    const client = fixture(() => now);
    await expect(
      client.mutate(mutation().expectBefore(2000).expectBefore(1000).build()),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    await client.mutate({
      steps: [
        { op: "expectBefore", deadlineMs: 1100 },
        {
          op: "schedule",
          when: { type: "runAt", ms: 1200 },
          txn: {
            steps: [
              { op: "expectBefore", deadlineMs: 1150 },
              { op: "insert", table: "tasks", doc: { name: "child" } },
            ],
          },
        },
      ],
    });
    now = 1200;
    client.tick();
    expect(await client.query(all)).toEqual([]);
  });
  it("replays an already committed key after expiry without executing the transaction again", async () => {
    let now = 1000;
    const client = fixture(() => now);
    const txn = mutation().expectBefore(1100).insert("tasks", { name: "once" }).build();
    const original = await client.mutate(txn, { idempotencyKey: "same-canonical-request" });
    now = 1200;
    expect(await client.mutate(txn, { idempotencyKey: "same-canonical-request" })).toEqual(
      original,
    );
    await expect(client.mutate(txn, { idempotencyKey: "new-request" })).rejects.toMatchObject({
      code: "PRECONDITION_FAILED",
    });
    expect(await client.query(all)).toHaveLength(1);
  });
  it("rejects malformed deadline values and extra keys before writes", async () => {
    const client = fixture(() => 1000);
    const values: unknown[] = [
      null,
      true,
      "2000",
      1.5,
      -1,
      Number.MAX_SAFE_INTEGER + 1,
      NaN,
      Infinity,
      undefined,
    ];
    for (const deadlineMs of values) {
      const step = { op: "expectBefore", deadlineMs } as unknown as StepJson;
      await expect(client.mutate({ steps: [step] })).rejects.toMatchObject({ code: "BAD_REQUEST" });
    }
    await expect(
      client.mutate({
        steps: [{ op: "expectBefore", deadlineMs: 2000, extra: true } as unknown as StepJson],
      }),
    ).rejects.toMatchObject({ code: "BAD_REQUEST" });
    expect(await client.query(all)).toEqual([]);
  });
  it("does not optimistically publish effects whose deadline depends on the authoritative clock", () => {
    expect(
      projectOptimisticUpdate(
        all.json,
        [],
        mutation().expectBefore(2000).insert("tasks", { name: "pending" }).build(),
      ),
    ).toEqual({ overlaid: false });
  });
});
