import { describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { TxnBuilder } from "../src/mutation.js";
import type { StepJson } from "../src/protocol.js";

async function fixture() {
  const client = new InMemoryRtDbClient({ now: () => 1000 });
  client.pushSchema({
    tables: {
      counters: {
        fields: {
          key: { type: "string" },
          value: { type: "number" },
          initialized: { type: "boolean" },
          maybe: { type: "optional", inner: { type: "any" } },
          doubled: { type: "number" },
          updated: { type: "number" },
          sequence: { type: "int64" },
        },
        indexes: [{ name: "by_key", fields: ["key"], unique: true }],
        updatedAtField: "updated",
        autoIncrementField: "sequence",
        computed: {
          doubled: {
            op: "add",
            left: { op: "field", field: "value" },
            right: { op: "field", field: "value" },
          },
        },
      },
      accepted: { fields: { value: { type: "number" } }, indexes: [] },
    },
  });
  const [result] = await client.mutate({
    steps: [
      { op: "insert", table: "counters", doc: { key: "budget", value: 0, initialized: true } },
    ],
  });
  if (!result || !("id" in result)) throw new Error("counter insert failed");
  const id = result.id;
  const read = () =>
    client.query<{ value: number; doubled: number }>({ json: { table: "counters", get: id } });
  const adjust = (extra: Partial<Extract<StepJson, { op: "adjustCounter" }>> = {}) => ({
    op: "adjustCounter" as const,
    table: "counters",
    id,
    field: "value",
    delta: 1,
    min: 0,
    max: 10,
    expected: { initialized: true },
    ...extra,
  });
  return { client, id, read, adjust };
}

describe("atomic counter adjustments", () => {
  it("rolls back a workflow cancellation when a later counter bound rejects the transaction", async () => {
    const { client, adjust } = await fixture();
    const workflow = await client.startWorkflow({
      name: "counter-rollback",
      steps: [{ txn: { steps: [] } }],
    });
    await expect(
      client.mutate({
        steps: [{ op: "cancelWorkflow", id: workflow.id }, adjust({ max: 0 })],
      }),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    expect(await client.getWorkflow(workflow.id)).toMatchObject({ status: "pending" });
  });

  it("admits exactly the bounded number of concurrent transactions and rolls back rejected inserts", async () => {
    const { client, adjust, read } = await fixture();
    const results = await Promise.allSettled(
      Array.from({ length: 50 }, (_, value) =>
        client.mutate({
          steps: [{ op: "insert", table: "accepted", doc: { value } }, adjust()],
        }),
      ),
    );
    expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(10);
    expect(results.filter((result) => result.status === "rejected")).toHaveLength(40);
    for (const result of results)
      if (result.status === "rejected")
        expect(result.reason).toMatchObject({ code: "PRECONDITION_FAILED" });
    expect(await read()).toMatchObject({ value: 10, doubled: 20 });
    expect(await client.query({ json: { table: "accepted", count: true } })).toBe(10);
    expect(await client.mutate({ steps: [adjust({ delta: -1 })] })).toEqual([null]);
    expect(await client.mutate({ steps: [adjust()] })).toEqual([null]);
    expect(await read()).toMatchObject({ value: 10, doubled: 20 });
  });

  it("rejects invalid numeric operations and server-controlled fields without changing the row", async () => {
    const { client, adjust, read } = await fixture();
    for (const step of [
      adjust({ delta: 0.5 }),
      adjust({ delta: Number.MAX_SAFE_INTEGER + 1 }),
      adjust({ min: 0.5 }),
      adjust({ max: Number.POSITIVE_INFINITY }),
      adjust({ min: 4, max: 3 }),
      adjust({ field: "doubled" }),
      adjust({ field: "updated" }),
      adjust({ field: "sequence" }),
    ])
      await expect(client.mutate({ steps: [step] })).rejects.toMatchObject({ code: "BAD_REQUEST" });
    await expect(client.mutate({ steps: [adjust({ field: "key" })] })).rejects.toMatchObject({
      code: "SCHEMA_VIOLATION",
    });
    await expect(
      client.mutate({ steps: [adjust({ expected: { maybe: null } })] }),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    await expect(
      client.mutate({ steps: [adjust({ expected: { unknown: true } })] }),
    ).rejects.toMatchObject({ code: "SCHEMA_VIOLATION" });
    expect(await read()).toMatchObject({ value: 0, doubled: 0 });
  });

  it("encodes a bounded adjustment through the transaction builder", () => {
    const transaction = new TxnBuilder()
      .adjustCounter("counters", "counter-id", "value", -2, {
        min: 0,
        max: 10,
        expected: { initialized: true },
      })
      .build();
    expect(transaction).toEqual({
      steps: [
        {
          op: "adjustCounter",
          table: "counters",
          id: "counter-id",
          field: "value",
          delta: -2,
          min: 0,
          max: 10,
          expected: { initialized: true },
        },
      ],
    });
  });
});
