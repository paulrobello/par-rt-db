import { describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { mutation } from "../src/mutation.js";
import type { TransactionJson } from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  rooms: defineTable({ generation: t.number(), metadata: t.any() }).softDelete(),
  chunks: defineTable({ name: t.string() }),
});
const api = createApi(schema);
const metadata = { base: 1, tail: 1, scope: { backend: "wasm", version: 1 }, nil: null };

async function fixture(): Promise<{ client: InMemoryRtDbClient; id: string }> {
  const client = new InMemoryRtDbClient();
  client.pushSchema(schema);
  const result = await client.mutate(
    mutation(schema).insert("rooms", { generation: 1, metadata }).build(),
  );
  const first = result[0];
  if (!first || !("id" in first)) throw new Error("insert result missing id");
  return { client, id: first.id };
}

describe("field guard", () => {
  it("preserves row state, while allowing unasserted tail growth", async () => {
    const { client, id } = await fixture();
    const before = structuredClone(await client.query(api.rooms.get(id)));
    const guarded = mutation(schema)
      .expectFields("rooms", id, [
        { path: ["generation"], value: 1 },
        { path: ["metadata", "base"], value: 1.0 },
        { path: ["metadata", "scope"], value: { version: 1, backend: "wasm" } },
      ])
      .build();
    expect(await client.mutate(guarded)).toEqual([null]);
    expect(await client.query(api.rooms.get(id))).toEqual(before);
    await client.mutate(
      mutation(schema)
        .patch("rooms", id, { metadata: { ...metadata, tail: 2 } })
        .build(),
    );
    expect(await client.mutate(guarded)).toEqual([null]);
    await client.mutate(mutation(schema).patch("rooms", id, { generation: 2 }).build());
    await expect(client.mutate(guarded)).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
  });

  it("rolls back preceding writes and compares selected objects exactly", async () => {
    const { client, id } = await fixture();
    await expect(
      client.mutate(
        mutation(schema)
          .insert("chunks", { name: "before" })
          .expectFields("rooms", id, [{ path: ["metadata", "scope"], value: { backend: "wasm" } }])
          .build(),
      ),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    expect(await client.query(api.chunks.query().take(5))).toEqual([]);
    expect(
      await client.mutate(
        mutation(schema)
          .expectFields("rooms", id, [{ path: ["metadata", "nil"], value: null }])
          .build(),
      ),
    ).toEqual([null]);
    await expect(
      client.mutate(
        mutation(schema)
          .expectFields("rooms", id, [{ path: ["metadata", "missing"], value: null }])
          .build(),
      ),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    await expect(
      client.mutate(
        mutation(schema)
          .expectFields("rooms", id, [{ path: ["generation"], value: true }])
          .build(),
      ),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
  });

  it("rejects malformed wire shapes and bounded or undeclared paths", async () => {
    const { client, id } = await fixture();
    for (const assertion of [
      { path: ["metadata"] },
      { path: ["metadata"], value: null, extra: true },
    ]) {
      const malformed = {
        steps: [{ op: "expectFields", table: "rooms", id, expected: [assertion] }],
      } as unknown as TransactionJson;
      await expect(client.mutate(malformed)).rejects.toMatchObject({ code: "BAD_REQUEST" });
    }
    for (const expected of [
      [],
      [{ path: [], value: null }],
      [
        { path: ["metadata"], value: null },
        { path: ["metadata"], value: null },
      ],
    ]) {
      await expect(
        client.mutate(mutation(schema).expectFields("rooms", id, expected).build()),
      ).rejects.toMatchObject({ code: "BAD_REQUEST" });
    }
    await expect(
      client.mutate(
        mutation(schema)
          .expectFields("rooms", id, [{ path: ["unknown"], value: null }])
          .build(),
      ),
    ).rejects.toMatchObject({ code: "SCHEMA_VIOLATION" });
    await client.mutate(mutation(schema).delete("rooms", id).build());
    await expect(
      client.mutate(mutation(schema).expectFields("rooms", id, []).build()),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("observes transaction order and existing deadline semantics", async () => {
    const { client, id } = await fixture();
    await client.mutate(
      mutation(schema)
        .patch("rooms", id, { generation: 2 })
        .expectFields("rooms", id, [{ path: ["generation"], value: 2 }])
        .patch("rooms", id, { generation: 3 })
        .build(),
    );
    expect((await client.query(api.rooms.get(id)))?.generation).toBe(3);
    await expect(
      client.mutate(
        mutation(schema)
          .expectFields("rooms", id, [{ path: ["generation"], value: 3 }])
          .expectBefore(1)
          .insert("chunks", { name: "expired" })
          .build(),
      ),
    ).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
    expect(await client.query(api.chunks.query().take(5))).toEqual([]);
  });
});
