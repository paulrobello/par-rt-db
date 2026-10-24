import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

/** Declarative per-field write constraints (the server's
 *  `TableDef::constraints`): `.constraints()` builder wire shape plus
 *  in-memory push-time and write-path enforcement. Cross-engine agreement is
 *  pinned by the `constraint-*` / `error-constraint-*` semantics-corpus
 *  cases; these unit tests pin the DSL builder surface. */

function tasksSchema() {
  return defineSchema({
    tasks: defineTable({
      name: t.string(),
      score: t.number(),
      counter: t.int64(),
      note: t.optional(t.string()),
    })
      .index("by_name", ["name"])
      .constraints({
        score: { min: 1, max: 10 },
        counter: { min: 1, max: 100 },
        name: { minLength: 2, maxLength: 5, pattern: "^[a-z]+$" },
      }),
  });
}

function client(): InMemoryRtDbClient {
  return new InMemoryRtDbClient({ now: () => 1700000000000, random: () => 0 });
}

describe("field constraints — DSL builder wire shape", () => {
  it("round-trips constraints on the wire, omitted when absent", () => {
    const json = tasksSchema().toJSON();
    const table = json.tables.tasks;
    expect(table.constraints).toEqual({
      score: { min: 1, max: 10 },
      counter: { min: 1, max: 100 },
      name: { minLength: 2, maxLength: 5, pattern: "^[a-z]+$" },
    });

    const plain = defineTable({ name: t.string() }).toJSON();
    expect(plain.constraints).toBeUndefined();
  });
});

describe("field constraints — in-memory push-time validation", () => {
  it("rejects a misapplied constraint naming the field", () => {
    const schema = defineSchema({
      tasks: defineTable({ name: t.string() })
        .index("by_name", ["name"])
        .constraints({ name: { min: 1 } }),
    });
    expect(() => client().pushSchema(schema)).toThrowError(RtDbError);
    try {
      client().pushSchema(schema);
    } catch (err) {
      expect((err as RtDbError).code).toBe("SCHEMA_VIOLATION");
      expect((err as RtDbError).message).toContain("'name'");
      expect((err as RtDbError).message).toContain("number/int64");
    }
  });

  it("rejects an invalid regex naming the field", () => {
    const schema = defineSchema({
      tasks: defineTable({ name: t.string() })
        .index("by_name", ["name"])
        .constraints({ name: { pattern: "([a-z" } }),
    });
    try {
      client().pushSchema(schema);
    } catch (err) {
      expect((err as RtDbError).code).toBe("SCHEMA_VIOLATION");
      expect((err as RtDbError).message).toContain("invalid pattern");
    }
  });
});

describe("field constraints — in-memory write-path enforcement", () => {
  it("accepts inclusive boundary values", async () => {
    const c = client();
    c.pushSchema(tasksSchema());
    await expect(
      c.mutate({
        steps: [
          {
            op: "insert",
            table: "tasks",
            doc: { name: "aa", score: 1, counter: "1" },
          },
          {
            op: "insert",
            table: "tasks",
            doc: { name: "abcde", score: 10, counter: "100" },
          },
        ],
      }),
    ).resolves.toBeDefined();
  });

  it("fails an insert below min with SCHEMA_VIOLATION and rolls back", async () => {
    const c = client();
    c.pushSchema(tasksSchema());
    try {
      await c.mutate({
        steps: [
          {
            op: "insert",
            table: "tasks",
            doc: { name: "good", score: 5, counter: "1" },
          },
          {
            op: "insert",
            table: "tasks",
            doc: { name: "bad", score: 0.5, counter: "1" },
          },
        ],
      });
      expect.unreachable("mutation should have failed");
    } catch (err) {
      expect((err as RtDbError).code).toBe("SCHEMA_VIOLATION");
    }
    expect(await c.query({ json: { table: "tasks" } })).toEqual([]);
  });

  it("enforces length and pattern on strings", async () => {
    const c = client();
    c.pushSchema(tasksSchema());
    await expect(
      c.mutate({
        steps: [{ op: "insert", table: "tasks", doc: { name: "a", score: 5, counter: "1" } }],
      }),
    ).rejects.toThrowError(/'minLength' constraint/);
    await expect(
      c.mutate({
        steps: [{ op: "insert", table: "tasks", doc: { name: "abcdef", score: 5, counter: "1" } }],
      }),
    ).rejects.toThrowError(/'maxLength' constraint/);
    await expect(
      c.mutate({
        steps: [{ op: "insert", table: "tasks", doc: { name: "ABC", score: 5, counter: "1" } }],
      }),
    ).rejects.toThrowError(/'pattern' constraint/);
  });

  it("enforces constraints through patch, replace, upsert, and patchByQuery", async () => {
    const c = client();
    c.pushSchema(tasksSchema());
    const [inserted] = await c.mutate({
      steps: [
        {
          op: "insert",
          table: "tasks",
          doc: { name: "ok", score: 5, counter: "1" },
        },
      ],
    });
    const id = (inserted as { id: string }).id;
    await expect(
      c.mutate({
        steps: [{ op: "patch", table: "tasks", id, fields: { score: 50 } }],
      }),
    ).rejects.toThrowError(/'max' constraint/);
    await expect(
      c.mutate({
        steps: [
          {
            op: "replace",
            table: "tasks",
            id,
            doc: { name: "ab", score: 99, counter: "2" },
          },
        ],
      }),
    ).rejects.toThrowError(RtDbError);
    await expect(
      c.mutate({
        steps: [
          {
            op: "upsert",
            table: "tasks",
            index: "by_name",
            eq: ["ok"],
            insert: { name: "ok", score: 5, counter: "1" },
            patch: { counter: "101" },
          },
        ],
      }),
    ).rejects.toThrowError(/'max' constraint/);
    await expect(
      c.mutate({
        steps: [
          {
            op: "patchByQuery",
            table: "tasks",
            filter: { field: "name", op: "eq", value: "ok" },
            patch: { score: 99 },
          },
        ],
      }),
    ).rejects.toThrowError(/'max' constraint/);
    // Nothing survived: the stored doc is untouched.
    expect(await c.query({ json: { table: "tasks" } })).toEqual([
      {
        _id: id,
        name: "ok",
        score: 5,
        counter: "1",
        _version: 1,
        _creationTime: expect.any(Number),
      },
    ]);
  });

  it("never violates on null or absent values", async () => {
    const c = client();
    c.pushSchema(tasksSchema());
    await expect(
      c.mutate({
        steps: [
          {
            op: "insert",
            table: "tasks",
            doc: { name: "ok", score: 5, counter: "1", note: null },
          },
        ],
      }),
    ).resolves.toBeDefined();
  });
});
