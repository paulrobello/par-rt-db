import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { mutation, parseStepResults } from "../src/mutation.js";
import type { Id } from "../src/schema.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

describe("transaction builder", () => {
  it("builds an ordered multi-step txn with table on every step", () => {
    const txn = mutation()
      .insert("items", { projectId: "p1", title: "a" })
      .patch("items", "i1", { title: "b" })
      .replace("items", "i4", { projectId: "p1", title: "c" })
      .delete("items", "i2")
      .expectVersion("items", "i3", 7)
      .expectAbsent("items", "by_project_and_title", ["p1", "dup"])
      .upsert("items", {
        index: "by_project_and_title",
        eq: ["p1", "x"],
        insert: { projectId: "p1", title: "x" },
        patch: { title: "x2" },
      })
      .build();

    expect(txn).toEqual({
      steps: [
        { op: "insert", table: "items", doc: { projectId: "p1", title: "a" } },
        { op: "patch", table: "items", id: "i1", fields: { title: "b" } },
        { op: "replace", table: "items", id: "i4", doc: { projectId: "p1", title: "c" } },
        { op: "delete", table: "items", id: "i2" },
        { op: "expectVersion", table: "items", id: "i3", version: 7 },
        { op: "expectAbsent", table: "items", index: "by_project_and_title", eq: ["p1", "dup"] },
        {
          op: "upsert",
          table: "items",
          index: "by_project_and_title",
          eq: ["p1", "x"],
          insert: { projectId: "p1", title: "x" },
          patch: { title: "x2" },
        },
      ],
    });
  });

  it("produces an empty txn when nothing is added", () => {
    expect(mutation().build()).toEqual({ steps: [] });
  });

  it("builds a patchByQuery step (limit omitted when absent, included when set)", () => {
    const filter = { op: "eq", field: "status", value: "todo" } as const;
    const noLimit = mutation().patchByQuery("items", filter, { title: "x" }).build();
    expect(noLimit).toEqual({
      steps: [{ op: "patchByQuery", table: "items", filter, patch: { title: "x" } }],
    });

    const withLimit = mutation().patchByQuery("items", filter, { title: "x" }, 50).build();
    expect(withLimit).toEqual({
      steps: [{ op: "patchByQuery", table: "items", filter, patch: { title: "x" }, limit: 50 }],
    });
  });

  it("builds a deleteByQuery step (limit omitted when absent, included when set)", () => {
    const filter = { op: "eq", field: "status", value: "done" } as const;
    const noLimit = mutation().deleteByQuery("items", filter).build();
    expect(noLimit).toEqual({
      steps: [{ op: "deleteByQuery", table: "items", filter }],
    });

    const withLimit = mutation().deleteByQuery("items", filter, 10).build();
    expect(withLimit).toEqual({
      steps: [{ op: "deleteByQuery", table: "items", filter, limit: 10 }],
    });
  });

  it("produces the same JSON step shape when built with a typed schema", () => {
    const schema = defineSchema({
      projects: defineTable({
        name: t.string(),
        archived: t.optional(t.boolean()),
      }).index("by_name", ["name"]),
      items: defineTable({
        projectId: t.id("projects"),
        title: t.string(),
      }).index("by_project", ["projectId"]),
    });

    const txn = mutation(schema)
      .insert("items", { projectId: "p1" as Id<"projects">, title: "a" })
      .patch("projects", "id1", { archived: true })
      .replace("items", "i4", { projectId: "p1" as Id<"projects">, title: "c" })
      .delete("items", "i2")
      .expectVersion("items", "i3", 7)
      .expectAbsent("items", "by_project", ["p1"])
      .upsert("items", {
        index: "by_project",
        eq: ["p1"],
        insert: { projectId: "p1" as Id<"projects">, title: "x" },
        patch: { title: "x2" },
      })
      .build();

    expect(txn).toEqual({
      steps: [
        { op: "insert", table: "items", doc: { projectId: "p1", title: "a" } },
        { op: "patch", table: "projects", id: "id1", fields: { archived: true } },
        { op: "replace", table: "items", id: "i4", doc: { projectId: "p1", title: "c" } },
        { op: "delete", table: "items", id: "i2" },
        { op: "expectVersion", table: "items", id: "i3", version: 7 },
        { op: "expectAbsent", table: "items", index: "by_project", eq: ["p1"] },
        {
          op: "upsert",
          table: "items",
          index: "by_project",
          eq: ["p1"],
          insert: { projectId: "p1" as Id<"projects">, title: "x" },
          patch: { title: "x2" },
        },
      ],
    });
  });
});

describe("parseStepResults", () => {
  it("decodes null as a no-op step result", () => {
    expect(parseStepResults([null])).toEqual([null]);
  });

  it("decodes {id} as an insert/patch/replace/delete step result", () => {
    expect(parseStepResults([{ id: "i1" }])).toEqual([{ id: "i1" }]);
  });

  it("decodes {id, inserted:true} as an upsert that inserted", () => {
    expect(parseStepResults([{ id: "i1", inserted: true }])).toEqual([
      { id: "i1", inserted: true },
    ]);
  });

  it("decodes {id, inserted:false} as an upsert that patched", () => {
    expect(parseStepResults([{ id: "i1", inserted: false }])).toEqual([
      { id: "i1", inserted: false },
    ]);
  });

  it("decodes a mixed array positionally aligned with steps", () => {
    const decoded = parseStepResults([
      { id: "a" },
      { id: "b", inserted: true },
      { id: "c", inserted: false },
      null,
    ]);
    expect(decoded).toEqual([
      { id: "a" },
      { id: "b", inserted: true },
      { id: "c", inserted: false },
      null,
    ]);
    // Narrowing smoke checks: the upsert variant carries `inserted`, the plain
    // variant carries only `id` (the by-query `{patched}`/`{deleted}` variants
    // carry neither), and null is its own branch — mirroring rust/python.
    const [plain, upsertInsert, upsertPatch, noop] = decoded;
    if (plain && typeof plain === "object" && "id" in plain && !("inserted" in plain)) {
      expect(plain.id).toBe("a");
    } else {
      throw new Error("plain variant did not narrow");
    }
    if (upsertInsert && typeof upsertInsert === "object" && "inserted" in upsertInsert) {
      expect(upsertInsert.inserted).toBe(true);
    } else {
      throw new Error("upsert(inserted:true) variant did not narrow");
    }
    if (upsertPatch && typeof upsertPatch === "object" && "inserted" in upsertPatch) {
      expect(upsertPatch.inserted).toBe(false);
    } else {
      throw new Error("upsert(inserted:false) variant did not narrow");
    }
    expect(noop).toBeNull();
  });

  it("decodes {patched, truncated} as a patchByQuery step result", () => {
    expect(parseStepResults([{ patched: 7, truncated: false }])).toEqual([
      { patched: 7, truncated: false },
    ]);
  });

  it("decodes {deleted, truncated} as a deleteByQuery step result", () => {
    expect(parseStepResults([{ deleted: 1000, truncated: true }])).toEqual([
      { deleted: 1000, truncated: true },
    ]);
  });

  it("narrows by-query results via 'patched'/'deleted' keys", () => {
    const [patched, deleted] = parseStepResults([
      { patched: 3, truncated: false },
      { deleted: 1, truncated: true },
    ]);
    if (patched && typeof patched === "object" && "patched" in patched) {
      expect(patched.patched).toBe(3);
      expect(patched.truncated).toBe(false);
    } else {
      throw new Error("patchByQuery variant did not narrow");
    }
    if (deleted && typeof deleted === "object" && "deleted" in deleted) {
      expect(deleted.deleted).toBe(1);
      expect(deleted.truncated).toBe(true);
    } else {
      throw new Error("deleteByQuery variant did not narrow");
    }
  });

  it("throws RtDbError on a shape the server contract does not permit", () => {
    expect(() => parseStepResults([{ noId: true }])).toThrow(RtDbError);
    expect(() => parseStepResults(["not-an-object"])).toThrow(RtDbError);
    expect(() => parseStepResults([{ id: 123 }])).toThrow(RtDbError);
    expect(() => parseStepResults([{ patched: 1 }])).toThrow(RtDbError); // missing truncated
    expect(() => parseStepResults([{ deleted: "x", truncated: false }])).toThrow(RtDbError);
  });
});
