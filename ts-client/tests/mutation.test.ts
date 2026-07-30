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
    // variant does not, and null is its own branch — mirroring rust/python.
    const [plain, upsertInsert, upsertPatch, noop] = decoded;
    if (plain && typeof plain === "object" && !("inserted" in plain)) {
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

  it("throws RtDbError on a shape the server contract does not permit", () => {
    expect(() => parseStepResults([{ noId: true }])).toThrow(RtDbError);
    expect(() => parseStepResults(["not-an-object"])).toThrow(RtDbError);
    expect(() => parseStepResults([{ id: 123 }])).toThrow(RtDbError);
  });
});
