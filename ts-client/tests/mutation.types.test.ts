import { describe, expectTypeOf, it } from "vitest";
import { mutation } from "../src/mutation.js";
import type { Id } from "../src/schema.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  projects: defineTable({
    name: t.string(),
    status: t.union(t.literal("active"), t.literal("paused")),
    order: t.number(),
    archived: t.optional(t.boolean()),
  }).index("by_name", ["name"]),
  items: defineTable({
    projectId: t.id("projects"),
    title: t.string(),
  }).index("by_project", ["projectId"]),
});

const builder = mutation(schema);

describe("typed mutation builder", () => {
  it("accepts a valid insert doc, including omitting optional fields", () => {
    expectTypeOf(builder.insert).toBeCallableWith("projects", {
      name: "p1",
      status: "active",
      order: 1,
    });
    expectTypeOf(builder.insert).toBeCallableWith("items", {
      projectId: "proj1" as Id<"projects">,
      title: "a",
    });
  });

  it("rejects an insert against an unknown table", () => {
    // @ts-expect-error - "bogus" is not a table in the schema
    expectTypeOf(builder.insert).toBeCallableWith("bogus", { name: "p1" });
  });

  it("rejects an insert with a wrong field type", () => {
    // @ts-expect-error - order must be a number
    builder.insert("projects", { name: "p1", status: "active", order: "one" });
  });

  it("rejects an insert with an unknown field", () => {
    // @ts-expect-error - "nickname" is not a declared field
    builder.insert("projects", { name: "p1", status: "active", order: 1, nickname: "x" });
  });

  it("rejects an insert missing a required field", () => {
    // @ts-expect-error - "order" is required
    builder.insert("projects", { name: "p1", status: "active" });
  });

  it("accepts a partial patch", () => {
    expectTypeOf(builder.patch).toBeCallableWith("projects", "id1", { archived: true });
    expectTypeOf(builder.patch).toBeCallableWith("projects", "id1", { name: "renamed" });
  });

  it("rejects a patch with a wrong field type", () => {
    // @ts-expect-error - archived must be a boolean
    builder.patch("projects", "id1", { archived: "yes" });
  });

  it("rejects a patch with an unknown field", () => {
    // @ts-expect-error - "nickname" is not a declared field
    builder.patch("projects", "id1", { nickname: "x" });
  });

  it("rejects a patch that writes a system field", () => {
    // @ts-expect-error - "_id" cannot be patched directly
    builder.patch("projects", "id1", { _id: "x" });
  });

  it("keeps the untyped entry point permissive for arbitrary tables and docs", () => {
    const untyped = mutation();
    expectTypeOf(untyped.insert).toBeCallableWith("anything", { any: "shape" });
  });
});

// Server-stamped fields (FM-36 updatedAtField / FM-37 autoIncrementField)
// are optional in insert/replace inputs — the server stamps an omitted value
// and overwrites a supplied one — while a plain required field stays required.
const stampedSchema = defineSchema({
  tasks: defineTable({
    title: t.string(),
    updatedAt: t.number(),
  })
    .index("by_title", ["title"])
    .updatedAtField("updatedAt"),
  tickets: defineTable({
    title: t.string(),
    num: t.int64(),
  })
    .index("by_title", ["title"])
    .autoIncrementField("num"),
});

const stampedBuilder = mutation(stampedSchema);

describe("typed mutation builder — server-stamped fields", () => {
  it("accepts an insert/replace omitting the updatedAtField field", () => {
    expectTypeOf(stampedBuilder.insert).toBeCallableWith("tasks", { title: "a" });
    expectTypeOf(stampedBuilder.insert).toBeCallableWith("tasks", {
      title: "a",
      updatedAt: 123,
    });
    expectTypeOf(stampedBuilder.replace).toBeCallableWith("tasks", "id1", { title: "a" });
  });

  it("accepts an insert omitting the autoIncrementField field", () => {
    expectTypeOf(stampedBuilder.insert).toBeCallableWith("tickets", { title: "a" });
  });

  it("still rejects omitting a plain required field on a stamped table", () => {
    // @ts-expect-error - "title" is required
    stampedBuilder.insert("tasks", { updatedAt: 123 });
  });
});
