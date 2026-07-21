import { describe, expectTypeOf, it } from "vitest";
import type { Doc, Id, WithoutSystemFields } from "../src/schema.js";
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

type Project = Doc<typeof schema, "projects">;
type NewProject = WithoutSystemFields<typeof schema, "projects">;

describe("schema type inference", () => {
  it("derives read docs with system fields and correct optionality", () => {
    expectTypeOf<Project["name"]>().toEqualTypeOf<string>();
    expectTypeOf<Project["status"]>().toEqualTypeOf<"active" | "paused">();
    expectTypeOf<Project["order"]>().toEqualTypeOf<number>();
    // optional field -> optional key
    expectTypeOf<Project>().toHaveProperty("archived");
    expectTypeOf<Project["_id"]>().toEqualTypeOf<Id<"projects">>();
    expectTypeOf<Project["_creationTime"]>().toEqualTypeOf<number>();
    expectTypeOf<Project["_version"]>().toEqualTypeOf<number>();
  });

  it("excludes system fields from insert input", () => {
    expectTypeOf<NewProject>().not.toHaveProperty("_id");
    expectTypeOf<NewProject>().not.toHaveProperty("_creationTime");
  });

  it("brands ids per table", () => {
    expectTypeOf<Doc<typeof schema, "items">["projectId"]>().toEqualTypeOf<Id<"projects">>();
  });
});
