import { describe, expectTypeOf, it } from "vitest";
import type { Doc, Id, Int64, WithoutSystemFields } from "../src/schema.js";
import { defineSchema, defineTable, fromInt64, t, toInt64 } from "../src/schema.js";

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
  widgets: defineTable({
    tags: t.record(t.string()),
    payload: t.any(),
    blob: t.bytes(),
    big: t.int64(),
  }),
});

type Project = Doc<typeof schema, "projects">;
type NewProject = WithoutSystemFields<typeof schema, "projects">;
type Widget = Doc<typeof schema, "widgets">;

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

  it("infers record/any/bytes/int64 field types", () => {
    expectTypeOf<Widget["tags"]>().toEqualTypeOf<Record<string, string>>();
    expectTypeOf<Widget["payload"]>().toEqualTypeOf<unknown>();
    expectTypeOf<Widget["blob"]>().toEqualTypeOf<string>();
    expectTypeOf<Widget["big"]>().toEqualTypeOf<Int64>();
  });

  it("Int64 is a branded string convertible via toInt64/fromInt64", () => {
    expectTypeOf<Int64>().toMatchTypeOf<string>();
    expectTypeOf(toInt64).returns.toEqualTypeOf<Int64>();
    expectTypeOf(fromInt64).parameter(0).toEqualTypeOf<Int64>();
    expectTypeOf(fromInt64).returns.toEqualTypeOf<bigint>();
  });
});
