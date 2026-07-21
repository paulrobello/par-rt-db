import { describe, expect, it } from "vitest";
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
    tags: t.array(t.string()),
  })
    .index("by_project", ["projectId"])
    .index("by_project_and_title", ["projectId", "title"]),
});

describe("schema builder", () => {
  it("serializes to the server SchemaDef JSON", () => {
    expect(schema.toJSON()).toEqual({
      tables: {
        projects: {
          fields: {
            name: { type: "string" },
            status: {
              type: "union",
              variants: [
                { type: "literal", value: "active" },
                { type: "literal", value: "paused" },
              ],
            },
            order: { type: "number" },
            archived: { type: "optional", inner: { type: "boolean" } },
          },
          indexes: [{ name: "by_name", fields: ["name"] }],
        },
        items: {
          fields: {
            projectId: { type: "id", table: "projects" },
            title: { type: "string" },
            tags: { type: "array", element: { type: "string" } },
          },
          indexes: [
            { name: "by_project", fields: ["projectId"] },
            { name: "by_project_and_title", fields: ["projectId", "title"] },
          ],
        },
      },
    });
  });

  it("omits the indexes key for a table with no indexes", () => {
    const s = defineSchema({ notes: defineTable({ body: t.string() }) });
    expect(s.toJSON().tables.notes).toEqual({ fields: { body: { type: "string" } } });
  });

  it("serializes nested object and id-array field types", () => {
    const s = defineSchema({
      t1: defineTable({
        meta: t.object({ a: t.string(), b: t.optional(t.number()) }),
        refs: t.array(t.id("t1")),
      }),
    });
    expect(s.toJSON().tables.t1.fields).toEqual({
      meta: {
        type: "object",
        fields: { a: { type: "string" }, b: { type: "optional", inner: { type: "number" } } },
      },
      refs: { type: "array", element: { type: "id", table: "t1" } },
    });
  });
});
