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

  it("serializes record/any/bytes/int64 field types", () => {
    const s = defineSchema({
      widgets: defineTable({
        tags: t.record(t.string()),
        payload: t.any(),
        blob: t.bytes(),
        big: t.int64(),
      }),
    });
    expect(s.toJSON().tables.widgets.fields).toEqual({
      tags: { type: "record", value: { type: "string" } },
      payload: { type: "any" },
      blob: { type: "bytes" },
      big: { type: "int64" },
    });
  });

  it("serializes a record of optional numbers", () => {
    const s = defineSchema({
      widgets: defineTable({ counts: t.record(t.optional(t.number())) }),
    });
    expect(s.toJSON().tables.widgets.fields).toEqual({
      counts: { type: "record", value: { type: "optional", inner: { type: "number" } } },
    });
  });
});

describe("searchIndex builder", () => {
  it("emits a search index with search:true alongside a btree index", () => {
    const s = defineSchema({
      notes: defineTable({
        title: t.string(),
        body: t.string(),
      })
        .index("by_title", ["title"])
        .searchIndex("search_content", ["title", "body"]),
    });
    expect(s.toJSON().tables.notes.indexes).toEqual([
      { name: "by_title", fields: ["title"] },
      { name: "search_content", fields: ["title", "body"], search: true },
    ]);
  });

  it("a btree index omits the search flag", () => {
    const s = defineSchema({
      notes: defineTable({ title: t.string() }).index("by_title", ["title"]),
    });
    expect(s.toJSON().tables.notes.indexes).toEqual([{ name: "by_title", fields: ["title"] }]);
  });
});
