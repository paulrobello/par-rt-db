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

describe("vectorIndex builder", () => {
  it("emits a vector index with dimensions and filterFields alongside a btree index", () => {
    const s = defineSchema({
      docs: defineTable({
        embedding: t.vector(4),
        userId: t.string(),
      })
        .index("by_user", ["userId"])
        .vectorIndex("by_embedding", "embedding", 4, ["userId"]),
    });
    expect(s.toJSON().tables.docs.fields).toEqual({
      embedding: { type: "vector", dimensions: 4 },
      userId: { type: "string" },
    });
    expect(s.toJSON().tables.docs.indexes).toEqual([
      { name: "by_user", fields: ["userId"] },
      {
        name: "by_embedding",
        fields: ["embedding"],
        vector: { dimensions: 4, filterFields: ["userId"] },
      },
    ]);
  });

  it("omits filterFields on the wire when none are declared", () => {
    const s = defineSchema({
      docs: defineTable({ embedding: t.vector(8) }).vectorIndex("by_embedding", "embedding", 8),
    });
    expect(s.toJSON().tables.docs.indexes).toEqual([
      { name: "by_embedding", fields: ["embedding"], vector: { dimensions: 8 } },
    ]);
  });

  it("a btree index omits the vector key", () => {
    const s = defineSchema({
      docs: defineTable({ embedding: t.vector(4), userId: t.string() }).index("by_user", [
        "userId",
      ]),
    });
    expect(s.toJSON().tables.docs.indexes).toEqual([{ name: "by_user", fields: ["userId"] }]);
  });
});

describe("ownerField builder", () => {
  it("emits ownerField on the wire when set, alongside an index", () => {
    const s = defineSchema({
      notes: defineTable({ userId: t.string(), title: t.string() })
        .index("by_user", ["userId"])
        .ownerField("userId"),
    });
    expect(s.toJSON().tables.notes).toMatchObject({
      fields: {
        userId: { type: "string" },
        title: { type: "string" },
      },
      indexes: [{ name: "by_user", fields: ["userId"] }],
      ownerField: "userId",
    });
  });

  it("omits ownerField on the wire when absent", () => {
    const s = defineSchema({ notes: defineTable({ title: t.string() }) });
    expect(s.toJSON().tables.notes).not.toHaveProperty("ownerField");
  });
});

describe("collaboratorsField builder", () => {
  it("emits collaboratorsField on the wire when set alongside ownerField", () => {
    const s = defineSchema({
      notes: defineTable({
        userId: t.string(),
        collaborators: t.array(t.string()),
        title: t.string(),
      })
        .index("by_user", ["userId"])
        .ownerField("userId")
        .collaboratorsField("collaborators"),
    });
    expect(s.toJSON().tables.notes).toMatchObject({
      fields: {
        userId: { type: "string" },
        collaborators: { type: "array", element: { type: "string" } },
        title: { type: "string" },
      },
      indexes: [{ name: "by_user", fields: ["userId"] }],
      ownerField: "userId",
      collaboratorsField: "collaborators",
    });
  });

  it("omits collaboratorsField on the wire when absent", () => {
    const s = defineSchema({ notes: defineTable({ title: t.string() }) });
    expect(s.toJSON().tables.notes).not.toHaveProperty("collaboratorsField");
  });
});
