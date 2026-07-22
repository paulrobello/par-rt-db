import { describe, expect, it } from "vitest";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  items: defineTable({
    projectId: t.id("projects"),
    status: t.string(),
    order: t.number(),
  })
    .index("by_project", ["projectId"])
    .index("by_project_and_status", ["projectId", "status"]),
});

const api = createApi(schema);

describe("query builder", () => {
  it("builds an index query with eq, order, and take", () => {
    const q = api.items
      .query()
      .withIndex("by_project_and_status", ["p1", "in_progress"])
      .order("desc")
      .take(500);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1", "in_progress"],
      order: "desc",
      take: 500,
    });
  });

  it("builds a collect query (no take)", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).collect();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"] });
  });

  it("builds a unique query", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).unique();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], unique: true });
  });

  it("builds a full-table collect with no index", () => {
    const q = api.items.query().collect();
    expect(q.json).toEqual({ table: "items" });
  });

  it("builds a point read", () => {
    const q = api.items.get("abc123");
    expect(q.json).toEqual({ table: "items", get: "abc123" });
  });

  it("builds a range query with gt and lt after an eq prefix", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).gt(1).lt(5).collect();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], gt: 1, lt: 5 });
  });

  it("builds a range query with gte and lte", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).gte("a").lte("m").collect();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], gte: "a", lte: "m" });
  });

  it("combines a range bound with order and take", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).gt(1).order("desc").take(10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      gt: 1,
      order: "desc",
      take: 10,
    });
  });
});
