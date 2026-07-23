import { describe, expect, it } from "vitest";
import { encodeCursor, decodeCursor } from "../src/pagination.js";
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

  it("builds a first query", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).first();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], first: true });
  });

  it("builds a first query with no matching rows (empty index prefix still shapes correctly)", () => {
    const q = api.items.query().first();
    expect(q.json).toEqual({ table: "items", first: true });
  });

  it("combines first with eq, order, and a range bound", () => {
    const q = api.items
      .query()
      .withIndex("by_project_and_status", ["p1", "in_progress"])
      .gt(1)
      .order("desc")
      .first();
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1", "in_progress"],
      gt: 1,
      order: "desc",
      first: true,
    });
  });

  it("builds a count query", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).count();
    expect(q.json).toEqual({ table: "items", index: "by_project", eq: ["p1"], count: true });
  });

  it("builds a count query with no index (whole-table count)", () => {
    const q = api.items.query().count();
    expect(q.json).toEqual({ table: "items", count: true });
  });

  it("combines count with eq and a range bound", () => {
    const q = api.items
      .query()
      .withIndex("by_project_and_status", ["p1", "in_progress"])
      .gt(1)
      .count();
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1", "in_progress"],
      gt: 1,
      count: true,
    });
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

describe("TableQuery.paginate", () => {
  it("builds a paginate query without cursor", () => {
    const q = api.items
      .query()
      .withIndex("by_project", ["p1"])
      .paginate(undefined, 10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      paginate: { numItems: 10 },
    });
  });

  it("builds a paginate query with cursor", () => {
    const q = api.items
      .query()
      .withIndex("by_project", ["p1"])
      .paginate("Zm9vYmFy", 10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      paginate: { cursor: "Zm9vYmFy", numItems: 10 },
    });
  });

  it("combines paginate with order", () => {
    const q = api.items
      .query()
      .withIndex("by_project", ["p1"])
      .order("desc")
      .paginate("cursor123", 20);
    expect(q.json.order).toBe("desc");
    expect(q.json.paginate).toEqual({ cursor: "cursor123", numItems: 20 });
  });
});

describe("cursor utilities", () => {
  it("round-trips an array of mixed values", () => {
    const values = ["value1", 123, 456];
    const cursor = encodeCursor(values);
    const decoded = decodeCursor(cursor);
    expect(decoded).toEqual(values);
  });

  it("round-trips an empty array", () => {
    const cursor = encodeCursor([]);
    const decoded = decodeCursor(cursor);
    expect(decoded).toEqual([]);
  });

  it("throws on an invalid cursor string", () => {
    expect(() => decodeCursor("not-valid-base64!!!")).toThrow("Invalid cursor");
  });
});

