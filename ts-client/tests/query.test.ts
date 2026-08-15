import { describe, expect, it } from "vitest";
import { encodeCursor, decodeCursor } from "../src/pagination.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";
import type { FilterExpr } from "../src/protocol.js";

const schema = defineSchema({
  items: defineTable({
    projectId: t.id("projects"),
    status: t.string(),
    order: t.number(),
  })
    .index("by_project", ["projectId"])
    .index("by_project_and_status", ["projectId", "status"]),
  docs: defineTable({
    embedding: t.vector(4),
    userId: t.string(),
  }).vectorIndex("by_embedding", "embedding", 4, ["userId"]),
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

  it("builds a distinct query (consumes one eq prefix value, distincts on the next index field)", () => {
    const q = api.items.query().withIndex("by_project_and_status", ["p1"]).distinct();
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1"],
      distinct: true,
    });
  });

  it("builds an aggregate query (consumes one eq prefix value, aggregates the next index field)", () => {
    const q = api.items.query().withIndex("by_project_and_status", ["p1"]).aggregate("sum");
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1"],
      aggregate: { op: "sum" },
    });
  });

  it("builds an aggregate groupBy query (groupBy flag emitted when true)", () => {
    const q = api.items.query().withIndex("by_project_and_status", ["p1"]).aggregate("sum", true);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project_and_status",
      eq: ["p1"],
      aggregate: { op: "sum", groupBy: true },
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

describe("TableQuery.filter", () => {
  it("builds an eq filter (composable, not a terminal)", () => {
    const q = api.items.query().filter({ op: "eq", field: "status", value: "done" }).collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "eq", field: "status", value: "done" },
    });
  });

  it("builds a neq filter", () => {
    const q = api.items.query().filter({ op: "neq", field: "status", value: "done" }).collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "neq", field: "status", value: "done" },
    });
  });

  it("builds an in filter", () => {
    const q = api.items
      .query()
      .filter({ op: "in", field: "status", values: ["blocked", "backlog"] })
      .collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "in", field: "status", values: ["blocked", "backlog"] },
    });
  });

  it("nests range leaves under and (gte/lt)", () => {
    const q = api.items
      .query()
      .filter({
        op: "and",
        exprs: [
          { op: "gte", field: "order", value: 1 },
          { op: "lt", field: "order", value: 10 },
        ],
      })
      .collect();
    expect(q.json).toEqual({
      table: "items",
      filter: {
        op: "and",
        exprs: [
          { op: "gte", field: "order", value: 1 },
          { op: "lt", field: "order", value: 10 },
        ],
      },
    });
  });

  it("nests combinators under or (in + lte)", () => {
    const q = api.items
      .query()
      .filter({
        op: "or",
        exprs: [
          { op: "in", field: "status", values: ["blocked", "backlog"] },
          { op: "lte", field: "order", value: 3 },
        ],
      })
      .collect();
    expect(q.json).toEqual({
      table: "items",
      filter: {
        op: "or",
        exprs: [
          { op: "in", field: "status", values: ["blocked", "backlog"] },
          { op: "lte", field: "order", value: 3 },
        ],
      },
    });
  });

  it("composes with an index prefix, a gt filter, and take", () => {
    const q = api.items
      .query()
      .withIndex("by_project", ["p1"])
      .filter({ op: "gt", field: "order", value: 0 })
      .take(10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      filter: { op: "gt", field: "order", value: 0 },
      take: 10,
    });
  });

  // New variants mirroring server FilterExpr (Task 1, commit b6b6c2a):
  // `not` / `contains` / `exists` — wire shapes must match the server byte-for-byte.
  it("builds a not filter wrapping a nested expr", () => {
    const q = api.items
      .query()
      .filter({ op: "not", expr: { op: "eq", field: "status", value: "done" } })
      .collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "not", expr: { op: "eq", field: "status", value: "done" } },
    });
  });

  it("builds a contains filter (value in doc.field[])", () => {
    const q = api.items.query().filter({ op: "contains", field: "tags", value: "red" }).collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "contains", field: "tags", value: "red" },
    });
  });

  it("builds an exists filter (field present and non-null)", () => {
    const q = api.items.query().filter({ op: "exists", field: "dueAt" }).collect();
    expect(q.json).toEqual({
      table: "items",
      filter: { op: "exists", field: "dueAt" },
    });
  });
});

describe("TableQuery.search", () => {
  it("builds a search terminal and composes with take", () => {
    const q = api.items.query().search("search_content", "hello world").take(10);
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello world" },
      take: 10,
    });
  });

  it("includes filter on the wire when provided", () => {
    const filter: FilterExpr = { op: "and", exprs: [{ op: "eq", field: "status", value: "open" }] };
    const q = api.items.query().search("search_content", "hello", { filter }).take(10);
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello", filter },
      take: 10,
    });
  });

  it("omits filter on the wire when not provided", () => {
    const q = api.items.query().search("search_content", "hello world").take(10);
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello world" },
      take: 10,
    });
  });

  it("includes mode on the wire when provided", () => {
    const q = api.items.query().search("search_content", "conv", { mode: "trgm" }).take(10);
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "conv", mode: "trgm" },
      take: 10,
    });
  });

  it("omits mode on the wire when not provided", () => {
    const q = api.items.query().search("search_content", "hello world").take(10);
    expect(q.json.search).not.toHaveProperty("mode");
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello world" },
      take: 10,
    });
  });

  it("includes snippet on the wire when provided (true and false)", () => {
    const on = api.items
      .query()
      .search("search_content", "hello world", { snippet: true })
      .take(10);
    expect(on.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello world", snippet: true },
      take: 10,
    });
    // Explicit false is meaningful on the wire (server serializes Some(false));
    // only undefined drops the field.
    const off = api.items
      .query()
      .search("search_content", "hello world", { snippet: false })
      .take(10);
    expect(off.json.search).toEqual({
      index: "search_content",
      query: "hello world",
      snippet: false,
    });
  });

  it("omits snippet on the wire when not provided", () => {
    const q = api.items.query().search("search_content", "hello world").take(10);
    expect(q.json.search).not.toHaveProperty("snippet");
    expect(q.json).toEqual({
      table: "items",
      search: { index: "search_content", query: "hello world" },
      take: 10,
    });
  });
});

describe("TableQuery.vectorSearch", () => {
  it("builds a vectorSearch terminal with limit", () => {
    // `.vectorSearch` returns a TableQuery (non-terminal, mirroring `.search`);
    // read the built JSON directly via `.collect()`'s RtQuery.
    const q = api.docs.query().vectorSearch("by_embedding", [1, 0, 0], { limit: 5 }).collect();
    expect(q.json).toEqual({
      table: "docs",
      vectorSearch: { index: "by_embedding", vector: [1, 0, 0], limit: 5 },
    });
  });

  it("includes filter on the wire when provided", () => {
    const filter: FilterExpr = { op: "eq", field: "userId", value: "u1" };
    const q = api.docs
      .query()
      .vectorSearch("by_embedding", [1, 0, 0], { limit: 5, filter })
      .collect();
    expect(q.json).toEqual({
      table: "docs",
      vectorSearch: { index: "by_embedding", vector: [1, 0, 0], limit: 5, filter },
    });
  });

  it("omits filter on the wire when not provided", () => {
    const q = api.docs.query().vectorSearch("by_embedding", [1, 0], { limit: 3 }).collect();
    expect(q.json).toEqual({
      table: "docs",
      vectorSearch: { index: "by_embedding", vector: [1, 0], limit: 3 },
    });
  });
});

describe("TableQuery.hybridSearch", () => {
  it("builds a hybridSearch terminal with required fields only", () => {
    const q = api.docs.query().hybridSearch("hello world", [1, 0, 0], 5).collect();
    expect(q.json).toEqual({
      table: "docs",
      hybridSearch: { query: "hello world", vector: [1, 0, 0], limit: 5 },
    });
  });

  it("includes searchIndex/vectorIndex/k when provided", () => {
    const q = api.docs
      .query()
      .hybridSearch("hello", [1, 0, 0], 5, {
        searchIndex: "search_body",
        vectorIndex: "by_embedding",
        k: 42,
      })
      .collect();
    expect(q.json).toEqual({
      table: "docs",
      hybridSearch: {
        query: "hello",
        vector: [1, 0, 0],
        limit: 5,
        searchIndex: "search_body",
        vectorIndex: "by_embedding",
        k: 42,
      },
    });
  });

  it("omits optional fields when not provided", () => {
    const q = api.docs.query().hybridSearch("hello", [1, 0, 0], 3).collect();
    const hs = q.json.hybridSearch;
    if (hs === undefined) throw new Error("hybridSearch should be set");
    expect(hs.searchIndex).toBeUndefined();
    expect(hs.vectorIndex).toBeUndefined();
    expect(hs.k).toBeUndefined();
  });
});

describe("TableQuery.paginate", () => {
  it("builds a paginate query without cursor", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).paginate(undefined, 10);
    expect(q.json).toEqual({
      table: "items",
      index: "by_project",
      eq: ["p1"],
      paginate: { numItems: 10 },
    });
  });

  it("builds a paginate query with cursor", () => {
    const q = api.items.query().withIndex("by_project", ["p1"]).paginate("Zm9vYmFy", 10);
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
