/**
 * QA-001 / QA-002 cross-client combination-matrix safety net (TS mirror).
 *
 * Mirrors `server/tests/query_combinations.rs` case-for-case. Both files run the
 * SAME matrix against their respective `execute_query` implementations and must
 * agree on every accept/reject. Adding a new terminal? Add cases here AND in the
 * server mirror — the matrix exists so the next terminal addition fails the gate
 * on whichever side forgets (this is exactly the drift class that produced
 * QA-001: the TS `get` guard omitted `filter`/`search`/`vectorSearch`).
 */
import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { InMemoryRtDbClient } from "../src/in_memory.js";
import type { QueryJson } from "../src/protocol.js";
import type { RtQuery } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

// Schema with btree + search + vector indexes on one table — enough to drive
// every terminal in the matrix. The 2-field `by_title_count` index covers
// aggregate's `eq prefix + aggregate field` shape and the groupBy
// 2-fields-beyond-prefix shape.
const schema = defineSchema({
  items: defineTable({
    title: t.string(),
    body: t.string(),
    count: t.number(),
    embedding: t.vector(3),
  })
    .index("by_title", ["title"])
    .index("by_title_count", ["title", "count"])
    .searchIndex("search_body", ["title", "body"])
    .vectorIndex("by_embedding", "embedding", 3),
});

function newClient(): InMemoryRtDbClient {
  const c = new InMemoryRtDbClient({ now: () => 1_700_000_000_000, random: () => 0 });
  c.pushSchema(schema);
  return c;
}

const ID = "0123456789abcdef0123456789abcdef";

function baseQuery(): QueryJson {
  return { table: "items" };
}

function filterEqTitleX() {
  return { op: "eq" as const, field: "title", value: "x" };
}
function searchBodyX() {
  return { index: "search_body", query: "x" };
}
function vectorEmbeddingLimit1() {
  return { index: "by_embedding", vector: [0, 0, 0], limit: 1 };
}
function hybridQueryDatabaseX() {
  return { query: "x", vector: [0, 0, 0], limit: 1 };
}
function paginateNum1() {
  return { numItems: 1 };
}

enum Outcome {
  Accept,
  Reject,
}

interface Case {
  name: string;
  build: (q: QueryJson) => void;
  expected: Outcome;
}

const CASES: readonly Case[] = [
  // ============ Solo accepts (each terminal alone is valid baseline) ============
  {
    name: "solo: get",
    build: (q) => {
      q.get = ID;
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: collect",
    build: () => {
      /* base only */
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: index",
    build: (q) => {
      q.index = "by_title";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: eq",
    build: (q) => {
      q.index = "by_title";
      q.eq = ["x"];
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: gt",
    build: (q) => {
      q.index = "by_title";
      q.gt = "x";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: gte",
    build: (q) => {
      q.index = "by_title";
      q.gte = "x";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: lt",
    build: (q) => {
      q.index = "by_title";
      q.lt = "x";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: lte",
    build: (q) => {
      q.index = "by_title";
      q.lte = "x";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: order",
    build: (q) => {
      q.order = "asc";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: take",
    build: (q) => {
      q.take = 1;
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: unique",
    build: (q) => {
      q.unique = true;
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: first",
    build: (q) => {
      q.first = true;
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: count",
    build: (q) => {
      q.count = true;
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: distinct",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: paginate",
    build: (q) => {
      q.paginate = paginateNum1();
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: filter",
    build: (q) => {
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: search",
    build: (q) => {
      q.search = searchBodyX();
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: vectorSearch",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
    },
    expected: Outcome.Accept,
  },
  {
    name: "solo: hybridSearch",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Accept,
  },
  // ============ get rejects every peer (QA-001: last 3 are the drift) ============
  {
    name: "get+index",
    build: (q) => {
      q.get = ID;
      q.index = "by_title";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+eq",
    build: (q) => {
      q.get = ID;
      q.eq = ["x"];
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+gt",
    build: (q) => {
      q.get = ID;
      q.gt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+gte",
    build: (q) => {
      q.get = ID;
      q.gte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+lt",
    build: (q) => {
      q.get = ID;
      q.lt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+lte",
    build: (q) => {
      q.get = ID;
      q.lte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+order",
    build: (q) => {
      q.get = ID;
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+take",
    build: (q) => {
      q.get = ID;
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+unique",
    build: (q) => {
      q.get = ID;
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+first",
    build: (q) => {
      q.get = ID;
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+count",
    build: (q) => {
      q.get = ID;
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+paginate",
    build: (q) => {
      q.get = ID;
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+filter",
    build: (q) => {
      q.get = ID;
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+search",
    build: (q) => {
      q.get = ID;
      q.search = searchBodyX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+vectorSearch",
    build: (q) => {
      q.get = ID;
      q.vectorSearch = vectorEmbeddingLimit1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "get+hybridSearch",
    build: (q) => {
      q.get = ID;
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Reject,
  },
  // ============ unique rejects take, order ============
  {
    name: "unique+take",
    build: (q) => {
      q.unique = true;
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "unique+order",
    build: (q) => {
      q.unique = true;
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  // ============ first rejects unique, take ============
  {
    name: "first+unique",
    build: (q) => {
      q.first = true;
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "first+take",
    build: (q) => {
      q.first = true;
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  // ============ count rejects unique, take, first, order, distinct ============
  {
    name: "count+unique",
    build: (q) => {
      q.count = true;
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "count+take",
    build: (q) => {
      q.count = true;
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "count+first",
    build: (q) => {
      q.count = true;
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "count+order",
    build: (q) => {
      q.count = true;
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "count+distinct",
    build: (q) => {
      q.count = true;
      q.distinct = true;
    },
    expected: Outcome.Reject,
  },
  // ============ distinct rejects get, take, unique, first, count, order,
  //              paginate, search, vectorSearch (standalone terminal like count) ============
  {
    name: "distinct+get",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.get = ID;
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+take",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+unique",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+first",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+count",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+order",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+paginate",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+search",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.search = searchBodyX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+vectorSearch",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.vectorSearch = vectorEmbeddingLimit1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "distinct+hybridSearch",
    build: (q) => {
      q.distinct = true;
      q.index = "by_title";
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Reject,
  },
  // ============ aggregate rejects get, take, unique, first, count, distinct,
  //              order, paginate, search, vectorSearch (standalone terminal
  //              like count/distinct); composes with index/eq/range/filter ============
  {
    name: "solo: aggregate",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
    },
    expected: Outcome.Accept,
  },
  {
    name: "aggregate+get",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.get = ID;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+take",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+unique",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+first",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+count",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+distinct",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.distinct = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+order",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+paginate",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+search",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.search = searchBodyX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+vectorSearch",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.vectorSearch = vectorEmbeddingLimit1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "aggregate+hybridSearch",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "compose: aggregate+eq",
    build: (q) => {
      // by_title_count has [title, count]; consuming title leaves count (numeric)
      // as the aggregate field, so SUM is valid.
      q.aggregate = { op: "sum" };
      q.index = "by_title_count";
      q.eq = ["x"];
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: aggregate+filter",
    build: (q) => {
      q.aggregate = { op: "min" };
      q.index = "by_title";
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Accept,
  },
  // ============ paginate rejects count, unique, first, take (get covered above) ============
  {
    name: "paginate+count",
    build: (q) => {
      q.paginate = paginateNum1();
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "paginate+unique",
    build: (q) => {
      q.paginate = paginateNum1();
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "paginate+first",
    build: (q) => {
      q.paginate = paginateNum1();
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "paginate+take",
    build: (q) => {
      q.paginate = paginateNum1();
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  // ============ range-bound incompatibilities ============
  {
    name: "gt+gte",
    build: (q) => {
      q.index = "by_title";
      q.gt = "x";
      q.gte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "lt+lte",
    build: (q) => {
      q.index = "by_title";
      q.lt = "x";
      q.lte = "x";
    },
    expected: Outcome.Reject,
  },
  // ============ vectorSearch rejects every peer (take included) ============
  {
    name: "vectorSearch+index",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.index = "by_title";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+eq",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.eq = ["x"];
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+gt",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.gt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+gte",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.gte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+lt",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.lt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+lte",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.lte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+order",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+unique",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+first",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+count",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+paginate",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+filter",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+search",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.search = searchBodyX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+take",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  {
    name: "vectorSearch+hybridSearch",
    build: (q) => {
      q.vectorSearch = vectorEmbeddingLimit1();
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Reject,
  },
  // ============ search rejects every peer except take ============
  {
    name: "search+index",
    build: (q) => {
      q.search = searchBodyX();
      q.index = "by_title";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+eq",
    build: (q) => {
      q.search = searchBodyX();
      q.eq = ["x"];
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+gt",
    build: (q) => {
      q.search = searchBodyX();
      q.gt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+gte",
    build: (q) => {
      q.search = searchBodyX();
      q.gte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+lt",
    build: (q) => {
      q.search = searchBodyX();
      q.lt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+lte",
    build: (q) => {
      q.search = searchBodyX();
      q.lte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+order",
    build: (q) => {
      q.search = searchBodyX();
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+unique",
    build: (q) => {
      q.search = searchBodyX();
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+first",
    build: (q) => {
      q.search = searchBodyX();
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+count",
    build: (q) => {
      q.search = searchBodyX();
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+paginate",
    build: (q) => {
      q.search = searchBodyX();
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+filter",
    build: (q) => {
      q.search = searchBodyX();
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+vectorSearch",
    build: (q) => {
      q.search = searchBodyX();
      q.vectorSearch = vectorEmbeddingLimit1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "search+hybridSearch",
    build: (q) => {
      q.search = searchBodyX();
      q.hybridSearch = hybridQueryDatabaseX();
    },
    expected: Outcome.Reject,
  },
  // ============ hybridSearch rejects every peer (standalone, like vectorSearch) ============
  {
    name: "hybridSearch+index",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.index = "by_title";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+eq",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.eq = ["x"];
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+gt",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.gt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+gte",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.gte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+lt",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.lt = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+lte",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.lte = "x";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+order",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.order = "asc";
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+unique",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.unique = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+first",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.first = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+count",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.count = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+distinct",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.distinct = true;
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+aggregate",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.aggregate = { op: "min" };
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+paginate",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.paginate = paginateNum1();
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+filter",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Reject,
  },
  {
    name: "hybridSearch+take",
    build: (q) => {
      q.hybridSearch = hybridQueryDatabaseX();
      q.take = 1;
    },
    expected: Outcome.Reject,
  },
  // ============ composition accepts (smoke that valid combos don't false-reject) ============
  {
    name: "compose: search+take",
    build: (q) => {
      q.search = searchBodyX();
      q.take = 1;
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: index+take",
    build: (q) => {
      q.index = "by_title";
      q.take = 1;
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: index+eq+take",
    build: (q) => {
      q.index = "by_title";
      q.eq = ["x"];
      q.take = 1;
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: index+order",
    build: (q) => {
      q.index = "by_title";
      q.order = "asc";
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: index+gt+lt",
    build: (q) => {
      q.index = "by_title";
      q.gt = "a";
      q.lt = "z";
    },
    expected: Outcome.Accept,
  },
  {
    name: "compose: take+filter",
    build: (q) => {
      q.take = 1;
      q.filter = filterEqTitleX();
    },
    expected: Outcome.Accept,
  },
];

describe("InMemoryRtDbClient — combination matrix (mirror of server query_combinations.rs)", () => {
  for (const testCase of CASES) {
    it(`${testCase.expected === Outcome.Accept ? "accepts" : "rejects"}: ${testCase.name}`, async () => {
      const c = newClient();
      const q = baseQuery();
      testCase.build(q);
      let actual: Outcome;
      try {
        await c.query({ json: q } as RtQuery<unknown>);
        actual = Outcome.Accept;
      } catch (e) {
        if (e instanceof RtDbError && e.code === "BAD_REQUEST") {
          actual = Outcome.Reject;
        } else {
          throw e;
        }
      }
      expect(actual).toBe(testCase.expected);
    });
  }

  it("matrix covers the documented drift cases (QA-001 regression guard)", () => {
    // These three cases are the QA-001 drift surface — the TS `get` guard used
    // to omit them and silently accept `get+filter`/`get+search`/`get+vectorSearch`.
    // If any are removed or reclassified, fail loudly: they are the load-bearing
    // regression cases for the QA-001 fix.
    const names = CASES.map((c) => c.name);
    expect(names).toContain("get+filter");
    expect(names).toContain("get+search");
    expect(names).toContain("get+vectorSearch");
    for (const name of ["get+filter", "get+search", "get+vectorSearch"]) {
      const c = CASES.find((x) => x.name === name);
      expect(c?.expected).toBe(Outcome.Reject);
    }
  });
});
