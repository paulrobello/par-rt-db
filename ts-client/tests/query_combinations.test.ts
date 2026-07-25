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
// every terminal in the matrix.
const schema = defineSchema({
  items: defineTable({
    title: t.string(),
    body: t.string(),
    embedding: t.vector(3),
  })
    .index("by_title", ["title"])
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
function paginateNum1() {
  return { numItems: 1 };
}

const enum Outcome {
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
  // ============ count rejects unique, take, first, order ============
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
