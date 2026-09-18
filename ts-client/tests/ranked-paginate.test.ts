/**
 * ENH-030: `paginate` composed with the ranked terminals
 * (`search`/`vectorSearch`/`hybridSearch`).
 *
 * Two surfaces are covered. The BUILDER half pins the wire JSON — `paginate`
 * is a peer clause, so the terminal key stays alongside it rather than being
 * replaced. The ENGINE half pins the in-memory execution semantics: pages that
 * do not overlap, keep the ranked order, and concatenate back to exactly the
 * unpaginated result, with `nextCursor` present only when a further page
 * exists.
 *
 * The engine models `search`'s relevance ranking (a query-lexeme-frequency
 * stand-in) but neither pgvector distance nor RRF fusion, so the
 * `vectorSearch`/`hybridSearch` cases assert the behaviour this harness
 * actually implements — see each block's comment — not a distance ranking it
 * does not have.
 */
import { beforeEach, describe, expect, it } from "vitest";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { decodeCursor } from "../src/pagination.js";
import type { PaginatedResultJson } from "../src/protocol.js";
import { createApi } from "../src/query.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

const schema = defineSchema({
  notes: defineTable({
    name: t.string(),
    body: t.string(),
    embedding: t.vector(3),
  })
    .searchIndex("search_body", ["body"])
    .vectorIndex("by_embedding", "embedding", 3),
});

const api = createApi(schema);

/** Deterministic clock/RNG so `_creationTime` and minted ids are stable and the
 *  `created_at`/`id` tie-breakers are reproducible across runs. */
function newClient(): InMemoryRtDbClient {
  let ms = 1_700_000_000_000;
  const c = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
  c.pushSchema(schema);
  return c;
}

interface NoteDoc {
  name: string;
  body: string;
}

async function seedNotes(
  c: InMemoryRtDbClient,
  rows: Array<{ name: string; body: string; embedding?: number[] }>,
): Promise<void> {
  for (const row of rows) {
    await c.mutate({
      steps: [
        {
          op: "insert",
          table: "notes",
          doc: { embedding: [0, 0, 0], ...row },
        },
      ],
    });
  }
}

/** The corpus's ranking fixture: one term repeated a different number of times
 *  per doc, so relevance is a strict total order and the page split never
 *  depends on a tie-break. Seeded out of rank order on purpose. */
const RANKED_SEED = [
  { name: "b", body: "task task" },
  { name: "a", body: "task" },
  { name: "c", body: "task task task" },
];

function names(page: PaginatedResultJson): string[] {
  return (page.docs as NoteDoc[]).map((d) => d.name);
}

describe("TableQuery: paginate composes with the ranked terminals (ENH-030 builder)", () => {
  it("search + paginate keeps both clauses on the wire", () => {
    const q = api.notes.query().search("search_body", "task").paginate(undefined, 2);
    expect(q.json).toEqual({
      table: "notes",
      search: { index: "search_body", query: "task" },
      paginate: { numItems: 2 },
    });
  });

  it("search + paginate carries a resume cursor when one is supplied", () => {
    const q = api.notes.query().search("search_body", "task").paginate("Y3Vyc29y", 2);
    expect(q.json.paginate).toEqual({ cursor: "Y3Vyc29y", numItems: 2 });
    expect(q.json.search).toEqual({ index: "search_body", query: "task" });
  });

  it("vectorSearch + paginate keeps `limit` (the candidate pool) beside `numItems`", () => {
    const q = api.notes
      .query()
      .vectorSearch("by_embedding", [1, 0, 0], { limit: 10 })
      .paginate(undefined, 3);
    expect(q.json).toEqual({
      table: "notes",
      vectorSearch: { index: "by_embedding", vector: [1, 0, 0], limit: 10 },
      paginate: { numItems: 3 },
    });
  });

  it("hybridSearch + paginate keeps `limit` (the fused pool) beside `numItems`", () => {
    const q = api.notes.query().hybridSearch("task", [1, 0, 0], 10).paginate(undefined, 3);
    expect(q.json).toEqual({
      table: "notes",
      hybridSearch: { query: "task", vector: [1, 0, 0], limit: 10 },
      paginate: { numItems: 3 },
    });
  });
});

describe("InMemory: search + paginate (ENH-030)", () => {
  let c: InMemoryRtDbClient;

  beforeEach(async () => {
    c = newClient();
    await seedNotes(c, RANKED_SEED);
  });

  it("pages the ranked order and concatenates back to the unpaginated result", async () => {
    const full = (await c.query(
      api.notes.query().search("search_body", "task").collect(),
    )) as NoteDoc[];
    expect(full.map((d) => d.name)).toEqual(["c", "b", "a"]);

    const page1 = (await c.query(
      api.notes.query().search("search_body", "task").paginate(undefined, 2),
    )) as PaginatedResultJson;
    expect(names(page1)).toEqual(["c", "b"]);
    expect(page1.nextCursor).toBeDefined();

    const page2 = (await c.query(
      api.notes.query().search("search_body", "task").paginate(page1.nextCursor, 2),
    )) as PaginatedResultJson;
    expect(names(page2)).toEqual(["a"]);
    // A short page is the last page: no cursor to follow.
    expect(page2.nextCursor).toBeUndefined();

    // Non-overlapping, and the concatenation is exactly the ranked result.
    expect([...names(page1), ...names(page2)]).toEqual(full.map((d) => d.name));
  });

  it("walks every page one row at a time without skipping or duplicating", async () => {
    const seen: string[] = [];
    let cursor: string | undefined;
    for (let i = 0; i < 10; i++) {
      const page = (await c.query(
        api.notes.query().search("search_body", "task").paginate(cursor, 1),
      )) as PaginatedResultJson;
      seen.push(...names(page));
      if (page.nextCursor === undefined) break;
      cursor = page.nextCursor;
    }
    expect(seen).toEqual(["c", "b", "a"]);
    expect(new Set(seen).size).toBe(seen.length);
  });

  it("a page that exactly exhausts the result carries no cursor", async () => {
    const page = (await c.query(
      api.notes.query().search("search_body", "task").paginate(undefined, 3),
    )) as PaginatedResultJson;
    expect(names(page)).toEqual(["c", "b", "a"]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("an empty result is an empty page with no cursor", async () => {
    const page = (await c.query(
      api.notes.query().search("search_body", "nomatch").paginate(undefined, 2),
    )) as PaginatedResultJson;
    expect(page.docs).toEqual([]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("mints a three-column cursor: the ranking key, then created_at and id", async () => {
    const page = (await c.query(
      api.notes.query().search("search_body", "task").paginate(undefined, 1),
    )) as PaginatedResultJson;
    expect(names(page)).toEqual(["c"]);
    // biome-ignore lint/style/noNonNullAssertion: the preceding page is not the last, so a cursor exists
    const values = decodeCursor(page.nextCursor!);
    expect(values).toHaveLength(3);
    // `c` repeats the query lexeme three times — the engine's relevance stand-in.
    expect(values[0]).toBe(3);
    expect(typeof values[1]).toBe("number");
    expect(typeof values[2]).toBe("string");
  });

  it("rejects a cursor whose column count does not match the ranked sort", async () => {
    await expect(
      c.query(api.notes.query().search("search_body", "task").paginate(btoa("[1,2]"), 2)),
    ).rejects.toThrow(/cursor has 2 value\(s\) but this query sorts over 3 column\(s\)/);
  });

  it("rejects a cursor whose ranking key is not a number", async () => {
    await expect(
      c.query(api.notes.query().search("search_body", "task").paginate(btoa('["x",1,"y"]'), 2)),
    ).rejects.toThrow(/cursor value for the ranking key must be a number/);
  });

  it("carries `_searchSnippet` onto a paginated page when snippet is requested", async () => {
    const page = (await c.query(
      api.notes.query().search("search_body", "task", { snippet: true }).paginate(undefined, 1),
    )) as PaginatedResultJson;
    const doc = page.docs[0] as { _searchSnippet?: string };
    expect(doc._searchSnippet).toContain("<mark>task</mark>");
  });
});

describe("InMemory: vectorSearch + paginate (ENH-030)", () => {
  // The harness models no pgvector distance, so `limit` bounds the
  // filter-narrowed candidate POOL in insertion order and the page order is the
  // `created_at`/`id` DESC tie-breaker the server applies BELOW the distance —
  // i.e. the oldest `limit` rows, served newest-first. That is the behaviour
  // asserted here, not a nearest-neighbour ranking.
  let c: InMemoryRtDbClient;

  beforeEach(async () => {
    c = newClient();
    await seedNotes(c, [
      { name: "n0", body: "x" },
      { name: "n1", body: "x" },
      { name: "n2", body: "x" },
      { name: "n3", body: "x" },
    ]);
  });

  it("`limit` bounds the pool and `numItems` slices it into pages", async () => {
    const page1 = (await c.query(
      api.notes
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], { limit: 3 })
        .paginate(undefined, 2),
    )) as PaginatedResultJson;
    // Pool = the first 3 inserted (n0,n1,n2); page order is createdAt DESC.
    expect(names(page1)).toEqual(["n2", "n1"]);
    expect(page1.nextCursor).toBeDefined();

    const page2 = (await c.query(
      api.notes
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], { limit: 3 })
        .paginate(page1.nextCursor, 2),
    )) as PaginatedResultJson;
    // n3 is outside the pool, so the walk ends at the pool's last row.
    expect(names(page2)).toEqual(["n0"]);
    expect(page2.nextCursor).toBeUndefined();
  });

  it("pages carry the system fields (server parity: merged docs)", async () => {
    const page = (await c.query(
      api.notes
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], { limit: 2 })
        .paginate(undefined, 1),
    )) as PaginatedResultJson;
    const doc = page.docs[0] as { _id?: string; _creationTime?: number };
    expect(typeof doc._id).toBe("string");
    expect(typeof doc._creationTime).toBe("number");
  });

  it("mints a two-column cursor (no distance column is modelled)", async () => {
    const page = (await c.query(
      api.notes
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], { limit: 4 })
        .paginate(undefined, 1),
    )) as PaginatedResultJson;
    // biome-ignore lint/style/noNonNullAssertion: 4 candidates and a page of 1 guarantees a next page
    const values = decodeCursor(page.nextCursor!);
    expect(values).toHaveLength(2);
    expect(typeof values[0]).toBe("number");
    expect(typeof values[1]).toBe("string");
  });

  it("narrows the pool by the vector-search-level filter before paging", async () => {
    const page = (await c.query(
      api.notes
        .query()
        .vectorSearch("by_embedding", [1, 0, 0], {
          limit: 10,
          filter: { op: "eq", field: "name", value: "n1" },
        })
        .paginate(undefined, 5),
    )) as PaginatedResultJson;
    expect(names(page)).toEqual(["n1"]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("rejects a cursor whose column count does not match", async () => {
    await expect(
      c.query(
        api.notes
          .query()
          .vectorSearch("by_embedding", [1, 0, 0], { limit: 4 })
          .paginate(btoa("[1,2,3]"), 2),
      ),
    ).rejects.toThrow(/cursor has 3 value\(s\) but this query sorts over 2 column\(s\)/);
  });
});

describe("InMemory: hybridSearch + paginate (ENH-030)", () => {
  // The harness fuses no ts_rank with any vector distance, so every hybrid
  // result is empty — paginated or not. An empty page is always the last page.
  it("returns an empty page with no cursor", async () => {
    const c = newClient();
    await seedNotes(c, RANKED_SEED);
    const page = (await c.query(
      api.notes.query().hybridSearch("task", [1, 0, 0], 10).paginate(undefined, 2),
    )) as PaginatedResultJson;
    expect(page.docs).toEqual([]);
    expect(page.nextCursor).toBeUndefined();
  });

  it("still returns the bare empty array without paginate", async () => {
    const c = newClient();
    await seedNotes(c, RANKED_SEED);
    const docs = await c.query(api.notes.query().hybridSearch("task", [1, 0, 0], 10).collect());
    expect(docs).toEqual([]);
  });
});
