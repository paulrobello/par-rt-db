/**
 * QA-001: Golden-vector parity test (ts-client view).
 *
 * Loads `wire-corpus/golden-vector.json` (repo root — the single source of
 * truth) and runs each query case through the ts-client in-memory engine,
 * comparing canonicalized projected results. The same fixture is consumed by
 * the rust-client, python-client, and server (against Postgres) tests; a
 * divergence in any one implementation surfaces there.
 *
 * The fixture encodes the dataset, the per-case wire-shape `QueryJson`, and
 * the expected canonical result. System fields (`_id`, `_creationTime`,
 * `_owner`, `_updatedAt`) are projected out before comparison so the client's
 * id-minting order doesn't cause spurious divergence — the audit point is to
 * catch **sort-comparator / boundary / terminal-cascade** divergence, not
 * id-minting drift.
 */

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

import { InMemoryRtDbClient } from "../src/in_memory.js";
import { mutation } from "../src/mutation.js";
import type { QueryJson } from "../src/protocol.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

interface GoldenCase {
  id: string;
  terminal: string;
  query: QueryJson;
  expected?:
    | Array<{ name: string; status: string; order: number }>
    | { name: string; status: string; order: number };
  expected_scalar?: number;
  expected_value?: number | null;
  expected_groups?: Array<{ key: unknown; value: number | null }>;
  expected_distinct?: Array<number | string>;
  expected_unordered?: boolean;
  expected_has_next_cursor?: boolean;
}

interface GoldenFixture {
  schema_table: string;
  schema_fields: Record<string, string>;
  schema_indexes: Array<{
    name: string;
    fields: string[];
    search?: boolean;
    vector?: { dimensions: number };
  }>;
  seed: Array<Record<string, unknown>>;
  cases: GoldenCase[];
}

const FIXTURE_PATH = resolve(__dirname, "../../wire-corpus/golden-vector.json");

function loadFixture(): GoldenFixture {
  return JSON.parse(readFileSync(FIXTURE_PATH, "utf8")) as GoldenFixture;
}

function buildSchema(fx: GoldenFixture) {
  // The fixture's schema_fields shape mirrors the existing `items` fixture in
  // in_memory.test.ts. Translate it to the defineSchema call.
  const fields: Parameters<typeof defineTable>[0] = {};
  for (const [name, ty] of Object.entries(fx.schema_fields)) {
    if (ty === "string") fields[name] = t.string();
    else if (ty === "number") fields[name] = t.number();
    else if (ty === "optional(string)") fields[name] = t.optional(t.string());
    else if (ty === "array(string)") fields[name] = t.array(t.string());
    else if (ty.startsWith("vector(") && ty.endsWith(")")) {
      const dims = Number.parseInt(ty.slice("vector(".length, -1), 10);
      fields[name] = t.vector(dims);
    } else throw new Error(`fixture field type not implemented: ${ty}`);
  }
  let builder = defineTable(fields);
  for (const ix of fx.schema_indexes) {
    if (ix.search) {
      builder = builder.searchIndex(ix.name, ix.fields as [string, ...string[]]);
    } else if (ix.vector) {
      builder = builder.vectorIndex(ix.name, ix.fields[0] as string, ix.vector.dimensions, []);
    } else {
      builder = builder.index(ix.name, ix.fields as [string, ...string[]]);
    }
  }
  return defineSchema({ [fx.schema_table]: builder });
}

function seedClient(fx: GoldenFixture): InMemoryRtDbClient {
  let ms = 1_700_000_000_000;
  const client = new InMemoryRtDbClient({ now: () => ms++, random: () => 0 });
  client.pushSchema(buildSchema(fx));
  for (const doc of fx.seed) {
    client.mutate(mutation().insert(fx.schema_table, doc).build());
  }
  return client;
}

type Projected = { name: string; status: string; order: number };

function project(doc: Record<string, unknown>): Projected {
  return {
    name: doc.name as string,
    status: doc.status as string,
    order: doc.order as number,
  };
}

function projectList(docs: unknown[]): Projected[] {
  return (docs as Array<Record<string, unknown>>).map(project);
}

/** Numeric-tolerant equality so the SQL-numeric server result and the JS number
 * aggregate result agree (e.g. `6` == `6.0`). Recurses into arrays/objects. */
function jsonEqNumeric(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") {
    return a === b || Math.abs(a - b) < 1e-9;
  }
  if (a === null && b === null) return true;
  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x, i) => jsonEqNumeric(x, b[i]));
  }
  if (a !== null && b !== null && typeof a === "object" && typeof b === "object") {
    const ao = a as Record<string, unknown>;
    const bo = b as Record<string, unknown>;
    const ak = Object.keys(ao);
    const bk = Object.keys(bo);
    return ak.length === bk.length && ak.every((k) => k in bo && jsonEqNumeric(ao[k], bo[k]));
  }
  return a === b;
}

describe("QA-001 golden-vector parity", () => {
  const fx = loadFixture();
  const client = seedClient(fx);

  for (const case_ of fx.cases) {
    it(`${case_.id} (${case_.terminal})`, async () => {
      // Construct an RtQuery shell directly from the wire-shape query JSON.
      const result = await client.query({ json: case_.query });

      if (case_.expected_scalar !== undefined) {
        expect(result).toBe(case_.expected_scalar);
        return;
      }

      if ("expected_value" in case_) {
        // aggregate scalar: a bare number, or null for an empty match set.
        // `in` (not !== undefined) so a present null is distinct from absent.
        const wantVal = case_.expected_value ?? null;
        expect(jsonEqNumeric(result, wantVal)).toBe(true);
        return;
      }

      const wantGroups = case_.expected_groups;
      if (wantGroups !== undefined) {
        expect(Array.isArray(result)).toBe(true);
        const got = result as Array<{ key: unknown; value: number | null }>;
        expect(got.length).toBe(wantGroups.length);
        for (let i = 0; i < got.length; i++) {
          expect(got[i].key).toEqual(wantGroups[i].key);
          expect(jsonEqNumeric(got[i].value, wantGroups[i].value)).toBe(true);
        }
        return;
      }

      const wantDistinct = case_.expected_distinct;
      if (wantDistinct !== undefined) {
        expect(Array.isArray(result)).toBe(true);
        const got = result as unknown[];
        expect(got.length).toBe(wantDistinct.length);
        for (let i = 0; i < got.length; i++) {
          expect(jsonEqNumeric(got[i], wantDistinct[i])).toBe(true);
        }
        return;
      }

      if (case_.expected_unordered) {
        const got = (result as unknown[]).map((d) => project(d as Record<string, unknown>));
        got.sort((a, b) => a.name.localeCompare(b.name));
        const want = [...(case_.expected as Projected[])].sort((a, b) =>
          a.name.localeCompare(b.name),
        );
        expect(got).toEqual(want);
        return;
      }

      if (case_.expected_has_next_cursor) {
        const page = result as { docs: unknown[]; nextCursor?: string };
        expect(projectList(page.docs)).toEqual(case_.expected);
        expect(page.nextCursor).toBeDefined();
        return;
      }

      if (Array.isArray(case_.expected)) {
        const got = projectList(result as unknown[]);
        expect(got).toEqual(case_.expected);
        return;
      }

      // single-doc terminal (get / first / unique)
      expect(project(result as Record<string, unknown>)).toEqual(case_.expected);
    });
  }
});
