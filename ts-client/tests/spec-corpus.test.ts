import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { decodeCursor, encodeCursor } from "../src/pagination.js";
import { InMemoryRtDbClient } from "../src/in_memory/index.js";
import { stampComputed } from "../src/in_memory/doc_stamp.js";
import { indexColumnType, type PgType } from "../src/in_memory/validate.js";
import type {
  FieldTypeJson,
  IndexJson,
  SchemaJson,
  TableJson,
  ValueExprJson,
} from "../src/protocol.js";

/**
 * ENH-052 family 1 — spec-table corpus runner: asserts this engine's
 * `indexColumnType` against every row of wire-corpus/spec/index-column-types.json,
 * and that this implementation's FieldType variant set is covered by the
 * fixture. `columnType` carries the server's raw Postgres string; the
 * four-value `PgType` storage enum maps back to it.
 */

const CORPUS = JSON.parse(
  readFileSync(resolve(__dirname, "../../wire-corpus/spec/index-column-types.json"), "utf8"),
) as {
  rows: {
    fieldType: FieldTypeJson;
    columnType?: string;
    nullable?: boolean;
    error?: boolean;
  }[];
};

/** This engine's storage enum mapped back to the server's raw column strings. */
const RAW_PG: Record<PgType, string> = {
  text: "text",
  number: "double precision",
  boolean: "boolean",
  int64: "bigint",
};

/**
 * One representative per FieldType variant this engine's indexColumnType
 * distinguishes, in the wire spelling (the corpus README's own-variant
 * coverage rule: a variant without a fixture row fails).
 */
const OWN_VARIANTS: FieldTypeJson[] = [
  { type: "string" },
  { type: "number" },
  { type: "boolean" },
  { type: "int64" },
  { type: "id", table: "t" },
  { type: "id", table: "t", onDelete: "cascade" },
  { type: "literal", value: "x" },
  { type: "literal", value: 42 },
  { type: "optional", inner: { type: "string" } },
  {
    type: "union",
    variants: [
      { type: "literal", value: "a" },
      { type: "literal", value: "b" },
    ],
  },
  { type: "union", variants: [] },
  { type: "union", variants: [{ type: "literal", value: "a" }, { type: "number" }] },
  { type: "null" },
  { type: "array", element: { type: "string" } },
  { type: "object", fields: { a: { type: "string" } } },
  { type: "bytes" },
  { type: "any" },
  { type: "record", value: { type: "string" } },
  { type: "vector", dimensions: 3 },
];

/** Canonical JSON (object keys sorted recursively) for row lookup. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonical).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    return `{${Object.keys(obj)
      .sort()
      .map((k) => `${JSON.stringify(k)}:${canonical(obj[k])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

describe("spec corpus: index column types", () => {
  it("every FieldType variant this engine carries has a fixture row", () => {
    const rowKeys = new Set(CORPUS.rows.map((row) => canonical(row.fieldType)));
    for (const ty of OWN_VARIANTS) {
      expect(rowKeys.has(canonical(ty)), `no fixture row for ${canonical(ty)}`).toBe(true);
    }
  });

  it("maps every fixture row identically to the server contract", () => {
    for (const row of CORPUS.rows) {
      if (row.error) {
        expect(() => indexColumnType(row.fieldType)).toThrow(RtDbError);
        continue;
      }
      const resolved = indexColumnType(row.fieldType);
      expect(RAW_PG[resolved.pg]).toBe(row.columnType);
      expect(resolved.nullable).toBe(row.nullable);
    }
  });
});

describe("spec corpus: computed stamping", () => {
  const CORPUS: {
    rows: {
      id: string;
      fields: Record<string, unknown>;
      computed: Record<string, ValueExprJson>;
      inputDoc: Record<string, unknown>;
      now: number;
      expect: { stampedValues?: Record<string, unknown>; error?: boolean };
    }[];
  } = JSON.parse(
    readFileSync(resolve(__dirname, "../../wire-corpus/spec/computed-stamping.json"), "utf8"),
  );

  /** One representative per ValueExpr op this engine's evalValueExpr
   *  distinguishes, in the wire spelling (the corpus README's own-op coverage
   *  rule: a grammar node without a fixture row fails). */
  const OWN_OPS: ValueExprJson[] = [
    { op: "field", field: "f" },
    { op: "literal", value: "x" },
    { op: "concat", parts: [{ op: "literal", value: "x" }] },
    { op: "add", left: { op: "literal", value: 1 }, right: { op: "literal", value: 1 } },
    { op: "sub", left: { op: "literal", value: 1 }, right: { op: "literal", value: 1 } },
    { op: "mul", left: { op: "literal", value: 1 }, right: { op: "literal", value: 1 } },
    { op: "div", left: { op: "literal", value: 1 }, right: { op: "literal", value: 1 } },
    { op: "coalesce", parts: [{ op: "literal", value: "x" }] },
    { op: "lower", value: { op: "literal", value: "x" } },
    { op: "upper", value: { op: "literal", value: "x" } },
    { op: "trim", value: { op: "literal", value: "x" } },
    { op: "cast", value: { op: "literal", value: "x" }, to: "toString" },
    { op: "now" },
    {
      op: "case",
      whens: [
        {
          when: { field: "f", op: "eq", value: "v" },
          // biome-ignore lint/suspicious/noThenProperty: wire CaseWhenJson field is named `then`
          then: { op: "literal", value: "x" },
        },
      ],
      otherwise: { op: "literal", value: "y" },
    },
  ];

  /** Per-key assertion: an expected JSON null means the stamped doc must NOT
   *  carry the key; anything else deep-equals (vitest's toEqual compares
   *  numbers by value). */
  function assertStamped(
    row: (typeof CORPUS.rows)[number],
    stamped: Record<string, unknown>,
  ): void {
    const exp = row.expect;
    if (exp.error) {
      throw new Error(`row ${row.id}: expected the stamp to fail`);
    }
    for (const [name, expected] of Object.entries(exp.stampedValues ?? {})) {
      if (expected === null) {
        expect(name in stamped, `row ${row.id}: key '${name}' must be absent after stamping`).toBe(
          false,
        );
      } else {
        expect(stamped[name], `row ${row.id}: stamped value of '${name}'`).toEqual(expected);
      }
    }
  }

  it("every ValueExpr op this engine carries has a fixture row", () => {
    const seen = new Set<string>();
    for (const row of CORPUS.rows) {
      for (const expr of Object.values(row.computed)) {
        seen.add(expr.op);
      }
    }
    for (const own of OWN_OPS) {
      expect(seen.has(own.op), `no fixture row exercises op '${own.op}'`).toBe(true);
    }
  });

  it("stamps every fixture row identically to the server contract", () => {
    for (const row of CORPUS.rows) {
      const table = { fields: row.fields, computed: row.computed } as unknown as TableJson;
      let stamped: Record<string, unknown>;
      try {
        stamped = stampComputed(table, { ...row.inputDoc }, row.now);
      } catch (err) {
        if (row.expect.error) {
          expect(
            err instanceof RtDbError && err.code === "BAD_REQUEST",
            `row ${row.id}: expected BAD_REQUEST, got ${String(err)}`,
          ).toBe(true);
          continue;
        }
        throw err;
      }
      if (row.expect.error) {
        throw new Error(
          `row ${row.id}: expected the stamp to fail, got ${JSON.stringify(stamped)}`,
        );
      }
      assertStamped(row, stamped);
    }
  });
});

describe("spec corpus: aggregate terminals", () => {
  const CORPUS: {
    rows: {
      id: string;
      fields: Record<string, FieldTypeJson>;
      index: IndexJson;
      docs: Record<string, unknown>[];
      eq: unknown[];
      aggregate: Record<string, unknown>;
      expect: unknown;
    }[];
  } = JSON.parse(
    readFileSync(resolve(__dirname, "../../wire-corpus/spec/aggregate-terminals.json"), "utf8"),
  );

  const OWN_OPS = ["sum", "avg", "min", "max", "count"];

  /** The corpus README's own-op coverage rule: every AggregateOp this engine
   *  distinguishes must be exercised by a fixture row. */
  it("every aggregate op this engine distinguishes has a fixture row", () => {
    const seen = new Set<string>();
    for (const row of CORPUS.rows) {
      const agg = row.aggregate as { op?: string; aggregates?: Record<string, string> };
      if (agg.op) seen.add(agg.op);
      for (const op of Object.values(agg.aggregates ?? {})) seen.add(op);
    }
    for (const op of OWN_OPS) {
      expect(seen.has(op), `no fixture row exercises aggregate op '${op}'`).toBe(true);
    }
  });

  /** Per-surface execution: push the row's schema fragment, insert the docs
   *  through the real write path, run the aggregate query through the real
   *  query terminal, and compare the bare wire value. */
  it("evaluates every fixture row identically to the server contract", async () => {
    let ms = 1_700_000_000_000;
    for (const row of CORPUS.rows) {
      const client = new InMemoryRtDbClient({ now: () => ++ms, random: () => 0 });
      const schema: SchemaJson = {
        tables: { items: { fields: row.fields, indexes: [row.index] } },
      };
      client.pushSchema(schema);
      await client.mutate({
        steps: [{ op: "insertMany", table: "items", docs: row.docs }],
      });
      const q = {
        table: "items",
        index: row.index.name,
        eq: row.eq,
        aggregate: row.aggregate,
      };
      const expectErr =
        row.expect !== null &&
        typeof row.expect === "object" &&
        !Array.isArray(row.expect) &&
        (row.expect as { error?: boolean }).error === true;
      if (expectErr) {
        await expect(client.query({ json: q }), row.id).rejects.toThrow(RtDbError);
        continue;
      }
      const result = await client.query({ json: q });
      expect(result, row.id).toEqual(row.expect);
    }
  });
});

describe("spec corpus: pagination", () => {
  const CORPUS: {
    rows: {
      id: string;
      fields: Record<string, FieldTypeJson>;
      index: IndexJson;
      docs: Record<string, unknown>[];
      eq: unknown[];
      order: "asc" | "desc";
      paginate: { numItems: number; cursor?: unknown[] };
      expect: {
        docs?: Record<string, unknown>[];
        nextCursorPrefix?: unknown[];
        error?: { code: string };
      };
    }[];
  } = JSON.parse(
    readFileSync(resolve(__dirname, "../../wire-corpus/spec/pagination.json"), "utf8"),
  );

  const OWN_ORDERS = ["asc", "desc"];
  const OWN_KINDS = ["number", "int64", "boolean", "optional"];

  /** The corpus README's own-coverage rule: every Order spelling and keyset
   *  value kind this engine distinguishes must be exercised by a fixture
   *  row. */
  it("every order spelling and value kind this engine distinguishes has a fixture row", () => {
    const seenOrders = new Set<string>();
    const seenKinds = new Set<string>();
    for (const row of CORPUS.rows) {
      seenOrders.add(row.order);
      const ty = row.fields.v?.type ?? "string";
      seenKinds.add(ty === "optional" ? "optional" : ty);
    }
    for (const order of OWN_ORDERS) {
      expect(seenOrders.has(order), `no fixture row exercises order '${order}'`).toBe(true);
    }
    for (const kind of OWN_KINDS) {
      expect(seenKinds.has(kind), `no fixture row exercises value kind '${kind}'`).toBe(true);
    }
  });

  /** Per-row execution: push the row's schema fragment, insert the docs
   *  through the real write path, encode the RAW cursor values through this
   *  engine's codec, run the paginate query through the real terminal, and
   *  assert the page envelope — `nextCursorPrefix` is the unbound-index
   *  prefix of the decoded minted cursor. */
  it("pages every fixture row identically to the server contract", async () => {
    let ms = 1_700_000_000_000;
    for (const row of CORPUS.rows) {
      const client = new InMemoryRtDbClient({ now: () => ++ms, random: () => 0 });
      const schema: SchemaJson = {
        tables: { items: { fields: row.fields, indexes: [row.index] } },
      };
      client.pushSchema(schema);
      await client
        .mutate({
          steps: [{ op: "insertMany", table: "items", docs: row.docs }],
        })
        .catch((e: unknown) => {
          throw new Error(`row ${row.id}: insert failed: ${String(e)}`);
        });
      const cursor =
        row.paginate.cursor !== undefined ? encodeCursor(row.paginate.cursor) : undefined;
      const q = {
        table: "items",
        index: row.index.name,
        eq: row.eq,
        order: row.order,
        paginate: { numItems: row.paginate.numItems, ...(cursor !== undefined ? { cursor } : {}) },
      };
      const expectedCode = row.expect.error?.code;
      if (expectedCode !== undefined) {
        const err = await client.query({ json: q }).then(
          () => null,
          (e: unknown) => e,
        );
        expect(err, row.id).toBeInstanceOf(RtDbError);
        expect((err as RtDbError).code, row.id).toBe(expectedCode);
        continue;
      }
      const result = (await client.query({ json: q })) as {
        docs: Record<string, unknown>[];
        nextCursor?: string;
      };
      // The wire page docs carry the system fields (`_id`, `_creationTime`,
      // `_version`) — minted per engine, so asserted under the semantics
      // corpus's normalize projection (README "Semantics corpus format").
      const SYSTEM_KEYS = ["_id", "_creationTime", "_version"];
      const projected = result.docs.map((doc) => {
        const clone = { ...doc };
        for (const key of SYSTEM_KEYS) {
          delete clone[key];
        }
        return clone;
      });
      expect(projected, row.id).toEqual(row.expect.docs);
      const prefix = row.expect.nextCursorPrefix;
      if (prefix === undefined) {
        expect(result.nextCursor, `${row.id}: no nextCursor expected`).toBeUndefined();
        continue;
      }
      expect(result.nextCursor, `${row.id}: nextCursor must be present`).toBeDefined();
      const decoded = decodeCursor(result.nextCursor as string);
      expect(decoded.length, `${row.id}: minted cursor arity`).toBe(prefix.length + 2);
      expect(decoded.slice(0, prefix.length), `${row.id}: minted cursor prefix`).toEqual(prefix);
      expect(typeof decoded[decoded.length - 2], `${row.id}: created_at cursor kind`).toBe(
        "number",
      );
      expect(typeof decoded[decoded.length - 1], `${row.id}: id cursor kind`).toBe("string");
    }
  });
});
