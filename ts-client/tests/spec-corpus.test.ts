import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { RtDbError } from "../src/errors.js";
import { indexColumnType, type PgType } from "../src/in_memory/validate.js";
import type { FieldTypeJson } from "../src/protocol.js";

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
