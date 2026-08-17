/**
 * Value/filter validation for the in-memory harness — the leaf module of the
 * `in_memory/` decomposition (mirrors `rust-client/src/in_memory/validate.rs`).
 *
 * Two concerns live here:
 * - structural validation + evaluation of the query DSL's `FilterExpr`
 *   (mirroring server `query::compile_filter_node` / `jsonb_lhs_and_bind`);
 * - the pure JSON value predicates (`isHexId`, `isInt64String`, `clone`, …)
 *   shared by the store (`validateValue`), the query engine
 *   (`coerceIndexValue`), and the migration engine (`coerceValue`).
 */

import { RtDbError } from "../errors.js";
import type { FieldTypeJson, FilterExpr } from "../protocol.js";

/** Deep clone of a JSON doc (docs are pure JSON — safe to round-trip). */
export function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

export function isHexId(value: unknown): value is string {
  return typeof value === "string" && value.length === 32 && /^[0-9a-f]+$/.test(value);
}

export function isInt64String(value: unknown): boolean {
  if (typeof value !== "string" || !/^-?\d+$/.test(value)) {
    return false;
  }
  try {
    const n = BigInt(value);
    return n >= -(2n ** 63n) && n <= 2n ** 63n - 1n;
  } catch {
    return false;
  }
}

export function isBase64String(value: unknown): boolean {
  return (
    typeof value === "string" && /^[A-Za-z0-9+/]*={0,2}$/.test(value) && value.length % 4 === 0
  );
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

type FilterLeafOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

/**
 * Structural validation of a `FilterExpr` against a table's declared fields,
 * mirroring server `query::compile_filter_node` / `field_lhs_and_bind`
 * (`query.rs`). Throws `BAD_REQUEST` for an unknown field, an empty `and`/`or`,
 * an empty `in`, or a non-string/number/boolean leaf value. Call once before
 * evaluating per row.
 */
export function validateFilter(node: FilterExpr, fields: ReadonlySet<string>): void {
  switch (node.op) {
    case "and":
    case "or":
      if (node.exprs.length === 0) {
        throw new RtDbError("BAD_REQUEST", `${node.op} filter requires at least one expr`);
      }
      for (const e of node.exprs) validateFilter(e, fields);
      return;
    case "in": {
      if (node.values.length === 0) {
        throw new RtDbError("BAD_REQUEST", "in filter requires at least one value");
      }
      for (const v of node.values) checkLeafValue(node.field, v, fields);
      const firstKind = inValueKind(node.values[0]);
      for (const v of node.values.slice(1)) {
        if (inValueKind(v) !== firstKind) {
          throw new RtDbError("BAD_REQUEST", "in filter values must all be the same type");
        }
      }
      return;
    }
    case "not":
      validateFilter(node.expr, fields);
      return;
    case "contains":
      checkLeafValue(node.field, node.value, fields);
      return;
    case "exists":
      if (!fields.has(node.field)) {
        throw new RtDbError("BAD_REQUEST", `filter references unknown field '${node.field}'`);
      }
      return;
    default:
      checkLeafValue(node.field, node.value, fields);
  }
}

function checkLeafValue(field: string, value: unknown, fields: ReadonlySet<string>): void {
  if (!fields.has(field)) {
    throw new RtDbError("BAD_REQUEST", `filter references unknown field '${field}'`);
  }
  if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
    throw new RtDbError("BAD_REQUEST", "filter value must be a string, number, or boolean");
  }
}

function inValueKind(value: unknown): "string" | "number" | "boolean" {
  if (typeof value === "string") return "string";
  if (typeof value === "number") return "number";
  return "boolean";
}

/** The declared field map of a table (`tableDef.fields`), keyed by field name.
 * Pass an empty object for type-less evaluation (e.g. unit tests). */
export type FieldMap = Readonly<Record<string, FieldTypeJson>>;

/**
 * Evaluate a `FilterExpr` predicate against a stored doc, mirroring server
 * `query::jsonb_lhs_and_bind` (`query.rs`): the filter value's kind picks the
 * comparison domain — string compares the doc field's `->>` text, number
 * compares it as `float8`, boolean as `boolean` — EXCEPT on a declared
 * `int64` field, where a string value (the wire form the server types as a
 * `bigint` bind, whether via an index's typed column or the jsonb path)
 * compares numerically: decimal strings must order `-605 < -1 < 15`, not
 * lexicographically (ENH-027 parity fix). A null/absent field never matches
 * (SQL NULL exclusion). `fields` is the table's declared field map (pass an
 * empty object for type-less evaluation, e.g. unit tests). Assumes
 * `validateFilter` already passed.
 */
export function evalFilterExpr(
  node: FilterExpr,
  doc: Record<string, unknown>,
  fields: FieldMap,
): boolean {
  switch (node.op) {
    case "and":
      return node.exprs.every((e) => evalFilterExpr(e, doc, fields));
    case "or":
      return node.exprs.some((e) => evalFilterExpr(e, doc, fields));
    case "in":
      return node.values.some((v) => compareLeaf("eq", node.field, v, doc, fields));
    case "not":
      return !evalFilterExpr(node.expr, doc, fields);
    case "contains": {
      const arr = doc[node.field];
      const want = JSON.stringify(node.value);
      return Array.isArray(arr) && arr.some((v) => JSON.stringify(v) === want);
    }
    case "exists": {
      const v = doc[node.field];
      return v !== undefined && v !== null;
    }
    default:
      return compareLeaf(node.op, node.field, node.value, doc, fields);
  }
}

function compareLeaf(
  op: FilterLeafOp,
  field: string,
  filterValue: unknown,
  doc: Record<string, unknown>,
  fields: FieldMap,
): boolean {
  const docVal = doc[field];
  if (docVal === null || docVal === undefined) {
    return false;
  }
  if (typeof filterValue === "string" && isInt64Field(fields[field])) {
    // The server binds a string filter value on an int64 field as a typed
    // `bigint` against the typed column (indexed fields) and rejects it on
    // the jsonb path — so any legal comparison is numeric. Parse both sides
    // exactly as i64 (i64::MAX is not float-exact); an unparseable value
    // never matches.
    const lhs = typeof docVal === "string" ? parseI64(docVal) : null;
    if (lhs === null) {
      return false;
    }
    const rhs = parseI64(filterValue);
    return rhs === null ? false : compareValues(op, lhs, rhs);
  }
  if (typeof filterValue === "string") {
    return compareValues(op, docToText(docVal), filterValue);
  }
  if (typeof filterValue === "number") {
    const lhs = docToNumber(docVal);
    return lhs === null ? false : compareValues(op, lhs, filterValue);
  }
  if (typeof docVal === "boolean") {
    return compareValues(op, docVal, filterValue as boolean);
  }
  return false;
}

/** Whether a declared field type is `int64` (an `optional<int64>` unwraps to
 * it — mirrors the server's `eq_bind_for` Optional unwrap). */
function isInt64Field(ty: FieldTypeJson | undefined): boolean {
  if (ty === undefined) {
    return false;
  }
  return ty.type === "int64" || (ty.type === "optional" && ty.inner.type === "int64");
}

/** Exact `i64::from_str` mirror: an optional `+`/`-` sign then one or more
 * ASCII digits, within the i64 range. Returns the value as a `bigint` (i64
 * is not JS-number-exact) or `null` when `s` is not a strict i64 decimal. */
function parseI64(s: string): bigint | null {
  if (!/^[+-]?\d+$/.test(s)) {
    return null;
  }
  const n = BigInt(s);
  return n >= -(2n ** 63n) && n <= 2n ** 63n - 1n ? n : null;
}

/** Mirrors Postgres `doc->>'field'`: the JSON text of the value. */
function docToText(docVal: unknown): string {
  if (typeof docVal === "string") return docVal;
  if (typeof docVal === "number") return JSON.stringify(docVal);
  if (typeof docVal === "boolean") return docVal ? "true" : "false";
  return JSON.stringify(docVal);
}

/** Mirrors Postgres `(doc->>'field')::float8`: a number, or a numeric string. */
function docToNumber(docVal: unknown): number | null {
  if (typeof docVal === "number") return Number.isFinite(docVal) ? docVal : null;
  if (typeof docVal === "string" && docVal.trim() !== "") {
    const n = Number(docVal);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function compareValues(
  op: FilterLeafOp,
  lhs: string | number | boolean | bigint,
  rhs: string | number | boolean | bigint,
): boolean {
  switch (op) {
    case "eq":
      return lhs === rhs;
    case "neq":
      return lhs !== rhs;
    case "gt":
      return lhs > rhs;
    case "gte":
      return lhs >= rhs;
    case "lt":
      return lhs < rhs;
    case "lte":
      return lhs <= rhs;
  }
}
