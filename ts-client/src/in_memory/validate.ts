/**
 * Value/filter validation for the in-memory harness — the leaf module of the
 * `in_memory/` decomposition (mirrors `rust-client/src/in_memory/validate.rs`).
 *
 * Four concerns live here:
 * - structural validation + evaluation of the query DSL's `FilterExpr`
 *   (mirroring server `query::compile_filter_node` / `field_lhs_and_bind` /
 *   `jsonb_lhs_and_bind`, including the SEC-126 value-kind checks);
 * - the eq-bind typing of index values (`indexColumnType` /
 *   `coerceIndexValue`, mirroring server `eq_bind_for`) — shared by the query
 *   engine's eq/range binds and the filter validator's indexed path;
 * - the pure JSON value predicates (`isHexId`, `isInt64String`, `clone`, …)
 *   shared by the store (`validateValue`), the query engine
 *   (`coerceIndexValue`), and the migration engine (`coerceValue`);
 * - the `ValueExpr` interpreter (`evalValueExpr`) and its field walkers —
 *   mirrors of server `value_expr.rs` (`eval_value_expr` /
 *   `walk_value_expr_fields`), shared by the store's computed-field stamping
 *   and push validation and the migration engine's rename/drop handling.
 */

import { RtDbError } from "../errors.js";
import type {
  CaseWhenJson,
  FieldTypeJson,
  FilterExpr,
  TableJson,
  ValueExprJson,
} from "../protocol.js";

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

/** Indexed-column storage type, mirroring server `indexed_column_type`. */
export type PgType = "text" | "number" | "boolean" | "int64";

interface IndexedType {
  pg: PgType;
  nullable: boolean;
}

function typeTag(ty: { type: string }): string {
  return ty.type;
}

/** Indexable column type — a port of server `schema::indexed_column_type`. */
export function indexColumnType(ty: TableJson["fields"][string]): IndexedType {
  switch (ty.type) {
    case "string":
    case "id":
      return { pg: "text", nullable: false };
    case "number":
      return { pg: "number", nullable: false };
    case "int64":
      return { pg: "int64", nullable: false };
    case "boolean":
      return { pg: "boolean", nullable: false };
    case "literal":
      if (typeof ty.value === "string") {
        return { pg: "text", nullable: false };
      }
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
    case "union":
      if (ty.variants.every((v) => v.type === "literal" && typeof v.value === "string")) {
        return { pg: "text", nullable: false };
      }
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
    case "optional": {
      const inner = indexColumnType(ty.inner);
      return { pg: inner.pg, nullable: true };
    }
    default:
      throw new RtDbError("SCHEMA_VIOLATION", `field type '${typeTag(ty)}' is not indexable`);
  }
}

/** Type-checks an eq/range bind value, mirroring server `eq_bind_for`. */
export function coerceIndexValue(table: TableJson, fieldName: string, value: unknown): unknown {
  const fieldTy = table.fields[fieldName];
  if (!fieldTy) {
    throw new RtDbError("INTERNAL", `index references unknown field '${fieldName}'`);
  }
  const { pg } = indexColumnType(fieldTy);
  switch (pg) {
    case "text":
      if (typeof value !== "string") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a string");
      }
      return value;
    case "number":
      if (typeof value !== "number") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a number");
      }
      return value;
    case "int64":
      // Canonical decimal string, validated exactly as on insert: `isInt64String`
      // mirrors the server's `i64::from_str` (rejects a leading `+` and
      // out-of-range values). eq is string === string, so the value is returned
      // as-is; only the comparator parses to BigInt for ordering.
      if (!isInt64String(value)) {
        throw new RtDbError("BAD_REQUEST", "eq value must be an int64 string");
      }
      return value;
    case "boolean":
      if (typeof value !== "boolean") {
        throw new RtDbError("BAD_REQUEST", "eq value must be a boolean");
      }
      return value;
  }
}

type FilterLeafOp = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";

/**
 * Structural validation of a `FilterExpr` against a table's declared fields,
 * mirroring server `query::compile_filter_node` / `field_lhs_and_bind`
 * (`query.rs`). Throws `BAD_REQUEST` for an unknown field, an empty `and`/`or`,
 * an empty `in`, a non-string/number/boolean leaf value, or — SEC-126 — a
 * value whose JSON kind does not match the field's declared type (indexed
 * fields type through the eq-bind conversion, other declared fields through
 * the server's `validate_jsonb_comparison_value`). Call once before
 * evaluating per row.
 *
 * `allowRelativeTime` (the mirror of server `validate_filter_expr_fields`'s
 * fourth param) admits the `olderThan` leaf — only the by-query step scan
 * path (`scanByQuery`) passes `true`; every other caller (read/query filters,
 * search/vector filters, computed `case` whens) rejects it, exactly like the
 * server's `compile_filter` chokepoint.
 */
export function validateFilter(
  node: FilterExpr,
  table: TableJson,
  allowRelativeTime = false,
): void {
  switch (node.op) {
    case "and":
    case "or":
      if (node.exprs.length === 0) {
        throw new RtDbError("BAD_REQUEST", `${node.op} filter requires at least one expr`);
      }
      for (const e of node.exprs) validateFilter(e, table, allowRelativeTime);
      return;
    case "in": {
      if (node.values.length === 0) {
        throw new RtDbError("BAD_REQUEST", "in filter requires at least one value");
      }
      for (const v of node.values) checkLeafValue(node.field, v, table);
      const firstKind = inValueKind(node.values[0]);
      for (const v of node.values.slice(1)) {
        if (inValueKind(v) !== firstKind) {
          throw new RtDbError("BAD_REQUEST", "in filter values must all be the same type");
        }
      }
      return;
    }
    case "not":
      validateFilter(node.expr, table, allowRelativeTime);
      return;
    case "olderThan": {
      // The server's OlderThan arm of validate_filter_expr_fields, in its
      // exact order: context, ms sign, declaredness, field kind.
      if (!allowRelativeTime) {
        throw new RtDbError(
          "BAD_REQUEST",
          "olderThan filter is only allowed in patchByQuery/deleteByQuery filters",
        );
      }
      if (node.ms < 0) {
        throw new RtDbError("BAD_REQUEST", "olderThan ms must be >= 0");
      }
      if (!Object.hasOwn(table.fields, node.field)) {
        throw new RtDbError("BAD_REQUEST", `filter references undeclared field '${node.field}'`);
      }
      const ty = table.fields[node.field];
      const inner = ty.type === "optional" ? ty.inner : ty;
      if (inner.type !== "number" && inner.type !== "int64") {
        throw new RtDbError(
          "BAD_REQUEST",
          `field '${node.field}' must be a number or int64 field for olderThan`,
        );
      }
      return;
    }
    case "contains":
      checkLeafValue(node.field, node.value, table);
      return;
    case "exists":
      if (!Object.hasOwn(table.fields, node.field)) {
        throw new RtDbError("BAD_REQUEST", `filter references unknown field '${node.field}'`);
      }
      return;
    default:
      checkLeafValue(node.field, node.value, table);
  }
}

function checkLeafValue(field: string, value: unknown, table: TableJson): void {
  if (!Object.hasOwn(table.fields, field)) {
    throw new RtDbError("BAD_REQUEST", `filter references unknown field '${field}'`);
  }
  if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") {
    throw new RtDbError("BAD_REQUEST", "filter value must be a string, number, or boolean");
  }
  // SEC-126: reject a value whose JSON kind contradicts the declared field
  // type BEFORE evaluation, exactly like server `field_lhs_and_bind` — left
  // unchecked, `gt(5)` on a string field compiles to a float8 cast Postgres
  // evaluates per row, so a subscription re-run on it fails forever and
  // silently never pushes. Indexed fields type the value through the same
  // eq-bind conversion as `query.eq` binds (`eq_bind_for`); other declared
  // fields get the jsonb kind check.
  const indexed = table.indexes?.some((idx) => idx.fields.includes(field)) ?? false;
  if (indexed) {
    coerceIndexValue(table, field, value);
  } else {
    validateJsonbComparisonValue(field, table.fields[field], value);
  }
}

/** Mirrors server `validate_jsonb_comparison_value` (SEC-126): passes when
 * `value`'s JSON kind can be ordered against a declared-but-not-indexed field
 * of type `ty`; the `optional` wrapper is unwrapped first. Note the deliberate
 * asymmetry with the indexed path: a non-indexed int64 field takes a JSON
 * NUMBER (the jsonb `(doc->>'f')::float8` comparison) and rejects the decimal
 * string the typed bigint column binds — that is the server's actual
 * behavior. */
function validateJsonbComparisonValue(field: string, ty: FieldTypeJson, value: unknown): void {
  const inner = ty.type === "optional" ? ty.inner : ty;
  let ok: boolean;
  switch (inner.type) {
    case "string":
    case "id":
    case "bytes":
      ok = typeof value === "string";
      break;
    case "number":
    case "int64":
      ok = typeof value === "number";
      break;
    case "boolean":
      ok = typeof value === "boolean";
      break;
    // Any / Literal / Union / Array / Object / Record / Vector / Null:
    // no reliable static check; accept any scalar (existing behavior).
    default:
      ok = typeof value === "string" || typeof value === "number" || typeof value === "boolean";
  }
  if (!ok) {
    throw new RtDbError(
      "BAD_REQUEST",
      `filter on field '${field}' value kind does not match declared field type`,
    );
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
 *
 * `nowMs` is the execution-time clock the `olderThan` leaf derives its cutoff
 * from (`nowMs − ms`, strict `<`) — only the by-query step scan passes it
 * (one read per step, mirroring the server's compile-once `now_ms()`). Without
 * a clock the leaf evaluates `false` — the fail-closed arm of server
 * `filter_matches`, which only ever sees authorize/case-when predicates
 * (both push-reject the op) and answers "deny"/"otherwise" on doubt.
 */
export function evalFilterExpr(
  node: FilterExpr,
  doc: Record<string, unknown>,
  fields: FieldMap,
  nowMs?: number,
): boolean {
  switch (node.op) {
    case "and":
      return node.exprs.every((e) => evalFilterExpr(e, doc, fields, nowMs));
    case "or":
      return node.exprs.some((e) => evalFilterExpr(e, doc, fields, nowMs));
    case "in":
      return node.values.some((v) => compareLeaf("eq", node.field, v, doc, fields));
    case "not":
      return !evalFilterExpr(node.expr, doc, fields, nowMs);
    case "contains": {
      const arr = doc[node.field];
      const want = JSON.stringify(node.value);
      return Array.isArray(arr) && arr.some((v) => JSON.stringify(v) === want);
    }
    case "exists": {
      const v = doc[node.field];
      return v !== undefined && v !== null;
    }
    case "olderThan": {
      if (nowMs === undefined) {
        return false;
      }
      const docVal = doc[node.field];
      if (docVal === null || docVal === undefined) {
        return false;
      }
      const cutoff = nowMs - node.ms;
      if (isInt64Field(fields[node.field])) {
        // int64 rides the decimal-string wire form end to end; parse exactly
        // (i64::MAX is not float-exact — the server's typed bigint column
        // compares in i64). The cutoff is an integer in the wire domain
        // (epoch-ms clock minus an integer ms); the Number fallback only
        // guards a hostile fractional ms.
        if (typeof docVal !== "string") {
          return false;
        }
        const lhs = parseI64(docVal);
        return (
          lhs !== null && (Number.isInteger(cutoff) ? lhs < BigInt(cutoff) : Number(lhs) < cutoff)
        );
      }
      if (typeof docVal !== "number" || !Number.isFinite(docVal)) {
        return false;
      }
      return docVal < cutoff;
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

// ---- ValueExpr interpreter (ENH-028) -----------------------------------------
//
// Mirrors server `value_expr.rs::eval_value_expr` — the per-write counterpart
// of the SQL compiler. Semantics are pinned by the computed-fields plan's
// "ValueExpr interpreter semantics" table: field reads are text extraction
// (the `doc->>'field'` convention), arithmetic is IEEE doubles with SQL-NULL
// propagation, a non-finite result is an error, trim strips spaces only, and
// `Case` predicates reuse this module's `evalFilterExpr` (principal markers
// are push-rejected inside computed exprs, so no principal context is needed).

/** JSON value → text, mirroring server `value_expr::to_text` (the SQL
 * `doc->>'field'` extraction the compile path emits): `null` (or an absent
 * key, which arrives as `undefined`) maps to SQL NULL; numbers use their JSON
 * number text form; objects/arrays use COMPACT JSON text (`{"a":1}` — the
 * convention all five implementations pin, deliberately not Postgres's spaced
 * jsonb text). Returns `null` for SQL NULL, never a JSON `"null"` string. */
function toText(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "string") return v;
  if (typeof v === "number") return String(v);
  if (typeof v === "boolean") return v ? "true" : "false";
  return JSON.stringify(v);
}

/** Strict decimal grammar for {@link toNumeric}'s string parse — JS `Number()`
 * accepts forms Rust's `f64::from_str` rejects (`"0x10"`, `""` → 0), so the
 * grammar is checked first and the empty string (post-trim) is an error on
 * both sides. */
function parseNumericString(s: string): number {
  const t = s.trim();
  if (!/^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$/.test(t)) {
    throw new RtDbError("BAD_REQUEST", `cannot cast '${s}' to number`);
  }
  const n = Number(t);
  if (!Number.isFinite(n)) {
    throw new RtDbError("BAD_REQUEST", `cannot cast '${s}' to number`);
  }
  return n;
}

/** JSON value → number for the arithmetic nodes, mirroring server
 * `value_expr::to_numeric`: `null` (or an absent key) is SQL NULL —
 * propagation, not an error; numbers yield their value; strings are trimmed
 * and strictly parsed; bool/object/array are type errors. */
function toNumeric(v: unknown): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return v;
  if (typeof v === "string") return parseNumericString(v);
  throw new RtDbError("BAD_REQUEST", "cannot cast to number");
}

/** IEEE double → JSON number, mirroring server `finite_number`: a non-finite
 * result (NaN, ±inf — overflow-shaped arithmetic) is an error rather than a
 * stored value. */
function finiteNumber(x: number): number {
  if (!Number.isFinite(x)) {
    throw new RtDbError("BAD_REQUEST", "numeric result is not finite");
  }
  return x;
}

/** `cast: "toInt64"` — a number must be an in-range integer (a float payload
 * like `3.5` is not), a string is trimmed and strictly parsed. The result is a
 * JSON NUMBER; the int64 decimal-string wire convention applies only to stored
 * int64 fields (the plan's "Int64 note"). */
function castToInt64(v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") {
    // Range-checked through BigInt — the i64 bounds are not JS-number-exact.
    if (!Number.isInteger(v)) {
      throw new RtDbError("BAD_REQUEST", `cannot cast ${v} to int64`);
    }
    const bi = BigInt(v);
    if (bi < -(2n ** 63n) || bi > 2n ** 63n - 1n) {
      throw new RtDbError("BAD_REQUEST", `cannot cast ${v} to int64`);
    }
    return v;
  }
  if (typeof v === "string") {
    if (!/^[+-]?\d+$/.test(v.trim())) {
      throw new RtDbError("BAD_REQUEST", `cannot cast '${v}' to int64`);
    }
    const n = BigInt(v.trim());
    if (n < -(2n ** 63n) || n > 2n ** 63n - 1n) {
      throw new RtDbError("BAD_REQUEST", `cannot cast '${v}' to int64`);
    }
    return Number(n);
  }
  throw new RtDbError("BAD_REQUEST", "cannot cast to int64");
}

const BOOLEAN_TRUE_WORDS = ["true", "t", "yes", "on", "1"];
const BOOLEAN_FALSE_WORDS = ["false", "f", "no", "off", "0"];

/** `cast: "toBoolean"` — bools pass through; numbers accept exactly `1`/`0`
 * (numeric equality, so `1.0`/`0.0` agree); strings match case-insensitively
 * against Postgres's boolean literal set. Mirrors server `cast_to_boolean`. */
function castToBoolean(v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (typeof v === "boolean") return v;
  if (typeof v === "number") {
    if (v === 1) return true;
    if (v === 0) return false;
    throw new RtDbError("BAD_REQUEST", `cannot cast ${v} to boolean`);
  }
  if (typeof v === "string") {
    const lower = v.toLowerCase();
    if (BOOLEAN_TRUE_WORDS.includes(lower)) return true;
    if (BOOLEAN_FALSE_WORDS.includes(lower)) return false;
    throw new RtDbError("BAD_REQUEST", `cannot cast '${v}' to boolean`);
  }
  throw new RtDbError("BAD_REQUEST", "cannot cast to boolean");
}

/** Evaluates a {@link ValueExprJson} against a doc — a port of server
 * `value_expr::eval_value_expr`. `fields` is the table's declared field map,
 * used only by `Case` predicates (this module's `evalFilterExpr`); markers are
 * push-rejected inside computed exprs, so no principal context exists here.
 * Throws `BAD_REQUEST` on an evaluation error (cast failure, division by
 * zero, non-finite arithmetic); the caller names the computed field. */
export function evalValueExpr(
  expr: ValueExprJson,
  doc: Record<string, unknown>,
  nowMs: number,
  fields: FieldMap = {},
): unknown {
  switch (expr.op) {
    case "field": {
      const text = toText(doc[expr.field]);
      return text === null ? null : text;
    }
    case "literal":
      return expr.value ?? null;
    case "concat": {
      let out = "";
      for (const part of expr.parts) {
        // toText is null exactly for null parts — concat skips them rather
        // than nulling the result; all-null parts yield "".
        const text = toText(evalValueExpr(part, doc, nowMs, fields));
        if (text !== null) {
          out += text;
        }
      }
      return out;
    }
    case "add":
    case "sub":
    case "mul":
    case "div": {
      const l = toNumeric(evalValueExpr(expr.left, doc, nowMs, fields));
      const r = toNumeric(evalValueExpr(expr.right, doc, nowMs, fields));
      if (l === null || r === null) {
        // Either operand SQL-NULL → NULL; propagation precedes the zero-divisor
        // and finiteness checks (null / 0 is null, not an error).
        return null;
      }
      if (expr.op === "div" && r === 0) {
        // True for -0 too (IEEE equality), so both zero spellings error.
        throw new RtDbError("BAD_REQUEST", "division by zero");
      }
      const x =
        expr.op === "add" ? l + r : expr.op === "sub" ? l - r : expr.op === "mul" ? l * r : l / r;
      return finiteNumber(x);
    }
    case "coalesce": {
      for (const part of expr.parts) {
        const v = evalValueExpr(part, doc, nowMs, fields);
        if (v !== null && v !== undefined) {
          return v;
        }
      }
      return null;
    }
    case "lower":
    case "upper":
    case "trim": {
      const text = toText(evalValueExpr(expr.value, doc, nowMs, fields));
      if (text === null) {
        return null;
      }
      if (expr.op === "lower") return text.toLowerCase();
      if (expr.op === "upper") return text.toUpperCase();
      // Spaces only — Postgres btrim's default, not Unicode whitespace: a
      // leading tab survives.
      return text.replace(/^ +/, "").replace(/ +$/, "");
    }
    case "cast": {
      const v = evalValueExpr(expr.value, doc, nowMs, fields);
      switch (expr.to) {
        case "toString": {
          const text = toText(v);
          return text === null ? null : text;
        }
        case "toNumber": {
          const n = toNumeric(v);
          return n === null ? null : finiteNumber(n);
        }
        case "toInt64":
          return castToInt64(v);
        case "toBoolean":
          return castToBoolean(v);
      }
      throw new RtDbError("BAD_REQUEST", `unknown cast '${String(expr.to)}'`);
    }
    case "now":
      return nowMs;
    case "case": {
      for (const cw of expr.whens) {
        if (evalFilterExpr(cw.when, doc, fields)) {
          return evalValueExpr(cw.then, doc, nowMs, fields);
        }
      }
      return evalValueExpr(expr.otherwise, doc, nowMs, fields);
    }
  }
  throw new RtDbError(
    "BAD_REQUEST",
    `unknown value expr op '${String((expr as { op: string }).op)}'`,
  );
}

/** Visits every field name a {@link ValueExprJson} reads: each `field` node,
 * every `case` branch's `then`/`otherwise`, and every `FilterExpr` field
 * inside `case.whens` — a port of server `walk_value_expr_fields`, used by
 * computed-field push validation and the migrate rename/drop handling. */
export function walkValueExprFields(expr: ValueExprJson, visit: (field: string) => void): void {
  switch (expr.op) {
    case "field":
      visit(expr.field);
      return;
    case "literal":
    case "now":
      return;
    case "concat":
    case "coalesce":
      for (const part of expr.parts) {
        walkValueExprFields(part, visit);
      }
      return;
    case "add":
    case "sub":
    case "mul":
    case "div":
      walkValueExprFields(expr.left, visit);
      walkValueExprFields(expr.right, visit);
      return;
    case "lower":
    case "upper":
    case "trim":
    case "cast":
      walkValueExprFields(expr.value, visit);
      return;
    case "case":
      for (const cw of expr.whens as CaseWhenJson[]) {
        walkFilterExprFields(cw.when, visit);
        walkValueExprFields(cw.then, visit);
      }
      walkValueExprFields(expr.otherwise, visit);
      return;
  }
  // The rust walkers are exhaustive matches; the JSON shape is not a closed
  // enum at runtime, so an unknown op must throw here — silently visiting
  // nothing would let push validation accept it.
  throw new RtDbError(
    "BAD_REQUEST",
    `unknown value expr op '${String((expr as { op: string }).op)}'`,
  );
}

/** The `FilterExpr` half of the walk: `and`/`or`/`not` recurse; every leaf
 * variant carries a `field`. A port of server
 * `value_expr::walk_filter_expr_fields`. */
export function walkFilterExprFields(expr: FilterExpr, visit: (field: string) => void): void {
  switch (expr.op) {
    case "eq":
    case "neq":
    case "gt":
    case "gte":
    case "lt":
    case "lte":
    case "in":
    case "contains":
    case "exists":
    case "olderThan":
      visit(expr.field);
      return;
    case "and":
    case "or":
      for (const e of expr.exprs) {
        walkFilterExprFields(e, visit);
      }
      return;
    case "not":
      walkFilterExprFields(expr.expr, visit);
      return;
  }
  throw new RtDbError(
    "BAD_REQUEST",
    `unknown filter expr op '${String((expr as { op: string }).op)}'`,
  );
}

/** Whether a `FilterExpr` tree carries an `olderThan` leaf anywhere — the
 * push-time guard for the two schema-declared filter surfaces (`authorize`
 * predicates and partial-index `where` predicates), which the harness
 * otherwise leaves unvalidated (a deliberate subset of server push
 * validation; server rejects the op in `validate_structure` and
 * `render_filter_literal_node` respectively). */
export function filterContainsOlderThan(expr: FilterExpr): boolean {
  switch (expr.op) {
    case "olderThan":
      return true;
    case "and":
    case "or":
      return expr.exprs.some((e) => filterContainsOlderThan(e));
    case "not":
      return filterContainsOlderThan(expr.expr);
    default:
      return false;
  }
}
