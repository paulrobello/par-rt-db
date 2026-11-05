/**
 * Document validation and stamping for the in-memory par-rt-db harness (split
 * out of `./store.ts`, QA-003b): the free functions ported from server
 * `schema.rs`/`txn.rs` — recursive value/doc validation, constraint checks,
 * ttl/updatedAt/defaults/computed stamping, push-time computed/constraint
 * validation, and `applyPatch`. `InMemoryRtDbClient`'s write paths call these
 * unchanged; filter validation/evaluation stays in `./validate.ts`.
 */

import { RtDbError } from "../errors.js";
import type { FieldTypeJson, SchemaJson, TableJson, ValueExprJson } from "../protocol.js";
import {
  clone,
  evalValueExpr,
  isBase64String,
  isHexId,
  isInt64String,
  isPlainObject,
  validateFilter,
  walkFilterExprFields,
  walkValueExprFields,
} from "./validate.js";

/** Recursive value validator — a port of server `schema::validate_value`. */
export function validateValue(ty: FieldTypeJson, value: unknown): boolean {
  switch (ty.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number";
    case "boolean":
      return typeof value === "boolean";
    case "null":
      return value === null;
    case "id":
      return isHexId(value);
    case "literal":
      return value === ty.value;
    case "optional":
      return value === null || validateValue(ty.inner, value);
    case "union":
      return ty.variants.some((variant) => validateValue(variant, value));
    case "array":
      return Array.isArray(value) && value.every((item) => validateValue(ty.element, item));
    case "object":
      if (!isPlainObject(value)) {
        return false;
      }
      if (Object.keys(value).some((key) => !(key in ty.fields))) {
        return false;
      }
      return Object.entries(ty.fields).every(([field, fieldTy]) => {
        if (field in value) {
          return validateValue(fieldTy, value[field]);
        }
        return fieldTy.type === "optional";
      });
    case "int64":
      return isInt64String(value);
    case "bytes":
      return isBase64String(value);
    case "any":
      return true;
    case "record":
      return isPlainObject(value) && Object.values(value).every((v) => validateValue(ty.value, v));
    case "vector":
      return (
        Array.isArray(value) &&
        value.length === ty.dimensions &&
        value.every((item) => typeof item === "number" && Number.isFinite(item))
      );
  }
}

/** Write-path constraint check for one present field value — a port of server
 *  `schema::value::check_constraints_for_field`. Null never violates; bounds
 *  compare inclusively (exact i64 compare on int64 decimal strings);
 *  `pattern` is an unanchored `RegExp` search. Violations are
 *  SCHEMA_VIOLATION naming the field and constraint. */
export function checkConstraintsForField(table: TableJson, field: string, value: unknown): void {
  const fc = table.constraints?.[field];
  if (fc === undefined || value === null) {
    return;
  }
  const violate = (label: string): RtDbError =>
    new RtDbError("SCHEMA_VIOLATION", `field '${field}' violates its '${label}' constraint`);
  const base = (() => {
    let ty = table.fields[field];
    while (ty?.type === "optional") {
      ty = ty.inner;
    }
    return ty?.type;
  })();
  if (base === "number") {
    const v = typeof value === "number" ? value : NaN;
    if (fc.min !== undefined && v < fc.min) {
      throw violate("min");
    }
    if (fc.max !== undefined && v > fc.max) {
      throw violate("max");
    }
  } else if (base === "int64") {
    // Exact i64 compare on the decimal string (BigInt) — IEEE doubles are
    // exact only to 2^53, so a float parse mis-compares bounds past it
    // (pinned by the corpus's constraint-int64-bounds-past-2p53 cases). A
    // non-parsable value or non-integer bound never violates, matching the
    // server's `parse::<i64>` (bounds are push-validated integers anyway).
    const v = typeof value === "string" && isInt64String(value) ? BigInt(value) : null;
    if (v !== null) {
      const bound = (b: number | undefined): bigint | null =>
        b !== undefined && Number.isInteger(b) ? BigInt(b) : null;
      const min = bound(fc.min);
      if (min !== null && v < min) {
        throw violate("min");
      }
      const max = bound(fc.max);
      if (max !== null && v > max) {
        throw violate("max");
      }
    }
  } else if (base === "string" && typeof value === "string") {
    if (fc.minLength !== undefined && value.length < fc.minLength) {
      throw violate("minLength");
    }
    if (fc.maxLength !== undefined && value.length > fc.maxLength) {
      throw violate("maxLength");
    }
    if (fc.pattern !== undefined && !new RegExp(fc.pattern).test(value)) {
      throw violate("pattern");
    }
  }
}

/** Full-document validation — a port of server `schema::validate_doc`. */
export function validateDoc(table: TableJson, doc: Record<string, unknown>): void {
  for (const key of Object.keys(doc)) {
    if (key.startsWith("_")) {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${key}' is reserved`);
    }
    if (!(key in table.fields)) {
      throw new RtDbError("SCHEMA_VIOLATION", `unknown field '${key}'`);
    }
  }
  for (const [field, fieldTy] of Object.entries(table.fields)) {
    if (field in doc) {
      if (!validateValue(fieldTy, doc[field])) {
        throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' has an invalid value`);
      }
      checkConstraintsForField(table, field, doc[field]);
    } else if (fieldTy.type !== "optional") {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' is required`);
    }
  }
}

/** Removes keys whose value is `null` for an `Optional` field whose inner type
 * does not itself accept `null` — a port of server `strip_unset_optionals`, so
 * an inserted/patched-then-nulled optional lands as "key absent", matching the
 * server's single representation of an unset optional. */
export function stripUnsetOptionals(
  table: TableJson,
  doc: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    if (value === null) {
      const fieldTy = table.fields[key];
      if (fieldTy?.type === "optional" && !validateValue(fieldTy.inner, null)) {
        continue;
      }
    }
    out[key] = value;
  }
  return out;
}

/** Stamps the TTL field at insert when the table declares a
 * `defaultDurationMs` and the document omits the field. After this, the TTL
 * field is ordinary (patch/replace manipulate it normally). Mirrors server
 * `txn::stamp_ttl_default`; runs BEFORE validation so the stamped value
 * satisfies a required numeric field. Returns the same `doc` reference when no
 * stamp is needed, otherwise a shallow copy with the field set. */
export function stampTtlDefault(
  table: TableJson,
  doc: Record<string, unknown>,
  now: number,
): Record<string, unknown> {
  const ttl = table.ttl;
  if (ttl?.defaultDurationMs != null && !(ttl.field in doc)) {
    const out: Record<string, unknown> = { ...doc };
    out[ttl.field] = now + ttl.defaultDurationMs;
    return out;
  }
  return doc;
}

/** Stamps the table's `updatedAtField` (FM-36) with the current epoch-ms,
 * overwriting any client-supplied value — the same authority model as
 * `stampTtlDefault`. Runs on every version-bumping write path: insert, patch,
 * replace, upsert (both branches), patchByQuery, and cascade setNull. On the
 * patch-shaped paths the stamp rides INSIDE the patch payload (applied before
 * `applyPatch`), so a client-supplied value is replaced before validation
 * ever sees it. The value matches the field's wire convention: a JSON number
 * on `number`, a decimal string on `int64`. Mirrors server
 * `txn::stamp_updated_at`; returns the same `target` reference when the table
 * declares no `updatedAtField`, otherwise a shallow copy with the field set. */
export function stampUpdatedAt(
  table: TableJson,
  target: Record<string, unknown>,
  now: number,
): Record<string, unknown> {
  const field = table.updatedAtField;
  if (field === undefined) {
    return target;
  }
  const out: Record<string, unknown> = { ...target };
  out[field] = table.fields[field]?.type === "int64" ? String(now) : now;
  return out;
}

/** Applies the table's push-time-validated `defaults` (FM-32) to a NEW
 * document: every key the doc omits is stamped from the schema. Runs after
 * `stampTtlDefault` (a ttl default on the same field wins — it stamped the key
 * first) and before any principal stamps (server-stamped owner/authorize
 * values win). Callers are exactly the new-document paths — insert, replace,
 * upsert-insert; `patch` (and upsert-update / patchByQuery) never re-apply, so
 * clearing an optional field stays cleared. Mirrors server `txn::apply_defaults`;
 * returns the same `doc` reference when no stamp is needed, otherwise a
 * shallow copy with the fields set. */
export function applyDefaults(
  table: TableJson,
  doc: Record<string, unknown>,
): Record<string, unknown> {
  const defaults = table.defaults;
  if (!defaults) return doc;
  const missing = Object.keys(defaults).filter((field) => !(field in doc));
  if (missing.length === 0) return doc;
  const out: Record<string, unknown> = { ...doc };
  for (const field of missing) {
    // Clone so array/object defaults are never aliased into a doc (the server
    // clones the serde value too).
    out[field] = clone(defaults[field]);
  }
  return out;
}

/** Stamps the table's computed fields (ENH-028): every `computed` entry is
 * re-evaluated against the final doc and stored — a null result REMOVES the
 * key (an unset optional field is an absent key, `stripUnsetOptionals`' shape
 * convention) and a non-null result overwrites whatever is there (the
 * ownerField authority model: client-supplied values never survive). An
 * evaluation error fails the whole write as BAD_REQUEST, naming the field.
 * Runs last in the stamp chain — after ttl default, updatedAt, defaults, and
 * autoIncrement, so expressions see final inputs — and before `validateDoc` at
 * every site: `applyPatch` (patch, upsert's update branch, patchByQuery, and
 * cascade setNull), `doInsert` (insert + upsert's insert branch), and
 * `doReplace`. Mirrors server `txn::stamp_computed`; returns the same `doc`
 * reference when the table declares no computed fields. */
export function stampComputed(
  table: TableJson,
  doc: Record<string, unknown>,
  now: number,
): Record<string, unknown> {
  const computed = table.computed;
  if (!computed) return doc;
  const out: Record<string, unknown> = { ...doc };
  for (const [name, expr] of Object.entries(computed)) {
    let value: unknown;
    try {
      value = evalValueExpr(expr, out, now, table.fields);
    } catch (err) {
      const message = err instanceof RtDbError ? err.message : String(err);
      throw new RtDbError("BAD_REQUEST", `computed field '${name}': ${message}`);
    }
    if (value === null || value === undefined) {
      delete out[name];
    } else {
      out[name] = value;
    }
  }
  return out;
}

/** The statically-known result kind of a `ValueExprJson`, for the computed-field
 * push check — a port of server `schema::infer_static_kind`. `undefined` means
 * the result kind varies by input: `field` (text extraction of any JSON
 * value), `coalesce`/`case` (whichever branch wins), and the null / object /
 * array literals whose runtime `validateDoc` check is the only guard. */
type StaticKind = "string" | "number" | "boolean";

function inferStaticKind(expr: ValueExprJson): StaticKind | undefined {
  switch (expr.op) {
    case "field":
    case "coalesce":
    case "case":
      return undefined;
    case "literal": {
      const v = expr.value;
      if (typeof v === "string") return "string";
      if (typeof v === "number") return "number";
      if (typeof v === "boolean") return "boolean";
      return undefined;
    }
    case "concat":
    case "lower":
    case "upper":
    case "trim":
      return "string";
    case "cast":
      switch (expr.to) {
        case "toString":
          return "string";
        case "toNumber":
        case "toInt64":
          return "number";
        case "toBoolean":
          return "boolean";
      }
      return undefined;
    case "add":
    case "sub":
    case "mul":
    case "div":
    case "now":
      return "number";
  }
}

/** Computed-field push validation (ENH-028) — a port of server
 *  `schema::TableDef::validate_computed`, called from `pushSchema` and re-run
 *  on a migration's derived schema (the `changeType` re-validation path). Rules:
 *  1. every `computed` key names a declared field;
 *  2. the key is not one of the server-stamped declaration fields
 *     (`ownerField`/`collaboratorsField`/`autoIncrementField`);
 *  3. every field the expression references (including `case.whens` filter
 *     fields) is declared and not itself computed (no chained evaluation);
 *  4. `case.when` filters reject principal markers — computed exprs run on
 *     every write with no interactive principal (the engine's `validateFilter`
 *     rejects a marker's object value as a non-scalar filter value);
 *  5. when the expression's result kind is statically known, the field's type
 *     must accept a value of that kind (an int64 field admits a String kind —
 *     decimal-string possibility — but never a Number kind, since arithmetic
 *     yields JSON numbers);
 *  6. the table's `authorize` predicate references no computed field.
 *  Every failure is BAD_REQUEST, like the server. */
/** Push-time constraint validation — a port of server `schema::validate`'s
 *  `validate_constraints`: each key names a declared field, `min`/`max` sit
 *  only on number/int64 (integer bounds on int64), `minLength`/`maxLength`/
 *  `pattern` only on string, bounds don't cross, and `pattern` compiles under
 *  `RegExp` (unanchored search). Violations are SCHEMA_VIOLATION naming the
 *  field, matching the server's typed push error. */
export function validateConstraints(schema: SchemaJson): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [field, fc] of Object.entries(table.constraints ?? {})) {
      const violate = (detail: string): RtDbError =>
        new RtDbError("SCHEMA_VIOLATION", `constraints on field '${field}': ${detail}`);
      let fieldTy = table.fields[field];
      if (fieldTy === undefined) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `constraints key '${field}' is not a declared field of table '${tableName}'`,
        );
      }
      while (fieldTy.type === "optional") {
        fieldTy = fieldTy.inner;
      }
      if (fc.min !== undefined || fc.max !== undefined) {
        if (fieldTy.type !== "number" && fieldTy.type !== "int64") {
          throw violate(`'min'/'max' is legal only on number/int64 fields`);
        }
        if (
          fieldTy.type === "int64" &&
          [fc.min, fc.max].some((b) => b !== undefined && !Number.isInteger(b))
        ) {
          throw violate(`'min'/'max' bound must be an integer on an int64 field`);
        }
      }
      if (
        (fc.minLength !== undefined || fc.maxLength !== undefined || fc.pattern !== undefined) &&
        fieldTy.type !== "string"
      ) {
        throw violate(`'minLength'/'maxLength'/'pattern' is legal only on string fields`);
      }
      if (fc.min !== undefined && fc.max !== undefined && fc.min > fc.max) {
        throw violate(`'min' exceeds 'max'`);
      }
      if (fc.minLength !== undefined && fc.maxLength !== undefined && fc.minLength > fc.maxLength) {
        throw violate(`'minLength' exceeds 'maxLength'`);
      }
      if (fc.pattern !== undefined) {
        try {
          // Validate compile only (same check the server runs at push time).
          new RegExp(fc.pattern);
        } catch {
          throw violate(`invalid pattern: '${fc.pattern}'`);
        }
      }
    }
  }
}

export function validateComputed(schema: SchemaJson): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    const computed = table.computed;
    if (computed) {
      for (const [field, expr] of Object.entries(computed)) {
        if (!Object.hasOwn(table.fields, field)) {
          throw new RtDbError(
            "BAD_REQUEST",
            `computed field '${tableName}.${field}' is not a declared field`,
          );
        }
        if (table.ownerField === field) {
          throw new RtDbError(
            "BAD_REQUEST",
            `computed field '${tableName}.${field}' must not be the table's ownerField`,
          );
        }
        if (table.collaboratorsField === field) {
          throw new RtDbError(
            "BAD_REQUEST",
            `computed field '${tableName}.${field}' must not be the table's collaboratorsField`,
          );
        }
        if (table.autoIncrementField === field) {
          throw new RtDbError(
            "BAD_REQUEST",
            `computed field '${tableName}.${field}' must not be the table's autoIncrementField`,
          );
        }
        walkValueExprFields(expr, (referenced) => {
          if (!Object.hasOwn(table.fields, referenced)) {
            throw new RtDbError(
              "BAD_REQUEST",
              `computed field '${tableName}.${field}' references undeclared field '${referenced}'`,
            );
          }
          if (Object.hasOwn(computed, referenced)) {
            throw new RtDbError(
              "BAD_REQUEST",
              `computed field '${tableName}.${field}' references computed field '${referenced}' (computed fields may not reference each other)`,
            );
          }
        });
        validateComputedCaseWhens(expr, table);
        const kind = inferStaticKind(expr);
        if (kind !== undefined) {
          const sample = kind === "string" ? "s" : kind === "number" ? 1 : true;
          // `validateValue` is the wire contract, but int64's wire form is a
          // decimal STRING: a Number-kind result can never validate
          // (arithmetic yields JSON numbers), while a String-kind one can
          // ("42") — decimal-ness stays a runtime `validateDoc` check.
          // Optional unwrapping admits the nullable spelling.
          let inner = table.fields[field];
          while (inner.type === "optional") {
            inner = inner.inner;
          }
          const accepts =
            validateValue(table.fields[field], sample) ||
            (inner.type === "int64" && kind === "string");
          if (!accepts) {
            throw new RtDbError(
              "BAD_REQUEST",
              `computed field '${tableName}.${field}' produces ${kind === "string" ? "a string" : kind === "number" ? "a number" : "a boolean"}, which the field type does not accept`,
            );
          }
        }
      }
    }
    // Rule 6: authorize runs pre-stamp on the insert paths, so a predicate
    // over a computed field would read client input.
    if (table.authorize !== undefined && computed !== undefined) {
      walkFilterExprFields(table.authorize, (referenced) => {
        if (Object.hasOwn(computed, referenced)) {
          throw new RtDbError(
            "BAD_REQUEST",
            `computed field '${tableName}.${referenced}' must not be referenced by the table's authorize predicate (authorize predicates may not reference computed fields)`,
          );
        }
      });
    }
  }
}

/** Walks a computed expression's `case` nodes validating each `when` filter
 * with the engine's filter validator — the marker-rejecting mode the server's
 * `validate_computed_case_whens` applies (a `{"$user":true}` marker value
 * fails the scalar-value check). `then`/`otherwise` recurse so a `case`
 * nested inside a branch is covered. */
function validateComputedCaseWhens(expr: ValueExprJson, table: TableJson): void {
  switch (expr.op) {
    case "case":
      for (const cw of expr.whens) {
        validateFilter(cw.when, table);
        validateComputedCaseWhens(cw.then, table);
      }
      validateComputedCaseWhens(expr.otherwise, table);
      return;
    case "concat":
    case "coalesce":
      for (const part of expr.parts) {
        validateComputedCaseWhens(part, table);
      }
      return;
    case "add":
    case "sub":
    case "mul":
    case "div":
      validateComputedCaseWhens(expr.left, table);
      validateComputedCaseWhens(expr.right, table);
      return;
    case "lower":
    case "upper":
    case "trim":
    case "cast":
      validateComputedCaseWhens(expr.value, table);
      return;
    case "field":
    case "literal":
    case "now":
      return;
  }
}

/** Applies a patch's fields onto `doc` — a port of server `txn::apply_patch`.
 * Every patch-shaped path funnels through here (`patch`, upsert's update
 * branch, `patchByQuery`, and cascade setNull), so the computed-field stamp
 * sits here too: client-supplied computed keys are dropped pre-merge (the
 * stamp after the loop overwrites them, but a wrong-typed value must not fail
 * validation first), and each computed field is re-evaluated against the
 * merged doc before `validateDoc`. */
export function applyPatch(
  table: TableJson,
  doc: Record<string, unknown>,
  fields: Record<string, unknown>,
  now: number,
): Record<string, unknown> {
  // The auto-increment field is server-assigned and immutable after insert.
  // A patch may carry it back unchanged (round-trip friendly), but any
  // different value — including a type-shifted form of the same number — is
  // rejected. Covers `patch`, upsert's update branch, and `patchByQuery`
  // (every patch-shaped path funnels through here). Mirrors the check at the
  // top of server `txn::apply_patch`.
  const auto = table.autoIncrementField;
  if (auto !== undefined && auto in fields && doc[auto] !== fields[auto]) {
    throw new RtDbError("BAD_REQUEST", `autoIncrementField '${auto}' cannot be changed`);
  }
  const merged: Record<string, unknown> = { ...doc };
  for (const [field, value] of Object.entries(fields)) {
    // ENH-028: a computed key in the patch payload is dropped — the stamp
    // below re-derives it from the merged doc (server `apply_patch`'s
    // skip+drop of `table_def.computed` keys).
    if (table.computed !== undefined && Object.hasOwn(table.computed, field)) {
      continue;
    }
    const fieldTy = table.fields[field];
    if (!fieldTy) {
      throw new RtDbError("SCHEMA_VIOLATION", `unknown field '${field}'`);
    }
    if (value === null && fieldTy.type === "optional" && !validateValue(fieldTy.inner, null)) {
      delete merged[field];
      continue;
    }
    if (!validateValue(fieldTy, value)) {
      throw new RtDbError("SCHEMA_VIOLATION", `field '${field}' has an invalid value`);
    }
    merged[field] = value;
  }
  const stamped = stampComputed(table, merged, now);
  validateDoc(table, stamped);
  return stamped;
}
