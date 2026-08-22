/**
 * Schema-migration engine for the in-memory harness (mirrors
 * `rust-client/src/in_memory/migrate.rs`): destructive-change detection,
 * `onDelete` push validation, and the migration-directive interpreter.
 *
 * `applyMigrationDirective` is a thin dispatcher over one function per
 * directive kind (`applyRenameFieldDirective`, `applyRenameTableDirective`,
 * …) — the same per-directive decomposition the rust and python engines
 * carry. Doc-store access goes through the {@link MigrationStore} handle the
 * client core passes in (its `tables` map reference + a lazy `rowsFor`
 * accessor), so the directive functions stay pure with respect to client
 * state they don't own.
 */

import { RtDbError } from "../errors.js";
import type {
  Cast,
  DirectiveJson,
  DirectiveReportJson,
  FieldTypeJson,
  FilterExpr,
  OnDeleteAction,
  SchemaJson,
  TableJson,
  ValueExprJson,
} from "../protocol.js";
import type { StoredRow } from "./store.js";
import {
  clone,
  filterContainsOlderThan,
  indexColumnType,
  isInt64String,
  walkValueExprFields,
} from "./validate.js";

/** Returns the values a finite literal-union (or lone literal) accepts, mirroring
 *  server `schema::literal_set`: a lone `literal` yields its single value, and a
 *  `union` yields its variants' literal values only when EVERY variant is a
 *  `literal` (and the union is non-empty). Returns `null` for any other type
 *  (scalar, optional, object, array, mixed/open union, empty union) — those are
 *  not finite sets and cannot widen. */
function literalSet(ty: FieldTypeJson): unknown[] | null {
  switch (ty.type) {
    case "literal":
      return [ty.value];
    case "union": {
      if (ty.variants.length === 0) {
        return null;
      }
      const vals: unknown[] = [];
      for (const variant of ty.variants) {
        if (variant.type !== "literal") {
          return null;
        }
        vals.push(variant.value);
      }
      return vals;
    }
    default:
      return null;
  }
}

/** True iff every value accepted by `old` is also accepted by `next` — a port
 *  of server `schema::is_widening_of`. Both sides must be finite literal sets
 *  (per {@link literalSet}); membership is compared by `===` since literal values
 *  are primitives (`string | number | boolean`). Linear scan, matching the
 *  Rust `Vec::contains` semantics over the new set. */
function isWideningOf(old: FieldTypeJson, next: FieldTypeJson): boolean {
  const oldVals = literalSet(old);
  const newVals = literalSet(next);
  if (oldVals === null || newVals === null) {
    return false;
  }
  return oldVals.every((o) => newVals.some((n) => n === o));
}

/** Rejects destructive schema changes — a port of server
 *  `ddl::detect_destructive_changes`. A second `pushSchema` may only ADD tables,
 *  fields, and indexes; removing or retyping any existing table/field/index is a
 *  `BAD_REQUEST` with the same message the live server returns. Additive changes
 *  (new tables, new fields, new indexes) pass through. Field types and index
 *  `fields`/`vector` are compared by `JSON.stringify` deep equality — after
 *  stripping `onDelete` actions (FM-33: adding or changing an action is
 *  additive); index kind
 *  (btree vs search) by the presence/absence of `search`. A field-type change is
 *  accepted when it is a safe widening (server `schema::is_widening_of`): a
 *  finite literal-union that grows, or a single literal that becomes a union.
 *  Flipping `unique`, attaching/changing a `where` predicate, or changing a
 *  search index's `language` are all breaking index changes too — rejected with
 *  the server's messages. */
export function detectDestructiveChanges(oldSchema: SchemaJson, newSchema: SchemaJson): void {
  for (const [tableName, oldTable] of Object.entries(oldSchema.tables)) {
    const newTable = newSchema.tables[tableName];
    if (!newTable) {
      throw new RtDbError("BAD_REQUEST", `removed table '${tableName}'`);
    }
    for (const [fieldName, oldFieldType] of Object.entries(oldTable.fields)) {
      const newFieldType = newTable.fields[fieldName];
      if (!newFieldType) {
        throw new RtDbError("BAD_REQUEST", `removed field '${tableName}.${fieldName}'`);
      }
      const strippedNew = JSON.stringify(stripOnDelete(newFieldType));
      const strippedOld = JSON.stringify(stripOnDelete(oldFieldType));
      if (strippedNew !== strippedOld && !isWideningOf(oldFieldType, newFieldType)) {
        throw new RtDbError("BAD_REQUEST", `changed type of field '${tableName}.${fieldName}'`);
      }
    }
    for (const oldIndex of oldTable.indexes ?? []) {
      const newIndex = (newTable.indexes ?? []).find((i) => i.name === oldIndex.name);
      if (!newIndex) {
        throw new RtDbError("BAD_REQUEST", `removed index '${oldIndex.name}'`);
      }
      if (JSON.stringify(newIndex.fields) !== JSON.stringify(oldIndex.fields)) {
        throw new RtDbError("BAD_REQUEST", `changed fields of index '${oldIndex.name}'`);
      }
      if (!!newIndex.search !== !!oldIndex.search) {
        throw new RtDbError(
          "BAD_REQUEST",
          `changed kind of index '${oldIndex.name}' (btree <-> search)`,
        );
      }
      if (JSON.stringify(newIndex.vector ?? null) !== JSON.stringify(oldIndex.vector ?? null)) {
        throw new RtDbError("BAD_REQUEST", `changed vector spec of index '${oldIndex.name}'`);
      }
      if (!!newIndex.unique !== !!oldIndex.unique) {
        throw new RtDbError("BAD_REQUEST", `changed uniqueness of index '${oldIndex.name}'`);
      }
      if (JSON.stringify(newIndex.where ?? null) !== JSON.stringify(oldIndex.where ?? null)) {
        throw new RtDbError("BAD_REQUEST", `changed partial predicate of index '${oldIndex.name}'`);
      }
      if ((newIndex.language ?? null) !== (oldIndex.language ?? null)) {
        throw new RtDbError("BAD_REQUEST", `changed language of search index '${oldIndex.name}'`);
      }
    }
  }
}

/** Push-time schema validation — the TTL, updatedAtField, autoIncrementField,
 *  and index-field rules of server `schema::validate` (`schema::validate_indexes`
 *  + `validate_ttl` + `validate_updated_at` + `validate_auto_increment`),
 *  mirroring the rust harness's `SchemaDef::validate`:
 *  index fields must be declared and indexable, search indexes must cover text
 *  fields, and a TTL must name a numeric field carrying a single-field,
 *  non-unique, non-partial btree index. Deliberately a subset — identifier
 *  formats, owner/collaborator fields, defaults, and `onDelete` shapes stay
 *  server-side (the last has its own `validateOnDelete` pass) — EXCEPT the
 *  `olderThan` boundary: `authorize` predicates reject the op here
 *  (SCHEMA_VIOLATION, like the server's `validate_structure` arm) and
 *  partial-index `where` predicates reject it (BAD_REQUEST, the server's
 *  partial-index literal-compile arm), since an execution-time-relative
 *  cutoff has no static meaning in either. */
export function validateSchema(schema: SchemaJson): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    if (table.authorize !== undefined && filterContainsOlderThan(table.authorize)) {
      throw new RtDbError(
        "SCHEMA_VIOLATION",
        "olderThan filter is only allowed in patchByQuery/deleteByQuery filters",
      );
    }
    for (const index of table.indexes ?? []) {
      if (index.where !== undefined && filterContainsOlderThan(index.where)) {
        throw new RtDbError(
          "BAD_REQUEST",
          "olderThan filter is not allowed in a partial-index predicate",
        );
      }
      if (index.fields.length === 0) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `index '${index.name}' on table '${tableName}' has no fields`,
        );
      }
      // A vector index's `fields[0]` is a Vector column, which is not
      // btree-indexable — the server validates vector specs in their own
      // branch and skips the per-field loop below.
      if (index.vector) {
        continue;
      }
      for (const fieldName of index.fields) {
        const fieldType = table.fields[fieldName];
        if (!fieldType) {
          throw new RtDbError(
            "SCHEMA_VIOLATION",
            `index '${index.name}' on table '${tableName}' references unknown field '${fieldName}'`,
          );
        }
        const { pg } = indexColumnType(fieldType);
        if (index.search && pg !== "text") {
          throw new RtDbError(
            "SCHEMA_VIOLATION",
            `search index '${index.name}' on table '${tableName}' has non-text field '${fieldName}'`,
          );
        }
      }
    }
    const ttl = table.ttl;
    if (ttl) {
      const fieldType = table.fields[ttl.field];
      if (!fieldType) {
        throw new RtDbError("SCHEMA_VIOLATION", `ttl.field '${ttl.field}' is not a declared field`);
      }
      if (fieldType.type !== "number" && fieldType.type !== "int64") {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `ttl.field '${ttl.field}' must be a number or bigint field`,
        );
      }
      const hasTtlIndex = (table.indexes ?? []).some(
        (idx) =>
          !idx.search &&
          !idx.vector &&
          !idx.unique &&
          !idx.where &&
          idx.fields.length === 1 &&
          idx.fields[0] === ttl.field,
      );
      if (!hasTtlIndex) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `ttl.field '${ttl.field}' requires a single-field, non-unique, non-partial btree index on it`,
        );
      }
      if (ttl.defaultDurationMs != null && ttl.defaultDurationMs <= 0) {
        throw new RtDbError("SCHEMA_VIOLATION", "ttl.defaultDurationMs must be greater than 0");
      }
    }
    // FM-36 `updatedAtField` push validation — mirrors server
    // `schema::validate_updated_at` (minus the identifier-format check, which
    // stays server-side like every other identifier rule here): the field must
    // be declared numeric (the stamp is an epoch-ms number — a decimal string
    // on `int64`, matching the int64 wire convention) and must differ from
    // `ttl.field` (both stamps write unconditionally, so a shared field would
    // silently drop the expiry). No index is required on the field.
    const updatedAt = table.updatedAtField;
    if (updatedAt !== undefined) {
      const fieldType = table.fields[updatedAt];
      if (!fieldType) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `updatedAtField '${updatedAt}' is not a declared field`,
        );
      }
      if (fieldType.type !== "number" && fieldType.type !== "int64") {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `updatedAtField '${updatedAt}' must be a number or bigint field`,
        );
      }
      if (ttl !== undefined && ttl.field === updatedAt) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `updatedAtField '${updatedAt}' must differ from ttl.field (both stamps write unconditionally; a shared field would drop the expiry)`,
        );
      }
    }
    // FM-37 `autoIncrementField` push validation — mirrors server
    // `schema::validate_auto_increment` (minus the identifier-format check,
    // which stays server-side like every other identifier rule here): the
    // field must be declared `int64` exactly (the counter produces int64; a
    // `number` would lose precision, an `optional` would admit a missing
    // counter) and must differ from `ttl.field` and `updatedAtField` (both
    // stamp unconditionally on writes the counter must survive verbatim). A
    // `defaults` entry on the field is allowed but always loses to the
    // stamp.
    const autoIncrement = table.autoIncrementField;
    if (autoIncrement !== undefined) {
      const fieldType = table.fields[autoIncrement];
      if (!fieldType) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `autoIncrementField '${autoIncrement}' is not a declared field`,
        );
      }
      if (fieldType.type !== "int64") {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `autoIncrementField '${autoIncrement}' must be an int64 field`,
        );
      }
      if (ttl !== undefined && ttl.field === autoIncrement) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `autoIncrementField '${autoIncrement}' must differ from ttl.field (the ttl reaper would delete counter rows)`,
        );
      }
      if (updatedAt !== undefined && updatedAt === autoIncrement) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `autoIncrementField '${autoIncrement}' must differ from updatedAtField (the timestamp would overwrite the counter on every write)`,
        );
      }
    }
  }
}

/** Strips `onDelete` from id fields, recursing through every compositor — a
 *  port of server `schema::strip_on_delete`. Used by the additive-push
 *  comparison so adding or changing an `onDelete` action (FM-33) never counts
 *  as a destructive field-type change. */
function stripOnDelete(ty: FieldTypeJson): FieldTypeJson {
  switch (ty.type) {
    case "id":
      return ty.onDelete !== undefined ? { type: "id", table: ty.table } : ty;
    case "optional":
      return { type: "optional", inner: stripOnDelete(ty.inner) };
    case "union":
      return { type: "union", variants: ty.variants.map(stripOnDelete) };
    case "array":
      return { type: "array", element: stripOnDelete(ty.element) };
    case "object": {
      const fields: Record<string, FieldTypeJson> = {};
      for (const [key, fieldTy] of Object.entries(ty.fields)) {
        fields[key] = stripOnDelete(fieldTy);
      }
      return { type: "object", fields };
    }
    case "record":
      return { type: "record", value: stripOnDelete(ty.value) };
    default:
      return ty;
  }
}

/** True iff an id field carrying `onDelete` appears anywhere in `ty` (at any
 *  nesting depth) — the probe behind the FM-33 push-validation rule that
 *  confines `onDelete` to a top-level id or optional-id field. */
function fieldHasNestedOnDelete(ty: FieldTypeJson): boolean {
  switch (ty.type) {
    case "id":
      return ty.onDelete !== undefined;
    case "optional":
      return fieldHasNestedOnDelete(ty.inner);
    case "union":
      return ty.variants.some(fieldHasNestedOnDelete);
    case "array":
      return fieldHasNestedOnDelete(ty.element);
    case "object":
      return Object.values(ty.fields).some(fieldHasNestedOnDelete);
    case "record":
      return fieldHasNestedOnDelete(ty.value);
    default:
      return false;
  }
}

/** Validates `onDelete` declarations at push time — a port of server
 *  `schema::validate_on_delete` (FM-33). An action is legal only on a top-level
 *  `id` field (or one `optional` wrapping it — required for `setNull`); the
 *  referencing field needs a single-field, non-unique, non-partial btree index;
 *  and the referenced table must exist in the same schema. */
export function validateOnDelete(schema: SchemaJson): void {
  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [fieldName, fieldTy] of Object.entries(table.fields)) {
      const topId =
        fieldTy.type === "id"
          ? fieldTy
          : fieldTy.type === "optional" && fieldTy.inner.type === "id"
            ? fieldTy.inner
            : null;
      if (topId?.onDelete === undefined) {
        if (fieldHasNestedOnDelete(fieldTy)) {
          throw new RtDbError(
            "SCHEMA_VIOLATION",
            `field '${fieldName}' on table '${tableName}': onDelete is legal only on a top-level id or optional-id field`,
          );
        }
        continue;
      }
      const action = topId.onDelete;
      if (action === "setNull" && fieldTy.type !== "optional") {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `onDelete 'setNull' requires the id field to be optional`,
        );
      }
      const hasIndex = (table.indexes ?? []).some(
        (index) =>
          !index.search &&
          !index.vector &&
          !index.unique &&
          !index.where &&
          index.fields.length === 1 &&
          index.fields[0] === fieldName,
      );
      if (!hasIndex) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `onDelete field '${fieldName}' on table '${tableName}' requires a single-field, non-unique, non-partial btree index on it`,
        );
      }
    }
  }
  // Second pass (server order): every referenced table must exist.
  for (const [tableName, table] of Object.entries(schema.tables)) {
    for (const [fieldName, fieldTy] of Object.entries(table.fields)) {
      const topId =
        fieldTy.type === "id"
          ? fieldTy
          : fieldTy.type === "optional" && fieldTy.inner.type === "id"
            ? fieldTy.inner
            : null;
      if (topId?.onDelete === undefined) {
        continue;
      }
      if (!(topId.table in schema.tables)) {
        throw new RtDbError(
          "SCHEMA_VIOLATION",
          `onDelete field '${fieldName}' on table '${tableName}' references unknown table '${topId.table}'`,
        );
      }
    }
  }
}

/** The `onDelete` action `ty` declares against `parentTable`, if any — a port
 *  of server `txn::on_delete_ref`. Only a top-level id (or one `optional`
 *  wrapping it) can carry one; push validation (`validateOnDelete`) keeps every
 *  other shape from reaching this walk. */
export function onDeleteRef(ty: FieldTypeJson, parentTable: string): OnDeleteAction | undefined {
  if (ty.type === "id") {
    return ty.table === parentTable ? ty.onDelete : undefined;
  }
  if (ty.type === "optional") {
    return onDeleteRef(ty.inner, parentTable);
  }
  return undefined;
}

/** True iff `cast` can coerce from `old` — a port of server `migrate::cast_valid_for`.
 *  Mirrors the spec's coercion matrix: only the listed scalar source types are
 *  accepted; an `optional`/`object`/`array`/etc. source has no sound coercion. */
function castValidFor(cast: Cast, old: FieldTypeJson): boolean {
  const t = old.type;
  switch (cast) {
    case "toString":
      return t === "string" || t === "number" || t === "boolean" || t === "int64";
    case "toNumber":
      return t === "string" || t === "boolean" || t === "int64";
    case "toInt64":
      return t === "string" || t === "number";
    case "toBoolean":
      return t === "string" || t === "number";
  }
}

/** Pure TS coercion mirroring server `migrate::coerce_value`. Returns the
 *  coerced JSON value, or `undefined` when the value cannot be coerced under
 *  `cast` — the caller then substitutes a (coerced) default or raises a
 *  row-named `BAD_REQUEST`, matching the server's per-row decision.
 *
 *  `toInt64` emits a decimal-string JSON value (int64 travels as a canonical
 *  decimal string on this wire — see `schema::is_valid_int64` and
 *  `FEATURE_MATRIX.md` #13); `toNumber` emits a JSON number. The other casts
 *  produce the natural JSON representation. */
function coerceValue(cast: Cast, v: unknown): unknown {
  switch (cast) {
    case "toString":
      if (typeof v === "string") return v;
      if (typeof v === "number") return String(v);
      if (typeof v === "boolean") return v ? "true" : "false";
      return undefined;
    case "toNumber": {
      if (typeof v === "string") {
        const n = Number(v);
        return Number.isFinite(n) ? n : undefined;
      }
      if (typeof v === "number") return v;
      if (typeof v === "boolean") return v ? 1 : 0;
      return undefined;
    }
    case "toInt64": {
      if (typeof v === "string") {
        // `isInt64String` validates the canonical decimal-string form and the
        // i64 range; the value passes through unchanged.
        return isInt64String(v) ? v : undefined;
      }
      if (typeof v === "number") {
        if (!Number.isInteger(v)) return undefined;
        const bi = BigInt(v);
        if (bi < -(2n ** 63n) || bi > 2n ** 63n - 1n) return undefined;
        return String(v);
      }
      return undefined;
    }
    case "toBoolean": {
      if (typeof v === "string") {
        if (v === "true" || v === "1") return true;
        if (v === "false" || v === "0") return false;
        return undefined;
      }
      if (typeof v === "number") return v !== 0;
      return undefined;
    }
  }
}

/** Rewrite every `field` reference in `expr` that equals `from` to `to`, in
 *  place — the mutating mirror of `walkValueExprFields` (server
 *  `migrate::rename_value_expr_fields`). `case.whens` predicates reuse
 *  {@link renameFilterFields} (the same rewrite `authorize` gets on the
 *  server), so a rename carries computed expressions across intact. `to` is
 *  fresh (renameField rejects an existing target), so no reference set can
 *  collide. */
function renameValueExprFields(expr: ValueExprJson, from: string, to: string): void {
  switch (expr.op) {
    case "field":
      if (expr.field === from) expr.field = to;
      return;
    case "literal":
    case "now":
      return;
    case "concat":
    case "coalesce":
      for (const part of expr.parts) {
        renameValueExprFields(part, from, to);
      }
      return;
    case "add":
    case "sub":
    case "mul":
    case "div":
      renameValueExprFields(expr.left, from, to);
      renameValueExprFields(expr.right, from, to);
      return;
    case "lower":
    case "upper":
    case "trim":
    case "cast":
      renameValueExprFields(expr.value, from, to);
      return;
    case "case":
      for (const cw of expr.whens) {
        renameFilterFields(cw.when, from, to);
        renameValueExprFields(cw.then, from, to);
      }
      renameValueExprFields(expr.otherwise, from, to);
      return;
  }
}

/** Rewrite every `field` reference in a `case.when` predicate — a port of
 *  server `migrate::rename_filter_fields`. Recurses through `and`/`or`/`not`. */
function renameFilterFields(expr: FilterExpr, from: string, to: string): void {
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
      if (expr.field === from) expr.field = to;
      return;
    case "and":
    case "or":
      for (const e of expr.exprs) {
        renameFilterFields(e, from, to);
      }
      return;
    case "not":
      renameFilterFields(expr.expr, from, to);
      return;
  }
}

/** True if any `field` reference in `expr` (including `case.when` filter
 *  fields) equals `field` — the dropField reference check. */
function valueExprReferencesField(expr: ValueExprJson, field: string): boolean {
  let referenced = false;
  walkValueExprFields(expr, (f) => {
    if (f === field) referenced = true;
  });
  return referenced;
}

/** Doc-store handle the directive functions operate through: the live
 *  `tables` map (renames/drops re-key it) plus the lazy per-table row
 *  accessor from the client core. */
export interface MigrationStore {
  tables: Map<string, Map<string, StoredRow>>;
  rowsFor(table: string): Map<string, StoredRow>;
}

type RenameFieldDirective = Extract<DirectiveJson, { op: "renameField" }>;
type RenameTableDirective = Extract<DirectiveJson, { op: "renameTable" }>;
type ChangeTypeDirective = Extract<DirectiveJson, { op: "changeType" }>;
type DropFieldDirective = Extract<DirectiveJson, { op: "dropField" }>;
type DropTableDirective = Extract<DirectiveJson, { op: "dropTable" }>;
type DropIndexDirective = Extract<DirectiveJson, { op: "dropIndex" }>;
type SetDefaultDirective = Extract<DirectiveJson, { op: "setDefault" }>;
type EvalExprDirective = Extract<DirectiveJson, { op: "evalExpr" }>;

/** Validates and applies one directive: folds the structural effect into
 *  `planned` (the working schema copy) and rewrites the in-memory doc map.
 *  Thin dispatcher — one function per directive kind below. */
export function applyMigrationDirective(
  planned: SchemaJson,
  d: DirectiveJson,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  switch (d.op) {
    case "renameField":
      return applyRenameFieldDirective(planned, d, store);
    case "renameTable":
      return applyRenameTableDirective(planned, d, store);
    case "changeType":
      return applyChangeTypeDirective(planned, d, store);
    case "dropField":
      return applyDropFieldDirective(planned, d, store);
    case "dropTable":
      return applyDropTableDirective(planned, d, store);
    case "dropIndex":
      return applyDropIndexDirective(planned, d, store);
    case "setDefault":
      return applySetDefaultDirective(planned, d, store);
    case "evalExpr":
      return applyEvalExprDirective(planned, d, store);
  }
}

/** Resolves a mutable table definition from the working schema, throwing the
 *  server-shaped `BAD_REQUEST` when the table is absent. */
function migrateTable(schema: SchemaJson, name: string): TableJson {
  const t = schema.tables[name];
  if (!t) {
    throw new RtDbError("BAD_REQUEST", `table '${name}' does not exist`);
  }
  return t;
}

function applyRenameFieldDirective(
  planned: SchemaJson,
  d: RenameFieldDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const t = migrateTable(planned, d.table);
  if (d.to in t.fields) {
    throw new RtDbError("BAD_REQUEST", `rename target '${d.table}.${d.to}' already exists`);
  }
  const ftype = t.fields[d.from];
  if (!ftype) {
    throw new RtDbError("BAD_REQUEST", `renamed field '${d.table}.${d.from}' does not exist`);
  }
  delete t.fields[d.from];
  t.fields[d.to] = ftype;
  for (const ix of t.indexes ?? []) {
    for (let i = 0; i < ix.fields.length; i++) {
      if (ix.fields[i] === d.from) ix.fields[i] = d.to;
    }
  }
  if (t.ownerField === d.from) t.ownerField = d.to;
  if (t.collaboratorsField === d.from) t.collaboratorsField = d.to;
  // ENH-028: the computed map follows the rename the way owner/collaborators
  // do — an entry KEYED on the renamed field moves to the new name (leaving
  // it keyed on `from` would fail push validation's declared-field rule on
  // the derived schema), and every expression's `field` references (including
  // `case.whens` predicates) are rewritten to read the renamed doc key.
  // Input values are unchanged by the rename, so stored computed values stay
  // correct; the next write re-stamps.
  if (t.computed !== undefined) {
    const keyed = t.computed[d.from];
    if (keyed !== undefined) {
      delete t.computed[d.from];
      t.computed[d.to] = keyed;
    }
    for (const expr of Object.values(t.computed)) {
      renameValueExprFields(expr, d.from, d.to);
    }
  }
  let affected = 0;
  for (const row of store.rowsFor(d.table).values()) {
    if (d.from in row.doc) {
      row.doc[d.to] = row.doc[d.from];
      delete row.doc[d.from];
      affected++;
    }
  }
  return { report: { op: "renameField", affectedRows: affected }, table: d.table };
}

function applyRenameTableDirective(
  planned: SchemaJson,
  d: RenameTableDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  if (d.to in planned.tables) {
    throw new RtDbError("BAD_REQUEST", `rename target table '${d.to}' already exists`);
  }
  const def = planned.tables[d.from];
  if (!def) {
    throw new RtDbError("BAD_REQUEST", `renamed table '${d.from}' does not exist`);
  }
  delete planned.tables[d.from];
  // Id references to `from` in other tables follow the rename.
  for (const other of Object.values(planned.tables)) {
    for (const [fname, ftype] of Object.entries(other.fields)) {
      if (ftype.type === "id" && ftype.table === d.from) {
        other.fields[fname] = { type: "id", table: d.to };
      }
    }
  }
  planned.tables[d.to] = def;
  const rows = store.tables.get(d.from);
  if (rows) {
    store.tables.delete(d.from);
    store.tables.set(d.to, rows);
  }
  return { report: { op: "renameTable", affectedRows: 0 }, table: d.to };
}

function applyChangeTypeDirective(
  planned: SchemaJson,
  d: ChangeTypeDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const t = migrateTable(planned, d.table);
  const oldTy = t.fields[d.field];
  if (!oldTy) {
    throw new RtDbError("BAD_REQUEST", `changed field '${d.table}.${d.field}' does not exist`);
  }
  if (!castValidFor(d.cast, oldTy)) {
    throw new RtDbError("BAD_REQUEST", `cast ${d.cast} is not valid for ${d.table}.${d.field}`);
  }
  const rows = [...store.rowsFor(d.table).values()];
  let affected = 0;
  for (const row of rows) {
    if (!(d.field in row.doc)) continue;
    affected++;
    const coerced = coerceValue(d.cast, row.doc[d.field]);
    if (coerced !== undefined) {
      row.doc[d.field] = coerced;
      continue;
    }
    if (d.default !== undefined) {
      const dv = coerceValue(d.cast, d.default);
      row.doc[d.field] = dv ?? d.default;
      continue;
    }
    throw new RtDbError(
      "BAD_REQUEST",
      `changeType cannot coerce value in ${d.table}.${row.id} (${row.doc[d.field]}) and no default given`,
    );
  }
  t.fields[d.field] = d.to;
  return { report: { op: "changeType", affectedRows: affected }, table: d.table };
}

function applyDropFieldDirective(
  planned: SchemaJson,
  d: DropFieldDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const t = migrateTable(planned, d.table);
  if (!(d.field in t.fields)) {
    throw new RtDbError("BAD_REQUEST", `dropped field '${d.table}.${d.field}' does not exist`);
  }
  // ENH-028: a computed expression reading the dropped field would dangle —
  // every future write fails its stamp. Reject, naming the computed field, so
  // the caller amends the computed map first (a push removing the entry
  // leaves stored values in place).
  if (t.computed !== undefined) {
    for (const [computedField, expr] of Object.entries(t.computed)) {
      if (valueExprReferencesField(expr, d.field)) {
        throw new RtDbError(
          "BAD_REQUEST",
          `cannot drop field '${d.table}.${d.field}': it is referenced by computed field '${d.table}.${computedField}'; drop the computed field first`,
        );
      }
    }
  }
  delete t.fields[d.field];
  for (const ix of t.indexes ?? []) {
    ix.fields = ix.fields.filter((f) => f !== d.field);
  }
  // ARC-133: `delete` rather than `= undefined` because TableJson's
  // ownerField/collaboratorsField are `?:`-optional (omitted on the wire
  // when unset, per protocol.ts); exactOptionalPropertyTypes forbids
  // assigning literal `undefined` to them. `delete` removes the key.
  if (t.ownerField === d.field) delete t.ownerField;
  if (t.collaboratorsField === d.field) delete t.collaboratorsField;
  // An entry KEYED on the dropped field goes with it: the applier removes the
  // stored key from every doc, so leaving the entry would fail push
  // validation's declared-field rule on the derived schema. An emptied map
  // drops the key entirely — the server's BTreeMap skips serialization when
  // empty, so the derived schema stays wire-identical.
  if (t.computed !== undefined) {
    delete t.computed[d.field];
    if (Object.keys(t.computed).length === 0) {
      delete t.computed;
    }
  }
  const rows = store.rowsFor(d.table);
  let affected = 0;
  for (const row of rows.values()) {
    if (!(d.field in row.doc)) continue;
    delete row.doc[d.field];
    affected++;
  }
  return { report: { op: "dropField", affectedRows: affected }, table: d.table };
}

function applyDropTableDirective(
  planned: SchemaJson,
  d: DropTableDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const def = planned.tables[d.name];
  if (!def) {
    throw new RtDbError("BAD_REQUEST", `dropped table '${d.name}' does not exist`);
  }
  const count = store.rowsFor(d.name).size;
  delete planned.tables[d.name];
  store.tables.delete(d.name);
  return { report: { op: "dropTable", affectedRows: count }, table: d.name };
}

function applyDropIndexDirective(
  planned: SchemaJson,
  d: DropIndexDirective,
  _store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const t = migrateTable(planned, d.table);
  const ix = (t.indexes ?? []).find((i) => i.name === d.name);
  if (!ix) {
    throw new RtDbError("BAD_REQUEST", `dropped index '${d.table}.${d.name}' does not exist`);
  }
  t.indexes = (t.indexes ?? []).filter((i) => i.name !== d.name);
  return { report: { op: "dropIndex", affectedRows: 0 }, table: d.table };
}

function applySetDefaultDirective(
  planned: SchemaJson,
  d: SetDefaultDirective,
  store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  const t = migrateTable(planned, d.table);
  if (!(d.field in t.fields)) {
    throw new RtDbError("BAD_REQUEST", `setDefault target '${d.table}.${d.field}' does not exist`);
  }
  let affected = 0;
  for (const row of store.rowsFor(d.table).values()) {
    if (!(d.field in row.doc)) {
      row.doc[d.field] = clone(d.value);
      affected++;
    }
  }
  return { report: { op: "setDefault", affectedRows: affected }, table: d.table };
}

function applyEvalExprDirective(
  _planned: SchemaJson,
  _d: EvalExprDirective,
  _store: MigrationStore,
): { report: DirectiveReportJson; table?: string } {
  // No SQL engine in the harness — throw rather than silently misbehave.
  // Both dual-accept arms (typed ValueExprJson and legacy raw-SQL string)
  // are unsupported here for the same reason.
  throw new RtDbError("BAD_REQUEST", "evalExpr unsupported in-memory");
}
