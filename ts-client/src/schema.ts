import type {
  CaseWhenJson,
  Cast,
  DistanceMetric,
  FieldTypeJson,
  FilterExpr,
  IndexJson,
  OnDeleteAction,
  SchemaJson,
  TableJson,
  TtlDef,
  ValueExprJson,
} from "./protocol.js";

/** Branded id string. `Id<"projects">` is assignable to `string` but distinct across tables. */
export type Id<TableName extends string> = string & { readonly __idBrand: TableName };

/**
 * Branded decimal-string int64. The wire value is a JSON string of canonical
 * decimal digits (whatever `i64::from_str` on the server accepts) — JSON has
 * no 64-bit integer type, and a JS `number` cannot exactly represent the full
 * `i64` range past `Number.MAX_SAFE_INTEGER`. Branded rather than a real
 * `bigint` because this SDK is entirely schema-type-erased at runtime (see
 * `t.int64` below): a `bigint` would need a `JSON.stringify` replacer on every
 * write and schema-aware result marshaling on every read, which no other
 * validator needs today. Use `toInt64`/`fromInt64` to convert at the edges.
 */
export type Int64 = string & { readonly __int64Brand: unique symbol };

/** Convert a `bigint` or `number` into the wire representation of an `int64`
 * field (a decimal string — see `Int64`). Numbers outside the safe-integer
 * range should come in as `bigint`. */
export function toInt64(value: bigint | number): Int64 {
  return String(value) as Int64;
}

/** Convert an `Int64` wire value back into a `bigint`. */
export function fromInt64(value: Int64): bigint {
  return BigInt(value);
}

/**
 * A field validator: a runtime JSON serialization plus two phantom type params —
 * `T` (the inferred value type) and `Optional` (whether the field may be omitted).
 * `__out`/`__optional` are type-only; they never exist at runtime.
 */
export interface Validator<T, Optional extends boolean = false> {
  readonly json: FieldTypeJson;
  readonly __out?: T;
  readonly __optional?: Optional;
}

export type Infer<V> = V extends Validator<infer T, boolean> ? T : never;

function makeValidator<T, Optional extends boolean = false>(
  json: FieldTypeJson,
): Validator<T, Optional> {
  return { json };
}

function fieldsToJson(
  fields: Record<string, Validator<unknown, boolean>>,
): Record<string, FieldTypeJson> {
  const out: Record<string, FieldTypeJson> = {};
  for (const [key, validator] of Object.entries(fields)) {
    out[key] = validator.json;
  }
  return out;
}

/** Builders for the `ValueExpr` grammar (ENH-028) — the expression language a
 * `.computed(name, expr)` field is derived from (and, in its raw
 * {@link ValueExprJson} form, what migrate's typed `evalExpr` accepts). One
 * helper per wire variant, producing the wire shape directly; `ve.field`
 * reads are text extraction (numbers become `"42"`-style strings, mirroring
 * the server's `doc->>'field'`), arithmetic coerces its operands back to
 * numbers, and `concat` skips null parts. */
export const ve = {
  /** A declared field on the same table, read as text (`doc->>'field'`). */
  field: (name: string): ValueExprJson => ({ op: "field", field: name }),
  /** Any JSON literal (string/number/boolean/object/array/null). */
  literal: (value: unknown): ValueExprJson => ({ op: "literal", value }),
  /** String concatenation; null parts are skipped (all-null → `""`). */
  concat: (...parts: ValueExprJson[]): ValueExprJson => ({ op: "concat", parts }),
  add: (left: ValueExprJson, right: ValueExprJson): ValueExprJson => ({ op: "add", left, right }),
  sub: (left: ValueExprJson, right: ValueExprJson): ValueExprJson => ({ op: "sub", left, right }),
  mul: (left: ValueExprJson, right: ValueExprJson): ValueExprJson => ({ op: "mul", left, right }),
  /** Division; a zero divisor fails the write at evaluation time — guard the
   * divisor with `ve.case`/`ve.coalesce` when it may be zero. */
  div: (left: ValueExprJson, right: ValueExprJson): ValueExprJson => ({ op: "div", left, right }),
  /** First non-null part, else null. */
  coalesce: (...parts: ValueExprJson[]): ValueExprJson => ({ op: "coalesce", parts }),
  /** ASCII lowercasing of the operand's text form; null stays null. */
  lower: (value: ValueExprJson): ValueExprJson => ({ op: "lower", value }),
  /** ASCII uppercasing of the operand's text form; null stays null. */
  upper: (value: ValueExprJson): ValueExprJson => ({ op: "upper", value }),
  /** Strips leading/trailing SPACES only (not tabs/newlines); null stays
   * null. */
  trim: (value: ValueExprJson): ValueExprJson => ({ op: "trim", value }),
  /** A closed scalar coercion — `to` is one of the {@link Cast} wire values
   * (`"toString" | "toNumber" | "toInt64" | "toBoolean"`). */
  cast: (value: ValueExprJson, to: Cast): ValueExprJson => ({ op: "cast", value, to }),
  /** Current timestamp as epoch-ms (a JSON number). */
  now: (): ValueExprJson => ({ op: "now" }),
  /** First matching `when`'s `then`, else `otherwise`. Each `when` is a
   * `FilterExpr` over the doc — the same DSL `.filter()` queries use. */
  case: (whens: CaseWhenJson[], otherwise: ValueExprJson): ValueExprJson => ({
    op: "case",
    whens,
    otherwise,
  }),
};

export const t = {
  string: (): Validator<string> => makeValidator({ type: "string" }),
  number: (): Validator<number> => makeValidator({ type: "number" }),
  boolean: (): Validator<boolean> => makeValidator({ type: "boolean" }),
  null: (): Validator<null> => makeValidator({ type: "null" }),
  /** An id referencing `table`. `opts.onDelete` (FM-33) declares the action the
   * server takes on child rows when the referenced parent row is hard-deleted:
   * `"cascade"` deletes the children (recursively, soft-deleting children whose
   * own table declares `.softDelete()`), `"restrict"` blocks the parent delete
   * with a Conflict while live children exist, `"setNull"` clears the child
   * field (requires `t.optional(t.id(...))`). Legal only on a top-level or
   * optional-wrapped id, and the field needs a single-field, non-unique,
   * non-partial btree index. Omitted on the wire when absent. */
  id: <T extends string>(table: T, opts?: { onDelete?: OnDeleteAction }): Validator<Id<T>> =>
    makeValidator({
      type: "id",
      table,
      ...(opts?.onDelete ? { onDelete: opts.onDelete } : {}),
    }),
  literal: <L extends string | number | boolean>(value: L): Validator<L> =>
    makeValidator({ type: "literal", value }),
  optional: <T>(inner: Validator<T, boolean>): Validator<T | undefined, true> =>
    makeValidator({ type: "optional", inner: inner.json }),
  union: <Vs extends [Validator<unknown, boolean>, ...Validator<unknown, boolean>[]]>(
    ...variants: Vs
  ): Validator<Infer<Vs[number]>> =>
    makeValidator({ type: "union", variants: variants.map((v) => v.json) }),
  array: <T>(element: Validator<T, boolean>): Validator<T[]> =>
    makeValidator({ type: "array", element: element.json }),
  object: <S extends Record<string, Validator<unknown, boolean>>>(
    fields: S,
  ): Validator<{ [K in keyof S]: Infer<S[K]> }> =>
    makeValidator({ type: "object", fields: fieldsToJson(fields) }),
  record: <T>(value: Validator<T, boolean>): Validator<Record<string, T>> =>
    makeValidator({ type: "record", value: value.json }),
  any: (): Validator<unknown> => makeValidator({ type: "any" }),
  bytes: (): Validator<string> => makeValidator({ type: "bytes" }),
  int64: (): Validator<Int64> => makeValidator({ type: "int64" }),
  vector: (dimensions: number): Validator<number[]> =>
    makeValidator({ type: "vector", dimensions }),
};

export class TableDefinition<
  Fields extends Record<string, Validator<unknown, boolean>>,
  Indexes extends string = never,
  Stamped extends string = never,
> {
  /** Phantom type-level marker naming the fields the server stamps on this
   * table's insert paths (updatedAtField / autoIncrementField), so insert/
   * replace input types can make them optional while read types keep them
   * required. Never assigned at runtime. */
  declare readonly __stamped?: Stamped;

  constructor(
    readonly fields: Fields,
    readonly indexes: IndexJson[] = [],
    readonly ownerFieldName?: string,
    readonly collaboratorsFieldName?: string,
    readonly ttlDef?: TtlDef,
    readonly authorizeDef?: FilterExpr,
    readonly defaultsMap?: Record<string, unknown>,
    readonly softDeleteFlag?: boolean,
    readonly updatedAtFieldName?: string,
    readonly autoIncrementFieldName?: string,
    readonly computedMap?: Record<string, ValueExprJson>,
  ) {}

  index<Name extends string>(
    name: Name,
    fields: [keyof Fields & string, ...(keyof Fields & string)[]],
  ): TableDefinition<Fields, Indexes | Name, Stamped> {
    return new TableDefinition(
      this.fields,
      [...this.indexes, { name, fields: [...fields] }],
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Mark the most-recently appended btree index as `UNIQUE`. The server
   * compiles this to `CREATE UNIQUE INDEX` over the index's declared `fields`
   * (no trailing tiebreaker — uniqueness is on `fields` only). Btree-only: do
   * not chain after `searchIndex`/`vectorIndex` (the server rejects the combo). */
  unique(): TableDefinition<Fields, Indexes, Stamped> {
    return this.amendLastIndex((last) => ({ ...last, unique: true }));
  }

  /** Attach a partial-index predicate to the most-recently appended btree index.
   * The server bakes the predicate into `CREATE INDEX … WHERE` (literal SQL — no
   * bind params at DDL time). Same `FilterExpr` shape as the query-time
   * `.filter()` terminal. Btree-only. */
  where(predicate: FilterExpr): TableDefinition<Fields, Indexes, Stamped> {
    return this.amendLastIndex((last) => ({ ...last, where: predicate }));
  }

  /** Returns a new `TableDefinition` with the last-appended index replaced by
   * `amend(last)`. Throws if no index has been declared yet. The index object is
   * copied rather than mutated, so earlier `TableDefinition` instances stay
   * intact (matching the immutable style of `index`/`searchIndex`/`vectorIndex`). */
  private amendLastIndex(
    amend: (last: IndexJson) => IndexJson,
  ): TableDefinition<Fields, Indexes, Stamped> {
    if (this.indexes.length === 0) {
      throw new Error("unique()/where() require a preceding index() call");
    }
    const indexes = [...this.indexes];
    indexes[indexes.length - 1] = amend(indexes[indexes.length - 1]);
    return new TableDefinition(
      this.fields,
      indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare a full-text search index. The server tsvectorizes the (text)
   * `fields` into a GIN-indexed generated column ranked via the `search` query
   * terminal. Mirrors `index` but carries `search: true`. `language` optionally
   * selects the Postgres `regconfig` (e.g. `"english"`, `"simple"`,
   * `"spanish"`) used to tsvectorize; omitted on the wire when absent (server
   * default behaves as `english`). */
  searchIndex<Name extends string>(
    name: Name,
    fields: [keyof Fields & string, ...(keyof Fields & string)[]],
    language?: string,
  ): TableDefinition<Fields, Indexes | Name, Stamped> {
    return new TableDefinition(
      this.fields,
      [
        ...this.indexes,
        { name, fields: [...fields], search: true, ...(language ? { language } : {}) },
      ],
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare a vector (approximate nearest-neighbor) index. `field` is a single
   * `t.vector(dimensions)` field; the server stores a pgvector column ranked by the
   * configured distance `metric` via the `vectorSearch` query terminal. `filterFields`
   * are scalar fields that get indexed `f_` columns (accelerating eq on them); the
   * `vectorSearch` `filter` itself accepts the full `FilterExpr` DSL over any field,
   * not just these. `metric` selects the
   * distance function (`cosine` default, also `l2` or `ip`); the default is omitted on
   * the wire for backward compatibility. */
  vectorIndex<Name extends string>(
    name: Name,
    field: keyof Fields & string,
    dimensions: number,
    filterFields: (keyof Fields & string)[] = [],
    metric: DistanceMetric = "cosine",
  ): TableDefinition<Fields, Indexes | Name, Stamped> {
    return new TableDefinition(
      this.fields,
      [
        ...this.indexes,
        {
          name,
          fields: [field],
          vector: {
            dimensions,
            ...(filterFields.length > 0 ? { filterFields: [...filterFields] } : {}),
            ...(metric && metric !== "cosine" ? { metric } : {}),
          },
        },
      ],
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare the per-row owner field for authorization. `field` names a declared
   * string-compatible field whose value is the owning user's id. Server-enforced;
   * the client only declares it and round-trips it on the wire as `ownerField`. */
  ownerField(field: string): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      field,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare the per-row collaborators field for authorization. `field` names a
   * declared array-of-strings (or array-of-id) field whose values are additional
   * user ids that may read/mutate the row (owner OR collaborator). Server-enforced;
   * the client only declares it and round-trips it on the wire as `collaboratorsField`. */
  collaboratorsField(field: string): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      field,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare this table's document-TTL field. `field` names a declared numeric
   * field whose value is each document's absolute epoch-ms expiry; the server's
   * per-db reaper deletes rows whose value is in the past. `defaultDurationMs`
   * stamps `field` at insert time when the document omits it (after insert the
   * field is ordinary). Server-enforced; the client only declares it and
   * round-trips it on the wire as `ttl`. The server requires a single-field,
   * non-unique, non-partial btree index on `field` (declare one with `index()`
   * before/after this call). */
  ttl(field: string, defaultDurationMs?: number): TableDefinition<Fields, Indexes, Stamped> {
    const ttlDef: TtlDef = defaultDurationMs != null ? { field, defaultDurationMs } : { field };
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare the per-row authorization predicate (Model C). `predicate` is a
   * `FilterExpr` over this table's declared doc fields and the principal's
   * markers (`{"$user":true}` / `{"$email":true}`). Enforced on the same
   * read/write/subscription seams as `ownerField`; additive to it. Marker
   * values are valid only here — client `.filter()` queries reject them.
   * Server-enforced; the client only declares it and round-trips it on the wire
   * as `authorize`. */
  authorize(predicate: FilterExpr): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      predicate,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare field-level default values (FM-32): every key an inserted /
   * replaced document omits is stamped from `map` (client-provided values
   * always win; patch and upsert-update never re-apply). Runs after the ttl
   * default stamp server-side, so a `ttl(field, durationMs)` on the same field
   * wins over a `defaults` entry. Push-time-validated server-side (each key
   * must be a declared field of this table, values non-null and matching the
   * field's type); the client only declares it and round-trips it on the wire
   * as `defaults`, omitted when the table declares none. */
  defaults(map: Record<string, unknown>): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      map,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Opt into soft delete (FM-33): `delete`/`deleteByQuery` stamp an internal
   * `deleted_at` instead of removing the row. A soft-deleted row is invisible
   * to every read terminal, eq-lookup (`expectAbsent`/`upsert`), and unique
   * index, and an `undelete` mutation step restores it. The TTL reaper always
   * hard-deletes. Server-enforced; the client only declares it and round-trips
   * it on the wire as `softDelete: true`, omitted when unset. */
  softDelete(): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      true,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare the server-stamped last-write field (FM-36). `field` names a
   * declared `number` or `int64` field the server stamps with the current
   * epoch-ms on every version-bumping write — insert, patch, replace, upsert
   * (both branches), patchByQuery, and cascade setNull — overwriting any
   * client-supplied value (a JSON number on `number`, a decimal string on
   * `int64`). The stamp sits in the same authority family as the ttl
   * `defaultDurationMs` stamp (before field defaults fill), and the field
   * must differ from `ttl.field`. No index is required on the field.
   * Server-enforced; the client only declares it and round-trips it on the
   * wire as `updatedAtField`. */
  updatedAtField<F extends keyof Fields & string>(
    field: F,
  ): TableDefinition<Fields, Indexes, Stamped | F> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      field,
      this.autoIncrementFieldName,
      this.computedMap,
    );
  }

  /** Declare the server-assigned per-table counter (FM-37). `field` names a
   * declared `int64` field the server stamps with the next value of a
   * per-table Postgres sequence on insert (and upsert's insert branch) —
   * overwriting any client-supplied value, and winning over a `defaults`
   * entry on the same field — as a decimal string (the int64 wire
   * convention). After insert the field is immutable: a patch or replace
   * that changes the stored value is rejected (round-tripping the equal
   * value is allowed). The field must be declared `int64` exactly and must
   * differ from `ttl.field` and `updatedAtField`; it is legal in a unique
   * index (the ticket-number guarantee), and gaps are possible on
   * rolled-back transactions (sequences are monotonic, not gap-free).
   * Server-enforced; the client only declares it and round-trips it on the
   * wire as `autoIncrementField`. */
  autoIncrementField<F extends keyof Fields & string>(
    field: F,
  ): TableDefinition<Fields, Indexes, Stamped | F> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      field,
      this.computedMap,
    );
  }

  /** Declare a server-computed field (ENH-028): `field` names a declared
   * field whose value the server re-evaluates from `expr` on EVERY write —
   * insert, patch, replace, upsert (both branches), patchByQuery, and cascade
   * setNull — overwriting any client-supplied value (the `ownerField`
   * authority model). A null result REMOVES the key, so an optional computed
   * field is absent rather than stored-null when its expression yields null.
   * Build `expr` with the {@link ve} helpers. Because the value lands in a
   * typed column, a computed field is indexable and orderable like any other.
   * Push-time-validated server-side (the key must be a declared,
   * non-stamped field; referenced fields declared and non-computed; a
   * statically-known result kind must fit the field's type); the client only
   * declares it and round-trips it on the wire as `computed`, omitted when
   * the table declares none. */
  computed(field: string, expr: ValueExprJson): TableDefinition<Fields, Indexes, Stamped> {
    return new TableDefinition(
      this.fields,
      this.indexes,
      this.ownerFieldName,
      this.collaboratorsFieldName,
      this.ttlDef,
      this.authorizeDef,
      this.defaultsMap,
      this.softDeleteFlag,
      this.updatedAtFieldName,
      this.autoIncrementFieldName,
      { ...this.computedMap, [field]: expr },
    );
  }

  toJSON(): TableJson {
    const json: TableJson = { fields: fieldsToJson(this.fields) };
    if (this.indexes.length > 0) {
      json.indexes = this.indexes;
    }
    if (this.ownerFieldName) {
      json.ownerField = this.ownerFieldName;
    }
    if (this.collaboratorsFieldName) {
      json.collaboratorsField = this.collaboratorsFieldName;
    }
    if (this.ttlDef) {
      json.ttl = this.ttlDef;
    }
    if (this.updatedAtFieldName) {
      json.updatedAtField = this.updatedAtFieldName;
    }
    if (this.autoIncrementFieldName) {
      json.autoIncrementField = this.autoIncrementFieldName;
    }
    if (this.authorizeDef) {
      json.authorize = this.authorizeDef;
    }
    if (this.defaultsMap && Object.keys(this.defaultsMap).length > 0) {
      json.defaults = this.defaultsMap;
    }
    if (this.computedMap && Object.keys(this.computedMap).length > 0) {
      json.computed = this.computedMap;
    }
    if (this.softDeleteFlag) {
      json.softDelete = true;
    }
    return json;
  }
}

/** Declare one table from a field-name → validator map (e.g. `t.string()`),
 * then chain `.index()`/`.searchIndex()`/`.vectorIndex()`/`.ownerField()`/
 * `.collaboratorsField()`/`.authorize()`/`.ttl()`/`.defaults()`/`.softDelete()`/
 * `.updatedAtField()`/`.autoIncrementField()`/`.computed()`.
 * Both the runtime schema pushed to the server and the inferred TS document
 * types derive from this one declaration — there is no codegen. */
export function defineTable<Fields extends Record<string, Validator<unknown, boolean>>>(
  fields: Fields,
): TableDefinition<Fields> {
  return new TableDefinition(fields);
}

export class SchemaDefinition<Tables extends Record<string, TableDefinition<any, string, any>>> {
  constructor(readonly tables: Tables) {}

  toJSON(): SchemaJson {
    const tables: Record<string, TableJson> = {};
    for (const [name, table] of Object.entries(this.tables)) {
      tables[name] = table.toJSON();
    }
    return { tables };
  }
}

/** Declare the whole schema from a table-name → `TableDefinition` map. Pass
 * the result to `createApi(schema)` for the typed query/mutation builders and
 * to `admin.pushSchema(db, schema)` to push it (additive DDL only —
 * destructive changes go through `Migration`). */
export function defineSchema<Tables extends Record<string, TableDefinition<any, string, any>>>(
  tables: Tables,
): SchemaDefinition<Tables> {
  return new SchemaDefinition(tables);
}

// ---- Type-level document derivation ----

export interface SystemFields<TableName extends string> {
  _id: Id<TableName>;
  _creationTime: number;
  _version: number;
}

type OptionalFieldKeys<Fields> = {
  [K in keyof Fields]: Fields[K] extends Validator<unknown, true> ? K : never;
}[keyof Fields];

type RequiredFieldKeys<Fields> = Exclude<keyof Fields, OptionalFieldKeys<Fields>>;

/** The user-facing shape of a table's document fields (system fields excluded). */
export type DocFields<Fields extends Record<string, Validator<unknown, boolean>>> = {
  [K in RequiredFieldKeys<Fields>]: Infer<Fields[K]>;
} & {
  [K in OptionalFieldKeys<Fields>]?: Infer<Fields[K]>;
};

export type TableNames<S extends SchemaDefinition<any>> = keyof S["tables"] & string;

type FieldsOf<S extends SchemaDefinition<any>, T extends TableNames<S>> =
  S["tables"][T] extends TableDefinition<infer F, string, any> ? F : never;

export type IndexNamesOf<S extends SchemaDefinition<any>, T extends TableNames<S>> =
  S["tables"][T] extends TableDefinition<any, infer I, any> ? I : never;

/** The table's server-stamped insert fields (`updatedAtField` /
 * `autoIncrementField`), carried on the table type's phantom marker. */
type StampedFieldsOf<S extends SchemaDefinition<any>, T extends TableNames<S>> =
  S["tables"][T] extends TableDefinition<any, string, infer ST> ? ST : never;

/** Insert/replace input keys that must be supplied: every non-optional field
 * except the server-stamped ones (a stamped field may be omitted — and any
 * supplied value is overwritten). */
type InsertRequiredKeys<Fields, Stamped extends string> = {
  [K in keyof Fields]: Fields[K] extends Validator<unknown, true>
    ? never
    : K extends Stamped
      ? never
      : K;
}[keyof Fields];

/** A read document: declared fields plus the merged system fields. */
export type Doc<S extends SchemaDefinition<any>, T extends TableNames<S>> = DocFields<
  FieldsOf<S, T>
> &
  SystemFields<T>;

/** An insert/replace input: declared fields only, no system fields. Fields
 * the server stamps (updatedAtField / autoIncrementField) are optional —
 * omission is accepted and stamped, a supplied value is overwritten — while
 * they stay required in the read type {@link Doc} (every stored doc carries
 * them). */
export type WithoutSystemFields<S extends SchemaDefinition<any>, T extends TableNames<S>> = {
  [K in InsertRequiredKeys<FieldsOf<S, T>, StampedFieldsOf<S, T>>]: Infer<FieldsOf<S, T>[K]>;
} & {
  [K in Exclude<
    keyof FieldsOf<S, T>,
    InsertRequiredKeys<FieldsOf<S, T>, StampedFieldsOf<S, T>>
  >]?: Infer<FieldsOf<S, T>[K]>;
};
