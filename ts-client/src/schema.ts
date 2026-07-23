import type { FieldTypeJson, IndexJson, SchemaJson, TableJson } from "./protocol.js";

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

export function toInt64(value: bigint | number): Int64 {
  return String(value) as Int64;
}

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

export const t = {
  string: (): Validator<string> => makeValidator({ type: "string" }),
  number: (): Validator<number> => makeValidator({ type: "number" }),
  boolean: (): Validator<boolean> => makeValidator({ type: "boolean" }),
  null: (): Validator<null> => makeValidator({ type: "null" }),
  id: <T extends string>(table: T): Validator<Id<T>> => makeValidator({ type: "id", table }),
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
};

export class TableDefinition<
  Fields extends Record<string, Validator<unknown, boolean>>,
  Indexes extends string = never,
> {
  constructor(
    readonly fields: Fields,
    readonly indexes: IndexJson[] = [],
  ) {}

  index<Name extends string>(
    name: Name,
    fields: [keyof Fields & string, ...(keyof Fields & string)[]],
  ): TableDefinition<Fields, Indexes | Name> {
    return new TableDefinition(this.fields, [...this.indexes, { name, fields: [...fields] }]);
  }

  /** Declare a full-text search index. The server tsvectorizes the (text)
   * `fields` into a GIN-indexed generated column ranked via the `search` query
   * terminal. Mirrors `index` but carries `search: true`. */
  searchIndex<Name extends string>(
    name: Name,
    fields: [keyof Fields & string, ...(keyof Fields & string)[]],
  ): TableDefinition<Fields, Indexes | Name> {
    return new TableDefinition(this.fields, [
      ...this.indexes,
      { name, fields: [...fields], search: true },
    ]);
  }

  toJSON(): TableJson {
    const json: TableJson = { fields: fieldsToJson(this.fields) };
    if (this.indexes.length > 0) {
      json.indexes = this.indexes;
    }
    return json;
  }
}

export function defineTable<Fields extends Record<string, Validator<unknown, boolean>>>(
  fields: Fields,
): TableDefinition<Fields> {
  return new TableDefinition(fields);
}

export class SchemaDefinition<Tables extends Record<string, TableDefinition<any, string>>> {
  constructor(readonly tables: Tables) {}

  toJSON(): SchemaJson {
    const tables: Record<string, TableJson> = {};
    for (const [name, table] of Object.entries(this.tables)) {
      tables[name] = table.toJSON();
    }
    return { tables };
  }
}

export function defineSchema<Tables extends Record<string, TableDefinition<any, string>>>(
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
  S["tables"][T] extends TableDefinition<infer F, string> ? F : never;

export type IndexNamesOf<S extends SchemaDefinition<any>, T extends TableNames<S>> =
  S["tables"][T] extends TableDefinition<any, infer I> ? I : never;

/** A read document: declared fields plus the merged system fields. */
export type Doc<S extends SchemaDefinition<any>, T extends TableNames<S>> = DocFields<
  FieldsOf<S, T>
> &
  SystemFields<T>;

/** An insert/patch input: declared fields only, no system fields. */
export type WithoutSystemFields<
  S extends SchemaDefinition<any>,
  T extends TableNames<S>,
> = DocFields<FieldsOf<S, T>>;
