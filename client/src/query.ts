import type { Order, QueryJson } from "./protocol.js";
import type { Doc, Id, IndexNamesOf, SchemaDefinition, TableNames } from "./schema.js";

/** A finished query carrying a phantom `Result` type used by the client/hooks. */
export interface RtQuery<Result> {
  readonly json: QueryJson;
  readonly __result?: Result;
}

/** Chainable builder for one table. `DocT` is the read-doc type; `Indexes` the index-name union. */
export class TableQuery<DocT, Indexes extends string> {
  constructor(private readonly json: QueryJson) {}

  withIndex(index: Indexes, eq: unknown[] = []): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, index, eq });
  }

  gt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gt: value });
  }

  gte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gte: value });
  }

  lt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lt: value });
  }

  lte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lte: value });
  }

  order(order: Order): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, order });
  }

  take(n: number): RtQuery<DocT[]> {
    return { json: { ...this.json, take: n } };
  }

  unique(): RtQuery<DocT | null> {
    return { json: { ...this.json, unique: true } };
  }

  first(): RtQuery<DocT | null> {
    return { json: { ...this.json, first: true } };
  }

  collect(): RtQuery<DocT[]> {
    return { json: { ...this.json } };
  }
}

export interface TableApi<DocT, Indexes extends string> {
  query(): TableQuery<DocT, Indexes>;
  get(id: Id<string> | string): RtQuery<DocT | null>;
}

export type ClientApi<S extends SchemaDefinition<any>> = {
  [T in TableNames<S>]: TableApi<Doc<S, T>, IndexNamesOf<S, T>>;
};

/** Builds a per-table typed query surface from a schema definition. */
export function createApi<S extends SchemaDefinition<any>>(schema: S): ClientApi<S> {
  const api: Record<string, TableApi<unknown, string>> = {};
  for (const table of Object.keys(schema.tables)) {
    api[table] = {
      query: () => new TableQuery<unknown, string>({ table }),
      get: (id: string) => ({ json: { table, get: id } }),
    };
  }
  return api as ClientApi<S>;
}
