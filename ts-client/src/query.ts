import type { FilterExpr, Order, PaginatedResultJson, QueryJson, VectorQuery } from "./protocol.js";
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

  /** Append a db-side `filter` predicate. Composes with index/range/order/take;
   * the server validates terminal combinations. Not a terminal. */
  filter(expr: FilterExpr): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, filter: expr });
  }

  /** Full-text `search` over a declared search index. Composes only with `take`
   * (e.g. `.search("idx", "text").take(10)`); the server rejects every other
   * terminal alongside it. */
  search(index: string, query: string): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, search: { index, query } });
  }

  /** Vector-similarity `vectorSearch` over a declared vector index. The server
   * ranks by cosine distance and applies `limit`; `filter` is an eq-map over the
   * index's declared `filterFields`. Terminal — the server rejects other
   * terminals alongside it. */
  vectorSearch(
    index: string,
    vector: number[],
    opts: { limit: number; filter?: Record<string, unknown> },
  ): TableQuery<DocT, Indexes> {
    const vectorSearch: VectorQuery = {
      index,
      vector,
      limit: opts.limit,
      ...(opts.filter ? { filter: opts.filter } : {}),
    };
    return new TableQuery({ ...this.json, vectorSearch });
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

  count(): RtQuery<number> {
    return { json: { ...this.json, count: true } };
  }

  paginate(cursor: string | undefined, numItems: number): RtQuery<PaginatedResultJson> {
    return {
      json: {
        ...this.json,
        paginate: {
          cursor: cursor || undefined,
          numItems: numItems,
        },
      },
    };
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
