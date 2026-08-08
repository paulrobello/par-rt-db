import type {
  AggregateGroup,
  AggregateOp,
  AggregateSpec,
  FilterExpr,
  HybridSearchQuery,
  Order,
  PaginatedResultJson,
  QueryJson,
  VectorQuery,
} from "./protocol.js";
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
      ...(opts.filter && Object.keys(opts.filter).length > 0 ? { filter: opts.filter } : {}),
    };
    return new TableQuery({ ...this.json, vectorSearch });
  }

  /** Hybrid `hybridSearch` terminal: fuses full-text and vector ranking over the
   * same table via Reciprocal Rank Fusion. The table must declare BOTH a search
   * index and a vector index. `opts.searchIndex`/`opts.vectorIndex` optionally
   * name the indexes (auto-selected when omitted); `opts.k` is the RRF constant
   * (default 60). Terminal — the server rejects other terminals alongside it. */
  hybridSearch(
    query: string,
    vector: number[],
    limit: number,
    opts?: { searchIndex?: string; vectorIndex?: string; k?: number },
  ): TableQuery<DocT, Indexes> {
    const hybridSearch: HybridSearchQuery = {
      query,
      vector,
      limit,
      ...(opts?.searchIndex ? { searchIndex: opts.searchIndex } : {}),
      ...(opts?.vectorIndex ? { vectorIndex: opts.vectorIndex } : {}),
      ...(opts?.k ? { k: opts.k } : {}),
    };
    return new TableQuery({ ...this.json, hybridSearch });
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

  /** Distinct-values terminal: returns the unique values of the index field
   * immediately after the `eq` prefix over the matching set (an array of
   * scalar values, e.g. `["alice","bob"]`). Server rejects when no index is
   * set or the eq prefix consumes every index field; mutually exclusive with
   * every other terminal except `eq`/range bounds/`filter`. */
  distinct(): RtQuery<unknown[]> {
    return { json: { ...this.json, distinct: true } };
  }

  /** Aggregate terminal: runs `<op>` (SUM/AVG/MIN/MAX/COUNT) over the index
   * field immediately after the `eq` prefix. Without `groupBy`, returns one
   * scalar (`null` if no rows match; `0` for `count`). With `groupBy: true`,
   * groups by the index field after the eq prefix and aggregates the one after
   * that, returning `{key, value}[]` ordered by group key. `sum`/`avg` require
   * a numeric aggregate field; `count` aggregates rows and consumes no
   * aggregate field (a scalar `count` needs no index, a grouped `count` needs
   * one index field to group by). The server rejects non-numeric, no-index, or
   * no-field-beyond-prefix cases for the field-bearing ops. Mutually exclusive
   * with every other terminal except `eq`/range bounds/`filter` (which narrow
   * the matching set); `take` is also rejected — group count is capped
   * internally by MAX_TAKE. */
  aggregate(
    op: AggregateOp,
    groupBy: boolean = false,
  ): RtQuery<unknown> | RtQuery<AggregateGroup[]> {
    const spec: AggregateSpec = groupBy ? { op, groupBy: true } : { op };
    return { json: { ...this.json, aggregate: spec } };
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
