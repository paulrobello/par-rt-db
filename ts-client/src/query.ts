import type {
  AggregateGroup,
  AggregateOp,
  AggregateSpec,
  FilterExpr,
  HybridSearchQuery,
  Order,
  PaginatedResultJson,
  QueryJson,
  SearchMode,
  SearchQuery,
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

  /** Selects a declared index and an optional equality prefix (`eq`) over
   * its leading fields. Range bounds (`gt`/`gte`/`lt`/`lte`) apply to the
   * index field immediately after the `eq` prefix. */
  withIndex(index: Indexes, eq: unknown[] = []): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, index, eq });
  }

  /** Restricts the index field after the `eq` prefix to values strictly
   * greater than `value`. Requires `withIndex`. */
  gt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gt: value });
  }

  /** Restricts the index field after the `eq` prefix to values greater
   * than or equal to `value`. Requires `withIndex`. */
  gte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, gte: value });
  }

  /** Restricts the index field after the `eq` prefix to values strictly
   * less than `value`. Requires `withIndex`. */
  lt(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lt: value });
  }

  /** Restricts the index field after the `eq` prefix to values less than
   * or equal to `value`. Requires `withIndex`. */
  lte(value: unknown): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, lte: value });
  }

  /** Sets result order (`"asc"` or `"desc"`) by the selected index. */
  order(order: Order): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, order });
  }

  /** Append a db-side `filter` predicate. Composes with index/range/order/take;
   * the server validates terminal combinations. Not a terminal. */
  filter(expr: FilterExpr): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, filter: expr });
  }

  /** Field projection: each result doc keeps only the listed user fields plus
   * every `_`-prefixed key (the system fields `_id`/`_creationTime`/`_version`
   * and synthetics like `_searchSnippet` — always present; listing one is an
   * accepted no-op). Calling with no arguments (`fields()`) is the meaningful
   * system-fields-only (ids-only) view. Composes with every doc-bearing
   * terminal (get/collect/first/unique/paginate and the search family);
   * doc-less terminals (count/distinct/aggregate) are unaffected. Not a
   * terminal. Unknown names are rejected `BAD_REQUEST` by the server (and the
   * in-memory harness) at compile time. */
  fields(...names: string[]): TableQuery<DocT, Indexes> {
    return new TableQuery({ ...this.json, fields: names });
  }

  /** Full-text `search` over a declared search index. Composes only with `take`
   * (e.g. `.search("idx", "text").take(10)`); the server rejects every other
   * terminal alongside it. The optional `filter` narrows results server-side via
   * the full `FilterExpr` DSL (not to be confused with the query-level
   * `.filter()` builder, which is mutually exclusive with `search`); the
   * optional `mode` selects the match strategy — `tsquery` (default, word/stem
   * full-text) or `trgm` (case-insensitive substring, ranked by similarity);
   * the optional `snippet` (FM-31) opts each hit into a `_searchSnippet`
   * fragment with matched terms wrapped in `<mark>` (tsquery mode only) — all
   * omitted on the wire when absent so existing requests stay byte-identical. */
  search(
    index: string,
    query: string,
    opts?: { filter?: FilterExpr; mode?: SearchMode; snippet?: boolean },
  ): TableQuery<DocT, Indexes> {
    const search: SearchQuery = {
      index,
      query,
      ...(opts?.filter ? { filter: opts.filter } : {}),
      ...(opts?.mode ? { mode: opts.mode } : {}),
      ...(opts?.snippet !== undefined ? { snippet: opts.snippet } : {}),
    };
    return new TableQuery({ ...this.json, search });
  }

  /** Vector-similarity `vectorSearch` over a declared vector index. The server
   * ranks by the index's distance metric and applies `limit`; the optional
   * `filter` narrows results server-side via the full `FilterExpr` DSL (not to
   * be confused with the query-level `.filter()` builder); omitted on the wire
   * when absent so existing requests stay byte-identical. Terminal — the server
   * rejects other terminals alongside it. */
  vectorSearch(
    index: string,
    vector: number[],
    opts: { limit: number; filter?: FilterExpr },
  ): TableQuery<DocT, Indexes> {
    const vectorSearch: VectorQuery = {
      index,
      vector,
      limit: opts.limit,
      ...(opts.filter ? { filter: opts.filter } : {}),
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

  /** Terminal: returns up to `n` matching docs. */
  take(n: number): RtQuery<DocT[]> {
    return { json: { ...this.json, take: n } };
  }

  /** Terminal: returns the single matching doc, or `null` if none match.
   * The server rejects the query if more than one doc matches. */
  unique(): RtQuery<DocT | null> {
    return { json: { ...this.json, unique: true } };
  }

  /** Terminal: returns the first matching doc in the selected order, or
   * `null` if none match. */
  first(): RtQuery<DocT | null> {
    return { json: { ...this.json, first: true } };
  }

  /** Terminal: returns the count of matching docs. */
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

  /** Terminal: cursor-based pagination. `cursor` is the opaque cursor from
   * a prior page (`undefined`/empty starts from the beginning); `numItems`
   * caps the page size. Use `decodeCursor`/`encodeCursor` to inspect or
   * construct cursors. */
  paginate(cursor: string | undefined, numItems: number): RtQuery<PaginatedResultJson> {
    // ARC-133: Paginate.cursor is `?:`-optional, so include it only when set
    // (exactOptionalPropertyTypes forbids literal `undefined`). `cursor || ""`
    // collapses empty string to absence — same runtime as before.
    const c = cursor || "";
    return {
      json: {
        ...this.json,
        paginate: {
          ...(c === "" ? {} : { cursor: c }),
          numItems: numItems,
        },
      },
    };
  }

  /** Terminal: returns every matching doc (no limit). */
  collect(): RtQuery<DocT[]> {
    return { json: { ...this.json } };
  }
}

/** Per-table query surface returned by {@link createApi}. */
export interface TableApi<DocT, Indexes extends string> {
  /** Starts a new query builder for this table. */
  query(): TableQuery<DocT, Indexes>;
  /** Builds a query for the single doc with the given id, or `null` if
   * absent. */
  get(id: Id<string> | string): RtQuery<DocT | null>;
}

/**
 * Typed API client surface mapped from database schema definition S.
 */
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
