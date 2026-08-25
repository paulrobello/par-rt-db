/**
 * Query clause combination rules for the in-memory engine's `checkQueryCombinations`.
 *
 * Hand-mirrored copy of `wire-corpus/query-combinations.json` — the declarative
 * source of truth for which clauses of the read `Query` DSL may not be set
 * together (server `query::combinations`, and the equivalent evaluator in every
 * other client). `tests/wire-corpus.test.ts` asserts this table matches the JSON
 * file exactly, the same convention `errors.ts`'s `ALL_ERROR_CODES` uses for
 * `wire-corpus/error-codes.json` (ARC-017) — a straight relative import of the
 * JSON from outside `ts-client/`'s own source tree would violate the package
 * build's `rootDir` (see `tsconfig.build.json`), so the table is embedded as a
 * TS literal instead of imported. Update BOTH files together (ENH-028).
 */

import type { RtDbErrorCode } from "../errors.js";

export type QueryComboClause =
  | "get"
  | "index"
  | "eq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "order"
  | "take"
  | "unique"
  | "first"
  | "count"
  | "distinct"
  | "aggregate"
  | "paginate"
  | "filter"
  | "search"
  | "vectorSearch"
  | "hybridSearch";

export const QUERY_COMBO_CLAUSES: readonly QueryComboClause[] = [
  "get",
  "index",
  "eq",
  "gt",
  "gte",
  "lt",
  "lte",
  "order",
  "take",
  "unique",
  "first",
  "count",
  "distinct",
  "aggregate",
  "paginate",
  "filter",
  "search",
  "vectorSearch",
  "hybridSearch",
];

export interface QueryComboRule {
  id: string;
  forbid?: readonly QueryComboClause[];
  atMostOne?: readonly QueryComboClause[];
  code: RtDbErrorCode;
  message: string;
}

/** In JSON declaration order — the evaluator applies rules in this order and
 *  returns the first failing rule, so this order must not change without
 *  re-verifying against `wire-corpus/semantics/query-combo-*.json`. */
export const QUERY_COMBO_RULES: readonly QueryComboRule[] = [
  {
    id: "terminal-exclusive",
    atMostOne: ["aggregate", "count", "distinct", "first", "get", "paginate", "take", "unique"],
    code: "BAD_REQUEST",
    message: "only one terminal may be set",
  },
  {
    id: "search-mode-exclusive",
    atMostOne: ["hybridSearch", "search", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "only one search mode terminal may be set",
  },
  {
    id: "get-excludes-index",
    forbid: ["get", "index"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "eq-excludes-get",
    forbid: ["eq", "get"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-gt",
    forbid: ["get", "gt"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-gte",
    forbid: ["get", "gte"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-lt",
    forbid: ["get", "lt"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-lte",
    forbid: ["get", "lte"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-order",
    forbid: ["get", "order"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "filter-excludes-get",
    forbid: ["filter", "get"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-search",
    forbid: ["get", "search"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-vectorSearch",
    forbid: ["get", "vectorSearch"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "get-excludes-hybridSearch",
    forbid: ["get", "hybridSearch"],
    code: "BAD_REQUEST",
    message:
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
  },
  {
    id: "order-excludes-unique",
    forbid: ["order", "unique"],
    code: "BAD_REQUEST",
    message: "unique cannot be combined with take, order, distinct, or aggregate",
  },
  {
    id: "count-excludes-order",
    forbid: ["count", "order"],
    code: "BAD_REQUEST",
    message: "count cannot be combined with order",
  },
  {
    id: "distinct-excludes-order",
    forbid: ["distinct", "order"],
    code: "BAD_REQUEST",
    message: "distinct cannot be combined with order",
  },
  {
    id: "distinct-excludes-search",
    forbid: ["distinct", "search"],
    code: "BAD_REQUEST",
    message: "distinct cannot be combined with search",
  },
  {
    id: "distinct-excludes-vectorSearch",
    forbid: ["distinct", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "distinct cannot be combined with vector search",
  },
  {
    id: "distinct-excludes-hybridSearch",
    forbid: ["distinct", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "distinct cannot be combined with hybrid search",
  },
  {
    id: "aggregate-excludes-order",
    forbid: ["aggregate", "order"],
    code: "BAD_REQUEST",
    message: "aggregate cannot be combined with order",
  },
  {
    id: "aggregate-excludes-search",
    forbid: ["aggregate", "search"],
    code: "BAD_REQUEST",
    message: "aggregate cannot be combined with search",
  },
  {
    id: "aggregate-excludes-vectorSearch",
    forbid: ["aggregate", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "aggregate cannot be combined with vector search",
  },
  {
    id: "aggregate-excludes-hybridSearch",
    forbid: ["aggregate", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "aggregate cannot be combined with hybrid search",
  },
  {
    id: "gt-excludes-gte",
    forbid: ["gt", "gte"],
    code: "BAD_REQUEST",
    message: "gt and gte cannot both be set",
  },
  {
    id: "lt-excludes-lte",
    forbid: ["lt", "lte"],
    code: "BAD_REQUEST",
    message: "lt and lte cannot both be set",
  },
  {
    id: "index-excludes-vectorSearch",
    forbid: ["index", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "eq-excludes-vectorSearch",
    forbid: ["eq", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "gt-excludes-vectorSearch",
    forbid: ["gt", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "gte-excludes-vectorSearch",
    forbid: ["gte", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "lt-excludes-vectorSearch",
    forbid: ["lt", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "lte-excludes-vectorSearch",
    forbid: ["lte", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "order-excludes-vectorSearch",
    forbid: ["order", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "unique-excludes-vectorSearch",
    forbid: ["unique", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "first-excludes-vectorSearch",
    forbid: ["first", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "count-excludes-vectorSearch",
    forbid: ["count", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "paginate-excludes-vectorSearch",
    forbid: ["paginate", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "filter-excludes-vectorSearch",
    forbid: ["filter", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "take-excludes-vectorSearch",
    forbid: ["take", "vectorSearch"],
    code: "BAD_REQUEST",
    message: "vectorSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-index",
    forbid: ["hybridSearch", "index"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "eq-excludes-hybridSearch",
    forbid: ["eq", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "gt-excludes-hybridSearch",
    forbid: ["gt", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "gte-excludes-hybridSearch",
    forbid: ["gte", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-lt",
    forbid: ["hybridSearch", "lt"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-lte",
    forbid: ["hybridSearch", "lte"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-order",
    forbid: ["hybridSearch", "order"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-take",
    forbid: ["hybridSearch", "take"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-unique",
    forbid: ["hybridSearch", "unique"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "first-excludes-hybridSearch",
    forbid: ["first", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "count-excludes-hybridSearch",
    forbid: ["count", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "hybridSearch-excludes-paginate",
    forbid: ["hybridSearch", "paginate"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "filter-excludes-hybridSearch",
    forbid: ["filter", "hybridSearch"],
    code: "BAD_REQUEST",
    message: "hybridSearch cannot be combined with any other terminal",
  },
  {
    id: "index-excludes-search",
    forbid: ["index", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "eq-excludes-search",
    forbid: ["eq", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "gt-excludes-search",
    forbid: ["gt", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "gte-excludes-search",
    forbid: ["gte", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "lt-excludes-search",
    forbid: ["lt", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "lte-excludes-search",
    forbid: ["lte", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "order-excludes-search",
    forbid: ["order", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "search-excludes-unique",
    forbid: ["search", "unique"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "first-excludes-search",
    forbid: ["first", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "count-excludes-search",
    forbid: ["count", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "paginate-excludes-search",
    forbid: ["paginate", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
  {
    id: "filter-excludes-search",
    forbid: ["filter", "search"],
    code: "BAD_REQUEST",
    message:
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
  },
];
