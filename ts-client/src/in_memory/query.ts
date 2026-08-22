/**
 * Query engine for the in-memory harness (mirrors
 * `rust-client/src/in_memory/query.rs`): the `executeQuery` dispatcher, the
 * per-terminal executors, and the index/cursor/aggregate/search helpers they
 * share.
 *
 * `executeQuery` is a thin dispatcher — combination guards
 * (`checkQueryCombinations`), scan preparation (`prepareScan`), the row scan
 * (`fetchFilteredRows`), the sort (`sortFilteredRows`), then one executor per
 * terminal (`executeGetTerminal`, `executeVectorSearchTerminal`,
 * `executeHybridSearchTerminal`, `executeSearchTerminal`,
 * `executeCountTerminal`, `executeDistinctTerminal`,
 * `executeAggregateTerminal`, `executePaginateTerminal`,
 * `executeCollectTerminal`) — the same decomposition the rust and python
 * engines carry. Table access goes through the lazy `rowsFor` accessor the
 * client core passes in.
 */

import { RtDbError } from "../errors.js";
import { decodeCursor, encodeCursor } from "../pagination.js";
import type {
  AggregateOp,
  IndexJson,
  Order,
  Paginate,
  PaginatedResultJson,
  QueryJson,
  TableJson,
} from "../protocol.js";
import type { StoredRow } from "./store.js";
import {
  coerceIndexValue,
  evalFilterExpr,
  type FieldMap,
  indexColumnType,
  isPlainObject,
  type PgType,
  validateFilter,
} from "./validate.js";

const MAX_TAKE = 4096;

/** Lowercase a value to FTS-indexable text. Mirrors the text the server feeds
 *  into a search index's generated tsvector for a declared field. */
function ftsStringify(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v;
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  return JSON.stringify(v);
}

/** Split text into lowercase word tokens — an approximation of the lexemes
 *  `websearch_to_tsquery` produces, close enough for match/no-match test parity.
 *  Stemming/stopwords are deliberately not replicated (a deterministic stand-in
 *  is sufficient; exact `ts_rank` ordering is out of scope for the harness). */
function ftsTokens(s: string): string[] {
  return s.toLowerCase().match(/[a-z0-9]+/g) ?? [];
}

/** One `or`-separated alternative of a websearch-syntax query: the positive
 *  plain terms and phrases that must ALL be present (AND), plus the terms and
 *  phrases that must be absent (`-term` / `-"a phrase"` NOT). A query with no
 *  bare `or` parses to a single alternative. */
interface WebsearchAlt {
  terms: string[];
  phrases: string[][];
  excludedTerms: string[];
  excludedPhrases: string[][];
}

/** Parse `websearch_to_tsquery` syntax (FM-31): quoted phrases ("a b") require
 *  adjacency, a bare case-insensitive `or` (outside quotes) splits alternatives,
 *  `-term`/`-"phrase"` negates, and remaining plain terms stay AND. Constructs
 *  the harness can't express exactly (stemming, stopword dropping, tsquery
 *  precedence between AND/OR) over-approximate — adjacency and exclusion are
 *  the observable behaviors tests pin. */
function parseWebsearchQuery(q: string): WebsearchAlt[] {
  const alts: WebsearchAlt[] = [{ terms: [], phrases: [], excludedTerms: [], excludedPhrases: [] }];
  // `-?` prefix, then either a double-quoted phrase or a bare whitespace-free
  // token. The phrase branch must come first so `-"a b"` stays one token.
  const tokenRe = /(-?)(?:"([^"]*)"|(\S+))/g;
  for (let m = tokenRe.exec(q); m !== null; m = tokenRe.exec(q)) {
    const negated = m[1] === "-";
    if (m[2] !== undefined) {
      const words = ftsTokens(m[2]);
      if (words.length === 0) continue;
      (negated ? alts[alts.length - 1].excludedPhrases : alts[alts.length - 1].phrases).push(words);
    } else if (m[3] !== undefined) {
      const word = m[3].toLowerCase();
      if (word === "or") {
        alts.push({ terms: [], phrases: [], excludedTerms: [], excludedPhrases: [] });
        continue;
      }
      const words = ftsTokens(m[3]);
      if (words.length === 0) continue;
      if (negated) alts[alts.length - 1].excludedTerms.push(...words);
      else alts[alts.length - 1].terms.push(...words);
    }
  }
  return alts;
}

/** True when `phrase` appears in `tokens` as a consecutive run (the adjacency
 *  a quoted websearch phrase requires), case-normalized upstream by ftsTokens. */
function tokensContainRun(tokens: string[], phrase: string[]): boolean {
  if (phrase.length === 0 || tokens.length < phrase.length) return false;
  const last = tokens.length - phrase.length;
  for (let i = 0; i <= last; i++) {
    let ok = true;
    for (let j = 0; j < phrase.length; j++) {
      if (tokens[i + j] !== phrase[j]) {
        ok = false;
        break;
      }
    }
    if (ok) return true;
  }
  return false;
}

function altMatches(alt: WebsearchAlt, docTokens: string[]): boolean {
  for (const t of alt.excludedTerms) if (docTokens.includes(t)) return false;
  for (const p of alt.excludedPhrases) if (tokensContainRun(docTokens, p)) return false;
  for (const t of alt.terms) if (!docTokens.includes(t)) return false;
  for (const p of alt.phrases) if (!tokensContainRun(docTokens, p)) return false;
  if (alt.terms.length + alt.phrases.length === 0) {
    // A pure-negation alternative (`-term` alone) mirrors `!term`: it matches
    // every doc its exclusions don't rule out. A fully empty alternative
    // (stray `or`) matches nothing.
    return alt.excludedTerms.length + alt.excludedPhrases.length > 0;
  }
  return true;
}

/** Server-fixed word bound for harness snippets — mirrors the ts_headline
 *  `MaxWords=35` option the server pins for `snippet: true` (FM-31). */
const SNIPPET_MAX_WORDS = 35;

/** Snippet stand-in for the server's `ts_headline(<mark>, MaxWords=35)`: a
 *  window of ≤35 original-case words around the first matched term (or the
 *  doc's leading words when nothing marks cleanly), each matched term wrapped
 *  in `<mark>…</mark>`. Shape parity only — never byte-compared to Postgres. */
function buildSearchSnippet(source: string, matchTerms: Set<string>): string {
  const words = source.match(/[A-Za-z0-9]+/g) ?? [];
  let first = words.findIndex((w) => matchTerms.has(w.toLowerCase()));
  if (first === -1) first = 0;
  const start = Math.max(0, first - 5);
  return words
    .slice(start, start + SNIPPET_MAX_WORDS)
    .map((w) => (matchTerms.has(w.toLowerCase()) ? `<mark>${w}</mark>` : w))
    .join(" ");
}

/** `null`-sorts-last comparison for one sort key. JS relational ops order
 *  numbers and strings; booleans coerce too. Nulls sort last (asc) / first
 *  (desc, via the caller negating the result) — Postgres's default. When `pg`
 *  is `"int64"`, operands are parsed as `BigInt` so decimal-string values sort
 *  and range numerically (no 2^53 limit) instead of lexicographically. */
function compareIndexValues(a: unknown, b: unknown, pg?: PgType): number {
  const aNull = a === null || a === undefined;
  const bNull = b === null || b === undefined;
  if (aNull && bNull) {
    return 0;
  }
  if (aNull) {
    return 1;
  }
  if (bNull) {
    return -1;
  }
  if (pg === "int64") {
    // Both operands are decimal-string int64 values (validated by
    // `coerceIndexValue` or stored as the canonical form on insert), so the
    // `BigInt()` parse is total — no try/catch needed.
    const an = BigInt(a as string);
    const bn = BigInt(b as string);
    if (an < bn) {
      return -1;
    }
    if (an > bn) {
      return 1;
    }
    return 0;
  }
  const av = a as number | string;
  const bv = b as number | string;
  if (av < bv) {
    return -1;
  }
  if (av > bv) {
    return 1;
  }
  return 0;
}

/** Applies one aggregate op over a non-empty `values` array. Mirrors the SQL
 *  semantics: SUM/AVG require all entries numeric; MIN/MAX pick the smallest/
 *  largest per `compareIndexValues` so a string field's MIN/MAX matches Postgres
 *  lexicographic ordering, unless `pg === "int64"` in which case both ordering
 *  and numeric reduction parse the decimal strings (server `SUM(bigint)`/
 *  `AVG(bigint)` return Postgres `numeric` → JSON number, so `Number()` is the
 *  correct projection — accepted precision loss past 2^53). AVG returns the
 *  arithmetic mean (no rounding). */
function applyAggregate(op: AggregateOp, values: unknown[], pg?: PgType): unknown {
  switch (op) {
    case "count":
      // COUNT(*) over the matching set — consumes no aggregate field, so `pg`
      // is irrelevant and `values` is one entry per counted row.
      return values.length;
    case "sum":
      if (pg === "int64") {
        return values.reduce<number>((acc, v) => acc + Number(v), 0);
      }
      return values.reduce<number>((acc, v) => acc + (v as number), 0);
    case "avg":
      if (pg === "int64") {
        return values.reduce<number>((acc, v) => acc + Number(v), 0) / values.length;
      }
      return values.reduce<number>((acc, v) => acc + (v as number), 0) / values.length;
    case "min":
      return values.reduce((best, v) => (compareIndexValues(best, v, pg) <= 0 ? best : v));
    case "max":
      return values.reduce((best, v) => (compareIndexValues(best, v, pg) >= 0 ? best : v));
  }
}

/** Resolves an index definition from a table, throwing the server-shaped
 *  `BAD_REQUEST` when the name is unknown. */
export function requireIndex(tableDef: TableJson, name: string): IndexJson {
  const index = tableDef.indexes?.find((idx) => idx.name === name);
  if (!index) {
    throw new RtDbError("BAD_REQUEST", `index '${name}' not found`);
  }
  return index;
}

/** Merges a stored row with its system fields — a port of server `merge_doc`. */
export function mergeDoc(row: StoredRow): Record<string, unknown> {
  return { ...row.doc, _id: row.id, _creationTime: row.createdAt, _version: row.version };
}

/** Everything the row scan needs besides the query itself: the resolved index,
 *  the type-checked eq prefix, and the coerced range bounds. Produced once by
 *  `prepareScan`, consumed by `fetchFilteredRows` / `sortFilteredRows` /
 *  `executePaginateTerminal`. */
interface ScanPlan {
  indexDef: IndexJson | null;
  typedEq: unknown[];
  rangeField: string | null;
  rangeFieldPg: PgType | null;
  gt: unknown;
  gte: unknown;
  lt: unknown;
  lte: unknown;
}

/** The always-included system fields — listing one in a `fields` projection is
 *  an accepted no-op (server `validate_projection`'s `SYSTEM_FIELDS`). */
const PROJECTION_SYSTEM_FIELDS = new Set(["_id", "_creationTime", "_version"]);

/** Validate a `fields` projection against the table: every name must be a
 *  declared field or one of the system fields (`_id`/`_creationTime`/
 *  `_version` — always included, so listing them is an allowed no-op). Anything
 *  else — including typo'd system names and other `_`-prefixed names — is
 *  `BAD_REQUEST` at compile time, the same gate the server runs in
 *  `compile_query` (and `/explain`). `[]` (system fields only) validates
 *  trivially. */
function validateProjection(tableDef: TableJson, fields: string[]): void {
  for (const name of fields) {
    if (PROJECTION_SYSTEM_FIELDS.has(name) || name in tableDef.fields) continue;
    throw new RtDbError("BAD_REQUEST", `unknown projection field '${name}'`);
  }
}

/** Map `fn` over every doc of an executed result. The result shape is taken
 *  from the query's terminal (mirroring the server's match over the
 *  `QueryResult` enum), never sniffed from the value — a grouped aggregate's
 *  `[{key,value}]` rows must not be mistaken for a docs array. Doc-less
 *  terminals (`count`/`distinct`/`aggregate`) pass through unchanged, which is
 *  what makes them unaffected by projection by construction. */
function mapResultDocs(result: unknown, q: QueryJson, fn: (doc: unknown) => unknown): unknown {
  if (q.paginate !== undefined) {
    if (isPlainObject(result) && Array.isArray(result.docs)) {
      return { ...result, docs: result.docs.map(fn) };
    }
    return result;
  }
  if (q.count || q.distinct || q.aggregate !== undefined) {
    return result;
  }
  if (q.get !== undefined || q.unique || q.first) {
    return fn(result);
  }
  // collect / search / vectorSearch / hybridSearch: an array of docs.
  return Array.isArray(result) ? result.map(fn) : result;
}

/** Apply a `fields` projection to one doc: keep its `_`-prefixed keys (exactly
 *  the system fields plus synthetics like `_searchSnippet` — user fields can
 *  never be `_`-prefixed, `validateDoc` rejects them at write time) and the
 *  listed user fields; drop every other user field. The rebuild is
 *  delete-free, so key order is preserved and canonical output stays stable
 *  across subscription re-runs. */
function projectDoc(doc: unknown, fields: string[]): unknown {
  if (!isPlainObject(doc)) {
    return doc;
  }
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(doc)) {
    if (key.startsWith("_") || fields.includes(key)) {
      out[key] = value;
    }
  }
  return out;
}

/** Apply a `Query.fields` projection to an executed result — a port of server
 *  `project_result`. Sorting, cursors, and snippets were computed on the
 *  unprojected rows inside the terminals, so cursors still work and
 *  `_searchSnippet` survives. */
function projectResult(result: unknown, fields: string[], q: QueryJson): unknown {
  return mapResultDocs(result, q, (doc) => projectDoc(doc, fields));
}

/** Strip the volatile `_version` from every doc of an executed result — the
 *  projected-subscription diff's pre-comparison step (a port of server
 *  `diff_canonical`'s stripping half; the caller then canonicalizes).
 *  `_version` bumps on every write, so an unstripped comparison would push on
 *  any member write even when no projected field changed. Pushed payloads
 *  still carry `_version`; only change detection ignores it. */
export function stripVersionForDiff(result: unknown, q: QueryJson): unknown {
  const stripDoc = (doc: unknown): unknown => {
    if (!isPlainObject(doc)) {
      return doc;
    }
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(doc)) {
      if (key !== "_version") {
        out[key] = value;
      }
    }
    return out;
  };
  return mapResultDocs(result, q, stripDoc);
}

/**
 * One-shot query — same shape as the http client's `query`. Thin dispatcher:
 * guards, standalone terminals, then the shared scan → per-terminal executors.
 * Projection validation runs before every early return so all terminals
 * (including `get`) reject unknown field names, and the projection itself is
 * applied at this one seam — every doc-bearing terminal's rows flow back
 * through here (mirrors server `execute_query`).
 */
export function executeQuery(
  q: QueryJson,
  tableDef: TableJson,
  rowsFor: (table: string) => Map<string, StoredRow>,
): unknown {
  if (q.fields !== undefined) {
    validateProjection(tableDef, q.fields);
  }
  const result = executeUnprojected(q, tableDef, rowsFor);
  return q.fields === undefined ? result : projectResult(result, q.fields, q);
}

function executeUnprojected(
  q: QueryJson,
  tableDef: TableJson,
  rowsFor: (table: string) => Map<string, StoredRow>,
): unknown {
  const eq = q.eq ?? [];
  const hasRange =
    q.gt !== undefined || q.gte !== undefined || q.lt !== undefined || q.lte !== undefined;

  if (q.get !== undefined) {
    return executeGetTerminal(q, eq, hasRange, rowsFor);
  }

  checkQueryCombinations(q);

  if (q.vectorSearch !== undefined) {
    return executeVectorSearchTerminal(q, tableDef, eq, hasRange, rowsFor);
  }

  if (q.hybridSearch !== undefined) {
    return executeHybridSearchTerminal(q, eq, hasRange);
  }

  if (q.search !== undefined) {
    return executeSearchTerminal(q, tableDef, eq, hasRange, rowsFor);
  }

  const plan = prepareScan(q, tableDef, eq, hasRange);
  const filtered = fetchFilteredRows(q, plan, rowsFor, tableDef.fields);

  if (q.count) {
    return executeCountTerminal(filtered);
  }

  if (q.distinct) {
    return executeDistinctTerminal(tableDef, plan.indexDef, plan.typedEq.length, filtered);
  }

  if (q.aggregate !== undefined) {
    return executeAggregateTerminal(q, tableDef, plan.indexDef, plan.typedEq.length, filtered);
  }

  const dir: Order = q.order ?? "asc";
  sortFilteredRows(filtered, tableDef, plan, dir);

  if (q.paginate !== undefined) {
    return executePaginateTerminal(q.paginate, tableDef, filtered, plan, dir);
  }

  return executeCollectTerminal(q, filtered);
}

/** Conflicting-terminal guards, in the server's validation order: each
 *  terminal rejects the peers it cannot compose with, then the range-bound and
 *  take-cap checks apply to every remaining shape. */
function checkQueryCombinations(q: QueryJson): void {
  if (
    q.unique &&
    (q.take !== undefined || q.order !== undefined || q.distinct || q.aggregate !== undefined)
  ) {
    throw new RtDbError(
      "BAD_REQUEST",
      "unique cannot be combined with take, order, distinct, or aggregate",
    );
  }
  if (q.first && q.unique) {
    throw new RtDbError("BAD_REQUEST", "first cannot be combined with unique");
  }
  if (q.first && q.take !== undefined) {
    throw new RtDbError("BAD_REQUEST", "first cannot be combined with take");
  }
  if (q.first && q.distinct) {
    throw new RtDbError("BAD_REQUEST", "first cannot be combined with distinct");
  }
  if (q.first && q.aggregate !== undefined) {
    throw new RtDbError("BAD_REQUEST", "first cannot be combined with aggregate");
  }
  if (q.count && q.unique) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with unique");
  }
  if (q.count && q.take !== undefined) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with take");
  }
  if (q.count && q.first) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with first");
  }
  if (q.count && q.order !== undefined) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with order");
  }
  if (q.count && q.distinct) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with distinct");
  }
  if (q.count && q.aggregate !== undefined) {
    throw new RtDbError("BAD_REQUEST", "count cannot be combined with aggregate");
  }
  // `distinct` is a standalone terminal like `count`: it rejects every other
  // terminal except `index`/`eq`/range bounds/`filter` (which compose by
  // narrowing the matching set). The `get`/`unique`/`first`/`count` peers
  // above already throw on `+distinct`; this branch covers the rest.
  if (q.distinct) {
    if (q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with take");
    }
    if (q.order !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with order");
    }
    if (q.paginate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with paginate");
    }
    if (q.search !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with search");
    }
    if (q.vectorSearch !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with vector search");
    }
    if (q.hybridSearch !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with hybrid search");
    }
    if (q.aggregate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "distinct cannot be combined with aggregate");
    }
  }
  // `aggregate` is a standalone terminal like `distinct`: it rejects every
  // other terminal except `index`/`eq`/range bounds/`filter`. The
  // `get`/`unique`/`first`/`count`/`distinct` peers above already throw on
  // `+aggregate`; this branch covers the rest.
  if (q.aggregate !== undefined) {
    if (q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with take");
    }
    if (q.order !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with order");
    }
    if (q.paginate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with paginate");
    }
    if (q.search !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with search");
    }
    if (q.vectorSearch !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with vector search");
    }
    if (q.hybridSearch !== undefined) {
      throw new RtDbError("BAD_REQUEST", "aggregate cannot be combined with hybrid search");
    }
  }
  if (q.paginate !== undefined) {
    // Combination guards mirror server `validate_query`: paginate is one-shot
    // paging, so it can't also narrow to count/unique/first/take. (`get` is
    // rejected above; `order`, index, eq, and range bounds are allowed.)
    if (q.count) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with count");
    }
    if (q.distinct) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with distinct");
    }
    if (q.aggregate !== undefined) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with aggregate");
    }
    if (q.unique) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with unique");
    }
    if (q.first) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with first");
    }
    if (q.take !== undefined) {
      throw new RtDbError("BAD_REQUEST", "paginate cannot be combined with take");
    }
  }
  if (q.gt !== undefined && q.gte !== undefined) {
    throw new RtDbError("BAD_REQUEST", "gt and gte cannot both be set");
  }
  if (q.lt !== undefined && q.lte !== undefined) {
    throw new RtDbError("BAD_REQUEST", "lt and lte cannot both be set");
  }
  if (q.take !== undefined && q.take > MAX_TAKE) {
    throw new RtDbError("BAD_REQUEST", `take exceeds maximum of ${MAX_TAKE}`);
  }
}

/** Index resolution, eq-prefix binding, range-bound coercion, and one-time
 *  filter validation — everything the row scan needs before touching a row. */
function prepareScan(
  q: QueryJson,
  tableDef: TableJson,
  eq: unknown[],
  hasRange: boolean,
): ScanPlan {
  const indexDef: IndexJson | null = q.index
    ? requireIndex(tableDef, q.index)
    : eq.length > 0
      ? (() => {
          throw new RtDbError("BAD_REQUEST", "eq requires an index");
        })()
      : null;

  const eqLen = eq.length;
  if (indexDef && eqLen > indexDef.fields.length) {
    throw new RtDbError(
      "BAD_REQUEST",
      `index '${indexDef.name}' expects at most ${indexDef.fields.length} eq value(s), got ${eqLen}`,
    );
  }
  // Type-check each eq prefix bind (server `eq_binds`).
  const typedEq = indexDef
    ? eq.map((value, i) => coerceIndexValue(tableDef, indexDef.fields[i], value))
    : [];

  let rangeField: string | null = null;
  let rangeFieldPg: PgType | null = null;
  if (hasRange) {
    if (!indexDef) {
      throw new RtDbError("BAD_REQUEST", "range bound requires an index");
    }
    if (eqLen >= indexDef.fields.length) {
      throw new RtDbError("BAD_REQUEST", "range bound requires a remaining index field after eq");
    }
    rangeField = indexDef.fields[eqLen];
    rangeFieldPg = indexColumnType(tableDef.fields[rangeField]).pg;
  }

  const gt = q.gt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gt) : null;
  const gte =
    q.gte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.gte) : null;
  const lt = q.lt !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lt) : null;
  const lte =
    q.lte !== undefined && rangeField ? coerceIndexValue(tableDef, rangeField, q.lte) : null;

  // Validate the filter against the table def once (mirrors server compile_filter).
  if (q.filter) {
    validateFilter(q.filter, tableDef);
  }

  return { indexDef, typedEq, rangeField, rangeFieldPg, gt, gte, lt, lte };
}

/** Row fetch + filter (eq prefix → range → filter hook). FM-33: stamped
 *  (soft-deleted) rows are invisible to every read terminal. `fields` is the
 *  table's declared field map — the filter evaluator's typed-int64 arm keys
 *  off it (ENH-027). */
function fetchFilteredRows(
  q: QueryJson,
  plan: ScanPlan,
  rowsFor: (table: string) => Map<string, StoredRow>,
  fields: FieldMap,
): StoredRow[] {
  const { indexDef, typedEq, rangeField, rangeFieldPg, gt, gte, lt, lte } = plan;
  const eqLen = typedEq.length;
  const filtered: StoredRow[] = [];
  for (const row of rowsFor(q.table).values()) {
    if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are invisible to every read terminal
    if (indexDef) {
      let ok = true;
      for (let i = 0; i < eqLen; i++) {
        const v = row.doc[indexDef.fields[i]];
        if (v === null || v === undefined || v !== typedEq[i]) {
          ok = false;
          break;
        }
      }
      if (!ok) {
        continue;
      }
    }
    if (rangeField) {
      const v = row.doc[rangeField];
      if (v === null || v === undefined) {
        continue;
      }
      if (gt !== null && compareIndexValues(v, gt, rangeFieldPg ?? undefined) <= 0) {
        continue;
      }
      if (gte !== null && compareIndexValues(v, gte, rangeFieldPg ?? undefined) < 0) {
        continue;
      }
      if (lt !== null && compareIndexValues(v, lt, rangeFieldPg ?? undefined) >= 0) {
        continue;
      }
      if (lte !== null && compareIndexValues(v, lte, rangeFieldPg ?? undefined) > 0) {
        continue;
      }
    }
    if (q.filter && !evalFilterExpr(q.filter, row.doc, fields)) {
      continue;
    }
    filtered.push(row);
  }
  return filtered;
}

/** The sort column list every ordered terminal shares: unbound index fields
 *  (after the eq prefix), then `__createdAt`, then `__id`. `sortPgs[i]` is the
 *  storage type of `sortKeys[i]` so the comparator can pick the int64 numeric
 *  path for decimal-string fields. `__createdAt` is a number column; `__id` is
 *  a text column on the server. */
function sortKeysFor(
  tableDef: TableJson,
  indexDef: IndexJson | null,
  eqLen: number,
): { sortKeys: string[]; sortPgs: PgType[] } {
  const sortKeys: string[] = [];
  const sortPgs: PgType[] = [];
  if (indexDef) {
    for (const field of indexDef.fields.slice(eqLen)) {
      sortKeys.push(field);
      sortPgs.push(indexColumnType(tableDef.fields[field]).pg);
    }
  }
  sortKeys.push("__createdAt");
  sortPgs.push("number");
  sortKeys.push("__id");
  sortPgs.push("text");
  return { sortKeys, sortPgs };
}

/** Sorts the filtered set by the shared sort columns in direction `dir`. The
 *  unique `__id` tiebreaker means the order is total — no row is ambiguous
 *  relative to another. */
function sortFilteredRows(
  filtered: StoredRow[],
  tableDef: TableJson,
  plan: ScanPlan,
  dir: Order,
): void {
  const { sortKeys, sortPgs } = sortKeysFor(tableDef, plan.indexDef, plan.typedEq.length);
  filtered.sort((a, b) => {
    for (let i = 0; i < sortKeys.length; i++) {
      const field = sortKeys[i];
      const av = field === "__createdAt" ? a.createdAt : field === "__id" ? a.id : a.doc[field];
      const bv = field === "__createdAt" ? b.createdAt : field === "__id" ? b.id : b.doc[field];
      const cmp = compareIndexValues(av, bv, sortPgs[i]);
      if (cmp !== 0) {
        return dir === "desc" ? -cmp : cmp;
      }
    }
    return 0;
  });
}

/** `get` terminal: point read by id. */
function executeGetTerminal(
  q: QueryJson,
  eq: unknown[],
  hasRange: boolean,
  rowsFor: (table: string) => Map<string, StoredRow>,
): unknown {
  if (
    q.index !== undefined ||
    eq.length > 0 ||
    hasRange ||
    q.order !== undefined ||
    q.take !== undefined ||
    q.unique ||
    q.first ||
    q.count ||
    q.distinct ||
    q.aggregate !== undefined ||
    q.paginate !== undefined ||
    q.filter !== undefined ||
    q.search !== undefined ||
    q.vectorSearch !== undefined ||
    q.hybridSearch !== undefined
  ) {
    throw new RtDbError(
      "BAD_REQUEST",
      "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, distinct, aggregate, paginate, filter, search, or vector search",
    );
  }
  // biome-ignore lint/style/noNonNullAssertion: dispatcher only calls this under q.get !== undefined
  const row = rowsFor(q.table).get(q.get!);
  // FM-33: a soft-deleted row is absent to the get terminal.
  return row && row.deletedAt === undefined ? mergeDoc(row) : null;
}

/** `vectorSearch` terminal: filter-narrowed candidates (in-memory does not
 *  rank by vector distance). */
function executeVectorSearchTerminal(
  q: QueryJson,
  tableDef: TableJson,
  eq: unknown[],
  hasRange: boolean,
  rowsFor: (table: string) => Map<string, StoredRow>,
): unknown {
  if (
    q.index !== undefined ||
    eq.length > 0 ||
    hasRange ||
    q.order !== undefined ||
    q.unique ||
    q.first ||
    q.count ||
    q.distinct ||
    q.aggregate !== undefined ||
    q.paginate !== undefined ||
    q.filter !== undefined ||
    q.search !== undefined ||
    q.take !== undefined ||
    q.hybridSearch !== undefined
  ) {
    throw new RtDbError("BAD_REQUEST", "vectorSearch cannot be combined with any other terminal");
  }
  // biome-ignore lint/style/noNonNullAssertion: dispatcher only calls this under q.vectorSearch !== undefined
  const vs = q.vectorSearch!;
  const vectorDef = tableDef.indexes?.find((i) => i.name === vs.index && i.vector);
  if (!vectorDef) {
    throw new RtDbError("BAD_REQUEST", `vector index '${vs.index}' not found`);
  }
  // Validate the vector-search-level filter against declared fields once
  // (mirrors server `compile_filter` composed into the vector WHERE) via the
  // SAME evaluator `search` uses. The in-memory replica does not rank by
  // vector distance, so it returns filter-narrowed candidates in insertion
  // order (a deterministic stand-in that exercises the filter path); the
  // real server ranks by the index's distance metric.
  if (vs.filter) {
    validateFilter(vs.filter, tableDef);
  }
  const out: unknown[] = [];
  for (const row of rowsFor(q.table).values()) {
    if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are invisible
    if (vs.filter && !evalFilterExpr(vs.filter, row.doc, tableDef.fields)) {
      continue;
    }
    out.push(row.doc);
    if (out.length >= vs.limit) {
      break;
    }
  }
  return out;
}

/** `hybridSearch` terminal: in-memory returns an empty result (no ts_rank +
 *  vector distance fusion). */
function executeHybridSearchTerminal(q: QueryJson, eq: unknown[], hasRange: boolean): unknown {
  if (
    q.index !== undefined ||
    eq.length > 0 ||
    hasRange ||
    q.order !== undefined ||
    q.unique ||
    q.first ||
    q.count ||
    q.distinct ||
    q.aggregate !== undefined ||
    q.paginate !== undefined ||
    q.filter !== undefined ||
    q.search !== undefined ||
    q.vectorSearch !== undefined ||
    q.take !== undefined
  ) {
    throw new RtDbError("BAD_REQUEST", "hybridSearch cannot be combined with any other terminal");
  }
  // No in-memory hybrid ranking; return an empty result rather than silently
  // misranking by falling through to the collect path.
  return [];
}

/** `search` terminal: full-text matching under websearch syntax (default
 *  `tsquery` mode — quoted phrases, `or`, `-term`; FM-31) or
 *  case-insensitive substring matching (`trgm` mode), each with a
 *  deterministic relevance stand-in. */
function executeSearchTerminal(
  q: QueryJson,
  tableDef: TableJson,
  eq: unknown[],
  hasRange: boolean,
  rowsFor: (table: string) => Map<string, StoredRow>,
): unknown {
  if (
    q.index !== undefined ||
    eq.length > 0 ||
    hasRange ||
    q.order !== undefined ||
    q.unique ||
    q.first ||
    q.count ||
    q.distinct ||
    q.aggregate !== undefined ||
    q.paginate !== undefined ||
    q.filter !== undefined ||
    q.vectorSearch !== undefined ||
    q.hybridSearch !== undefined
  ) {
    throw new RtDbError(
      "BAD_REQUEST",
      "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, aggregate, paginate, filter, or vector search",
    );
  }
  // Full-text matching (not ts_rank ordering): mirror
  // `websearch_to_tsquery` semantics closely enough that a unit test can
  // assert match/no-match — quoted phrases require adjacency, bare `or`
  // unions alternatives, `-term` excludes, plain terms stay AND (a pure
  // superset of the former plainto token-AND for plain input). A doc matches
  // when ANY alternative matches against the concatenated text of the search
  // index's declared fields. Ranking is a deterministic stand-in
  // (query-lexeme frequency desc, then `created_at` desc, then `id` desc) —
  // exact `ts_rank` order is intentionally not replicated. `take` (already
  // capped to MAX_TAKE above) limits the result.
  // biome-ignore lint/style/noNonNullAssertion: dispatcher only calls this under q.search !== undefined
  const search = q.search!;
  if (search.query.trim().length === 0) {
    throw new RtDbError("BAD_REQUEST", "search query text must not be empty");
  }
  const searchDef = tableDef.indexes?.find((i) => i.name === search.index && i.search);
  if (!searchDef) {
    throw new RtDbError("BAD_REQUEST", `search index '${search.index}' not found`);
  }
  // Validate the search-level filter against declared fields once (mirrors
  // server `compile_filter` composed into the search WHERE).
  if (search.filter) {
    validateFilter(search.filter, tableDef);
  }
  // `snippet` needs a tsquery tree to highlight; trgm mode matches raw
  // substrings, so the combination is rejected rather than silently
  // ignored (mirrors server compile_search).
  const snippet = search.snippet === true;
  if (snippet && search.mode === "trgm") {
    throw new RtDbError("BAD_REQUEST", "snippet is only supported in tsquery mode");
  }
  const limit = q.take ?? MAX_TAKE;
  const scored: Array<{ row: StoredRow; score: number; snippet?: string }> = [];
  if (search.mode === "trgm") {
    // `trgm` mode (ILIKE '%q%' + similarity() on the server): a doc matches
    // when ANY indexed field's lowercased text contains the lowercased query
    // as a substring — infix/prefix hits token-AND cannot make. Similarity
    // ranking stand-in, pinned for cross-client harness parity: per doc,
    // over the indexed fields that contain the query, score =
    // query.length / field.length (a shorter containing field is more
    // similar), max across fields. Same `created_at`/`id` tie-breaks as the
    // tsquery path.
    const needle = search.query.toLowerCase();
    for (const row of rowsFor(q.table).values()) {
      if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are invisible
      if (search.filter && !evalFilterExpr(search.filter, row.doc, tableDef.fields)) {
        continue;
      }
      let best = 0;
      for (const field of searchDef.fields) {
        const text = ftsStringify(row.doc[field]).toLowerCase();
        if (text.includes(needle)) {
          const similarity = needle.length / text.length;
          if (similarity > best) best = similarity;
        }
      }
      if (best > 0) scored.push({ row, score: best });
    }
  } else {
    const alts = parseWebsearchQuery(search.query);
    // Every positive lexeme across the alternatives (the query tree ts_headline
    // would mark on the server) — reused for scoring and snippet highlights.
    const positives = new Set(alts.flatMap((a) => [...a.terms, ...a.phrases.flat()]));
    for (const row of rowsFor(q.table).values()) {
      if (row.deletedAt !== undefined) continue; // FM-33: stamped rows are invisible
      if (search.filter && !evalFilterExpr(search.filter, row.doc, tableDef.fields)) {
        continue;
      }
      const source = searchDef.fields.map((f) => ftsStringify(row.doc[f])).join(" ");
      const docTokens = ftsTokens(source);
      if (!alts.some((a) => altMatches(a, docTokens))) {
        continue;
      }
      let score = 0;
      for (const dt of docTokens) {
        if (positives.has(dt)) score++;
      }
      const hit: { row: StoredRow; score: number; snippet?: string } = { row, score };
      if (snippet) hit.snippet = buildSearchSnippet(source, positives);
      scored.push(hit);
    }
  }
  scored.sort((a, b) =>
    a.score !== b.score
      ? b.score - a.score
      : a.row.createdAt !== b.row.createdAt
        ? b.row.createdAt - a.row.createdAt
        : a.row.id > b.row.id
          ? -1
          : a.row.id < b.row.id
            ? 1
            : 0,
  );
  return scored
    .slice(0, limit)
    .map((s) =>
      s.snippet !== undefined ? { ...mergeDoc(s.row), _searchSnippet: s.snippet } : mergeDoc(s.row),
    );
}

/** `count` terminal: COUNT(*) over the matching set. */
function executeCountTerminal(filtered: StoredRow[]): number {
  return filtered.length;
}

/** `distinct` terminal: unique values of the index field after the eq prefix
 *  over the matching set. */
function executeDistinctTerminal(
  tableDef: TableJson,
  indexDef: IndexJson | null,
  eqLen: number,
  filtered: StoredRow[],
): unknown {
  if (!indexDef) {
    throw new RtDbError("BAD_REQUEST", "distinct requires an index field beyond the eq prefix");
  }
  if (eqLen >= indexDef.fields.length) {
    throw new RtDbError("BAD_REQUEST", "distinct requires an index field beyond the eq prefix");
  }
  const field = indexDef.fields[eqLen];
  const fieldPg = indexColumnType(tableDef.fields[field]).pg;
  const seen = new Set<unknown>();
  const values: unknown[] = [];
  for (const row of filtered) {
    // An absent optional field is SQL NULL in the typed column; DISTINCT
    // keeps one NULL row (corpus `distinct-includes-null`; the server's
    // NULLS-last asc ordering puts it after every string). A null key is
    // distinct from the string "null" in the Set, so the two cannot collapse.
    const raw = row.doc[field];
    const v = raw === undefined ? null : raw;
    const key =
      v === null
        ? null
        : typeof v === "number" || typeof v === "string" || typeof v === "boolean"
          ? v
          : JSON.stringify(v);
    if (!seen.has(key)) {
      seen.add(key);
      values.push(v);
    }
  }
  values.sort((a, b) => compareIndexValues(a, b, fieldPg));
  return values.slice(0, MAX_TAKE);
}

/** `aggregate` terminal: OP over the index field after the eq prefix, with
 *  optional `groupBy`. */
function executeAggregateTerminal(
  q: QueryJson,
  tableDef: TableJson,
  indexDef: IndexJson | null,
  eqLen: number,
  filtered: StoredRow[],
): unknown {
  const isNumeric = (fieldName: string): boolean => {
    const ft = tableDef.fields[fieldName];
    // `number` and `int64` are the numeric indexable types; an optional
    // wrapper unwraps to its inner type. Mirrors server `is_numeric_index_field`.
    if (!ft) return false;
    const tag = (ft as { type: string }).type;
    if (tag === "number" || tag === "int64") return true;
    if (tag === "optional") {
      const inner = (ft as { inner: { type: string } }).inner;
      return inner?.type === "number" || inner?.type === "int64";
    }
    return false;
  };
  // biome-ignore lint/style/noNonNullAssertion: dispatcher only calls this under q.aggregate !== undefined
  const { op, groupBy = false } = q.aggregate!;
  // `count` aggregates rows, not a field — it consumes no aggregate index
  // field (mirrors server `AggregateOp::needs_field`).
  const needsField = op !== "count";
  if (groupBy) {
    if (!indexDef || eqLen >= indexDef.fields.length) {
      throw new RtDbError(
        "BAD_REQUEST",
        "aggregate groupBy requires an index field beyond the eq prefix",
      );
    }
    const groupField = indexDef.fields[eqLen];
    const groupFieldPg = indexColumnType(tableDef.fields[groupField]).pg;
    let aggField: string | undefined;
    let aggFieldPg: PgType | undefined;
    if (needsField) {
      if (eqLen + 1 >= indexDef.fields.length) {
        throw new RtDbError(
          "BAD_REQUEST",
          "aggregate groupBy requires two index fields beyond the eq prefix",
        );
      }
      aggField = indexDef.fields[eqLen + 1];
      aggFieldPg = indexColumnType(tableDef.fields[aggField]).pg;
      if ((op === "sum" || op === "avg") && !isNumeric(aggField)) {
        throw new RtDbError("BAD_REQUEST", `aggregate op ${op} requires a numeric index field`);
      }
    }
    // Group rows by `groupField` value, preserving first-seen order and then
    // sorting by key ascending for parity with the server's ORDER BY k — rows
    // missing the group field form one null group (the server's GROUP BY
    // includes the SQL NULL group; compareIndexValues sorts it last, matching
    // Postgres NULLS LAST). `count` counts rows (one entry per row); else
    // aggregate the field's non-null values (SQL aggregates skip NULL — a
    // group left with none aggregates to null).
    const groups = new Map<unknown, unknown[]>();
    for (const row of filtered) {
      const k = row.doc[groupField] ?? null;
      const entry = aggField !== undefined ? row.doc[aggField] : row;
      const existing = groups.get(k);
      if (existing) {
        existing.push(entry);
      } else {
        groups.set(k, [entry]);
      }
    }
    const out = Array.from(groups.entries())
      .map(([k, values]) => {
        if (op === "count") {
          return { key: k, value: applyAggregate(op, values, aggFieldPg) };
        }
        const present = values.filter((v) => v !== null && v !== undefined);
        return {
          key: k,
          value: present.length > 0 ? applyAggregate(op, present, aggFieldPg) : null,
        };
      })
      .sort((a, b) => compareIndexValues(a.key, b.key, groupFieldPg))
      .slice(0, MAX_TAKE);
    return out;
  }
  // Scalar: `count` needs no index/field (COUNT(*) over the matching set);
  // sum/avg/min/max require an aggregate field beyond the eq prefix.
  if (needsField) {
    if (!indexDef) {
      throw new RtDbError("BAD_REQUEST", "aggregate requires an index field beyond the eq prefix");
    }
    if (eqLen >= indexDef.fields.length) {
      throw new RtDbError("BAD_REQUEST", "aggregate requires an index field beyond the eq prefix");
    }
    const aggField = indexDef.fields[eqLen];
    const aggFieldPg = indexColumnType(tableDef.fields[aggField]).pg;
    if ((op === "sum" || op === "avg") && !isNumeric(aggField)) {
      throw new RtDbError("BAD_REQUEST", `aggregate op ${op} requires a numeric index field`);
    }
    const values = filtered
      .map((row) => row.doc[aggField])
      .filter((v) => v !== null && v !== undefined);
    // Empty set → null (matches server SUM/AVG/MIN/MAX over zero rows).
    return values.length === 0 ? null : applyAggregate(op, values, aggFieldPg);
  }
  // Scalar count: COUNT(*) over the matching set (0 when empty).
  return filtered.length;
}

/** `paginate` terminal: keyset-cursor paging over the already-filtered,
 * already-sorted set. The sort columns mirror the producing sort (unbound
 * index fields after the eq prefix, then `__createdAt`, then `__id`); the
 * cursor encodes one value per column. */
function executePaginateTerminal(
  paginate: Paginate,
  tableDef: TableJson,
  filtered: StoredRow[],
  plan: ScanPlan,
  dir: Order,
): PaginatedResultJson {
  const { sortKeys, sortPgs } = sortKeysFor(tableDef, plan.indexDef, plan.typedEq.length);
  return paginateResult(paginate, tableDef, filtered, sortKeys, sortPgs, dir);
}

/** Collect terminal: the post-sort tail covering `unique` (at-most-one
 *  match), `first` (head), and the default `take`-limited collect. Mirrors the
 *  server's `execute_collect_terminal`. */
function executeCollectTerminal(q: QueryJson, filtered: StoredRow[]): unknown {
  if (q.unique) {
    if (filtered.length > 1) {
      throw new RtDbError("PRECONDITION_FAILED", "unique query matched multiple documents");
    }
    return filtered[0] ? mergeDoc(filtered[0]) : null;
  }
  if (q.first) {
    return filtered[0] ? mergeDoc(filtered[0]) : null;
  }
  const limit = q.take ?? MAX_TAKE;
  return filtered.slice(0, limit).map((row) => mergeDoc(row));
}

/** Cursor keyset pagination — a port of server `query.rs`'s paginate branch.
 *  `sorted` is already filtered (eq/range) and sorted over `sortKeys` (unbound
 *  index fields, then `__createdAt`, then `__id`) in direction `dir`. The
 *  cursor stores one value per sort column; the resume predicate is the
 *  standard OR-of-AND row-value comparison, so paging is stable (the unique
 *  `id` tiebreaker means no row is skipped or duplicated across pages).
 *  `sortPgs[i]` is the storage type of `sortKeys[i]` so the resume predicate
 *  uses the same int64-aware comparator as the producing sort. */
function paginateResult(
  paginate: Paginate,
  tableDef: TableJson,
  sorted: StoredRow[],
  sortKeys: string[],
  sortPgs: PgType[],
  dir: Order,
): PaginatedResultJson {
  const { numItems: requested, cursor } = paginate;
  const numItems = Math.min(requested, MAX_TAKE);

  let rows = sorted;
  if (cursor) {
    const cursorValues = decodePaginateCursor(cursor);
    if (cursorValues.length !== sortKeys.length) {
      throw new RtDbError(
        "BAD_REQUEST",
        `cursor has ${cursorValues.length} value(s) but this query sorts over ${sortKeys.length} column(s)`,
      );
    }
    validateCursorValues(cursorValues, sortKeys, tableDef);
    rows = sorted.filter((row) => isAfterCursor(row, cursorValues, sortKeys, sortPgs, dir));
  }

  // Fetch one past the page size so a next page is detectable without a second
  // pass; the extra is discarded after the has-next check (server `LIMIT n+1`).
  const fetched = rows.slice(0, numItems + 1);
  const hasNext = fetched.length > numItems;
  if (hasNext) {
    fetched.pop();
  }
  const docs = fetched.map((row) => mergeDoc(row));
  // The next cursor is built from the page's last row; absent when the page is
  // empty or this was the final page. ARC-133:PaginatedResultJson.nextCursor
  // is `?:`-optional, so the key is included only when a cursor exists
  // (exactOptionalPropertyTypes forbids assigning literal `undefined`).
  const nextCursor =
    hasNext && fetched.length > 0
      ? encodeCursor(sortKeys.map((key) => sortValue(fetched[fetched.length - 1], key)))
      : undefined;
  return { docs, ...(nextCursor === undefined ? {} : { nextCursor }) };
}

/** Decodes a paginate cursor, rethrowing the live client's generic parse error
 *  as a server-shaped `BAD_REQUEST` (server `decode_cursor` → bad_request). */
function decodePaginateCursor(cursor: string): unknown[] {
  let values: unknown;
  try {
    values = decodeCursor(cursor);
  } catch (e) {
    throw new RtDbError("BAD_REQUEST", `invalid cursor: ${(e as Error).message}`);
  }
  if (!Array.isArray(values)) {
    throw new RtDbError("BAD_REQUEST", "invalid cursor: expected an array");
  }
  return values;
}

/** Type-checks decoded cursor values positionally against the sort columns —
 *  a port of server `SortCol::cursor_bind` (index fields via `eq_bind_for`,
 *  `created_at` as number, `id` as string). The final two columns are always
 *  `__createdAt` / `__id`; the rest are unbound indexed fields. */
function validateCursorValues(
  cursorValues: unknown[],
  sortKeys: string[],
  tableDef: TableJson,
): void {
  for (let i = 0; i < sortKeys.length - 2; i++) {
    const value = cursorValues[i];
    // Null sorts (nulls-last) and is a legitimate value for an optional index
    // field; only type-check present values, mirroring the server's typed bind.
    if (value !== null) {
      coerceIndexValue(tableDef, sortKeys[i], value);
    }
  }
  const createdAt = cursorValues[sortKeys.length - 2];
  if (typeof createdAt !== "number") {
    throw new RtDbError("BAD_REQUEST", "cursor value for created_at must be a number");
  }
  const id = cursorValues[sortKeys.length - 1];
  if (typeof id !== "string") {
    throw new RtDbError("BAD_REQUEST", "cursor value for id must be a string");
  }
}

/** The keyset resume predicate: true when `row` sorts strictly after the cursor
 *  row. This is the lexicographic "greater than" expanded to OR-of-AND —
 *
 *    (c0 OP v0) OR (c0 = v0 AND c1 OP v1) OR ... —
 *
 *  where OP is `>` (asc) / `<` (desc). Evaluated with the same `null`-sorts-last
 *  comparator as the sort, so it agrees with the ordering that produced `sorted`. */
function isAfterCursor(
  row: StoredRow,
  cursorValues: unknown[],
  sortKeys: string[],
  sortPgs: PgType[],
  dir: Order,
): boolean {
  for (let i = 0; i < sortKeys.length; i++) {
    let prefixEqual = true;
    for (let j = 0; j < i; j++) {
      if (compareIndexValues(sortValue(row, sortKeys[j]), cursorValues[j], sortPgs[j]) !== 0) {
        prefixEqual = false;
        break;
      }
    }
    if (!prefixEqual) {
      continue;
    }
    const cmp = compareIndexValues(sortValue(row, sortKeys[i]), cursorValues[i], sortPgs[i]);
    if (dir === "desc" ? cmp < 0 : cmp > 0) {
      return true;
    }
  }
  return false;
}

/** Sort value for a synthetic sort key, normalizing an absent optional index
 *  field to `null` so cursor encoding and the resume predicate stay consistent
 *  with the `null`-sorts-last comparator. */
function sortValue(row: StoredRow, key: string): unknown {
  if (key === "__createdAt") {
    return row.createdAt;
  }
  if (key === "__id") {
    return row.id;
  }
  const v = row.doc[key];
  return v === undefined ? null : v;
}
