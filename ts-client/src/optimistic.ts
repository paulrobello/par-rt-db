import type { QueryJson, TransactionJson } from "./protocol.js";

/**
 * Client-side optimistic-update projection.
 *
 * The reactive {@link RtDbClient} caches each subscription's last result and
 * holds neither a schema nor a table store, so an optimistic overlay can only be
 * computed from the documents already present in that cached result. This module
 * mirrors the server/in-memory DSL semantics (`server/src/txn.rs`,
 * `ts-client/src/in_memory.ts`) for the cases where the effect on the result set
 * is unambiguous from those documents alone — and deliberately declines to guess
 * everywhere else. "Correctness over coverage": a wrong overlay is worse than a
 * brief wait for the authoritative `queryUpdate`.
 */

/** A result document: user fields plus the merged system fields `_id`/`_creationTime`/`_version`. */
type ResultDoc = Record<string, unknown> & { _id: string };

export interface OptimisticProjection {
  /**
   * `true` with a computed `value` to surface immediately. `false` for either a
   * no-op (the result is unchanged) or an ambiguous effect (the caller must wait
   * for the server). The caller does not distinguish the two — both mean "no overlay".
   */
  overlaid: boolean;
  value?: unknown;
}

/** Skip: do not overlay (no-op or ambiguous). */
const SKIP: OptimisticProjection = { overlaid: false };

let syntheticIdCounter = 0;

function isArrayQuery(q: QueryJson): boolean {
  return q.get === undefined && !q.unique && !q.first && !q.count && q.paginate === undefined;
}

/** A query with an eq/range filter whose membership we cannot evaluate without the schema. */
function hasFilter(q: QueryJson): boolean {
  return (
    q.index !== undefined ||
    (q.eq !== undefined && q.eq.length > 0) ||
    q.gt !== undefined ||
    q.gte !== undefined ||
    q.lt !== undefined ||
    q.lte !== undefined
  );
}

/** Canonical string form (key-sorted) so a no-op projection is detected regardless of key order. */
function canonical(value: unknown): string {
  return JSON.stringify(value, (_k, v) => {
    if (v && typeof v === "object" && !Array.isArray(v)) {
      const sorted: Record<string, unknown> = {};
      for (const key of Object.keys(v as Record<string, unknown>).sort()) {
        sorted[key] = (v as Record<string, unknown>)[key];
      }
      return sorted;
    }
    return v;
  });
}

/** A clearly-branded temporary id for an optimistically-inserted doc (replaced on reconcile). */
function syntheticId(): string {
  syntheticIdCounter += 1;
  return `__optimistic__${syntheticIdCounter}`;
}

function isResultDoc(value: unknown): value is ResultDoc {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    typeof (value as { _id?: unknown })._id === "string"
  );
}

/**
 * Projects a transaction onto a subscription's last result. See the file doc for
 * the scope; in short — unfiltered `collect`/`take` (insert/patch/replace/delete),
 * filtered `collect`/`take` (delete only), and `get(id)` (patch/replace/delete of
 * that id) are overlaid; `upsert`, `first`, `unique`, `count`, `paginate`, and any
 * filter-membership-dependent change are not.
 */
export function projectOptimisticUpdate(
  query: QueryJson,
  last: unknown,
  txn: TransactionJson,
  now: () => number = Date.now,
): OptimisticProjection {
  // A projected query (`fields` set) caches docs that carry only the projected
  // fields, while txn steps carry full docs/fields — an overlay computed from
  // the two would surface fields the authoritative (projected) result drops.
  // Decline, per the file rule: a wrong overlay is worse than a brief wait.
  if (query.fields !== undefined) {
    return SKIP;
  }
  if (query.get !== undefined) {
    return projectGet(query, last, txn);
  }
  if (!isArrayQuery(query)) {
    return SKIP;
  }
  return hasFilter(query)
    ? projectFilteredArray(query, last, txn)
    : projectUnfilteredArray(query, last, txn, now);
}

/** Unfiltered full-table read (`collect`/`take` with no index/eq/range): every doc is present,
 * so insert/patch/replace/delete on a known id are all unambiguous. */
function projectUnfilteredArray(
  query: QueryJson,
  last: unknown,
  txn: TransactionJson,
  now: () => number,
): OptimisticProjection {
  if (!Array.isArray(last)) {
    return SKIP;
  }
  const working: ResultDoc[] = last.map((d) => ({ ...(d as ResultDoc) }));
  for (const step of txn.steps) {
    // schedule/cancelSchedule (FM-28) target the scheduler, not a table.
    if (!("table" in step) || step.table !== query.table) {
      continue;
    }
    switch (step.op) {
      case "insert": {
        // A full-table window already at its `take` limit would evict an unknown
        // doc — we can't pick the right window, so decline.
        if (query.take !== undefined && working.length >= query.take) {
          return SKIP;
        }
        working.push({ ...step.doc, _id: syntheticId(), _creationTime: now(), _version: 1 });
        break;
      }
      case "patch": {
        const i = working.findIndex((d) => d._id === step.id);
        if (i >= 0) {
          working[i] = { ...working[i], ...step.fields };
        }
        break;
      }
      case "replace": {
        const i = working.findIndex((d) => d._id === step.id);
        if (i >= 0) {
          const old = working[i];
          working[i] = { ...step.doc, _id: old._id, _creationTime: old._creationTime };
        }
        break;
      }
      case "delete": {
        const i = working.findIndex((d) => d._id === step.id);
        if (i >= 0) {
          working.splice(i, 1);
        }
        break;
      }
      case "upsert":
        return SKIP;
      // expectVersion / expectAbsent are preconditions with no data effect.
    }
  }
  return canonical(working) === canonical(last) ? SKIP : { overlaid: true, value: working };
}

/** Filtered read (index/eq/range): only a delete of a doc already known to be in the result
 * is unambiguous — adding or changing a doc may move it in or out of the filter. */
function projectFilteredArray(
  query: QueryJson,
  last: unknown,
  txn: TransactionJson,
): OptimisticProjection {
  if (!Array.isArray(last)) {
    return SKIP;
  }
  const working: ResultDoc[] = last.map((d) => ({ ...(d as ResultDoc) }));
  for (const step of txn.steps) {
    // schedule/cancelSchedule (FM-28) target the scheduler, not a table.
    if (!("table" in step) || step.table !== query.table) {
      continue;
    }
    if (step.op === "delete") {
      const i = working.findIndex((d) => d._id === step.id);
      if (i >= 0) {
        working.splice(i, 1);
      }
      continue;
    }
    if (
      step.op === "insert" ||
      step.op === "patch" ||
      step.op === "replace" ||
      step.op === "upsert"
    ) {
      return SKIP;
    }
    // expectVersion / expectAbsent: no data effect.
  }
  return canonical(working) === canonical(last) ? SKIP : { overlaid: true, value: working };
}

/** Point read by id: the result is exactly that id's doc (or null), so patch/replace/delete of
 * the same id are unambiguous; a freshly inserted id can never match. */
function projectGet(query: QueryJson, last: unknown, txn: TransactionJson): OptimisticProjection {
  if (last !== null && !isResultDoc(last)) {
    return SKIP;
  }
  let working: ResultDoc | null = last === null ? null : { ...last };
  const target = query.get;
  for (const step of txn.steps) {
    // schedule/cancelSchedule (FM-28) target the scheduler, not a table.
    if (!("table" in step) || step.table !== query.table) {
      continue;
    }
    switch (step.op) {
      case "delete":
        if (working && step.id === target) {
          working = null;
        }
        break;
      case "patch":
        if (working && step.id === target) {
          working = { ...working, ...step.fields };
        }
        break;
      case "replace":
        if (working && step.id === target) {
          working = { ...step.doc, _id: working._id, _creationTime: working._creationTime };
        }
        break;
      case "insert":
        break;
      case "upsert":
        return SKIP;
    }
  }
  return canonical(working) === canonical(last) ? SKIP : { overlaid: true, value: working };
}
