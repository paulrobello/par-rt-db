import { useCallback, useEffect, useRef, useState } from "react";
import type { PaginatedResultJson, QueryJson } from "./protocol.js";
import type { RtQuery } from "./query.js";
import { useRtDbClient } from "./react.js";

/**
 * Options configuring the behavior of `usePaginatedQuery`.
 */
export interface UsePaginatedQueryOptions {
  /**
   * The maximum number of items to return in a single page.
   */
  pageSize?: number;
  /**
   * Flag to temporarily disable the pagination query logic.
   */
  enabled?: boolean;
}

/**
 * The structured result object returned by `usePaginatedQuery`.
 */
export interface UsePaginatedQueryResult<T> {
  /**
   * Aligned stitched list of loaded query documents.
   */
  data: T[];
  /**
   * Flag indicating whether a page subscription is currently loading.
   */
  loading: boolean;
  /**
   * The last encountered subscription error.
   */
  error: Error | null;
  /**
   * Flag indicating whether there are more pages that can be loaded.
   */
  hasNextPage: boolean;
  /**
   * Triggers subscription and loading of the next page of results.
   */
  loadMore: () => Promise<void>;
  /**
   * Refetches all pages from the beginning.
   */
  refetch: () => Promise<void>;
}

interface Page {
  /** Cursor used to request this page; `undefined` for the first page. */
  readonly cursor: string | undefined;
  /** Latest page result, or `null` while the subscription has not delivered. */
  result: PaginatedResultJson | null;
}

const DEFAULT_PAGE_SIZE = 20;

/**
 * Reactive, cursor-based pagination over a `paginate` query. Each loaded page
 * is a live `client.subscribe` subscription; docs are stitched across pages,
 * and `loadMore` advances the cursor returned by the last page. Mirrors the
 * `useQuery` subscription lifecycle (context-sourced client, keyed by the
 * serialized query shape, immediate cached-value replay on re-subscribe).
 *
 * The page-subscription set is incrementally diffed against the desired cursor
 * list, so requesting page N+1 does not re-subscribe pages 1..N.
 */
export function usePaginatedQuery<T>(
  queryFactory: () => QueryJson,
  options: UsePaginatedQueryOptions = {},
): UsePaginatedQueryResult<T> {
  const { pageSize = DEFAULT_PAGE_SIZE, enabled = true } = options;
  const client = useRtDbClient();

  const [pages, setPages] = useState<Page[]>(enabled ? [{ cursor: undefined, result: null }] : []);
  // Bumped by `refetch` to force every page subscription to re-create (fresh
  // server round-trip) even when the cursor list is unchanged.
  const [refetchNonce, setRefetchNonce] = useState(0);

  // Build the base query each render and key effects off its serialized shape,
  // so a new factory returning the same shape does not resubscribe (same trick
  // `useQuery` plays).
  const baseQuery = queryFactory();
  const queryKey = JSON.stringify(baseQuery);
  const cursorsKey = JSON.stringify(pages.map((p) => p.cursor));

  // When the query shape, page size, or enabled flag changes, drop accumulated
  // pages and re-request the first page. Done in the render phase (guarded by a
  // ref) so pages is reset before effects observe the new key — otherwise the
  // subscribe effect would briefly re-request deeper pages under the new query.
  const resetKey = `${queryKey} ${pageSize} ${enabled}`;
  const prevResetKey = useRef(resetKey);
  if (prevResetKey.current !== resetKey) {
    prevResetKey.current = resetKey;
    setPages(enabled ? [{ cursor: undefined, result: null }] : []);
  }

  // Live page subscriptions, keyed by (query shape, page size, cursor, nonce).
  // Incrementally diffed so a stable page keeps its subscription — and its
  // cached value — across loads of later pages.
  const subsRef = useRef<Map<string, () => void>>(new Map());

  // Unmount-only teardown. The subscribe effect below intentionally returns no
  // cleanup so cursor/nonce changes don't drop stable page subscriptions; this
  // effect is the sole place they are released.
  useEffect(() => {
    return () => {
      for (const off of subsRef.current.values()) off();
      subsRef.current.clear();
    };
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: keyed by client + serialized query shape + cursor set + refetch nonce; baseQuery/pages are captured via queryKey/cursorsKey
  useEffect(() => {
    if (!enabled || pages.length === 0) {
      return;
    }
    const subs = subsRef.current;
    const pageKey = (cursor: string | undefined) =>
      `${queryKey} ${pageSize} ${cursor ?? ""} ${refetchNonce}`;
    const desired = new Set(pages.map((p) => pageKey(p.cursor)));
    // Tear down stale subs BEFORE creating new ones: `client.subscribe` dedupes
    // by serialized query and replays the cached value to a new listener, so an
    // add-before-remove ordering would replay a stale page on refetch instead of
    // forcing a fresh server round-trip.
    for (const [key, off] of subs) {
      if (!desired.has(key)) {
        off();
        subs.delete(key);
      }
    }
    for (const page of pages) {
      const cursor = page.cursor;
      const key = pageKey(cursor);
      if (!subs.has(key)) {
        // ARC-133: Paginate.cursor is `?:`-optional; include only when non-empty
        // (exactOptionalPropertyTypes forbids literal `undefined`).
        const c = cursor || "";
        const q: RtQuery<PaginatedResultJson> = {
          json: {
            ...baseQuery,
            paginate: { ...(c === "" ? {} : { cursor: c }), numItems: pageSize },
          },
        };
        const off = client.subscribe<PaginatedResultJson>(q, (result) => {
          setPages((prev) => {
            const idx = prev.findIndex((p) => p.cursor === cursor);
            if (idx < 0) {
              return prev; // page dropped by reset/refetch — stale write
            }
            const next = prev.slice();
            next[idx] = { cursor, result };
            return next;
          });
        });
        subs.set(key, off);
      }
    }
  }, [client, queryKey, pageSize, enabled, cursorsKey, refetchNonce]);

  const loadMore = useCallback((): Promise<void> => {
    setPages((prev) => {
      const last = prev[prev.length - 1];
      if (!last?.result || last.result.nextCursor == null) {
        return prev; // last page still pending, or no cursor -> nothing to load
      }
      return [...prev, { cursor: last.result.nextCursor, result: null }];
    });
    return Promise.resolve();
  }, []);

  const refetch = useCallback((): Promise<void> => {
    setPages([{ cursor: undefined, result: null }]);
    setRefetchNonce((n) => n + 1);
    return Promise.resolve();
  }, []);

  const data = pages.flatMap((p) => (p.result ? (p.result.docs as T[]) : []));
  const loading = enabled && pages.some((p) => p.result === null);
  const lastResult = pages.length > 0 ? pages[pages.length - 1].result : null;
  const hasNextPage = lastResult ? lastResult.nextCursor != null : false;
  // The WS subscription surface has no error channel (`subscribeErr` simply
  // drops the listener, leaving `result` null); surfaced for API parity.
  const error: Error | null = null;

  return { data, loading, error, hasNextPage, loadMore, refetch };
}
