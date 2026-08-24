import type { DependencyList } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { toErrorMessage } from "./errors";

export interface UseAsyncOptions {
  /**
   * When `false`, the fetcher never runs: `data` sits at `initial`, `loading`
   * is `false`, and `error` is `null` — the `if (!db) return` guard the pages
   * hand-rolled before a required id/filter was chosen, generalized. Flipping
   * back to `true` (e.g. once a database is selected) triggers a normal load.
   * Defaults to `true`.
   */
  enabled?: boolean;
}

export interface UseAsyncResult<T> {
  data: T;
  loading: boolean;
  error: string | null;
  /** Re-run the fetcher. Also fires on mount and whenever `deps` change. */
  refresh: () => Promise<void>;
  /** Imperative data setter (e.g. for optimistic updates before a refresh). */
  setData: (next: T | ((prev: T) => T)) => void;
}

/**
 * Load-on-mount async data with loading/error state and a manual `refresh`.
 *
 * `fetcher` is recalled whenever `deps` change. On error, `data` resets to the
 * `initial` value passed (matching the `setX([])` convention the pages used
 * inline) and `error` holds the {@link toErrorMessage} string. Pages that need
 * richer reset behavior, or whose fetch populates several state slots at once,
 * should keep the manual `try/catch` + `toErrorMessage` pattern instead.
 *
 * Pass `{ enabled: false }` (see {@link UseAsyncOptions}) while a required
 * dependency — a selected database, an id — isn't available yet; the fetcher
 * is skipped and `data`/`loading`/`error` sit at their empty state instead of
 * firing a doomed request.
 *
 * Like `useEffect`, list every closed-over value in `deps` or the refresh goes
 * stale; `fetcher` itself is read through a ref so it does not need to be
 * memoized by the caller.
 */
export function useAsync<T>(
  fetcher: () => Promise<T>,
  deps: DependencyList,
  initial: T,
  options?: UseAsyncOptions,
): UseAsyncResult<T> {
  const enabled = options?.enabled ?? true;
  const [data, setData] = useState<T>(initial);
  const [loading, setLoading] = useState(enabled);
  const [error, setError] = useState<string | null>(null);
  // Latest fetcher + initial-value live in refs so the stable `refresh` callback
  // never goes stale without forcing a re-run (and re-fetch) on every render.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const initialRef = useRef(initial);
  initialRef.current = initial;
  const enabledRef = useRef(enabled);
  enabledRef.current = enabled;

  const refresh = useCallback(async () => {
    if (!enabledRef.current) return;
    setLoading(true);
    setError(null);
    try {
      setData(await fetcherRef.current());
    } catch (e) {
      setError(toErrorMessage(e));
      setData(initialRef.current);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!enabled) {
      setData(initialRef.current);
      setLoading(false);
      setError(null);
      return;
    }
    void refresh();
    // `deps` (plus `enabled`) is the caller-controlled trigger list (see hook
    // docstring); `refresh` is stable (empty-dep useCallback) so listing it
    // does not change when this effect re-runs.
  }, [enabled, ...deps, refresh]);

  return { data, loading, error, refresh, setData };
}
