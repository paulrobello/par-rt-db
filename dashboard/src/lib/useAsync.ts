import type { DependencyList } from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { toErrorMessage } from "./errors";

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
 * Like `useEffect`, list every closed-over value in `deps` or the refresh goes
 * stale; `fetcher` itself is read through a ref so it does not need to be
 * memoized by the caller.
 */
export function useAsync<T>(
  fetcher: () => Promise<T>,
  deps: DependencyList,
  initial: T,
): UseAsyncResult<T> {
  const [data, setData] = useState<T>(initial);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Latest fetcher + initial-value live in refs so the stable `refresh` callback
  // never goes stale without forcing a re-run (and re-fetch) on every render.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const initialRef = useRef(initial);
  initialRef.current = initial;

  const refresh = useCallback(async () => {
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
    void refresh();
    // `deps` is the caller-controlled trigger list (see hook docstring).
    // biome-ignore lint/correctness/useExhaustiveDependencies: intentional
  }, deps);

  return { data, loading, error, refresh, setData };
}
