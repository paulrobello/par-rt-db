import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useAsync } from "./useAsync";

// The load-on-mount hook the pages use instead of hand-rolled try/catch:
// loading/data/error transitions, error-resets-to-initial, the `enabled` gate,
// dep-change refetch, and the stable `refresh`.

describe("useAsync", () => {
  it("loads on mount: loading then data", async () => {
    const { result } = renderHook(() => useAsync(async () => ["a"], [], [] as string[]));

    expect(result.current.loading).toBe(true);
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual(["a"]);
    expect(result.current.error).toBeNull();
  });

  it("resets data to initial and surfaces toErrorMessage on fetch failure", async () => {
    let fail = false;
    const { result } = renderHook(() =>
      useAsync(
        async () => {
          if (fail) throw new Error("boom");
          return [1, 2];
        },
        [],
        [] as number[],
      ),
    );
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.data).toEqual([1, 2]);

    fail = true;
    await act(async () => {
      await result.current.refresh();
    });
    expect(result.current.error).toBe("boom");
    // data reset to the initial value (the pages' old `setX([])` convention)
    expect(result.current.data).toEqual([]);
  });

  it("skips the fetcher while disabled, then loads when enabled flips true", async () => {
    const fetcher = vi.fn(async () => 7);
    const { result, rerender } = renderHook(
      ({ enabled }: { enabled: boolean }) => useAsync(fetcher, [enabled], 0, { enabled }),
      { initialProps: { enabled: false } },
    );

    expect(result.current.loading).toBe(false);
    expect(result.current.data).toBe(0);
    expect(fetcher).not.toHaveBeenCalled();

    await act(async () => {
      rerender({ enabled: true });
    });
    await waitFor(() => expect(result.current.data).toBe(7));
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("refetches when deps change", async () => {
    const fetcher = vi.fn(async (n: number) => n * 10);
    const { result, rerender } = renderHook(
      ({ db }: { db: string }) => useAsync(() => fetcher(db.length), [db], 0),
      {
        initialProps: { db: "" },
      },
    );
    await waitFor(() => expect(result.current.data).toBe(0));
    expect(fetcher).toHaveBeenCalledTimes(1);

    await act(async () => {
      rerender({ db: "abcd" });
    });
    await waitFor(() => expect(result.current.data).toBe(40));
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("refresh is stable across renders and does not refetch on its own", async () => {
    const fetcher = vi.fn(async () => 1);
    const { result, rerender } = renderHook(() => useAsync(fetcher, [], 0));
    await waitFor(() => expect(result.current.loading).toBe(false));
    const refreshBefore = result.current.refresh;
    rerender();
    expect(result.current.refresh).toBe(refreshBefore);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });
});
