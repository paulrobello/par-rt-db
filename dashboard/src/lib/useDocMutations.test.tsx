import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { RtDbError } from "@par-rt-db/client";
import { useDocMutations } from "./useDocMutations";

// The data browser's mutation surface (extracted from DataBrowserPage in QA-007):
// every flow routes through `mutate` → `client.adminMutate` + refresh, with
// error/busy state and the patch/delete/undelete composers on top.

function makeClient() {
  return { adminMutate: vi.fn<() => Promise<void>>().mockResolvedValue(undefined) };
}

describe("useDocMutations", () => {
  let client: ReturnType<typeof makeClient>;
  beforeEach(() => {
    client = makeClient();
  });

  it("mutate sends steps to adminMutate, refreshes on success, clears busy", async () => {
    const refresh = vi.fn();
    const { result } = renderHook(() => useDocMutations(client as never, "db1", "docs", refresh));

    let ok = false;
    await act(async () => {
      ok = await result.current.mutate([{ op: "insert", table: "docs", doc: { a: 1 } }]);
    });
    expect(ok).toBe(true);
    expect(client.adminMutate).toHaveBeenCalledWith("db1", {
      steps: [{ op: "insert", table: "docs", doc: { a: 1 } }],
    });
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(result.current.mutBusy).toBe(false);
    expect(result.current.mutError).toBeNull();
  });

  it("surfaces the wire error envelope on failure and returns false", async () => {
    client.adminMutate.mockRejectedValue(new RtDbError("CONFLICT", "version clash"));
    const refresh = vi.fn();
    const { result } = renderHook(() => useDocMutations(client as never, "db1", "docs", refresh));

    let ok = true;
    await act(async () => {
      ok = await result.current.mutate([{ op: "delete", table: "docs", id: "d1" }]);
    });
    expect(ok).toBe(false);
    expect(result.current.mutError).toBe("CONFLICT: version clash");
    expect(refresh).not.toHaveBeenCalled();
    expect(result.current.mutBusy).toBe(false);
  });

  it("doPatch rejects invalid JSON and missing ids without calling adminMutate", async () => {
    const refresh = vi.fn();
    const { result } = renderHook(() => useDocMutations(client as never, "db1", "docs", refresh));

    await act(async () => {
      result.current.setPatchFields("not json");
      result.current.setPatchId("d1");
    });
    await act(async () => {
      await result.current.doPatch();
    });
    expect(result.current.mutError).toBe("invalid JSON for fields");
    expect(client.adminMutate).not.toHaveBeenCalled();

    await act(async () => {
      result.current.setPatchFields("{}");
      result.current.setPatchId("   ");
    });
    await act(async () => {
      await result.current.doPatch();
    });
    expect(result.current.mutError).toBe("document id is required");
    expect(client.adminMutate).not.toHaveBeenCalled();
  });

  it("doPatch succeeds: patches by trimmed id, closes the patch dialog", async () => {
    const refresh = vi.fn();
    const { result } = renderHook(() => useDocMutations(client as never, "db1", "docs", refresh));

    await act(async () => {
      result.current.setPatchOpen(true);
      result.current.setPatchFields('{"score": 5}');
      result.current.setPatchId("  d9 ");
    });
    await act(async () => {
      await result.current.doPatch();
    });
    expect(client.adminMutate).toHaveBeenCalledWith("db1", {
      steps: [{ op: "patch", table: "docs", id: "d9", fields: { score: 5 } }],
    });
    expect(result.current.patchOpen).toBe(false);
    expect(refresh).toHaveBeenCalledTimes(1);
  });

  it("doDelete clears the inline confirm only on success; doUndelete always restores", async () => {
    const refresh = vi.fn();
    const { result } = renderHook(() => useDocMutations(client as never, "db1", "docs", refresh));

    await act(async () => {
      result.current.setConfirmId("d1");
      await result.current.doUndelete("d1");
    });
    expect(client.adminMutate).toHaveBeenLastCalledWith("db1", {
      steps: [{ op: "undelete", table: "docs", id: "d1" }],
    });

    await act(async () => {
      result.current.setConfirmId("d2");
      await result.current.doDelete("d2");
    });
    expect(client.adminMutate).toHaveBeenLastCalledWith("db1", {
      steps: [{ op: "delete", table: "docs", id: "d2" }],
    });
    // success cleared the confirm state
    expect(result.current.confirmId).toBeNull();

    await act(async () => {
      result.current.setConfirmId("d3");
      client.adminMutate.mockRejectedValueOnce(new Error("row gone"));
      await result.current.doDelete("d3");
    });
    // failure keeps the confirm open so the operator can retry
    expect(result.current.confirmId).toBe("d3");
    expect(result.current.mutError).toBe("row gone");
  });
});
