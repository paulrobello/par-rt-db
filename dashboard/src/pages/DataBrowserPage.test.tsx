import type { ChangeOp } from "@par-rt-db/client";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ getSchema: vi.fn(), adminMutate: vi.fn() }));
vi.mock("../lib/admin", () => ({ useAdmin: () => ({ client: mocks }) }));
vi.mock("../lib/useLiveTable", () => ({
  useLiveTable: () => ({
    docs: [{ _id: "d1", _creationTime: 1, _version: 3, name: "current", extra: true }],
    loading: false,
    error: null,
    live: false,
    refresh: async () => {},
  }),
}));

import { diffDocuments, loadDocumentHistory } from "../lib/changeFeed";
import { DataBrowserPage } from "./DataBrowserPage";

const fetchMock = vi.fn();
function op(seq: number, doc: Record<string, unknown> | null, docId = "d1"): ChangeOp {
  return { seq, table: "docs", docId, kind: "patch", doc, ts: 1758300000000 + seq };
}
function page(ops: ChangeOp[], nextSeq = 100, head = 100): Response {
  return new Response(JSON.stringify({ ops, nextSeq, head, logId: "log1" }), { status: 200 });
}
function mount(): void {
  render(
    <MemoryRouter initialEntries={["/dbs/test-db/tables/docs"]}>
      <Routes>
        <Route path="/dbs/:db/tables/:table" element={<DataBrowserPage />} />
      </Routes>
    </MemoryRouter>,
  );
}
async function openHistory(): Promise<ReturnType<typeof userEvent.setup>> {
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "History" }));
  await user.type(screen.getByLabelText("Machine token"), "existing-token");
  await user.click(screen.getByRole("button", { name: "Load history" }));
  await screen.findByLabelText("Version history");
  return user;
}
const versions = [
  op(1, { _id: "d1", _creationTime: 1, _version: 1, name: "first", nested: { a: 1 } }),
  op(2, { _id: "d1", _creationTime: 1, _version: 2, name: "second", nested: { a: 1 } }),
  op(3, { _id: "d1", _creationTime: 1, _version: 3, name: "third", nested: { a: 1 } }),
];
beforeEach(() => {
  mocks.getSchema.mockReset().mockResolvedValue({ tables: {} });
  mocks.adminMutate.mockReset().mockResolvedValue([]);
  fetchMock.mockReset().mockImplementation(async () => page(versions));
  vi.stubGlobal("fetch", fetchMock);
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("document history", () => {
  it("lists three patch post-images in sequence order, filtering other documents", async () => {
    fetchMock.mockResolvedValueOnce(page([...versions, op(4, { name: "other" }, "other")]));
    mount();
    await openHistory();
    const list = screen.getByLabelText("Version history");
    expect(
      within(list)
        .getAllByText(/^seq /)
        .map((node) => node.textContent?.split(" · ")[0]),
    ).toEqual(["seq 1", "seq 2", "seq 3"]);
    for (const name of ["first", "second", "third"]) {
      expect(within(list).getByText(new RegExp(`"name": "${name}"`))).toBeInTheDocument();
    }
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("since=0"),
      expect.objectContaining({
        headers: { Authorization: "Bearer existing-token" },
        credentials: "omit",
      }),
    );
    expect(localStorage.length).toBe(0);
  });
  it("highlights the changed field between two selected entries", async () => {
    mount();
    const user = await openHistory();
    await user.click(screen.getByLabelText("compare from seq 1"));
    await user.click(screen.getByLabelText("compare to seq 2"));
    const field = screen.getByText("name (changed)");
    expect(field.parentElement?.className).toContain("diffChanged");
    expect(within(field.parentElement as HTMLElement).getByText("first")).toBeInTheDocument();
    expect(within(field.parentElement as HTMLElement).getByText("second")).toBeInTheDocument();
    expect(screen.queryByText("nested (changed)")).not.toBeInTheDocument();
  });
  it("confirms replacement with the chosen body, strips system fields, and guards the current version", async () => {
    mount();
    const user = await openHistory();
    await user.click(screen.getAllByRole("button", { name: "Restore this version" })[0]);
    expect(mocks.adminMutate).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "confirm restore" }));
    expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
      steps: [
        { op: "expectVersion", table: "docs", id: "d1", version: 3 },
        { op: "replace", table: "docs", id: "d1", doc: { name: "first", nested: { a: 1 } } },
      ],
    });
    await screen.findByRole("status");
  });
  it("caps scanning at ten pages and shows the bound/retention disclosure", async () => {
    fetchMock.mockImplementation(async (url: string) => {
      const since = Number(new URL(url, "http://local").searchParams.get("since"));
      const ops = Array.from({ length: 500 }, (_, i) => op(since + i + 1, { value: i }, "other"));
      return page(ops, since + 500, 6000);
    });
    mount();
    await openHistory();
    expect(await screen.findByText(/Scan stopped after 5000 ops/)).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledTimes(10);
  });
  it("surfaces expired retention without inventing versions", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({ code: "CURSOR_EXPIRED", message: "cursor predates retention" }),
        { status: 410 },
      ),
    );
    mount();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "History" }));
    await user.type(screen.getByLabelText("Machine token"), "token");
    await user.click(screen.getByRole("button", { name: "Load history" }));
    expect(await screen.findByText(/No missing versions have been inferred/)).toBeInTheDocument();
    expect(screen.queryByLabelText("Version history")).not.toBeInTheDocument();
  });
  it("pages forward and rejects a changed log identity", async () => {
    fetchMock.mockResolvedValueOnce(page([op(1, { a: 1 })], 1, 2));
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ ops: [], nextSeq: 2, head: 2, logId: "different" })),
    );
    await expect(
      loadDocumentHistory("db", "docs", "d1", "token", new AbortController().signal),
    ).rejects.toThrow("recreated");
    expect(fetchMock.mock.calls[1][0]).toContain("since=1");
  });
  it("compares keys and arrays structurally", () => {
    expect(
      diffDocuments(
        { obj: { a: 1, b: 2 }, removed: 1, list: [1] },
        { obj: { b: 2, a: 1 }, added: 2, list: [2] },
      ),
    ).toEqual([
      { field: "added", kind: "added", before: undefined, after: 2 },
      { field: "list", kind: "changed", before: [1], after: [2] },
      { field: "removed", kind: "removed", before: 1, after: undefined },
    ]);
  });
});
