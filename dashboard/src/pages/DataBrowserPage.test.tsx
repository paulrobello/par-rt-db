import type { ChangeOp, RtDbAdminClient } from "@par-rt-db/client";
import { RtDbError } from "@par-rt-db/client";
import { cleanup, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getSchema: vi.fn(),
  adminMutate: vi.fn(),
  adminChanges: vi.fn(),
}));
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

function op(seq: number, doc: Record<string, unknown> | null, docId = "d1"): ChangeOp {
  return { seq, table: "docs", docId, kind: "patch", doc, ts: 1758300000000 + seq };
}
function feed(
  ops: ChangeOp[],
  nextSeq = 100,
  head = 100,
): ReturnType<RtDbAdminClient["adminChanges"]> {
  return Promise.resolve({ ops, nextSeq, head, logId: "log1" });
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
  mocks.adminChanges.mockReset().mockImplementation((_db: string, opts: { since: number }) =>
    feed(
      versions.filter((v) => v.seq > opts.since).map((v) => ({ ...v })),
      100,
      100,
    ),
  );
});
afterEach(cleanup);

describe("document history", () => {
  it("lists three patch post-images in sequence order, filtering other documents", async () => {
    mocks.adminChanges.mockImplementation((_db: string, opts: { since: number }) =>
      feed([...versions, op(4, { name: "other" }, "other")].filter((v) => v.seq > opts.since)),
    );
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
    expect(mocks.adminChanges).toHaveBeenCalledWith(
      "test-db",
      expect.objectContaining({ since: 0, table: "docs", limit: 500 }),
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
    mocks.adminChanges.mockImplementation((_db: string, opts: { since: number }) => {
      const since = opts.since;
      const ops = Array.from({ length: 500 }, (_, i) => op(since + i + 1, { value: i }, "other"));
      return feed(ops, since + 500, 6000);
    });
    mount();
    await openHistory();
    expect(await screen.findByText(/Scan stopped after 5000 ops/)).toBeInTheDocument();
    expect(mocks.adminChanges).toHaveBeenCalledTimes(10);
  });
  it("surfaces expired retention without inventing versions", async () => {
    mocks.adminChanges.mockRejectedValueOnce(
      RtDbError.fromEnvelope({ code: "CURSOR_EXPIRED", message: "cursor predates retention" }, 410),
    );
    mount();
    await userEvent.setup().click(screen.getByRole("button", { name: "History" }));
    expect(await screen.findByText(/No missing versions have been inferred/)).toBeInTheDocument();
    expect(screen.queryByLabelText("Version history")).not.toBeInTheDocument();
  });
  it("pages forward and rejects a changed log identity", async () => {
    const client = {
      adminChanges: vi
        .fn()
        .mockImplementationOnce(() => feed([op(1, { a: 1 })], 1, 2))
        .mockImplementationOnce(() =>
          Promise.resolve({ ops: [], nextSeq: 2, head: 2, logId: "different" }),
        ),
    };
    await expect(
      loadDocumentHistory(
        client as unknown as RtDbAdminClient,
        "db",
        "docs",
        "d1",
        new AbortController().signal,
      ),
    ).rejects.toThrow("recreated");
    expect(client.adminChanges.mock.calls[1][1]).toEqual(expect.objectContaining({ since: 1 }));
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
