import type { ChangeOp, RtDbAdminClient } from "@par-rt-db/client";
import { RtDbError } from "@par-rt-db/client";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const deletedIdsForTest = vi.hoisted(() => new Set<string>());
const mocks = vi.hoisted(() => ({
  getSchema: vi.fn(),
  adminMutate: vi.fn(),
  adminChanges: vi.fn(),
  adminQuery: vi.fn(),
}));
vi.mock("../lib/admin", () => ({ useAdmin: () => ({ client: mocks }) }));
vi.mock("../lib/useLiveTable", () => ({
  useLiveTable: () => ({
    deletedIds: deletedIdsForTest,
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

describe("document editor", () => {
  async function openEdit() {
    mount();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Edit" }));
    return user;
  }
  function setJson(text: string) {
    const box = screen.getByLabelText("Document JSON") as HTMLTextAreaElement;
    fireEvent.change(box, { target: { value: text } });
  }

  it("preloads the body without system fields and saves a version-guarded replace", async () => {
    const user = await openEdit();
    const box = screen.getByLabelText("Document JSON") as HTMLTextAreaElement;
    expect(JSON.parse(box.value)).toEqual({ name: "current", extra: true });
    expect(screen.getByRole("button", { name: "Save changes" })).toBeDisabled();
    setJson('{"name":"renamed","extra":true}');
    expect(screen.getByText(/1 field changed: name \(changed\)/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Save changes" }));
    expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
      steps: [
        { op: "expectVersion", table: "docs", id: "d1", version: 3 },
        { op: "replace", table: "docs", id: "d1", doc: { name: "renamed", extra: true } },
      ],
    });
    await waitFor(() => expect(screen.queryByLabelText("Document JSON")).not.toBeInTheDocument());
  });

  it("blocks save on malformed JSON without calling the server", async () => {
    await openEdit();
    setJson("{nope");
    expect(screen.getByRole("alert")).toHaveTextContent(/Invalid JSON/);
    expect(screen.getByRole("button", { name: "Save changes" })).toBeDisabled();
    expect(mocks.adminMutate).not.toHaveBeenCalled();
  });

  it("shows the server {code,message} envelope and keeps the editor open when rejected", async () => {
    mocks.adminMutate.mockRejectedValueOnce(
      RtDbError.fromEnvelope({ code: "SCHEMA_VIOLATION", message: "name must be string" }, 400),
    );
    const user = await openEdit();
    setJson('{"name":5}');
    await user.click(screen.getByRole("button", { name: "Save changes" }));
    const alert = await screen.findByText(/Rejected, not saved/);
    expect(alert).toHaveTextContent("SCHEMA_VIOLATION: name must be string");
    expect(screen.getByLabelText("Document JSON")).toBeInTheDocument();
  });

  it("creates a document through insert", async () => {
    mount();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "New document" }));
    setJson('{"name":"fresh"}');
    await user.click(screen.getByRole("button", { name: "Create document" }));
    expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
      steps: [{ op: "insert", table: "docs", doc: { name: "fresh" } }],
    });
  });

  it("deletes only after the confirm step", async () => {
    mount();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "delete" }));
    expect(mocks.adminMutate).not.toHaveBeenCalled();
    await user.click(screen.getByRole("button", { name: "confirm delete" }));
    expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
      steps: [{ op: "delete", table: "docs", id: "d1" }],
    });
  });
});

describe("soft-deleted rows", () => {
  it("offers show-deleted and undelete on a softDelete table", async () => {
    mocks.getSchema.mockResolvedValue({ tables: { docs: { fields: {}, softDelete: true } } });
    deletedIdsForTest.add("d1");
    try {
      mount();
      const user = userEvent.setup();
      await screen.findByLabelText("show deleted");
      expect(screen.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
      await user.click(screen.getByRole("button", { name: "undelete" }));
      expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
        steps: [{ op: "undelete", table: "docs", id: "d1" }],
      });
    } finally {
      deletedIdsForTest.clear();
    }
  });
});
