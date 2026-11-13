import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SchemaJson } from "@par-rt-db/client";

// Gap coverage for DataBrowserPage beyond DataBrowserPage.test.tsx: the
// toolbar (order/take/show-deleted feeding useLiveTable), CSV/JSON export
// buttons, the patch-by-id composer (validation, success, server error), the
// empty/loading/error list states, the schema-load error, and the live vs
// polling status lamp.

const mocks = vi.hoisted(() => {
  const state = {
    docs: [
      { _id: "doc-0001", _creationTime: 1_700_000_000_000, name: "first" },
      { _id: "doc-0002", _creationTime: 1_700_000_001_000, name: "second" },
    ] as Array<Record<string, unknown> & { _id: string; _creationTime: number }>,
    loading: false,
    error: null as string | null,
    live: true,
    deletedIds: new Set<string>(),
    refresh: async () => {},
    lastArgs: null as unknown[] | null,
  };
  return {
    state,
    getSchema: vi.fn<() => Promise<SchemaJson>>(),
    adminMutate: vi.fn(),
  };
});

vi.mock("../../lib/admin", () => ({
  useAdmin: () => ({ client: { getSchema: mocks.getSchema, adminMutate: mocks.adminMutate } }),
}));

vi.mock("../../lib/useLiveTable", () => ({
  useLiveTable: (...args: unknown[]) => {
    mocks.state.lastArgs = args;
    return mocks.state;
  },
}));

import { DataBrowserPage } from "../DataBrowserPage";

function mount(): ReturnType<typeof render> {
  return render(
    // eslint-disable-next-line testing-library/no-wait-for-side-effects
    <MemoryRouter initialEntries={["/dbs/test-db/tables/docs"]}>
      <Routes>
        <Route path="/dbs/:db/tables/:table" element={<DataBrowserPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

import { MemoryRouter, Route, Routes } from "react-router-dom";

const nameField = { type: "String" } as const;
const defaultSchema = {
  tables: { docs: { fields: { name: nameField } } },
} as unknown as SchemaJson;

beforeEach(() => {
  mocks.getSchema.mockReset().mockResolvedValue(defaultSchema);
  mocks.adminMutate.mockReset().mockResolvedValue([]);
  mocks.state.docs = [
    { _id: "doc-0001", _creationTime: 1_700_000_000_000, name: "first" },
    { _id: "doc-0002", _creationTime: 1_700_000_001_000, name: "second" },
  ];
  mocks.state.loading = false;
  mocks.state.error = null;
  mocks.state.live = true;
  mocks.state.deletedIds.clear();
});

describe("DataBrowserPage toolbar and list states", () => {
  it("renders the breadcrumb placard, rows, and the live status lamp", async () => {
    mount();
    expect(await screen.findByText("Data · test-db / docs")).toBeInTheDocument();
    expect(screen.getByText(/live · \/sync/)).toBeInTheDocument();
    expect(screen.getByText("…doc-0001")).toBeInTheDocument(); // truncated _id cell
    expect(screen.getByText("first", { selector: "td" })).toBeInTheDocument();
    expect(screen.getByText("second", { selector: "td" })).toBeInTheDocument();
  });

  it("shows the polling lamp when the table is not live", () => {
    mocks.state.live = false;
    mount();
    expect(screen.getByText(/polling · 2s/)).toBeInTheDocument();
  });

  it("shows the empty, loading, and error list states", async () => {
    mocks.state.docs = [];
    const empty = mount();
    expect(await screen.findByText("No documents.")).toBeInTheDocument();
    empty.unmount();

    mocks.state.loading = true;
    const loading = mount();
    expect(screen.getByText("loading documents")).toBeInTheDocument();
    // Exports stay disabled mid-load.
    expect(screen.getByRole("button", { name: "Export CSV" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Export JSON" })).toBeDisabled();
    loading.unmount();

    mocks.state.loading = false;
    mocks.state.error = "sync failed";
    const failed = mount();
    expect(await screen.findByText("sync failed")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Export CSV" })).toBeDisabled();
    failed.unmount();
  });

  it("feeds the order segment, take select, and show-deleted checkbox into useLiveTable", async () => {
    mocks.getSchema.mockResolvedValue({
      tables: { docs: { fields: { name: { type: "String" } }, softDelete: true } },
    } as unknown as SchemaJson);
    const user = userEvent.setup();
    mount();
    await screen.findByText("…doc-0001");
    expect(mocks.state.lastArgs?.[4]).toBe(false); // showDeleted starts off

    await user.click(screen.getByRole("button", { name: "asc" }));
    expect(mocks.state.lastArgs?.[2]).toBe("asc");
    await user.selectOptions(screen.getByLabelText("take"), "200");
    expect(mocks.state.lastArgs?.[3]).toBe(200);
    await user.click(screen.getByLabelText("show deleted"));
    expect(mocks.state.lastArgs?.[4]).toBe(true);

    // The default order button is marked active; after switching, asc is.
    expect(screen.getByRole("button", { name: "asc" }).className).toContain("segBtnActive");
    expect(screen.getByRole("button", { name: "desc" }).className).not.toContain("segBtnActive");
  });

  it("hides the show-deleted toggle without a softDelete table", async () => {
    mount();
    await screen.findByText("…doc-0001");
    expect(screen.queryByLabelText("show deleted")).not.toBeInTheDocument();
  });

  it("exports CSV and JSON through a client-side download", async () => {
    const created: Blob[] = [];
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const original = URL.createObjectURL;
    URL.createObjectURL = vi.fn((blob: Blob) => {
      created.push(blob);
      return "blob:mock";
    }) as typeof URL.createObjectURL;
    try {
      const user = userEvent.setup();
      mount();
      await screen.findByText("…doc-0001");
      await user.click(screen.getByRole("button", { name: "Export CSV" }));
      await user.click(screen.getByRole("button", { name: "Export JSON" }));
      expect(clickSpy).toHaveBeenCalledTimes(2);
      expect(created).toHaveLength(2);
      expect(created[0].type).toBe("text/csv;charset=utf-8");
      expect(created[1].type).toBe("application/json");
      const csv = await created[0].text();
      expect(csv).toContain("_id,name,_creationTime");
      expect(csv).toContain("first");
      const json = JSON.parse(await created[1].text());
      expect(json).toHaveLength(2);
      expect(json[0].name).toBe("first");
    } finally {
      clickSpy.mockRestore();
      URL.createObjectURL = original;
      URL.revokeObjectURL("blob:mock");
    }
  });

  it("surfaces a schema load failure above the table", async () => {
    mocks.getSchema.mockRejectedValue(new Error("schema locked"));
    mount();
    expect(await screen.findByText(/Failed to load schema: schema locked/)).toBeInTheDocument();
    // The table still renders — schema loss degrades columns, not the list.
    expect(await screen.findByText("…doc-0001")).toBeInTheDocument();
  });
});

describe("patch-by-id composer", () => {
  async function openPatch() {
    const user = userEvent.setup();
    mount();
    await screen.findByText("…doc-0001");
    await user.click(screen.getByRole("button", { name: "Patch" }));
    expect(screen.getByText("Patch document")).toBeInTheDocument();
    return user;
  }

  it("requires a document id before patching", async () => {
    const user = await openPatch();
    await user.click(
      within(screen.getByText("Patch document").closest("div") as HTMLElement).getByRole("button", {
        name: "Patch",
      }),
    );
    expect(screen.getByText("document id is required")).toBeInTheDocument();
    expect(mocks.adminMutate).not.toHaveBeenCalled();
  });

  it("rejects malformed fields JSON without calling the server", async () => {
    const user = await openPatch();
    const composer = screen.getByText("Patch document").closest("div") as HTMLElement;
    fireEvent.change(within(composer).getByPlaceholderText("document _id"), {
      target: { value: "doc-0001" },
    });
    fireEvent.change(within(composer).getByPlaceholderText('{"field":"value"}'), {
      target: { value: "{bad" },
    });
    await user.click(within(composer).getByRole("button", { name: "Patch" }));
    expect(screen.getByText("invalid JSON for fields")).toBeInTheDocument();
    expect(mocks.adminMutate).not.toHaveBeenCalled();
  });

  it("sends a patch step and closes the composer on success", async () => {
    const user = await openPatch();
    const composer = screen.getByText("Patch document").closest("div") as HTMLElement;
    fireEvent.change(within(composer).getByPlaceholderText("document _id"), {
      target: { value: " doc-0001 " },
    });
    fireEvent.change(within(composer).getByPlaceholderText('{"field":"value"}'), {
      target: { value: '{"name":"patched"}' },
    });
    await user.click(within(composer).getByRole("button", { name: "Patch" }));
    await waitFor(() => {
      expect(mocks.adminMutate).toHaveBeenCalledWith("test-db", {
        steps: [{ op: "patch", table: "docs", id: "doc-0001", fields: { name: "patched" } }],
      });
    });
    // Success refreshes the live table and closes the composer.
    await waitFor(() => {
      expect(screen.queryByText("Patch document")).not.toBeInTheDocument();
    });
  });

  it("shows the server error and keeps the composer open on failure", async () => {
    mocks.adminMutate.mockRejectedValue(new Error("write rejected"));
    const user = await openPatch();
    const composer = screen.getByText("Patch document").closest("div") as HTMLElement;
    fireEvent.change(within(composer).getByPlaceholderText("document _id"), {
      target: { value: "doc-0001" },
    });
    await user.click(within(composer).getByRole("button", { name: "Patch" }));
    expect(await screen.findByText("write rejected")).toBeInTheDocument();
    expect(screen.getByText("Patch document")).toBeInTheDocument();
  });

  it("toggles the composer closed on a second Patch click", async () => {
    const user = await openPatch();
    // Two "Patch" buttons are mounted (toolbar + composer); the first is the
    // toolbar toggle that closes the composer.
    await user.click(screen.getAllByRole("button", { name: "Patch" })[0]);
    expect(screen.queryByText("Patch document")).not.toBeInTheDocument();
  });
});
