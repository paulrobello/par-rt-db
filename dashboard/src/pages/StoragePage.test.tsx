import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FileMeta } from "../lib/types";

// StoragePage lists a database's stored files and wires upload/delete to
// AdminClient. The list must render on db load; delete must confirm first then
// call deleteFile; upload must call uploadFile with the chosen file's bytes.

const adminClientMock = vi.hoisted(() => ({
  listFiles: vi.fn(),
  uploadFile: vi.fn(),
  deleteFile: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { StoragePage } from "./StoragePage";

const fileRow: FileMeta = {
  id: "file0001-aaaa-bbbb-cccc-dddddddddddd",
  sha256: "abcdef0123456789fedcba9876543210".repeat(2),
  size: 2048,
  contentType: "text/plain",
  creationTime: 1_750_000_000_000,
};

describe("StoragePage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listFiles.mockResolvedValue([]);
    adminClientMock.deleteFile.mockResolvedValue({ ok: true });
    adminClientMock.uploadFile.mockResolvedValue({ id: "newid001-aaaa" });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("lists files for the selected database", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    render(<StoragePage />);

    expect(await screen.findByText("file0001")).toBeInTheDocument();
    expect(screen.getByText("2.0 KB")).toBeInTheDocument();
    expect(screen.getByText("text/plain")).toBeInTheDocument();
  });

  it("renders an empty state when there are no files", async () => {
    render(<StoragePage />);
    expect(await screen.findByText("no stored files.")).toBeInTheDocument();
  });

  it("requires a confirm click before deleting a file", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    const user = userEvent.setup();
    render(<StoragePage />);
    const row = (await screen.findByText("file0001")).closest("tr");
    if (!row) throw new Error("row not found");

    // First click arms confirmation; delete must not fire yet.
    await user.click(within(row).getByRole("button", { name: "delete" }));
    expect(adminClientMock.deleteFile).not.toHaveBeenCalled();

    // Confirm click fires the delete.
    await user.click(within(row).getByRole("button", { name: "confirm" }));
    await waitFor(() => {
      expect(adminClientMock.deleteFile).toHaveBeenCalledWith("db1", fileRow.id);
    });
  });

  it("uploads the chosen file via uploadFile", async () => {
    adminClientMock.listFiles.mockResolvedValue([]);
    const user = userEvent.setup();
    const { container } = render(<StoragePage />);
    await screen.findByText("no stored files.");

    const input = container.querySelector('input[type="file"]');
    if (!input) throw new Error("file input not found");
    const file = new File(["hello storage"], "note.txt", { type: "text/plain" });
    await user.upload(input as HTMLInputElement, file);

    await user.click(screen.getByRole("button", { name: "upload" }));
    await waitFor(() => {
      expect(adminClientMock.uploadFile).toHaveBeenCalledWith("db1", file);
    });
  });
});
