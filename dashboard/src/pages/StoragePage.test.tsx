import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FileMeta } from "../lib/types";

// StoragePage lists a database's stored files and wires upload/delete to
// AdminClient. The list must render on db load; delete must confirm first then
// call deleteFile; upload must call uploadFile with the chosen file's bytes.
// Copy-URL mints a signed, expiring link via GET /api/storage/{db}/{id}/
// signed-url (same-origin session cookie) — never copies a bare /storage link,
// which 403s once the server requires signed URLs — and disables the row's
// copy button with an inline reason when minting fails.

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

const SIGNED_URL = `https://rtdb.example/storage/${fileRow.id}?exp=1750003600000&sig=abc123`;

function stubFetchSignedUrl(url: string = SIGNED_URL) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => Response.json({ url, expiresAt: 1_750_003_600_000 }, { status: 200 })),
  );
}

describe("StoragePage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listFiles.mockResolvedValue([]);
    adminClientMock.deleteFile.mockResolvedValue({ ok: true });
    adminClientMock.uploadFile.mockResolvedValue({ id: "newid001-aaaa" });
  });
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
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

  it("renders a single header image-size selector", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    render(<StoragePage />);
    await screen.findByText("file0001");
    // The size control is one select in the actions column header, not per row.
    const select = screen.getByRole("combobox", { name: /size/i });
    expect(select).toBeInTheDocument();
  });

  it("mints a signed URL with the selected image transform before copying", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    const signedTransformed = `https://rtdb.example/storage/${fileRow.id}?w=512&fit=contain&exp=1750003600000&sig=def456`;
    stubFetchSignedUrl(signedTransformed);
    const user = userEvent.setup();
    render(<StoragePage />);
    const row = (await screen.findByText("file0001")).closest("tr");
    if (!row) throw new Error("row not found");

    const writeText = vi.fn<(text: string) => Promise<void>>().mockResolvedValue(undefined);
    vi.spyOn(navigator, "clipboard", "get").mockReturnValue({ writeText } as never);

    // Transform params must be in the mint request so the server binds them
    // into the signature — never append them locally to the signed URL.
    await user.selectOptions(screen.getByRole("combobox", { name: /size/i }), "md");
    await user.click(within(row).getByRole("button", { name: "copy signed URL" }));

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        `${window.location.origin}/api/storage/db1/${fileRow.id}/signed-url?w=512&fit=contain`,
        { credentials: "include" },
      );
      expect(writeText).toHaveBeenCalledWith(signedTransformed);
    });
  });

  it("copies the minted signed URL, not a bare link, when size is original", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    stubFetchSignedUrl();
    const user = userEvent.setup();
    render(<StoragePage />);
    const row = (await screen.findByText("file0001")).closest("tr");
    if (!row) throw new Error("row not found");

    const writeText = vi.fn<(text: string) => Promise<void>>().mockResolvedValue(undefined);
    vi.spyOn(navigator, "clipboard", "get").mockReturnValue({ writeText } as never);

    const copy = within(row).getByRole("button", { name: "copy signed URL" });
    expect(copy).toHaveAttribute("title", "copies a signed, time-limited URL (it expires)");
    await user.click(copy);
    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith(
        `${window.location.origin}/api/storage/db1/${fileRow.id}/signed-url`,
        { credentials: "include" },
      );
      expect(writeText).toHaveBeenCalledWith(SIGNED_URL);
    });
    expect(writeText).not.toHaveBeenCalledWith(`${window.location.origin}/storage/${fileRow.id}`);
  });

  it("disables copy with an inline reason if signed-URL mint fails", async () => {
    adminClientMock.listFiles.mockResolvedValue([fileRow]);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json({ code: "FORBIDDEN", message: "denied" }, { status: 403 })),
    );
    const user = userEvent.setup();
    render(<StoragePage />);
    const row = (await screen.findByText("file0001")).closest("tr");
    if (!row) throw new Error("row not found");

    const writeText = vi.fn<(text: string) => Promise<void>>().mockResolvedValue(undefined);
    vi.spyOn(navigator, "clipboard", "get").mockReturnValue({ writeText } as never);

    await user.click(within(row).getByRole("button", { name: "copy signed URL" }));
    const disabled = await within(row).findByRole("button", { name: "copy disabled" });
    expect(disabled).toBeDisabled();
    expect(disabled).toHaveAttribute(
      "title",
      expect.stringContaining("signed-URL mint failed (HTTP 403)"),
    );
    expect(within(row).getByRole("alert")).toHaveTextContent(
      "copy disabled — signed-URL mint failed (HTTP 403)",
    );
    expect(writeText).not.toHaveBeenCalled();
  });
});
