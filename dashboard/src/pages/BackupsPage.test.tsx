import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// BackupsPage lists existing backups newest-first and wires download /
// restore (exact-name confirm) / delete (inline confirm) / back-up-now to
// AdminClient. The list must render on load; restore must require the
// operator to type the exact dump name before it fires.

const adminClientMock = vi.hoisted(() => ({
  listBackups: vi.fn(),
  backupNow: vi.fn(),
  downloadBackup: vi.fn(),
  deleteBackup: vi.fn(),
  restoreBackup: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ client: adminClientMock }),
}));

import { BackupsPage } from "./BackupsPage";

const dumpOld = {
  name: "rtdb-20260701T000000Z.dump",
  sizeBytes: 512,
  createdMs: 1_750_000_000_000,
};
const dumpNew = {
  name: "rtdb-20260728T143045Z.dump",
  sizeBytes: 1024,
  createdMs: 1_756_302_845_000,
};

describe("BackupsPage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listBackups.mockResolvedValue({ running: false, backups: [] });
    adminClientMock.backupNow.mockResolvedValue(undefined);
    adminClientMock.downloadBackup.mockResolvedValue(undefined);
    adminClientMock.deleteBackup.mockResolvedValue(undefined);
    adminClientMock.restoreBackup.mockResolvedValue({
      target: "db://primary",
      instructions: "stop the server, restore the dump, restart",
    });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("renders an empty state when there are no backups", async () => {
    render(<BackupsPage />);
    expect(await screen.findByText(/No backups yet/i)).toBeInTheDocument();
  });

  it("lists backups newest-first", async () => {
    adminClientMock.listBackups.mockResolvedValue({
      running: false,
      backups: [dumpOld, dumpNew],
    });
    render(<BackupsPage />);
    const cells = await screen.findAllByText(/rtdb-\d+T\d+Z\.dump/);
    // newest-first: dumpNew appears before dumpOld in DOM order
    const names = cells.map((c) => c.textContent ?? "");
    expect(names.indexOf(dumpNew.name)).toBeLessThan(names.indexOf(dumpOld.name));
  });

  it("triggers backupNow on the Back up now click", async () => {
    const user = userEvent.setup();
    render(<BackupsPage />);
    await screen.findByText(/No backups yet/i);
    await user.click(screen.getByRole("button", { name: /back up now/i }));
    await waitFor(() => expect(adminClientMock.backupNow).toHaveBeenCalled());
  });

  it("downloads a backup via downloadBackup(name)", async () => {
    adminClientMock.listBackups.mockResolvedValue({
      running: false,
      backups: [dumpNew],
    });
    const user = userEvent.setup();
    render(<BackupsPage />);
    await screen.findByText(dumpNew.name);
    await user.click(screen.getByRole("button", { name: /^download$/i }));
    await waitFor(() => expect(adminClientMock.downloadBackup).toHaveBeenCalledWith(dumpNew.name));
  });

  it("requires an exact-name confirm before deleting", async () => {
    adminClientMock.listBackups.mockResolvedValue({
      running: false,
      backups: [dumpNew],
    });
    const user = userEvent.setup();
    render(<BackupsPage />);
    const row = (await screen.findByText(dumpNew.name)).closest("tr");
    if (!row) throw new Error("row not found");

    // First click arms confirmation; delete must not fire yet.
    await user.click(within(row).getByRole("button", { name: /^delete$/i }));
    expect(adminClientMock.deleteBackup).not.toHaveBeenCalled();

    await user.click(within(row).getByRole("button", { name: /^confirm$/i }));
    await waitFor(() => expect(adminClientMock.deleteBackup).toHaveBeenCalledWith(dumpNew.name));
  });

  it("gates restore behind an exact-name confirm and shows the cutover banner", async () => {
    adminClientMock.listBackups.mockResolvedValue({
      running: false,
      backups: [dumpNew],
    });
    const user = userEvent.setup();
    render(<BackupsPage />);
    await screen.findByText(dumpNew.name);

    await user.click(screen.getByRole("button", { name: /^restore$/i }));
    const dialog = await screen.findByRole("dialog");
    const submit = within(dialog).getByRole("button", { name: /^restore$/i });

    // Wrong name: button stays disabled, clicking does nothing.
    await user.type(within(dialog).getByRole("textbox"), "not-the-right-name.dump");
    expect(submit).toBeDisabled();

    // Correct the text to the exact dump name.
    await user.clear(within(dialog).getByRole("textbox"));
    await user.type(within(dialog).getByRole("textbox"), dumpNew.name);
    await user.click(submit);

    await waitFor(() => expect(adminClientMock.restoreBackup).toHaveBeenCalledWith(dumpNew.name));
    // The cutover banner surfaces target + instructions from the server reply.
    expect(await screen.findByText(/db:\/\/primary/)).toBeInTheDocument();
    expect(screen.getByText(/stop the server, restore the dump, restart/)).toBeInTheDocument();
  });
});
