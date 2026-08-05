import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { AuditEntry } from "../lib/types";

// AuditPage lists the durable audit log for a database and wires filter +
// pagination controls to AdminClient.getAudit. The list must render on db load
// (including rows where op/principal are null); the op select must refetch with
// the new filter; the next/prev buttons must advance offset by the page size.

const adminClientMock = vi.hoisted(() => ({
  getAudit: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { AuditPage } from "./AuditPage";

const insertRow: AuditEntry = {
  id: 1,
  tsMs: 1_750_000_000_000,
  db: "db1",
  table: "users",
  op: "insert",
  docId: "user_01H",
  principal: "alice@example.com",
  source: "mutate",
};

const deleteRow: AuditEntry = {
  id: 2,
  tsMs: 1_750_000_001_000,
  db: "db1",
  table: "sessions",
  op: "delete",
  docId: "sess_02J",
  principal: "alice@example.com",
  source: "mutate",
};

// System-initiated write: op is null (the server could not label it) and
// principal is null (no interactive user — e.g. TTL reaper, scheduled job).
const systemRow: AuditEntry = {
  id: 3,
  tsMs: 1_750_000_002_000,
  db: "db1",
  table: "sessions",
  op: null,
  docId: "sess_03K",
  principal: null,
  source: "ttl",
};

describe("AuditPage", () => {
  beforeEach(() => {
    adminClientMock.getAudit.mockReset();
    adminClientMock.getAudit.mockResolvedValue([]);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("lists audit entries for the selected database", async () => {
    adminClientMock.getAudit.mockResolvedValue([insertRow, deleteRow]);
    render(<AuditPage />);

    // Initial fetch is scoped to db1 with the default paging (limit 100, offset 0)
    // and no filters — empty strings are dropped so the body matches the server's
    // "absent = match all" contract.
    await waitFor(() => {
      expect(adminClientMock.getAudit).toHaveBeenCalledWith({
        db: "db1",
        limit: 100,
        offset: 0,
      });
    });

    expect(await screen.findByText("user_01H")).toBeInTheDocument();
    expect(screen.getByText("sess_02J")).toBeInTheDocument();

    // op badge + null-principal placeholder both render.
    const table = screen.getByRole("table");
    expect(within(table).getByText("insert")).toBeInTheDocument();
    expect(within(table).getByText("delete")).toBeInTheDocument();
  });

  it("renders rows with null op and principal (system-initiated writes)", async () => {
    adminClientMock.getAudit.mockResolvedValue([systemRow]);
    render(<AuditPage />);
    expect(await screen.findByText("sess_03K")).toBeInTheDocument();

    const table = screen.getByRole("table");
    // No op badge — the cell falls back to "—".
    expect(within(table).getAllByText("—").length).toBeGreaterThan(0);
    expect(within(table).getByText("ttl")).toBeInTheDocument();
  });

  it("renders an empty state when there are no entries", async () => {
    render(<AuditPage />);
    expect(await screen.findByText("no audit entries.")).toBeInTheDocument();
  });

  it("refetches with the op filter when the op select changes", async () => {
    adminClientMock.getAudit.mockResolvedValue([]);
    const user = userEvent.setup();
    render(<AuditPage />);
    await screen.findByText("no audit entries.");

    // Initial fetch carries no op filter.
    expect(adminClientMock.getAudit).toHaveBeenLastCalledWith({
      db: "db1",
      limit: 100,
      offset: 0,
    });

    await user.selectOptions(screen.getByLabelText("op filter"), "insert");
    await waitFor(() => {
      expect(adminClientMock.getAudit).toHaveBeenLastCalledWith({
        db: "db1",
        op: "insert",
        limit: 100,
        offset: 0,
      });
    });
  });

  it("advances offset via the next button and resets offset on filter change", async () => {
    // A full page enables the next button.
    const fullPage: AuditEntry[] = Array.from({ length: 100 }, (_, i) => ({
      ...insertRow,
      id: 1000 + i,
      docId: `doc_${i}`,
    }));
    adminClientMock.getAudit.mockResolvedValue(fullPage);
    const user = userEvent.setup();
    render(<AuditPage />);
    await screen.findByText("doc_0");

    await user.click(screen.getByRole("button", { name: "next" }));
    await waitFor(() => {
      expect(adminClientMock.getAudit).toHaveBeenLastCalledWith({
        db: "db1",
        limit: 100,
        offset: 100,
      });
    });

    // Changing a filter resets offset back to 0.
    await user.selectOptions(screen.getByLabelText("source filter"), "ttl");
    await waitFor(() => {
      expect(adminClientMock.getAudit).toHaveBeenLastCalledWith({
        db: "db1",
        source: "ttl",
        limit: 100,
        offset: 0,
      });
    });
  });
});
