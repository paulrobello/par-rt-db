import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Single-database overview: stats bar (size, quotas, table count), per-table
// browse links, and the delete-database flow gated behind typing the db name.

const adminClientMock = vi.hoisted(() => ({
  dbStats: vi.fn(),
  deleteDb: vi.fn(),
}));

const refreshDatabases = vi.fn();

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    refreshDatabases,
  }),
}));

const navigateMock = vi.fn();

vi.mock("react-router-dom", async () => {
  const actual = await vi.importActual<typeof import("react-router-dom")>("react-router-dom");
  return { ...actual, useNavigate: () => navigateMock };
});

import { DbPage } from "./DbPage";

const stats = {
  totalSizeBytes: 2048,
  tables: [{ name: "docs", rowCount: 12, sizeBytes: 1024 }],
  storageUsedBytes: 1024,
  storageQuotaBytes: 4096,
  storage: null,
  tablesUsed: 1,
  tablesQuota: 10,
  subsUsed: 2,
  subsQuota: 20,
};

function renderPage() {
  return render(
    <MemoryRouter initialEntries={["/dbs/prod-db"]}>
      <Routes>
        <Route path="/dbs/:db" element={<DbPage />} />
      </Routes>
    </MemoryRouter>,
  );
}

describe("DbPage", () => {
  beforeEach(() => {
    adminClientMock.dbStats.mockReset().mockResolvedValue(stats);
    adminClientMock.deleteDb.mockReset().mockResolvedValue(undefined);
    refreshDatabases.mockReset();
    navigateMock.mockReset();
  });

  it("renders stats, quota share, and table rows for the routed database", async () => {
    renderPage();
    expect(screen.getAllByText("prod-db").length).toBeGreaterThan(0);
    await waitFor(() => {
      expect(adminClientMock.dbStats).toHaveBeenCalledWith("prod-db");
    });
    // 1 KB of 4 KB quota = 25%
    expect(await screen.findByText(/25%/)).toBeTruthy();
    expect(screen.getByText("docs")).toBeTruthy();
    expect(screen.getByText("12")).toBeTruthy();
    expect(screen.getByText("browse →")).toHaveAttribute("href", "/dbs/prod-db/tables/docs");
  });

  it("shows the empty state for a database with no tables", async () => {
    adminClientMock.dbStats.mockResolvedValue({
      totalSizeBytes: 0,
      tables: [],
      storageUsedBytes: 0,
      storageQuotaBytes: 0,
      storage: null,
    });
    renderPage();
    expect(await screen.findByText("No tables — push a schema to this database.")).toBeTruthy();
  });

  it("gates deletion behind typing the database name", async () => {
    renderPage();
    await screen.findByText("docs");
    await userEvent.click(screen.getByRole("button", { name: "Delete database" }));

    const confirmInput = screen.getByPlaceholderText("prod-db");
    // wrong name -> still disabled
    await userEvent.type(confirmInput, "wrong-name");
    expect(screen.getByRole("button", { name: "delete forever" })).toHaveProperty("disabled", true);

    await userEvent.clear(confirmInput);
    await userEvent.type(confirmInput, "prod-db");
    await userEvent.click(screen.getByRole("button", { name: "delete forever" }));

    await waitFor(() => {
      expect(adminClientMock.deleteDb).toHaveBeenCalledWith("prod-db", "prod-db");
    });
    expect(refreshDatabases).toHaveBeenCalledTimes(1);
    expect(navigateMock).toHaveBeenCalledWith("/databases");
  });

  it("surfaces the delete error and stays on the page", async () => {
    adminClientMock.deleteDb.mockRejectedValue(new Error("db is pinned"));
    renderPage();
    await screen.findByText("docs");
    await userEvent.click(screen.getByRole("button", { name: "Delete database" }));
    await userEvent.type(screen.getByPlaceholderText("prod-db"), "prod-db");
    await userEvent.click(screen.getByRole("button", { name: "delete forever" }));
    expect(await screen.findByText("db is pinned")).toBeTruthy();
    expect(navigateMock).not.toHaveBeenCalled();
  });
});
