import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

// Database directory: renders the db list as links, filters as you type, and
// creates a database via the client then refreshes.

const adminClientMock = vi.hoisted(() => ({
  createDb: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["alpha", "beta", "kanban"],
    databasesLoading: false,
    databasesError: null,
    refreshDatabases: vi.fn(),
  }),
}));

import { DatabasesPage } from "./DatabasesPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <DatabasesPage />
    </MemoryRouter>,
  );
}

describe("DatabasesPage", () => {
  beforeEach(() => {
    adminClientMock.createDb.mockReset().mockResolvedValue(undefined);
  });

  it("lists databases as links into each db", async () => {
    renderPage();
    expect(screen.getByRole("link", { name: "alpha" })).toHaveAttribute("href", "/dbs/alpha");
    expect(screen.getByRole("link", { name: "beta" })).toHaveAttribute("href", "/dbs/beta");
    expect(screen.getByText("3")).toBeTruthy();
  });

  it("filters the list as the operator types", async () => {
    renderPage();
    await userEvent.type(screen.getByPlaceholderText("filter by name"), "kan");
    expect(screen.getByRole("link", { name: "kanban" })).toBeTruthy();
    expect(screen.queryByText("alpha")).toBeNull();
    expect(screen.queryByText("beta")).toBeNull();
  });

  it("shows the no-match state for a filter with zero hits", async () => {
    renderPage();
    await userEvent.type(screen.getByPlaceholderText("filter by name"), "zzz");
    expect(screen.getByText("no databases match")).toBeTruthy();
  });

  it("creates a database and refreshes the directory", async () => {
    renderPage();
    await userEvent.type(screen.getByPlaceholderText("name"), "gamma");
    await userEvent.click(screen.getByRole("button", { name: "Create" }));
    await waitFor(() => {
      expect(adminClientMock.createDb).toHaveBeenCalledWith("gamma");
    });
  });
});
