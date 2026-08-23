import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SessionRow } from "../lib/types";

// SessionsPage lists interactive sessions and wires per-row revoke (with
// inline confirm) and the toolbar's bulk "remove all expired" (with inline
// confirm) to the admin client.

const adminClientMock = vi.hoisted(() => ({
  listSessions: vi.fn(),
  revokeSession: vi.fn(),
  revokeExpiredSessions: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { SessionsPage } from "./SessionsPage";

const now = Date.now();

function row(over: Partial<SessionRow>): SessionRow {
  return {
    tokenHash: "a".repeat(64),
    userId: "u1",
    email: "user@example.com",
    login: "user",
    anonymous: false,
    createdAt: now - 60_000,
    expiresAt: now + 60_000,
    ...over,
  };
}

describe("SessionsPage", () => {
  beforeEach(() => {
    adminClientMock.listSessions.mockReset();
    adminClientMock.revokeSession.mockReset();
    adminClientMock.revokeExpiredSessions.mockReset();
    adminClientMock.listSessions.mockResolvedValue([]);
  });

  it("renders rows and badges expired sessions", async () => {
    adminClientMock.listSessions.mockResolvedValue([
      row({ tokenHash: "1".repeat(64), login: "live" }),
      row({ tokenHash: "2".repeat(64), login: "stale", expiresAt: now - 1_000 }),
    ]);
    render(<SessionsPage />);

    expect(await screen.findByText("live")).toBeInTheDocument();
    const table = screen.getByRole("table");
    expect(within(table).getAllByText("expired")).toHaveLength(1);
    // the bulk button reflects the expired count
    expect(screen.getByRole("button", { name: /remove all expired/ })).toHaveTextContent(
      "remove all expired (1)",
    );
  });

  it("disables remove-all-expired when no session is expired", async () => {
    adminClientMock.listSessions.mockResolvedValue([row({ tokenHash: "1".repeat(64) })]);
    render(<SessionsPage />);

    await screen.findByRole("table");
    expect(screen.getByRole("button", { name: /remove all expired/ })).toBeDisabled();
  });

  it("confirms, bulk-removes expired sessions, and refreshes", async () => {
    adminClientMock.listSessions.mockResolvedValue([
      row({ tokenHash: "1".repeat(64), login: "stale", expiresAt: now - 1_000 }),
    ]);
    adminClientMock.revokeExpiredSessions.mockResolvedValue({ ok: true, revoked: 1 });
    const user = userEvent.setup();
    render(<SessionsPage />);

    await screen.findByRole("table");
    await user.click(screen.getByRole("button", { name: /remove all expired/ }));
    // inline confirm, then the action
    expect(screen.getByText(/remove 1 expired session/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "confirm" }));

    await waitFor(() => {
      expect(adminClientMock.revokeExpiredSessions).toHaveBeenCalledTimes(1);
    });
    // the list is re-read after the sweep
    await waitFor(() => {
      expect(adminClientMock.listSessions.mock.calls.length).toBeGreaterThanOrEqual(2);
    });
  });

  it("cancelling the bulk confirm calls nothing", async () => {
    adminClientMock.listSessions.mockResolvedValue([
      row({ tokenHash: "1".repeat(64), expiresAt: now - 1_000 }),
    ]);
    const user = userEvent.setup();
    render(<SessionsPage />);

    await screen.findByRole("table");
    await user.click(screen.getByRole("button", { name: /remove all expired/ }));
    await user.click(screen.getByRole("button", { name: "no" }));

    expect(adminClientMock.revokeExpiredSessions).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: /remove all expired/ })).toBeInTheDocument();
  });

  it("surfaces a bulk failure as the page action error", async () => {
    adminClientMock.listSessions.mockResolvedValue([
      row({ tokenHash: "1".repeat(64), expiresAt: now - 1_000 }),
    ]);
    adminClientMock.revokeExpiredSessions.mockRejectedValue(
      new Error("missing or mismatched admin CSRF token"),
    );
    const user = userEvent.setup();
    render(<SessionsPage />);

    await screen.findByRole("table");
    await user.click(screen.getByRole("button", { name: /remove all expired/ }));
    await user.click(screen.getByRole("button", { name: "confirm" }));

    expect(await screen.findByText(/missing or mismatched admin CSRF token/)).toBeInTheDocument();
  });
});
