import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { TokenRow } from "../lib/types";

// TokensPage lists a database's machine tokens and wires mint/revoke to
// AdminClient. The list must render on db load; mint must call mintToken with
// the form's capabilities and surface the one-time secret; revoke must confirm
// inline before calling revokeToken.

const adminClientMock = vi.hoisted(() => ({
  listTokens: vi.fn(),
  mintToken: vi.fn(),
  revokeToken: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { TokensPage } from "./TokensPage";

const fullAccess: TokenRow = {
  id: "fulltd01-aaaa-bbbb-cccc-dddddddddddd",
  name: "CI runner",
  createdAt: 1_750_000_000_000,
  revoked: false,
  expiresAt: null,
  readOnly: false,
  tables: null,
};

const scoped: TokenRow = {
  id: "scopet02-aaaa-bbbb-cccc-dddddddddddd",
  name: "prod reader",
  createdAt: 1_750_000_000_000,
  revoked: false,
  readOnly: true,
  tables: ["users"],
  expiresAt: Date.now() + 60_000,
};

const revokedRow: TokenRow = {
  id: "revtd003-aaaa-bbbb-cccc-dddddddddddd",
  name: "old ci",
  createdAt: 1_750_000_000_000,
  revoked: true,
  expiresAt: null,
  readOnly: false,
  tables: ["audit"],
};

const expiredRow: TokenRow = {
  id: "exptd004-aaaa-bbbb-cccc-dddddddddddd",
  name: "stale token",
  createdAt: 1_750_000_000_000,
  revoked: false,
  readOnly: true,
  tables: ["users"],
  expiresAt: Date.now() - 60_000,
};

describe("TokensPage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listTokens.mockResolvedValue({ tokens: [] });
    adminClientMock.revokeToken.mockResolvedValue({ ok: true });
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("lists tokens for the selected database", async () => {
    adminClientMock.listTokens.mockResolvedValue({
      tokens: [fullAccess, scoped, revokedRow, expiredRow],
    });
    render(<TokensPage />);

    // id (8-char prefix) + name for each row.
    expect(await screen.findByText("fulltd01")).toBeInTheDocument();
    expect(screen.getByText("CI runner")).toBeInTheDocument();
    expect(screen.getByText("scopet02")).toBeInTheDocument();
    expect(screen.getByText("prod reader")).toBeInTheDocument();

    // Badges render inside the token table; the always-mounted mint form's
    // "read-only"/"read-write" segment buttons would otherwise shadow them, so
    // scope these assertions to the table.
    const table = screen.getByRole("table");
    expect(within(table).getAllByText("read-write")).toHaveLength(2);
    expect(within(table).getAllByText("read-only")).toHaveLength(2);
    expect(within(table).getByText("all")).toBeInTheDocument();
    expect(within(table).getAllByText("users")).toHaveLength(2);
    expect(within(table).getByText("audit")).toBeInTheDocument();
    expect(within(table).getAllByText("active")).toHaveLength(2);
    expect(within(table).getByText("revoked")).toBeInTheDocument();
    expect(within(table).getByText("expired")).toBeInTheDocument();
  });

  it("mints a token with the form's capabilities and reveals the secret", async () => {
    adminClientMock.mintToken.mockResolvedValue({
      tokenId: "newtid01-aaaa",
      token: "rtdb.secret.token.value",
    });
    const user = userEvent.setup();
    render(<TokensPage />);
    await screen.findByText("no machine tokens.");

    await user.type(screen.getByLabelText("name"), "ci token");
    await user.click(screen.getByRole("button", { name: "read-only" }));
    await user.type(screen.getByLabelText(/tables/), "users");
    await user.click(screen.getByRole("button", { name: "mint" }));

    await waitFor(() => {
      expect(adminClientMock.mintToken).toHaveBeenCalledWith("db1", "ci token", {
        readOnly: true,
        tables: ["users"],
      });
    });
    // the one-time plaintext token is surfaced for copy.
    expect(await screen.findByText("rtdb.secret.token.value")).toBeInTheDocument();
  });

  it("requires a confirm click before revoking a token", async () => {
    adminClientMock.listTokens.mockResolvedValue({ tokens: [fullAccess] });
    const user = userEvent.setup();
    render(<TokensPage />);
    const row = (await screen.findByText("fulltd01")).closest("tr");
    if (!row) throw new Error("row not found");

    // First click arms confirmation; revoke must not fire yet.
    await user.click(within(row).getByRole("button", { name: "revoke" }));
    expect(adminClientMock.revokeToken).not.toHaveBeenCalled();

    // Confirm click fires the revoke.
    await user.click(within(row).getByRole("button", { name: "confirm" }));
    await waitFor(() => {
      expect(adminClientMock.revokeToken).toHaveBeenCalledWith(fullAccess.id);
    });
  });
});
