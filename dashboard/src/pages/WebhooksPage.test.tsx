import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Webhook, WebhookDelivery } from "../lib/types";

// WebhooksPage lists a database's webhooks and wires create/edit/delete +
// delivery drill-down to AdminClient. The list must render on db load (covering
// enabled/disabled rows and table all-vs-scoped); create must send only the
// provided keys; edit must PUT a partial body (including `table: null` to clear);
// delete must confirm inline first; the deliveries action must open the
// drill-down panel and call listDeliveries.

const adminClientMock = vi.hoisted(() => ({
  listWebhooks: vi.fn(),
  createWebhook: vi.fn(),
  editWebhook: vi.fn(),
  deleteWebhook: vi.fn(),
  listDeliveries: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { WebhooksPage } from "./WebhooksPage";

const enabledAll: Webhook = {
  id: 1,
  db: "db1",
  table: null,
  url: "https://example.com/hook",
  events: ["*"],
  createdAt: 1_750_000_000_000,
  enabled: true,
};

const scopedDisabled: Webhook = {
  id: 2,
  db: "db1",
  table: "users",
  url: "https://example.com/users",
  events: ["insert", "patch"],
  createdAt: 1_750_000_000_000,
  enabled: false,
};

const deliveryDelivered: WebhookDelivery = {
  id: 10,
  attempts: 1,
  status: "delivered",
  nextAttempt: 1_750_000_001_000,
  lastError: null,
  payload: { op: "insert", table: "users", id: "u1" },
};

const deliveryFailed: WebhookDelivery = {
  id: 11,
  attempts: 3,
  status: "failed",
  nextAttempt: 1_750_000_002_000,
  lastError: "connection refused",
  payload: { op: "patch", table: "users", id: "u2" },
};

describe("WebhooksPage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listWebhooks.mockResolvedValue([]);
    adminClientMock.deleteWebhook.mockResolvedValue({ ok: true });
    adminClientMock.createWebhook.mockResolvedValue({ id: 42 });
    adminClientMock.editWebhook.mockResolvedValue(enabledAll);
    adminClientMock.listDeliveries.mockResolvedValue([]);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("lists webhooks for the selected database", async () => {
    adminClientMock.listWebhooks.mockResolvedValue([enabledAll, scopedDisabled]);
    render(<WebhooksPage />);

    expect(await screen.findByText("https://example.com/hook")).toBeInTheDocument();
    expect(screen.getByText("https://example.com/users")).toBeInTheDocument();

    // Table column: all vs scoped table.
    const table = screen.getByRole("table");
    expect(within(table).getByText("all")).toBeInTheDocument();
    expect(within(table).getByText("users")).toBeInTheDocument();

    // Status badges (scoped to the table — the create form carries its own
    // enabled/disabled segment buttons that would otherwise shadow these).
    expect(within(table).getByText("enabled")).toBeInTheDocument();
    expect(within(table).getByText("disabled")).toBeInTheDocument();
    expect(within(table).getByText("insert, patch")).toBeInTheDocument();
  });

  it("renders an empty state when there are no webhooks", async () => {
    render(<WebhooksPage />);
    expect(await screen.findByText("no webhooks.")).toBeInTheDocument();
  });

  it("creates a webhook with only the provided keys", async () => {
    const user = userEvent.setup();
    render(<WebhooksPage />);
    await screen.findByText("no webhooks.");

    // Note: there are two inputs labelled "url"/"events" (create form) and one
    // for "table" — the edit form is hidden until a row is being edited, so the
    // create-form inputs are the only ones mounted here.
    await user.type(screen.getByLabelText("url"), "https://example.com/new");
    await user.type(screen.getByLabelText("create table"), "users");
    await user.type(screen.getByLabelText("create events"), "insert, patch");
    await user.click(screen.getByRole("button", { name: "disabled" })); // flip enabled off
    await user.click(screen.getByRole("button", { name: "create" }));

    await waitFor(() => {
      expect(adminClientMock.createWebhook).toHaveBeenCalledWith("db1", {
        url: "https://example.com/new",
        table: "users",
        events: ["insert", "patch"],
        enabled: false,
      });
    });
  });

  it("edits a webhook and clears the table filter to all-tables", async () => {
    adminClientMock.listWebhooks.mockResolvedValue([scopedDisabled]);
    const user = userEvent.setup();
    render(<WebhooksPage />);
    const row = (await screen.findByText("https://example.com/users")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "edit" }));

    // Tick the "clear table to all-tables" checkbox in the edit panel.
    await user.click(screen.getByLabelText("clear table"));
    await user.click(screen.getByRole("button", { name: "save" }));

    await waitFor(() => {
      // table: null must be preserved on the wire (not dropped, not "").
      expect(adminClientMock.editWebhook).toHaveBeenCalledWith("db1", 2, {
        url: "https://example.com/users",
        table: null,
        events: ["insert", "patch"],
        enabled: false,
      });
    });
  });

  it("requires a confirm click before deleting a webhook", async () => {
    adminClientMock.listWebhooks.mockResolvedValue([enabledAll]);
    const user = userEvent.setup();
    render(<WebhooksPage />);
    const row = (await screen.findByText("https://example.com/hook")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "delete" }));
    expect(adminClientMock.deleteWebhook).not.toHaveBeenCalled();

    await user.click(within(row).getByRole("button", { name: "confirm" }));
    await waitFor(() => {
      expect(adminClientMock.deleteWebhook).toHaveBeenCalledWith("db1", 1);
    });
  });

  it("opens the deliveries drill-down and lists delivery rows", async () => {
    adminClientMock.listWebhooks.mockResolvedValue([enabledAll]);
    adminClientMock.listDeliveries.mockResolvedValue([deliveryDelivered, deliveryFailed]);
    const user = userEvent.setup();
    render(<WebhooksPage />);
    const row = (await screen.findByText("https://example.com/hook")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "deliveries" }));

    // Header on the deliveries panel scopes it from the main table.
    expect(await screen.findByText("Deliveries — webhook #1")).toBeInTheDocument();
    expect(adminClientMock.listDeliveries).toHaveBeenCalledWith("db1", 1, {});

    // Delivery rows render inside the deliveries sub-table — scope by role to
    // avoid picking up badges from the main table.
    const subTables = screen.getAllByRole("table");
    const deliveriesTable = subTables.find((t) => within(t).queryByText("connection refused"));
    if (!deliveriesTable) throw new Error("deliveries table not found");
    expect(within(deliveriesTable).getByText("delivered")).toBeInTheDocument();
    expect(within(deliveriesTable).getByText("failed")).toBeInTheDocument();
    expect(within(deliveriesTable).getByText("11")).toBeInTheDocument(); // delivery id
  });

  it("filters deliveries by status via the status select", async () => {
    adminClientMock.listWebhooks.mockResolvedValue([enabledAll]);
    adminClientMock.listDeliveries.mockResolvedValue([]);
    const user = userEvent.setup();
    render(<WebhooksPage />);
    const row = (await screen.findByText("https://example.com/hook")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "deliveries" }));
    await screen.findByText("no deliveries.");

    // Initial fetch happens with no status filter.
    expect(adminClientMock.listDeliveries).toHaveBeenCalledWith("db1", 1, {});

    await user.selectOptions(screen.getByLabelText("status filter"), "failed");
    await waitFor(() => {
      expect(adminClientMock.listDeliveries).toHaveBeenCalledWith("db1", 1, {
        status: "failed",
      });
    });
  });
});
