import type { RtDbAdminClient } from "@par-rt-db/client";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Webhook, WebhookDelivery } from "../../lib/types";
import {
  timeLabel,
  WebhookCreateForm,
  WebhookDeliveriesPanel,
  WebhookEditPanel,
} from "../webhooks";

// Direct tests for the webhook sub-panels (components/webhooks.tsx), covering
// branches the page-level WebhooksPage.test.tsx does not reach: create-form
// validation and body-minimization branches, the edit panel's tri-state table
// handling and error paths, deliveries error/empty/badge/payload-preview
// branches, and the exported timeLabel helper.

const clientMock = {
  createWebhook: vi.fn(),
  editWebhook: vi.fn(),
  listDeliveries: vi.fn(),
};

const client = clientMock as unknown as RtDbAdminClient;

const hook: Webhook = {
  id: 7,
  db: "db1",
  table: "users",
  url: "https://example.com/hook",
  events: ["insert", "patch"],
  createdAt: 1_750_000_000_000,
  enabled: true,
  secret: "s3cret",
} as unknown as Webhook;

const allTablesHook: Webhook = {
  ...hook,
  id: 8,
  table: null,
  events: ["*"],
} as unknown as Webhook;

const delivery: WebhookDelivery = {
  id: 20,
  attempts: 2,
  status: "delivered",
  nextAttempt: 1_750_000_005_000,
  lastError: null,
  payload: { op: "insert", table: "users", id: "u1" },
};

beforeEach(() => {
  clientMock.createWebhook.mockReset().mockResolvedValue({ id: 42 });
  clientMock.editWebhook.mockReset().mockResolvedValue(hook);
  clientMock.listDeliveries.mockReset().mockResolvedValue([]);
});

describe("WebhookCreateForm", () => {
  it("requires a url and shows the validation error without calling the client", async () => {
    const user = userEvent.setup();
    const onCreated = vi.fn();
    render(<WebhookCreateForm client={client} db="db1" onCreated={onCreated} />);
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(screen.getByText("url is required.")).toBeInTheDocument();
    expect(clientMock.createWebhook).not.toHaveBeenCalled();
  });

  it("sends a minimal body for a default webhook and resets the form", async () => {
    const user = userEvent.setup();
    const onCreated = vi.fn();
    render(<WebhookCreateForm client={client} db="db1" onCreated={onCreated} />);
    await user.type(screen.getByLabelText("url"), "https://example.com/new");
    await user.click(screen.getByRole("button", { name: "create" }));
    await waitFor(() => {
      // An untouched events input parses to an empty list, which IS sent
      // (only a literal ["*"] collapses) — the body the code actually builds.
      expect(clientMock.createWebhook).toHaveBeenCalledWith("db1", {
        url: "https://example.com/new",
        events: [],
      });
    });
    expect(await screen.findByText("created — id 42")).toBeInTheDocument();
    expect(onCreated).toHaveBeenCalled();
    expect((screen.getByLabelText("url") as HTMLInputElement).value).toBe("");
  });

  it("trims url/table and parses, trims, and de-duplicates the events list", async () => {
    const user = userEvent.setup();
    render(<WebhookCreateForm client={client} db="db1" onCreated={async () => {}} />);
    await user.type(screen.getByLabelText("url"), "  https://x.example  ");
    await user.type(screen.getByLabelText("create table"), " users ");
    await user.type(screen.getByLabelText("create events"), " insert , insert , patch");
    await user.click(screen.getByRole("button", { name: "create" }));
    await waitFor(() => {
      expect(clientMock.createWebhook).toHaveBeenCalledWith("db1", {
        url: "https://x.example",
        table: "users",
        events: ["insert", "patch"],
      });
    });
  });

  it("omits events when the list collapses to * and sends enabled:false when disabled", async () => {
    const user = userEvent.setup();
    render(<WebhookCreateForm client={client} db="db1" onCreated={async () => {}} />);
    await user.type(screen.getByLabelText("url"), "https://x.example");
    await user.type(screen.getByLabelText("create events"), " * , * ");
    await user.click(screen.getByRole("button", { name: "disabled" }));
    await user.click(screen.getByRole("button", { name: "create" }));
    await waitFor(() => {
      expect(clientMock.createWebhook).toHaveBeenCalledWith("db1", {
        url: "https://x.example",
        enabled: false,
      });
    });
  });

  it("shows the client error and keeps the form values on a failed create", async () => {
    const user = userEvent.setup();
    clientMock.createWebhook.mockRejectedValue(new Error("url already registered"));
    render(<WebhookCreateForm client={client} db="db1" onCreated={vi.fn()} />);
    await user.type(screen.getByLabelText("url"), "https://x.example");
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(await screen.findByText("url already registered")).toBeInTheDocument();
    expect((screen.getByLabelText("url") as HTMLInputElement).value).toBe("https://x.example");
  });
});

describe("WebhookEditPanel", () => {
  it("initializes its fields from the webhook being edited", () => {
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={hook}
        onClose={vi.fn()}
        onSaved={vi.fn()}
      />,
    );
    expect(screen.getByLabelText("url")).toBeInTheDocument();
    expect((screen.getByLabelText("url") as HTMLInputElement).value).toBe(
      "https://example.com/hook",
    );
    expect((screen.getByLabelText("table") as HTMLInputElement).value).toBe("users");
    expect((screen.getByLabelText("events") as HTMLInputElement).value).toBe("insert, patch");
    expect(screen.getByLabelText("clear table")).not.toBeChecked();
  });

  it("rejects an empty url without calling the client", async () => {
    const user = userEvent.setup();
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={hook}
        onClose={vi.fn()}
        onSaved={vi.fn()}
      />,
    );
    await user.clear(screen.getByLabelText("url"));
    await user.click(screen.getByRole("button", { name: "save" }));
    expect(screen.getByText("url must not be empty.")).toBeInTheDocument();
    expect(clientMock.editWebhook).not.toHaveBeenCalled();
  });

  it("sends table:null when the clear checkbox is ticked, omitting a blank table otherwise", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={hook}
        onClose={vi.fn()}
        onSaved={onSaved}
      />,
    );
    await user.click(screen.getByLabelText("clear table"));
    await user.click(screen.getByRole("button", { name: "save" }));
    await waitFor(() => {
      expect(clientMock.editWebhook).toHaveBeenCalledWith("db1", 7, {
        url: "https://example.com/hook",
        table: null,
        events: ["insert", "patch"],
        enabled: true,
      });
    });
    expect(onSaved).toHaveBeenCalled();

    // Re-mount with the checkbox ticked, then type into the table input: the
    // panel must untick "clear" and send the typed table instead of null.
    clientMock.editWebhook.mockClear();
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={allTablesHook}
        onClose={vi.fn()}
        onSaved={vi.fn()}
      />,
    );
    const tableInput = screen.getAllByLabelText("table")[1];
    expect((tableInput as HTMLInputElement).value).toBe("");
    expect(screen.getAllByLabelText("clear table")[1]).toBeChecked();
    // The table input is disabled while "clear" is ticked — untick it first,
    // then typing a table sends that table instead of null.
    await user.click(screen.getAllByLabelText("clear table")[1]);
    await user.type(tableInput, "audit");
    expect(screen.getAllByLabelText("clear table")[1]).not.toBeChecked();
    await user.click(screen.getAllByRole("button", { name: "save" })[1]);
    await waitFor(() => {
      // The all-tables webhook's events are ["*"], so events is omitted.
      expect(clientMock.editWebhook).toHaveBeenLastCalledWith("db1", 8, {
        url: "https://example.com/hook",
        table: "audit",
        enabled: true,
      });
    });
  });

  it("collapses a *-only events list and always sends enabled", async () => {
    const user = userEvent.setup();
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={allTablesHook}
        onClose={vi.fn()}
        onSaved={vi.fn()}
      />,
    );
    await user.clear(screen.getByLabelText("events"));
    await user.type(screen.getByLabelText("events"), " * ");
    await user.click(screen.getByRole("button", { name: "disabled" }));
    await user.click(screen.getByRole("button", { name: "save" }));
    await waitFor(() => {
      // table:null rides along because an all-tables webhook starts with the
      // clear checkbox ticked; events collapse to omitted; enabled is always sent.
      expect(clientMock.editWebhook).toHaveBeenCalledWith("db1", 8, {
        url: "https://example.com/hook",
        table: null,
        enabled: false,
      });
    });
  });

  it("shows the client error and keeps the panel open on a failed save", async () => {
    const user = userEvent.setup();
    clientMock.editWebhook.mockRejectedValue(new Error("webhook not found"));
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={hook}
        onClose={vi.fn()}
        onSaved={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "save" }));
    expect(await screen.findByText("webhook not found")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "save" })).toBeInTheDocument();
  });

  it("calls onClose via the cancel button", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(
      <WebhookEditPanel
        client={client}
        db="db1"
        webhook={hook}
        onClose={onClose}
        onSaved={vi.fn()}
      />,
    );
    await user.click(screen.getByRole("button", { name: "cancel" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});

describe("WebhookDeliveriesPanel", () => {
  it("shows the empty state when there are no deliveries", async () => {
    render(<WebhookDeliveriesPanel client={client} db="db1" webhookId={7} onClose={vi.fn()} />);
    expect(await screen.findByText("no deliveries.")).toBeInTheDocument();
    expect(clientMock.listDeliveries).toHaveBeenCalledWith("db1", 7, {});
  });

  it("renders rows with badge classes and a truncated payload preview", async () => {
    const longPayload = { op: "patch", fields: { note: "x".repeat(80) } };
    clientMock.listDeliveries.mockResolvedValue([
      delivery,
      {
        ...delivery,
        id: 21,
        status: "some-unknown-status",
        lastError: null,
        payload: longPayload,
      },
    ]);
    render(<WebhookDeliveriesPanel client={client} db="db1" webhookId={7} onClose={vi.fn()} />);
    const deliveredBadge = await screen.findByText("delivered", { selector: "span" });
    expect(deliveredBadge.className).toContain("badgeDelivered");
    const unknownBadge = screen.getByText("some-unknown-status");
    expect(unknownBadge.className).not.toContain("badgeDelivered");
    expect(unknownBadge.className).not.toContain("badgeFailed");

    // The long payload renders truncated with the full JSON on the title.
    const truncated = screen.getByText(/…$/);
    expect(truncated.textContent?.length).toBeLessThanOrEqual(61);
    const expected = JSON.stringify(longPayload);
    expect(truncated.getAttribute("title")).toBe(expected);

    // lastError:null renders the em-dash placeholder.
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(2);
  });

  it("shows the fetch error from listDeliveries", async () => {
    clientMock.listDeliveries.mockRejectedValue(new Error("deliveries offline"));
    render(<WebhookDeliveriesPanel client={client} db="db1" webhookId={7} onClose={vi.fn()} />);
    expect(await screen.findByText("deliveries offline")).toBeInTheDocument();
  });

  it("refreshes deliveries on the refresh button and closes on close", async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(<WebhookDeliveriesPanel client={client} db="db1" webhookId={7} onClose={onClose} />);
    await screen.findByText("no deliveries.");
    await user.click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() => {
      expect(clientMock.listDeliveries).toHaveBeenCalledTimes(2);
    });
    await user.click(screen.getByRole("button", { name: "close" }));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("refetches with a status filter when the select changes", async () => {
    const user = userEvent.setup();
    render(<WebhookDeliveriesPanel client={client} db="db1" webhookId={7} onClose={vi.fn()} />);
    await screen.findByText("no deliveries.");
    await user.selectOptions(screen.getByLabelText("status filter"), "failed");
    await waitFor(() => {
      expect(clientMock.listDeliveries).toHaveBeenCalledWith("db1", 7, { status: "failed" });
    });
  });
});

describe("timeLabel", () => {
  it("formats a timestamp as a locale string", () => {
    const out = timeLabel(1_750_000_000_000);
    expect(typeof out).toBe("string");
    expect(out.length).toBeGreaterThan(0);
    expect(out).toMatch(/2025/);
  });

  it("renders a delivery row's timestamp through the table cell", async () => {
    clientMock.listDeliveries.mockResolvedValue([delivery]);
    render(<WebhookDeliveriesPanel client={client} db="tdb1" webhookId={7} onClose={vi.fn()} />);
    const cell = await screen.findByText(/2025/);
    expect(cell.closest("td")).not.toBeNull();
  });
});
