import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

// SchemaHistoryPage lists schema snapshots newest-first, shows a selected
// snapshot with a client-side diff against the live schema, and restores with a
// typed db-name confirm. The contract:
//   - the list renders captured versions
//   - selecting a version shows the structural diff (added/removed tables &
//     indexes) against the current schema
//   - restore calls restoreSchema with confirm = db name (typed guard)
//   - the restore button stays disabled while the typed confirm does not match
//   - a server-side restore error surfaces in the error panel

const adminClientMock = vi.hoisted(() => ({
  getSchemaHistory: vi.fn(),
  getSchemaVersion: vi.fn(),
  getSchema: vi.fn(),
  restoreSchema: vi.fn(),
}));

vi.mock("react-router-dom", async (importActual) => {
  const actual = await importActual<typeof import("react-router-dom")>();
  return {
    ...actual,
    useParams: () => ({ db: "test-db" }),
    // Link needs a Router context we don't mount in the test; stub it.
    Link: ({ children, to }: { children: React.ReactNode; to: string }) => (
      <a href={to}>{children}</a>
    ),
  };
});

vi.mock("../lib/admin", async (importActual) => {
  const actual = await importActual<typeof import("../lib/admin")>();
  return {
    ...actual,
    useAdmin: () => ({ client: adminClientMock }),
  };
});

import { RtDbRequestError } from "../lib/admin";
import { SchemaHistoryPage } from "./SchemaHistoryPage";

// Version 1 (older): only `items` with a single `by_name` index.
const V1_SCHEMA = {
  tables: {
    items: {
      fields: { name: { type: "string" } },
      indexes: [{ name: "by_name", fields: ["name"] }],
    },
  },
};

// Current schema: `items` gained a `by_count` index and a new `users` table
// exists — so diffing v1 -> current yields one added table and one added index.
const CURRENT_SCHEMA = {
  tables: {
    items: {
      fields: { name: { type: "string" } },
      indexes: [
        { name: "by_name", fields: ["name"] },
        { name: "by_count", fields: ["count"] },
      ],
    },
    users: {
      fields: { email: { type: "string" } },
    },
  },
};

const HISTORY = [
  { version: 2, capturedAt: 1722789600000, source: "push", principal: "admin@x" },
  { version: 1, capturedAt: 1722703200000, source: "push", principal: "admin@x" },
];

describe("SchemaHistoryPage", () => {
  beforeEach(() => {
    adminClientMock.getSchemaHistory.mockReset();
    adminClientMock.getSchemaVersion.mockReset();
    adminClientMock.getSchema.mockReset();
    adminClientMock.restoreSchema.mockReset();
    adminClientMock.getSchemaHistory.mockResolvedValue(HISTORY);
    adminClientMock.getSchema.mockResolvedValue(CURRENT_SCHEMA);
    adminClientMock.getSchemaVersion.mockImplementation((_db: string, version: number) =>
      Promise.resolve({
        version,
        capturedAt: version === 1 ? 1722703200000 : 1722789600000,
        source: "push",
        principal: "admin@x",
        schema: version === 1 ? V1_SCHEMA : CURRENT_SCHEMA,
      }),
    );
  });

  it("renders the list of schema versions", async () => {
    render(<SchemaHistoryPage />);
    expect(await screen.findByText("v2")).toBeInTheDocument();
    expect(screen.getByText("v1")).toBeInTheDocument();
    expect(adminClientMock.getSchemaHistory).toHaveBeenCalledWith("test-db");
  });

  it("shows an empty state when there is no history", async () => {
    adminClientMock.getSchemaHistory.mockResolvedValue([]);
    render(<SchemaHistoryPage />);
    expect(await screen.findByText(/no schema history yet/i)).toBeInTheDocument();
  });

  it("shows the structural diff against current when a version is selected", async () => {
    const user = userEvent.setup();
    render(<SchemaHistoryPage />);
    await screen.findByText("v1");

    await user.click(screen.getByRole("button", { name: /^version 1$/i }));

    // v1 -> current: `users` table added, `items#by_count` index added.
    expect(await screen.findByText(/tables added/i)).toBeInTheDocument();
    expect(screen.getByText("users")).toBeInTheDocument();
    expect(screen.getByText(/indexes added/i)).toBeInTheDocument();
    expect(screen.getByText("#by_count")).toBeInTheDocument();
    // The snapshot's own table renders too.
    expect(screen.getByText("Snapshot")).toBeInTheDocument();
  });

  it("reports no changes when the snapshot matches current", async () => {
    const user = userEvent.setup();
    render(<SchemaHistoryPage />);
    await screen.findByText("v2");

    await user.click(screen.getByRole("button", { name: /^version 2$/i }));

    expect(await screen.findByText(/no changes/i)).toBeInTheDocument();
  });

  it("restore calls restoreSchema with confirm = db name", async () => {
    const user = userEvent.setup();
    adminClientMock.restoreSchema.mockResolvedValue({ ok: true, restoredTo: 1 });
    render(<SchemaHistoryPage />);
    await screen.findByText("v1");

    await user.click(screen.getByRole("button", { name: /^version 1$/i }));
    await screen.findByText("Snapshot");

    await user.click(screen.getByRole("button", { name: /restore to this version/i }));
    const input = screen.getByRole("textbox", { name: "database name confirm" });
    await user.type(input, "test-db");
    await user.click(screen.getByRole("button", { name: /^restore$/i }));

    await waitFor(() => {
      expect(adminClientMock.restoreSchema).toHaveBeenCalledWith("test-db", 1, "test-db");
    });
  });

  it("blocks restore while the typed confirm does not match the db name", async () => {
    const user = userEvent.setup();
    adminClientMock.restoreSchema.mockResolvedValue({ ok: true, restoredTo: 1 });
    render(<SchemaHistoryPage />);
    await screen.findByText("v1");

    await user.click(screen.getByRole("button", { name: /^version 1$/i }));
    await screen.findByText("Snapshot");

    await user.click(screen.getByRole("button", { name: /restore to this version/i }));
    await user.type(screen.getByRole("textbox", { name: "database name confirm" }), "wrong-name");

    // The restore button is the client-side guard: disabled until the typed
    // name equals the db, so the call never fires with a wrong confirm.
    expect(screen.getByRole("button", { name: /^restore$/i })).toBeDisabled();
    expect(adminClientMock.restoreSchema).not.toHaveBeenCalled();
  });

  it("surfaces a restore server error envelope", async () => {
    const user = userEvent.setup();
    adminClientMock.restoreSchema.mockRejectedValue(
      new RtDbRequestError("BAD_REQUEST", 400, "confirm does not match db name"),
    );
    render(<SchemaHistoryPage />);
    await screen.findByText("v1");

    await user.click(screen.getByRole("button", { name: /^version 1$/i }));
    await screen.findByText("Snapshot");

    await user.click(screen.getByRole("button", { name: /restore to this version/i }));
    await user.type(screen.getByRole("textbox", { name: "database name confirm" }), "test-db");
    await user.click(screen.getByRole("button", { name: /^restore$/i }));

    expect(await screen.findByText(/BAD_REQUEST/)).toBeInTheDocument();
    expect(screen.getByText(/confirm does not match/)).toBeInTheDocument();
  });

  it("surfaces a list-fetch error", async () => {
    adminClientMock.getSchemaHistory.mockRejectedValue(new Error("server down"));
    render(<SchemaHistoryPage />);
    expect(await screen.findByText(/server down/i)).toBeInTheDocument();
  });
});
