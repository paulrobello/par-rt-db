import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { MigrateResultJson } from "@par-rt-db/client";

// MigratePage runs a declarative migration through `client.migrate`. The
// contract:
//   - dry-run renders the per-directive report (op, affectedRows, castFailures,
//     sampleChanges)
//   - apply is gated on a reviewed dry-run, and calls migrate with dryRun:false
//   - server error envelopes surface as code + status + message
//   - invalid JSON surfaces a local INVALID_JSON error and never calls migrate

const adminClientMock = vi.hoisted(() => ({
  migrate: vi.fn(),
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
import { MigratePage } from "./MigratePage";

const REPORT: MigrateResultJson = {
  applied: false,
  schema: { tables: {} },
  directives: [
    {
      op: "renameField",
      affectedRows: 3,
      sampleChanges: [{ id: "i1", before: "Widget", after: "Widget" }],
    },
    {
      op: "changeType",
      affectedRows: 2,
      castFailures: [{ id: "i9", value: "not-a-number" }],
    },
  ],
};

describe("MigratePage", () => {
  beforeEach(() => {
    adminClientMock.migrate.mockReset();
  });

  it("dry-run renders the per-directive report", async () => {
    const user = userEvent.setup();
    adminClientMock.migrate.mockResolvedValue(REPORT);
    render(<MigratePage />);

    await user.click(screen.getByRole("button", { name: "dry-run" }));

    expect(await screen.findByText("renameField")).toBeInTheDocument();
    expect(screen.getByText(/3 row/i)).toBeInTheDocument();
    expect(screen.getByText(/sample changes \(1\)/i)).toBeInTheDocument();
    // The cast-failure row surfaces both the id and the offending value.
    expect(screen.getByText("changeType")).toBeInTheDocument();
    expect(screen.getByText(/cast failures/i)).toBeInTheDocument();
    expect(screen.getByText("i9")).toBeInTheDocument();
    expect(screen.getByText(/"not-a-number"/)).toBeInTheDocument();

    // Dry-run only — never commits.
    expect(adminClientMock.migrate).toHaveBeenCalledTimes(1);
    expect(adminClientMock.migrate.mock.calls[0][1]).toEqual(
      expect.objectContaining({ dryRun: true }),
    );
  });

  it("apply is disabled until a dry-run has been reviewed", async () => {
    const user = userEvent.setup();
    adminClientMock.migrate.mockResolvedValue(REPORT);
    render(<MigratePage />);

    const applyBtn = screen.getByRole("button", { name: "apply" });
    expect(applyBtn).toBeDisabled();

    await user.click(screen.getByRole("button", { name: "dry-run" }));
    await screen.findByText("renameField");
    expect(screen.getByRole("button", { name: "apply" })).toBeEnabled();
  });

  it("apply calls migrate with dryRun:false after a reviewed dry-run", async () => {
    const user = userEvent.setup();
    adminClientMock.migrate
      .mockResolvedValueOnce(REPORT)
      .mockResolvedValueOnce({ ...REPORT, applied: true });

    render(<MigratePage />);

    await user.click(screen.getByRole("button", { name: "dry-run" }));
    await screen.findByText("renameField");

    await user.click(screen.getByRole("button", { name: "apply" }));

    await waitFor(() => {
      expect(adminClientMock.migrate).toHaveBeenCalledTimes(2);
    });
    expect(adminClientMock.migrate.mock.calls[1][1]).toEqual(
      expect.objectContaining({ dryRun: false }),
    );
    expect(await screen.findByText(/directive\(s\) committed/i)).toBeInTheDocument();
  });

  it("editing the editor after a dry-run forces a re-preview (apply disabled again)", async () => {
    const user = userEvent.setup();
    adminClientMock.migrate.mockResolvedValue(REPORT);
    render(<MigratePage />);

    await user.click(screen.getByRole("button", { name: "dry-run" }));
    await screen.findByText("renameField");
    expect(screen.getByRole("button", { name: "apply" })).toBeEnabled();

    const editor = screen.getByRole("textbox", { name: "directives JSON" });
    await user.type(editor, " ");

    expect(screen.getByRole("button", { name: "apply" })).toBeDisabled();
  });

  it("surfaces a server error envelope from a dry-run", async () => {
    const user = userEvent.setup();
    adminClientMock.migrate.mockRejectedValue(
      new RtDbRequestError("MIGRATION_FAILED", 400, "unknown directive op"),
    );
    render(<MigratePage />);

    await user.click(screen.getByRole("button", { name: "dry-run" }));

    expect(await screen.findByText(/MIGRATION_FAILED/)).toBeInTheDocument();
    expect(screen.getByText(/unknown directive op/)).toBeInTheDocument();
  });

  it("shows a local error for invalid JSON and does not call migrate", async () => {
    const user = userEvent.setup();
    render(<MigratePage />);

    const editor = screen.getByRole("textbox", { name: "directives JSON" }) as HTMLTextAreaElement;
    await user.clear(editor);
    await user.type(editor, "not json");
    await user.click(screen.getByRole("button", { name: "dry-run" }));

    expect(await screen.findByText("INVALID_JSON")).toBeInTheDocument();
    expect(adminClientMock.migrate).not.toHaveBeenCalled();
  });
});
