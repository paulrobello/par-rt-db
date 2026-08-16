import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WorkflowInfo, WorkflowInfoFull } from "../lib/types";

// WorkflowsPage lists a database's workflow runs, filters by lifecycle (with a
// client-side `stuck` preset), expands a run into its per-step outcome trail,
// and wires cancel/start to AdminClient. Cancel must require a confirm click.

const adminClientMock = vi.hoisted(() => ({
  adminListWorkflows: vi.fn(),
  adminGetWorkflow: vi.fn(),
  adminStartWorkflow: vi.fn(),
  adminCancelWorkflow: vi.fn(),
  adminDeleteWorkflow: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { WorkflowsPage } from "./WorkflowsPage";

const now = Date.now();

function run(over: Partial<WorkflowInfo> & Pick<WorkflowInfo, "id" | "name">): WorkflowInfo {
  return {
    status: "running",
    currentStep: 0,
    stepCount: 3,
    attempts: 0,
    createdAt: now - 5_000,
    updatedAt: now - 1_000,
    ...over,
  };
}

const runningRun = run({ id: "run00001-aaaa", name: "ingest-daily" });
const failedRun = run({
  id: "run00002-bbbb",
  name: "backfill",
  status: "failed",
  currentStep: 2,
  attempts: 3,
  lastError: "write conflict",
  finishedAt: now - 500,
});
const retryingRun = run({
  id: "run00003-cccc",
  name: "notify",
  status: "pending",
  currentStep: 1,
  attempts: 2,
  sleepUntil: now + 30_000,
});

const fullRun: WorkflowInfoFull = {
  ...runningRun,
  currentStep: 2,
  stepOutcomes: [
    { stepIndex: 0, status: "success", attempts: 1, at: now - 4_000 },
    { stepIndex: 1, status: "failed", attempts: 3, at: now - 2_000, error: "boom" },
  ],
};

describe("WorkflowsPage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.adminListWorkflows.mockResolvedValue([]);
  });

  it("lists runs for the selected database", async () => {
    adminClientMock.adminListWorkflows.mockResolvedValue([runningRun, failedRun]);
    render(<WorkflowsPage />);

    expect(await screen.findByText("ingest-daily")).toBeInTheDocument();
    expect(screen.getByText("backfill")).toBeInTheDocument();
    expect(screen.getByText("1/3")).toBeInTheDocument();
    expect(screen.getByText("3/3")).toBeInTheDocument();
    expect(screen.getByText("write conflict")).toBeInTheDocument();
  });

  it("passes a single-status filter server-side", async () => {
    adminClientMock.adminListWorkflows.mockResolvedValue([]);
    const user = userEvent.setup();
    render(<WorkflowsPage />);

    await user.selectOptions(await screen.findByLabelText("status"), "failed");

    await waitFor(() => {
      expect(adminClientMock.adminListWorkflows).toHaveBeenCalledWith("db1", {
        status: "failed",
      });
    });
  });

  it("filters the stuck preset client-side to failed or retrying runs", async () => {
    adminClientMock.adminListWorkflows.mockResolvedValue([
      failedRun,
      retryingRun,
      run({
        id: "run00004-dddd",
        name: "fine",
        status: "success",
        currentStep: 3,
        finishedAt: now,
      }),
    ]);
    const user = userEvent.setup();
    render(<WorkflowsPage />);

    await user.selectOptions(await screen.findByLabelText("status"), "stuck");
    await screen.findByText("backfill");

    // Fetched unfiltered (stuck spans two server statuses), then narrowed here.
    expect(adminClientMock.adminListWorkflows).toHaveBeenLastCalledWith("db1", {});
    expect(screen.getByText("notify")).toBeInTheDocument();
    expect(screen.queryByText("fine")).not.toBeInTheDocument();
  });

  it("expands a run into its per-step timeline", async () => {
    adminClientMock.adminListWorkflows.mockResolvedValue([runningRun]);
    adminClientMock.adminGetWorkflow.mockResolvedValue(fullRun);
    const user = userEvent.setup();
    render(<WorkflowsPage />);
    const row = (await screen.findByText("ingest-daily")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "steps" }));

    expect(adminClientMock.adminGetWorkflow).toHaveBeenCalledWith("db1", "run00001-aaaa");
    expect(await screen.findByText("step 1")).toBeInTheDocument();
    expect(screen.getByText("step 2")).toBeInTheDocument();
    expect(screen.getByText("boom")).toBeInTheDocument();
    // The in-flight current step is marked, not just the finished trail.
    expect(screen.getByText("in flight")).toBeInTheDocument();
  });

  it("requires a confirm click before cancelling a run", async () => {
    adminClientMock.adminListWorkflows.mockResolvedValue([runningRun]);
    adminClientMock.adminCancelWorkflow.mockResolvedValue({ ok: true });
    const user = userEvent.setup();
    render(<WorkflowsPage />);
    const row = (await screen.findByText("ingest-daily")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "cancel" }));
    expect(adminClientMock.adminCancelWorkflow).not.toHaveBeenCalled();

    await user.click(within(row).getByRole("button", { name: "confirm" }));
    await waitFor(() => {
      expect(adminClientMock.adminCancelWorkflow).toHaveBeenCalledWith("db1", "run00001-aaaa");
    });
  });

  it("starts a run from the JSON editor", async () => {
    adminClientMock.adminStartWorkflow.mockResolvedValue({ id: "new00001-eeee" });
    const user = userEvent.setup();
    render(<WorkflowsPage />);
    await screen.findByText("no workflow runs.");

    // fireEvent.change (not user.type) — user-event's keyboard parser reads
    // `{`/`}` as key descriptors, so raw JSON can't go through user.type.
    fireEvent.change(screen.getByLabelText("workflow spec"), {
      target: {
        value:
          '{"name":"e2e","steps":[{"txn":{"steps":[{"op":"insert","table":"tasks","doc":{"done":false}}]}}]}',
      },
    });
    await user.click(screen.getByRole("button", { name: "start" }));

    await waitFor(() => {
      expect(adminClientMock.adminStartWorkflow).toHaveBeenCalledWith("db1", {
        name: "e2e",
        steps: [{ txn: { steps: [{ op: "insert", table: "tasks", doc: { done: false } }] } }],
      });
    });
    expect(await screen.findByText(/started — id new00001/)).toBeInTheDocument();
  });

  it("renders an error row when the poll fails instead of crashing", async () => {
    adminClientMock.adminListWorkflows.mockRejectedValue(new Error("poll blew up"));
    render(<WorkflowsPage />);

    expect(await screen.findByText("poll blew up")).toBeInTheDocument();
    // The start editor still renders — the page did not crash.
    expect(screen.getByRole("button", { name: "start" })).toBeInTheDocument();
  });
});
