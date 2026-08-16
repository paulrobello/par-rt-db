import type { StepJson, TransactionJson } from "@par-rt-db/client";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ScheduleInfo } from "../lib/types";

// ScheduledJobsPage lists a database's scheduled jobs and wires pause/resume/
// cancel actions to AdminClient. The list must render on db load, and the
// pause/resume toggle must call the method matching the row's current status.

const adminClientMock = vi.hoisted(() => ({
  listSchedules: vi.fn(),
  pauseSchedule: vi.fn(),
  resumeSchedule: vi.fn(),
  cancelSchedule: vi.fn(),
  createSchedule: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1"],
  }),
}));

import { DEFAULT_TXN, ScheduledJobsPage } from "./ScheduledJobsPage";

const cronJob: ScheduleInfo = {
  id: "cron0001-aaaa",
  kind: "cron",
  dueAt: Date.now() + 60_000,
  cron: "*/5 * * * *",
  status: "pending",
  createdAt: Date.now() - 1_000,
  firedCount: 3,
};

const pausedJob: ScheduleInfo = {
  id: "paused002-bbbb",
  kind: "oneshot",
  dueAt: Date.now() + 120_000,
  status: "paused",
  createdAt: Date.now() - 2_000,
  firedCount: 0,
};

describe("ScheduledJobsPage", () => {
  beforeEach(() => {
    for (const fn of Object.values(adminClientMock)) fn.mockReset();
    adminClientMock.listSchedules.mockResolvedValue([]);
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("ships a default template that satisfies the wire Step union", () => {
    // The untouched template must deserialize server-side: the assignment
    // below fails typecheck if a step shape drifts from the wire contract
    // (e.g. a patch step carrying `doc` instead of `fields`).
    const txn: TransactionJson = JSON.parse(DEFAULT_TXN);
    const step: StepJson = txn.steps[0];
    expect(step).toEqual({ op: "patch", table: "users", id: "k1...", fields: { ping: true } });
  });

  it("lists schedules for the selected database", async () => {
    adminClientMock.listSchedules.mockResolvedValue([cronJob, pausedJob]);
    render(<ScheduledJobsPage />);

    expect(await screen.findByText("cron0001")).toBeInTheDocument();
    expect(screen.getByText("paused00")).toBeInTheDocument();
    expect(screen.getByText("*/5 * * * *")).toBeInTheDocument();
    expect(screen.getByText("3", { selector: ".tnum" })).toBeInTheDocument();
  });

  it("calls pauseSchedule for a pending job when pause is clicked", async () => {
    adminClientMock.listSchedules
      .mockResolvedValueOnce([cronJob])
      .mockResolvedValueOnce([{ ...cronJob, status: "paused" }]);
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    const row = (await screen.findByText("cron0001")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "pause" }));

    await waitFor(() => {
      expect(adminClientMock.pauseSchedule).toHaveBeenCalledWith("db1", "cron0001-aaaa");
    });
  });

  it("calls resumeSchedule for a paused job when resume is clicked", async () => {
    adminClientMock.listSchedules
      .mockResolvedValueOnce([pausedJob])
      .mockResolvedValueOnce([{ ...pausedJob, status: "pending" }]);
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    const row = (await screen.findByText("paused00")).closest("tr");
    if (!row) throw new Error("row not found");

    await user.click(within(row).getByRole("button", { name: "resume" }));

    await waitFor(() => {
      expect(adminClientMock.resumeSchedule).toHaveBeenCalledWith("db1", "paused002-bbbb");
    });
  });

  it("requires a confirm click before cancelling a job", async () => {
    adminClientMock.listSchedules.mockResolvedValue([cronJob]);
    adminClientMock.cancelSchedule.mockResolvedValue({ ok: true });
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    const row = (await screen.findByText("cron0001")).closest("tr");
    if (!row) throw new Error("row not found");

    // First click arms confirmation; cancel must not fire yet.
    await user.click(within(row).getByRole("button", { name: "cancel" }));
    expect(adminClientMock.cancelSchedule).not.toHaveBeenCalled();

    // Confirm click fires the cancel.
    await user.click(within(row).getByRole("button", { name: "confirm" }));
    await waitFor(() => {
      expect(adminClientMock.cancelSchedule).toHaveBeenCalledWith("db1", "cron0001-aaaa");
    });
  });
});
