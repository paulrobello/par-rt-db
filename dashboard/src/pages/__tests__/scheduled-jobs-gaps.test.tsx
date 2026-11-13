import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ScheduleInfo } from "../../lib/types";

// Gap coverage for ScheduledJobsPage beyond ScheduledJobsPage.test.tsx: the
// create-form validation branches (afterMs/everyMs/cron), the JSON parse and
// server-error create paths, list errors, the no-database empty state, db
// switching, mode switching, and the error/running status rendering + disabled
// pause button.

const adminMock = vi.hoisted(() => ({
  client: {
    listSchedules: vi.fn(),
    pauseSchedule: vi.fn(),
    resumeSchedule: vi.fn(),
    cancelSchedule: vi.fn(),
    createSchedule: vi.fn(),
  },
  state: { databases: ["db1", "db2"] as string[] },
}));

vi.mock("../../lib/admin", () => ({
  useAdmin: () => ({ client: adminMock.client, databases: adminMock.state.databases }),
}));

import { ScheduledJobsPage } from "../ScheduledJobsPage";

const runningJob: ScheduleInfo = {
  id: "runn0001-aaaa",
  kind: "oneshot",
  dueAt: Date.now() + 60_000,
  status: "running",
  createdAt: Date.now() - 1_000,
  firedCount: 1,
};

const errorJob: ScheduleInfo = {
  id: "erro0002-bbbb",
  kind: "cron",
  cron: "0 0 * * *",
  dueAt: Date.now() + 60_000,
  status: "error",
  lastError: "txn rejected",
  createdAt: Date.now() - 2_000,
  firedCount: 2,
};

beforeEach(() => {
  for (const fn of Object.values(adminMock.client)) fn.mockReset();
  adminMock.client.listSchedules.mockResolvedValue([]);
  adminMock.state.databases = ["db1", "db2"];
});

describe("ScheduledJobsPage gaps", () => {
  it("shows the no-database state with refresh and create disabled", async () => {
    adminMock.state.databases = [];
    render(<ScheduledJobsPage />);
    expect(await screen.findByText("select a database.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "refresh" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "create" })).toBeDisabled();
    expect(adminMock.client.listSchedules).not.toHaveBeenCalled();
    expect(screen.getByRole("combobox")).toBeDisabled();
  });

  it("shows the list error from listSchedules", async () => {
    adminMock.client.listSchedules.mockRejectedValue(new Error("boom"));
    render(<ScheduledJobsPage />);
    expect(await screen.findByText("boom")).toBeInTheDocument();
    expect(screen.getByText("0 job(s)")).toBeInTheDocument();
  });

  it("rejects a non-numeric afterMs value", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.clear(screen.getByLabelText("afterMs (ms)"));
    await user.type(screen.getByLabelText("afterMs (ms)"), "not-a-number");
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(
      screen.getByText("afterMs must be a non-negative number of milliseconds."),
    ).toBeInTheDocument();
    expect(adminMock.client.createSchedule).not.toHaveBeenCalled();
  });

  it("rejects a negative afterMs value", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.clear(screen.getByLabelText("afterMs (ms)"));
    await user.type(screen.getByLabelText("afterMs (ms)"), "-5");
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(
      screen.getByText("afterMs must be a non-negative number of milliseconds."),
    ).toBeInTheDocument();
    expect(adminMock.client.createSchedule).not.toHaveBeenCalled();
  });

  it("rejects an everyMs value that is not positive", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.click(screen.getByRole("button", { name: "interval" }));
    await user.clear(screen.getByLabelText("everyMs (ms)"));
    await user.type(screen.getByLabelText("everyMs (ms)"), "0");
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(
      screen.getByText("everyMs must be a positive number of milliseconds."),
    ).toBeInTheDocument();
    expect(adminMock.client.createSchedule).not.toHaveBeenCalled();
  });

  it("rejects an empty cron expression", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.click(screen.getByRole("button", { name: "cron" }));
    await user.clear(screen.getByLabelText("cron (5-field, UTC)"));
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(screen.getByText("cron expression is required.")).toBeInTheDocument();
    expect(adminMock.client.createSchedule).not.toHaveBeenCalled();
  });

  it("rejects a malformed transaction DSL without calling the server", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    fireEvent.change(screen.getByLabelText("transaction DSL"), {
      target: { value: "{nope" },
    });
    await user.click(screen.getByRole("button", { name: "create" }));
    // JSON.parse of "{nope" throws a SyntaxError whose message the page shows
    // verbatim (wording is engine-specific), and createSchedule is never reached.
    expect(screen.getByText(/in JSON at position/)).toBeInTheDocument();
    expect(adminMock.client.createSchedule).not.toHaveBeenCalled();
  });

  it("creates a one-shot job, shows the success hint, and refreshes the list", async () => {
    adminMock.client.createSchedule.mockResolvedValue({ id: "abcdef1234567890" });
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.click(screen.getByRole("button", { name: "create" }));
    await waitFor(() => {
      expect(adminMock.client.createSchedule).toHaveBeenCalledWith(
        "db1",
        { type: "afterMs", ms: 60000 },
        { steps: [{ op: "patch", table: "users", id: "k1...", fields: { ping: true } }] },
      );
    });
    expect(await screen.findByText("scheduled — id abcdef12")).toBeInTheDocument();
    // refresh() re-runs listSchedules after the create lands.
    await waitFor(() => {
      expect(adminMock.client.listSchedules).toHaveBeenCalledTimes(2);
    });
  });

  it("surfaces a createSchedule server error", async () => {
    adminMock.client.createSchedule.mockRejectedValue(new Error("quota exceeded"));
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    await user.click(screen.getByRole("button", { name: "create" }));
    expect(await screen.findByText("quota exceeded")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "create" })).toBeEnabled();
  });

  it("switches schedule mode via the segment buttons", async () => {
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    await screen.findByText("no scheduled jobs.");
    expect(screen.getByLabelText("afterMs (ms)")).toHaveValue("60000");
    await user.click(screen.getByRole("button", { name: "interval" }));
    expect(screen.getByLabelText("everyMs (ms)")).toHaveValue("60000");
    await user.click(screen.getByRole("button", { name: "cron" }));
    expect(screen.getByLabelText("cron (5-field, UTC)")).toHaveValue("*/5 * * * *");
    await user.click(screen.getByRole("button", { name: "one-shot" }));
    expect(screen.getByLabelText("afterMs (ms)")).toHaveValue("60000");
  });

  it("resets job state and refetches when the database changes", async () => {
    adminMock.client.listSchedules.mockImplementation((db: string) =>
      db === "db1" ? Promise.resolve([runningJob]) : Promise.resolve([]),
    );
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    expect(await screen.findByText("runn0001")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("database"), "db2");
    // The previous db's rows clear immediately; db2 has no jobs.
    await waitFor(() => {
      expect(screen.getByText("no scheduled jobs.")).toBeInTheDocument();
    });
    await waitFor(() => {
      expect(adminMock.client.listSchedules).toHaveBeenCalledWith("db2");
    });
  });

  it("renders error and running rows with pause disabled for both", async () => {
    adminMock.client.listSchedules.mockResolvedValue([runningJob, errorJob]);
    render(<ScheduledJobsPage />);
    const runningRow = (await screen.findByText("runn0001")).closest("tr");
    const errorRow = screen.getByText("erro0002").closest("tr");
    if (!runningRow || !errorRow) throw new Error("rows not found");
    expect(within(runningRow).getByRole("button", { name: "pause" })).toBeDisabled();
    expect(within(errorRow).getByRole("button", { name: "pause" })).toBeDisabled();
    expect(within(errorRow).getByText("txn rejected")).toBeInTheDocument();
    // Cancel is still armed for a running row even though pause is disabled.
    await userEvent.setup().click(within(errorRow).getByRole("button", { name: "cancel" }));
    expect(within(errorRow).getByText("cancel?")).toBeInTheDocument();
  });

  it("surfaces a pause error in the shared action-error slot", async () => {
    adminMock.client.listSchedules.mockResolvedValue([{ ...runningJob, status: "pending" }]);
    adminMock.client.pauseSchedule.mockRejectedValue(new Error("lease lost"));
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    const row = (await screen.findByText("runn0001")).closest("tr");
    if (!row) throw new Error("row not found");
    await user.click(within(row).getByRole("button", { name: "pause" }));
    expect(await screen.findByText("lease lost")).toBeInTheDocument();
  });

  it("surfaces a cancel error in the shared action-error slot", async () => {
    adminMock.client.listSchedules.mockResolvedValue([runningJob]);
    adminMock.client.cancelSchedule.mockRejectedValue(new Error("already fired"));
    const user = userEvent.setup();
    render(<ScheduledJobsPage />);
    const row = (await screen.findByText("runn0001")).closest("tr");
    if (!row) throw new Error("row not found");
    await user.click(within(row).getByRole("button", { name: "cancel" }));
    await user.click(within(row).getByRole("button", { name: "confirm" }));
    expect(await screen.findByText("already fired")).toBeInTheDocument();
    // The confirm row stays mounted (confirmation not cleared on failure) and
    // the busy state clears.
    expect(within(row).getByRole("button", { name: "confirm" })).toBeEnabled();
  });
});
