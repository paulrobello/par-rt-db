import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SlowQueriesResponse } from "@par-rt-db/client";

// Slow-query log + inline explain panel: list renders rows with a duration
// warning threshold at 1s, the db filter refetches, and the explain panel
// compiles the pasted DSL (result, RtDbError, and no-db error arms).

const adminClientMock = vi.hoisted(() => ({
  getSlowQueries: vi.fn(),
  explainQuery: vi.fn(),
}));

vi.mock("../lib/admin", async () => {
  const { RtDbError } = await import("@par-rt-db/client");
  return {
    useAdmin: () => ({
      client: adminClientMock,
      databases: ["db1", "db2"],
    }),
    RtDbRequestError: RtDbError,
  };
});

import { RtDbError } from "@par-rt-db/client";
import { SlowQueriesPage } from "./SlowQueriesPage";

function row(
  over: Partial<SlowQueriesResponse["queries"][number]>,
): SlowQueriesResponse["queries"][number] {
  return {
    startedAtMs: 1_750_000_000_000,
    db: "db1",
    table: "users",
    terminal: "select * from users",
    durationMs: 120,
    sql: "select * from users",
    params: undefined,
    ...over,
  };
}

function mockData(queries: SlowQueriesResponse["queries"]) {
  return { queries, thresholdMs: 500, capacity: 100 };
}

describe("SlowQueriesPage", () => {
  beforeEach(() => {
    adminClientMock.getSlowQueries.mockReset().mockResolvedValue(mockData([]));
    adminClientMock.explainQuery.mockReset().mockResolvedValue({
      terminal: "SELECT",
      sql: "select * from users",
      params: ["42"],
      warnings: [],
    });
  });

  it("renders the recorded list with threshold metadata and per-row durations", async () => {
    adminClientMock.getSlowQueries.mockResolvedValue(
      mockData([row({}), row({ durationMs: 1500, terminal: "seq scan", sql: "select count(*)" })]),
    );
    render(<SlowQueriesPage />);

    expect(await screen.findByText("2 recorded")).toBeTruthy();
    expect(screen.getByText("500 ms")).toBeTruthy();
    // sub-second durations stay in ms; >=1s warn in seconds
    expect(screen.getByText("120 ms")).toBeTruthy();
    expect(screen.getByText("1.50 s")).toBeTruthy();
  });

  it("expands a row to show its SQL and the params-redacted note", async () => {
    adminClientMock.getSlowQueries.mockResolvedValue(mockData([row({})]));
    render(<SlowQueriesPage />);
    expect(await screen.findByText("120 ms")).toBeTruthy();

    await userEvent.click(screen.getByText("120 ms"));
    expect(screen.getByText("SQL")).toBeTruthy();
    // terminal cell + expanded SQL pre both carry the query text
    expect(screen.getAllByText("select * from users").length).toBe(2);
    expect(screen.getByText(/params redacted/)).toBeTruthy();
  });

  it("shows the disabled banner and empty state when thresholdMs is 0", async () => {
    adminClientMock.getSlowQueries.mockResolvedValue({
      queries: [],
      thresholdMs: 0,
      capacity: 100,
    });
    render(<SlowQueriesPage />);
    expect(await screen.findByText(/Slow-query logging is disabled/)).toBeTruthy();
    expect(screen.getByText(/slow-query logging is disabled\./)).toBeTruthy();
  });

  it("refetches scoped to the selected database", async () => {
    render(<SlowQueriesPage />);
    await screen.findByText("0 recorded");
    await userEvent.selectOptions(screen.getByDisplayValue("all databases"), "db2");
    await waitFor(() => {
      expect(adminClientMock.getSlowQueries).toHaveBeenLastCalledWith({ db: "db2" });
    });
  });

  it("renders the explain result with SQL and bind params", async () => {
    render(<SlowQueriesPage />);
    await screen.findByText("0 recorded");
    // db auto-selects to the first database; explain button is enabled
    await userEvent.click(screen.getByRole("button", { name: "explain" }));
    expect(await screen.findByText("select * from users")).toBeTruthy();
    expect(screen.getByText("$1 = 42")).toBeTruthy();
    expect(adminClientMock.explainQuery).toHaveBeenCalledWith("db1", { table: "users", take: 100 });
  });

  it("renders explain compile warnings and wire errors verbatim", async () => {
    adminClientMock.explainQuery.mockRejectedValue(
      new RtDbError("BAD_REQUEST", "bad filter", undefined, 400),
    );
    render(<SlowQueriesPage />);
    await screen.findByText("0 recorded");
    await userEvent.click(screen.getByRole("button", { name: "explain" }));
    expect(await screen.findByText(/BAD_REQUEST · HTTP 400/)).toBeTruthy();
    expect(screen.getByText("bad filter")).toBeTruthy();
  });

  it("explains invalid DSL JSON as an INVALID_JSON error without a client call", async () => {
    render(<SlowQueriesPage />);
    await screen.findByText("0 recorded");
    fireEvent.change(screen.getByLabelText("Query DSL JSON"), { target: { value: "{oops" } });
    await userEvent.click(screen.getByRole("button", { name: "explain" }));
    expect(await screen.findByText("INVALID_JSON")).toBeTruthy();
    expect(adminClientMock.explainQuery).not.toHaveBeenCalled();
  });
});
