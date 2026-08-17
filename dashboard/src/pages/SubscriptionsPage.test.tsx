import { render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { SubscriptionsResponse } from "../lib/types";
import styles from "./SubscriptionsPage.module.css";

// SubscriptionsPage lists active subscriptions (GET /admin/subscriptions) and a
// counters panel. It must render rows for both interactive-user and system
// (null-principal) subscribers, show the global counters, and fall back to an
// empty state. It auto-polls on an interval, so tests use fake timers to keep
// the initial fetch deterministic.

const adminClientMock = vi.hoisted(() => ({
  listSubscriptions: vi.fn(),
}));

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({
    client: adminClientMock,
    databases: ["db1", "db2"],
  }),
}));

import { SubscriptionsPage } from "./SubscriptionsPage";

const twoSubs: SubscriptionsResponse = {
  subscriptions: [
    {
      db: "db1",
      table: "workItems",
      terminal: "collect",
      readSetClass: "indexed",
      principal: { userId: "user-1", email: "alice@example.com" },
    },
    {
      db: "db2",
      table: "workItems",
      terminal: "get",
      readSetClass: "point",
      principal: null, // machine token / admin bypass → "system"
    },
  ],
  subsRerunsTotal: 5,
  subsSkipsPointTotal: 1,
  subsSkipsIndexedTotal: 2,
  subsSkipsOrderedTotal: 3,
  subsMissedPushesTotal: 0,
  perDb: [
    {
      db: "db1",
      reruns: 4,
      skipsPoint: 1,
      skipsIndexed: 2,
      skipsOrdered: 3,
      missed: 0,
      skips: 6,
      rerunRatio: 0.4,
    },
  ],
};

describe("SubscriptionsPage", () => {
  beforeEach(() => {
    adminClientMock.listSubscriptions.mockReset();
  });

  it("lists subscriptions and surfaces the invalidation counters", async () => {
    adminClientMock.listSubscriptions.mockResolvedValue(twoSubs);
    render(<SubscriptionsPage />);

    // Default db filter is "" (all databases) → call carries no db.
    await waitFor(() => {
      expect(adminClientMock.listSubscriptions).toHaveBeenCalledWith({});
    });

    // Both subscriptions render, including the system (null-principal) row.
    expect(await screen.findByText("alice@example.com")).toBeInTheDocument();
    expect(screen.getByText("system")).toBeInTheDocument();
    expect(screen.getByText("get")).toBeInTheDocument();

    // Global counters render (5 re-runs, 0 missed).
    expect(screen.getByText("5")).toBeInTheDocument();
  });

  it("renders an empty state when there are no subscriptions", async () => {
    adminClientMock.listSubscriptions.mockResolvedValue({
      ...twoSubs,
      subscriptions: [],
      perDb: [],
    });
    render(<SubscriptionsPage />);
    expect(await screen.findByText(/no active subscriptions/i)).toBeInTheDocument();
  });

  it("warns on a per-db rerun ratio above 0.5 and not below", async () => {
    // db1 at 0.25 stays neutral; db2 at 0.80 gets the amber warning treatment.
    adminClientMock.listSubscriptions.mockResolvedValue({
      ...twoSubs,
      perDb: [
        {
          db: "db1",
          reruns: 2,
          skipsPoint: 6,
          skipsIndexed: 0,
          skipsOrdered: 0,
          missed: 0,
          skips: 6,
          rerunRatio: 0.25,
        },
        {
          db: "db2",
          reruns: 8,
          skipsPoint: 2,
          skipsIndexed: 0,
          skipsOrdered: 0,
          missed: 0,
          skips: 2,
          rerunRatio: 0.8,
        },
      ],
    });
    render(<SubscriptionsPage />);

    const high = await screen.findByText("0.80");
    expect(high.classList.contains(styles.warnText)).toBe(true);
    expect(high.getAttribute("title")?.includes("write latency to its subscriber load")).toBe(true);

    const low = screen.getByText("0.25");
    expect(low.classList.contains(styles.warnText)).toBe(false);
  });
});
