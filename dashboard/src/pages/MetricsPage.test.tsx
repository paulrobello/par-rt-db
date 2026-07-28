import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { MetricsSnapshot } from "../lib/types";

const snap: MetricsSnapshot = {
  queriesTotal: 423_901,
  mutationsTotal: 1_204,
  uploadsTotal: 7,
  wsConnections: 42,
  activeSubscriptions: 118,
  poolSize: 10,
  poolIdle: 4,
  uptimeSeconds: 3_600,
};

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ metrics: snap }),
}));

import { MetricsPage } from "./MetricsPage";

describe("MetricsPage", () => {
  it("renders the heading and instrument labels", () => {
    render(<MetricsPage />);
    expect(screen.getByText("Live instruments")).toBeInTheDocument();
    expect(screen.getByText("queries")).toBeInTheDocument();
    expect(screen.getByText("subscriptions")).toBeInTheDocument();
  });

  it("renders a sparkline per metric (role=img)", () => {
    const { container } = render(<MetricsPage />);
    expect(container.querySelectorAll("svg[role='img']").length).toBeGreaterThanOrEqual(1);
  });

  it("shows cumulative totals as sub-lines", () => {
    render(<MetricsPage />);
    expect(screen.getByText(/423,901 total/i)).toBeInTheDocument();
  });
});
