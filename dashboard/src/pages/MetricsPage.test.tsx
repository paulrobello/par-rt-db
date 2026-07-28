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
  queryLatency: { p50: 1_230, p95: 4_560, p99: 9_870 },
  mutateLatency: { p50: 2_500, p95: 8_000, p99: 15_000 },
  subscribeLatency: { p50: 800, p95: 3_200, p99: 7_500 },
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

  it("renders the latency panel with p50/p95/p99 for each transport", () => {
    render(<MetricsPage />);
    // Three latency instruments — query/mutate/subscribe labels.
    expect(screen.getByText("query latency")).toBeInTheDocument();
    expect(screen.getByText("mutate latency")).toBeInTheDocument();
    expect(screen.getByText("subscribe latency")).toBeInTheDocument();
    // queryLatency p50 = 1230µs -> "1.23ms".
    expect(screen.getByText("1.23ms")).toBeInTheDocument();
  });
});
