import { render, screen, within } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { MetricsSnapshot } from "../lib/types";

const baseSnap: MetricsSnapshot = {
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
  subsRerunsTotal: 12,
  subsSkipsPointTotal: 80,
  subsSkipsIndexedTotal: 15,
  subsSkipsOrderedTotal: 5,
  subsSkipVerificationsTotal: 0,
  subsMissedPushesTotal: 0,
};

// mutable so individual tests can drive a different snapshot into useAdmin()
let metrics: MetricsSnapshot = baseSnap;

vi.mock("../lib/admin", () => ({
  useAdmin: () => ({ metrics }),
}));

import { MetricsPage } from "./MetricsPage";
import styles from "./MetricsPage.module.css";

/** Locate an instrument tile by its label text. */
function tileFor(label: string): HTMLElement {
  return screen.getByText(label).parentElement as HTMLElement;
}

describe("MetricsPage", () => {
  beforeEach(() => {
    metrics = baseSnap;
  });

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

  describe("subscription invalidation", () => {
    it("computes the skip rate from the three skip classes and reruns", () => {
      render(<MetricsPage />);
      const tile = tileFor("skip rate");
      // 80 + 15 + 5 = 100 skips, 12 reruns -> 100 / 112 = 89.3%.
      expect(within(tile).getByText("89.3%")).toBeInTheDocument();
      expect(within(tile).getByText("100 skipped · 12 rerun")).toBeInTheDocument();
      expect(within(tileFor("reruns")).getByText("12")).toBeInTheDocument();
      expect(within(tileFor("point skips")).getByText("80")).toBeInTheDocument();
      expect(within(tileFor("indexed skips")).getByText("15")).toBeInTheDocument();
      expect(within(tileFor("ordered skips")).getByText("5")).toBeInTheDocument();
    });

    it("renders an em dash for the skip rate when no decisions have been made yet", () => {
      metrics = {
        ...baseSnap,
        subsRerunsTotal: 0,
        subsSkipsPointTotal: 0,
        subsSkipsIndexedTotal: 0,
        subsSkipsOrderedTotal: 0,
      };
      render(<MetricsPage />);
      expect(within(tileFor("skip rate")).getByText("—")).toBeInTheDocument();
    });

    it("presents verification as inactive (not a zero result) when the sampler is off", () => {
      render(<MetricsPage />); // baseSnap has subsSkipVerificationsTotal: 0
      const verifyTile = tileFor("skip verification");
      const value = within(verifyTile).getByText("off");
      expect(value.classList.contains(styles.instrumentValue_muted)).toBe(true);
      expect(within(verifyTile).getByText("RTDB_SUBS_VERIFY_SKIP_EVERY unset")).toBeInTheDocument();

      // A 0 missed-push count while verification is off must read as "unsampled", not "confirmed clean".
      const missedTile = tileFor("missed pushes");
      expect(within(missedTile).getByText("0")).toBeInTheDocument();
      expect(within(missedTile).getByText("sampling off")).toBeInTheDocument();
      expect(missedTile.classList.contains(styles.instrument_alarm)).toBe(false);
    });

    it("shows the verification count once the sampler is active", () => {
      metrics = { ...baseSnap, subsSkipVerificationsTotal: 500, subsMissedPushesTotal: 0 };
      render(<MetricsPage />);
      const verifyTile = tileFor("skip verification");
      const value = within(verifyTile).getByText("500");
      expect(value.classList.contains(styles.instrumentValue_muted)).toBe(false);
      expect(within(verifyTile).getByText("sampled shadow checks")).toBeInTheDocument();

      const missedTile = tileFor("missed pushes");
      expect(within(missedTile).getByText("of 500 verified")).toBeInTheDocument();
      expect(missedTile.classList.contains(styles.instrument_alarm)).toBe(false);
    });

    it("makes a non-zero missed-push count visually unmissable (alarm state)", () => {
      metrics = { ...baseSnap, subsSkipVerificationsTotal: 500, subsMissedPushesTotal: 3 };
      render(<MetricsPage />);
      const missedTile = tileFor("missed pushes");
      expect(missedTile.classList.contains(styles.instrument_alarm)).toBe(true);
      const value = within(missedTile).getByText("3");
      expect(value.classList.contains(styles.instrumentValue_alarm)).toBe(true);
      expect(within(missedTile).getByText("of 500 verified")).toBeInTheDocument();
    });
  });
});
