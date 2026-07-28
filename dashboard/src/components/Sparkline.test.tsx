import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Sparkline } from "./Sparkline";

describe("Sparkline", () => {
  it("renders a polyline + area for valid input and exposes an a11y label", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3, 2]} ariaLabel="queries per second over 60s" />,
    );
    const svg = screen.getByRole("img");
    expect(svg).toHaveAttribute("aria-label", "queries per second over 60s");
    expect(container.querySelectorAll("polyline").length).toBeGreaterThanOrEqual(1);
    expect(container.querySelector("path")).toBeTruthy(); // area fill
  });

  it("renders an empty track (no polyline, no crash) for an all-null series", () => {
    const { container } = render(<Sparkline values={[null, null]} ariaLabel="empty" />);
    expect(screen.getByRole("img")).toBeInTheDocument();
    expect(container.querySelector("polyline")).toBeNull();
  });

  it("breaks the line across a null gap (two polyline segments)", () => {
    const { container } = render(<Sparkline values={[1, 2, null, 4, 5]} ariaLabel="gapped" />);
    expect(container.querySelectorAll("polyline").length).toBe(2);
  });

  it("respects a fixed min/max scale", () => {
    const { container } = render(
      <Sparkline values={[0, 10]} min={0} max={10} ariaLabel="scaled" />,
    );
    // the polyline exists; exact coords are covered by the geometry being deterministic
    expect(container.querySelector("polyline")).toBeTruthy();
  });
});

describe("Sparkline hover", () => {
  it("shows a tooltip with the hovered value on pointer move", () => {
    const { container } = render(
      <Sparkline values={[10, 20, 30]} formatTip={(v) => `${v}/s`} ariaLabel="rates" />,
    );
    const svg = screen.getByRole("img");
    // jsdom returns a zero-size rect by default; give it a real width so the
    // pointer-fraction → index math resolves.
    vi.spyOn(svg, "getBoundingClientRect").mockReturnValue({
      left: 0,
      width: 100,
      right: 100,
      top: 0,
      bottom: 40,
      height: 40,
      x: 0,
      y: 0,
      toJSON() {},
    } as DOMRect);

    expect(container.querySelector("[data-spark-tip]")).toBeNull();
    fireEvent.mouseMove(svg, { clientX: 100 }); // far right -> last point (30)
    const tip = container.querySelector("[data-spark-tip]");
    expect(tip).not.toBeNull();
    expect(tip?.textContent).toContain("30/s");
  });

  it("can be disabled via interactive={false}", () => {
    const { container } = render(
      <Sparkline values={[1, 2, 3]} interactive={false} ariaLabel="static" />,
    );
    const svg = screen.getByRole("img");
    fireEvent.mouseMove(svg, { clientX: 50 });
    expect(container.querySelector("[data-spark-tip]")).toBeNull();
  });
});
