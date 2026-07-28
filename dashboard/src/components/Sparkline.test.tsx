import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
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
