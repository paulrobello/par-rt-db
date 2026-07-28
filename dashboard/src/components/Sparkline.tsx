import type { ReactNode } from "react";
import type { Point } from "../lib/metrics-series";

const W = 100; // viewBox width; the svg stretches to its container via width:100%

export interface SparklineProps {
  values: Point[];
  stroke?: string; // default var(--accent)
  fill?: string; // default var(--accent-soft); "none" disables the area
  height?: number; // default 40 (px)
  min?: number; // optional fixed floor; else autoscale
  max?: number; // optional fixed ceiling; else autoscale
  ariaLabel: string;
  children?: ReactNode; // overlay slot (hover crosshair — Task 4)
}

export function Sparkline({
  values,
  stroke = "var(--accent)",
  fill = "var(--accent-soft)",
  height = 40,
  min,
  max,
  ariaLabel,
  children,
}: SparklineProps) {
  const n = values.length;
  const finite = values.filter((v): v is number => v != null && Number.isFinite(v));
  const lo = min ?? (finite.length ? Math.min(...finite) : 0);
  const hi = max ?? (finite.length ? Math.max(...finite) : 1);
  const span = hi - lo || 1;

  const x = (i: number) => (n <= 1 ? 0 : (i / (n - 1)) * W);
  const y = (v: number) => height - ((v - lo) / span) * height;

  // Split into contiguous non-null runs: each becomes one polyline, and the line
  // breaks (move) across nulls. Collect runs for area fill as well.
  const runs: string[][] = [];
  let cur: string[] = [];
  for (let i = 0; i < n; i++) {
    const v = values[i];
    if (v == null || !Number.isFinite(v)) {
      if (cur.length) {
        runs.push(cur);
        cur = [];
      }
      continue;
    }
    cur.push(`${x(i).toFixed(2)},${y(v).toFixed(2)}`);
  }
  if (cur.length) runs.push(cur);

  const baseline = height;
  const linePoints = runs.map((run) => run.join(" "));
  const areaPaths = runs.map((run) => {
    const first = run[0].split(",");
    const last = run[run.length - 1].split(",");
    return `M${first[0]},${baseline} L${run.join(" ")} L${last[0]},${baseline} Z`;
  });

  // Last non-null point gets a rounded dot anchored at the right edge.
  let last: { cx: number; cy: number } | null = null;
  for (let i = n - 1; i >= 0; i--) {
    const v = values[i];
    if (v != null && Number.isFinite(v)) {
      last = { cx: x(i), cy: y(v) };
      break;
    }
  }

  return (
    <svg
      viewBox={`0 0 ${W} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={ariaLabel}
      style={{ width: "100%", height, display: "block" }}
    >
      {fill !== "none" &&
        areaPaths.map((d) => <path key={d} d={d} style={{ fill }} stroke="none" />)}
      {linePoints.map((pts) => (
        <polyline
          key={pts}
          points={pts}
          fill="none"
          style={{ stroke, strokeWidth: 2, vectorEffect: "non-scaling-stroke" }}
        />
      ))}
      {last && (
        <circle
          cx={last.cx}
          cy={last.cy}
          r={2}
          style={{ fill: stroke, vectorEffect: "non-scaling-stroke" }}
        />
      )}
      {children}
    </svg>
  );
}
