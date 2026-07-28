import { type ReactNode, useRef, useState } from "react";
import { nearestIndex, type Point } from "../lib/metrics-series";

const W = 100; // viewBox width; the svg stretches to its container via width:100%

export interface SparklineProps {
  values: Point[];
  stroke?: string; // default var(--accent)
  fill?: string; // default var(--accent-soft); "none" disables the area
  height?: number; // default 40 (px)
  min?: number; // optional fixed floor; else autoscale
  max?: number; // optional fixed ceiling; else autoscale
  ariaLabel: string;
  interactive?: boolean; // default true — crosshair + tooltip
  formatTip?: (v: number) => string; // default: String(value)
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
  interactive = true,
  formatTip,
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
  // `runs` holds only non-empty arrays: every push is guarded by a `cur.length` check.
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

  const svgRef = useRef<SVGSVGElement>(null);
  const [hover, setHover] = useState<number | null>(null);
  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    if (!interactive) return;
    const rect = svgRef.current?.getBoundingClientRect();
    if (!rect || rect.width === 0) return;
    const fraction = (e.clientX - rect.left) / rect.width;
    setHover(nearestIndex(fraction, n));
  };
  const onLeave = () => setHover(null);

  const candidate = hover != null ? values[hover] : null;
  const hoverValue = candidate != null && Number.isFinite(candidate) ? candidate : null;
  const tipText =
    hoverValue != null ? (formatTip ? formatTip(hoverValue) : String(hoverValue)) : null;

  return (
    <div style={{ position: "relative", width: "100%" }}>
      <svg
        ref={svgRef}
        viewBox={`0 0 ${W} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={ariaLabel}
        onMouseMove={onMove}
        onMouseLeave={onLeave}
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
        {hover != null && (
          <line
            x1={x(hover)}
            y1={0}
            x2={x(hover)}
            y2={height}
            style={{
              stroke: "var(--rule-strong)",
              strokeWidth: 1,
              vectorEffect: "non-scaling-stroke",
            }}
          />
        )}
        {children}
      </svg>
      {tipText != null && hover != null && (
        <span
          data-spark-tip
          style={{
            position: "absolute",
            left: `${(hover / Math.max(n - 1, 1)) * 100}%`,
            top: 0,
            transform: "translateX(-50%)",
            padding: "1px 4px",
            fontFamily: "var(--mono)",
            fontSize: "var(--t-mono-xs)",
            color: "var(--ink)",
            background: "var(--inset)",
            border: "1px solid var(--rule)",
            whiteSpace: "nowrap",
            pointerEvents: "none",
          }}
        >
          {tipText}
        </span>
      )}
    </div>
  );
}
