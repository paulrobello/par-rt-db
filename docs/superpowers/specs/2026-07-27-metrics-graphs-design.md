# Metrics Trend Graphs — Design

- **Date:** 2026-07-27
- **Status:** Approved (brainstormed)
- **Scope:** `dashboard/` only — no server, SDK, or other-client changes
- **Depends on:** existing `/admin/stream` `gauges` push (1 snapshot / second)

## Goal

Add live trend graphs to the operator dashboard's `/metrics` page. Each numeric
"instrument" tile keeps its headline value and gains a 60-second rolling sparkline
so an operator sees *shape over time*, not just the instantaneous number. No new
runtime dependencies; no server changes.

## Context — current state

- `dashboard/src/pages/MetricsPage.tsx` renders seven static numeric tiles across
  three panels:
  - **Activity (cumulative counters):** `queriesTotal`, `mutationsTotal`, `uploadsTotal`
  - **Live (gauges):** `wsConnections`, `activeSubscriptions`, `poolSize` (with
    `poolIdle` shown as a busy/idle sub-line), and **System:** `uptimeSeconds`
- `server/src/metrics.rs` documents that **rates are derived client-side from
  successive snapshots** — the server exposes only raw counters/gauges.
- `server/src/admin.rs` pushes a `{kind:"gauges", gauges: MetricsSnapshot}` frame
  every 1 second (`Duration::from_secs(1)`, line 864) over the `/admin/stream`
  WebSocket; `dashboard/src/lib/admin.tsx` stores only the latest snapshot in
  `AdminProvider` state (it discards history).
- `dashboard` has **no charting dependency** (only `react`, `react-dom`,
  `react-router-dom`) and a hand-rolled dark "Instrument Manual" design system
  (`dashboard/src/styles/tokens.css`): phosphor-green accent `#3dd68c`, monospace
  type, squared radii, tabular numerics, hairline rules.

## Non-goals

- No server-side metrics, no persisted/historical series beyond the in-memory
  60-second window.
- No new npm dependency (charts are hand-rolled SVG).
- No changes to `AdminProvider`'s public shape, to other dashboard pages, to the
  server, or to any client SDK.
- No dual-axis or multi-metric-overlaid charts (metrics differ wildly in scale —
  `queries/s` in the 100s vs `uploads/s` near zero — so each is its own chart).

## Architecture — rolling history

A dedicated hook accumulates the snapshot stream; pure functions derive series.

### `dashboard/src/lib/useMetricsHistory.ts`

- Reads `metrics: MetricsSnapshot | null` from `useAdmin()`.
- On each new snapshot, appends `{ t: number; snap: MetricsSnapshot }` to a
  `useRef`-held ring buffer, where `t` is `Date.now()` stamped **on receipt**
  (snapshots carry no server timestamp; receive-time is sufficient for rate math).
- Cap: the **most recent `MAX_SAMPLES = 61`** samples (60 s of history + the
  current point). Older entries are dropped (FIFO).
- Returns `{ samples: Sample[] }`. Re-renders the consumer each second — which is
  intended and localized to `/metrics` (no other page mounts this hook).

> **Why a hook, not `AdminProvider`?** No other page needs history; storing it in
> the provider would force every page to re-render once per second. The hook keeps
> the 1 Hz churn local to `/metrics` and leaves `AdminProvider` untouched (surgical).

### `dashboard/src/lib/metrics-series.ts` (pure, fully unit-tested)

All edge-case logic lives here, not in components.

```ts
export interface Sample { t: number; snap: MetricsSnapshot }
export type Point = number | null;   // null = gap (break the line)

// Derive per-second rates from a cumulative counter across the sample window.
// Output length == samples.length; index 0 is null (needs two samples).
export function rateSeries(
  samples: Sample[],
  counter: (s: MetricsSnapshot) => number,
): Point[]

// Extract a gauge's level over the window (no derivation).
export function levelSeries(
  samples: Sample[],
  gauge: (s: MetricsSnapshot) => number,
): Point[]
```

**Edge-case rules (all in `rateSeries`, all tested):**

- **Rate formula:** `rate[i] = (c[i] − c[i−1]) / (t[i] − t[i−1])`, seconds.
- **First sample:** `rate[0] = null` (needs a predecessor).
- **Counter reset** (server restarted, `c[i] < c[i−1]`): emit `null` and resume
  accumulation from `c[i]` — never a negative rate.
- **`dt` clamp:** normalize `dt` into `[DT_MIN, DT_MAX] = [0.5 s, 5.0 s]` so network
  jitter doesn't spike the rate.
- **Gap (dt > DT_MAX, e.g. reconnect or background-tab throttle):** emit `null`
  rather than a misleadingly low averaged rate; the sparkline draws a break.
- **Empty window / single sample:** return an all-`null` array of the right length.

`levelSeries` needs none of this — it is a straight projection — but still returns
`null`-padded output of length `samples.length` so both series types share a
sparkline component.

## Components

### `dashboard/src/components/Sparkline.tsx`

A pure, dependency-free SVG. ~60 lines.

```ts
interface SparklineProps {
  values: Point[];            // one series; nulls break the line
  stroke?: string;            // default var(--accent)
  fill?: string;              // default var(--accent-soft); set to "none" to disable area
  height?: number;            // default 40 (px); width is 100% of container
  min?: number; max?: number; // optional fixed scale; else autoscale to non-null range (with a small pad)
  lastDot?: boolean;          // default true — rounded dot on the newest point
  ariaLabel: string;          // required — the sparkline is a decorative-ish image of the trend
}
```

- **Geometry:** `viewBox="0 0 W H"` with `preserveAspectRatio="none"` (sparklines
  intentionally stretch). X is index/(n−1)·W; Y maps `[min,max]` → `[H, 0]`.
- **Line:** 2 px `polyline` through non-null points; on a `null`, `M` (move) instead
  of `L` so the line breaks across gaps.
- **Area:** optional soft fill from the line down to the baseline, drawn only over
  contiguous non-null runs (so a gap doesn't fill across the break).
- **Last-point dot:** 2 px radius, rounded, anchored on the baseline edge per the
  dataviz mark spec.
- **Empty input** (no non-null values): render an empty recessive track, no path,
  no throw.
- **A11y:** `role="img"` + `aria-label={ariaLabel}`; the precise current value is
  already visible in the tile, so the SVG itself carries no text.

### `dashboard/src/pages/MetricsPage.tsx` (edited)

- The local `Instrument` helper gains an optional `sparkline?: ReactNode` slot
  rendered beneath the value/sub.
- The page mounts `useMetricsHistory()` once and derives the series it needs.
- A lightweight hover layer (see *Interaction*) is wired in the tile, not in
  `Sparkline`, so the SVG stays pure.

## Visuals & layout

Stays inside the existing `Panel` / instrument grid; themed entirely from
`tokens.css`. The dataviz skill's "small multiples for differing scales" rule is
satisfied by giving each metric its own tile+sparkline.

- **Activity · rates** — the three counters become *rate* tiles:
  - Headline = current rate, e.g. `12.4/s` (computed from the last two samples).
  - Sub-line = cumulative total, e.g. `423,901 total`.
  - Sparkline = `rateSeries(...)` over 60 s.
  - *Rationale:* a counter's informative trend is its **slope**; plotting the
    cumulative total would just draw a line going monotonically up-right.
- **Live · levels** — the gauges keep their current headline value and add a
  `levelSeries` sparkline:
  - `wsConnections`, `activeSubscriptions`: single-series line + area.
  - **Pool:** a two-series **stacked area** — busy (`poolSize − poolIdle`, filled
    `--accent`) over idle (`poolIdle`, filled muted `--ink-3` @ low alpha) — so the
    top edge == `poolSize` and the colored band shows utilization. Identity is
    reinforced by the existing `6 busy · 4 idle` sub-line (not color-alone). This is
    the only multi-series chart; it is rendered by extending `Sparkline` to accept an
    optional second stacked series rather than a separate component.
- **System · uptime:** unchanged tile, **no sparkline** (monotonic, uninformative).

Mark specs (dataviz): 2 px strokes, thin marks, no axes (sparkline convention), the
current value direct-labeled in the tile. Grid/axes are recessive or absent.

## Color (validated, not eyeballed)

Per the dataviz skill, color is validated with `scripts/validate_palette.js`:

- Single-series sparklines use one hue (`--accent`); a single-series chart needs no
  legend (the tile label names it).
- The pool's two-series case (busy green vs idle gray) is run through the validator
  against the dark surface (`--surface #141619`). Green-vs-gray is also redundantly
  encoded by the `busy/idle` sub-line, so identity is never color-alone. If any pair
  fails CVD separation we widen the gray toward a cooler neutral until it passes.
- Text (values, labels, subs) stays in `--ink` / `--ink-2` / `--ink-3` — never the
  series color.

## Interaction (hover layer)

`Sparkline` is an interactive chart, so it ships a hover layer by default (dataviz
step 5), kept minimal:

- A vertical crosshair line following the pointer's nearest x-index.
- A tooltip showing that sample's value and relative time (e.g. `12.4/s · 12 s ago`).
- Hit target spans the full chart height (bigger than the 2 px line).
- On touch / no-hover devices it degrades gracefully to the static line (no
  pointer events → no tooltip); no functionality is lost since the current value is
  always shown in the tile.

## Accessibility

- Each sparkline: `role="img"` + descriptive `aria-label` (e.g. *"queries per second
  over the last 60 seconds, current 12.4"*).
- Headline numbers and sub-lines remain real text (screen-reader-friendly).
- Identity is not color-alone (labels + the busy/idle sub-line).
- Motion is bounded (existing `--tick` / `--ease` tokens); the only animation is the
  last-point dot settling. `prefers-reduced-motion` disables it.

## Testing

Pure logic is the priority (R4 forced verification).

- `dashboard/src/lib/metrics-series.test.ts` — `rateSeries`: basic slope, first-sample
  null, counter reset → null + resume, `dt` clamp bounds, gap on `dt > DT_MAX`,
  empty/single-sample. `levelSeries`: projection + null padding.
- `dashboard/src/components/Sparkline.test.tsx` — renders with valid input (asserts a
  `polyline` is present), renders empty track for all-null input (no crash), breaks
  the line across a `null` gap (multiple `polyline`/path segments).
- `dashboard/src/pages/MetricsPage.test.tsx` (**created** — no such test exists today;
  the dashboard's only page tests are `useLiveTable`, `session`, `ConfigPage`) — smoke
  test that the page renders rate tiles and sparklines given a seeded metrics stream.

## Verification

1. Ensure dashboard deps are installed in the worktree (`make dashboard-install`) and
   that `ts-client/dist` is built (`make ts-client-build`) — the dashboard typecheck
   resolves `@par-rt-db/client` from it, and a fresh git worktree carries no
   gitignored `node_modules`.
2. **Gate:** `make checkall` — fmt-check + clippy + `tsc` + vitest across all five
   packages. Must pass clean before commit.
3. **Eyeball:** run the dashboard dev server, load `/metrics`, confirm sparklines
   fill in over the first 60 s, the pool area stacks correctly, a reconnect
   introduces a visible gap, and reduced-motion disables the dot animation.
4. **Palette:** `node scripts/validate_palette.js …` on the pool pair before ship.

## File manifest

**New:**
- `dashboard/src/lib/useMetricsHistory.ts`
- `dashboard/src/lib/metrics-series.ts`
- `dashboard/src/lib/metrics-series.test.ts`
- `dashboard/src/components/Sparkline.tsx`
- `dashboard/src/components/Sparkline.test.tsx`

**Edited (surgical):**
- `dashboard/src/pages/MetricsPage.tsx` (Instrument slot + series wiring)
- `dashboard/src/pages/MetricsPage.module.css` (sparkline/tile-ratio styles)
- `dashboard/src/pages/MetricsPage.test.tsx` (created)

**Untouched:** server, all client SDKs, `AdminProvider`, other dashboard pages.
