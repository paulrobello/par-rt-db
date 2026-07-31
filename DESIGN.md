# Design — par-rt-db console

<!-- impeccable:design 1 -->

> **Direction contract (chosen: par-mem ops cockpit)**
>
> **THESIS** — A deep-navy operations cockpit that treats the database as a live
> machine: rounded glowing cards, a glassy masthead, big mono readouts, and teal
> liveness. Adopted from the **par-mem** dashboard (`../par-mem/web`) — that
> aesthetic is the pinned reference; this is its visual language on par-rt-db's
> own layout and content.
> **OWN-WORLD** — Deep-navy ground lit by teal + violet radial glows and a faint
> masked grid; rounded gradient panels with a teal→violet top hairline and soft
> shadow; cool off-white ink over blue-tinted hairlines; teal is the live/active
> register, signal-green marks positive/ok, violet and amber/red encode kind and
> state. Monospace carries data, numerics, and identifiers; a humanist sans
> carries navigation, headings, and labels.
> **STORY** — The operator opens the console to a glassy masthead and floating
> rounded rails: databases and nav on the left, the live op-feed glowing on the
> right, and the work center stage — instruments as big numeric cards, document
> tables as rounded shells, values flashing as they change.
> **FIRST VIEWPORT** — glassy masthead top, rounded command rail left, dense
> rounded document table center with a live count, live op-feed rail right, the
> connection dot glowing. Primary action: open a table.
> **FORM** — par-mem ops cockpit (pinned reference: `pm-*` dashboard CSS in
> `../par-mem/web/src/dashboard/index.ts` and `APP_CSS` in
> `../par-mem/web/src/app/theme.ts`). Layout and content are par-rt-db's own.

## Mode
Operate. Scanability, consistency, and the real usage scene outrank expression.
Energy is permitted where it proves liveness (glow, flash, pulse), never as
ambient decoration.

## Color strategy
Restrained but warmer than a flat admin. Navy neutrals carry the surface; teal
(`--accent` / `--flux`) marks the live/active register; signal-green marks
ok/positive; violet and amber/red encode kind and state (warn / error). Color
earns its place by encoding meaning; glows reinforce live/active state only.

## Palette
- Panel ground `#0a0f1e` (deep navy). Raised surface `#0e1628`. Recessed `#09101f`.
- Hairline `rgba(129,156,204,0.15)`. Stronger divider `rgba(129,156,204,0.28)`.
- Ink primary `#f2f6ff`. Ink secondary `#95a4bf`. Ink muted `#61708d`.
- Accent (live / active) `#45e6d3` teal; accent-dim `#2bb8a8`; accent-soft `rgba(69,230,211,0.14)`.
- Signal (ok / positive) `#79f2ad`. Violet `#a998ff`.
- Status warn `#ffc66d` amber. Status error `#ff766c` alert. Status ok = signal.
- Atmosphere: teal radial glow top-right + violet radial glow mid-left over a vertical navy gradient; a faint masked 54px grid underneath.
- Card material: `linear-gradient(145deg, rgba(15,23,42,.96), rgba(7,12,24,.96))`, `box-shadow: 0 18px 48px rgba(0,0,0,.18), inset 0 1px 0 rgba(255,255,255,.025)`, teal→violet top hairline.

Dark is primary (physical scene: dim, dev-workflow, long sessions). A light theme is out of scope.

## Typography
- Data, numerics, identifiers, code, op-feed lines, tokens → monospace (`"JetBrains Mono", "SF Mono", ui-monospace, Menlo, Consolas, monospace`).
- Navigation, labels, headings, prose → humanist sans (`"Avenir Next", "IBM Plex Sans", "Helvetica Neue", -apple-system, "Segoe UI", Roboto, sans-serif`); display weight for page titles.
- Big readout numerics at `pm-num` scale (`~30–34px`, weight 650, `letter-spacing: -0.045em`).
- Tabular figures (`font-variant-numeric: tabular-nums`) on every numeric readout so live-ticking values don't jitter.
- Eyebrow labels: mono, ~9px, weight 650, uppercase, tracked ~0.16em, muted — caption sections (was "placards").

## Grid & spacing
- 4px base unit. Floating rounded panels separated by a ~14px gutter (masthead + body padding). More space above a heading than below it.
- Dense surfaces (data browser, op feed, metrics, databases list): card grids, tight rhythm, many rows in view. Calm surfaces (schema, config): rounded panels, generous margins, document-like.

## Density rule (by surface)
- Dense + live: data browser, op feed, metrics, databases list. Rounded cards/rows; the live rail visible.
- Calm + precise: schema viewer, hot config, admin management. Rounded panels, focused, hairline-ruled.

## Components (in the par-mem vocabulary)
- **Masthead** — glassy sticky top bar: translucent navy, `backdrop-filter: blur(22px) saturate(140%)`, bottom hairline + shadow, brand mark with a teal glyph.
- **Command rail** — rounded floating panel; nav items active = accent-soft fill + accent left border.
- **Live rail** — rounded floating panel; op-feed rows with glowing kind-colored glyphs; the newest event settles with a teal wash.
- **Cards** — rounded (15px) gradient panels with soft shadow + teal→violet top hairline; hover lifts and adds a faint teal wash.
- **Data table** — rounded shell, blue-tinted hairline rows, sticky header, mono cells, teal hover tint, live count flashes on change.
- **Instrument readout (metrics)** — big mono value + eyebrow label + sub + sparkline; alarm state = alert border/glow.
- **Sparkline** — teal line with a drop-shadow glow, teal area, signal-green "now" dot with glow.
- **Status lamp / connection pulse** — glowing dot; ok/signal, warn/amber, error/alert, idle/muted; the live link breathes.
- **Buttons / inputs** — rounded (9px), hairline-bordered; hover lifts + teal border; the single primary action per view is solid teal with a soft glow.
- **Eyebrows** — mono uppercase tracked micro-labels captioning sections.

## Motion (material, bounded)
- Values flash signal-green on change (`~800ms`, returns to the class color so alarms keep their meaning).
- Connection dot glows and breathes when live; warn on lag, muted on disconnect.
- Op feed's newest entry settles with a brief teal wash; the data browser's newest row does the same.
- `prefers-reduced-motion` disables non-essential motion. Motion proves liveness; it is never ambient decoration.

## Iconography
Minimal, geometric, drawn in the world's own grammar. No emoji, no decorative
rounded filled icon tiles; a small diamond/◆ brand glyph and dot/orbit markers carry identity and state.

## Responsive
Desktop-first (operator on a monitor). Degrade gracefully to tablet / phone: rails collapse, tables become card-lists, instruments stack by criticality. Core actions stay reachable.

## Bans (this world refuses)
- The generic SaaS admin over a pastel ground with an avatar top-bar.
- Skeuomorphic 3D and the faux "hacker terminal" (scanlines, CRT, Matrix rain).
- Scattered accent color; color earns its place by encoding meaning.
- Stock component chrome inside committed surfaces.

## What is not literalized from par-mem
par-mem's layout (graph workspace stage, reorderable card canvas, repo/worktree
selectors) is **not** adopted — par-rt-db keeps its own routes, content, and
information architecture. Only par-mem's visual language is ported. Exact layout,
real data shapes, interaction states, accessibility, and responsive behavior are
implementation responsibilities resolved against par-rt-db's backend contract.
