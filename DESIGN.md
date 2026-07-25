# Design — par-rt-db console

<!-- impeccable:design 1 -->

> **Direction contract (chosen: Instrument Manual)**
>
> **THESIS** — A dark instrument console that treats the database as a live machine, refusing the generic stat-card admin. Precision plus committed motion are the proof it is alive.
> **OWN-WORLD** — Matte near-black panel, hairline engineering grid, off-white ink, one phosphor-green accent, amber/red reserved strictly for state. Monospace carries data, numerics, and identifiers; a grotesk carries navigation and labels. Hairline-ruled tables; spec-sheet calm for schema and config.
> **STORY** — The operator opens a live database and immediately sees documents moving, counts ticking, operations streaming in the rail — then reaches schema, metrics, or config in the same disciplined language.
> **FIRST VIEWPORT** — command rail left, dense document table center with a ticking count, live op-feed trace right, the connection pulse lit. Primary action: open a table.
> **FORM** — Technical-publication instrument manual; grounded candidate #5; staging = data browser on a live database; concept-seed key `45f0c85a`.

## Mode
Operate. Scanability, consistency, and the real usage scene outrank expression. Energy is permitted where it proves liveness, never as decoration.

## Color strategy
Restrained. Neutrals carry the surface; one accent (phosphor green) marks the live/active register; amber and red encode state only (warn / error). Color commits at field/region scale, never as scattered accents on a neutral ground.

## Palette (provisional until the first build settles them)
- Panel ground `#0d0e10` (matte near-black). Raised surface `#141619`. Recessed / inset `#0a0b0d`.
- Hairline rule / grid `rgba(231,233,234,0.10)`. Stronger divider `rgba(231,233,234,0.16)`.
- Ink primary `#e7e9ea`. Ink secondary `#9aa0a6`. Ink muted `#5f656d`.
- Accent (live / active) `#3dd68c` phosphor green; accent-dim `#2a9d68`.
- Status warn `#f5a524` amber. Status error `#f4515e` red. Status ok = accent.
- Focus / selection ring: accent at ~0.5 alpha.

Dark is primary (physical scene: dim, dev-workflow, long sessions). A light theme is out of scope for v1.

## Typography
- Data, numerics, identifiers, code, op-feed lines, tokens → monospace (`ui-monospace, "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace`).
- Navigation, labels, headings, prose → grotesk (`-apple-system, "Segoe UI", Roboto, Helvetica, Arial`).
- Tabular figures (`font-variant-numeric: tabular-nums`) on every numeric readout so live-ticking values don't jitter.
- Hierarchy by size / weight / color, never decoration. Mono uppercase tracked micro-labels caption sections like spec-sheet placards.

## Grid & spacing
- 4px base unit. 1px hairline rules on the engineering grid. More space above a heading than below it.
- Dense surfaces (data browser, op feed, metrics): tight 8–12px rhythm, many rows in view. Calm surfaces (schema, config): 16–24px rhythm, generous margins, document-like.

## Density rule (by surface)
- Dense + live: data browser, op feed, metrics, databases list. Multi-pane; the live rail visible.
- Calm + precise: schema viewer, hot config, admin management. Focused, documented, hairline-ruled.

## Components (rebuilt in the form's vocabulary — never stock)
- **Command rail** — slim left rail, mono database list, nav labels, active item marked with accent.
- **Live rail** — right column; op-feed trace + activity pulse. Collapses on calm surfaces.
- **Data table** — hairline-ruled rows, mono cells, sticky header, no zebra, hover reveals row actions, live cells tick (damped) on change.
- **Instrument readout (metrics)** — value + trend + status lamp; damped spring motion, never snap.
- **Op-feed trace** — scrolling telemetry log; monochrome with accent for the active event; severity tinted (amber/red).
- **Status lamp** — small dot/chip; ok = accent, warn = amber, error = red, idle = muted.
- **Buttons / inputs / links** — squared or micro-radius, hairline-bordered, mono labels for data actions; accent only on the single primary action per view.
- **Placards** — mono uppercase tracked micro-labels captioning sections like a spec sheet.

## Motion (material, bounded)
- Values tick / flash briefly on change (accent, ~400ms fade).
- Connection pulse — a slow, subtle accent breath indicating the live link is up; warn / red on lag, muted on disconnect.
- Op feed scrolls a live trace; entries settle with a brief accent, never bounce.
- Damped springs on instruments; `prefers-reduced-motion` disables non-essential motion.
- Motion proves liveness; it is never ambient decoration.

## Iconography
Icons drawn in the world's own grammar — thin-stroke, geometric, instrument-panel symbology. No emoji, no rounded filled icon tiles.

## Responsive
Desktop-first (operator on a monitor). Degrade gracefully to tablet / phone: rails collapse, tables become card-lists, instruments stack by criticality. Core actions stay reachable.

## Bans (this world refuses)
- The generic SaaS admin: rounded stat-cards with up/down arrows over a pastel ground, avatar top-bar.
- Skeuomorphic 3D, glossy gradients, glassmorphism, neon glow halos.
- The faux "hacker terminal" (green-on-black scanlines, CRT, Matrix rain) — the world is precise, not theatrical.
- Scattered accent color; color earns its place by encoding meaning.
- Stock component chrome inside committed surfaces.

## What is not literalized from the concept render
The render is a north star for palette, density, and mood — not a pixel spec. Exact layout, real data shapes, interaction states, accessibility, and responsive behavior are implementation responsibilities resolved against the backend contract, not copied from the render.
