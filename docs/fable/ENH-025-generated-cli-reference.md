# ENH-025 — Generated, drift-gated CLI reference

## Goal

Stop `cli/README.md` from drifting (audit finding DOC-202 found a false `--url` default and 7 of 16
commands undocumented) by generating the command-reference section from the CLI's own `--help`
output and gating it in `make checkall` — the same pattern the kanban CLI uses
(`tests/cli/skill-sync.test.ts` keeping its skill doc in sync with `USAGE`).

## Current state

- `cli/README.md` is hand-written; DOC-202 (this cycle) rewrites it correct once, but nothing stops
  the next subcommand from landing undocumented.
- The CLI is clap-derive based in `cli/src/main.rs` (post-ARC-206: `cli/src/args.rs`), 16 commands.
- **Sequence after DOC-202 and ARC-206** so the generator targets the settled surface.

## Implementation

1. **Generator**: add `cli/src/bin/gen-cli-docs.rs` (or a `--generate-docs` hidden flag on the main
   binary — prefer the separate bin; it keeps the shipped binary clean). It builds the clap
   `Command` (factor the clap definition into a `pub fn cli() -> clap::Command` so both main and the
   generator share it), then renders markdown: one `###` section per subcommand with its
   `--help` text in a fenced block, plus the global-flags/env-var table (clap `env=` metadata is
   introspectable via `Arg::get_env`).
2. **Markers**: the generated block lands between `<!-- cli-reference:begin -->` /
   `<!-- cli-reference:end -->` markers in `cli/README.md`; hand-written prose outside the markers
   is untouched.
3. **Make targets**: `make cli-docs` regenerates in place; `make cli-docs-check` regenerates to a
   temp file and diffs the marker region, exiting non-zero on drift. Append `cli-docs-check` to
   `checkall` (mirror how `env-drift-check` is wired).
4. **Determinism**: strip terminal-width-dependent wrapping (`clap` renders at a fixed width when
   not a tty; set `term_width(80)` explicitly on the shared `cli()` so output is stable across
   environments).
5. Run `make cli-docs` once and commit the generated section (this supersedes the hand-maintained
   command list DOC-202 wrote, keeping any prose around it).

## Files to touch

- `cli/src/main.rs` / `cli/src/args.rs` (factor out `pub fn cli()`, set `term_width`)
- `cli/src/bin/gen-cli-docs.rs` (new)
- `cli/README.md` (markers + generated region)
- `Makefile` (`cli-docs`, `cli-docs-check`, wire into `checkall`)

## Verification

- `make cli-docs && git diff --exit-code cli/README.md` is clean immediately after generation.
- Drift test: add a dummy flag to one subcommand, run `make cli-docs-check` — it must fail;
  revert the flag.
- `make checkall` green with the new gate included.
- Every one of the 16 commands appears inside the marker region (`grep -c '^### ' region == 16`,
  adjust for actual heading depth).

## Rollback

Remove the Makefile targets and the generator bin; the README keeps its last generated (accurate)
state as plain markdown. No runtime code is affected.
