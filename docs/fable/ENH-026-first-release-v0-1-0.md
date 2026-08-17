# ENH-026 — Cut the first release (v0.1.0) and establish the release process

## Goal

Give the changelog and the four-package version claim teeth: tag `v0.1.0`, move the ~390
`[Unreleased]` CHANGELOG lines under a real version heading, align the four package manifests, and
document a lightweight repeatable release procedure. Audit finding DOC-217 (2026-08-16): the
CHANGELOG claims semver but no release has ever been cut.

## Current state

- `CHANGELOG.md` is Keep-a-Changelog form, everything under `[Unreleased]` with ad-hoc dated
  subsections; the diff-link footer points at the repo root.
- Four publishable-ish surfaces with independent manifests: `server/Cargo.toml`,
  `ts-client/package.json` (`@par-rt-db/client`), `rust-client/Cargo.toml` (`par-rt-db-client`),
  `python-client/pyproject.toml` (`par-rt-db`), plus `cli/Cargo.toml` and `dashboard/`.
- Nothing is published to a registry (private repo; consumers vendor the ts-client — see the
  "Client changes vendored to projects dashboard" memory). CI (`ci.yml`) gates but publishes
  nothing. **This plan tags and versions; it does NOT publish to any registry** (that remains a
  separate, user-approved decision).

## Implementation

1. **Version choice**: lockstep `0.1.0` across server, cli, and the three clients (one version for
   the whole protocol surface — the four-way mirror makes independent client versions a lie today).
   Record the lockstep decision in `CONTRIBUTING.md`.
2. **Manifests**: set `version = "0.1.0"` in the two workspace Cargo.tomls' members (or
   `workspace.package.version` + `version.workspace = true` if not already wired — check the
   workspace unification from ARC-117), `ts-client/package.json`, `python-client/pyproject.toml`.
3. **CHANGELOG**: insert `## [0.1.0] - <date>` above the accumulated entries, keeping the existing
   dated subsections as-is beneath it; fresh empty `[Unreleased]` on top; fix the footer compare
   links (`[Unreleased]: <repo>/compare/v0.1.0...HEAD`, `[0.1.0]: <repo>/releases/tag/v0.1.0`).
4. **Release procedure doc**: short `docs/RELEASING.md` — bump versions (lockstep), update
   CHANGELOG, `make checkall`, commit `release: v0.x.y`, `git tag v0.x.y`, push with tags
   (push requires user confirmation per standing policy), then deploy per `deploy/README.md`.
5. **Tag**: create the annotated tag `v0.1.0` on the release commit. Do not push without the
   user's explicit confirmation.
6. **Deploy label check**: `deploy/` stamps a `git_commit` label — confirm nothing hardcodes a
   version string that now needs the manifest version (grep `0.0.0`/`version` in deploy scripts
   and `lib.rs` build-info; the admin-gated build fingerprint should pick up the new version
   automatically if it reads `CARGO_PKG_VERSION`).

## Files to touch

- `Cargo.toml` (workspace) + member Cargo.tomls, `ts-client/package.json`,
  `python-client/pyproject.toml`
- `CHANGELOG.md`, `CONTRIBUTING.md`, `docs/RELEASING.md` (new)

## Verification

- `make checkall` green after the version bumps (lockfiles regenerate: `bun install` from root,
  `cargo build`, `uv sync` — commit the lockfile changes).
- `git tag --list v0.1.0` shows the tag; `git show v0.1.0 --stat` is the release commit.
- CHANGELOG has an empty `[Unreleased]` and a populated `[0.1.0]`; footer links resolve to the
  right compare URLs.
- `grep -n "version" ts-client/package.json python-client/pyproject.toml rust-client/Cargo.toml server/Cargo.toml` all show 0.1.0.

## Rollback

Before push: `git tag -d v0.1.0` and revert the commit. After push: leave the tag (tags are
outward-facing; never delete a pushed tag silently) and cut `0.1.1` instead.
