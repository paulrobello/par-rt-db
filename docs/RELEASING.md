# Releasing par-rt-db

The repeatable cut procedure. Everything versions in **lockstep** — `server`,
`cli`, `dashboard`, and the client SDKs (`ts-client`, `rust-client`,
`python-client`, `swift-client`) share one version (see
[`../CONTRIBUTING.md`'s Versioning section](../CONTRIBUTING.md#versioning)).
`swift-client` has no manifest version to bump — SPM carries no version field,
so the release tag itself is its version. Nothing here publishes to a registry;
a release is a git tag plus the CHANGELOG
heading. Publishing the SDKs to crates.io / npm / PyPI is a separate,
user-approved decision.

## Procedure

1. **Bump versions (lockstep)** to the next `0.x.y` in:
   - `server/Cargo.toml`, `rust-client/Cargo.toml`, `cli/Cargo.toml`
   - `ts-client/package.json`, `dashboard/package.json`
   - `python-client/pyproject.toml`
   Regenerate the lockfiles (`cargo build` for `Cargo.lock`; `bun install` from
   the root for the bun lockfile) and commit them. `swift-client` is absent
   from this list on purpose: SPM manifests carry no version field, so the
   release tag is its version — consumers pin the tag.
2. **Update `CHANGELOG.md`**: move the accumulated `[Unreleased]` entries under
   a new `## [0.x.y] - <date>` heading, leave a fresh empty `## [Unreleased]`
   on top, and update the footer compare links
   (`[Unreleased]: …/compare/v0.x.y...HEAD`, `[0.x.y]: …/releases/tag/v0.x.y`).
3. **Run the gate**: `make checkall` from the repo root must be green. On
   Darwin that already sweeps `swift-client` (the aggregate fmt/lint/typecheck/
   test targets carry Darwin-guarded swift lines); on Linux those lines skip
   loudly and the macOS CI job runs `make swift-client-checkall`.
4. **Commit**: `release: v0.x.y` (versions, lockfiles, CHANGELOG together).
5. **Tag the release commit** (annotated, so it carries the release message):
   ```bash
   git tag -a v0.x.y -m "par-rt-db v0.x.y"
   ```
6. **Push the commit and the tag.** Pushing a tag is an outward-facing action —
   it requires the owner's explicit confirmation per the standing policy.
   ```bash
   git push origin main && git push origin v0.x.y
   ```
7. **Optionally create the GitHub release** from the tag with the CHANGELOG
   section as notes (`gh release create v0.x.y --title "v0.x.y" --notes-file …`).
8. **Deploy** per [`deploy/README.md`](deploy/README.md) if this release ships
   to the operator's instance. The deployed binary's version label comes from
   `CARGO_PKG_VERSION` automatically (`/healthz` reports it); no deploy-script
   version to update.

## Rules

- **Never delete or move a pushed tag.** If a cut turns out bad, fix forward
  with `0.x.(y+1)`. Before push, `git tag -d v0.x.y` and revert is fine.
- CI (`ci.yml`) gates every push including tags; it publishes nothing, so a tag
  alone triggers no registry action.
- The pre-release check is the same gate as everything else: `make checkall`.
