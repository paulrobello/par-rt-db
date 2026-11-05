# ENH-031 — Tag-driven publish pipeline (npm, crates.io, PyPI) with dry-run gate

## Goal

ENH-026 cut `v0.1.0` with lockstep versions and `docs/RELEASING.md`, but nothing is published:
every client README's install line (`bun add @par-rt-db/client`, `par-rt-db-client = "0.1"`,
`pip install par-rt-db`) fails (audit DOC-009). Add a CI workflow that, on a `v*` tag, publishes
`@par-rt-db/client` to npm, `par-rt-db-client` (and `par-rt-db-core` once ARC-004 lands) to
crates.io, and `par-rt-db` to PyPI via trusted publishing, and — on every PR — runs each registry's
dry-run so a broken manifest is caught before tag time. Swift stays git-tag based (SwiftPM has no
registry step); the workflow verifies the tag resolves from a scratch `Package.swift`.

## Current state

- `.github/workflows/ci.yml`: `checkall` and `swift` jobs only; no `release`/`publish` job.
- No `publish = false` guards, no `"private"` in `ts-client/package.json`, no `[project.urls]`
  check — nothing prevents an accidental publish either.
- Versions are lockstep (`0.1.0` everywhere) and RELEASING.md describes the manual bump.
- Deployment (`make deploy`) is separate and unaffected.

## Implementation

1. **Dry-run gate in `checkall`** (new `publish-check` make target, run in CI's `checkall` job):
   - `cargo publish --dry-run --manifest-path rust-client/Cargo.toml --allow-dirty` (and `core/`,
     `cli/` if the CLI is to be published; decide and document — the CLI probably ships as a
     release binary, not a crate).
   - `bun pm pack --dry-run --cwd ts-client` (or `npm pack --dry-run`) and assert the tarball
     contains `dist/` and not `src/tests`.
   - `uv build --directory python-client` then `uvx twine check python-client/dist/*`.
   - `scripts/version-lockstep-check.sh`: all package manifests report the same version as the
     newest `v*` tag prefix (ENH-026 may already have this; reuse if so).
2. **Registry prerequisites** (human steps, documented in RELEASING.md): create the npm org/package
   with an automation token stored as `NPM_TOKEN`; crates.io API token as `CARGO_REGISTRY_TOKEN`;
   PyPI trusted publisher configured for `paulrobello/par-rt-db` workflow `release.yml`
   (no secret needed). Record these in `docs/RELEASING.md` under "One-time setup".
3. **`.github/workflows/release.yml`** on `push: tags: ["v*"]`:
   - job `verify`: run `make checkall` and `make publish-check` on the tag.
   - job `publish-crates` (needs verify): `cargo publish` for `core` then `rust-client` (order matters
     for the path dependency), each with `continue-on-error: false` and a check that skips if the
     version already exists on crates.io (idempotent re-run).
   - job `publish-npm`: `bun run --cwd ts-client build && npm publish --provenance --access public`
     using `NPM_TOKEN`.
   - job `publish-pypi`: `uv build` then `pypa/gh-action-pypi-publish` (pinned SHA) with
     `id-token: write`.
   - job `swift-tag-check`: from a scratch dir, `swift package resolve` against
     `.package(url: "https://github.com/paulrobello/par-rt-db", exact: "<tag>")` — this requires a
     root `Package.swift`; if the repo keeps `swift-client/Package.swift` only, add a thin root
     manifest re-exporting the products, or document that Swift consumers use a local path. Decide
     once and align `swift-client/README.md` + RELEASING.md (DOC-009).
   - job `github-release`: `gh release create` with the CHANGELOG section for the tag (DOC-001 makes
     this meaningful) and the `rtdb` CLI binaries built for linux-x86_64 and darwin-arm64.
4. **READMEs**: once the first publish succeeds, restore the registry install lines and drop the
   "not yet published" wording DOC-009 adds.
5. **RELEASING.md**: replace the manual steps with "bump lockstep, update CHANGELOG, tag, push;
   CI does the rest", plus the recovery procedure for a partial publish (which job to re-run).

## Files to touch

- `.github/workflows/release.yml` (new), `.github/workflows/ci.yml`, `Makefile`
- `scripts/version-lockstep-check.sh` (new or reuse), `scripts/publish-check.sh` (new)
- `ts-client/package.json` (`files`, `publishConfig`), `rust-client/Cargo.toml`, `core/Cargo.toml` (metadata: description, license, repository, readme), `python-client/pyproject.toml` (`[project.urls]`, classifiers)
- `docs/RELEASING.md`, four client READMEs, `CONTRIBUTING.md`

## Verify

- `make publish-check` green locally and in the CI `checkall` job.
- Push a `v0.1.1-rc.1` tag on a branch (or use `workflow_dispatch` with a dry-run input): all publish jobs reach their upload step and the swift-tag-check resolves.
- After the first real tag: `npm view @par-rt-db/client version`, `cargo search par-rt-db-client`, `pip index versions par-rt-db` each report the tag version.
- Re-running the release workflow on the same tag is a no-op (idempotency check), not a failure.
- `make checkall` green.

## Rollback

Delete the `release.yml` workflow; published versions cannot be unpublished on crates.io (yank
instead) — which is why the dry-run gate lands first and the first real tag is an `-rc`.
