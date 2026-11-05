# Releasing par-rt-db

Releases are lockstep, push-to-main publishes. A push to `main` that passes
the gate starts `.github/workflows/release.yml`, which publishes the version
the tree declares and creates the matching `vX.Y.Z` tag and GitHub release.
This mirrors the owner's deploy-on-push services (green CI on `main` is the
ship event).

## What is published

| Artifact | Destination | Release behavior |
| --- | --- | --- |
| `par-rt-db-core`, then `par-rt-db-client` | crates.io | Core is published first because the client has a path dependency on it. |
| `@par-rt-db/client` | npm | Built from `ts-client`, published with npm provenance. Stable versions carry the `latest` dist-tag; prereleases carry `rc`. |
| `par-rt-db` | PyPI | Built with uv and published through a PyPI trusted publisher. |
| `rtdb` CLI | GitHub Release assets | Binary-only archives: `linux-x86_64` and `darwin-arm64`. |
| `ParRtDbClient`, `ParRtDbUI` | Git tags via SwiftPM | Not a registry package. The repository root `Package.swift` is the package manifest. |

`server` and `rtdb-cli` are not registry packages (`publish = false`). The CLI
is distributed only as the GitHub Release binaries above. `kotlin-client` is
also unpublished: its `build.gradle.kts` version exists only so the lockstep
check below can prove it agrees — consume it from a checkout (see
[`kotlin-client/README.md`](../kotlin-client/README.md)).

Swift consumers use the repository's root package and an exact semantic version
without the `v` prefix:

```swift
.package(url: "https://github.com/paulrobello/par-rt-db", exact: "X.Y.Z")
```

The `X.Y.Z` resolution maps to the repository's `vX.Y.Z` tag. Swift has no
separate registry publication step.

## One-time registry setup

Perform these steps once, before the first real release:

1. Create/claim `@par-rt-db/client` on npm and create a narrowly scoped
   automation token. Store it in the repository Actions secrets as `NPM_TOKEN`.
2. Create a crates.io API token with publish permission and store it as the
   repository Actions secret `CARGO_REGISTRY_TOKEN`. The workflow sends a
   descriptive User-Agent when querying crates.io.
3. On PyPI, before the first upload creates `par-rt-db`, open the account's
   **Publishing** settings and add a **pending publisher** for owner
   `paulrobello`, repository `par-rt-db`, and workflow
   `.github/workflows/release.yml`. Leave the GitHub **Environment** field
   blank: `publish-pypi` declares no environment, so configuring one would
   make the OIDC subject mismatch. No PyPI API secret is needed; the publish
   job requests the GitHub OIDC identity token.
4. Ensure GitHub Actions can create releases by leaving the workflow's
   `contents: write` permission on the `github-release` job. Other jobs remain
   read-only (the npm and PyPI jobs additionally request only `id-token: write`).

## Normal release flow

All eight versioned manifests must agree before releasing:

- `server/Cargo.toml`, `rust-client/Cargo.toml`, `cli/Cargo.toml`, and
  `core/Cargo.toml`
- `ts-client/package.json` and `dashboard/package.json`
- `python-client/pyproject.toml`
- `kotlin-client/build.gradle.kts` (lockstep-only — see above)

1. Bump those manifests to the next version in one commit. Swift has no
   manifest version; its version is represented by the tag. Regenerate the
   lockfiles after bumping (`cargo build` for `Cargo.lock`; `bun install` from
   the repo root for the bun workspace lockfile, which records the workspace
   package versions; `uv lock` in `python-client/` for `uv.lock`, which records
   the package's own version) and include them in the same commit. Gradle has
   no committed lockfile, so the Kotlin manifest needs no regeneration step.
2. Move the matching entries from `CHANGELOG.md`'s `[Unreleased]` section to a
   `## [X.Y.Z] - YYYY-MM-DD` section and update comparison links.
3. Run `make checkall` (which includes `make publish-check`) and fix every
   failure. The publish check dry-runs crates, validates the npm tarball, and
   checks the Python distributions.
4. Commit the manifests and CHANGELOG together, for example:
   `release: vX.Y.Z`.

   Pushing that commit to `main` is the event that starts the release
   workflow. A push to `main` is an outward-facing action — it requires the
   owner's explicit confirmation per the standing push policy, and it now
   doubles as release approval (it publishes whatever version the tree
   declares, if that version is not already out). The pipeline creates the
   `vX.Y.Z` tag itself when it creates the GitHub release; manual tags are no
   longer needed (a manual `v*` tag still backfills Pages via CI's `pages`
   job but triggers no publish).

An `-rc` version such as `X.Y.Z-rc.1` follows the same path and is
recommended for the first real publication. Prerelease handling is derived
from the version string:

- **npm** publishes a prerelease under the `rc` dist-tag, never `latest`
  (install it with `npm install @par-rt-db/client@rc`); a stable version
  publishes under `latest`.
- **GitHub** marks the release `--prerelease` for `-rc` tags.
- **crates.io / PyPI** need no special handling: consumers must request a
  prerelease version explicitly, so nothing resolves to it implicitly.

## CI job order

The workflow is bounded by job timeouts and runs in this order:

1. `verify` checks out `main`, installs the normal Rust/Bun/uv dependencies,
   runs `make checkall`, runs `make publish-check`, and resolves the release
   version from the tree (version-lockstep proved the manifests agree).
2. After verification, `publish-crates`, `publish-npm`, `publish-pypi`, and
   `swift-version-check` run independently. The Swift job resolves the exact
   semantic version from a fresh scratch package, proving a main-push release
   is remotely consumable via SwiftPM once the pipeline creates the tag.
3. `cli-binaries` builds the two platform archives in parallel.
4. `github-release` waits for the publish jobs and both binary builds,
   extracts the matching CHANGELOG section, then creates or updates the
   GitHub release and its `vX.Y.Z` tag at the released commit.
5. CI's `pages` job publishes the API references on every main push, so the
   docs site updates with each release push without depending on the
   pipeline-created tag firing a workflow.

## Idempotency and partial-publish recovery

Every publish operation checks for the exact tree version before uploading. A
successful rerun therefore skips an existing crate/npm/PyPI version. GitHub
release uploads use `--clobber`; release creation is preceded by `gh release
view`, so an existing release is updated rather than recreated.

If a job fails after a partial publish, do not bump the version merely to work
around the failure:

- **Crates:** rerun the workflow. The core guard skips core if it succeeded;
  the client then publishes after the guard. If crates.io is still processing a
  package, wait until its version is visible before rerunning.
- **npm:** fix the token, package metadata, or build issue and rerun. The exact
  `npm view` guard skips an already published version (its dist-tag is left
  untouched).
- **PyPI:** fix trusted-publisher/OIDC or distribution metadata configuration and
  rerun. The guard probes the per-version endpoint
  `https://pypi.org/pypi/par-rt-db/<version>/json` (HTTP 200 means the version
  exists), so it skips any already-published version — including an older
  prerelease that is no longer the latest release. A failed upload can be
  retried without creating a second version.
- **Swift:** rerun the workflow after fixing the package contents. The
  pipeline creates the `vX.Y.Z` tag when it creates the GitHub release; a
  created tag must not be moved — use a new version if its contents need
  correction.
- **CLI/GitHub release:** rerun after fixing the build or permissions. The
  binary artifacts are rebuilt, and `gh release upload --clobber` replaces
  same-named assets while preserving the release and CHANGELOG notes.

Never delete or move a pushed tag. If a published artifact is genuinely bad,
follow the registry's yank/deprecation process and release the next lockstep
version.

## Deploy after release

Deploy per [`deploy/README.md`](../deploy/README.md) if this release ships to
the operator's instance. The deployed binary's version label comes from
`CARGO_PKG_VERSION` automatically (`/healthz` reports `version` and the
deployed commit sha only on an admin-authenticated request; unauthenticated
responses omit both); there is no deploy-script version to update.

## Post-first-publish README restoration

The client READMEs intentionally retain their pre-publication wording until a
first release has successfully published. After confirming the first tag in
all three registries:

- restore the npm install command in `ts-client/README.md`;
- restore the crates.io dependency in `rust-client/README.md`;
- restore the `pip install par-rt-db` instructions in `python-client/README.md`;
- update any corresponding pre-publication note in `docs/clients.md`.

Keep `swift-client/README.md`'s git-tag installation instructions: Swift
continues to consume the root package by exact semantic version (without `v`),
not a registry artifact. The tag itself is created by the release pipeline
when it creates the GitHub release.
