#!/usr/bin/env bash
# ENH-031: verify all publishable and workspace manifests share one version.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  echo "usage: scripts/version-lockstep-check.sh [--require-tag]" >&2
  exit 2
}

require_tag=0
if [ "$#" -gt 1 ]; then usage; fi
if [ "$#" -eq 1 ]; then
  [ "$1" = "--require-tag" ] || usage
  require_tag=1
fi

cargo_version() {
  awk '
    /^\[package\][[:space:]]*$/ { in_package=1; next }
    /^\[/ { in_package=0 }
    in_package && /^version[[:space:]]*=/ {
      v=$3; gsub(/["[:space:]]/, "", v); print v; found=1; exit
    }
    END { if (!found) exit 1 }
  ' "$1"
}

python_version() {
  awk '
    /^\[project\][[:space:]]*$/ { in_project=1; next }
    /^\[/ { in_project=0 }
    in_project && /^version[[:space:]]*=/ {
      v=$3; gsub(/["[:space:]]/, "", v); print v; found=1; exit
    }
    END { if (!found) exit 1 }
  ' "$1"
}

json_version() {
  command -v bun >/dev/null 2>&1 || {
    echo "version-lockstep: bun is required to parse package.json" >&2
    return 1
  }
  bun -e 'console.log(JSON.parse(require("fs").readFileSync(process.argv[1], "utf8")).version)' "$1"
}

manifests=(
  server/Cargo.toml
  rust-client/Cargo.toml
  cli/Cargo.toml
  core/Cargo.toml
  ts-client/package.json
  dashboard/package.json
  python-client/pyproject.toml
)
versions=()
for manifest in "${manifests[@]}"; do
  [ -f "$manifest" ] || { echo "version-lockstep: missing manifest $manifest" >&2; exit 1; }
  case "$manifest" in
    */package.json) version=$(json_version "$manifest") ;;
    python-client/pyproject.toml) version=$(python_version "$manifest") ;;
    *) version=$(cargo_version "$manifest") ;;
  esac
  [ -n "$version" ] || { echo "version-lockstep: no version found in $manifest" >&2; exit 1; }
  versions+=("$version")
done

expected="${versions[0]}"
drifting=()
for i in "${!manifests[@]}"; do
  if [ "${versions[$i]}" != "$expected" ]; then
    drifting+=("${manifests[$i]}: ${versions[$i]} (expected $expected)")
  fi
done
if [ "${#drifting[@]}" -gt 0 ]; then
  echo "version-lockstep: manifests disagree:" >&2
  printf '  - %s\n' "${drifting[@]}" >&2
  exit 1
fi

newest_tag=$(git tag --list 'v*' --sort=-v:refname | head -n 1)
if [ -z "$newest_tag" ]; then
  if [ "$require_tag" -eq 1 ]; then
    echo "version-lockstep: --require-tag set but no v* tag exists" >&2
    exit 1
  fi
  echo "version-lockstep: all ${#manifests[@]} manifests at $expected (no v* tag; nothing to compare)"
  exit 0
fi

tag_version="${newest_tag#v}"
if [ "$tag_version" != "$expected" ]; then
  if [ "$require_tag" -eq 1 ]; then
    echo "version-lockstep: manifests are at $expected but newest tag ${newest_tag} is ${tag_version}" >&2
    exit 1
  fi
  echo "version-lockstep: all ${#manifests[@]} manifests at $expected"
  echo "NOTICE: newest tag ${newest_tag} is ${tag_version} (pass --require-tag to enforce)"
  exit 0
fi

echo "version-lockstep: all ${#manifests[@]} manifests at $expected, matching newest tag ${newest_tag}"
