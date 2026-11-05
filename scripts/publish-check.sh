#!/usr/bin/env bash
# ENH-031: validate every artifact before a registry publish or release tag.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
  echo "usage: scripts/publish-check.sh [--require-tag]" >&2
  exit 2
}
require_tag=0
if [ "$#" -gt 1 ]; then usage; fi
if [ "$#" -eq 1 ]; then
  [ "$1" = "--require-tag" ] || usage
  require_tag=1
fi

for tool in awk bun cargo curl grep npm uv uvx; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "publish-check: required command not found: $tool" >&2
    exit 1
  }
done

if [ "$require_tag" -eq 1 ]; then
  scripts/version-lockstep-check.sh --require-tag
else
  scripts/version-lockstep-check.sh
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

cargo_publish_false() {
  awk '
    /^\[package\][[:space:]]*$/ { in_package=1; next }
    /^\[/ { in_package=0 }
    in_package && /^publish[[:space:]]*=/ {
      value=$3; gsub(/["[:space:]]/, "", value)
      if (value == "false") found=1
    }
    END { exit(found ? 0 : 1) }
  ' "$1"
}

echo "publish-check: checking server and CLI publish guards"
for manifest in server/Cargo.toml cli/Cargo.toml; do
  if ! cargo_publish_false "$manifest"; then
    echo "publish-check: $manifest must set publish = false in [package]" >&2
    exit 1
  fi
done

echo "publish-check: dry-running par-rt-db-core"
cargo publish --dry-run --allow-dirty --manifest-path core/Cargo.toml

core_version=$(cargo_version core/Cargo.toml)
core_url="https://crates.io/api/v1/crates/par-rt-db-core/$core_version"
if ! core_status=$(curl -sS -H 'User-Agent: par-rt-db-publish-check/1.0 (probello@gmail.com)' -o /dev/null -w '%{http_code}' "$core_url"); then
  echo "publish-check: unable to query crates.io for par-rt-db-core $core_version" >&2
  exit 1
fi
case "$core_status" in
  200)
    echo "publish-check: par-rt-db-core $core_version is on crates.io; dry-running rust client"
    cargo publish --dry-run --allow-dirty --manifest-path rust-client/Cargo.toml
    ;;
  404)
    echo "NOTICE: par-rt-db-core $core_version is not on crates.io; checking rust-client package contents without registry resolution"
    cargo package --list --allow-dirty --manifest-path rust-client/Cargo.toml >/tmp/par-rt-db-rust-package-list
    grep -q '^src/' /tmp/par-rt-db-rust-package-list || {
      echo "publish-check: rust-client package contains no src/ files" >&2
      exit 1
    }
    ;;
  *)
    echo "publish-check: crates.io returned HTTP $core_status for $core_url" >&2
    exit 1
    ;;
esac

echo "publish-check: building npm package"
(
  cd ts-client
  bun run build
)
npm_output=$(cd ts-client && npm pack --dry-run 2>&1)
printf '%s\n' "$npm_output"
if ! printf '%s\n' "$npm_output" | grep -Eq '[[:space:]]dist/'; then
  echo "publish-check: npm dry-run contains no dist/ files" >&2
  exit 1
fi
if printf '%s\n' "$npm_output" | grep -Eq '[[:space:]](src|tests)/'; then
  echo "publish-check: npm dry-run unexpectedly contains src/ or tests/ files" >&2
  exit 1
fi

if ! cmp -s wire-corpus/query-combinations.json python-client/src/par_rt_db/_data/query-combinations.json; then
  echo "publish-check: Python package corpus differs from canonical wire-corpus" >&2
  exit 1
fi

echo "publish-check: building Python artifacts"
uv build --directory python-client
shopt -s nullglob
python_artifacts=(python-client/dist/*)
shopt -u nullglob
if [ "${#python_artifacts[@]}" -eq 0 ]; then
  echo "publish-check: uv build produced no python-client/dist artifacts" >&2
  exit 1
fi
uvx twine check python-client/dist/*
if git check-ignore -q python-client/dist; then
  echo "publish-check: retaining ignored python-client/dist artifacts"
else
  rm -rf python-client/dist
  echo "publish-check: removed generated python-client/dist artifacts"
fi

echo "publish-check: all registry artifacts and publish guards passed"
