#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"

fail() {
    printf '%s\n' "ts-dist-preflight: $1" \
        "The dashboard resolves @par-rt-db/client from gitignored ts-client/dist." \
        "Run 'make ts-client-build' and retry." >&2
    exit 1
}

# Both exports must exist. Checking the oldest entry point also catches a
# partially refreshed build rather than trusting one newly emitted file.
for output in ts-client/dist/index.js ts-client/dist/index.d.ts; do
    [ -f "$output" ] || fail "missing $output"
    newer=$(find ts-client/src ts-client/package.json ts-client/tsconfig.json \
        ts-client/tsconfig.build.json -type f -newer "$output" -print -quit)
    [ -z "$newer" ] || fail "$output is stale (newer input: $newer)"
done
printf '%s\n' 'ts-dist-preflight: SDK entry points are present and current'
