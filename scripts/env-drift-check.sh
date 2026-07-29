#!/usr/bin/env bash
# Fails when a RTDB_* variable documented in .env.example is not forwarded to
# the server container by docker-compose.yml.
#
# Compose's `environment:` block is an explicit allowlist: a key set in `.env`
# but missing there never reaches the process, which silently keeps its code
# default. That bit us on 2026-07-29 — RTDB_SUBS_VERIFY_SKIP_EVERY was set in
# prod's .env and did nothing, and five other documented knobs turned out to be
# unreachable the same way. This check makes that drift a build failure.
#
# Direction is deliberately one-way: documented-but-not-forwarded is a bug,
# while forwarded-but-not-documented is fine (RTDB_PORT and RTDB_DATABASE_URL
# are composed from other values and intentionally absent from .env.example).
set -euo pipefail

cd "$(dirname "$0")/.."

documented=$(grep -oE '^RTDB_[A-Z_]+' .env.example | sort -u)
forwarded=$(grep -oE '^ *RTDB_[A-Z_]+:' docker-compose.yml | tr -d ' :' | sort -u)

# RTDB_BUILD_COMMIT is a build-arg for the image build stage, not a runtime env
# var for the server service, so it is exempt from the runtime allowlist.
missing=$(comm -23 <(echo "$documented") <(echo "$forwarded") | grep -v '^RTDB_BUILD_COMMIT$' || true)

if [ -n "$missing" ]; then
  echo "env drift: documented in .env.example but NOT forwarded in docker-compose.yml:" >&2
  echo "$missing" | sed 's/^/  - /' >&2
  echo >&2
  echo "Add each to the server service's 'environment:' block with a default that" >&2
  echo "matches the server's code default, or remove it from .env.example." >&2
  exit 1
fi

echo "env-drift-check: all $(echo "$documented" | wc -l | tr -d ' ') documented RTDB_* vars are forwarded"
