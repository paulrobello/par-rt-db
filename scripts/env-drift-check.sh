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

# ARC-010: Config::from_env → docker-compose.yml bridge. The check above only
# knows about `.env.example`. A `Config::from_env` key that is neither in
# `.env.example` nor in docker-compose.yml would slip past it and silently keep
# its code default in every docker deploy. This second pass closes that gap by
# diffing every `std::env::var("RTDB_*")` call across server/src/ (not just
# config.rs — RTDB_ADMIN_EMAILS is read at main.rs) against the forwarded set.
#
# A short exempt list documents the keys that are intentionally defaulted in the
# docker deploy — adding to this list requires a comment explaining why the code
# default is the right value to ship with (not just "we forgot"). New keys
# added to `Config::from_env` are NOT exempt and must be added to
# docker-compose.yml at the same PR or this check fails.
#
# Every production RTDB_* key is forwarded with a code-matching default
# (DOC2-001 / DOC2-019). The only exemptions are test-only keys.
exempt=$(cat <<'EOF'
RTDB_TEST_DATABASE_URL
EOF
)
# RTDB_TEST_DATABASE_URL: read only inside #[cfg(test)] blocks in backup.rs
# (integration tests point it at the dev Postgres). Not a production knob —
# must never be forwarded to the docker deploy.

# Pull every RTDB_* key the server reads (config.rs + main.rs + any module).
read_in_code=$(grep -roE 'std::env::var\("RTDB_[A-Z_]+"\)' server/src/ \
  | sed -E 's/.*"(.+)".*/\1/' | sort -u)

# Subtract the forwarded set and the exempt list. Anything left is a key the
# code reads but the docker deploy never forwards AND that isn't on the
# documented exemption list — i.e., a silent default waiting to bite.
# `{ ...; } | sort -u` is required so comm sees a single sorted stream — two
# separate echoes inside the process substitution would leave the second half
# unsorted and comm would emit garbage.
missing_from_code=$(comm -23 \
  <(echo "$read_in_code") \
  <({ echo "$forwarded"; echo "$exempt"; } | sort -u) \
  | grep -v '^$' || true)

if [ -n "$missing_from_code" ]; then
  echo "env drift: read by the server but NOT forwarded in docker-compose.yml and NOT on the exempt list:" >&2
  echo "$missing_from_code" | sed 's/^/  - /' >&2
  echo >&2
  echo "Either add each to docker-compose.yml's 'environment:' block with the code default," >&2
  echo "or add it to the exempt list in scripts/env-drift-check.sh with a comment." >&2
  exit 1
fi

echo "env-drift-check: all $(echo "$read_in_code" | wc -l | tr -d ' ') server-read RTDB_* keys are forwarded or explicitly exempt"
