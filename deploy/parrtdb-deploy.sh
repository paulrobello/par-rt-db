#!/usr/bin/env bash
# Forced-command deploy for the par-rt-db CI deploy key. Invoked over SSH by
# GitHub Actions on a green main push with the full commit SHA as $1.
# Deploys the CI-built, commit-pinned image ghcr.io/paulrobello/par-rt-db:sha-<SHA>.
# The pull happens BEFORE any container stops, so a failed pull (registry
# outage, auth gap) leaves the old container serving — no swarm-style
# stop-then-fail downtime (the par-prd 0/1 incident). Falls back to the
# on-host buildx build (CPU clamps, host-reservation lease, same as
# `make deploy`) when the pull fails, e.g. until the registry tag exists.
# Restricted in authorized_keys so this key can do NOTHING but run this
# script; it deploys that exact SHA and verifies the server came up healthy.
# Tracked source of truth; /usr/local/bin/parrtdb-deploy on the deploy host
# is kept in sync with this file (2026-11-18).
set -euo pipefail
SHA="${1:-}"
# A bare 40-hex SHA is the pinned-image tag suffix AND the only accepted
# input, so an empty or malformed command can never widen into arbitrary
# execution or a default deploy.
if [[ ! "$SHA" =~ ^[0-9a-f]{40}$ ]]; then
  echo "parrtdb-deploy: refusing non-SHA argument" >&2
  exit 2
fi
# Quiet-window lease shared with other tenants of this host (the-cube capacity
# runs) — the same check `make deploy` runs before building. An active lease
# BLOCKS the deploy; the operator releases the lease or uses the manual path.
RESERVATION_FILE=/var/tmp/par-host-reservation.json
if [ -e "$RESERVATION_FILE" ]; then
  rsv=$(jq -ce 'select(type == "object" and (.owner | type == "string" and length > 0) and (.reason | type == "string" and length > 0) and (.until | type == "number" and . >= 0 and . == floor))' "$RESERVATION_FILE") || { echo "parrtdb-deploy: cannot read valid lease at $RESERVATION_FILE" >&2; exit 1; }
  if printf '%s\n' "$rsv" | jq -e --argjson now "$(date +%s)" '.until > $now' >/dev/null; then
    printf '%s\n' "$rsv" | jq -r '"parrtdb-deploy: BLOCKED owner=\(.owner | tojson) reason=\(.reason | tojson) until=\(.until) (unix seconds)"' >&2
    exit 1
  fi
fi
cd /docker/par-rt-db
git fetch origin
git checkout -f "$SHA"
SHORT="$(git rev-parse --short HEAD)"
IMAGE="ghcr.io/paulrobello/par-rt-db:sha-$SHA"
# Pinned-image path: pull first (before anything stops), then swap with
# --no-build so compose can never substitute a local build. SERVER_IMAGE_TAG
# must match the pulled tag exactly; compose addresses the image through it.
if docker pull -q "$IMAGE"; then
  echo "==> parrtdb-deploy: pulled pinned image sha-$SHA"
  SERVER_IMAGE_TAG="sha-$SHA" docker compose up -d --no-build server
else
  # Fallback: registry pull failed (tag absent, auth, outage) — build on
  # host as before. First pin of the rollback generation: tag the outgoing
  # image as :rollback locally so the host always has a known-good neighbor.
  echo "==> parrtdb-deploy: pinned image not pullable; falling back to on-host build"
  OLD_ID=$(docker compose ps -q server | xargs -r docker inspect --format '{{.Image}}' 2>/dev/null || true)
  if [ -n "$OLD_ID" ]; then
    docker tag "$OLD_ID" ghcr.io/paulrobello/par-rt-db:rollback 2>/dev/null || true
  fi
  BUILDER=par-rt-db-builder
  if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
    docker buildx create --name "$BUILDER" --driver docker-container \
      --driver-opt default-load=true --driver-opt cpu-quota=400000 --driver-opt cpu-period=100000 \
      --buildkitd-config /docker/par-rt-db/deploy/buildkitd.toml
  fi
  docker buildx use "$BUILDER"
  docker buildx inspect --bootstrap "$BUILDER" >/dev/null
  BK=$(docker ps --format "{{.Names}}" | grep "^buildx_buildkit_${BUILDER}" | head -1)
  [ -n "$BK" ] || { echo "parrtdb-deploy: builder container not found after bootstrap" >&2; exit 1; }
  docker update --cpu-period 100000 --cpu-quota 300000 "$BK" >/dev/null
  EC=0
  RTDB_BUILD_COMMIT="$SHORT" SERVER_IMAGE_TAG="sha-$SHORT" docker compose up -d --build || EC=$?
  docker update --cpu-period 100000 --cpu-quota 400000 "$BK" >/dev/null
  [ "$EC" -eq 0 ] || exit "$EC"
fi
echo "==> parrtdb-deploy: polling /healthz"
for i in $(seq 1 30); do
  if curl --fail --silent --output /dev/null http://127.0.0.1:8300/healthz; then
    echo "==> parrtdb-deploy: $SHORT ready"
    exit 0
  fi
  sleep 10
done
echo "parrtdb-deploy: /healthz never returned 200 within the 5-minute ceiling" >&2
exit 1
